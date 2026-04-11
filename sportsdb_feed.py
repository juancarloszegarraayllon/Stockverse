"""TheSportsDB fallback poller for leagues not covered by ESPN.

ESPN's public scoreboard API doesn't expose every league (e.g. J2
League, K League, Polish Ekstraklasa). TheSportsDB's free livescore
endpoint aggregates live games across leagues worldwide in one call
per sport, so we use it as a secondary source: /api/events tries
ESPN first, then falls back to SPORTSDB_GAMES.

Runs as an asyncio background task, polls serially (not in parallel)
and sleeps between requests so we stay under the free tier's 10
requests/minute rate limit. The free key "3" is embedded in their
docs for low-volume public use.
"""
import asyncio
import logging
import re
import time
import unicodedata
from typing import Any, Dict, List, Optional

try:
    import httpx
except ImportError:
    httpx = None

log = logging.getLogger("sportsdb_feed")

# TheSportsDB free public key (from their docs, intended for
# development / low-volume use).
API_KEY = "3"
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

# (SportsDB sport name, OddsIQ sport name). Names on the right must
# match the Kalshi-derived `_sport` values so match_game can filter.
SPORTS = [
    ("Soccer", "Soccer"),
    ("Basketball", "Basketball"),
    ("American Football", "Football"),
    ("Baseball", "Baseball"),
    ("Ice Hockey", "Hockey"),
]

POLL_INTERVAL = 30  # seconds between full refresh cycles

SPORTSDB_GAMES: List[Dict[str, Any]] = []

STATUS = {
    "running": False,
    "last_fetch_ts": None,
    "games": 0,
    "sports": {},   # sport label -> count
    "last_error": None,
}


def _normalize(s: Any) -> str:
    """Lowercase + strip accents."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower()


def _team_phrases(team_name: str) -> List[str]:
    """Extract search phrases from a SportsDB team name, similar to
    espn_feed's version but from a single field instead of several."""
    if not team_name:
        return []
    n = _normalize(team_name).strip()
    phrases = set()
    if len(n) >= 3:
        phrases.add(n)
    words = n.split()
    # Add the last 1-2 words so "JEF United Chiba" also matches on
    # "chiba" or "united chiba".
    if len(words) >= 2:
        last_two = " ".join(words[-2:])
        if len(last_two) >= 3:
            phrases.add(last_two)
        if len(words[-1]) >= 3:
            phrases.add(words[-1])
    # Also add the first word so "Club America" matches on "america".
    if len(words) >= 2 and len(words[0]) >= 3:
        phrases.add(words[0])
    return sorted(phrases, key=lambda s: -len(s))


def _parse_event(ev: Dict[str, Any], sport_label: str) -> Optional[Dict[str, Any]]:
    status = (ev.get("strStatus") or "").strip()
    lstatus = status.lower()
    # Skip anything not yet started or administratively dead.
    if not status or any(k in lstatus for k in (
        "not started", "postponed", "cancelled", "tbd",
    )):
        return None
    finished_tokens = (
        "ft", "full time", "finished", "final", "ended",
        "match finished", "after extra time", "aet",
        "pen.", "penalty shootout",
    )
    state = "post" if any(k in lstatus for k in finished_tokens) else "in"
    home_name = (ev.get("strHomeTeam") or "").strip()
    away_name = (ev.get("strAwayTeam") or "").strip()
    if not home_name or not away_name:
        return None
    home_score = ev.get("intHomeScore")
    away_score = ev.get("intAwayScore")
    g = {
        "sport": sport_label,
        "league": (ev.get("strLeague") or "").strip(),
        "home_display": home_name,
        "away_display": away_name,
        "home_phrases": _team_phrases(home_name),
        "away_phrases": _team_phrases(away_name),
        # SportsDB doesn't expose abbreviations — fall back to short
        # upper-case nicknames so the score badge reads naturally.
        "home_abbr": (home_name.split()[0][:3] or "HOM").upper(),
        "away_abbr": (away_name.split()[0][:3] or "AWY").upper(),
        "home_score": "" if home_score in (None, "") else str(home_score),
        "away_score": "" if away_score in (None, "") else str(away_score),
        "state": state,
        "display_clock": "",
        "period": 0,
        "short_detail": status,
        "detail": status,
        "description": status,
        "clock_running": False,  # SportsDB doesn't give us a running clock
    }
    # Derive period + clock for soccer from the strProgress minute.
    progress = (ev.get("strProgress") or "").strip()
    if sport_label == "Soccer" and progress:
        try:
            m = int(re.match(r"(\d+)", progress).group(1))
            if "1h" in lstatus or "first half" in lstatus:
                g["period"] = 1
                g["display_clock"] = f"{m}'"
            elif "2h" in lstatus or "second half" in lstatus:
                g["period"] = 2
                g["display_clock"] = f"{m}'"
        except Exception:
            pass
    return g


async def _fetch_sport(client, sportsdb_name: str, label: str):
    try:
        url = f"{BASE_URL}/livescore.php?s={sportsdb_name.replace(' ', '%20')}"
        r = await client.get(url, timeout=15.0)
        if r.status_code != 200:
            log.debug("sportsdb %s: HTTP %d", sportsdb_name, r.status_code)
            return None
        data = r.json() or {}
        events = data.get("livescore") or data.get("events") or []
        if not isinstance(events, list):
            return []
        now_ms = int(time.time() * 1000)
        out = []
        for ev in events:
            parsed = _parse_event(ev, label)
            if parsed:
                parsed["captured_at_ms"] = now_ms
                out.append(parsed)
        return out
    except Exception as e:
        log.debug("sportsdb %s err: %s", sportsdb_name, e)
        return None


async def run_sportsdb_feed():
    global SPORTSDB_GAMES
    if httpx is None:
        log.warning("httpx not installed — sportsdb feed disabled")
        return
    STATUS["running"] = True
    headers = {"User-Agent": "oddsiq/1.0"}
    async with httpx.AsyncClient(headers=headers) as client:
        while True:
            try:
                all_games: List[Dict[str, Any]] = []
                sport_counts: Dict[str, int] = {}
                # Serial, with a small delay between requests — the
                # free tier is rate-limited to ~10 req/min.
                for sportsdb_name, label in SPORTS:
                    res = await _fetch_sport(client, sportsdb_name, label)
                    if res is not None:
                        all_games.extend(res)
                        sport_counts[label] = sport_counts.get(label, 0) + len(res)
                    await asyncio.sleep(1.2)
                SPORTSDB_GAMES = all_games
                STATUS["last_fetch_ts"] = time.time()
                STATUS["games"] = len(all_games)
                STATUS["sports"] = sport_counts
                STATUS["last_error"] = None
                log.info("sportsdb: %d live games across %d sports",
                         len(all_games), len(sport_counts))
            except Exception as e:
                STATUS["last_error"] = f"{type(e).__name__}: {e}"
                log.error("sportsdb poll error: %s", e)
            await asyncio.sleep(POLL_INTERVAL)


def _phrase_in_title(phrase: str, normalized_title: str) -> bool:
    if not phrase:
        return False
    pattern = r"(?<!\w)" + re.escape(phrase) + r"(?!\w)"
    return re.search(pattern, normalized_title) is not None


def match_game(title: str, sport: str) -> Optional[Dict[str, Any]]:
    """Same matching rules as espn_feed.match_game: whole-word,
    accent-insensitive. Returns the first SportsDB live game whose
    home and away phrases both appear in the Kalshi title."""
    if not title or not sport:
        return None
    t = _normalize(title)
    for g in SPORTSDB_GAMES:
        if g.get("sport") != sport:
            continue
        home_hit = any(_phrase_in_title(p, t) for p in g.get("home_phrases", []))
        away_hit = any(_phrase_in_title(p, t) for p in g.get("away_phrases", []))
        if home_hit and away_hit:
            return g
    return None
