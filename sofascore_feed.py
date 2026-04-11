"""SofaScore fallback feed for leagues ESPN and SportsDB don't cover.

SofaScore exposes a public, unauthenticated JSON API at
api.sofascore.com that returns all currently-live events per sport
in a single call — no key required. It covers virtually every
league worldwide (J1/J2, K League, A-League, Chinese SL, NCAA
baseball, cricket, tennis, etc.) which is exactly the gap we have
from ESPN + SportsDB.

The API is unofficial but has been stable for years and is what
most "public sports tracker" side projects use. If their schema
changes, log + recover.
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

log = logging.getLogger("sofascore_feed")

# (SofaScore slug, OddsIQ sport name)
SPORTS = [
    ("football",           "Soccer"),
    ("basketball",         "Basketball"),
    ("american-football",  "Football"),
    ("baseball",           "Baseball"),
    ("ice-hockey",         "Hockey"),
    ("cricket",            "Cricket"),
    ("tennis",             "Tennis"),
    ("rugby",              "Rugby"),
]

BASE_URL = "https://api.sofascore.com/api/v1/sport/{slug}/events/live"
POLL_INTERVAL = 15  # seconds between full refresh cycles

SOFASCORE_GAMES: List[Dict[str, Any]] = []

STATUS = {
    "running": False,
    "last_fetch_ts": None,
    "games": 0,
    "sports": {},   # sport label -> count
    "last_error": None,
    # Per-sport fetch result for diagnosing why we might be returning
    # zero games. Shape: {slug: {"status": int|str, "count": int|None}}
    "per_sport": {},
}


def _normalize(s: Any) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower()


def _team_phrases(team: Dict[str, Any]) -> List[str]:
    """Build accent-stripped search phrases from a SofaScore team dict."""
    phrases = set()
    for key in ("name", "shortName", "fullName"):
        v = team.get(key)
        if v:
            n = _normalize(v).strip()
            if len(n) >= 3:
                phrases.add(n)
    slug = team.get("slug")
    if slug:
        # "mito-hollyhock" -> "mito hollyhock"
        n = _normalize(slug.replace("-", " ")).strip()
        if len(n) >= 3:
            phrases.add(n)
    # Last word + last two words of the full name, so "JEF United Chiba"
    # also matches on "chiba" or "united chiba" in Kalshi titles that
    # trim the club prefix.
    for src in (team.get("name"), team.get("shortName"), team.get("fullName")):
        if not src:
            continue
        words = _normalize(src).split()
        if len(words) >= 2:
            last_two = " ".join(words[-2:])
            if len(last_two) >= 3:
                phrases.add(last_two)
            if len(words[-1]) >= 3:
                phrases.add(words[-1])
            if len(words[0]) >= 3:
                phrases.add(words[0])
    return sorted(phrases, key=lambda s: -len(s))


def _parse_event(ev: Dict[str, Any], sport_label: str) -> Optional[Dict[str, Any]]:
    status = ev.get("status") or {}
    stype = status.get("type", "")
    desc = (status.get("description") or "").strip()
    if stype == "inprogress":
        state = "in"
    elif stype == "finished":
        state = "post"
    else:
        return None
    home = ev.get("homeTeam") or {}
    away = ev.get("awayTeam") or {}
    home_name = (home.get("name") or "").strip()
    away_name = (away.get("name") or "").strip()
    if not home_name or not away_name:
        return None
    home_score_obj = ev.get("homeScore") or {}
    away_score_obj = ev.get("awayScore") or {}
    home_score = home_score_obj.get("current")
    away_score = away_score_obj.get("current")
    tournament = ev.get("tournament") or {}
    league = (tournament.get("name") or "").strip()

    g = {
        "sport": sport_label,
        "league": league,
        "home_display": home_name,
        "away_display": away_name,
        "home_phrases": _team_phrases(home),
        "away_phrases": _team_phrases(away),
        "home_abbr": (home.get("nameCode") or home_name.split()[0][:3] or "HOM").upper(),
        "away_abbr": (away.get("nameCode") or away_name.split()[0][:3] or "AWY").upper(),
        "home_score": "" if home_score is None else str(home_score),
        "away_score": "" if away_score is None else str(away_score),
        "state": state,
        "display_clock": "",
        "period": 0,
        "short_detail": desc,
        "detail": desc,
        "description": desc,
        "clock_running": False,
    }

    low = desc.lower()
    # Derive period for soccer from the status description; derive the
    # minute by adding wall-clock seconds since currentPeriodStartTimestamp.
    if sport_label == "Soccer":
        if "1st half" in low or "first half" in low:
            g["period"] = 1
        elif "2nd half" in low or "second half" in low:
            g["period"] = 2
        elif "halftime" in low:
            g["short_detail"] = "HT"
        time_obj = ev.get("time") or {}
        start_ts = time_obj.get("currentPeriodStartTimestamp")
        try:
            if start_ts and g["period"] in (1, 2):
                elapsed_s = int(time.time()) - int(start_ts)
                if elapsed_s >= 0:
                    m_in_period = max(1, elapsed_s // 60)
                    total_m = m_in_period if g["period"] == 1 else 45 + m_in_period
                    g["display_clock"] = f"{total_m}'"
        except Exception:
            pass
    return g


async def _fetch_sport(client, slug: str, label: str):
    """Returns (list_of_games, per_sport_status_dict). list_of_games
    is None if the fetch failed outright; per_sport_status_dict is
    always populated for diagnostics."""
    info: Dict[str, Any] = {"status": None, "count": None}
    try:
        url = BASE_URL.format(slug=slug)
        r = await client.get(url, timeout=15.0)
        info["status"] = r.status_code
        if r.status_code != 200:
            log.debug("sofascore %s: HTTP %d", slug, r.status_code)
            return None, info
        data = r.json() or {}
        events = data.get("events") or []
        if not isinstance(events, list):
            info["count"] = 0
            return [], info
        now_ms = int(time.time() * 1000)
        out = []
        for ev in events:
            parsed = _parse_event(ev, label)
            if parsed:
                parsed["captured_at_ms"] = now_ms
                out.append(parsed)
        info["count"] = len(out)
        return out, info
    except Exception as e:
        info["status"] = f"err:{type(e).__name__}"
        log.debug("sofascore %s err: %s", slug, e)
        return None, info


async def run_sofascore_feed():
    global SOFASCORE_GAMES
    if httpx is None:
        log.warning("httpx not installed — sofascore feed disabled")
        return
    STATUS["running"] = True
    # SofaScore's API is behind Cloudflare and 403s any request that
    # doesn't look like a browser. Send the same headers the web UI
    # sends: a real Chrome UA, an Accept header, and a Referer/Origin
    # that matches their site.
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
    }
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        while True:
            try:
                tasks = [
                    _fetch_sport(client, slug, label)
                    for slug, label in SPORTS
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                all_games: List[Dict[str, Any]] = []
                sport_counts: Dict[str, int] = {}
                per_sport: Dict[str, Dict[str, Any]] = {}
                for (slug, label), res in zip(SPORTS, results):
                    if isinstance(res, Exception):
                        per_sport[slug] = {"status": f"err:{type(res).__name__}", "count": None}
                        continue
                    if res is None:
                        per_sport[slug] = {"status": "none", "count": None}
                        continue
                    games, info = res
                    per_sport[slug] = info
                    if games:
                        all_games.extend(games)
                        sport_counts[label] = sport_counts.get(label, 0) + len(games)
                SOFASCORE_GAMES = all_games
                STATUS["last_fetch_ts"] = time.time()
                STATUS["games"] = len(all_games)
                STATUS["sports"] = sport_counts
                STATUS["per_sport"] = per_sport
                STATUS["last_error"] = None
                log.info("sofascore: %d live games across %d sports (per_sport=%s)",
                         len(all_games), len(sport_counts), per_sport)
            except Exception as e:
                STATUS["last_error"] = f"{type(e).__name__}: {e}"
                log.error("sofascore poll error: %s", e)
            await asyncio.sleep(POLL_INTERVAL)


def _phrase_in_title(phrase: str, normalized_title: str) -> bool:
    if not phrase:
        return False
    pattern = r"(?<!\w)" + re.escape(phrase) + r"(?!\w)"
    return re.search(pattern, normalized_title) is not None


def match_game(title: str, sport: str) -> Optional[Dict[str, Any]]:
    if not title or not sport:
        return None
    t = _normalize(title)
    for g in SOFASCORE_GAMES:
        if g.get("sport") != sport:
            continue
        home_hit = any(_phrase_in_title(p, t) for p in g.get("home_phrases", []))
        away_hit = any(_phrase_in_title(p, t) for p in g.get("away_phrases", []))
        if home_hit and away_hit:
            return g
    return None
