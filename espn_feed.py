"""ESPN scoreboard poller that keeps ESPN_GAMES up to date.

Runs as an asyncio background task. Every POLL_INTERVAL seconds it
fetches each configured league's ESPN scoreboard in parallel, filters
to games currently in progress (status.type.state == "in"), and
rebuilds the ESPN_GAMES list. main.py matches Kalshi live events
against this list by checking whether both team name phrases appear
in the Kalshi event title.

ESPN's public scoreboard API is unauthenticated and free, but it's
unofficial — if the shape changes, this module logs and keeps going.
"""
import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import httpx
except ImportError:
    httpx = None

log = logging.getLogger("espn_feed")

# (slug, league label, OddsIQ sport name)
# Sport names must match the Kalshi-derived `_sport` values so the
# Kalshi→ESPN matcher can filter by sport first.
LEAGUES = [
    ("basketball/nba", "NBA", "Basketball"),
    ("basketball/wnba", "WNBA", "Basketball"),
    ("basketball/mens-college-basketball", "NCAAM", "Basketball"),
    ("football/nfl", "NFL", "Football"),
    ("football/college-football", "NCAAF", "Football"),
    ("baseball/mlb", "MLB", "Baseball"),
    ("hockey/nhl", "NHL", "Hockey"),
    ("soccer/eng.1", "EPL", "Soccer"),
    ("soccer/esp.1", "La Liga", "Soccer"),
    ("soccer/ita.1", "Serie A", "Soccer"),
    ("soccer/ger.1", "Bundesliga", "Soccer"),
    ("soccer/fra.1", "Ligue 1", "Soccer"),
    ("soccer/ned.1", "Eredivisie", "Soccer"),
    ("soccer/por.1", "Liga Portugal", "Soccer"),
    ("soccer/uefa.champions", "UCL", "Soccer"),
    ("soccer/uefa.europa", "UEL", "Soccer"),
    ("soccer/uefa.europa.conf", "UECL", "Soccer"),
    ("soccer/usa.1", "MLS", "Soccer"),
    ("soccer/mex.1", "Liga MX", "Soccer"),
    ("soccer/bra.1", "Brasileiro", "Soccer"),
    ("soccer/arg.1", "Argentina Primera", "Soccer"),
    ("soccer/conmebol.libertadores", "Libertadores", "Soccer"),
    ("soccer/conmebol.sudamericana", "Sudamericana", "Soccer"),
    ("soccer/concacaf.champions", "CONCACAF", "Soccer"),
]

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/{slug}/scoreboard"
POLL_INTERVAL = 10  # seconds between full refresh cycles

ESPN_GAMES: List[Dict[str, Any]] = []

# Previous observation per game, used to tell whether the clock is
# actually advancing between fetches. Games whose clock hasn't moved
# are flagged clock_running=False so the frontend freezes them instead
# of interpolating forward during timeouts, fouls, commercial breaks,
# etc. Key is (sport, home_display, away_display).
_PREV_OBS: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

STATUS = {
    "running": False,
    "last_fetch_ts": None,
    "leagues_ok": 0,
    "leagues_err": 0,
    "games": 0,
    "last_error": None,
}


def _parse_clock_secs(s: Optional[str]) -> Optional[int]:
    """Turn an ESPN clock string ('8:47' or '38'') into total seconds."""
    if not s:
        return None
    s = str(s).strip()
    m = re.match(r"^(\d+):(\d{1,2})$", s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r"^(\d+)'?$", s)
    if m:
        return int(m.group(1)) * 60
    return None


def _annotate_clock_running(games: List[Dict[str, Any]]):
    """Compare each game's clock to the previous observation and set
    clock_running=True iff the clock actually advanced. Games whose
    period changed get a reset (assumed running until next fetch)."""
    global _PREV_OBS
    next_obs: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for g in games:
        key = (
            g.get("sport", ""),
            g.get("home_display", ""),
            g.get("away_display", ""),
        )
        clock_str = (g.get("display_clock") or "").strip()
        period = g.get("period", 0)
        # Default: assume running. The first observation of any game
        # gets this optimistic default so the clock ticks immediately;
        # subsequent observations correct it if the clock is stuck.
        running = True
        prev = _PREV_OBS.get(key)
        if prev and prev.get("period") == period:
            prev_secs = _parse_clock_secs(prev.get("display_clock"))
            new_secs = _parse_clock_secs(clock_str)
            if prev_secs is not None and new_secs is not None:
                diff = new_secs - prev_secs
                if g.get("sport") == "Soccer":
                    running = diff > 0
                else:
                    running = diff < 0
        g["clock_running"] = running
        next_obs[key] = {
            "display_clock": clock_str,
            "period": period,
            "captured_at_ms": g.get("captured_at_ms", 0),
        }
    _PREV_OBS = next_obs


def _team_phrases(team: Dict[str, Any]) -> List[str]:
    """Lowercased search phrases for matching a team against Kalshi
    titles. Skips anything shorter than 3 chars to avoid abbreviation
    false positives."""
    phrases = set()
    for key in ("displayName", "shortDisplayName", "nickname", "location", "name"):
        v = team.get(key)
        if v:
            s = str(v).strip().lower()
            if len(s) >= 3:
                phrases.add(s)
    # Sort longest first so matching prefers specific names.
    return sorted(phrases, key=lambda s: -len(s))


def _parse_event(ev: Dict[str, Any], league: str, sport: str) -> Optional[Dict[str, Any]]:
    status = ev.get("status") or {}
    stype = status.get("type") or {}
    state = stype.get("state", "")
    if state != "in":
        return None
    comps = ev.get("competitions") or [{}]
    comp = comps[0] if comps else {}
    competitors = comp.get("competitors") or []
    if len(competitors) < 2:
        return None
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not home or not away:
        return None
    home_team = home.get("team") or {}
    away_team = away.get("team") or {}
    return {
        "sport": sport,
        "league": league,
        "home_phrases": _team_phrases(home_team),
        "away_phrases": _team_phrases(away_team),
        "home_display": home_team.get("displayName", ""),
        "away_display": away_team.get("displayName", ""),
        "state": state,
        "display_clock": status.get("displayClock", ""),
        "period": status.get("period", 0),
        "short_detail": stype.get("shortDetail", ""),
        "detail": stype.get("detail", ""),
        "description": stype.get("description", ""),
    }


async def _fetch_league(client, slug: str, league: str, sport: str):
    try:
        url = ESPN_BASE.format(slug=slug)
        r = await client.get(url, timeout=15.0)
        if r.status_code != 200:
            log.debug("espn %s: HTTP %d", slug, r.status_code)
            return None
        data = r.json()
        now_ms = int(time.time() * 1000)
        out = []
        for ev in data.get("events", []):
            parsed = _parse_event(ev, league, sport)
            if parsed:
                parsed["captured_at_ms"] = now_ms
                out.append(parsed)
        return out
    except Exception as e:
        log.debug("espn %s err: %s", slug, e)
        return None


async def run_espn_feed():
    global ESPN_GAMES
    if httpx is None:
        log.warning("httpx not installed — ESPN feed disabled")
        return
    STATUS["running"] = True
    headers = {"User-Agent": "oddsiq/1.0"}
    async with httpx.AsyncClient(headers=headers) as client:
        while True:
            try:
                tasks = [
                    _fetch_league(client, slug, league, sport)
                    for slug, league, sport in LEAGUES
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                all_games = []
                ok = err = 0
                for res in results:
                    if isinstance(res, Exception) or res is None:
                        err += 1
                    else:
                        ok += 1
                        all_games.extend(res)
                _annotate_clock_running(all_games)
                ESPN_GAMES = all_games
                STATUS["last_fetch_ts"] = time.time()
                STATUS["leagues_ok"] = ok
                STATUS["leagues_err"] = err
                STATUS["games"] = len(all_games)
                STATUS["last_error"] = None
                log.info("espn: %d live games, %d leagues ok, %d err",
                         len(all_games), ok, err)
            except Exception as e:
                STATUS["last_error"] = f"{type(e).__name__}: {e}"
                log.error("espn poll error: %s", e)
            await asyncio.sleep(POLL_INTERVAL)


def match_game(title: str, sport: str) -> Optional[Dict[str, Any]]:
    """Return the first ESPN live game whose home and away team phrases
    both appear in the Kalshi event title (case-insensitive), or None.
    """
    if not title or not sport:
        return None
    t = title.lower()
    for g in ESPN_GAMES:
        if g.get("sport") != sport:
            continue
        home_hit = any(p and p in t for p in g.get("home_phrases", []))
        away_hit = any(p and p in t for p in g.get("away_phrases", []))
        if home_hit and away_hit:
            return g
    return None


def compact_label(g: Dict[str, Any]) -> Optional[str]:
    """Build a short live-badge label like '1st 38\\'' or 'Q2 8:47'
    from an ESPN game entry. Returns None if nothing useful is known."""
    if not g:
        return None
    sport = g.get("sport", "")
    clock = (g.get("display_clock") or "").strip()
    period = g.get("period", 0)
    short = (g.get("short_detail") or "").strip()
    low = short.lower()

    if sport == "Soccer":
        # ESPN soccer shortDetail examples:
        #   "38' - 1st Half", "HT", "62' - 2nd Half", "FT"
        if "halftime" in low or low == "ht":
            return "HT"
        if period == 1 and clock:
            return f"1st {clock}"
        if period == 2 and clock:
            return f"2nd {clock}"
        return clock or short or "LIVE"
    if sport == "Basketball":
        if period and clock:
            return f"Q{period} {clock}"
        if period:
            return f"Q{period}"
    if sport == "Football":
        if period and clock:
            return f"Q{period} {clock}"
        if period:
            return f"Q{period}"
    if sport == "Hockey":
        if period and clock:
            return f"P{period} {clock}"
        if period:
            return f"P{period}"
    if sport == "Baseball":
        # Baseball shortDetail is already compact: "Top 3rd", "Bot 7th"
        return short or "LIVE"
    return short or clock or "LIVE"
