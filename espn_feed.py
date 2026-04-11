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
import unicodedata
from itertools import product
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
    ("baseball/college-baseball", "NCAAB", "Baseball"),
    ("hockey/nhl", "NHL", "Hockey"),
    # Top European leagues
    ("soccer/eng.1", "EPL", "Soccer"),
    ("soccer/eng.2", "Championship", "Soccer"),
    ("soccer/esp.1", "La Liga", "Soccer"),
    ("soccer/esp.2", "La Liga 2", "Soccer"),
    ("soccer/ita.1", "Serie A", "Soccer"),
    ("soccer/ita.2", "Serie B", "Soccer"),
    ("soccer/ger.1", "Bundesliga", "Soccer"),
    ("soccer/ger.2", "Bundesliga 2", "Soccer"),
    ("soccer/fra.1", "Ligue 1", "Soccer"),
    ("soccer/fra.2", "Ligue 2", "Soccer"),
    ("soccer/ned.1", "Eredivisie", "Soccer"),
    ("soccer/por.1", "Liga Portugal", "Soccer"),
    ("soccer/sco.1", "Scottish Premiership", "Soccer"),
    ("soccer/bel.1", "Belgian Pro", "Soccer"),
    ("soccer/tur.1", "Super Lig", "Soccer"),
    ("soccer/gre.1", "Greek Super League", "Soccer"),
    ("soccer/den.1", "Danish Superliga", "Soccer"),
    ("soccer/swe.1", "Allsvenskan", "Soccer"),
    ("soccer/nor.1", "Eliteserien", "Soccer"),
    ("soccer/sui.1", "Swiss Super League", "Soccer"),
    ("soccer/aut.1", "Austrian Bundesliga", "Soccer"),
    ("soccer/cze.1", "Czech Liga", "Soccer"),
    ("soccer/rus.1", "Russian Premier", "Soccer"),
    # European competitions
    ("soccer/uefa.champions", "UCL", "Soccer"),
    ("soccer/uefa.europa", "UEL", "Soccer"),
    ("soccer/uefa.europa.conf", "UECL", "Soccer"),
    ("soccer/uefa.nations", "UEFA Nations", "Soccer"),
    # National cups
    ("soccer/eng.fa", "FA Cup", "Soccer"),
    ("soccer/eng.league_cup", "EFL Cup", "Soccer"),
    ("soccer/esp.copa_del_rey", "Copa del Rey", "Soccer"),
    ("soccer/ita.coppa_italia", "Coppa Italia", "Soccer"),
    ("soccer/ger.dfb_pokal", "DFB Pokal", "Soccer"),
    ("soccer/fra.coupe_de_france", "Coupe de France", "Soccer"),
    # Americas
    ("soccer/usa.1", "MLS", "Soccer"),
    ("soccer/usa.nwsl", "NWSL", "Soccer"),
    ("soccer/mex.1", "Liga MX", "Soccer"),
    ("soccer/bra.1", "Brasileiro", "Soccer"),
    ("soccer/bra.2", "Brasileiro Serie B", "Soccer"),
    ("soccer/arg.1", "Argentina Primera", "Soccer"),
    ("soccer/col.1", "Colombia Primera A", "Soccer"),
    ("soccer/chi.1", "Chile Primera", "Soccer"),
    ("soccer/uru.1", "Uruguay Primera", "Soccer"),
    ("soccer/ecu.1", "Ecuador LigaPro", "Soccer"),
    ("soccer/ven.1", "Venezuela Primera", "Soccer"),
    ("soccer/per.1", "Peru Primera", "Soccer"),
    ("soccer/par.1", "Paraguay Primera", "Soccer"),
    ("soccer/conmebol.libertadores", "Libertadores", "Soccer"),
    ("soccer/conmebol.sudamericana", "Sudamericana", "Soccer"),
    ("soccer/concacaf.champions", "CONCACAF", "Soccer"),
    ("soccer/concacaf.leagues.cup", "Leagues Cup", "Soccer"),
    # Asia / Oceania
    ("soccer/jpn.1", "J1 League", "Soccer"),
    ("soccer/chn.1", "Chinese Super League", "Soccer"),
    ("soccer/aus.1", "A-League", "Soccer"),
    ("soccer/ksa.1", "Saudi Pro League", "Soccer"),
    ("soccer/afc.champions", "AFC Champions", "Soccer"),
    ("soccer/afc.asian.cup", "AFC Asian Cup", "Soccer"),
    # International
    ("soccer/fifa.world", "World Cup", "Soccer"),
    ("soccer/fifa.worldq.uefa", "WC Qualifiers UEFA", "Soccer"),
    ("soccer/fifa.worldq.concacaf", "WC Qualifiers CONCACAF", "Soccer"),
    ("soccer/fifa.worldq.conmebol", "WC Qualifiers CONMEBOL", "Soccer"),
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
    # Per-league breakdown: slug -> {ok: bool, games: int, live: int}
    "leagues": {},
}


# Nordic / German character → ASCII digraph expansion. Applied BEFORE
# NFD stripping so we can emit both "vasteras" and "vasteraas" as
# phrase variants for names like "Västerås". Only chars whose strip
# form differs from their digraph form live here; æ/ß/ı/ð/þ are
# handled identically in _STRIP_MAP so they don't need variants.
_DIGRAPH_MAP = {
    "å": "aa", "ä": "ae", "ö": "oe", "ü": "ue", "ø": "oe",
}

# Precomposed characters that NFD doesn't decompose. Translated before
# NFD so the "stripped" variant is guaranteed plain ASCII — covers
# Kalshi titles that drop Nordic chars entirely ("Brondby" for
# "Brøndby") as well as German ß and Turkish dotless i.
_STRIP_MAP = str.maketrans({
    "ø": "o", "Ø": "O",
    "æ": "ae", "Æ": "Ae",
    "ß": "ss",
    "ı": "i", "İ": "I",
    "ð": "d", "Ð": "D",
    "þ": "th", "Þ": "Th",
})


def _nfd_strip(s: str) -> str:
    s = s.translate(_STRIP_MAP)
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _normalize(s: str) -> str:
    """Lowercase + strip accents, for case/accent-insensitive matching."""
    if not s:
        return ""
    return _nfd_strip(str(s).lower())


def _phrase_variants(raw: Any) -> Iterable[str]:
    """Yield all normalized phrase variants for a team name, covering
    every 2^N combination of stripped vs digraph-expanded Nordic /
    German characters. Plain names yield a single variant."""
    if not raw:
        return
    low = str(raw).lower()
    specials = [ch for ch in _DIGRAPH_MAP if ch in low]
    if not specials:
        result = _nfd_strip(low).strip()
        if result:
            yield result
        return
    seen = set()
    for mask in product((False, True), repeat=len(specials)):
        v = low
        for i, ch in enumerate(specials):
            if mask[i]:
                v = v.replace(ch, _DIGRAPH_MAP[ch])
        norm = _nfd_strip(v).strip()
        if norm and norm not in seen:
            seen.add(norm)
            yield norm


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
        # Finished games don't have a running clock — nothing to annotate.
        if g.get("state") != "in":
            g["clock_running"] = False
            continue
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
    """Lowercased, accent-stripped, digraph-expanded phrases for
    matching a team against Kalshi titles. Skips anything shorter
    than 3 chars to avoid abbreviation false positives."""
    phrases = set()
    for key in ("displayName", "shortDisplayName", "nickname", "location", "name"):
        v = team.get(key)
        if v:
            for variant in _phrase_variants(v):
                if len(variant) >= 3:
                    phrases.add(variant)
    # Sort longest first so matching prefers specific names.
    return sorted(phrases, key=lambda s: -len(s))


def _parse_event(ev: Dict[str, Any], league: str, sport: str) -> Optional[Dict[str, Any]]:
    status = ev.get("status") or {}
    stype = status.get("type") or {}
    state = stype.get("state", "")
    # Track both in-progress and completed games. "pre" (not started)
    # is skipped because we have nothing useful to display for it.
    if state not in ("in", "post"):
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
        "home_abbr": home_team.get("abbreviation", ""),
        "away_abbr": away_team.get("abbreviation", ""),
        "home_score": str(home.get("score", "")),
        "away_score": str(away.get("score", "")),
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
                per_league: Dict[str, Dict[str, Any]] = {}
                for (slug, league, sport), res in zip(LEAGUES, results):
                    if isinstance(res, Exception) or res is None:
                        err += 1
                        per_league[slug] = {"league": league, "ok": False, "games": 0}
                    else:
                        ok += 1
                        all_games.extend(res)
                        per_league[slug] = {
                            "league": league,
                            "ok": True,
                            "games": len(res),
                        }
                STATUS["leagues"] = per_league
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


def _phrase_in_title(phrase: str, normalized_title: str) -> bool:
    """True if `phrase` appears in the already-normalized title as a
    whole-word match (not a substring). Prevents false positives like
    'mito' matching 'atromitos'."""
    if not phrase:
        return False
    pattern = r"(?<!\w)" + re.escape(phrase) + r"(?!\w)"
    return re.search(pattern, normalized_title) is not None


def match_game(title: str, sport: str) -> Optional[Dict[str, Any]]:
    """Return the ESPN game whose home and away team phrases best
    match the Kalshi title. Score = len(longest matching home
    phrase) + len(longest matching away phrase). Also rejects games
    where both sides collapsed to the same phrase (shared-word false
    positive like "city" matching Leicester City and Swansea City)."""
    if not title or not sport:
        return None
    t = _normalize(title)
    best = None
    best_score = 0
    for g in ESPN_GAMES:
        if g.get("sport") != sport:
            continue
        home_best = 0
        home_phrase = ""
        for p in g.get("home_phrases", []):
            if _phrase_in_title(p, t) and len(p) > home_best:
                home_best = len(p)
                home_phrase = p
        if home_best == 0:
            continue
        away_best = 0
        away_phrase = ""
        for p in g.get("away_phrases", []):
            if _phrase_in_title(p, t) and len(p) > away_best:
                away_best = len(p)
                away_phrase = p
        if away_best == 0:
            continue
        if home_phrase == away_phrase:
            continue
        score = home_best + away_best
        if score > best_score:
            best_score = score
            best = g
    return best


def compact_label(g: Dict[str, Any]) -> Optional[str]:
    """Build a short live-badge label like '1st 38\\'' or 'Q2 8:47'
    from an ESPN game entry. Returns None if nothing useful is known."""
    if not g:
        return None
    if g.get("state") == "post":
        return "FINAL"
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
    if sport == "Tennis":
        # SofaScore feed builds "Set 2 3-4 30-0" into short_detail;
        # ESPN's tennis detail is similarly already compact.
        return short or "LIVE"
    return short or clock or "LIVE"
