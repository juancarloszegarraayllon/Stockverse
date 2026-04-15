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
from itertools import product
from typing import Any, Dict, Iterable, List, Optional

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
POLL_INTERVAL = 30         # normal cycle (seconds)
MAX_POLL_INTERVAL = 600    # hard cap when backing off (10 minutes)
BACKOFF_THRESHOLD = 0.5    # back off when ≥50% of sports 403 / error

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
    # Circuit breaker state — tells us whether the feed is currently
    # healthy or sitting in back-off after Cloudflare / Varnish hit us.
    "current_interval": POLL_INTERVAL,
    "backoff_active": False,
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


def _normalize(s: Any) -> str:
    if not s:
        return ""
    return _nfd_strip(str(s).lower())


def _phrase_variants(raw: Any) -> Iterable[str]:
    """Yield all normalized phrase variants of a team name, covering
    every 2^N combination of stripped vs digraph-expanded Nordic /
    German characters. Names without any special chars yield exactly
    one variant (plain NFD fold)."""
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
            # else: leave the char; NFD will strip it
        norm = _nfd_strip(v).strip()
        if norm and norm not in seen:
            seen.add(norm)
            yield norm


def _team_phrases(team: Dict[str, Any]) -> List[str]:
    """Build accent-stripped + digraph-expanded phrases from a
    SofaScore team dict. Longest phrases come first."""
    phrases = set()
    # Full-name variants from each name-ish field.
    for key in ("name", "shortName", "fullName"):
        v = team.get(key)
        if v:
            for variant in _phrase_variants(v):
                if len(variant) >= 3:
                    phrases.add(variant)
    slug = team.get("slug")
    if slug:
        for variant in _phrase_variants(slug.replace("-", " ")):
            if len(variant) >= 3:
                phrases.add(variant)
    # Word-level phrases: last 1-2 words and first word, computed from
    # each full-name variant so Västerås also produces "vasteras" and
    # "vasteraas" as single-word phrases.
    for src in (team.get("name"), team.get("shortName"), team.get("fullName")):
        if not src:
            continue
        for variant in _phrase_variants(src):
            words = variant.split()
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

    # SofaScore's startTimestamp is the scheduled kickoff in Unix
    # seconds. Store as epoch ms for main.py to use as an
    # authoritative kickoff override.
    start_ts = ev.get("startTimestamp")
    sched_ms = int(start_ts * 1000) if isinstance(start_ts, (int, float)) else None

    g = {
        "sport": sport_label,
        "league": league,
        "scheduled_kickoff_ms": sched_ms,
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
                    # Soccer clock runs continuously, so let the
                    # frontend tick loop interpolate it.
                    g["clock_running"] = True
        except Exception:
            pass
        # Two-leg knockout ties (Champions League round-of-16 +,
        # Copa Libertadores, domestic 2-leg cup ties, etc.).
        # SofaScore surfaces the running aggregate under
        # homeScore.aggregated / awayScore.aggregated (or at the
        # event level) once the first leg has been played. The
        # roundInfo.name usually contains "2nd leg" or "1st leg".
        try:
            agg_h = home_score_obj.get("aggregated")
            agg_a = away_score_obj.get("aggregated")
            if agg_h is None and agg_a is None:
                ascore = ev.get("aggregatedScore") or {}
                agg_h = ascore.get("home")
                agg_a = ascore.get("away")
            round_info = ev.get("roundInfo") or {}
            round_name = (round_info.get("name") or "").strip()
            # Detect leg number from the round name. SofaScore uses
            # "1st leg" / "2nd leg" literally. Fall back to None.
            leg_number = None
            rn_low = round_name.lower()
            if "2nd leg" in rn_low or "second leg" in rn_low:
                leg_number = 2
            elif "1st leg" in rn_low or "first leg" in rn_low:
                leg_number = 1
            if (agg_h is not None and agg_a is not None) or leg_number:
                g["is_two_leg"] = True
                g["aggregate_home"] = int(agg_h) if isinstance(agg_h, (int, float)) else None
                g["aggregate_away"] = int(agg_a) if isinstance(agg_a, (int, float)) else None
                g["leg_number"] = leg_number
                g["round_name"] = round_name
                g["tournament_name"] = league
                winner_code = ev.get("aggregatedWinnerCode") or ""
                g["aggregate_winner"] = winner_code  # "home" / "away" / "draw" / ""
        except Exception:
            pass
    # Tennis: build a full scoreboard-style label showing every
    # completed set plus the current set games plus current game
    # points. SofaScore's tennis score dict looks like:
    #   {"current": 1, "period1": 6, "period2": 3, "point": "30"}
    # A full 3-set match at "second set, Alcaraz serving 30-0"
    # would yield a label like "6-3 4-5 30-0" where the first pair
    # is the completed first set.
    if sport_label == "Tennis":
        try:
            set_scores = []
            for i in range(1, 6):
                hp = home_score_obj.get(f"period{i}")
                ap = away_score_obj.get(f"period{i}")
                if hp is None and ap is None:
                    continue
                set_scores.append((
                    int(hp) if hp is not None else 0,
                    int(ap) if ap is not None else 0,
                    i,
                ))
            if set_scores:
                _, _, cur_n = set_scores[-1]
                g["period"] = cur_n
                # Each set rendered as "h-a", space-separated.
                set_strs = [f"{h}-{a}" for (h, a, _) in set_scores]
                label = " ".join(set_strs)
                # Current game points (0/15/30/40/Ad).
                hp_pt = home_score_obj.get("point")
                ap_pt = away_score_obj.get("point")
                if hp_pt not in (None, "") or ap_pt not in (None, ""):
                    label += f" {hp_pt or '0'}-{ap_pt or '0'}"
                g["short_detail"] = label
            # Structured tennis data for the frontend's two-row
            # scoreboard. Sets won come from homeScore.current /
            # awayScore.current, current set games from the last
            # periodN pair, and current game points from .point.
            g["tennis_home_name"] = home_name
            g["tennis_away_name"] = away_name
            g["tennis_home_sets"] = (
                "" if home_score is None else str(home_score)
            )
            g["tennis_away_sets"] = (
                "" if away_score is None else str(away_score)
            )
            if set_scores:
                cur_h_games, cur_a_games, _ = set_scores[-1]
                g["tennis_home_games"] = str(cur_h_games)
                g["tennis_away_games"] = str(cur_a_games)
                # Full per-set history for clients that want it.
                g["tennis_set_history"] = [
                    {"set": n, "home": h, "away": a}
                    for (h, a, n) in set_scores
                ]
            else:
                g["tennis_home_games"] = "0"
                g["tennis_away_games"] = "0"
                g["tennis_set_history"] = []
            hp_pt2 = home_score_obj.get("point")
            ap_pt2 = away_score_obj.get("point")
            g["tennis_home_point"] = "" if hp_pt2 in (None,) else str(hp_pt2)
            g["tennis_away_point"] = "" if ap_pt2 in (None,) else str(ap_pt2)
            # Serving indicator: SofaScore exposes this via several
            # possible fields depending on the endpoint version.
            g["tennis_server"] = ""
            # Try direct "serving" field (string: "home" / "away")
            serving = ev.get("serving")
            if serving in ("home", "away"):
                g["tennis_server"] = serving
            # Try boolean field
            elif ev.get("homeTeamServing") is True:
                g["tennis_server"] = "home"
            elif ev.get("homeTeamServing") is False:
                g["tennis_server"] = "away"
            # Some tennis responses include server flag inside score objs
            elif home_score_obj.get("serve") is True or home_score_obj.get("server") is True:
                g["tennis_server"] = "home"
            elif away_score_obj.get("serve") is True or away_score_obj.get("server") is True:
                g["tennis_server"] = "away"
            # Try firstToServe (1=home, 2=away) + game parity
            elif ev.get("firstToServe") in (1, 2):
                fts = ev["firstToServe"]
                # Total games played in current set determines who
                # serves (alternates each game).
                total_games = 0
                if set_scores:
                    last = set_scores[-1]
                    total_games = (last.get("home") or 0) + (last.get("away") or 0)
                # Even total games → first server; odd → other
                if total_games % 2 == 0:
                    g["tennis_server"] = "home" if fts == 1 else "away"
                else:
                    g["tennis_server"] = "away" if fts == 1 else "home"
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
        bad = 0
        for ev in events:
            try:
                parsed = _parse_event(ev, label)
                if parsed:
                    parsed["captured_at_ms"] = now_ms
                    out.append(parsed)
            except Exception as e:
                bad += 1
                if bad == 1:
                    log.debug("sofascore %s parse err: %s", slug, e)
                continue
        info["count"] = len(out)
        if bad:
            info["bad"] = bad
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
    # SofaScore's API is behind Cloudflare, which 403s any request
    # that doesn't look like a real browser tab. Send the full set
    # of headers the sofascore.com web UI sends: real Chrome UA,
    # Accept-Language, Accept-Encoding, Referer + Origin pinned to
    # the site, plus the Sec-Fetch-* and Sec-Ch-Ua-* hints modern
    # Chromium includes automatically.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-Ch-Ua": '"Chromium";v="125", "Not.A/Brand";v="24", "Google Chrome";v="125"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "DNT": "1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        current_interval = POLL_INTERVAL
        while True:
            failure = False
            try:
                tasks = [
                    _fetch_sport(client, slug, label)
                    for slug, label in SPORTS
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                all_games: List[Dict[str, Any]] = []
                sport_counts: Dict[str, int] = {}
                per_sport: Dict[str, Dict[str, Any]] = {}
                error_count = 0
                for (slug, label), res in zip(SPORTS, results):
                    if isinstance(res, Exception):
                        per_sport[slug] = {"status": f"err:{type(res).__name__}", "count": None}
                        error_count += 1
                        continue
                    if res is None:
                        per_sport[slug] = {"status": "none", "count": None}
                        error_count += 1
                        continue
                    games, info = res
                    per_sport[slug] = info
                    # Any non-200 response counts as a failure for
                    # the circuit breaker, since Cloudflare/Varnish
                    # returns 403 for everything when it's blocking.
                    if isinstance(info.get("status"), int) and info["status"] != 200:
                        error_count += 1
                    if games:
                        all_games.extend(games)
                        sport_counts[label] = sport_counts.get(label, 0) + len(games)
                SOFASCORE_GAMES = all_games
                STATUS["last_fetch_ts"] = time.time()
                STATUS["games"] = len(all_games)
                STATUS["sports"] = sport_counts
                STATUS["per_sport"] = per_sport
                STATUS["last_error"] = None
                # Trip the breaker when more than half the sports
                # fail to return a 200 — that's the shape of a
                # Cloudflare/Varnish hard block, not a quiet night.
                failure = error_count >= max(1, int(len(SPORTS) * BACKOFF_THRESHOLD))
                log.info(
                    "sofascore: %d live games / %d sports ok / %d errors%s",
                    len(all_games), len(sport_counts), error_count,
                    " (backing off)" if failure else "",
                )
            except Exception as e:
                STATUS["last_error"] = f"{type(e).__name__}: {e}"
                log.error("sofascore poll error: %s", e)
                failure = True
            # Exponential backoff on repeated failure, instant reset
            # on first healthy cycle.
            if failure:
                current_interval = min(current_interval * 2, MAX_POLL_INTERVAL)
                STATUS["backoff_active"] = True
            else:
                current_interval = POLL_INTERVAL
                STATUS["backoff_active"] = False
            STATUS["current_interval"] = current_interval
            await asyncio.sleep(current_interval)


def _phrase_in_title(phrase: str, normalized_title: str) -> bool:
    if not phrase:
        return False
    pattern = r"(?<!\w)" + re.escape(phrase) + r"(?!\w)"
    return re.search(pattern, normalized_title) is not None


def match_game(title: str, sport: str) -> Optional[Dict[str, Any]]:
    """Return the SofaScore game whose team phrases best match the
    Kalshi title. Score = len(longest home phrase hit) + len(longest
    away phrase hit). Picking the max score disambiguates ambiguous
    cases; also rejects games where both sides fell back to the
    same shared word (e.g. Leicester City / Swansea City both hit
    "city" when the Kalshi title is "Montevideo City vs Albion")."""
    if not title or not sport:
        return None
    t = _normalize(title)
    best = None
    best_score = 0
    for g in SOFASCORE_GAMES:
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
        # Reject matches where both sides picked the same phrase —
        # that's the fingerprint of a shared-word false positive.
        if home_phrase == away_phrase:
            continue
        score = home_best + away_best
        if score > best_score:
            best_score = score
            best = g
    return best
