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
                    # Soccer clock runs continuously, so let the
                    # frontend tick loop interpolate it.
                    g["clock_running"] = True
        except Exception:
            pass
    # Tennis: the plain "1st set" description isn't very useful, so
    # build a richer label from the per-set game counts and the
    # current game points. SofaScore's tennis score dict looks like:
    #   {"current": 1, "period1": 6, "period2": 3, "point": "30"}
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
                cur_h, cur_a, cur_n = set_scores[-1]
                g["period"] = cur_n
                label = f"Set {cur_n} {cur_h}-{cur_a}"
                hp_pt = home_score_obj.get("point")
                ap_pt = away_score_obj.get("point")
                if hp_pt not in (None, "") or ap_pt not in (None, ""):
                    label += f" {hp_pt or '0'}-{ap_pt or '0'}"
                g["short_detail"] = label
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
