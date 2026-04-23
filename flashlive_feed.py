"""FlashLive Sports feed — live scores via RapidAPI.

Replaces the unreliable SofaScore scraper with a proper API-key
authenticated service. Covers 30+ sports with real-time scores,
game clock, and game state.

Runs as an asyncio background task. Every POLL_INTERVAL seconds
it fetches live events, parses scores, and stores them in the
module-level GAMES dict keyed by a normalized team-name key.

main.py's match_game() function queries GAMES to overlay live
scores on Kalshi event cards.
"""
import asyncio
import logging
import os
import time

try:
    import httpx
except ImportError:
    httpx = None

log = logging.getLogger("flashlive")

API_KEY = os.environ.get("FLASHLIVE_API_KEY", "").strip()
API_HOST = "flashlive-sports.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}"

POLL_INTERVAL = 60  # seconds between polls (conserve API quota)

# Only poll sports that Kalshi actively covers to save requests.
# Each sport = 1 API call per poll cycle.
ACTIVE_SPORTS = {
    "1": "Soccer",
    "2": "Tennis",
    "3": "Basketball",
    "4": "Hockey",
    "6": "Baseball",
}
GAMES: dict = {}    # normalized key → game dict

STATUS = {
    "running": False,
    "last_fetch_ts": None,
    "games": 0,
    "last_error": None,
    "polls": 0,
}

# FlashLive sport IDs → our sport names
SPORT_MAP = {
    "1": "Soccer",
    "2": "Tennis",
    "3": "Basketball",
    "4": "Hockey",
    "5": "Football",
    "6": "Baseball",
    "7": "Rugby",
    "8": "Cricket",
    "9": "Golf",
    "10": "MMA",
    "11": "Motorsport",
    "12": "Esports",
    "22": "Darts",
}


def _normalize(s: str) -> str:
    """Normalize a team/player name for matching."""
    import unicodedata
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s).lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    for rm in (" fc", " sc", " cf", " afc", " united", " city"):
        s = s.replace(rm, "")
    return s.strip()


def match_game(title: str, sport: str = ""):
    """Find a FlashLive game matching a Kalshi event title.
    Returns a game dict or None."""
    if not GAMES or not title:
        return None
    norm_title = _normalize(title)
    best = None
    best_score = 0
    for key, g in GAMES.items():
        if sport and g.get("sport") != sport:
            continue
        home_phrases = g.get("home_phrases", [])
        away_phrases = g.get("away_phrases", [])
        score = 0
        for phrase in home_phrases:
            if phrase and phrase in norm_title:
                score += len(phrase)
        for phrase in away_phrases:
            if phrase and phrase in norm_title:
                score += len(phrase)
        if score > best_score:
            best_score = score
            best = g
    return best if best_score >= 4 else None


def compact_label(g: dict) -> str:
    """Build a short label like 'BOS 3 - NYR 2'."""
    if not g:
        return ""
    ha = g.get("home_abbr") or g.get("home_name", "")[:3].upper()
    aa = g.get("away_abbr") or g.get("away_name", "")[:3].upper()
    hs = g.get("home_score", "")
    as_ = g.get("away_score", "")
    if hs == "" and as_ == "":
        return ""
    return f"{ha} {hs} - {aa} {as_}"


async def _fetch_live_events():
    """Fetch today's events from FlashLive for all sports we track."""
    if not API_KEY or httpx is None:
        return []
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST,
    }
    all_events = []
    raw_samples = []
    errors = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        for sport_id, sport_name in ACTIVE_SPORTS.items():
            try:
                r = await client.get(
                    f"{BASE_URL}/v1/events/list",
                    headers=headers,
                    params={
                        "sport_id": sport_id,
                        "indent_days": "0",
                        "timezone": "-4",
                        "locale": "en_INT",
                    },
                )
                if r.status_code == 200:
                    data = r.json()
                    top_data = data.get("DATA", []) if isinstance(data, dict) else data
                    # FlashLive nests events inside tournament groups.
                    # Each item in DATA can be a tournament header or
                    # an event. Events have an EVENT_ID field.
                    for item in (top_data if isinstance(top_data, list) else []):
                        if isinstance(item, dict):
                            if item.get("EVENT_ID"):
                                # Direct event
                                item["_sport"] = sport_name
                                all_events.append(item)
                            # Check for nested events
                            for k in ("EVENTS", "events", "ITEMS", "items"):
                                nested = item.get(k)
                                if isinstance(nested, list):
                                    for ev in nested:
                                        if isinstance(ev, dict):
                                            ev["_sport"] = sport_name
                                            ev["_league"] = item.get("SHORT_NAME") or item.get("NAME_PART_2") or ""
                                            ev["_country"] = item.get("COUNTRY_NAME") or ""
                                            all_events.append(ev)
                    # Save raw sample for debugging
                    if len(raw_samples) < 2 and top_data:
                        first = top_data[0] if isinstance(top_data, list) and top_data else {}
                        raw_samples.append({
                            "sport": sport_name,
                            "total_items": len(top_data) if isinstance(top_data, list) else 0,
                            "first_item_keys": list(first.keys())[:20] if isinstance(first, dict) else "?",
                            "first_item_preview": str(first)[:800],
                        })
                else:
                    errors.append(f"{sport_name}: HTTP {r.status_code} - {r.text[:200]}")
            except Exception as e:
                errors.append(f"{sport_name}: {str(e)[:200]}")
    STATUS["last_error"] = errors[0] if errors else ("no events found" if not all_events else None)
    STATUS["all_errors"] = errors[:5]
    STATUS["raw_samples"] = raw_samples
    return all_events


def _parse_event(ev):
    """Parse a FlashLive event into our standard game dict format."""
    try:
        # FlashLive event structure varies — handle common fields
        event_id = ev.get("EVENT_ID") or ev.get("id") or ""
        home_name = ev.get("HOME_NAME") or ev.get("home", {}).get("name", "") or ""
        away_name = ev.get("AWAY_NAME") or ev.get("away", {}).get("name", "") or ""
        home_score = str(ev.get("HOME_SCORE", "") or ev.get("home_score", ""))
        away_score = str(ev.get("AWAY_SCORE", "") or ev.get("away_score", ""))
        sport_id = str(ev.get("SPORT_ID") or ev.get("sport_id") or "")
        sport = SPORT_MAP.get(sport_id, "")

        # Game state
        stage_type = str(ev.get("STAGE_TYPE") or ev.get("stage_type") or "")
        status_type = str(ev.get("STATUS_TYPE") or ev.get("status_type") or "")
        minute = ev.get("STAGE") or ev.get("STAGE_START_TIME") or ""
        period = ev.get("PERIOD") or ""

        if status_type in ("2", "3"):  # live/in-progress
            state = "in"
        elif status_type in ("4", "5", "6"):  # finished
            state = "post"
        else:
            state = "pre"

        # Time display
        display_clock = str(minute) if minute else ""
        if period:
            display_clock = f"{period} {display_clock}".strip()

        # Short detail
        short_detail = display_clock or ("FT" if state == "post" else "")

        # League / tournament
        league = ev.get("TOURNAMENT_NAME") or ev.get("tournament", {}).get("name", "") or ""
        country = ev.get("COUNTRY_NAME") or ""

        # Abbreviations
        home_abbr = (home_name[:3].upper() if home_name else "")
        away_abbr = (away_name[:3].upper() if away_name else "")

        # Scheduled start
        start_ts = ev.get("START_TIME") or ev.get("start_time") or 0
        try:
            start_ms = int(float(start_ts)) * 1000 if start_ts else 0
        except (ValueError, TypeError):
            start_ms = 0

        # Normalized phrases for matching
        home_norm = _normalize(home_name)
        away_norm = _normalize(away_name)
        home_phrases = [home_norm]
        away_phrases = [away_norm]
        # Add short versions for matching
        for w in home_norm.split():
            if len(w) >= 4:
                home_phrases.append(w)
        for w in away_norm.split():
            if len(w) >= 4:
                away_phrases.append(w)

        return {
            "event_id": event_id,
            "sport": sport,
            "league": league,
            "country": country,
            "home_name": home_name,
            "away_name": away_name,
            "home_score": home_score,
            "away_score": away_score,
            "home_abbr": home_abbr,
            "away_abbr": away_abbr,
            "state": state,
            "display_clock": display_clock,
            "short_detail": short_detail,
            "period": period,
            "scheduled_kickoff_ms": start_ms,
            "home_phrases": home_phrases,
            "away_phrases": away_phrases,
            "captured_at_ms": int(time.time() * 1000),
        }
    except Exception as e:
        log.debug("parse error: %s", e)
        return None


async def run_flashlive_feed():
    """Background task: poll FlashLive for live scores."""
    if not API_KEY:
        log.info("FLASHLIVE_API_KEY not set — FlashLive feed disabled")
        return
    if httpx is None:
        log.warning("httpx not installed — FlashLive feed disabled")
        return

    STATUS["running"] = True
    log.info("FlashLive feed starting (poll every %ds)", POLL_INTERVAL)

    while True:
        try:
            events = await _fetch_live_events()
            parsed = 0
            new_games = {}
            for ev in events:
                g = _parse_event(ev)
                if g and g.get("home_name") and g.get("away_name"):
                    key = f"{g['sport']}:{_normalize(g['home_name'])}:{_normalize(g['away_name'])}"
                    new_games[key] = g
                    parsed += 1
            GAMES.clear()
            GAMES.update(new_games)
            STATUS["games"] = len(GAMES)
            STATUS["last_fetch_ts"] = time.time()
            STATUS["polls"] += 1
            if parsed:
                log.info("FlashLive: %d live games across all sports", parsed)
        except Exception as e:
            STATUS["last_error"] = str(e)[:200]
            log.error("FlashLive poll error: %s", e)

        await asyncio.sleep(POLL_INTERVAL)
