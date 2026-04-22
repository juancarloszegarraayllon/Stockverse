from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, Response, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import os, time, tempfile, functools, asyncio, threading, logging, hashlib, json
from datetime import date, timedelta, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO)

# ── Sentry error tracking (optional) ────────────────────────────────
# Enabled automatically when SENTRY_DSN is set in the environment.
# No-op otherwise, so local dev and unconfigured deploys skip it.
# Set SENTRY_TRACES_SAMPLE_RATE (0.0-1.0) to enable performance
# monitoring; defaults to 0 (error-only, free tier friendly).
_SENTRY_DSN = os.environ.get("SENTRY_DSN", "").strip()
if _SENTRY_DSN:
    try:
        import sentry_sdk
        sentry_sdk.init(
            dsn=_SENTRY_DSN,
            traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.0")),
            environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
            # Don't send PII by default. Safe since we don't have
            # user accounts yet.
            send_default_pii=False,
        )
        logging.getLogger("stochverse").info("sentry enabled")
    except Exception as e:
        logging.getLogger("stochverse").warning("sentry init failed: %s", e)

app = FastAPI(title="Stochverse API")


def _all_market_tickers():
    """Return every market ticker from the current REST snapshot, used
    by the Kalshi WebSocket client to know what to subscribe to.
    Includes sibling market tickers from grouped cards (Spread,
    Total, BTTS, To Advance, etc.) so their prices get recorded
    and their charts work on the detail page."""
    records = _cache.get("data") or []
    seen = set()
    out = []
    for r in records:
        for o in r.get("outcomes", []):
            tk = o.get("ticker")
            if tk and tk not in seen:
                seen.add(tk)
                out.append(tk)
        for g in r.get("_market_groups", []) or []:
            for o in g.get("_outcomes", []):
                tk = o.get("ticker")
                if tk and tk not in seen:
                    seen.add(tk)
                    out.append(tk)
    return out


@app.on_event("startup")
async def startup_event():
    global _cache
    _cache = {"data": None, "ts": 0}
    # Initialize database tables (no-op if DATABASE_URL is not set).
    try:
        from db import init_db, refresh_alias_sport_cache
        await init_db()
        # Prime the alias→sport cache so get_data() can classify
        # unknown Kalshi series via entity matches on first run.
        await refresh_alias_sport_cache()
    except Exception as e:
        logging.getLogger("stochverse").warning("db init skipped: %s", e)
    # Build the REST snapshot eagerly in a thread so the WS client has
    # tickers to subscribe to without waiting for a first user request.
    threading.Thread(target=get_data, daemon=True).start()
    # Launch the Kalshi WebSocket client as an asyncio background task.
    try:
        from kalshi_ws import run_ws_client
        asyncio.create_task(run_ws_client(_all_market_tickers))
    except Exception as e:
        logging.getLogger("stochverse").warning("failed to start ws client: %s", e)
    try:
        from espn_feed import run_espn_feed
        asyncio.create_task(run_espn_feed())
    except Exception as e:
        logging.getLogger("stochverse").warning("failed to start espn feed: %s", e)
    try:
        from sportsdb_feed import run_sportsdb_feed
        asyncio.create_task(run_sportsdb_feed())
    except Exception as e:
        logging.getLogger("stochverse").warning("failed to start sportsdb feed: %s", e)
    # SofaScore feed with a built-in exponential-backoff circuit
    # breaker. When Cloudflare / Varnish blocks ≥50% of sports with
    # 403s, the poll interval doubles up to a 10-minute cap until we
    # get a healthy cycle, at which point it resets instantly to
    # POLL_INTERVAL. So while we're blocked we waste at most a few
    # requests per hour, and the moment the block lifts we catch
    # back up on the next 30-second cycle.
    try:
        from sofascore_feed import run_sofascore_feed
        asyncio.create_task(run_sofascore_feed())
    except Exception as e:
        logging.getLogger("stochverse").warning("failed to start sofascore feed: %s", e)
    # Phase 4: periodically flush live scores from all feeds to the DB.
    asyncio.create_task(_score_flush_loop())
    # Phase 5: periodically prune old price rows to stay within
    # Neon free-tier storage limits (512 MB). Runs hourly.
    asyncio.create_task(_price_prune_loop())


@app.on_event("shutdown")
async def shutdown_event():
    """Graceful shutdown. Railway sends SIGTERM during a deploy and
    waits up to 30 s for the process to exit cleanly. We flush any
    pending DB buffers so in-flight WS price updates aren't lost,
    and log so we can confirm clean exits in the Railway logs."""
    log = logging.getLogger("stochverse")
    log.info("shutdown: starting graceful cleanup")
    # Flush any buffered prices to the DB one last time so the
    # final seconds of ticks aren't dropped on deploy.
    try:
        from kalshi_ws import _price_buffer as _pb
        if _pb:
            try:
                from db import batch_insert_prices
                snapshot = list(_pb)
                _pb.clear()
                await batch_insert_prices(snapshot)
                log.info("shutdown: flushed %d buffered prices", len(snapshot))
            except Exception as e:
                log.warning("shutdown: price flush skipped: %s", e)
    except Exception:
        pass
    log.info("shutdown: complete")


app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
# Gzip compression on responses >= 500 bytes. Screener + events JSON
# responses are typically 20-200KB uncompressed, usually 3-5x smaller
# once gzipped. Huge bandwidth savings for paginated card loads and
# the screener table.
app.add_middleware(GZipMiddleware, minimum_size=500)


# ── Cloudflare cache-safety headers ──────────────────────────────
class CloudflareCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/api/") and "Cache-Control" not in response.headers:
            response.headers["Cache-Control"] = "private, no-store"
        return response


app.add_middleware(CloudflareCacheMiddleware)


# ── Request timing + slow-request logging ──────────────────────────
# Logs every request that takes longer than SLOW_REQUEST_MS so we can
# spot regressions. Also sets X-Response-Time-Ms on the response.
SLOW_REQUEST_MS = int(os.environ.get("SLOW_REQUEST_MS", "1000"))


class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        t0 = time.time()
        response = await call_next(request)
        elapsed_ms = int((time.time() - t0) * 1000)
        response.headers["X-Response-Time-Ms"] = str(elapsed_ms)
        if elapsed_ms >= SLOW_REQUEST_MS:
            logging.getLogger("slow").warning(
                "SLOW %d ms %s %s",
                elapsed_ms,
                request.method,
                str(request.url.path) + ("?" + request.url.query if request.url.query else ""),
            )
        return response


app.add_middleware(TimingMiddleware)


# ── ETag middleware for /api/events ────────────────────────────────
# Live-refresh polls /api/events every 5s per open tab. When prices
# haven't changed, we return 304 Not Modified (empty body, ~200 B)
# instead of the full ~20-50 KB JSON. Saves substantial bandwidth at
# scale and reduces client parse cost.
class EventsETagMiddleware(BaseHTTPMiddleware):
    _etag_paths = ("/api/events", "/api/screener", "/api/meta",
                   "/api/categories", "/api/sports")

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.method != "GET":
            return response
        path = request.url.path
        if not any(path == p for p in self._etag_paths):
            return response
        if response.status_code != 200:
            return response
        # Buffer the response body so we can hash it. Gzip middleware
        # runs after this (middleware order is reverse of add order),
        # so the body here is the raw JSON — perfect for hashing
        # before it gets compressed.
        body_chunks = []
        async for chunk in response.body_iterator:
            body_chunks.append(chunk)
        body = b"".join(body_chunks)
        etag = '"' + hashlib.md5(body).hexdigest() + '"'
        inm = request.headers.get("if-none-match")
        if inm and inm == etag:
            return Response(status_code=304, headers={"ETag": etag,
                            "Cache-Control": "no-cache"})
        new_headers = dict(response.headers)
        new_headers["ETag"] = etag
        new_headers["Cache-Control"] = "no-cache"
        # Content-Length may no longer match after we rebuild; let
        # Starlette/Uvicorn recompute it.
        new_headers.pop("content-length", None)
        return Response(content=body, status_code=response.status_code,
                        headers=new_headers, media_type=response.media_type)


app.add_middleware(EventsETagMiddleware)


async def _price_prune_loop():
    """Hourly: delete price rows older than PRICE_RETENTION_HOURS.
    Also runs once immediately on startup to clear any overflow
    from a previous session (e.g. hitting the 512 MB Neon limit)."""
    _log = logging.getLogger("price_prune")
    await asyncio.sleep(10)  # let DB init finish
    while True:
        try:
            from db import prune_old_prices
            deleted = await prune_old_prices()
            if deleted and deleted > 0:
                _log.info("pruned %d old price rows", deleted)
        except Exception as e:
            _log.error("prune loop error: %s", e)
        await asyncio.sleep(3600)  # every hour


async def _score_flush_loop():
    """Every 30s, snapshot the in-memory game lists from ESPN,
    SportsDB, and SofaScore into the game_scores table, then
    seed any newly-seen teams into the entities/aliases tables."""
    _log = logging.getLogger("score_flush")
    await asyncio.sleep(15)  # let feeds warm up before first flush
    _seed_counter = 0  # only seed entities every 5th cycle (~2.5 min)
    while True:
        try:
            from db import sync_scores_to_db
            espn_snap = sportsdb_snap = sofascore_snap = []
            try:
                from espn_feed import ESPN_GAMES
                espn_snap = list(ESPN_GAMES)
                await sync_scores_to_db("espn", espn_snap)
            except Exception as e:
                _log.error("espn score flush: %s", e)
            try:
                from sportsdb_feed import SPORTSDB_GAMES
                sportsdb_snap = list(SPORTSDB_GAMES)
                await sync_scores_to_db("sportsdb", sportsdb_snap)
            except Exception as e:
                _log.error("sportsdb score flush: %s", e)
            try:
                from sofascore_feed import SOFASCORE_GAMES
                sofascore_snap = list(SOFASCORE_GAMES)
                await sync_scores_to_db("sofascore", sofascore_snap)
            except Exception as e:
                _log.error("sofascore score flush: %s", e)

            # Phase 5: seed entities every 5th cycle (~2.5 min)
            _seed_counter += 1
            if _seed_counter >= 5:
                _seed_counter = 0
                try:
                    from entity_seeder import extract_teams
                    from db import upsert_entities, refresh_alias_sport_cache
                    all_teams = []
                    if espn_snap:
                        all_teams.extend(extract_teams(espn_snap, "espn"))
                    if sportsdb_snap:
                        all_teams.extend(extract_teams(sportsdb_snap, "sportsdb"))
                    if sofascore_snap:
                        all_teams.extend(extract_teams(sofascore_snap, "sofascore"))
                    if all_teams:
                        await upsert_entities(all_teams)
                    # Always refresh the alias→sport cache so new
                    # entities are classifiable by get_data().
                    await refresh_alias_sport_cache()
                except Exception as e:
                    _log.error("entity seed: %s", e)
        except Exception as e:
            _log.error("score flush loop error: %s", e)
        await asyncio.sleep(30)

UTC = timezone.utc

# ── Paste all constants from app.py ───────────────────────────────────────────
# Kalshi API category names (must match exactly what API returns)
KALSHI_CATS = ["Sports","Elections","Politics","Economics","Financials",
"Crypto","Companies","Entertainment","Climate and Weather",
"Science and Technology","Health","Social","World","Transportation","Mentions"]

# Display names for UI (broader, cleaner)
CAT_DISPLAY = {
    "Sports":                "Sports",
    "Elections":             "Politics",   # merge Elections into Politics
    "Politics":              "Politics",
    "Economics":             "Economics",
    "Financials":            "Financials",
    "Crypto":                "Crypto",
    "Companies":             "Companies",
    "Entertainment":         "Culture",
    "Climate and Weather":   "Climate",
    "Science and Technology":"Tech & Science",
    "Health":                "Health",
    "Social":                "Social",
    "World":                 "World",
    "Transportation":        "Transportation",
    "Mentions":              "Mentions",
}

# UI tabs - deduplicated display names in order
TOP_CATS = ["Sports","Politics","Economics","Financials","Crypto",
"Companies","Culture","Climate","Tech & Science","Health","Social",
"World","Transportation","Mentions"]

# Map display name back to Kalshi API categories
DISPLAY_TO_CATS = {
    "Sports":         ["Sports"],
    "Politics":       ["Politics","Elections"],
    "Economics":      ["Economics"],
    "Financials":     ["Financials"],
    "Crypto":         ["Crypto"],
    "Companies":      ["Companies"],
    "Culture":        ["Entertainment"],
    "Climate":        ["Climate and Weather"],
    "Tech & Science": ["Science and Technology"],
    "Health":         ["Health"],
    "Social":         ["Social"],
    "World":          ["World"],
    "Transportation": ["Transportation"],
    "Mentions":       ["Mentions"],
}


# ── Category subcategory tags ─────────────────────────────────────────────────
CAT_TAGS = {
    "Politics":       ["US Elections","Senate","House","Governor","Primaries","Trump","Trump Agenda","Congress","Bills","SCOTUS","Tariffs","Immigration","Foreign Elections","Local","Recurring","Approval Ratings","Cabinet"],
    "Economics":      ["Fed","Interest Rates","Inflation","CPI","GDP","Jobs","Unemployment","Housing","Oil","Recession","Trade","Global"],
    "Financials":     ["S&P 500","Nasdaq","Dow","Gold","Metals","Oil & Gas","Treasuries","Agriculture","Volatility"],
    "Crypto":         ["Bitcoin","Ethereum","Solana","Dogecoin","XRP","BNB","Pre-Market","Altcoins"],
    "Companies":      ["Earnings","IPOs","Elon Musk","Tesla","SpaceX","CEOs","Tech","Layoffs","AI","Mergers","Streaming"],
    "Culture":        ["Movies","Television","Music","Awards","Oscars","Grammys","Emmys","Video games","Netflix","Spotify","Billboard","Rotten Tomatoes"],
    "Climate":        ["Hurricanes","Temperature","Snow & Rain","Climate Change","Natural Disasters","Heat","Energy"],
    "Tech & Science": ["AI","Space","Medicine","Energy","LLMs","OpenAI","Biotech","Autonomous vehicles"],
    "Health":         ["Disease","Vaccines","FDA","Mental health","Drugs","Measles","Flu"],
    "Social":         ["Social media","Demographics","Culture","Religion","Immigration"],
    "World":          ["Middle East","Europe","Asia","China","Russia","Ukraine","NATO","UN","Latin America","Africa"],
    "Transportation": ["Airlines","Electric vehicles","Infrastructure","FAA","Boeing"],
    "Mentions":       ["Trump","Elon Musk","Taylor Swift","Sports","Politics","AI","Economy"],
}

CAT_META = {
    "Sports":("🏟️","pill-sports"),"Elections":("🗳️","pill-elections"),
    "Politics":("🏛️","pill-politics"),"Economics":("📈","pill-economics"),
    "Financials":("💰","pill-financials"),"Crypto":("₿","pill-crypto"),
    "Companies":("🏢","pill-companies"),"Entertainment":("🎬","pill-entertainment"),
    "Climate and Weather":("🌍","pill-climate"),"Science and Technology":("🔬","pill-science"),
    "Health":("🏥","pill-health"),"Social":("👥","pill-default"),
    "World":("🌐","pill-default"),"Transportation":("✈️","pill-default"),
    "Mentions":("💬","pill-default"),
}

SPORT_ICONS = {
    "Soccer":"⚽","Basketball":"🏀","Baseball":"⚾","Football":"🏈",
    "Hockey":"🏒","Tennis":"🎾","Golf":"⛳","MMA":"🥊","Cricket":"🏏",
    "Esports":"🎮","Motorsport":"🏎️","Boxing":"🥊","Rugby":"🏉",
    "Lacrosse":"🥍","Chess":"♟️","Darts":"🎯","Aussie Rules":"🏉",
    "Other Sports":"🏆",
}

_SPORT_SERIES = {
"Soccer":["KXEPLGAME","KXEPL1H","KXEPLSPREAD","KXEPLTOTAL","KXEPLBTTS","KXEPLTOP4","KXEPLTOP2","KXEPLTOP6","KXEPLRELEGATION","KXPREMIERLEAGUE","KXARSENALCUPS","KXWINSTREAKMANU","KXNEXTMANAGERMANU","KXPFAPOY","KXLAMINEYAMAL","KXUCLGAME","KXUCL1H","KXUCLSPREAD","KXUCLTOTAL","KXUCLBTTS","KXUCL","KXUCLFINALIST","KXUCLRO4","KXUCLW","KXLEADERUCLGOALS","KXTEAMSINUCL","KXUELGAME","KXUELSPREAD","KXUELTOTAL","KXUEL","KXUECL","KXUECLGAME","KXLALIGAGAME","KXLALIGA1H","KXLALIGASPREAD","KXLALIGATOTAL","KXLALIGABTTS","KXLALIGA","KXLALIGATOP4","KXLALIGARELEGATION","KXLALIGA2GAME","KXSERIEAGAME","KXSERIEA1H","KXSERIEASPREAD","KXSERIEATOTAL","KXSERIEABTTS","KXSERIEA","KXSERIEATOP4","KXSERIEARELEGATION","KXSERIEBGAME","KXBUNDESLIGAGAME","KXBUNDESLIGA1H","KXBUNDESLIGASPREAD","KXBUNDESLIGATOTAL","KXBUNDESLIGABTTS","KXBUNDESLIGA","KXBUNDESLIGATOP4","KXBUNDESLIGARELEGATION","KXBUNDESLIGA2GAME","KXLIGUE1GAME","KXLIGUE11H","KXLIGUE1SPREAD","KXLIGUE1TOTAL","KXLIGUE1BTTS","KXLIGUE1","KXLIGUE1TOP4","KXLIGUE1RELEGATION","KXMLSGAME","KXMLSSPREAD","KXMLSTOTAL","KXMLSBTTS","KXMLSCUP","KXMLSEAST","KXMLSWEST","KXLIGAMXGAME","KXLIGAMXSPREAD","KXLIGAMXTOTAL","KXLIGAMX","KXBRASILEIROGAME","KXBRASILEIROSPREAD","KXBRASILEIROTOTAL","KXBRASILEIRO","KXBRASILEIROTOPX","KXWCGAME","KXWCROUND","KXWCGROUPWIN","KXWCGROUPQUAL","KXWCGOALLEADER","KXWCMESSIRONALDO","KXWCLOCATION","KXWCIRAN","KXWCSQUAD","KXMENWORLDCUP","KXSOCCERPLAYMESSI","KXSOCCERPLAYCRON","KXFIFAUSPULL","KXFIFAUSPULLGAME","KXSAUDIPLGAME","KXSAUDIPLSPREAD","KXSAUDIPLTOTAL","KXLIGAPORTUGALGAME","KXLIGAPORTUGAL","KXEREDIVISIEGAME","KXEREDIVISIE","KXCOPADELREY","KXDFBPOKAL","KXFACUP","KXCOPPAITALIA","KXEFLCHAMPIONSHIPGAME","KXEFLCHAMPIONSHIP","KXEFLPROMO","KXSUPERLIGGAME","KXSUPERLIG","KXCONCACAFCCUPGAME","KXCONMEBOLLIBGAME","KXCONMEBOLSUDGAME","KXUSLGAME","KXUSL","KXSCOTTISHPREMGAME","KXEKSTRAKLASAGAME","KXEKSTRAKLASA","KXALEAGUEGAME","KXALEAGUESPREAD","KXALEAGUETOTAL","KXKLEAGUEGAME","KXKLEAGUE","KXJLEAGUEGAME","KXCHNSLGAME","KXCHNSL","KXALLSVENSKANGAME","KXDENSUPERLIGAGAME","KXDENSUPERLIGA","KXSWISSLEAGUEGAME","KXARGPREMDIVGAME","KXDIMAYORGAME","KXURYPDGAME","KXURYPD","KXECULPGAME","KXECULP","KXVENFUTVEGAME","KXVENFUTVE","KXCHLLDPGAME","KXCHLLDP","KXAPFDDHGAME","KXAPFDDH","KXBALLERLEAGUEGAME","KXSLGREECEGAME","KXSLGREECE","KXTHAIL1GAME","KXTHAIL1","KXEGYPLGAME","KXHNLGAME","KXBELGIANPLGAME","KXBELGIANPL","KXPERLIGA1","KXKNVBCUP","KXSOCCERTRANSFER","KXJOINLEAGUE","KXJOINRONALDO","KXJOINCLUB","KXBALLONDOR","KXEPL","KXMLS","KXSAUDIPL","KXALEAGUE","KXSCOTTISHPREM","KXARGPREMDIV","KXDIMAYOR","KXBALLERLEAGUE","KXEGYPL","KXHNL","KXJLEAGUE","KXALLSVENSKAN","KXSWISSLEAGUE","KXEKSTRAKLASA","KXPERLIGA1GAME","KXURYPDGAME","KXBOLPDIVGAME","KXBOLPDIV","KXIT1GAME","KXIT1","KXNEXTMANAGERLALIGA"],
"Basketball":["KXNBAGAME","KXNBASPREAD","KXNBATOTAL","KXNBATEAMTOTAL","KXNBA1HWINNER","KXNBA1HSPREAD","KXNBA1HTOTAL","KXNBA2HWINNER","KXNBA2D","KXNBA3D","KXNBA3PT","KXNBAPTS","KXNBAREB","KXNBAAST","KXNBABLK","KXNBASTL","KXNBA","KXNBAEAST","KXNBAWEST","KXNBAPLAYOFF","KXNBAPLAYIN","KXNBAATLANTIC","KXNBACENTRAL","KXNBASOUTHEAST","KXNBANORTHWEST","KXNBAPACIFIC","KXNBASOUTHWEST","KXNBAEAST1SEED","KXNBAWEST1SEED","KXTEAMSINNBAF","KXTEAMSINNBAEF","KXTEAMSINNBAWF","KXNBAMATCHUP","KXNBAWINS","KXRECORDNBABEST","KXNBAMVP","KXNBAROY","KXNBACOY","KXNBADPOY","KXNBASIXTH","KXNBAMIMP","KXNBACLUTCH","KXNBAFINMVP","KXNBAWFINMVP","KXNBAEFINMVP","KXNBA1STTEAM","KXNBA2NDTEAM","KXNBA3RDTEAM","KXNBA1STTEAMDEF","KXNBA2NDTEAMDEF","KXLEADERNBAPTS","KXLEADERNBAREB","KXLEADERNBAAST","KXLEADERNBABLK","KXLEADERNBASTL","KXLEADERNBA3PT","KXNBADRAFT1","KXNBADRAFTPICK","KXNBADRAFTTOP","KXNBADRAFTCAT","KXNBADRAFTCOMP","KXNBATOPPICK","KXNBALOTTERYODDS","KXNBATOP5ROTY","KXNBATEAM","KXNBASEATTLE","KXCITYNBAEXPAND","KXSONICS","KXNEXTTEAMNBA","KXLBJRETIRE","KXSPORTSOWNERLBJ","KXSTEPHDEAL","KXQUADRUPLEDOUBLE","KXSHAI20PTREC","KXNBA2KCOVER","KXWNBADRAFT1","KXWNBADRAFTTOP3","KXWNBADELAY","KXWNBAGAMESPLAYED","KXMARMAD","KXNCAAMBNEXTCOACH","KXNBASERIESSCORE","KXEUROLEAGUEGAME","KXEUROLEAGUESPREAD","KXEUROLEAGUETOTAL","KXBSLGAME","KXBSLSPREAD","KXBSLTOTAL","KXBBLGAME","KXBBLSPREAD","KXBBLTOTAL","KXACBGAME","KXACBSPREAD","KXACBTOTAL","KXISLGAME","KXISLSPREAD","KXISLTOTAL","KXABAGAME","KXABASPREAD","KXABATOTAL","KXCBAGAME","KXCBASPREAD","KXCBATOTAL","KXBBSERIEAGAME","KXBBSERIEASPREAD","KXBBSERIEATOTAL","KXJBLEAGUEGAME","KXJBLEAGUESPREAD","KXJBLEAGUETOTAL","KXLNBELITEGAME","KXLNBELITESPREAD","KXLNBELITETOTAL","KXARGLNBGAME","KXARGLNBSPREAD","KXARGLNBTOTAL","KXVTBGAME","KXVTBSPREAD","KXVTBTOTAL"],
"Baseball":["KXMLBGAME","KXMLBRFI","KXMLBSPREAD","KXMLBTOTAL","KXMLBTEAMTOTAL","KXMLBF5","KXMLBF5SPREAD","KXMLBF5TOTAL","KXMLBHIT","KXMLBHR","KXMLBHRR","KXMLBKS","KXMLBTB","KXMLB","KXMLBAL","KXMLBNL","KXMLBALEAST","KXMLBALWEST","KXMLBALCENT","KXMLBNLEAST","KXMLBNLWEST","KXMLBNLCENT","KXMLBPLAYOFFS","KXTEAMSINWS","KXMLBBESTRECORD","KXMLBWORSTRECORD","KXMLBLSTREAK","KXMLBWSTREAK","KXMLBALMVP","KXMLBNLMVP","KXMLBALCY","KXMLBNLCY","KXMLBALROTY","KXMLBNLROTY","KXMLBEOTY","KXMLBALMOTY","KXMLBNLMOTY","KXMLBALHAARON","KXMLBNLHAARON","KXMLBALCPOTY","KXMLBNLCPOTY","KXMLBALRELOTY","KXMLBNLRELOTY","KXMLBSTAT","KXMLBSTATCOUNT","KXMLBSEASONHR","KXLEADERMLBAVG","KXLEADERMLBDOUBLES","KXLEADERMLBERA","KXLEADERMLBHITS","KXLEADERMLBHR","KXLEADERMLBKS","KXLEADERMLBOPS","KXLEADERMLBRBI","KXLEADERMLBRUNS","KXLEADERMLBSTEALS","KXLEADERMLBTRIPLES","KXLEADERMLBWAR","KXLEADERMLBWINS","KXMLBTRADE","KXWSOPENTRANTS","KXNPBGAME","KXKBOGAME","KXNCAABBGAME","KXNCAABASEBALL","KXNCAABBGS"],
"Football":["KXUFLGAME","KXSB","KXNFLPLAYOFF","KXNFLAFCCHAMP","KXNFLNFCCHAMP","KXNFLAFCEAST","KXNFLAFCWEST","KXNFLAFCNORTH","KXNFLAFCSOUTH","KXNFLNFCEAST","KXNFLNFCWEST","KXNFLNFCNORTH","KXNFLNFCSOUTH","KXNFLMVP","KXNFLOPOTY","KXNFLDPOTY","KXNFLOROTY","KXNFLDROTY","KXNFLCOTY","KXNFLDRAFT1","KXNFLDRAFT1ST","KXNFLDRAFTPICK","KXNFLDRAFTTOP","KXNFLDRAFTWR","KXNFLDRAFTDB","KXNFLDRAFTTE","KXNFLDRAFTQB","KXNFLDRAFTOL","KXNFLDRAFTEDGE","KXNFLDRAFTLB","KXNFLDRAFTRB","KXNFLDRAFTDT","KXNFLDRAFTTEAM","KXLEADERNFLSACKS","KXLEADERNFLINT","KXLEADERNFLPINT","KXLEADERNFLPTDS","KXLEADERNFLPYDS","KXLEADERNFLRTDS","KXLEADERNFLRUSHTDS","KXLEADERNFLRUSHYDS","KXLEADERNFLRYDS","KXNFLTEAM1POS","KXNFLPRIMETIME","KXNFLTRADE","KXNEXTTEAMNFL","KXRECORDNFLBEST","KXRECORDNFLWORST","KXKELCERETIRE","KXSTARTINGQBWEEK1","KXCOACHOUTNFL","KXCOACHOUTNCAAFB","KXARODGRETIRE","KXRELOCATIONCHI","KX1STHOMEGAME","KXSORONDO","KXNCAAF","KXHEISMAN","KXNCAAFCONF","KXNCAAFACC","KXNCAAFB10","KXNCAAFB12","KXNCAAFSEC","KXNCAAFAAC","KXNCAAFSBELT","KXNCAAFMWC","KXNCAAFMAC","KXNCAAFCUSA","KXNCAAFPAC12","KXNCAAFPLAYOFF","KXNCAAFFINALIST","KXNCAAFUNDEFEATED","KXNCAAFCOTY","KXNCAAFAPRANK","KXNDJOINCONF","KXCOVEREA","KXDONATEMRBEAST"],
"Hockey":["KXNHLGAME","KXNHLSPREAD","KXNHLTOTAL","KXNHL","KXNHLPLAYOFF","KXTEAMSINSC","KXNHLPRES","KXNHLEAST","KXNHLWEST","KXNHLADAMS","KXNHLCENTRAL","KXNHLATLANTIC","KXNHLMETROPOLITAN","KXNHLPACIFIC","KXNHLHART","KXNHLNORRIS","KXNHLVEZINA","KXNHLCALDER","KXNHLROSS","KXNHLRICHARD","KXAHLGAME","KXCANADACUP","KXNCAAHOCKEY","KXNCAAHOCKEYGAME","KXKHLGAME","KXSHLGAME","KXLIIGAGAME","KXELHGAME","KXNLGAME","KXDELGAME"],
"Tennis":["KXATPMATCH","KXATPSETWINNER","KXATPCHALLENGERMATCH","KXATPGRANDSLAM","KXATPGRANDSLAMFIELD","KXATP1RANK","KXMCMMEN","KXFOMEN","KXWTAMATCH","KXWTAGRANDSLAM","KXWTASERENA","KXFOWOMEN","KXGRANDSLAM","KXGRANDSLAMJFONSECA","KXGOLFTENNISMAJORS"],
"Golf":["KXPGATOUR","KXPGAH2H","KXPGA3BALL","KXPGA5BALL","KXPGAR1LEAD","KXPGAR1TOP5","KXPGAR1TOP10","KXPGAR1TOP20","KXPGAR2LEAD","KXPGAR2TOP5","KXPGAR2TOP10","KXPGAR3LEAD","KXPGAR3TOP5","KXPGAR3TOP10","KXPGATOP5","KXPGATOP10","KXPGATOP20","KXPGATOP40","KXPGAPLAYOFF","KXPGACUTLINE","KXPGAMAKECUT","KXPGAAGECUT","KXPGAWINNERREGION","KXPGALOWSCORE","KXPGASTROKEMARGIN","KXPGAWINNINGSCORE","KXPGAPLAYERCAT","KXPGABIRDIES","KXPGAROUNDSCORE","KXPGAEAGLE","KXPGAHOLEINONE","KXPGABOGEYFREE","KXPGAMAJORTOP10","KXPGAMAJORWIN","KXPGAMASTERS","KXGOLFMAJORS","KXGOLFTENNISMAJORS","KXPGARYDER","KXPGASOLHEIM","KXRYDERCUPCAPTAIN","KXPGACURRY","KXPGATIGER","KXBRYSONCOURSERECORDS","KXSCOTTIESLAM"],
"MMA":["KXUFCFIGHT","KXUFCHEAVYWEIGHTTITLE","KXUFCLHEAVYWEIGHTTITLE","KXUFCMIDDLEWEIGHTTITLE","KXUFCWELTERWEIGHTTITLE","KXUFCLIGHTWEIGHTTITLE","KXUFCFEATHERWEIGHTTITLE","KXUFCBANTAMWEIGHTTITLE","KXUFCFLYWEIGHTTITLE","KXMCGREGORFIGHTNEXT","KXCARDPRESENCEUFCWH","KXUFCWHITEHOUSE"],
"Cricket":["KXIPLGAME","KXIPL","KXIPLFOUR","KXIPLSIX","KXIPLTEAMTOTAL","KXPSLGAME","KXPSL","KXT20MATCH"],
"Esports":["KXVALORANTMAP","KXVALORANTGAME","KXLOLGAME","KXLOLMAP","KXLOLTOTALMAPS","KXR6GAME","KXR6MAP","KXCS2GAME","KXCS2MAP","KXCS2TOTALMAPS","KXDOTA2GAME","KXDOTA2MAP","KXOWGAME"],
"Motorsport":["KXF1RACE","KXF1RACEPODIUM","KXF1TOP5","KXF1TOP10","KXF1FASTLAP","KXF1CONSTRUCTORS","KXF1RETIRE","KXF1","KXF1OCCUR","KXF1CHINA","KXNASCARCUPSERIES","KXNASCARRACE","KXNASCARTOP3","KXNASCARTOP5","KXNASCARTOP10","KXNASCARTOP20","KXNASCARTRUCKSERIES","KXNASCARAUTOPARTSSERIES","KXMOTOGP","KXMOTOGPTEAMS","KXINDYCARSERIES"],
"Boxing":["KXBOXING","KXFLOYDTYSONFIGHT","KXWBCHEAVYWEIGHTTITLE","KXWBCCRUISERWEIGHTTITLE","KXWBCMIDDLEWEIGHTTITLE","KXWBCWELTERWEIGHTTITLE","KXWBCLIGHTWEIGHTTITLE","KXWBCFEATHERWEIGHTTITLE","KXWBCBANTAMWEIGHTTITLE","KXWBCFLYWEIGHTTITLE"],
"Rugby":["KXRUGBYNRLMATCH","KXNRLCHAMP","KXPREMCHAMP","KXSLRCHAMP","KXFRA14CHAMP"],
"Lacrosse":["KXNCAAMLAXGAME","KXNCAALAXFINAL","KXLAXTEWAARATON"],
"Chess":["KXCHESSWORLDCHAMPION","KXCHESSCANDIDATES"],
"Darts":["KXDARTSMATCH","KXPREMDARTS"],
"Aussie Rules":["KXAFLGAME"],
"Other Sports":["KXSAILGP","KXPIZZASCORE9","KXROCKANDROLLHALLOFFAME","KXEUROVISIONISRAELBAN","KXCOLLEGEGAMEDAYGUEST","KXWSOPENTRANTS"],
}

SOCCER_COMP = {
    "KXEPLGAME":"EPL","KXEPL1H":"EPL","KXEPLSPREAD":"EPL","KXEPLTOTAL":"EPL",
    "KXEPLBTTS":"EPL","KXEPLTOP4":"EPL","KXEPLTOP2":"EPL","KXEPLTOP6":"EPL",
    "KXEPLRELEGATION":"EPL","KXPREMIERLEAGUE":"EPL","KXARSENALCUPS":"EPL",
    "KXWINSTREAKMANU":"EPL","KXNEXTMANAGERMANU":"EPL","KXPFAPOY":"EPL","KXLAMINEYAMAL":"EPL",
    "KXUCLGAME":"Champions League","KXUCL1H":"Champions League","KXUCLSPREAD":"Champions League",
    "KXUCLTOTAL":"Champions League","KXUCLBTTS":"Champions League","KXUCL":"Champions League",
    "KXUCLFINALIST":"Champions League","KXUCLRO4":"Champions League","KXUCLW":"Champions League",
    "KXLEADERUCLGOALS":"Champions League","KXTEAMSINUCL":"Champions League",
    "KXUELGAME":"Europa League","KXUELSPREAD":"Europa League","KXUELTOTAL":"Europa League","KXUEL":"Europa League",
    "KXUECL":"Conference League","KXUECLGAME":"Conference League",
    "KXLALIGAGAME":"La Liga","KXLALIGA1H":"La Liga","KXLALIGASPREAD":"La Liga",
    "KXLALIGATOTAL":"La Liga","KXLALIGABTTS":"La Liga","KXLALIGA":"La Liga",
    "KXLALIGATOP4":"La Liga","KXLALIGARELEGATION":"La Liga",
    "KXLALIGA2GAME":"La Liga 2",
    "KXSERIEAGAME":"Serie A","KXSERIEA1H":"Serie A","KXSERIEASPREAD":"Serie A",
    "KXSERIEATOTAL":"Serie A","KXSERIEABTTS":"Serie A","KXSERIEA":"Serie A",
    "KXSERIEATOP4":"Serie A","KXSERIEARELEGATION":"Serie A",
    "KXSERIEBGAME":"Serie B",
    "KXBUNDESLIGAGAME":"Bundesliga","KXBUNDESLIGA1H":"Bundesliga","KXBUNDESLIGASPREAD":"Bundesliga",
    "KXBUNDESLIGATOTAL":"Bundesliga","KXBUNDESLIGABTTS":"Bundesliga","KXBUNDESLIGA":"Bundesliga",
    "KXBUNDESLIGATOP4":"Bundesliga","KXBUNDESLIGARELEGATION":"Bundesliga",
    "KXBUNDESLIGA2GAME":"Bundesliga 2",
    "KXLIGUE1GAME":"Ligue 1","KXLIGUE11H":"Ligue 1","KXLIGUE1SPREAD":"Ligue 1",
    "KXLIGUE1TOTAL":"Ligue 1","KXLIGUE1BTTS":"Ligue 1","KXLIGUE1":"Ligue 1",
    "KXLIGUE1TOP4":"Ligue 1","KXLIGUE1RELEGATION":"Ligue 1",
    "KXMLSGAME":"MLS","KXMLSSPREAD":"MLS","KXMLSTOTAL":"MLS","KXMLSBTTS":"MLS",
    "KXMLSCUP":"MLS","KXMLSEAST":"MLS","KXMLSWEST":"MLS",
    "KXLIGAMXGAME":"Liga MX","KXLIGAMXSPREAD":"Liga MX","KXLIGAMXTOTAL":"Liga MX","KXLIGAMX":"Liga MX",
    "KXBRASILEIROGAME":"Brasileiro","KXBRASILEIROSPREAD":"Brasileiro",
    "KXBRASILEIROTOTAL":"Brasileiro","KXBRASILEIRO":"Brasileiro","KXBRASILEIROTOPX":"Brasileiro",
    "KXWCGAME":"World Cup","KXWCROUND":"World Cup","KXWCGROUPWIN":"World Cup",
    "KXWCGROUPQUAL":"World Cup","KXWCGOALLEADER":"World Cup","KXWCMESSIRONALDO":"World Cup",
    "KXWCLOCATION":"World Cup","KXWCIRAN":"World Cup","KXWCSQUAD":"World Cup",
    "KXMENWORLDCUP":"World Cup","KXSOCCERPLAYMESSI":"World Cup","KXSOCCERPLAYCRON":"World Cup",
    "KXFIFAUSPULL":"World Cup","KXFIFAUSPULLGAME":"World Cup",
    "KXSAUDIPLGAME":"Saudi Pro League","KXSAUDIPLSPREAD":"Saudi Pro League","KXSAUDIPLTOTAL":"Saudi Pro League",
    "KXLIGAPORTUGALGAME":"Liga Portugal","KXLIGAPORTUGAL":"Liga Portugal",
    "KXEREDIVISIEGAME":"Eredivisie","KXEREDIVISIE":"Eredivisie",
    "KXCOPADELREY":"Copa del Rey","KXDFBPOKAL":"DFB Pokal",
    "KXFACUP":"FA Cup","KXCOPPAITALIA":"Coppa Italia",
    "KXEFLCHAMPIONSHIPGAME":"EFL Championship","KXEFLCHAMPIONSHIP":"EFL Championship","KXEFLPROMO":"EFL Championship",
    "KXSUPERLIGGAME":"Super Lig","KXSUPERLIG":"Super Lig",
    "KXCONCACAFCCUPGAME":"CONCACAF",
    "KXCONMEBOLLIBGAME":"Libertadores","KXCONMEBOLSUDGAME":"Copa Sudamericana",
    "KXUSLGAME":"USL","KXUSL":"USL",
    "KXSCOTTISHPREMGAME":"Scottish Prem",
    "KXEKSTRAKLASAGAME":"Ekstraklasa","KXEKSTRAKLASA":"Ekstraklasa",
    "KXALEAGUEGAME":"A-League","KXALEAGUESPREAD":"A-League","KXALEAGUETOTAL":"A-League",
    "KXKLEAGUEGAME":"K League","KXKLEAGUE":"K League",
    "KXJLEAGUEGAME":"J League",
    "KXCHNSLGAME":"Chinese SL","KXCHNSL":"Chinese SL",
    "KXALLSVENSKANGAME":"Allsvenskan",
    "KXDENSUPERLIGAGAME":"Danish SL","KXDENSUPERLIGA":"Danish SL",
    "KXSWISSLEAGUEGAME":"Swiss League",
    "KXARGPREMDIVGAME":"Argentinian Div","KXDIMAYORGAME":"Colombian Div",
    "KXURYPDGAME":"Uruguayan Div","KXURYPD":"Uruguayan Div",
    "KXECULPGAME":"Ecuador LigaPro","KXECULP":"Ecuador LigaPro",
    "KXVENFUTVEGAME":"Venezuelan Div","KXVENFUTVE":"Venezuelan Div",
    "KXCHLLDPGAME":"Chilean Div","KXCHLLDP":"Chilean Div",
    "KXAPFDDHGAME":"APF Paraguay","KXAPFDDH":"APF Paraguay",
    "KXBALLERLEAGUEGAME":"Baller League",
    "KXSLGREECEGAME":"Greek SL","KXSLGREECE":"Greek SL",
    "KXTHAIL1GAME":"Thai League","KXTHAIL1":"Thai League",
    "KXEGYPLGAME":"Egyptian PL",
    "KXHNLGAME":"HNL Croatia",
    "KXBELGIANPLGAME":"Belgian Pro","KXBELGIANPL":"Belgian Pro",
    "KXPERLIGA1":"Peruvian L1","KXKNVBCUP":"KNVB Cup",
    "KXSOCCERTRANSFER":"Transfers/News","KXJOINLEAGUE":"Transfers/News",
    "KXJOINRONALDO":"Transfers/News","KXJOINCLUB":"Transfers/News","KXBALLONDOR":"Transfers/News",
    "KXEPL":"EPL",
    "KXMLS":"MLS",
    "KXSAUDIPL":"Saudi Pro",
    "KXALEAGUE":"A-League",
    "KXSCOTTISHPREM":"Scottish Prem",
    "KXARGPREMDIV":"Arg Prim Div",
    "KXDIMAYOR":"Colombian Div",
    "KXBALLERLEAGUE":"Baller League",
    "KXEGYPL":"Egyptian PL",
    "KXHNL":"HNL",
    "KXJLEAGUE":"J-League",
    "KXALLSVENSKAN":"Allsvenskan",
    "KXSWISSLEAGUE":"Swiss Super",
    "KXEKSTRAKLASA":"Ekstraklasa",
    "KXPERLIGA1GAME":"Peruvian Liga 1",
    "KXURYPDGAME":"Uruguay Primera",
    "KXBOLPDIVGAME":"Bolivian Premier","KXBOLPDIV":"Bolivian Premier",
    "KXIT1GAME":"Italian Serie A","KXIT1":"Italian Serie A",
    "KXNEXTMANAGERLALIGA":"La Liga",
}

SERIES_SPORT = {}
for sport, series_list in _SPORT_SERIES.items():
    for s in series_list:
        SERIES_SPORT[s] = sport

# Prefix-based fallback: when a series like KXMLBPLAYEROTW or
# KXITFMATCH isn't in our hardcoded SERIES_SPORT map, classify by
# the well-known Kalshi ticker prefix family. Order matters — longer
# prefixes check first so KXMLBF5 still matches MLB (not some
# generic KX* rule). This is safer than entity-alias fallback, which
# can misfire on generic words like "pro" / "of" / "week".
_SPORT_PREFIX_FALLBACK = [
    # Baseball
    ("KXMLB", "Baseball"), ("KXNPB", "Baseball"), ("KXKBO", "Baseball"),
    ("KXNCAABB", "Baseball"), ("KXNCAABASEBALL", "Baseball"),
    # Basketball
    ("KXNBA", "Basketball"), ("KXWNBA", "Basketball"),
    ("KXNCAAMB", "Basketball"), ("KXNCAAWB", "Basketball"),
    ("KXEUROLEAGUE", "Basketball"), ("KXBSL", "Basketball"),
    ("KXBBL", "Basketball"), ("KXACB", "Basketball"),
    ("KXISL", "Basketball"), ("KXABA", "Basketball"),
    ("KXCBA", "Basketball"), ("KXBBSERIEA", "Basketball"),
    ("KXJBLEAGUE", "Basketball"), ("KXLNBELITE", "Basketball"),
    ("KXARGLNB", "Basketball"), ("KXVTB", "Basketball"),
    # Football (American)
    ("KXNFL", "Football"), ("KXUFL", "Football"),
    ("KXNCAAF", "Football"), ("KXSB", "Football"),
    # Hockey
    ("KXNHL", "Hockey"), ("KXAHL", "Hockey"),
    ("KXKHL", "Hockey"), ("KXSHL", "Hockey"),
    ("KXLIIGA", "Hockey"), ("KXELH", "Hockey"),
    ("KXNCAAHOCKEY", "Hockey"), ("KXDEL", "Hockey"),
    # Tennis
    ("KXATP", "Tennis"), ("KXWTA", "Tennis"), ("KXITF", "Tennis"),
    ("KXGRANDSLAM", "Tennis"), ("KXMCMMEN", "Tennis"),
    ("KXFOMEN", "Tennis"), ("KXFOWOMEN", "Tennis"),
    # Golf
    ("KXPGA", "Golf"), ("KXGOLFMAJORS", "Golf"),
    ("KXRYDERCUP", "Golf"),
    # MMA
    ("KXUFC", "MMA"),
    # Motorsport
    ("KXF1", "Motorsport"), ("KXNASCAR", "Motorsport"),
    ("KXMOTOGP", "Motorsport"), ("KXINDYCAR", "Motorsport"),
    # Cricket
    ("KXIPL", "Cricket"), ("KXPSL", "Cricket"), ("KXT20", "Cricket"),
    # Boxing
    ("KXBOXING", "Boxing"), ("KXWBC", "Boxing"),
    # Esports
    ("KXVALORANT", "Esports"), ("KXLOL", "Esports"),
    ("KXR6", "Esports"), ("KXCS2", "Esports"),
    ("KXDOTA2", "Esports"), ("KXOW", "Esports"),
    # Rugby
    ("KXRUGBY", "Rugby"), ("KXNRL", "Rugby"),
    ("KXPREMRUGBY", "Rugby"), ("KXSLR", "Rugby"), ("KXFRA14", "Rugby"),
    # Aussie Rules
    ("KXAFL", "Aussie Rules"),
    # Darts
    ("KXDARTS", "Darts"), ("KXPREMDARTS", "Darts"),
    # Lacrosse
    ("KXNCAAMLAX", "Lacrosse"), ("KXNCAALAX", "Lacrosse"),
    ("KXLAX", "Lacrosse"),
    # Chess
    ("KXCHESS", "Chess"),
    # Soccer (must be last — highly specific prefixes only, since
    # many of our Soccer prefixes are league codes. Generic KXSOCCER
    # catches misc. soccer markets).
    ("KXSOCCER", "Soccer"),
]
# Sort by prefix length descending so longest match wins.
_SPORT_PREFIX_FALLBACK.sort(key=lambda p: -len(p[0]))


def get_sport(series_ticker):
    s = str(series_ticker).upper()
    sport = SERIES_SPORT.get(s, "")
    if sport:
        return sport
    # Prefix-based classification for series not yet in the hardcoded
    # map (handles KXMLBPLAYEROTW, KXITFMATCH, etc.).
    for prefix, sp in _SPORT_PREFIX_FALLBACK:
        if s.startswith(prefix):
            return sp
    return ""


# ── Game-market grouping ──────────────────────────────────────────────────────
# Kalshi publishes a single game's different market types as
# separate events that all share the same game-suffix in the
# event_ticker. Examples:
#   KXNHLGAME-26APR11WSHPIT    →  moneyline (parent)
#   KXNHLSPREAD-26APR11WSHPIT  →  puck line
#   KXNHLTOTAL-26APR11WSHPIT   →  over/under
# Kalshi's own UI merges these into one card with tabs. We do the
# same: the moneyline becomes the parent, siblings become tabs,
# and sibling events are dropped from the records list.
#
# Auto-detect: scan _SPORT_SERIES for every series ending with
# "GAME" as a primary parent, then check which sibling suffixes
# (SPREAD, TOTAL, BTTS, 1H, etc.) also exist. This covers every
# league without a manually-maintained per-league map.
_SIBLING_SUFFIXES = [
    # (suffix, type_code, fallback_label, tab_priority)
    ("SPREAD",    "spread",    "Spread",     1),
    ("TOTAL",     "total",     "Totals",     2),
    ("BTTS",      "btts",      "Both Score", 3),
    ("TEAMTOTAL", "teamtotal", "Team Total", 4),
    ("1H",        "firsthalf", "1st Half",   5),
    ("1HWINNER",  "1hwinner",  "1st Half",   5),
    ("1HSPREAD",  "1hspread",  "1H Spread",  6),
    ("1HTOTAL",   "1htotal",   "1H Totals",  7),
    ("2HWINNER",  "2hwinner",  "2nd Half",   8),
    ("RFI",       "rfi",       "RFI",        9),
    ("F5",        "f5",        "First 5",   10),
    ("F5SPREAD",  "f5spread",  "F5 Spread", 11),
    ("F5TOTAL",   "f5total",   "F5 Totals", 12),
    ("SETWINNER", "setwinner", "Set Winner", 13),
    ("MAP",       "map",       "Map",        14),
    ("TOTALMAPS", "totalmaps", "Total Maps", 15),
    ("ADVANCE",   "advance",   "To Advance", 16),
]

# Build map automatically from _SPORT_SERIES.
_all_series = set()
for _sl in _SPORT_SERIES.values():
    _all_series.update(s.upper() for s in _sl)
GAME_MARKET_PREFIXES = {}
# Detect both "GAME" and "MATCH" as primary parents.
# Soccer/NBA/MLB/NHL use *GAME, Tennis uses *MATCH.
for _s in sorted(_all_series):
    for _primary_suffix, _strip_len in [("GAME", 4), ("MATCH", 5)]:
        if _s.endswith(_primary_suffix):
            _prefix = _s[:-_strip_len]
            if not _prefix:
                continue
            GAME_MARKET_PREFIXES[_s] = ("moneyline", "Winner", 0, True)
            for _suffix, _tc, _lbl, _pri in _SIBLING_SUFFIXES:
                _sibling = _prefix + _suffix
                if _sibling in _all_series:
                    GAME_MARKET_PREFIXES[_sibling] = (_tc, _lbl, _pri, False)
            break  # don't check MATCH if GAME already matched

# Series whose event tickers have a trailing set/map number
# (e.g. KXATPSETWINNER-26APR12BUSMOU-1). The "-1" must be
# stripped so the suffix matches the parent (26APR12BUSMOU).
# Series whose tickers have a trailing set/map number
# (e.g. KXATPSETWINNER-...-1, KXCS2MAP-...-2). The "-N"
# must be stripped so the suffix matches the parent.
_SUFFIXED_SERIES = {s for s in GAME_MARKET_PREFIXES
                    if s.endswith("SETWINNER") or
                       (s.endswith("MAP") and not s.endswith("TOTALMAPS"))}


def _game_suffix(event_ticker: str) -> str:
    """KXLALIGAGAME-26APR11SEVATM → '26APR11SEVATM'.
    Returns the part after the first '-', which Kalshi uses as the
    shared per-game identifier across sibling market events."""
    parts = (event_ticker or "").split("-", 1)
    return parts[1] if len(parts) == 2 else ""


def _group_game_markets(records):
    """Collapse sibling game-market events into a parent card.

    Walks records once, buckets any record whose series_ticker is in
    GAME_MARKET_PREFIXES by its game suffix, and for each suffix that
    has a primary (moneyline) record attaches the siblings as
    `_market_groups` on the primary. Siblings are dropped from the
    top-level list so they don't double-render as standalone cards.
    Orphan siblings (no moneyline parent) are left in place.
    """
    by_suffix = {}  # suffix → {type_code: record}
    for r in records:
        series = (r.get("series_ticker") or "").upper()
        mt = GAME_MARKET_PREFIXES.get(series)
        if not mt:
            continue
        suffix = _game_suffix(r.get("event_ticker", ""))
        if not suffix:
            continue
        # For series like KXATPSETWINNER, strip the trailing set/map
        # number ("-1", "-2") so the suffix matches the parent match.
        # KXATPSETWINNER-26APR12BUSMOU-1 → suffix "26APR12BUSMOU"
        if series in _SUFFIXED_SERIES:
            import re as _re
            # Extract the set/map number for a unique type_code
            # (setwinner_1, setwinner_2, etc.)
            num_match = _re.search(r'-(\d+)$', suffix)
            suffix = _re.sub(r'-\d+$', '', suffix)
            if num_match:
                tc = mt[0] + "_" + num_match.group(1)
            else:
                tc = mt[0]
            by_suffix.setdefault(suffix, {})[tc] = r
        else:
            by_suffix.setdefault(suffix, {})[mt[0]] = r

    to_drop = set()  # event_tickers of siblings to remove from list
    for suffix, type_map in by_suffix.items():
        primary = type_map.get("moneyline")
        if not primary:
            # Orphan — no parent GAME event. Leave siblings alone so
            # they still surface as individual cards.
            continue
        # Iterate only the type_codes present for THIS game, sorted
        # by their tab priority. (The old code iterated all 77
        # entries in GAME_MARKET_PREFIXES, causing duplicate tabs
        # because many entries share the same type_code — e.g.
        # every league's GAME prefix maps to "moneyline".)
        def _prio(tc):
            rec = type_map[tc]
            mt = GAME_MARKET_PREFIXES.get(
                (rec.get("series_ticker") or "").upper(), ("", "", 99, False)
            )
            return mt[2]  # tab priority
        groups = []
        for type_code in sorted(type_map.keys(), key=_prio):
            rec = type_map[type_code]
            series_up = (rec.get("series_ticker") or "").upper()
            mt = GAME_MARKET_PREFIXES.get(series_up)
            if not mt:
                continue
            _tc, fallback_label, _priority, is_primary = mt
            # Use Kalshi's own label from the event title so the tab
            # strip matches what Kalshi shows. Sibling titles look
            # like "Washington at Pittsburgh: Puck Line" — everything
            # after the last ": " is the market-type label Kalshi
            # publishes. Moneyline events have no ": " suffix, so we
            # fall back to the map default for those.
            title = str(rec.get("title") or "")
            if ": " in title and not is_primary:
                label = title.rsplit(": ", 1)[-1].strip() or fallback_label
            else:
                label = fallback_label
            # Build the Kalshi URL for this specific sibling so the
            # card's ticker link can update to point at the market
            # type that's currently shown on the active tab.
            sib_ticker = str(rec.get("event_ticker", ""))
            sib_series = str(rec.get("series_ticker", ""))
            if sib_series:
                _s = sib_series.lower()
                sib_url = (
                    f"https://kalshi.com/markets/{_s}/"
                    f"{_s.replace('kx', '')}/{sib_ticker.lower()}"
                )
            else:
                sib_url = ""
            groups.append({
                "type_code": type_code,
                "label":     label,
                # Store raw stored-outcomes here; _format_outcomes is
                # applied per-request by the /api/events formatter so
                # live WebSocket prices flow through without needing
                # to rebuild the get_data() cache.
                "_outcomes":     rec.get("outcomes", []),
                "event_ticker":  sib_ticker,
                "series_ticker": sib_series,
                "url":           sib_url,
            })
            if not is_primary:
                to_drop.add(rec.get("event_ticker"))
        # Only attach market_groups when there's more than just the
        # moneyline — otherwise there's nothing to tab between and
        # the frontend should render the card the normal way.
        if len(groups) > 1:
            primary["_market_groups"] = groups

    if not to_drop:
        return records
    return [r for r in records if r.get("event_ticker") not in to_drop]

# ── Sport sub-tabs ─────────────────────────────────────────────────────────────
SPORT_SUBTABS = {
"Basketball":[("NBA Games",["KXNBAGAME","KXNBASPREAD","KXNBATOTAL","KXNBATEAMTOTAL","KXNBA1HWINNER","KXNBA1HSPREAD","KXNBA1HTOTAL","KXNBA2HWINNER","KXNBA2D","KXNBA3D","KXNBA3PT","KXNBAPTS","KXNBAREB","KXNBAAST","KXNBABLK","KXNBASTL"]),("NBA Season",["KXNBA","KXNBAEAST","KXNBAWEST","KXNBAPLAYOFF","KXNBAPLAYIN","KXNBAATLANTIC","KXNBACENTRAL","KXNBASOUTHEAST","KXNBANORTHWEST","KXNBAPACIFIC","KXNBASOUTHWEST","KXNBAEAST1SEED","KXNBAWEST1SEED","KXTEAMSINNBAF","KXTEAMSINNBAEF","KXTEAMSINNBAWF","KXNBAMATCHUP","KXNBAWINS","KXRECORDNBABEST"]),("NBA Awards",["KXNBAMVP","KXNBAROY","KXNBACOY","KXNBADPOY","KXNBASIXTH","KXNBAMIMP","KXNBACLUTCH","KXNBAFINMVP","KXNBAWFINMVP","KXNBAEFINMVP","KXNBA1STTEAM","KXNBA2NDTEAM","KXNBA3RDTEAM","KXNBA1STTEAMDEF","KXNBA2NDTEAMDEF"]),("NBA Stats",["KXLEADERNBAPTS","KXLEADERNBAREB","KXLEADERNBAAST","KXLEADERNBABLK","KXLEADERNBASTL","KXLEADERNBA3PT"]),("NBA Draft",["KXNBADRAFT1","KXNBADRAFTPICK","KXNBADRAFTTOP","KXNBADRAFTCAT","KXNBADRAFTCOMP","KXNBATOPPICK","KXNBALOTTERYODDS","KXNBATOP5ROTY"]),("NBA Other",["KXNBATEAM","KXNBASEATTLE","KXCITYNBAEXPAND","KXSONICS","KXNEXTTEAMNBA","KXLBJRETIRE","KXSPORTSOWNERLBJ","KXSTEPHDEAL","KXQUADRUPLEDOUBLE","KXSHAI20PTREC","KXNBA2KCOVER"]),("WNBA",["KXWNBADRAFT1","KXWNBADRAFTTOP3","KXWNBADELAY","KXWNBAGAMESPLAYED"]),("NCAAB",["KXMARMAD","KXNCAAMBNEXTCOACH"]),("International",["KXEUROLEAGUEGAME","KXEUROLEAGUESPREAD","KXEUROLEAGUETOTAL","KXBSLGAME","KXBSLSPREAD","KXBSLTOTAL","KXBBLGAME","KXBBLSPREAD","KXBBLTOTAL","KXACBGAME","KXACBSPREAD","KXACBTOTAL","KXISLGAME","KXISLSPREAD","KXISLTOTAL","KXABAGAME","KXABASPREAD","KXABATOTAL","KXCBAGAME","KXCBASPREAD","KXCBATOTAL","KXBBSERIEAGAME","KXBBSERIEASPREAD","KXBBSERIEATOTAL","KXJBLEAGUEGAME","KXJBLEAGUESPREAD","KXJBLEAGUETOTAL","KXLNBELITEGAME","KXLNBELITESPREAD","KXLNBELITETOTAL","KXARGLNBGAME","KXARGLNBSPREAD","KXARGLNBTOTAL","KXVTBGAME","KXVTBSPREAD","KXVTBTOTAL"]),],
"Baseball":[("MLB Games",["KXMLBGAME","KXMLBRFI","KXMLBSPREAD","KXMLBTOTAL","KXMLBTEAMTOTAL","KXMLBF5","KXMLBF5SPREAD","KXMLBF5TOTAL","KXMLBHIT","KXMLBHR","KXMLBHRR","KXMLBKS","KXMLBTB"]),("MLB Season",["KXMLB","KXMLBAL","KXMLBNL","KXMLBALEAST","KXMLBALWEST","KXMLBALCENT","KXMLBNLEAST","KXMLBNLWEST","KXMLBNLCENT","KXMLBPLAYOFFS","KXTEAMSINWS","KXMLBBESTRECORD","KXMLBWORSTRECORD","KXMLBLSTREAK","KXMLBWSTREAK"]),("MLB Awards",["KXMLBALMVP","KXMLBNLMVP","KXMLBALCY","KXMLBNLCY","KXMLBALROTY","KXMLBNLROTY","KXMLBEOTY","KXMLBALMOTY","KXMLBNLMOTY","KXMLBALHAARON","KXMLBNLHAARON","KXMLBALCPOTY","KXMLBNLCPOTY","KXMLBALRELOTY","KXMLBNLRELOTY"]),("MLB Stats",["KXMLBSTAT","KXMLBSTATCOUNT","KXMLBSEASONHR","KXLEADERMLBAVG","KXLEADERMLBDOUBLES","KXLEADERMLBERA","KXLEADERMLBHITS","KXLEADERMLBHR","KXLEADERMLBKS","KXLEADERMLBOPS","KXLEADERMLBRBI","KXLEADERMLBRUNS","KXLEADERMLBSTEALS","KXLEADERMLBTRIPLES","KXLEADERMLBWAR","KXLEADERMLBWINS"]),("MLB Other",["KXMLBTRADE","KXWSOPENTRANTS"]),("International",["KXNPBGAME","KXKBOGAME","KXNCAABBGAME"]),("NCAA",["KXNCAABASEBALL","KXNCAABBGS"]),],
"Football":[("NFL Games",["KXUFLGAME"]),("NFL Season",["KXSB","KXNFLPLAYOFF","KXNFLAFCCHAMP","KXNFLNFCCHAMP","KXNFLAFCEAST","KXNFLAFCWEST","KXNFLAFCNORTH","KXNFLAFCSOUTH","KXNFLNFCEAST","KXNFLNFCWEST","KXNFLNFCNORTH","KXNFLNFCSOUTH","KXRECORDNFLBEST","KXRECORDNFLWORST"]),("NFL Awards",["KXNFLMVP","KXNFLOPOTY","KXNFLDPOTY","KXNFLOROTY","KXNFLDROTY","KXNFLCOTY"]),("NFL Draft",["KXNFLDRAFT1","KXNFLDRAFT1ST","KXNFLDRAFTPICK","KXNFLDRAFTTOP","KXNFLDRAFTWR","KXNFLDRAFTDB","KXNFLDRAFTTE","KXNFLDRAFTQB","KXNFLDRAFTOL","KXNFLDRAFTEDGE","KXNFLDRAFTLB","KXNFLDRAFTRB","KXNFLDRAFTDT","KXNFLDRAFTTEAM"]),("NFL Stats",["KXLEADERNFLSACKS","KXLEADERNFLINT","KXLEADERNFLPINT","KXLEADERNFLPTDS","KXLEADERNFLPYDS","KXLEADERNFLRTDS","KXLEADERNFLRUSHTDS","KXLEADERNFLRUSHYDS","KXLEADERNFLRYDS","KXNFLTEAM1POS","KXNFLPRIMETIME"]),("NFL Other",["KXNFLTRADE","KXNEXTTEAMNFL","KXKELCERETIRE","KXSTARTINGQBWEEK1","KXCOACHOUTNFL","KXCOACHOUTNCAAFB","KXARODGRETIRE","KXRELOCATIONCHI","KX1STHOMEGAME","KXSORONDO","KXDONATEMRBEAST"]),("NCAAF",["KXNCAAF","KXHEISMAN","KXNCAAFCONF","KXNCAAFACC","KXNCAAFB10","KXNCAAFB12","KXNCAAFSEC","KXNCAAFAAC","KXNCAAFSBELT","KXNCAAFMWC","KXNCAAFMAC","KXNCAAFCUSA","KXNCAAFPAC12","KXNCAAFPLAYOFF","KXNCAAFFINALIST","KXNCAAFUNDEFEATED","KXNCAAFCOTY","KXNCAAFAPRANK"]),("Other",["KXNDJOINCONF","KXCOVEREA"]),],
"Hockey":[("NHL Games",["KXNHLGAME","KXNHLSPREAD","KXNHLTOTAL"]),("NHL Season",["KXNHL","KXNHLPLAYOFF","KXTEAMSINSC","KXNHLPRES","KXNHLEAST","KXNHLWEST","KXNHLADAMS","KXNHLCENTRAL","KXNHLATLANTIC","KXNHLMETROPOLITAN","KXNHLPACIFIC"]),("NHL Awards",["KXNHLHART","KXNHLNORRIS","KXNHLVEZINA","KXNHLCALDER","KXNHLROSS","KXNHLRICHARD"]),("AHL",["KXAHLGAME"]),("International",["KXKHLGAME","KXSHLGAME","KXLIIGAGAME","KXELHGAME","KXNLGAME","KXDELGAME"]),("Other",["KXCANADACUP","KXNCAAHOCKEY","KXNCAAHOCKEYGAME"]),],
"Tennis":[("ATP Matches",["KXATPMATCH","KXATPSETWINNER","KXATPCHALLENGERMATCH","KXMCMMEN","KXFOMEN"]),("WTA Matches",["KXWTAMATCH","KXFOWOMEN"]),("Grand Slams",["KXGRANDSLAM","KXATPGRANDSLAM","KXWTAGRANDSLAM","KXATPGRANDSLAMFIELD","KXGRANDSLAMJFONSECA"]),("Rankings",["KXATP1RANK"]),("Other",["KXWTASERENA","KXGOLFTENNISMAJORS"]),],
"Golf":[("Tour Events",["KXPGATOUR","KXPGAH2H","KXPGA3BALL","KXPGA5BALL","KXPGAR1LEAD","KXPGAR1TOP5","KXPGAR1TOP10","KXPGAR1TOP20","KXPGAR2LEAD","KXPGAR2TOP5","KXPGAR2TOP10","KXPGAR3LEAD","KXPGAR3TOP5","KXPGAR3TOP10","KXPGATOP5","KXPGATOP10","KXPGATOP20","KXPGATOP40","KXPGAPLAYOFF","KXPGACUTLINE","KXPGAMAKECUT","KXPGAAGECUT","KXPGAWINNERREGION","KXPGALOWSCORE","KXPGASTROKEMARGIN","KXPGAWINNINGSCORE","KXPGAPLAYERCAT","KXPGABIRDIES","KXPGAROUNDSCORE","KXPGAEAGLE","KXPGAHOLEINONE","KXPGABOGEYFREE","KXPGAMASTERS"]),("Majors",["KXPGAMAJORTOP10","KXPGAMAJORWIN","KXGOLFMAJORS"]),("Ryder Cup",["KXPGARYDER","KXPGASOLHEIM","KXRYDERCUPCAPTAIN"]),("Player Props",["KXPGACURRY","KXPGATIGER","KXBRYSONCOURSERECORDS","KXSCOTTIESLAM","KXGOLFTENNISMAJORS"]),],
"MMA":[("UFC Fights",["KXUFCFIGHT"]),("UFC Titles",["KXUFCHEAVYWEIGHTTITLE","KXUFCLHEAVYWEIGHTTITLE","KXUFCMIDDLEWEIGHTTITLE","KXUFCWELTERWEIGHTTITLE","KXUFCLIGHTWEIGHTTITLE","KXUFCFEATHERWEIGHTTITLE","KXUFCBANTAMWEIGHTTITLE","KXUFCFLYWEIGHTTITLE"]),("UFC Other",["KXMCGREGORFIGHTNEXT","KXCARDPRESENCEUFCWH","KXUFCWHITEHOUSE"]),],
"Cricket":[("IPL",["KXIPLGAME","KXIPL","KXIPLFOUR","KXIPLSIX","KXIPLTEAMTOTAL"]),("PSL",["KXPSLGAME","KXPSL"]),("Other",["KXT20MATCH"]),],
"Esports":[("Valorant",["KXVALORANTMAP","KXVALORANTGAME"]),("League of Legends",["KXLOLGAME","KXLOLMAP","KXLOLTOTALMAPS"]),("CS2",["KXCS2GAME","KXCS2MAP","KXCS2TOTALMAPS"]),("Rainbow Six",["KXR6GAME","KXR6MAP"]),("Dota 2",["KXDOTA2GAME","KXDOTA2MAP"]),("Overwatch",["KXOWGAME"]),],
"Motorsport":[("F1",["KXF1RACE","KXF1RACEPODIUM","KXF1TOP5","KXF1TOP10","KXF1FASTLAP","KXF1CONSTRUCTORS","KXF1RETIRE","KXF1","KXF1OCCUR","KXF1CHINA"]),("NASCAR",["KXNASCARCUPSERIES","KXNASCARRACE","KXNASCARTOP3","KXNASCARTOP5","KXNASCARTOP10","KXNASCARTOP20","KXNASCARTRUCKSERIES","KXNASCARAUTOPARTSSERIES"]),("MotoGP",["KXMOTOGP","KXMOTOGPTEAMS"]),("IndyCar",["KXINDYCARSERIES"]),],
"Boxing":[("Fights",["KXBOXING","KXFLOYDTYSONFIGHT"]),("WBC Titles",["KXWBCHEAVYWEIGHTTITLE","KXWBCCRUISERWEIGHTTITLE","KXWBCMIDDLEWEIGHTTITLE","KXWBCWELTERWEIGHTTITLE","KXWBCLIGHTWEIGHTTITLE","KXWBCFEATHERWEIGHTTITLE","KXWBCBANTAMWEIGHTTITLE","KXWBCFLYWEIGHTTITLE"]),],
"Rugby":[("NRL",["KXRUGBYNRLMATCH","KXNRLCHAMP"]),("Premiership",["KXPREMCHAMP"]),("Super League",["KXSLRCHAMP"]),("Top 14",["KXFRA14CHAMP"]),],
"Lacrosse":[("NCAA",["KXNCAAMLAXGAME","KXNCAALAXFINAL"]),("Awards",["KXLAXTEWAARATON"]),],
"Chess":[("World Championship",["KXCHESSWORLDCHAMPION"]),("Candidates",["KXCHESSCANDIDATES"]),],
"Darts":[("Matches",["KXDARTSMATCH"]),("Premier League",["KXPREMDARTS"]),],
"Aussie Rules":[("AFL",["KXAFLGAME"]),],
"Other Sports":[("Sailing",["KXSAILGP"]),("Other",["KXPIZZASCORE9","KXROCKANDROLLHALLOFFAME","KXEUROVISIONISRAELBAN","KXCOLLEGEGAMEDAYGUEST","KXWSOPENTRANTS"]),],
}

SERIES_TO_SUBTAB = {}
for _sp, _tabs in SPORT_SUBTABS.items():
    SERIES_TO_SUBTAB[_sp] = {}
    for _tab_name, _series_list in _tabs:
        for _s in _series_list:
            SERIES_TO_SUBTAB[_sp][_s] = _tab_name


# ── Date helpers ───────────────────────────────────────────────────────────────
def safe_dt(val):
    """Parse a datetime from whatever Kalshi sends us into a UTC-aware
    datetime. Tolerates multiple ISO 8601 variations (with/without Z,
    microseconds, offsets) and falls back to strptime with common
    formats. Returns None for anything unparseable."""
    if val is None:
        return None
    # Already a datetime-ish object.
    if hasattr(val, "astimezone"):
        try:
            if val.tzinfo is None:
                val = val.replace(tzinfo=UTC)
            return val.astimezone(UTC)
        except Exception:
            return None
    if not isinstance(val, str):
        return None
    s = val.strip()
    if not s or s in ("NaT", "None", "nan"):
        return None
    from datetime import datetime as _dt
    # Try fromisoformat first on the raw string (Py 3.11+ handles Z
    # and most variants directly), then on a Z→+00:00 normalized form.
    candidates = [s]
    if s.endswith("Z"):
        candidates.append(s[:-1] + "+00:00")
    for candidate in candidates:
        try:
            dt = _dt.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            pass
    # strptime fallback for anything fromisoformat chokes on.
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = _dt.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            continue
    return None

def parse_game_date_from_ticker(event_ticker: str):
    import re
    from datetime import date as _date
    MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    try:
        parts = event_ticker.split("-")
        if len(parts) < 2: return None
        seg = parts[1]
        m = re.match(r"(\d{2})([A-Z]{3})(\d{2})", seg)
        if not m: return None
        yy, mon, dd = m.group(1), m.group(2), m.group(3)
        yr = 2000 + int(yy)
        mo = MONTHS.get(mon)
        if not mo: return None
        return _date(yr, mo, int(dd))
    except: return None

def fmt_date(d):
    from datetime import datetime, date as _date
    try:
        if d is None: return ""
        if hasattr(d, 'hour'):
            try:
                import pytz
                eastern = pytz.timezone('US/Eastern')
            except ImportError:
                from zoneinfo import ZoneInfo
                eastern = ZoneInfo('America/New_York')
            if d.tzinfo:
                d = d.astimezone(eastern)
            tz_label = d.strftime('%Z') or "ET"
            hour = d.hour % 12 or 12
            ampm = "am" if d.hour < 12 else "pm"
            return f"{d.strftime('%b')} {d.day}, {hour}:{d.strftime('%M')}{ampm} {tz_label}"
        return d.strftime("%b %-d")
    except:
        try: return d.strftime("%b %-d") if d else ""
        except: return ""

# ── Kalshi client ──────────────────────────────────────────────────────────────
_client = None

def get_client():
    global _client
    if _client: return _client
    from kalshi_python_sync import Configuration, KalshiClient
    key_id  = os.environ["KALSHI_API_KEY_ID"]
    key_str = os.environ["KALSHI_PRIVATE_KEY"]
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
        f.write(key_str); pem = f.name
    cfg = Configuration()
    cfg.api_key_id = key_id
    cfg.private_key_pem_path = pem
    _client = KalshiClient(cfg)
    return _client

def paginate(with_markets=False, max_pages=30):
    client = get_client()
    events = []
    seen = set()
    # Fetch both open and closed to include live/in-progress games
    for status in ["open", "closed"]:
        cursor = None
        for _ in range(max_pages):
            try:
                kw = {"limit":200,"status":status}
                if with_markets: kw["with_nested_markets"] = True
                if cursor: kw["cursor"] = cursor
                resp  = client.get_events(**kw).to_dict()
                batch = resp.get("events",[])
                if not batch: break
                for ev in batch:
                    eid = ev.get("event_ticker","")
                    if eid not in seen:
                        seen.add(eid)
                        events.append(ev)
                cursor = resp.get("cursor") or resp.get("next_cursor")
                if not cursor: break
                time.sleep(0.05)
            except Exception as e:
                if "429" in str(e): time.sleep(3)
                else: break
    return events

# ── Price helpers ──────────────────────────────────────────────────────────────
def _cents_from(mk, dollars_key, cents_key):
    """Read a Kalshi market-dict price into integer cents, accepting
    either the *_dollars decimal or the raw cents field."""
    v = mk.get(dollars_key)
    if v is not None:
        try: return float(v) * 100
        except: pass
    v = mk.get(cents_key)
    if v is not None:
        try: return float(v)
        except: pass
    return None


def _midprice_and_ask(yb, ya, nb, na):
    """Given bid/ask in cents for YES and NO, return (chance, yes, no)
    cents. Chance is the midprice between yes bid and yes ask (what
    Kalshi displays as the implied chance %). YES/NO prices are the
    asks (what you'd pay to buy), falling back to bids if no ask is
    quoted. Any side may be None."""
    if yb is not None and ya is not None:
        chance_c = (yb + ya) / 2
    elif yb is not None and nb is not None:
        chance_c = (yb + (100 - nb)) / 2
    elif ya is not None and na is not None:
        chance_c = ((100 - na) + ya) / 2
    elif ya is not None:
        chance_c = ya
    elif yb is not None:
        chance_c = yb
    elif nb is not None:
        chance_c = 100 - nb
    elif na is not None:
        chance_c = 100 - na
    else:
        chance_c = None
    yes_c = ya if ya is not None else yb
    no_c  = na if na is not None else nb
    return chance_c, yes_c, no_c


def _format_outcomes(stored_outcomes):
    """Turn stored raw-cents outcomes into display-ready outcomes,
    overlaying live WebSocket prices from LIVE_PRICES where
    available. Markets with no real liquidity (zero size on both
    yes-side and no-side) show — instead of a computed mid-price,
    matching how Kalshi's own UI renders illiquid markets. For
    markets with ≥5 outcomes the list is sorted by chance
    descending so the top 5 shown by default are the most likely
    results; shorter markets (binary yes/no, 3-way home/draw/away,
    etc.) preserve Kalshi's natural insertion order so row
    positions stay stable across live updates."""
    try:
        from kalshi_ws import LIVE_PRICES
    except Exception:
        LIVE_PRICES = {}
    tmp = []
    for o in stored_outcomes:
        tk = o.get("ticker", "")
        yb = o.get("_yb")
        ya = o.get("_ya")
        nb = o.get("_nb")
        na = o.get("_na")
        live = LIVE_PRICES.get(tk) if tk else None
        if live:
            if live.get("yes_bid") is not None: yb = live["yes_bid"]
            if live.get("yes_ask") is not None: ya = live["yes_ask"]
            if live.get("no_bid")  is not None: nb = live["no_bid"]
            if live.get("no_ask")  is not None: na = live["no_ask"]
        # Liquidity check. A market is treated as "dead" (shown as
        # — in all three columns, matching Kalshi's own --% render)
        # only when the order book is NOT two-sided. Concretely: we
        # need both a real bid and a real ask for the outcome to be
        # priceable. Note that a YES bid is the same order as a NO
        # ask (both are "buy YES / sell NO") so either side counts.
        #
        # This rule correctly handles three important cases:
        #   - Pregame Blackburn (bid=13¢/ask=18¢, vol=0, oi=0) →
        #     both sides present → LIVE. The old "vol=0 AND oi=0"
        #     rule wrongly hid these pregame MM quotes.
        #   - Stale Real Madrid WCL lone 80¢ ask against empty bid
        #     → bid side empty → DEAD. No fake 40% midprice.
        #   - Market with last_price > 0 (traded historically) →
        #     handled below — last_price overrides midprice even
        #     when the current book has gone one-sided.
        yb_sz = o.get("_yb_sz") or 0
        ya_sz = o.get("_ya_sz") or 0
        nb_sz = o.get("_nb_sz") or 0
        na_sz = o.get("_na_sz") or 0
        # YES-buy = NO-sell; YES-sell = NO-buy.
        bid_side = (yb_sz > 0) or (na_sz > 0)
        ask_side = (ya_sz > 0) or (nb_sz > 0)
        two_sided = bid_side and ask_side
        last = o.get("_last")
        if live and live.get("last_price") is not None:
            last = live["last_price"]
        has_last = last is not None and last > 0
        if two_sided:
            chance_c, yes_c, no_c = _midprice_and_ask(yb, ya, nb, na)
            # For WIDE spreads (e.g., 3¢/84¢ → midpoint 43%), the
            # last-traded price is more informative than the midpoint.
            # For tight spreads (e.g., 78¢/79¢), the midpoint IS the
            # market price and overriding with a stale last-trade would
            # cause visible bouncing. Only override when spread > 10¢.
            if has_last and chance_c is not None:
                spread_width = abs((ya or 0) - (yb or 0))
                if spread_width > 10:
                    chance_c = last
        elif has_last:
            # One-sided book but we have a historical trade price —
            # show last_price as the chance (what Kalshi's card
            # shows). YES/NO cells fall back to whatever quotes
            # still exist; either may be —.
            chance_c = last
            yes_c = ya if ya is not None else yb
            no_c  = na if na is not None else nb
        else:
            chance_c = yes_c = no_c = None
        tmp.append((chance_c, {
            "label":  o.get("label", ""),
            "ticker": tk,
            "chance": f"{int(round(chance_c))}%" if chance_c is not None else "—",
            "yes":    f"{int(round(yes_c))}¢"    if yes_c    is not None else "—",
            "no":     f"{int(round(no_c))}¢"     if no_c     is not None else "—",
        }))
    # Only sort long cards. Short cards (binary / 3-way) keep the
    # Kalshi API insertion order so row positions don't jitter on
    # live updates and users can anchor to a specific outcome.
    if len(tmp) >= 5:
        tmp.sort(key=lambda pair: (pair[0] is None, -(pair[0] or 0)))
    return [item for _, item in tmp]


# ── Cache with TTL ─────────────────────────────────────────────────────────────
# Stale-while-revalidate strategy:
#   - Cache valid for CACHE_TTL seconds → return as-is.
#   - Cache within (CACHE_TTL, CACHE_STALE_TTL) → return stale data
#     immediately AND kick off a background rebuild so the next
#     caller gets fresh data.
#   - Cache older than CACHE_STALE_TTL (or never built) → block the
#     caller on a synchronous rebuild. This only happens on the very
#     first request after container startup.
_cache = {"data": None, "ts": 0}  # cache cleared on startup
CACHE_TTL = 300            # 5 min — fresh
CACHE_STALE_TTL = 1800     # 30 min — hard expiry, beyond this we block
_rebuild_lock = threading.Lock()
_rebuilding = {"active": False}


# ── Per-market response cache ─────────────────────────────────────
# Short-TTL in-memory cache for expensive per-ticker endpoints
# (orderbook, trades). When 10 users view the same market at the
# same time, we hit Kalshi once and fan the result out to all 10.
# Prevents hammering Kalshi's signed API and cuts response times
# from 300-1500 ms (round-trip to Kalshi) to <5 ms (dict lookup).
_mk_cache = {}            # key -> (expires_ts, value)
_mk_cache_locks = {}      # key -> threading.Lock()
_mk_cache_meta_lock = threading.Lock()


def _mk_cache_get(key):
    entry = _mk_cache.get(key)
    if entry is None:
        return None
    expires, value = entry
    if expires <= time.time():
        return None
    return value


def _mk_cache_set(key, value, ttl_seconds):
    _mk_cache[key] = (time.time() + ttl_seconds, value)
    # Soft cap to keep memory bounded on long-running processes —
    # evict expired entries when the dict grows beyond 2000 keys.
    if len(_mk_cache) > 2000:
        now = time.time()
        for k in list(_mk_cache.keys()):
            exp, _ = _mk_cache[k]
            if exp <= now:
                _mk_cache.pop(k, None)


def _mk_cache_lock_for(key):
    """Per-key lock so a cache miss serializes concurrent requests
    for the same ticker. First caller fetches from Kalshi; everyone
    else waits a few ms and pulls from the now-populated cache."""
    with _mk_cache_meta_lock:
        lock = _mk_cache_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _mk_cache_locks[key] = lock
    return lock


def _rebuild_cache_async():
    """Kick off a non-blocking cache rebuild if one isn't already
    in progress. Uses a lock so concurrent stale requests don't each
    fire their own rebuild."""
    if _rebuilding["active"]:
        return
    if not _rebuild_lock.acquire(blocking=False):
        return
    def _worker():
        try:
            _rebuilding["active"] = True
            _build_cache()
        except Exception as e:
            logging.getLogger("stochverse").error("cache rebuild failed: %s", e)
        finally:
            _rebuilding["active"] = False
            try:
                _rebuild_lock.release()
            except RuntimeError:
                pass
    threading.Thread(target=_worker, daemon=True).start()


def get_data():
    global _cache
    now = time.time()
    age = now - _cache.get("ts", 0)
    have_cache = _cache.get("data") is not None
    # Hot cache — return immediately.
    if have_cache and age < CACHE_TTL:
        return _cache["data"]
    # Warm (stale) cache — serve stale, rebuild in the background.
    # Users never wait for the 20-40s Kalshi fetch during normal use.
    if have_cache and age < CACHE_STALE_TTL:
        _rebuild_cache_async()
        return _cache["data"]
    # Cold or too stale. Serialize rebuilds on the same lock the
    # async path uses so concurrent cold-start requests (the first
    # user, the startup-priming thread, etc.) never duplicate the
    # 20-40 s Kalshi fetch. acquire(blocking=True, timeout=60) makes
    # the first caller build and every other caller wait on the
    # same in-progress build, then all return with fresh data.
    acquired = _rebuild_lock.acquire(timeout=60)
    if not acquired:
        # Build took too long — give up and return whatever we have
        # (likely empty). Caller can retry; we don't want to hang.
        return _cache.get("data") or []
    try:
        # Double-check after acquiring: a previous waiter may have
        # just finished building, in which case we're done.
        age = time.time() - _cache.get("ts", 0)
        have_cache = _cache.get("data") is not None
        if have_cache and age < CACHE_TTL:
            return _cache["data"]
        _rebuilding["active"] = True
        try:
            _build_cache()
        finally:
            _rebuilding["active"] = False
    finally:
        try:
            _rebuild_lock.release()
        except RuntimeError:
            pass
    return _cache.get("data") or []


def _build_cache():
    """Synchronously rebuild the in-memory snapshot from Kalshi.
    Previously the body of get_data(); extracted so it can be
    invoked either inline (cold cache) or from a background thread
    (stale-while-revalidate)."""
    global _cache

    all_ev = paginate(with_markets=True, max_pages=50)
    if not all_ev:
        return

    # ── Auto-discover sibling series ────────────────────────────
    # Scan the live Kalshi data for series tickers that match a
    # known {prefix}{SUFFIX} pattern (e.g. KXEUROLEAGUESPREAD)
    # where the parent GAME/MATCH is already registered. Register
    # any new siblings in GAME_MARKET_PREFIXES dynamically so
    # _group_game_markets can collapse them into tabbed cards.
    # Also inherit the parent's sport classification so they show
    # the correct sport label instead of falling back to entity
    # matching (which can misclassify basketball as soccer when
    # team names overlap).
    _suffix_map = {s[0]: (s[1], s[2], s[3]) for s in _SIBLING_SUFFIXES}
    _live_series = set()
    for ev in all_ev:
        s = str(ev.get("series_ticker") or "").upper()
        if s:
            _live_series.add(s)
    # Build a reverse map: prefix → (parent_series, sport_name).
    _prefix_to_parent = {}
    for parent_series, meta in GAME_MARKET_PREFIXES.items():
        if meta[3]:  # is_primary
            for _psuffix in ("GAME", "MATCH"):
                if parent_series.endswith(_psuffix):
                    pfx = parent_series[:-len(_psuffix)]
                    sport = get_sport(parent_series)
                    _prefix_to_parent[pfx] = (parent_series, sport)
                    break
    _auto_registered = 0
    for series in _live_series:
        if series in GAME_MARKET_PREFIXES:
            continue
        for suffix, (tc, lbl, pri) in _suffix_map.items():
            if series.endswith(suffix):
                pfx = series[:-len(suffix)]
                parent_info = _prefix_to_parent.get(pfx)
                if parent_info:
                    GAME_MARKET_PREFIXES[series] = (tc, lbl, pri, False)
                    # Inherit sport from parent so classification
                    # doesn't fall through to entity matching.
                    if parent_info[1] and series not in _all_series:
                        _all_series.add(series)
                        sport_name = parent_info[1]
                        if sport_name in _SPORT_SERIES:
                            _SPORT_SERIES[sport_name].append(series)
                        _auto_registered += 1
                    break
    if _auto_registered:
        logging.getLogger("stochverse").info(
            "auto-registered %d sibling series from live data",
            _auto_registered,
        )

    # ── Auto-infer GAME/MATCH variants of known base series ───
    # Kalshi uses two variants for most leagues:
    #   - KXEPL        → season-long EPL markets
    #   - KXEPLGAME    → per-match EPL fixtures
    # Our _SPORT_SERIES and SOCCER_COMP mappings historically had
    # gaps where one variant was registered but the other wasn't,
    # causing events to fall through without sport/league tags.
    # This block closes any remaining gaps at runtime by scanning
    # live data for {base}GAME/{base}MATCH variants whose base is
    # known, and registering the GAME/MATCH variant (and vice versa)
    # with the same sport + subcat inherited from the base.
    _inferred_primary = 0
    # Build reverse index: series → sport label.
    _series_to_sport = {}
    for _sp_name, _sl2 in _SPORT_SERIES.items():
        for _s in _sl2:
            _series_to_sport[_s.upper()] = _sp_name
    # Soccer subcategory (league display name) lookup.
    global SOCCER_COMP
    for series in _live_series:
        up = series.upper()
        if up in _series_to_sport:
            continue  # already classified
        # Try stripping GAME/MATCH → is the base registered?
        for _psuffix in ("GAME", "MATCH"):
            if up.endswith(_psuffix):
                base = up[:-len(_psuffix)]
                if base in _series_to_sport:
                    sport_name = _series_to_sport[base]
                    if sport_name in _SPORT_SERIES:
                        _SPORT_SERIES[sport_name].append(up)
                        _series_to_sport[up] = sport_name
                        _inferred_primary += 1
                    # Inherit soccer subcat too.
                    if base in SOCCER_COMP and up not in SOCCER_COMP:
                        SOCCER_COMP[up] = SOCCER_COMP[base]
                    break
        else:
            # No GAME/MATCH suffix — check if a {series}GAME or
            # {series}MATCH variant is in our known set. If so,
            # inherit from there (handles "KXFOO → KXFOOGAME" gap).
            for _psuffix in ("GAME", "MATCH"):
                candidate = up + _psuffix
                if candidate in _series_to_sport:
                    sport_name = _series_to_sport[candidate]
                    if sport_name in _SPORT_SERIES:
                        _SPORT_SERIES[sport_name].append(up)
                        _series_to_sport[up] = sport_name
                        _inferred_primary += 1
                    if candidate in SOCCER_COMP and up not in SOCCER_COMP:
                        SOCCER_COMP[up] = SOCCER_COMP[candidate]
                    break
    if _inferred_primary:
        logging.getLogger("stochverse").info(
            "auto-inferred %d primary series from live data",
            _inferred_primary,
        )

    # Rough "exp_dt − kickoff" window per sport. Kalshi's
    # expected_expiration_time is set to the final-whistle + some
    # settlement buffer, so these values are slightly longer than
    # real game length. Used only when ESPN/SofaScore don't provide
    # a matched _live_state for the event — once we have real-time
    # data from a feed, isLive() trusts that directly.
    DURATION = {
        "Soccer": timedelta(hours=3),
        "Baseball": timedelta(hours=3, minutes=30),
        "Basketball": timedelta(hours=3),
        "Hockey": timedelta(hours=2, minutes=45),
        "Football": timedelta(hours=3, minutes=45),
        "Cricket": timedelta(hours=4),
        "Tennis": timedelta(hours=3),
        "Golf": timedelta(hours=4),
        "MMA": timedelta(hours=3),
        "Esports": timedelta(hours=2),
        "Motorsport": timedelta(hours=3),
        "Rugby": timedelta(hours=2, minutes=30),
    }

    def extract(row):
        mkts = row.get("markets")
        if not isinstance(mkts, list) or not mkts:
            return None, None, None, None, None, "", []
        first_mk = mkts[0]
        event_ticker = str(row.get("event_ticker",""))
        sport = str(row.get("_sport",""))
        game_date = parse_game_date_from_ticker(event_ticker)
        exp_dt   = safe_dt(first_mk.get("expected_expiration_time"))
        close_dt = safe_dt(first_mk.get("close_time"))
        open_dt  = safe_dt(first_mk.get("open_time"))
        kickoff_dt = None
        if game_date and sport and sport in DURATION:
            # exp_dt = game_end time on Kalshi. Subtract duration to get kickoff.
            if exp_dt and abs((exp_dt.date() - game_date).days) <= 2:
                kickoff_dt = exp_dt - DURATION[sport]

        sort_dt = game_date if game_date else (exp_dt.date() if exp_dt else (close_dt.date() if close_dt else None))
        # Precise sort timestamp: prefer the kickoff time we computed, then
        # the market's expected expiration / close time, and finally fall back
        # to the game date at UTC midnight. Used by the earliest/latest sort.
        if kickoff_dt:
            sort_ts_dt = kickoff_dt
        elif exp_dt:
            sort_ts_dt = exp_dt
        elif close_dt:
            sort_ts_dt = close_dt
        elif game_date:
            from datetime import datetime as _datetime
            sort_ts_dt = _datetime(game_date.year, game_date.month, game_date.day, tzinfo=UTC)
        else:
            sort_ts_dt = None
        outcomes = []
        for mk in mkts:
            # Skip markets that have already settled — result=yes or
            # result=no means the outcome is definitively resolved
            # (team eliminated, player scored more than X, etc.), and
            # status=finalized/closed means trading is over. These
            # markets still appear in Kalshi's API response with stale
            # yes_bid/yes_ask from their last tradable moment, so we
            # need to drop them explicitly — otherwise eliminated
            # Women's CL teams or out-of-contention Golden Boot
            # players would keep showing old percentages.
            mk_result = str(mk.get("result") or "").lower()
            mk_status = str(mk.get("status") or "").lower()
            if mk_result in ("yes", "no"):
                continue
            if mk_status in ("finalized", "settled", "determined"):
                continue
            label = str(mk.get("yes_sub_title") or "").strip()
            if not label:
                t = str(mk.get("ticker") or "")
                parts = t.rsplit("-", 1)
                label = parts[-1] if len(parts) > 1 else t
            yb = _cents_from(mk, "yes_bid_dollars", "yes_bid")
            ya = _cents_from(mk, "yes_ask_dollars", "yes_ask")
            nb = _cents_from(mk, "no_bid_dollars",  "no_bid")
            na = _cents_from(mk, "no_ask_dollars",  "no_ask")
            last_price = _cents_from(mk, "last_price_dollars", "last_price")
            # Raw liquidity sizes — used by _format_outcomes to
            # recognize "no real market" cases (both sides have zero
            # orders) and show — instead of computing a garbage
            # midprice from Kalshi's (0, 100) placeholder values.
            def _sz(key):
                v = mk.get(key)
                try:
                    return float(v) if v is not None else 0.0
                except Exception:
                    return 0.0
            yb_size = _sz("yes_bid_size_fp")
            ya_size = _sz("yes_ask_size_fp")
            nb_size = _sz("no_bid_size_fp")
            na_size = _sz("no_ask_size_fp")
            volume = _sz("volume_fp")
            open_interest = _sz("open_interest_fp")
            volume_24h = _sz("volume_24h_fp")
            liquidity = _sz("liquidity_dollars")
            prev_price = _cents_from(mk, "previous_price_dollars", None)
            # Store raw cents + market ticker. The chance/yes/no display
            # strings are computed per-request by _format_outcomes() so
            # live WebSocket updates flow through without rebuilding the
            # REST snapshot cache.
            outcomes.append({
                "label":  label[:35],
                "ticker": str(mk.get("ticker","")),
                "_yb": yb, "_ya": ya, "_nb": nb, "_na": na,
                "_yb_sz": yb_size, "_ya_sz": ya_size,
                "_nb_sz": nb_size, "_na_sz": na_size,
                "_vol":  volume,
                "_oi":   open_interest,
                "_last": last_price,
                "_vol24h": volume_24h,
                "_liq": liquidity,
                "_prev": prev_price,
                # Settlement rules — keep the full Kalshi text so the
                # detail page can render the "How this settles"
                # section. Cap at 4000 chars as a safety net; typical
                # rules_primary is ~150-400 chars, rules_secondary
                # ~300-800 chars.
                "_rules": str(mk.get("rules_primary") or "")[:4000],
                "_rules_secondary": str(mk.get("rules_secondary") or "")[:4000],
                "_early_close_condition": str(mk.get("early_close_condition") or "")[:1000],
                "_open_time": str(mk.get("open_time") or ""),
                "_market_close": str(mk.get("close_time") or ""),
                "_price_ranges": mk.get("price_ranges"),
            })
        # Show date+time if we have kickoff, otherwise just date
        if kickoff_dt and game_date:
            try:
                import pytz as _pytz
                eastern = _pytz.timezone("US/Eastern")
                kt = kickoff_dt.astimezone(eastern)
                hour = kt.hour % 12 or 12
                ampm = "am" if kt.hour < 12 else "pm"
                tz_label = kt.strftime("%Z")
                # Use Eastern date (kt) not UTC game_date to avoid off-by-one at midnight
                display = f"{kt.strftime('%b')} {kt.day}, {hour}:{kt.strftime('%M')}{ampm} {tz_label}"
            except:
                display = game_date.strftime("%b %-d") if game_date else ""
        elif game_date:
            display = game_date.strftime("%b %-d")
        else:
            display = ""
        return sort_dt, sort_ts_dt, game_date, kickoff_dt, exp_dt, close_dt, display, outcomes

    records = []
    for ev in all_ev:
        try:
            # Derive fields that used to come from DataFrame columns.
            category = (ev.get("category") or "Other")
            if isinstance(category, str):
                category = category.strip() or "Other"
            else:
                category = "Other"
            series_ticker_raw = ev.get("series_ticker") or ""
            series = str(series_ticker_raw).upper()
            _sport = get_sport(series)
            # Fallback: if the hardcoded series_ticker map doesn't
            # know this prefix, try to infer the sport by matching
            # team aliases in the event title against our entities
            # table. Powers automatic classification of new Kalshi
            # sports/leagues without manual mapping updates.
            if not _sport and category == "Sports":
                try:
                    from db import get_sport_from_entities
                    _sport = get_sport_from_entities(ev.get("title") or "")
                except Exception:
                    pass
            _is_sport = bool(_sport)
            _soccer_comp = ""
            if _sport == "Soccer":
                _soccer_comp = SOCCER_COMP.get(series, "")
                if not _soccer_comp:
                    # Auto-generate league label from the series ticker
                    # for unknown soccer leagues. Strip KX prefix and
                    # GAME/MATCH suffix, then humanize. E.g.:
                    #   KXBOLPDIVGAME → BOLPDIV → "Bolpdiv"
                    # Also auto-register it in SOCCER_COMP so the nav
                    # subtab appears and future events use the same label.
                    base = series
                    for sfx in ("GAME", "MATCH", "1H", "SPREAD", "TOTAL", "BTTS"):
                        if base.endswith(sfx):
                            base = base[:-len(sfx)]
                            break
                    if base.startswith("KX"):
                        base = base[2:]
                    if base:
                        label = base.replace("_", " ").title()
                        SOCCER_COMP[series] = label
                        _soccer_comp = label
            mkts = ev.get("markets")
            if not isinstance(mkts, list):
                mkts = []
            # Stuff into the event dict so extract() can read them.
            ev["category"] = category
            ev["_sport"] = _sport
            ev["_is_sport"] = _is_sport
            ev["_soccer_comp"] = _soccer_comp
            ev["markets"] = mkts

            sort_dt, sort_ts_dt, game_date, kickoff_dt, game_end_dt, close_dt, display_dt, outcomes = extract(ev)

            if _sport == "Soccer" and _soccer_comp and _soccer_comp not in ("Other", ""):
                _subcat = _soccer_comp
            elif _sport and _sport != "Soccer":
                _subcat = SERIES_TO_SUBTAB.get(_sport, {}).get(series, "")
            else:
                _subcat = ""

            r = {
                "event_ticker": str(ev.get("event_ticker", "")),
                "title": str(ev.get("title", ""))[:200],
                "category": category,
                "series_ticker": str(series_ticker_raw),
                "_sport": _sport,
                "_soccer_comp": _soccer_comp if _soccer_comp != "Other" else "",
                "_subcat": _subcat,
                "_is_sport": _is_sport,
                "_display_dt": display_dt,
                "_kickoff_dt": kickoff_dt.isoformat() if kickoff_dt else None,
                "_game_end_dt": game_end_dt.isoformat() if (kickoff_dt and game_end_dt) else None,
                "_close_dt": close_dt.isoformat() if close_dt else None,
                "_exp_dt": game_end_dt.isoformat() if game_end_dt else None,
                "_sort_ts": sort_ts_dt.isoformat() if sort_ts_dt else None,
                "outcomes": outcomes,
            }
            records.append(r)
        except Exception:
            pass

    raw_count = len(all_ev)
    # Free the raw events list explicitly so GC can reclaim the big
    # Kalshi payloads before we return.
    del all_ev
    # Pre-compute which sport events are confirmed live by ESPN /
    # SofaScore / SportsDB. This runs once per cache rebuild (~30min)
    # so the per-request Live filter can just check a flag instead of
    # running expensive match_game calls on every request. Non-sport
    # events are handled separately via close_dt/exp_dt in the filter.
    try:
        from espn_feed import match_game as _em_cache
    except Exception:
        _em_cache = None
    try:
        from sportsdb_feed import match_game as _sm_cache
    except Exception:
        _sm_cache = None
    try:
        from sofascore_feed import match_game as _fm_cache
    except Exception:
        _fm_cache = None
    live_count = 0
    for r in records:
        if not r.get("_is_sport"):
            continue
        _sp = r.get("_sport", "")
        _ti = r.get("title", "")
        if not (_sp and _ti):
            continue
        mg = None
        if _em_cache:
            mg = _em_cache(_ti, _sp)
        if mg is None and _sm_cache:
            mg = _sm_cache(_ti, _sp)
        if mg is None and _fm_cache:
            mg = _fm_cache(_ti, _sp)
        if mg and mg.get("state") == "in":
            # Date guard: reject matches where ESPN's scheduled
            # kickoff is >18h from Kalshi's estimated kickoff.
            # Prevents "Man Utd vs Leeds (today)" from marking
            # "Leeds vs Wolves (next week)" as live just because
            # both titles contain "Leeds United".
            sched_ms = mg.get("scheduled_kickoff_ms")
            kdt_str = r.get("_kickoff_dt") or r.get("_sort_ts")
            if sched_ms and kdt_str:
                try:
                    from datetime import datetime as _dtc
                    espn_dt = _dtc.fromtimestamp(sched_ms / 1000, tz=timezone.utc)
                    kalshi_dt = _dtc.fromisoformat(kdt_str)
                    if abs((espn_dt - kalshi_dt).total_seconds()) > 18 * 3600:
                        continue  # wrong day's game
                except Exception:
                    pass
            r["_is_live"] = True
            live_count += 1
    # Store ungrouped records (for "All Markets" view) — _is_live
    # is already set on each record so both views respect it.
    ungrouped = records
    # Group siblings into tabbed cards (for "Game View", the default).
    before_group = len(records)
    grouped = _group_game_markets(records)
    grouped_into = before_group - len(grouped)
    sport_count = sum(1 for r in grouped if r.get("_is_sport"))
    kickoff_count = sum(1 for r in grouped if r.get("_kickoff_dt"))
    logging.getLogger("stochverse").info(
        "get_data: raw=%d records=%d sport=%d kickoff=%d grouped=%d live=%d",
        raw_count, len(grouped), sport_count, kickoff_count, grouped_into, live_count,
    )
    _cache["data"] = grouped
    _cache["data_all"] = ungrouped
    _cache["ts"] = time.time()
    # Write-through: upsert events/markets to PostgreSQL in the
    # background. Uses the ungrouped list so every event (including
    # siblings) gets a row. Non-blocking — if it fails, the
    # in-memory cache still serves.
    try:
        import asyncio
        from db import sync_events_to_db
        asyncio.run(sync_events_to_db(ungrouped))
    except Exception as e:
        logging.getLogger("stochverse").warning("db write-through skipped: %s", e)

# ── API routes ─────────────────────────────────────────────────────────────────
@app.get("/api/events")
def get_events(
    category: Optional[str] = None,
    sport: Optional[str] = None,
    soccer_comp: Optional[str] = None,
    live_cat: Optional[str] = None,
    view: Optional[str] = "game",
    search: Optional[str] = None,
    date_filter: Optional[str] = "all",
    sort: Optional[str] = "earliest",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    offset: int = 0,
    limit: int = 24,
):
    from datetime import date as _date
    # "game" view = tabbed cards (grouped siblings, default).
    # "all" view  = every market type as its own card (ungrouped).
    # Calling get_data() ensures the cache is populated (both grouped
    # and ungrouped versions are stored during cache build).
    get_data()
    if view == "all" and _cache.get("data_all") is not None:
        records = _cache["data_all"]
    else:
        records = _cache.get("data") or []
    today = _date.today()
    from datetime import datetime as _dt
    now_utc = _dt.now(timezone.utc)

    # Import live-score feeds for the formatting loop.
    try:
        from espn_feed import match_game, compact_label
    except Exception:
        match_game = None
        compact_label = None
    try:
        from sportsdb_feed import match_game as sdb_match_game
    except Exception:
        sdb_match_game = None
    try:
        from sofascore_feed import match_game as sofa_match_game
    except Exception:
        sofa_match_game = None

    # Filter
    results = []
    for r in records:
        # Category filter
        if search:
            pass  # when searching, show all categories
        elif category and category != "All":
            if category == "Live":
                # Sport events: trust the _is_live flag pre-computed
                # during cache build from ESPN/SofaScore feeds (runs
                # once per 30min rebuild, not per request).
                # Non-sport events: check close/exp time window.
                if r.get("_is_live"):
                    pass  # confirmed live by ESPN/SofaScore
                elif r.get("_is_sport"):
                    # Sport but not confirmed live by feed. Include if:
                    #   - Ticker date is today (game scheduled today)
                    #   - OR currently within the kickoff-to-end window
                    #     (catches late-night games from yesterday that
                    #     are still in progress, e.g. Brazil 9PM local
                    #     = APR11 ticker but now APR12 UTC)
                    ticker_date = parse_game_date_from_ticker(r.get("event_ticker", ""))
                    in_window = False
                    kdt = r.get("_kickoff_dt")
                    gdt = r.get("_game_end_dt")
                    if kdt and gdt:
                        try:
                            k = _dt.fromisoformat(kdt)
                            g = _dt.fromisoformat(gdt)
                            in_window = k <= now_utc < g
                        except Exception:
                            pass
                    is_today = ticker_date and ticker_date == now_utc.date()
                    if not (is_today or in_window):
                        continue
                else:
                    # Non-sport event (crypto, politics, etc.).
                    # "Live" = event is happening today. Three ways
                    # to qualify:
                    #   1. Ticker date matches today (e.g. APR12 in
                    #      KXHUNGARYMOV-26APR12 = election today)
                    #   2. exp_dt is same UTC date as now
                    #   3. exp_dt within 18h (timezone edge cases)
                    # This handles cases where Kalshi's exp_dt is
                    # days/weeks after the actual event (elections
                    # settle later than they happen).
                    edt = r.get("_exp_dt")
                    ticker_date = parse_game_date_from_ticker(r.get("event_ticker", ""))
                    today_date = now_utc.date()
                    if ticker_date and ticker_date == today_date:
                        pass  # event happening today — include
                    elif edt:
                        try:
                            e = _dt.fromisoformat(edt)
                            if now_utc >= e:
                                continue  # already settled
                            same_day = e.date() == today_date
                            within_18h = (e - now_utc).total_seconds() <= 18 * 3600
                            if not (same_day or within_18h):
                                continue
                        except Exception:
                            continue
                    else:
                        continue
            elif category == "Sports":
                if not r["_is_sport"]: continue
            else:
                # Map display name to Kalshi API category names
                kalshi_cats = DISPLAY_TO_CATS.get(category, [category])
                if r["category"] not in kalshi_cats: continue

        # Sport filter - skip when searching globally
        if not search and sport and sport != "All sports":
            if r["_sport"] != sport: continue

        # Soccer comp / subtab filter
        if soccer_comp and soccer_comp != "All":
            if sport == "Soccer" or r["_sport"] == "Soccer":
                if r["_soccer_comp"] != soccer_comp: continue
            elif sport and r["_is_sport"]:
                # Non-soccer sport subtab filter
                sp = r["_sport"]
                tabs_def = SPORT_SUBTABS.get(sp, [])
                if tabs_def:
                    lk = SERIES_TO_SUBTAB.get(sp, {})
                    series = r.get("series_ticker", "").upper()
                    subtab = lk.get(series, "Other")
                    if subtab != soccer_comp: continue
            else:
                # Non-sport category keyword filter
                KEYWORD_MAP = {
                    "Bitcoin":        ["bitcoin","btc"],
                    "Ethereum":       ["ethereum","eth"],
                    "Solana":         ["solana","sol"],
                    "Dogecoin":       ["dogecoin","doge"],
                    "XRP":            ["xrp","ripple"],
                    "BNB":            ["bnb","binance"],
                    "S&P 500":        ["s&p","s&p 500","spx","spy"],
                    "Nasdaq":         ["nasdaq","ndx","qqq"],
                    "Dow":            ["dow","djia"],
                    "Gold":           ["gold","xau"],
                    "US Elections":   ["us election","presidential","electoral"],
                    "Fed":            ["fed","federal reserve","fomc"],
                    "Interest Rates": ["interest rate","rate cut","rate hike","basis point"],
                    "Inflation":      ["inflation","cpi","pce","price"],
                    "GDP":            ["gdp","gross domestic"],
                    "Jobs":           ["jobs","employment","payroll","unemployment"],
                    "AI":             ["artificial intelligence"," ai ","openai","chatgpt","llm","gpt","claude","gemini"],
                    "LLMs":           ["llm","large language","openai","anthropic","gemini","claude","gpt"],
                    "Trump Agenda":   ["trump","executive order","tariff","deport","doge"],
                    "Tariffs":        ["tariff","trade war","import tax","customs"],
                    "Approval Ratings":["approval rating","approve","disapprove","favorability"],
                    "Oscars":         ["oscar","academy award"],
                    "Grammys":        ["grammy"],
                    "Emmys":          ["emmy"],
                    "Billboard":      ["billboard","hot 100","chart"],
                    "Rotten Tomatoes":["rotten tomatoes","tomatometer"],
                    "Netflix":        ["netflix"],
                    "Spotify":        ["spotify"],
                    "Hurricanes":     ["hurricane","tropical storm","cyclone"],
                    "Daily Temperature":["temperature","high temp","low temp","degrees"],
                    "Snow and rain":  ["snow","rain","precipitation","blizzard"],
                    "Natural disasters":["earthquake","tornado","flood","wildfire","disaster"],
                    "Disease":        ["disease","virus","outbreak","measles","flu","covid"],
                    "Vaccines":       ["vaccine","vaccination","immunization"],
                    "China":          ["china","chinese","beijing","xi jinping"],
                    "Russia":         ["russia","russian","putin","moscow","ukraine"],
                    "Ukraine":        ["ukraine","ukrainian","zelensky","war"],
                    "Middle East":    ["israel","gaza","iran","saudi","middle east","hamas"],
                    "Latin America":  ["mexico","brazil","argentina","venezuela","colombia"],
                    "Elon Musk":      ["elon musk","elon","musk","doge","tesla","spacex","x.com","twitter"],
                    "Tesla":          ["tesla","tsla"],
                    "SpaceX":         ["spacex","starship","falcon","rocket"],
                }
                keywords = KEYWORD_MAP.get(soccer_comp, [soccer_comp.lower()])
                title_lower = r["title"].lower()
                if not any(kw in title_lower for kw in keywords):
                    continue

        # Live category filter (Crypto, Climate, etc. in Live sidebar)
        if live_cat:
            if r.get("_is_sport"):
                continue  # non-sport filter active, skip sports
            c = r.get("category", "Other")
            disp = CAT_DISPLAY.get(c, c)
            if disp != live_cat:
                continue

        # Search — match all whitespace-separated tokens in any order
        # against title or event_ticker (case-insensitive).
        if search:
            tokens = [t for t in search.lower().split() if t]
            if tokens:
                title_l = r["title"].lower()
                ticker_l = r["event_ticker"].lower()
                haystack = title_l + " " + ticker_l
                if not all(tok in haystack for tok in tokens):
                    continue

        # Date filter
        if date_filter != "all":
            kdt = r["_kickoff_dt"]
            if kdt:
                try:
                    kd = _date.fromisoformat(kdt[:10])
                    if date_filter == "today" and kd != today: continue
                    if date_filter == "week" and not (today <= kd <= today + timedelta(days=6)): continue
                    if date_filter == "custom":
                        if date_from:
                            df = _date.fromisoformat(date_from)
                            if kd < df: continue
                        if date_to:
                            dt = _date.fromisoformat(date_to)
                            if kd > dt: continue
                except: pass

        # Game view: only show actual game/match events, not standalone
        # prop markets (Points, Goals, Assists, First Goal, etc.). A
        # record qualifies as a "game" card if it has grouped siblings
        # (market_groups) or its series ends with GAME/MATCH.
        if view != "all":
            series_up = str(r.get("series_ticker", "")).upper()
            has_groups = bool(r.get("_market_groups"))
            is_game = series_up.endswith("GAME") or series_up.endswith("MATCH")
            if not has_groups and not is_game:
                continue

        results.append(r)

    # Sort by precise timestamp (kickoff → expiration → close → game date).
    # Undated events always go to the end, regardless of direction.
    dated = [r for r in results if r.get("_sort_ts")]
    undated = [r for r in results if not r.get("_sort_ts")]
    dated.sort(key=lambda r: r["_sort_ts"], reverse=(sort == "latest"))
    # When viewing Live, float in-progress events above today's
    # pre-match events so active games show first. Check both
    # _is_live (ESPN-confirmed) and the kickoff window (for events
    # ESPN didn't match but are in progress by time).
    if category == "Live":
        def _live_rank(r):
            if r.get("_is_live"):
                return 0  # ESPN-confirmed live
            kdt = r.get("_kickoff_dt")
            gdt = r.get("_game_end_dt")
            if kdt and gdt:
                try:
                    k = _dt.fromisoformat(kdt)
                    g = _dt.fromisoformat(gdt)
                    # 2h buffer for long-running matches (tennis
                    # 3-setters, soccer extra time, overtime, etc.)
                    buf = timedelta(hours=2)
                    if k <= now_utc < (g + buf):
                        return 0  # in kickoff window
                except Exception:
                    pass
            return 1  # pre-match / upcoming
        # Sort by live rank first (in-progress → top), then by time
        # respecting the user's earliest/latest preference.
        rev = (sort == "latest")
        dated.sort(key=lambda r: (
            _live_rank(r),
            r.get("_sort_ts", "") if not rev else "",
        ))
        if rev:
            # Stable sort: live group reversed, pre-match group reversed
            live_group = [r for r in dated if _live_rank(r) == 0]
            pre_group = [r for r in dated if _live_rank(r) != 0]
            live_group.sort(key=lambda r: r.get("_sort_ts", ""), reverse=True)
            pre_group.sort(key=lambda r: r.get("_sort_ts", ""), reverse=True)
            dated = live_group + pre_group
    results = dated + undated

    # (match_game imports moved above the filter loop)

    total = len(results)
    page  = results[offset:offset+limit]

    def _needs_flip(title: str, g: dict) -> bool:
        """Returns True if the home/away orientation should be flipped
        to match the Kalshi title order. Uses whichever team phrase
        appears first in the normalized title to decide."""
        if not g:
            return False
        try:
            from espn_feed import _normalize
            tl = _normalize(title or "")
        except Exception:
            tl = (title or "").lower()
        def first_pos(phrases):
            best = -1
            for p in phrases or ():
                if not p:
                    continue
                idx = tl.find(p)
                if idx >= 0 and (best == -1 or idx < best):
                    best = idx
            return best
        home_pos = first_pos(g.get("home_phrases", []))
        away_pos = first_pos(g.get("away_phrases", []))
        if home_pos >= 0 and (away_pos < 0 or home_pos < away_pos):
            return False
        return True

    def _score_display(title: str, g: dict) -> str:
        """Build an ordered score string whose team order matches how
        the teams appear in the Kalshi event title."""
        if not g:
            return ""
        hs = g.get("home_score", "")
        as_ = g.get("away_score", "")
        if hs == "" or as_ == "":
            return ""
        ha = g.get("home_abbr", "") or "HOME"
        aa = g.get("away_abbr", "") or "AWAY"
        if _needs_flip(title, g):
            return f"{aa} {as_} - {ha} {hs}"
        return f"{ha} {hs} - {aa} {as_}"

    def _flip_score_pairs(label: str) -> str:
        """Flip each "H-A" pair in a space-separated tennis label
        ("6-3 4-5 30-0" → "3-6 5-4 0-30") so the per-set breakdown
        matches the Kalshi-title orientation of score_display."""
        if not label:
            return label
        parts = label.split()
        flipped = []
        for p in parts:
            if "-" in p:
                a, b = p.split("-", 1)
                flipped.append(f"{b}-{a}")
            else:
                flipped.append(p)
        return " ".join(flipped)

    formatted = []
    for r in page:
        # Defense-in-depth: in Game View, skip any record whose
        # series_ticker is a non-primary sibling type (SPREAD/TOTAL/
        # BTTS/1H). These should have been removed by
        # _group_game_markets, but can leak through if the parent
        # GAME event wasn't fetched in the same pagination cycle.
        if view != "all":
            series_up = (r.get("series_ticker") or "").upper()
            mt = GAME_MARKET_PREFIXES.get(series_up)
            if mt and not mt[3]:  # mt[3] = is_primary
                continue
        rc = dict(r)
        rc["outcomes"] = _format_outcomes(r.get("outcomes", []))
        # When this record has sibling market groups (La Liga
        # spread / total / BTTS / 1H collapsed under the moneyline
        # parent by _group_game_markets), format each group's
        # outcomes the same way so live WebSocket prices flow into
        # every tab, not just the default Winner tab.
        mg = r.get("_market_groups") if view != "all" else None
        if mg:
            rc["market_groups"] = [
                {
                    "type_code":     g.get("type_code", ""),
                    "label":         g.get("label", ""),
                    "event_ticker":  g.get("event_ticker", ""),
                    "series_ticker": g.get("series_ticker", ""),
                    "url":           g.get("url", ""),
                    "outcomes":      _format_outcomes(g.get("_outcomes", [])),
                }
                for g in mg
            ]
            # Don't leak the private `_market_groups` key to clients.
            rc.pop("_market_groups", None)
        sport = r.get("_sport", "")
        title = r.get("title", "")
        g = None
        if sport and title:
            if match_game is not None:
                g = match_game(title, sport)
            if g is None and sdb_match_game is not None:
                g = sdb_match_game(title, sport)
            if g is None and sofa_match_game is not None:
                g = sofa_match_game(title, sport)
        # Enrich soccer 2-leg ties with SofaScore aggregate data
        # when the primary feed (usually ESPN for UCL) didn't
        # populate it. No-op for non-soccer or when aggregate is
        # already present.
        if g and sport == "Soccer":
            g = _enrich_soccer_aggregate(g, title)
        # Guard against wrong-date matches. The team-name matcher
        # can't distinguish games with overlapping names on different
        # days (e.g. "Leeds United vs Wolverhampton" Apr 18 matching
        # a live "Man Utd vs Leeds United" today because both contain
        # "Leeds United"). Compare the matched game's scheduled start
        # against the Kalshi event's estimated kickoff. If they're
        # more than 18 hours apart, drop the match — even if the ESPN
        # game is currently live ("in"), since the Kalshi event is
        # clearly for a different day's fixture.
        if g and g.get("scheduled_kickoff_ms"):
            kdt_str = r.get("_kickoff_dt") or r.get("_sort_ts")
            if kdt_str:
                try:
                    from datetime import datetime as _datetime
                    espn_dt = _datetime.fromtimestamp(
                        g["scheduled_kickoff_ms"] / 1000, tz=timezone.utc
                    )
                    kalshi_dt = _datetime.fromisoformat(kdt_str)
                    if abs((espn_dt - kalshi_dt).total_seconds()) > 18 * 3600:
                        g = None
                except Exception:
                    pass
        if g:
            # Base compact label from the feed. For tennis we flip
            # the per-set pairs to match the Kalshi title order so
            # the "6-3 4-5 30-0" breakdown lines up with the
            # "ALC 1 - SIN 1" summary to its left.
            base_label = compact_label(g) if compact_label else ""
            if g.get("sport") == "Tennis" and _needs_flip(title, g):
                base_label = _flip_score_pairs(base_label)
            rc["_live_state"] = {
                "label":          base_label,
                "state":          g.get("state", ""),
                "short_detail":   g.get("short_detail", ""),
                "display_clock":  g.get("display_clock", ""),
                "period":         g.get("period", 0),
                "league":         g.get("league", ""),
                "captured_at_ms": g.get("captured_at_ms", 0),
                "clock_running":  g.get("clock_running", True),
                "home_abbr":      g.get("home_abbr", ""),
                "away_abbr":      g.get("away_abbr", ""),
                "home_display":   g.get("home_display", ""),
                "away_display":   g.get("away_display", ""),
                "home_score":     g.get("home_score", "") or ("0" if g.get("state") == "in" else ""),
                "away_score":     g.get("away_score", "") or ("0" if g.get("state") == "in" else ""),
                "score_display":  _score_display(title, g),
                # Title-derived team names so the frontend can match
                # outcome labels even when Kalshi uses a different name
                # than ESPN (e.g. "Junin" vs "Sarmiento de Junín").
                "title_home":     "",
                "title_away":     "",
                # Playoff series metadata (only ESPN games surface
                # these; SofaScore/SportsDB matches leave them empty).
                "is_playoff":         bool(g.get("is_playoff")),
                "series_title":       g.get("series_title", ""),
                "series_summary":     g.get("series_summary", ""),
                "series_home_wins":   g.get("series_home_wins"),
                "series_away_wins":   g.get("series_away_wins"),
                "series_game_number": g.get("series_game_number"),
                # Two-leg knockout aggregate (soccer cup ties).
                "is_two_leg":         bool(g.get("is_two_leg")),
                "aggregate_home":     g.get("aggregate_home"),
                "aggregate_away":     g.get("aggregate_away"),
                "leg_number":         g.get("leg_number"),
                "round_name":         g.get("round_name", ""),
                "tournament_name":    g.get("tournament_name", "") or g.get("league", ""),
                "aggregate_winner":   g.get("aggregate_winner", ""),
            }
            # Parse team names from the Kalshi title ("A vs B")
            # and assign to title_home / title_away using flip.
            import re as _re
            _parts = _re.split(r'\s+(?:vs\.?|v|at)\s+', title, maxsplit=1, flags=_re.IGNORECASE)
            if len(_parts) == 2:
                _flip = _needs_flip(title, g)
                if _flip:
                    rc["_live_state"]["title_home"] = _parts[1].strip()
                    rc["_live_state"]["title_away"] = _parts[0].strip()
                else:
                    rc["_live_state"]["title_home"] = _parts[0].strip()
                    rc["_live_state"]["title_away"] = _parts[1].strip()
            # Tennis: attach structured per-player data so the
            # frontend can render a vertical 2-row scoreboard
            # instead of the single-line breakdown. Flip sides
            # when the Kalshi title lists the away player first.
            if g.get("sport") == "Tennis":
                flip = _needs_flip(title, g)
                home_key, away_key = ("away", "home") if flip else ("home", "away")
                rc["_live_state"]["tennis"] = {
                    "row1_name":   g.get(f"tennis_{home_key}_name", ""),
                    "row2_name":   g.get(f"tennis_{away_key}_name", ""),
                    "row1_sets":   g.get(f"tennis_{home_key}_sets", ""),
                    "row2_sets":   g.get(f"tennis_{away_key}_sets", ""),
                    "row1_games":  g.get(f"tennis_{home_key}_games", ""),
                    "row2_games":  g.get(f"tennis_{away_key}_games", ""),
                    "row1_point":  g.get(f"tennis_{home_key}_point", ""),
                    "row2_point":  g.get(f"tennis_{away_key}_point", ""),
                    "set_history": [
                        {
                            "set":  s.get("set"),
                            "row1": s.get(home_key),
                            "row2": s.get(away_key),
                        }
                        for s in (g.get("tennis_set_history") or [])
                    ],
                    "server": (
                        "row1" if g.get("tennis_server") == home_key
                        else ("row2" if g.get("tennis_server") == away_key else "")
                    ),
                }
            # If ESPN or SofaScore gave us the actual scheduled
            # kickoff time, override our DURATION-based estimate
            # with it. Kalshi's expected_expiration_time varies per
            # match, so no fixed DURATION can be universally
            # accurate — but ESPN's date field and SofaScore's
            # startTimestamp are authoritative.
            sched_ms = g.get("scheduled_kickoff_ms")
            if sched_ms:
                try:
                    from datetime import datetime as _dt2
                    rc["_kickoff_dt"] = _dt2.fromtimestamp(
                        sched_ms / 1000, tz=timezone.utc
                    ).isoformat()
                except Exception:
                    pass
        formatted.append(rc)
    # Re-overlay LIVE_PRICES on every outcome in the response so
    # cards always show current prices, not 5-min-old cache strings.
    # Lightweight: at most 24 events × ~5 outcomes = ~120 lookups.
    try:
        from kalshi_ws import LIVE_PRICES as _LP
    except Exception:
        _LP = {}
    if _LP:
        for ev in formatted:
            _overlay_live(ev.get("outcomes") or [], _LP)
            for mg in (ev.get("market_groups") or []):
                _overlay_live(mg.get("outcomes") or [], _LP)
    return {"total": total, "offset": offset, "limit": limit, "events": formatted}


def _overlay_live(outcomes, lp):
    """Re-compute chance/yes/no strings from LIVE_PRICES for a list
    of outcome dicts. Uses last_price for probability (matching
    Kalshi's own display). YES shows ask, NO shows ask. Mutates in place."""
    for o in outcomes:
        tk = o.get("ticker", "")
        live = lp.get(tk)
        if not live:
            continue
        ya = live.get("yes_ask")
        yb = live.get("yes_bid")
        na = live.get("no_ask")
        nb = live.get("no_bid")
        last = live.get("last_price")
        if last is not None and last > 0:
            o["chance"] = f"{round(last)}%"
        if ya is not None:
            o["yes"] = f"{round(ya)}¢"
        elif yb is not None:
            o["yes"] = f"{round(yb)}¢"
        if na is not None:
            o["no"] = f"{round(na)}¢"
        elif nb is not None:
            o["no"] = f"{round(nb)}¢"


def _kalshi_url(series_ticker: str, event_ticker: str) -> str:
    """Build canonical Kalshi event URL."""
    if not series_ticker or not event_ticker:
        return ""
    s = series_ticker.lower()
    return f"https://kalshi.com/markets/{s}/{s.replace('kx', '')}/{event_ticker.lower()}"


def _enrich_soccer_aggregate(g, title):
    """Fill in two-leg aggregate data on a soccer match dict that
    ESPN matched first but whose ESPN feed didn't include the
    "Aggregate: X-Y" note. Uses SofaScore's richer knockout data
    (homeScore.aggregated / awayScore.aggregated + aggregatedWinnerCode)
    to populate the fields in-place. Harmless if SofaScore doesn't
    have the match or the aggregate — just returns without side
    effects. Called from both /api/events and /api/event/{ticker}.

    Only runs for Soccer, and only when the primary match lacks
    aggregate info, so SofaScore is consulted at most once per
    knockout-tie card per request.
    """
    if not g or g.get("sport") != "Soccer":
        return g
    has_agg = g.get("aggregate_home") is not None and g.get("aggregate_away") is not None
    if has_agg and g.get("is_two_leg"):
        return g
    try:
        from sofascore_feed import match_game as sofa_match
    except Exception:
        return g
    try:
        sg = sofa_match(title, "Soccer")
    except Exception:
        return g
    if sg:
        # Prefer SofaScore's aggregate fields when present.
        if sg.get("is_two_leg"):
            g["is_two_leg"] = True
            if sg.get("aggregate_home") is not None:
                g["aggregate_home"] = sg.get("aggregate_home")
            if sg.get("aggregate_away") is not None:
                g["aggregate_away"] = sg.get("aggregate_away")
            if sg.get("leg_number") and not g.get("leg_number"):
                g["leg_number"] = sg.get("leg_number")
            if sg.get("round_name") and not g.get("round_name"):
                g["round_name"] = sg.get("round_name")
            if sg.get("tournament_name") and not g.get("tournament_name"):
                g["tournament_name"] = sg.get("tournament_name")
            if sg.get("aggregate_winner") and not g.get("aggregate_winner"):
                g["aggregate_winner"] = sg.get("aggregate_winner")
    # Final fallback — on-demand SofaScore search when the cached
    # games (live + scheduled) don't contain the fixture. This is a
    # blocking HTTP call per card, scoped to 2-leg soccer only and
    # cached for 5 minutes, so the cost is negligible in practice.
    still_missing = (g.get("aggregate_home") is None or g.get("aggregate_away") is None)
    if g.get("is_two_leg") and still_missing:
        try:
            from sofascore_feed import lookup_aggregate_sync
            home_hint = g.get("home_display") or ""
            away_hint = g.get("away_display") or ""
            agg = lookup_aggregate_sync(home_hint, away_hint) if home_hint and away_hint else None
            if agg:
                if agg.get("aggregate_home") is not None:
                    g["aggregate_home"] = agg["aggregate_home"]
                if agg.get("aggregate_away") is not None:
                    g["aggregate_away"] = agg["aggregate_away"]
                if agg.get("leg_number") and not g.get("leg_number"):
                    g["leg_number"] = agg["leg_number"]
                if agg.get("round_name") and not g.get("round_name"):
                    g["round_name"] = agg["round_name"]
                if agg.get("tournament_name") and not g.get("tournament_name"):
                    g["tournament_name"] = agg["tournament_name"]
                if agg.get("aggregate_winner") and not g.get("aggregate_winner"):
                    g["aggregate_winner"] = agg["aggregate_winner"]
        except Exception:
            pass
    return g


@app.get("/api/event/{ticker}")
def get_event_detail(ticker: str):
    """Full per-event detail for the dedicated event page.

    Looks up the event in the in-memory cache (built by get_data())
    and returns a superset of /api/events — includes the formatted
    outcome rows for card-style display plus enriched per-market
    fields (yes/no bid+ask in dollars, last price, volume, OI,
    liquidity, spread, change, Kalshi URL, rules) so the frontend
    can render a full order-book view.
    """
    if not ticker:
        return {"error": "ticker required"}
    # Ensure cache is primed. Search data_all first (every market
    # type, including spread/total siblings) then fall back to the
    # grouped list, then through the ungrouped set as a last resort.
    get_data()
    records_all = _cache.get("data_all") or []
    records_grouped = _cache.get("data") or []
    found = None
    for r in records_all:
        if r.get("event_ticker") == ticker:
            found = r
            break
    if found is None:
        for r in records_grouped:
            if r.get("event_ticker") == ticker:
                found = r
                break
            # Also scan grouped market_groups (sibling events live
            # under their moneyline parent in the grouped cache).
            for g in r.get("_market_groups", []) or []:
                if g.get("event_ticker") == ticker:
                    # Wrap the sibling group as a standalone record
                    # so the response shape stays consistent.
                    found = dict(r)
                    found["event_ticker"] = g.get("event_ticker")
                    found["series_ticker"] = g.get("series_ticker")
                    found["outcomes"] = g.get("_outcomes", [])
                    found["_market_groups"] = None
                    break
            if found:
                break
    if found is None:
        return {"error": f"event {ticker!r} not found in cache"}

    # Import live-score feeds + helpers the same way /api/events does.
    try:
        from espn_feed import match_game, compact_label
    except Exception:
        match_game = None
        compact_label = None
    try:
        from sportsdb_feed import match_game as sdb_match_game
    except Exception:
        sdb_match_game = None
    try:
        from sofascore_feed import match_game as sofa_match_game
    except Exception:
        sofa_match_game = None

    try:
        from kalshi_ws import LIVE_PRICES
    except Exception:
        LIVE_PRICES = {}

    def _enrich_outcomes(stored):
        """Turn raw stored outcomes into full per-market objects that
        include both display-ready string fields and numeric fields
        for the detail view's order-book / stats section."""
        out = []
        for o in stored:
            tk = o.get("ticker", "")
            lp = LIVE_PRICES.get(tk) or {}
            yb = lp.get("yes_bid") if lp.get("yes_bid") is not None else o.get("_yb")
            ya = lp.get("yes_ask") if lp.get("yes_ask") is not None else o.get("_ya")
            nb = lp.get("no_bid")  if lp.get("no_bid")  is not None else o.get("_nb")
            na = lp.get("no_ask")  if lp.get("no_ask")  is not None else o.get("_na")
            last = lp.get("last_price") if lp.get("last_price") is not None else o.get("_last")
            vol   = o.get("_vol", 0) or 0
            vol24 = o.get("_vol24h", 0) or 0
            oi    = o.get("_oi", 0) or 0
            liq   = o.get("_liq", 0) or 0
            prev  = o.get("_prev")
            # Derived
            if yb is not None and ya is not None and yb > 0 and ya > 0:
                prob = round((yb + ya) / 2)
                spread = round(ya - yb)
            elif last is not None and last > 0:
                prob = round(last)
                spread = None
            else:
                prob = None
                spread = None
            change = None
            if last is not None and prev is not None and prev > 0:
                change = round(last - prev)
            out.append({
                "label":    o.get("label", ""),
                "ticker":   tk,
                "chance":   f"{int(round(prob))}%" if prob is not None else "—",
                "yes":      f"{int(round(yb))}¢"   if yb   is not None else "—",
                "no":       f"{int(round(na))}¢"   if na   is not None else "—",
                "prob":     prob,
                "yes_bid":  round(yb) if yb is not None else None,
                "yes_ask":  round(ya) if ya is not None else None,
                "no_bid":   round(nb) if nb is not None else None,
                "no_ask":   round(na) if na is not None else None,
                "yes_bid_dollars": (yb / 100.0) if yb is not None else None,
                "yes_ask_dollars": (ya / 100.0) if ya is not None else None,
                "no_bid_dollars":  (nb / 100.0) if nb is not None else None,
                "no_ask_dollars":  (na / 100.0) if na is not None else None,
                "last_price":         round(last) if last is not None else None,
                "last_price_dollars": (last / 100.0) if last is not None else None,
                "spread":        spread,
                "change":        change,
                "volume":        round(vol),
                "volume_24h":    round(vol24),
                "open_interest": round(oi),
                "liquidity":     round(liq * 100) / 100,
                "rules":             o.get("_rules", ""),
                "rules_secondary":   o.get("_rules_secondary", ""),
                "early_close_condition": o.get("_early_close_condition", ""),
            })
        # Sort long markets by probability desc so the most likely
        # outcomes are first — same rule the card uses.
        if len(out) >= 5:
            out.sort(key=lambda x: (x.get("prob") is None, -(x.get("prob") or 0)))
        return out

    # Build the response, re-using the formatting conventions from
    # /api/events so the frontend card helpers work unchanged.
    r = found
    rc = dict(r)
    # Strip private sort/internal fields the detail view doesn't need.
    for k in ("_sort_ts", "_outcomes"):
        rc.pop(k, None)
    rc["outcomes"] = _enrich_outcomes(r.get("outcomes", []))
    mg = r.get("_market_groups")
    if mg:
        rc["market_groups"] = [
            {
                "type_code":     g.get("type_code", ""),
                "label":         g.get("label", ""),
                "event_ticker":  g.get("event_ticker", ""),
                "series_ticker": g.get("series_ticker", ""),
                "url":           g.get("url", ""),
                "outcomes":      _enrich_outcomes(g.get("_outcomes", [])),
            }
            for g in mg
        ]
    rc.pop("_market_groups", None)

    # Attach live-game state (scoreboard, clock, period) when the
    # event matches a currently-tracked feed game. Same logic as
    # /api/events but inlined here for a single event.
    sport = r.get("_sport", "")
    title = r.get("title", "")
    g = None
    if sport and title:
        if match_game is not None:
            g = match_game(title, sport)
        if g is None and sdb_match_game is not None:
            g = sdb_match_game(title, sport)
        if g is None and sofa_match_game is not None:
            g = sofa_match_game(title, sport)
    # Soccer 2-leg aggregate enrichment — see /api/events for details.
    if g and sport == "Soccer":
        g = _enrich_soccer_aggregate(g, title)
    # Wrong-date guard — same as /api/events.
    if g and g.get("scheduled_kickoff_ms"):
        kdt_str = r.get("_kickoff_dt") or r.get("_sort_ts")
        if kdt_str:
            try:
                from datetime import datetime as _datetime
                espn_dt = _datetime.fromtimestamp(
                    g["scheduled_kickoff_ms"] / 1000, tz=timezone.utc
                )
                kalshi_dt = _datetime.fromisoformat(kdt_str)
                if abs((espn_dt - kalshi_dt).total_seconds()) > 18 * 3600:
                    g = None
            except Exception:
                pass
    if g:
        rc["_live_state"] = {
            "label":          (compact_label(g) if compact_label else ""),
            "state":          g.get("state", ""),
            "short_detail":   g.get("short_detail", ""),
            "display_clock":  g.get("display_clock", ""),
            "period":         g.get("period", 0),
            "league":         g.get("league", ""),
            "clock_running":  g.get("clock_running", True),
            "home_abbr":      g.get("home_abbr", ""),
            "away_abbr":      g.get("away_abbr", ""),
            "home_display":   g.get("home_display", ""),
            "away_display":   g.get("away_display", ""),
            "home_score":     g.get("home_score", "") or ("0" if g.get("state") == "in" else ""),
            "away_score":     g.get("away_score", "") or ("0" if g.get("state") == "in" else ""),
            # Playoff series metadata — see /api/events for details.
            "is_playoff":         bool(g.get("is_playoff")),
            "series_title":       g.get("series_title", ""),
            "series_summary":     g.get("series_summary", ""),
            "series_home_wins":   g.get("series_home_wins"),
            "series_away_wins":   g.get("series_away_wins"),
            "series_game_number": g.get("series_game_number"),
            # Two-leg knockout aggregate (soccer cup ties).
            "is_two_leg":         bool(g.get("is_two_leg")),
            "aggregate_home":     g.get("aggregate_home"),
            "aggregate_away":     g.get("aggregate_away"),
            "leg_number":         g.get("leg_number"),
            "round_name":         g.get("round_name", ""),
            "tournament_name":    g.get("tournament_name", "") or g.get("league", ""),
            "aggregate_winner":   g.get("aggregate_winner", ""),
        }

    rc["url"] = _kalshi_url(r.get("series_ticker", ""), r.get("event_ticker", ""))
    return {"event": rc}


@app.get("/api/event/{ticker}/live_prices")
def get_event_live_prices(ticker: str):
    """Fetch guaranteed-fresh prices for all markets in an event
    directly from Kalshi's REST API. Bypasses the 30-min cache so
    the event detail page shows current prices on load.

    Returns { markets: { outcome_ticker: { yes_bid, yes_ask, ... } } }
    that the frontend overlays onto the cached event data."""
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return {"error": "ticker required", "markets": {}}
    # Find the outcome tickers for this event from the cache.
    get_data()
    records_all = _cache.get("data_all") or []
    market_tickers = []
    for r in records_all:
        if r.get("event_ticker") == ticker:
            for o in r.get("outcomes", []):
                tk = o.get("ticker")
                if tk:
                    market_tickers.append(tk)
            break
    if not market_tickers:
        return {"error": "event not found", "markets": {}}
    # Fetch each market's current state from Kalshi. Use the existing
    # signed-request pattern. Batch into one client session.
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        import base64, httpx as _httpx
        key_str = os.environ.get("KALSHI_PRIVATE_KEY", "")
        key_id = os.environ.get("KALSHI_API_KEY_ID", "")
        if not key_str or not key_id:
            return {"error": "credentials missing", "markets": {}}
        private_key = serialization.load_pem_private_key(
            key_str.encode(), password=None,
        )
        results = {}
        with _httpx.Client(timeout=10.0) as client:
            for mk in market_tickers:
                path = f"/trade-api/v2/markets/{mk}"
                ts_ms = str(int(time.time() * 1000))
                msg = (ts_ms + "GET" + path).encode()
                sig = private_key.sign(
                    msg,
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH,
                    ),
                    hashes.SHA256(),
                )
                headers = {
                    "KALSHI-ACCESS-KEY": key_id,
                    "KALSHI-ACCESS-TIMESTAMP": ts_ms,
                    "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
                    "Accept": "application/json",
                }
                url = f"https://api.elections.kalshi.com{path}"
                try:
                    r = client.get(url, headers=headers)
                    if r.status_code == 200:
                        d = r.json() or {}
                        m = d.get("market") or {}
                        yb = m.get("yes_bid") or m.get("yes_bid_dollars")
                        ya = m.get("yes_ask") or m.get("yes_ask_dollars")
                        nb = m.get("no_bid") or m.get("no_bid_dollars")
                        na = m.get("no_ask") or m.get("no_ask_dollars")
                        lp = m.get("last_price") or m.get("last_price_dollars")
                        vol = m.get("volume") or m.get("volume_fp")
                        vol24 = m.get("volume_24h") or m.get("volume_24h_fp")
                        oi = m.get("open_interest") or m.get("open_interest_fp")
                        def _to_cents(v):
                            if v is None: return None
                            if isinstance(v, str):
                                try: return round(float(v) * 100)
                                except: return None
                            if isinstance(v, (int, float)):
                                return round(v * 100) if v <= 1 else round(v)
                            return None
                        def _to_num(v):
                            if v is None: return None
                            try: return round(float(v))
                            except: return None
                        results[mk] = {
                            "yes_bid": _to_cents(yb),
                            "yes_ask": _to_cents(ya),
                            "no_bid": _to_cents(nb),
                            "no_ask": _to_cents(na),
                            "last_price": _to_cents(lp),
                            "volume": _to_num(vol),
                            "volume_24h": _to_num(vol24),
                            "open_interest": _to_num(oi),
                        }
                        # Also update LIVE_PRICES so subsequent
                        # renders don't show stale data.
                        try:
                            from kalshi_ws import LIVE_PRICES
                            cur = LIVE_PRICES.get(mk)
                            upd = {k: v for k, v in results[mk].items() if v is not None}
                            if cur is None:
                                LIVE_PRICES[mk] = upd
                            else:
                                cur.update(upd)
                        except Exception:
                            pass
                except Exception:
                    pass
        return {"markets": results}
    except Exception as e:
        return {"error": str(e), "markets": {}}


@app.get("/api/event/{ticker}/prices")
async def get_event_prices(ticker: str, hours: int = 24, max_points: int = 120):
    """Return time-series price history for every market under an
    event. Powers the sparkline on the event detail page.

    Queries the `prices` table (written every ~10s by the WS flush
    task). Downsamples to `max_points` per market by bucketing rows
    into fixed time windows and averaging `last_price` (falling
    back to midprice) within each bucket — keeps the payload small
    without losing the shape of the curve.

    Params:
      hours       lookback window in hours (default 24, max 168)
      max_points  target points per market (default 120 ≈ 12/hr)
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return {"error": "ticker required"}
    hours = max(1, min(int(hours), 168))
    max_points = max(10, min(int(max_points), 300))
    # Identify the markets we care about — pull them from the cache
    # so we don't need to re-hit Kalshi just to list tickers.
    get_data()
    records_all = _cache.get("data_all") or []
    records_grouped = _cache.get("data") or []
    market_tickers = []
    for r in records_all:
        if r.get("event_ticker") == ticker:
            for o in r.get("outcomes", []):
                if o.get("ticker"):
                    market_tickers.append(o["ticker"])
            break
    if not market_tickers:
        # Fallback — scan grouped market_groups too in case the
        # event is a sibling collapsed under a moneyline parent.
        for r in records_grouped:
            matched = False
            if r.get("event_ticker") == ticker:
                for o in r.get("outcomes", []):
                    if o.get("ticker"):
                        market_tickers.append(o["ticker"])
                matched = True
            for g in r.get("_market_groups", []) or []:
                if g.get("event_ticker") == ticker:
                    for o in g.get("_outcomes", []):
                        if o.get("ticker"):
                            market_tickers.append(o["ticker"])
                    matched = True
            if matched:
                break
    if not market_tickers:
        return {"error": f"event {ticker!r} not found in cache", "series": []}
    # No DB → no history.
    from db import DATABASE_URL, async_session
    if not DATABASE_URL or async_session is None:
        return {
            "series": [],
            "hours": hours,
            "note": "database not configured — set DATABASE_URL to record price history",
            "market_tickers": market_tickers,
        }
    try:
        # Delegate to the extracted helper so this endpoint and the
        # retry path below share exactly one query implementation.
        return await _query_price_history(market_tickers, hours, max_points)
    except Exception as e:
        # Retry up to 3 times with exponential backoff on transient
        # errors. Each retry disposes the pool so SQLAlchemy opens
        # fresh TCP connections.
        last_err = e
        if _is_transient_db_error(e):
            import asyncio as _a
            for attempt in range(3):
                try:
                    from db import engine as _eng
                    if _eng is not None:
                        await _eng.dispose()
                except Exception:
                    pass
                await _a.sleep(0.5 * (2 ** attempt))  # 0.5, 1.0, 2.0s
                try:
                    result = await _query_price_history(
                        market_tickers, hours, max_points,
                    )
                    logging.getLogger("stochverse").info(
                        "prices query recovered after %d retry(ies)",
                        attempt + 1,
                    )
                    return result
                except Exception as e2:
                    last_err = e2
                    if not _is_transient_db_error(e2):
                        break
        msg = str(last_err)
        transient = _is_transient_db_error(last_err)
        # Always log the final error so Railway logs capture the
        # exact failure after all retries — helps identify whether
        # Postgres is genuinely down vs a flaky connection.
        logging.getLogger("stochverse").warning(
            "prices query failed after retries: %s: %s",
            type(last_err).__name__, last_err,
        )
        return {
            "series": [],
            "error": msg,
            "error_type": type(last_err).__name__,
            "transient": transient,
        }


def _is_transient_db_error(e) -> bool:
    """Returns True for the specific error classes Railway's
    Postgres throws when it restarts, fails over, or drops a
    pooled connection. Frontend auto-retries on transient errors
    with a friendly "restarting" message instead of the generic
    "failed to load" one."""
    msg = str(e)
    return any(token in msg for token in (
        "CannotConnectNowError", "recovery mode",
        "starting up", "ServerDisconnectedError",
        "TimeoutError", "ConnectionResetError",
        "Connection reset by peer", "OperationalError",
        "InterfaceError", "connection was closed",
        "Broken pipe",
    ))


async def _query_price_history(market_tickers, hours, max_points):
    """The actual query loop — extracted so the retry path above
    can invoke it a second time without duplicating logic. Returns
    per-market price series plus an aggregated volume-per-bucket
    array for the volume bar chart."""
    from sqlalchemy import select
    from models import Price
    from db import async_session as _session
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    since = _dt.now(_tz.utc) - _td(hours=hours)
    bucket_s = max(30, int((hours * 3600) / max_points))
    out_series = []
    # volume_by_bucket[bucket_key] = total cumulative-volume delta
    # across every market during that bucket. Deltas computed as
    # (max_vol_in_bucket - min_vol_in_bucket) per market, then
    # summed. Gives a "total contracts traded in this window"
    # metric suitable for an aggregate bar chart.
    volume_by_bucket: Dict[int, float] = {}
    async with _session() as session:
        for mk in market_tickers:
            stmt = (
                select(Price.captured_at, Price.last_price,
                       Price.yes_bid, Price.yes_ask,
                       Price.volume)
                .where(Price.market_ticker == mk,
                       Price.captured_at >= since)
                .order_by(Price.captured_at.asc())
            )
            rows = (await session.execute(stmt)).all()
            if not rows:
                continue
            # Price bucketing: average the representative price
            # (last_price ?? midprice) across samples in the bucket.
            buckets: Dict[int, list] = {}
            # Volume bucketing: track min + max cumulative volume per
            # bucket for this market so we can compute the delta.
            vol_min: Dict[int, float] = {}
            vol_max: Dict[int, float] = {}
            for captured, last, yb, ya, vol in rows:
                try:
                    ts = captured.timestamp()
                except Exception:
                    continue
                key = int(ts // bucket_s) * bucket_s
                price_cents = last
                if price_cents is None and yb is not None and ya is not None:
                    price_cents = (yb + ya) / 2.0
                if price_cents is not None:
                    b = buckets.setdefault(key, [0.0, 0])
                    b[0] += float(price_cents)
                    b[1] += 1
                if vol is not None:
                    try:
                        v = float(vol)
                    except Exception:
                        continue
                    if key not in vol_min or v < vol_min[key]:
                        vol_min[key] = v
                    if key not in vol_max or v > vol_max[key]:
                        vol_max[key] = v
            points = []
            for key in sorted(buckets.keys()):
                total, count = buckets[key]
                if count == 0:
                    continue
                points.append({
                    "t": key * 1000,
                    "p": round(total / count, 2),
                })
            if points:
                out_series.append({
                    "market_ticker": mk,
                    "points": points,
                    "min": min(pt["p"] for pt in points),
                    "max": max(pt["p"] for pt in points),
                    "first": points[0]["p"],
                    "last": points[-1]["p"],
                })
            # Accumulate per-bucket volume deltas across markets.
            for key in vol_max:
                delta = max(0.0, vol_max[key] - vol_min.get(key, vol_max[key]))
                volume_by_bucket[key] = volume_by_bucket.get(key, 0.0) + delta
    # Shape volume into a sorted array with t+v for the frontend.
    volume = [
        {"t": key * 1000, "v": round(volume_by_bucket[key], 2)}
        for key in sorted(volume_by_bucket.keys())
    ]
    return {
        "series": out_series,
        "volume": volume,
        "hours": hours,
        "bucket_seconds": bucket_s,
        "market_tickers": market_tickers,
    }


@app.get("/api/debug_prices")
async def debug_prices(ticker: str = ""):
    """Diagnostic for the price-history pipeline. Reports whether
    DATABASE_URL is set, total rows in the prices table, the most
    recent capture timestamp, a handful of recently-seen market
    tickers, and — if the caller passes an event ticker — how many
    rows exist for each of that event's markets."""
    ticker = (ticker or "").strip().upper()
    out: Dict[str, Any] = {"ticker": ticker}
    from db import DATABASE_URL, async_session
    out["database_url_set"] = bool(DATABASE_URL)
    out["async_session_ready"] = async_session is not None
    if not DATABASE_URL or async_session is None:
        out["error"] = "database not configured"
        return out
    try:
        from sqlalchemy import select, func
        from models import Price
        async with async_session() as session:
            # Global stats
            total = (await session.execute(
                select(func.count()).select_from(Price)
            )).scalar_one()
            out["total_rows"] = int(total)
            latest = (await session.execute(
                select(func.max(Price.captured_at)).select_from(Price)
            )).scalar()
            out["latest_captured_at"] = latest.isoformat() if latest else None
            # Recent tickers — useful to sanity-check the WS flush.
            recent_stmt = (
                select(Price.market_ticker, func.max(Price.captured_at))
                .group_by(Price.market_ticker)
                .order_by(func.max(Price.captured_at).desc())
                .limit(10)
            )
            recent = (await session.execute(recent_stmt)).all()
            out["recent_tickers"] = [
                {"ticker": t, "latest": ts.isoformat() if ts else None}
                for t, ts in recent
            ]
            # Per-event breakdown when a ticker was supplied.
            if ticker:
                get_data()
                records_all = _cache.get("data_all") or []
                markets_for_event = []
                for r in records_all:
                    if r.get("event_ticker") == ticker:
                        markets_for_event = [
                            o.get("ticker") for o in r.get("outcomes", [])
                            if o.get("ticker")
                        ]
                        break
                out["markets_for_event"] = markets_for_event
                per_market = []
                for mk in markets_for_event:
                    row_count = (await session.execute(
                        select(func.count()).select_from(Price)
                        .where(Price.market_ticker == mk)
                    )).scalar_one()
                    latest_m = (await session.execute(
                        select(func.max(Price.captured_at))
                        .where(Price.market_ticker == mk)
                    )).scalar()
                    per_market.append({
                        "market_ticker": mk,
                        "row_count": int(row_count),
                        "latest": latest_m.isoformat() if latest_m else None,
                    })
                out["per_market"] = per_market
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        out["transient"] = _is_transient_db_error(e)
        # If transient, dispose the pool so the next call gets a
        # fresh connection. Caller can just retry the debug URL.
        if out.get("transient"):
            try:
                from db import engine as _eng
                if _eng is not None:
                    await _eng.dispose()
            except Exception:
                pass
    return out


@app.get("/api/screener")
async def get_screener(
    category: Optional[str] = None,
    sport: Optional[str] = None,
    status: Optional[str] = "active",      # active, live, all
    min_prob: Optional[int] = None,         # 0-100
    max_prob: Optional[int] = None,         # 0-100
    min_volume: Optional[float] = None,
    min_oi: Optional[float] = None,
    min_vol24h: Optional[float] = None,
    expires_before: Optional[str] = None,   # ISO date 'YYYY-MM-DD'
    max_days: Optional[int] = None,         # expires within N days from now
    sort_by: Optional[str] = "volume_24h",  # prob, volume, volume_24h, oi, spread, change, liquidity
    sort_dir: Optional[str] = "desc",       # asc, desc
    offset: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
):
    """Flat market-level screener. Returns individual outcomes with
    full Kalshi data fields for filtering and sorting."""
    get_data()
    from kalshi_ws import LIVE_PRICES
    records = _cache.get("data_all") or _cache.get("data") or []
    rows = []
    for r in records:
        cat = r.get("category", "")
        sp = r.get("_sport", "")
        is_live = r.get("_is_live", False)
        title = r.get("title", "")
        event_ticker = r.get("event_ticker", "")
        subcat = r.get("_subcat", "")
        exp_dt = r.get("_exp_dt") or r.get("_close_dt") or ""
        kickoff_dt = r.get("_kickoff_dt") or ""

        if category and category != "All" and cat != category:
            continue
        if sport and sp != sport:
            continue
        if status == "live" and not is_live:
            continue

        for o in r.get("outcomes", []) or r.get("_outcomes", []):
            tk = o.get("ticker", "")
            # Overlay live WS prices if available
            lp = LIVE_PRICES.get(tk) or {}
            yb = lp.get("yes_bid") if lp.get("yes_bid") is not None else o.get("_yb")
            ya = lp.get("yes_ask") if lp.get("yes_ask") is not None else o.get("_ya")
            nb = lp.get("no_bid") if lp.get("no_bid") is not None else o.get("_nb")
            na = lp.get("no_ask") if lp.get("no_ask") is not None else o.get("_na")
            last = lp.get("last_price") if lp.get("last_price") is not None else o.get("_last")
            vol = o.get("_vol", 0) or 0
            vol24 = o.get("_vol24h", 0) or 0
            oi = o.get("_oi", 0) or 0
            liq = o.get("_liq", 0) or 0
            prev = o.get("_prev")

            # Compute derived fields
            if yb is not None and ya is not None and yb > 0 and ya > 0:
                prob = round((yb + ya) / 2)
                spread = round(ya - yb)
            elif last is not None and last > 0:
                prob = round(last)
                spread = None
            else:
                prob = None
                spread = None
            if last is not None and prev is not None and prev > 0:
                change = round(last - prev)
            else:
                change = None

            # Apply filters
            if min_prob is not None and (prob is None or prob < min_prob):
                continue
            if max_prob is not None and (prob is None or prob > max_prob):
                continue
            if min_volume is not None and vol < min_volume:
                continue
            if min_oi is not None and oi < min_oi:
                continue
            if min_vol24h is not None and vol24 < min_vol24h:
                continue
            if expires_before:
                # Drop rows whose expected_expiration_time is strictly
                # after the user-selected date. Fail open if either
                # value is malformed so a bad filter never wipes the
                # whole table.
                try:
                    from datetime import datetime as _datetime
                    if exp_dt:
                        row_dt = _datetime.fromisoformat(exp_dt.replace("Z", "+00:00"))
                        cutoff = _datetime.fromisoformat(expires_before + "T23:59:59+00:00")
                        if row_dt > cutoff:
                            continue
                except Exception:
                    pass
            if max_days is not None and max_days >= 0:
                # Drop rows expiring more than N days from now.
                try:
                    from datetime import datetime as _dt2, timedelta as _td
                    if exp_dt:
                        row_dt = _dt2.fromisoformat(exp_dt.replace("Z", "+00:00"))
                        cutoff = _dt2.now(tz=timezone.utc) + _td(days=max_days)
                        if row_dt > cutoff:
                            continue
                except Exception:
                    pass
            if search:
                sq = search.lower()
                if sq not in title.lower() and sq not in o.get("label", "").lower():
                    continue

            rows.append({
                "event_ticker": event_ticker,
                "ticker": tk,
                "title": title,
                "label": o.get("label", ""),
                "url": _kalshi_url(r.get("series_ticker", ""), event_ticker),
                "category": cat,
                "sport": sp,
                "subcat": subcat,
                "is_live": is_live,
                "prob": prob,
                "yes": round(yb) if yb is not None else None,
                "no": round(na) if na is not None else None,
                # Dollar-formatted Kalshi-native prices for screener columns
                "yes_bid_dollars": (yb / 100.0) if yb is not None else None,
                "yes_ask_dollars": (ya / 100.0) if ya is not None else None,
                "no_bid_dollars":  (nb / 100.0) if nb is not None else None,
                "no_ask_dollars":  (na / 100.0) if na is not None else None,
                "last_price_dollars": (last / 100.0) if last is not None else None,
                "price_ranges": o.get("_price_ranges"),
                "expiration_time": exp_dt,
                "spread": spread,
                "volume": round(vol),
                "volume_24h": round(vol24),
                "open_interest": round(oi),
                "liquidity": round(liq * 100) / 100,
                "change": change,
                "last_price": round(last) if last is not None else None,
                "expires": exp_dt,
                "kickoff": kickoff_dt,
                "rules": o.get("_rules", ""),
            })

    # Sort
    desc = sort_dir == "desc"
    sort_key = {
        "prob": "prob", "volume": "volume", "volume_24h": "volume_24h",
        "oi": "open_interest", "spread": "spread", "change": "change",
        "liquidity": "liquidity", "yes": "yes", "no": "no",
        "last_price": "last_price",
        "last_price_dollars": "last_price_dollars",
        "yes_ask_dollars": "yes_ask_dollars", "yes_bid_dollars": "yes_bid_dollars",
        "no_ask_dollars":  "no_ask_dollars",  "no_bid_dollars":  "no_bid_dollars",
        "open_interest": "open_interest",
        "expiration_time": "expiration_time",
    }.get(sort_by, "volume_24h")
    rows.sort(key=lambda x: (x.get(sort_key) is None, x.get(sort_key) or 0),
              reverse=desc)

    total = len(rows)
    page = rows[offset:offset + limit]
    return {"total": total, "offset": offset, "limit": limit, "markets": page}


@app.get("/api/movers")
def get_movers(
    limit: int = 12,
    min_volume_24h: int = 100,
    direction: str = "both",  # "up", "down", or "both"
):
    """Top markets by 24h probability change. Powers the home-screen
    "movers" section — surfaces which events are experiencing real
    market action right now, based on Kalshi's previous_price vs
    last_price delta.

    Params:
      limit            max markets returned (default 12)
      min_volume_24h   filter out thin/noisy markets (default 100)
      direction        "up" (biggest risers) | "down" (biggest
                       fallers) | "both" (biggest abs change)
    """
    get_data()
    from kalshi_ws import LIVE_PRICES
    records = _cache.get("data_all") or _cache.get("data") or []
    rows = []
    for r in records:
        title = r.get("title", "")
        event_ticker = r.get("event_ticker", "")
        series_ticker = r.get("series_ticker", "")
        sport = r.get("_sport", "")
        cat = r.get("category", "")
        subcat = r.get("_subcat", "")
        for o in r.get("outcomes", []):
            tk = o.get("ticker", "")
            lp = LIVE_PRICES.get(tk) or {}
            last = lp.get("last_price") if lp.get("last_price") is not None else o.get("_last")
            prev = o.get("_prev")
            vol24 = o.get("_vol24h", 0) or 0
            # Require both prices + meaningful volume to avoid
            # showing a "mover" that moved because of a single
            # 1-contract trade on an illiquid market.
            if last is None or prev is None or prev <= 0 or last <= 0:
                continue
            if vol24 < min_volume_24h:
                continue
            change = last - prev
            if direction == "up" and change <= 0:
                continue
            if direction == "down" and change >= 0:
                continue
            # Current probability — prefer midprice if we have both
            # sides of the book, otherwise fall back to last_price.
            yb = lp.get("yes_bid") if lp.get("yes_bid") is not None else o.get("_yb")
            ya = lp.get("yes_ask") if lp.get("yes_ask") is not None else o.get("_ya")
            if yb is not None and ya is not None and yb > 0 and ya > 0:
                prob = round((yb + ya) / 2)
            else:
                prob = round(last)
            rows.append({
                "event_ticker": event_ticker,
                "ticker": tk,
                "title": title,
                "label": o.get("label", ""),
                "sport": sport,
                "category": cat,
                "subcat": subcat,
                "prob": prob,
                "last_price": round(last),
                "previous_price": round(prev),
                "change": round(change),          # in cents / percentage points
                "volume_24h": round(vol24),
                "url": _kalshi_url(series_ticker, event_ticker),
            })
    rows.sort(key=lambda x: abs(x["change"]), reverse=True)
    return {"movers": rows[:limit]}


@app.get("/api/sports")
def get_sports(live: bool = False):
    records = get_data()
    if live:
        from datetime import datetime as _dt
        now_utc = _dt.now(timezone.utc)
        filtered = []
        for r in records:
            if r.get("_is_live"):
                filtered.append(r)
            elif r.get("_is_sport"):
                ticker_date = parse_game_date_from_ticker(r.get("event_ticker", ""))
                in_window = False
                kdt = r.get("_kickoff_dt")
                gdt = r.get("_game_end_dt")
                if kdt and gdt:
                    try:
                        k = _dt.fromisoformat(kdt)
                        g = _dt.fromisoformat(gdt)
                        in_window = k <= now_utc < g
                    except Exception:
                        pass
                is_today = ticker_date and ticker_date == now_utc.date()
                if is_today or in_window:
                    filtered.append(r)
                continue
            else:
                edt = r.get("_exp_dt")
                ticker_date = parse_game_date_from_ticker(r.get("event_ticker", ""))
                today_date = now_utc.date()
                if ticker_date and ticker_date == today_date:
                    filtered.append(r)
                elif edt:
                    try:
                        e = _dt.fromisoformat(edt)
                        if now_utc >= e:
                            continue
                        same_day = e.date() == today_date
                        within_18h = (e - now_utc).total_seconds() <= 18 * 3600
                        if same_day or within_18h:
                            filtered.append(r)
                    except Exception:
                        pass
        records = filtered
    sport_counts = {}
    soccer_comps = set()
    sport_series = {}  # sport -> set of series tickers present in data

    for r in records:
        if r["_is_sport"]:
            s = r["_sport"]
            sport_counts[s] = sport_counts.get(s, 0) + 1
            if s not in sport_series:
                sport_series[s] = set()
            sport_series[s].add(r["series_ticker"].upper())
            if s == "Soccer" and r["_soccer_comp"] and r["_soccer_comp"] not in ("Other",""):
                soccer_comps.add(r["_soccer_comp"])

    sports = []
    for k, v in sport_counts.items():
        if k not in _SPORT_SERIES:
            continue
        # Build subtabs for this sport
        subtabs = []
        if k == "Soccer":
            subtabs = sorted(soccer_comps)
        else:
            tabs_def = SPORT_SUBTABS.get(k, [])
            if tabs_def:
                present = sport_series.get(k, set())
                for tab_name, series_list in tabs_def:
                    if any(s in present for s in series_list):
                        subtabs.append(tab_name)
        sports.append({
            "name": k,
            "count": v,
            "icon": SPORT_ICONS.get(k, "🏆"),
            "subtabs": subtabs
        })

    sports.sort(key=lambda x: list(_SPORT_SERIES.keys()).index(x["name"]) if x["name"] in _SPORT_SERIES else 99)
    # When live=true, also count non-sport categories (Crypto,
    # Climate, etc.) so the Live sidebar can show them too.
    live_cats = []
    if live:
        cat_counts = {}
        for r in records:
            if r.get("_is_sport"):
                continue
            c = r.get("category", "Other")
            disp = CAT_DISPLAY.get(c, c)
            cat_counts[disp] = cat_counts.get(disp, 0) + 1
        for c in TOP_CATS:
            if c == "Sports":
                continue
            cnt = cat_counts.get(c, 0)
            if cnt > 0:
                live_cats.append({"name": c, "count": cnt})
    return {"sports": sports, "soccer_comps": sorted(soccer_comps), "live_categories": live_cats}

# ── Shareable snapshots ──────────────────────────────────────────
@app.get("/api/admin/vacuum_prices")
async def vacuum_prices_endpoint():
    """Run VACUUM FULL on the prices table to reclaim disk space.
    Without FULL, Neon doesn't return pages to the OS — the DB stays
    "full" even after deleting rows. FULL rewrites the table from
    scratch, which locks it briefly."""
    try:
        from db import engine
        from sqlalchemy import text as _text
        if engine is None:
            return {"error": "database not configured"}
        async with engine.connect() as conn:
            # VACUUM FULL cannot run inside a transaction.
            await conn.execute(_text("COMMIT"))
            await conn.execute(_text("VACUUM FULL prices"))
        return {"status": "ok", "message": "VACUUM FULL prices completed"}
    except Exception as e:
        return JSONResponse({"error": str(e)[:400]}, status_code=500)


@app.get("/api/admin/truncate_prices")
async def truncate_prices_endpoint():
    """NUCLEAR OPTION — wipes all rows in the prices table and
    instantly returns the disk to Neon. Charts that depend on the
    1H / 6H DB-backed windows will be empty until WS flushes refill
    them (~1 hour). 24H+ chart windows are unaffected (they fetch
    from Kalshi's REST API, not the DB)."""
    try:
        from db import engine
        from sqlalchemy import text as _text
        if engine is None:
            return {"error": "database not configured"}
        async with engine.connect() as conn:
            await conn.execute(_text("COMMIT"))
            await conn.execute(_text("TRUNCATE TABLE prices"))
        return {"status": "ok", "message": "prices table truncated — DB instantly freed"}
    except Exception as e:
        return JSONResponse({"error": str(e)[:400]}, status_code=500)


@app.get("/api/admin/db_size")
async def db_size_endpoint():
    """Report per-table disk usage so we can see what's eating the
    Neon free-tier budget."""
    try:
        from db import engine
        from sqlalchemy import text as _text
        if engine is None:
            return {"error": "database not configured"}
        async with engine.connect() as conn:
            r = await conn.execute(_text("""
                SELECT
                    relname AS table,
                    pg_size_pretty(pg_total_relation_size(C.oid)) AS total_size,
                    pg_total_relation_size(C.oid) AS bytes
                FROM pg_class C
                LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
                WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                  AND C.relkind = 'r'
                ORDER BY pg_total_relation_size(C.oid) DESC
                LIMIT 20
            """))
            tables = [{"table": row[0], "size": row[1], "bytes": row[2]}
                      for row in r.fetchall()]
            r2 = await conn.execute(_text(
                "SELECT pg_size_pretty(pg_database_size(current_database())), "
                "pg_database_size(current_database())"
            ))
            row = r2.fetchone()
            total_pretty, total_bytes = row[0], row[1]
        return {
            "total_size": total_pretty,
            "total_bytes": total_bytes,
            "tables": tables,
        }
    except Exception as e:
        return JSONResponse({"error": str(e)[:400]}, status_code=500)


@app.get("/api/admin/ensure_snapshots_table")
async def ensure_snapshots_table_endpoint():
    """Force-create the snapshots table. Call once after a deploy
    that added the Snapshot model if `init_db()` didn't pick it up.
    Idempotent — CREATE TABLE IF NOT EXISTS."""
    try:
        from db import _ensure_snapshots_table, _snapshots_table_ensured
        import db as _db
        _db._snapshots_table_ensured = False  # force a retry
        err = await _ensure_snapshots_table()
        if err:
            return JSONResponse({"error": err}, status_code=500)
        return {"status": "ok", "ensured": _db._snapshots_table_ensured}
    except Exception as e:
        return JSONResponse({"error": str(e)[:400]}, status_code=500)


@app.post("/api/snapshot")
async def create_snapshot_endpoint(request: Request):
    """Persist a Snap/Pause freeze so the user can share a URL.
    Body: { section: "markets" | "orderbook" | "capflow",
            event_ticker: "KX...",  (optional, for context)
            data: {...}            (the snapshot object built
                                    client-side by _captureSnapshot) }
    Returns: { id, url, expires_at }."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)
    section = (body.get("section") or "").strip()
    data = body.get("data")
    event_ticker = (body.get("event_ticker") or "").strip()
    if section not in ("markets", "orderbook", "capflow"):
        return JSONResponse({"error": "section must be markets|orderbook|capflow"},
                            status_code=400)
    if not isinstance(data, dict):
        return JSONResponse({"error": "data must be an object"}, status_code=400)
    # Guard against oversized payloads (pathological trade floods).
    try:
        if len(json.dumps(data)) > 512_000:
            return JSONResponse({"error": "snapshot too large (>512 KB)"},
                                status_code=413)
    except Exception:
        pass
    try:
        from db import create_snapshot as _db_create
    except Exception:
        return JSONResponse({"error": "snapshot storage unavailable"},
                            status_code=503)
    slug, err = await _db_create(section, data, event_ticker=event_ticker)
    if not slug:
        return JSONResponse(
            {"error": err or "failed to persist snapshot"},
            status_code=500,
        )
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    expires = (_dt.now(_tz.utc) + _td(days=30)).isoformat()
    return {"id": slug, "url": f"/s/{slug}", "expires_at": expires}


@app.get("/api/snapshot/{slug}")
async def get_snapshot_endpoint(slug: str):
    """Fetch a shared snapshot by id. Returns 404 if not found or
    expired."""
    slug = (slug or "").strip()
    if not slug:
        return JSONResponse({"error": "id required"}, status_code=400)
    try:
        from db import get_snapshot as _db_get
    except Exception:
        return JSONResponse({"error": "snapshot storage unavailable"},
                            status_code=503)
    snap = await _db_get(slug)
    if not snap:
        return JSONResponse({"error": "snapshot not found or expired"},
                            status_code=404)
    return snap


@app.get("/s/{slug}", response_class=HTMLResponse)
def snapshot_page(slug: str):
    """Pretty share URL. Serves the same HTML shell as /, with a
    <meta name='stochverse-snapshot' content='{slug}'> hint so the JS
    knows to render in read-only snapshot mode on boot."""
    slug = (slug or "").strip()
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "index.html")
    if not _os.path.exists(p):
        return HTMLResponse("<h1>snapshot page missing</h1>")
    global _INDEX_HTML_CACHE
    mtime = _os.path.getmtime(p)
    if _INDEX_HTML_CACHE.get("mtime") != mtime:
        with open(p, "r", encoding="utf-8") as f:
            html = f.read()
        html = html.replace("<!--__ANALYTICS__-->", _analytics_snippet())
        _INDEX_HTML_CACHE["html"] = html
        _INDEX_HTML_CACHE["mtime"] = mtime
    html = _INDEX_HTML_CACHE["html"].replace(
        "</head>",
        f'<meta name="stochverse-snapshot" content="{slug}"></head>',
        1,
    )
    return HTMLResponse(html, headers={
        "Cache-Control": "public, max-age=60, must-revalidate",
    })


@app.get("/api/health")
def get_health():
    """Liveness / readiness probe for Railway and uptime monitors.
    Returns 200 with status info even on a cold cache so the
    container is considered healthy immediately after boot; the
    response body reports whether heavy subsystems (cache, WS) are
    ready separately, which is useful for alerting."""
    info = {"status": "ok"}
    try:
        info["cache_primed"] = _cache.get("data") is not None
        info["cache_records"] = len(_cache.get("data") or [])
        info["cache_age_s"] = int(time.time() - _cache.get("ts", 0)) if _cache.get("ts") else None
    except Exception:
        info["cache_primed"] = False
    try:
        from kalshi_ws import STATUS as _ws_status, LIVE_PRICES as _lp
        info["ws_connected"] = bool(_ws_status.get("connected"))
        info["ws_subscribed"] = _ws_status.get("subscribed", 0)
        info["live_prices"] = len(_lp)
    except Exception:
        info["ws_connected"] = False
    # Sentry + Analytics status so we can verify they're wired up
    # without shelling into Railway logs.
    info["sentry_enabled"] = bool(_SENTRY_DSN)
    info["analytics_enabled"] = bool(os.environ.get("ANALYTICS_DOMAIN", "").strip())
    return info


@app.get("/api/meta")
def get_meta():
    """Fast endpoint - returns static categories and sports list without waiting for data fetch."""
    # Build static soccer comps from SOCCER_COMP values
    soccer_comps = sorted(set(v for v in SOCCER_COMP.values() if v not in ("Other","")))
    sports_list = []
    for k in _SPORT_SERIES.keys():
        if k == "Soccer":
            subtabs = soccer_comps
        else:
            tabs_def = SPORT_SUBTABS.get(k, [])
            subtabs = [t for t,_ in tabs_def] if tabs_def else []
        sports_list.append({"name": k, "count": 0, "icon": SPORT_ICONS.get(k,"🏆"), "subtabs": subtabs})
    cats_list = [{"name": c, "count": 0, "subtabs": CAT_TAGS.get(c, [])} for c in TOP_CATS]
    return {"categories": cats_list, "sports": sports_list, "soccer_comps": soccer_comps}

@app.get("/api/categories")
def get_categories():
    records = get_data()
    # Count by display name
    display_counts = {}
    for r in records:
        c = r["category"]
        disp = CAT_DISPLAY.get(c, c)
        display_counts[disp] = display_counts.get(disp, 0) + 1
    return {"categories": [
        {"name": d, "count": display_counts.get(d, 0), "subtabs": CAT_TAGS.get(d, [])}
        for d in TOP_CATS if display_counts.get(d, 0) > 0
    ]}

@app.get("/api/refresh")
def refresh():
    global _cache
    _cache = {"data": None, "ts": 0}  # cache cleared on startup
    return {"ok": True}

@app.get("/api/ws_status")
def ws_status():
    """Debug endpoint: reports the Kalshi WebSocket connection state,
    how many markets have received at least one live price tick, and
    the health of the DB flush pipeline (last successful write,
    consecutive error count, last error message)."""
    try:
        from kalshi_ws import STATUS, LIVE_PRICES
        out = {"status": dict(STATUS), "live_count": len(LIVE_PRICES)}
    except Exception as e:
        out = {"status": None, "error": str(e)}
    try:
        from db import _flush_health
        out["flush"] = dict(_flush_health)
    except Exception:
        out["flush"] = None
    return out

@app.get("/api/ws_raw")
def ws_raw():
    """Debug endpoint: returns the last ~30 raw messages received from
    the Kalshi WebSocket, so we can inspect the exact schema."""
    try:
        from kalshi_ws import RAW_SAMPLES
        return {"samples": list(RAW_SAMPLES)}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/espn_status")
def espn_status():
    """Debug endpoint: reports the ESPN scoreboard poller state."""
    try:
        from espn_feed import STATUS, ESPN_GAMES
        return {"status": dict(STATUS), "games": len(ESPN_GAMES)}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sportsdb_day_probe")
async def sportsdb_day_probe(sport: str = "Basketball", d: str = ""):
    """Debug: TheSportsDB's /livescore.php endpoint is Patreon-only
    (confirmed 404 on free key 3), but their /eventsday.php endpoint
    IS free and returns all scheduled events for a given date with
    intHomeScore / intAwayScore / strStatus / strProgress fields.
    For in-progress matches, these fields may be updated in near
    real time. Probe the endpoint for the given sport/date to see
    whether the free tier returns usable live data for games we
    care about (e.g. Turkish Basketball, J League)."""
    try:
        import httpx
        if not d:
            d = date.today().isoformat()
        sport_enc = sport.replace(" ", "%20")
        url = f"https://www.thesportsdb.com/api/v1/json/3/eventsday.php?d={d}&s={sport_enc}"
        async with httpx.AsyncClient(headers={"User-Agent": "stochverse/1.0"}) as client:
            r = await client.get(url, timeout=15.0)
            out = {"sport": sport, "date": d, "status_code": r.status_code, "url": url}
            if r.status_code != 200:
                out["body_raw"] = r.text[:800]
                return out
            try:
                data = r.json() or {}
            except Exception as e:
                out["parse_error"] = str(e)
                out["body_raw"] = r.text[:800]
                return out
            events = data.get("events")
            if not isinstance(events, list):
                out["event_count"] = 0
                out["raw_body_preview"] = str(data)[:500]
                return out
            out["event_count"] = len(events)
            # Show which statuses are present — tells us if any games
            # are currently in progress.
            statuses: Dict[str, int] = {}
            live_with_score = 0
            sample_live = None
            for ev in events:
                st = (ev.get("strStatus") or "").strip() or "(empty)"
                statuses[st] = statuses.get(st, 0) + 1
                is_live = st.lower() not in ("", "(empty)", "not started", "match finished", "ft", "finished", "cancelled", "postponed")
                has_score = ev.get("intHomeScore") not in (None, "") and ev.get("intAwayScore") not in (None, "")
                if is_live and has_score and sample_live is None:
                    sample_live = {
                        "home": ev.get("strHomeTeam"),
                        "away": ev.get("strAwayTeam"),
                        "home_score": ev.get("intHomeScore"),
                        "away_score": ev.get("intAwayScore"),
                        "status": ev.get("strStatus"),
                        "progress": ev.get("strProgress"),
                        "league": ev.get("strLeague"),
                    }
                if is_live and has_score:
                    live_with_score += 1
            out["status_breakdown"] = statuses
            out["live_with_score_count"] = live_with_score
            out["sample_live_event"] = sample_live
            out["first_event_fields"] = sorted(list(events[0].keys())) if events else []
            out["first_event"] = events[0] if events else None
            return out
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

@app.get("/api/kalshi_event_raw")
def kalshi_event_raw(ticker: str = "", status: str = "", prefer: str = "sport"):
    """Debug: fetches Kalshi events and returns the full raw
    structure of one of them — every field on the event and on
    each of its markets, plus a per-status / per-result summary
    of the markets for quick eyeballing of settled vs active.

    If `ticker` is given, searches both open and closed listings
    (up to 15 pages each) and short-circuits as soon as found.
    If empty, returns the first matching sport event in the
    first few pages.
    """
    # Kalshi tickers are canonically uppercase. Browsers sometimes
    # percent-lowercase query strings and users often paste the
    # lowercase form from logs — normalize here so the lookup hits.
    ticker = (ticker or "").strip().upper()
    try:
        client = get_client()
        picked = None
        statuses_to_try = [status] if status else ["open", "closed"]
        all_seen = 0
        # Fast path: if we have a specific ticker, try Kalshi's direct
        # event lookup endpoint first. Works for any event Kalshi knows
        # about, regardless of pagination depth or unusual status.
        if ticker:
            try:
                resp = client.get_event(event_ticker=ticker).to_dict()
                ev_direct = resp.get("event")
                if isinstance(ev_direct, dict) and ev_direct.get("event_ticker") == ticker:
                    # Kalshi's single-event endpoint returns markets
                    # under resp["markets"] not nested in the event.
                    mk = resp.get("markets")
                    if isinstance(mk, list):
                        ev_direct = dict(ev_direct)
                        ev_direct["markets"] = mk
                    picked = ev_direct
            except Exception:
                # Fall through to pagination-based search below.
                pass
        for s in statuses_to_try:
            if picked:
                break
            events: List[Dict[str, Any]] = []
            cursor = None
            max_pages = 15 if ticker else 6
            for _ in range(max_pages):
                kw = {"limit": 200, "status": s, "with_nested_markets": True}
                if cursor:
                    kw["cursor"] = cursor
                try:
                    resp = client.get_events(**kw).to_dict()
                except Exception as e:
                    return {"error": f"get_events error on status={s}: {e}"}
                page = resp.get("events", []) or []
                events.extend(page)
                all_seen += len(page)
                cursor = resp.get("cursor") or resp.get("next_cursor")
                if ticker:
                    for ev in page:
                        if ev.get("event_ticker") == ticker:
                            picked = ev
                            break
                    if picked:
                        break
                if not cursor:
                    break
            if picked:
                break
            if not ticker and events:
                if prefer == "sport":
                    for ev in events:
                        series = str(ev.get("series_ticker") or "").upper()
                        if get_sport(series):
                            picked = ev
                            break
                if picked is None:
                    picked = events[0]
                break

        if not picked:
            if ticker:
                return {
                    "error": f"ticker {ticker!r} not found in {all_seen} open+closed events",
                    "hint": "event may be too deep in pagination or in an unusual state",
                }
            return {"error": "no events returned"}

        markets = picked.get("markets") or []
        first_market = markets[0] if markets else {}
        all_market_fields = set()
        status_counts: Dict[str, int] = {}
        result_counts: Dict[str, int] = {}
        sample_settled = None
        sample_active = None
        compact_markets = []
        for mk in markets:
            if isinstance(mk, dict):
                all_market_fields.update(mk.keys())
                s_val = str(mk.get("status") or "")
                r_val = str(mk.get("result") or "")
                status_counts[s_val] = status_counts.get(s_val, 0) + 1
                result_counts[r_val] = result_counts.get(r_val, 0) + 1
                if sample_settled is None and (r_val or s_val not in ("active", "")):
                    sample_settled = mk
                if sample_active is None and s_val == "active" and not r_val:
                    sample_active = mk
                # Compact per-market summary so every team/outcome
                # is visible in a single probe response without
                # dumping 30+ full dicts.
                compact_markets.append({
                    "ticker":          mk.get("ticker"),
                    "yes_sub_title":   mk.get("yes_sub_title"),
                    "status":          mk.get("status"),
                    "result":          mk.get("result"),
                    "yes_bid_dollars": mk.get("yes_bid_dollars"),
                    "yes_ask_dollars": mk.get("yes_ask_dollars"),
                    "no_bid_dollars":  mk.get("no_bid_dollars"),
                    "no_ask_dollars":  mk.get("no_ask_dollars"),
                    "yes_bid_size_fp": mk.get("yes_bid_size_fp"),
                    "yes_ask_size_fp": mk.get("yes_ask_size_fp"),
                    "no_bid_size_fp":  mk.get("no_bid_size_fp"),
                    "no_ask_size_fp":  mk.get("no_ask_size_fp"),
                    "last_price_dollars": mk.get("last_price_dollars"),
                    "volume_fp":       mk.get("volume_fp"),
                    "volume_24h_fp":   mk.get("volume_24h_fp"),
                    "open_interest_fp": mk.get("open_interest_fp"),
                    "liquidity_dollars": mk.get("liquidity_dollars"),
                })
        return {
            "event_ticker": picked.get("event_ticker"),
            "series_ticker": picked.get("series_ticker"),
            "derived_sport": get_sport(str(picked.get("series_ticker") or "").upper()),
            "event_fields": sorted(list(picked.keys())),
            "event_top_level_only": {k: v for k, v in picked.items() if k != "markets"},
            "market_count": len(markets),
            "market_status_counts": status_counts,
            "market_result_counts": result_counts,
            "first_market_fields": sorted(list(first_market.keys())) if isinstance(first_market, dict) else None,
            "union_of_all_market_fields": sorted(list(all_market_fields)),
            "sample_active_market": sample_active,
            "sample_settled_market": sample_settled,
            "all_markets_compact": compact_markets,
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/unmapped_series")
def unmapped_series():
    """Debug: list all series_tickers in the current Kalshi cache
    whose sport can't be resolved — neither the hardcoded
    SERIES_SPORT map nor the entity-alias fallback could classify
    them. Each unresolved series is reported with a count of events
    and a few sample titles so we can decide whether to add them
    to the mapping or wait for the entity cache to pick them up.

    Also reports which previously-unmapped series WERE resolved
    via the entity fallback, so we can see that system working."""
    get_data()
    records = _cache.get("data_all") or _cache.get("data") or []
    try:
        from db import get_sport_from_entities, ALIAS_SPORT_CACHE
    except Exception:
        get_sport_from_entities = lambda _t: ""  # type: ignore
        ALIAS_SPORT_CACHE = {}

    unresolved: dict = {}      # series → {count, titles[]}
    via_entities: dict = {}    # series → {count, sample}

    for r in records:
        cat = r.get("category", "")
        if cat != "Sports":
            continue
        series = str(r.get("series_ticker") or "").upper()
        if not series:
            continue
        hardcoded = get_sport(series)
        title = r.get("title", "")
        if hardcoded:
            continue
        # Series not in the hardcoded map — try entity fallback
        entity_sport = get_sport_from_entities(title) if title else ""
        if entity_sport:
            bucket = via_entities.setdefault(series, {
                "count": 0, "sport": entity_sport, "samples": []
            })
            bucket["count"] += 1
            if len(bucket["samples"]) < 3:
                bucket["samples"].append(title[:80])
        else:
            bucket = unresolved.setdefault(series, {
                "count": 0, "samples": []
            })
            bucket["count"] += 1
            if len(bucket["samples"]) < 3:
                bucket["samples"].append(title[:80])

    return {
        "cache_size": len(ALIAS_SPORT_CACHE),
        "unresolved_count": sum(v["count"] for v in unresolved.values()),
        "via_entities_count": sum(v["count"] for v in via_entities.values()),
        "unresolved": dict(sorted(
            unresolved.items(), key=lambda kv: -kv[1]["count"]
        )),
        "resolved_via_entities": dict(sorted(
            via_entities.items(), key=lambda kv: -kv[1]["count"]
        )),
    }


@app.get("/api/kalshi_search")
def kalshi_search(q: str = "", limit: int = 20):
    """Debug: search the cached Kalshi REST snapshot for any event
    whose title, sub_title, or event_ticker contains `q` (case-
    insensitive). Triggers a cache rebuild if none exists, so
    calling this right after /api/refresh will block briefly and
    then return populated results instead of 0."""
    if not q:
        return {"error": "q required"}
    needle = q.lower()
    records = get_data()
    hits = []
    seen_tickers = set()
    for r in records:
        t = (r.get("title") or "").lower()
        st = (r.get("sub_title") or "").lower() if r.get("sub_title") else ""
        tk = (r.get("event_ticker") or "").lower()
        if needle in t or needle in st or needle in tk:
            et = r.get("event_ticker")
            if et in seen_tickers:
                continue
            seen_tickers.add(et)
            hits.append({
                "event_ticker": r.get("event_ticker"),
                "title":        r.get("title"),
                "series_ticker": r.get("series_ticker"),
                "category":     r.get("category"),
                "outcome_count": len(r.get("outcomes") or []),
            })
            if len(hits) >= limit:
                break
    return {"q": q, "count": len(hits), "hits": hits}

@app.get("/api/kalshi_data_audit")
def kalshi_data_audit(
    sport: str = "",
    series: str = "",
    limit: int = 50,
    suspicious_only: bool = True,
):
    """Diagnostic: walk the cached REST snapshot and flag events
    whose outcomes render blank ("—") in the Stochverse UI. For each
    outcome we re-run the exact same dead-market rules as
    _format_outcomes (two-sided book + last_price override), then
    classify each dead row as one of:
      - no_book           → no orders on either side anywhere; truly dead
      - one_sided_bid     → only bids, no asks (stale/resolved)
      - one_sided_ask     → only asks, no bids (stale futures market)
      - suspicious        → one side has orders AND the other side
                            also has orders but was hidden anyway
                            (shouldn't happen — would be a bug)
    Also reports any outcome with last_price>0 that still rendered
    as —, which flags a fix regression.

    Query args:
      sport=Soccer         filter to a single sport
      series=KXEFLCHAMP..  filter to a specific series ticker
      limit=50             max events to return
      suspicious_only=1    only return events with at least one dead
                           outcome (default) — set to 0 to see every
                           event's outcome health

    Typical usage: hit /api/kalshi_data_audit?sport=Soccer to find
    every soccer card where one or more rows are blank, so we can
    tell at a glance whether Stochverse is hiding data that Kalshi
    itself shows.
    """
    records = get_data()
    try:
        from kalshi_ws import LIVE_PRICES
    except Exception:
        LIVE_PRICES = {}
    flagged = []
    totals = {
        "events_scanned": 0,
        "events_with_dead": 0,
        "events_with_suspicious": 0,
        "outcomes_scanned": 0,
        "outcomes_dead": 0,
        "outcomes_suspicious": 0,
    }
    for r in records:
        if sport and r.get("_sport") != sport:
            continue
        if series and r.get("series_ticker") != series:
            continue
        totals["events_scanned"] += 1
        outcomes = r.get("outcomes") or []
        dead_rows = []
        suspicious_rows = []
        for o in outcomes:
            totals["outcomes_scanned"] += 1
            tk = o.get("ticker", "")
            yb = o.get("_yb"); ya = o.get("_ya")
            nb = o.get("_nb"); na = o.get("_na")
            live = LIVE_PRICES.get(tk) if tk else None
            if live:
                if live.get("yes_bid") is not None: yb = live["yes_bid"]
                if live.get("yes_ask") is not None: ya = live["yes_ask"]
                if live.get("no_bid")  is not None: nb = live["no_bid"]
                if live.get("no_ask")  is not None: na = live["no_ask"]
            yb_sz = o.get("_yb_sz") or 0
            ya_sz = o.get("_ya_sz") or 0
            nb_sz = o.get("_nb_sz") or 0
            na_sz = o.get("_na_sz") or 0
            vol   = o.get("_vol")   or 0
            oi    = o.get("_oi")    or 0
            bid_side = (yb_sz > 0) or (na_sz > 0)
            ask_side = (ya_sz > 0) or (nb_sz > 0)
            last  = o.get("_last")
            if live and live.get("last_price") is not None:
                last = live["last_price"]
            has_last = last is not None and last > 0
            two_sided = bid_side and ask_side
            would_render = two_sided or has_last
            if would_render:
                continue
            # Row will render as —. Classify why.
            if not bid_side and not ask_side:
                reason = "no_book"
            elif bid_side and not ask_side:
                reason = "one_sided_bid"
            elif ask_side and not bid_side:
                reason = "one_sided_ask"
            else:
                reason = "unknown"
            row_info = {
                "ticker":   tk,
                "label":    o.get("label", ""),
                "reason":   reason,
                "yb":       yb,  "ya": ya,  "nb": nb,  "na": na,
                "yb_sz":    yb_sz, "ya_sz": ya_sz,
                "nb_sz":    nb_sz, "na_sz": na_sz,
                "vol":      vol,
                "oi":       oi,
                "last":     last,
            }
            dead_rows.append(row_info)
            totals["outcomes_dead"] += 1
            # Flag anything that "shouldn't" be dead but is — e.g.
            # has trading history (last_price>0) but no current book
            # AND our rule hid it. This should never happen under
            # the current fix (has_last short-circuits above) but
            # guards against future regressions.
            if has_last:
                suspicious_rows.append(row_info)
                totals["outcomes_suspicious"] += 1
        if dead_rows:
            totals["events_with_dead"] += 1
        if suspicious_rows:
            totals["events_with_suspicious"] += 1
        if suspicious_only and not dead_rows:
            continue
        if len(flagged) >= limit:
            continue
        flagged.append({
            "event_ticker":  r.get("event_ticker"),
            "title":         r.get("title"),
            "series_ticker": r.get("series_ticker"),
            "sport":         r.get("_sport"),
            "total_outcomes": len(outcomes),
            "dead_count":    len(dead_rows),
            "suspicious_count": len(suspicious_rows),
            "dead_rows":     dead_rows[:10],  # cap per-event noise
        })
    return {
        "filter": {"sport": sport or None, "series": series or None,
                   "suspicious_only": suspicious_only, "limit": limit},
        "totals": totals,
        "events":  flagged,
    }

@app.get("/api/espn_probe")
async def espn_probe(slug: str):
    """Debug: make a raw call to ESPN's scoreboard endpoint for the
    given slug (e.g. "tennis/atp", "basketball/euroleague") and
    return status code + event count + a sample event so we can see
    what ESPN actually publishes. Useful for figuring out why a
    slug returns 200 OK but 0 matched events."""
    try:
        import httpx
        url = f"https://site.api.espn.com/apis/site/v2/sports/{slug}/scoreboard"
        async with httpx.AsyncClient(headers={"User-Agent": "stochverse/1.0"}) as client:
            r = await client.get(url, timeout=15.0)
            out = {"slug": slug, "status_code": r.status_code}
            if r.status_code != 200:
                out["body_raw"] = r.text[:500]
                return out
            try:
                data = r.json() or {}
            except Exception as e:
                out["parse_error"] = str(e)
                return out
            events = data.get("events") or []
            out["event_count"] = len(events) if isinstance(events, list) else None
            out["league_name"] = (data.get("leagues") or [{}])[0].get("name", "")
            if events and isinstance(events, list):
                ev = events[0]
                out["sample_event_id"] = ev.get("id")
                out["sample_event_name"] = ev.get("name") or ev.get("shortName")
                # Full status object — includes clock, period,
                # displayClock, and any addedTime/stoppage fields
                out["sample_status"] = ev.get("status")
                comps = (ev.get("competitions") or [{}])[0]
                cps = comps.get("competitors") or []
                out["sample_competitor_count"] = len(cps)
                for i, cp in enumerate(cps[:2]):
                    out[f"competitor_{i}"] = {
                        "id": cp.get("id"),
                        "homeAway": cp.get("homeAway"),
                        "team": (cp.get("team") or {}).get("displayName"),
                        "score": cp.get("score"),
                        "statistics": cp.get("statistics"),
                    }
                # Include situation object (may contain clock details)
                out["situation"] = comps.get("situation")
            return out
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

@app.get("/api/espn_raw")
def espn_raw():
    """Debug endpoint: returns the current ESPN_GAMES list so we can
    inspect what matched from each league."""
    try:
        from espn_feed import ESPN_GAMES
        return {"games": list(ESPN_GAMES)[:50]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sportsdb_status")
def sportsdb_status():
    """Debug endpoint: reports the TheSportsDB poller state."""
    try:
        from sportsdb_feed import STATUS, SPORTSDB_GAMES
        return {"status": dict(STATUS), "games": len(SPORTSDB_GAMES)}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sportsdb_raw")
def sportsdb_raw():
    """Debug endpoint: returns the current SPORTSDB_GAMES list."""
    try:
        from sportsdb_feed import SPORTSDB_GAMES
        return {"games": list(SPORTSDB_GAMES)[:50]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sportsdb_probe")
async def sportsdb_probe():
    """Debug: makes a fresh call to TheSportsDB's Soccer livescore
    endpoint and returns the raw response so we can tell whether the
    free key is actually getting live data (vs being gated behind
    their Patreon tier)."""
    try:
        import httpx
        from sportsdb_feed import BASE_URL
        url = f"{BASE_URL}/livescore.php?s=Soccer"
        async with httpx.AsyncClient() as client:
            r = await client.get(url, timeout=15.0)
            ct = r.headers.get("content-type", "")
            out = {"status_code": r.status_code, "content_type": ct}
            if "json" in ct:
                try:
                    out["body"] = r.json()
                except Exception:
                    out["body_raw"] = r.text[:2000]
            else:
                out["body_raw"] = r.text[:2000]
            return out
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sofascore_status")
def sofascore_status():
    """Debug endpoint: reports the SofaScore poller state."""
    try:
        from sofascore_feed import STATUS, SOFASCORE_GAMES
        return {"status": dict(STATUS), "games": len(SOFASCORE_GAMES)}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sofascore_raw")
def sofascore_raw():
    """Debug endpoint: returns the current SOFASCORE_GAMES list."""
    try:
        from sofascore_feed import SOFASCORE_GAMES
        return {"games": list(SOFASCORE_GAMES)[:50]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/debug_team_search")
def debug_team_search(q: str, sport: str = "Soccer"):
    """Debug: substring-search ESPN_GAMES for any game whose home or
    away team display name / phrase contains `q` (case-insensitive,
    accent-insensitive). Useful for figuring out whether ESPN has a
    given team at all, and under what exact name + which league."""
    try:
        from espn_feed import ESPN_GAMES, _normalize
        needle = _normalize(q)
        if not needle:
            return {"q": q, "sport": sport, "hits": []}
        hits = []
        for g in ESPN_GAMES:
            if sport and g.get("sport") != sport:
                continue
            phrases = (g.get("home_phrases", []) or []) + (g.get("away_phrases", []) or [])
            home_hit = needle in _normalize(g.get("home_display", ""))
            away_hit = needle in _normalize(g.get("away_display", ""))
            phrase_hit = any(needle in p for p in phrases)
            if home_hit or away_hit or phrase_hit:
                hits.append({
                    "league": g.get("league"),
                    "home": g.get("home_display"),
                    "away": g.get("away_display"),
                    "home_phrases": g.get("home_phrases"),
                    "away_phrases": g.get("away_phrases"),
                    "state": g.get("state"),
                    "home_score": g.get("home_score"),
                    "away_score": g.get("away_score"),
                })
            if len(hits) >= 20:
                break
        return {"q": q, "sport": sport, "count": len(hits), "hits": hits}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

@app.get("/api/debug_sofa_search")
def debug_sofa_search(q: str = "", sport: str = ""):
    """Same as debug_team_search but against SOFASCORE_GAMES. Useful
    for figuring out whether SofaScore carries a specific match when
    ESPN doesn't. If `q` is empty, returns the first 20 games for
    the sport filter (or all sports if no filter)."""
    try:
        from sofascore_feed import SOFASCORE_GAMES, _normalize
        needle = _normalize(q) if q else ""
        hits = []
        for g in SOFASCORE_GAMES:
            if sport and g.get("sport") != sport:
                continue
            if needle:
                home_display_n = _normalize(g.get("home_display", ""))
                away_display_n = _normalize(g.get("away_display", ""))
                phrases = (g.get("home_phrases", []) or []) + (g.get("away_phrases", []) or [])
                if not (needle in home_display_n or needle in away_display_n
                        or any(needle in p for p in phrases)):
                    continue
            hits.append({
                "sport": g.get("sport"),
                "league": g.get("league"),
                "home": g.get("home_display"),
                "away": g.get("away_display"),
                "home_phrases": g.get("home_phrases"),
                "away_phrases": g.get("away_phrases"),
                "state": g.get("state"),
                "home_score": g.get("home_score"),
                "away_score": g.get("away_score"),
                "short_detail": g.get("short_detail"),
            })
            if len(hits) >= 20:
                break
        return {"q": q, "sport": sport, "count": len(hits), "hits": hits}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

@app.get("/api/debug_live")
def debug_live(title: str, sport: str = "Soccer"):
    """Debug: runs match_game against ESPN and SofaScore for the
    given (title, sport) and returns the raw game dict each feed
    would provide — display_clock, period, captured_at_ms, state,
    scores, team phrases, etc. Use this to figure out why a specific
    live card's clock or score looks wrong."""
    out: Dict[str, Any] = {"title": title, "sport": sport}
    try:
        from espn_feed import match_game as em, _normalize as en
        import time as _t
        g = em(title, sport) if em else None
        if g:
            age_s = None
            if g.get("captured_at_ms"):
                age_s = round((_t.time() * 1000 - g["captured_at_ms"]) / 1000, 1)
            out["espn"] = {
                "league": g.get("league"),
                "home": g.get("home_display"),
                "away": g.get("away_display"),
                "home_score": g.get("home_score"),
                "away_score": g.get("away_score"),
                "state": g.get("state"),
                "display_clock": g.get("display_clock"),
                "period": g.get("period"),
                "clock_running": g.get("clock_running"),
                "short_detail": g.get("short_detail"),
                "captured_age_seconds": age_s,
                "home_phrases": g.get("home_phrases"),
                "away_phrases": g.get("away_phrases"),
                "is_playoff": g.get("is_playoff"),
                "series_summary": g.get("series_summary"),
                "series_home_wins": g.get("series_home_wins"),
                "series_away_wins": g.get("series_away_wins"),
                "is_two_leg": g.get("is_two_leg"),
                "aggregate_home": g.get("aggregate_home"),
                "aggregate_away": g.get("aggregate_away"),
                "leg_number": g.get("leg_number"),
                "round_name": g.get("round_name"),
            }
        else:
            out["espn"] = None
    except Exception as e:
        out["espn_error"] = f"{type(e).__name__}: {e}"
    try:
        from sofascore_feed import match_game as sm
        import time as _t
        g = sm(title, sport) if sm else None
        if g:
            age_s = None
            if g.get("captured_at_ms"):
                age_s = round((_t.time() * 1000 - g["captured_at_ms"]) / 1000, 1)
            out["sofascore"] = {
                "league": g.get("league"),
                "home": g.get("home_display"),
                "away": g.get("away_display"),
                "home_score": g.get("home_score"),
                "away_score": g.get("away_score"),
                "state": g.get("state"),
                "display_clock": g.get("display_clock"),
                "period": g.get("period"),
                "clock_running": g.get("clock_running"),
                "short_detail": g.get("short_detail"),
                "captured_age_seconds": age_s,
                "home_phrases": g.get("home_phrases"),
                "away_phrases": g.get("away_phrases"),
                "is_two_leg": g.get("is_two_leg"),
                "aggregate_home": g.get("aggregate_home"),
                "aggregate_away": g.get("aggregate_away"),
                "leg_number": g.get("leg_number"),
                "round_name": g.get("round_name"),
                "tournament_name": g.get("tournament_name"),
                "aggregate_winner": g.get("aggregate_winner"),
                "sofa_event_id": g.get("_sofa_event_id"),
            }
        else:
            out["sofascore"] = None
    except Exception as e:
        out["sofascore_error"] = f"{type(e).__name__}: {e}"
    return out

@app.get("/api/debug_sofa")
def debug_sofa(title: str, sport: str = "Soccer"):
    """Debug: exercises sofascore_feed.match_game for a given title
    and sport, and dumps every game from SOFASCORE_GAMES whose home
    or away phrases overlap the title so we can see exactly why a
    match is or isn't happening."""
    try:
        from sofascore_feed import (
            SOFASCORE_GAMES, match_game, _normalize, _phrase_in_title,
        )
        t = _normalize(title)
        matched = match_game(title, sport)
        matching_sport_games = [g for g in SOFASCORE_GAMES if g.get("sport") == sport]
        out = {
            "title": title,
            "normalized_title": t,
            "sport": sport,
            "total_sofascore_games": len(SOFASCORE_GAMES),
            "games_in_sport": len(matching_sport_games),
            "matched": None,
            "partial_hits": [],
        }
        if matched:
            out["matched"] = {
                "league": matched.get("league"),
                "home": matched.get("home_display"),
                "away": matched.get("away_display"),
                "home_phrases": matched.get("home_phrases"),
                "away_phrases": matched.get("away_phrases"),
                "home_score": matched.get("home_score"),
                "away_score": matched.get("away_score"),
            }
            return out
        # Not matched — show any game where at least one side hits.
        for g in matching_sport_games:
            home_phrases = g.get("home_phrases", []) or []
            away_phrases = g.get("away_phrases", []) or []
            home_hits = [p for p in home_phrases if _phrase_in_title(p, t)]
            away_hits = [p for p in away_phrases if _phrase_in_title(p, t)]
            if home_hits or away_hits:
                out["partial_hits"].append({
                    "league": g.get("league"),
                    "home": g.get("home_display"),
                    "away": g.get("away_display"),
                    "home_phrases": home_phrases,
                    "away_phrases": away_phrases,
                    "home_hits": home_hits,
                    "away_hits": away_hits,
                })
        out["partial_hits"] = out["partial_hits"][:15]
        return out
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

@app.get("/api/sofascore_probe")
async def sofascore_probe(sport: str = "football"):
    """Debug: makes a fresh call to SofaScore's live events endpoint
    for a given sport (football/basketball/tennis/ice-hockey/...)
    and returns status, headers, and either the parsed event count
    + sample event or the first chunk of body."""
    try:
        import httpx
        url = f"https://api.sofascore.com/api/v1/sport/{sport}/events/live"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
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
        }
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            r = await client.get(url, timeout=15.0)
            out = {
                "status_code": r.status_code,
                "final_url": str(r.url),
                "content_type": r.headers.get("content-type", ""),
                "server": r.headers.get("server", ""),
                "cf_ray": r.headers.get("cf-ray", ""),
            }
            if "json" in out["content_type"]:
                try:
                    body = r.json()
                    events = body.get("events", []) if isinstance(body, dict) else []
                    out["event_count"] = len(events) if isinstance(events, list) else None
                    out["sample_event"] = events[0] if events else None
                except Exception:
                    out["body_raw"] = r.text[:1500]
            else:
                out["body_raw"] = r.text[:1500]
            return out
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

@app.get("/api/live_audit")
def live_audit():
    """Debug endpoint: reports the Live-tab pipeline end-to-end.
    How many cached records, how many pass the Live filter, how many
    have ESPN or SportsDB state attached, broken down by sport."""
    from datetime import datetime as _dt
    records = _cache.get("data") or []
    now_utc = _dt.now(timezone.utc)
    by_sport = {}
    total_live = 0
    for r in records:
        kdt = r.get("_kickoff_dt")
        gdt = r.get("_game_end_dt")
        if not (kdt and gdt):
            continue
        try:
            k = _dt.fromisoformat(kdt)
            g = _dt.fromisoformat(gdt)
        except Exception:
            continue
        if not (k <= now_utc < g):
            continue
        total_live += 1
        sp = r.get("_sport") or "(none)"
        by_sport[sp] = by_sport.get(sp, 0) + 1
    espn_matched = sportsdb_matched = sofascore_matched = unmatched = 0
    sample_unmatched = []
    try:
        from espn_feed import match_game as em
    except Exception:
        em = None
    try:
        from sportsdb_feed import match_game as sm
    except Exception:
        sm = None
    try:
        from sofascore_feed import match_game as fm
    except Exception:
        fm = None
    for r in records:
        kdt = r.get("_kickoff_dt")
        gdt = r.get("_game_end_dt")
        if not (kdt and gdt):
            continue
        try:
            k = _dt.fromisoformat(kdt)
            g = _dt.fromisoformat(gdt)
        except Exception:
            continue
        if not (k <= now_utc < g):
            continue
        title = r.get("title", "")
        sport = r.get("_sport", "")
        g_espn = em(title, sport) if em and sport and title else None
        if g_espn:
            espn_matched += 1
            continue
        g_sdb = sm(title, sport) if sm and sport and title else None
        if g_sdb:
            sportsdb_matched += 1
            continue
        g_sofa = fm(title, sport) if fm and sport and title else None
        if g_sofa:
            sofascore_matched += 1
            continue
        unmatched += 1
        if len(sample_unmatched) < 20:
            sample_unmatched.append({"title": title, "sport": sport})
    return {
        "total_cached": len(records),
        "total_live": total_live,
        "by_sport": by_sport,
        "espn_matched": espn_matched,
        "sportsdb_matched": sportsdb_matched,
        "sofascore_matched": sofascore_matched,
        "unmatched": unmatched,
        "sample_unmatched": sample_unmatched,
    }

@app.get("/api/debug_match")
def debug_match(title: str, sport: str = "Soccer"):
    """Debug endpoint: given a Kalshi-style title and sport, report
    whether any ESPN game matches, and if not, show candidate ESPN
    games for the sport whose team phrases match as whole words
    (same rules as the real matcher). Useful for figuring out why a
    specific live event isn't getting its score."""
    try:
        from espn_feed import ESPN_GAMES, match_game, _normalize, _phrase_in_title
        matched = match_game(title, sport)
        out = {"title": title, "sport": sport, "matched": None, "candidates": []}
        if matched:
            out["matched"] = {
                "league": matched.get("league"),
                "home_display": matched.get("home_display"),
                "away_display": matched.get("away_display"),
                "home_phrases": matched.get("home_phrases"),
                "away_phrases": matched.get("away_phrases"),
                "home_score": matched.get("home_score"),
                "away_score": matched.get("away_score"),
                "short_detail": matched.get("short_detail"),
                "state": matched.get("state"),
            }
            return out
        tl = _normalize(title)
        cands = []
        for g in ESPN_GAMES:
            if g.get("sport") != sport:
                continue
            home_hit = next((p for p in g.get("home_phrases", []) if _phrase_in_title(p, tl)), None)
            away_hit = next((p for p in g.get("away_phrases", []) if _phrase_in_title(p, tl)), None)
            if home_hit or away_hit:
                cands.append({
                    "league": g.get("league"),
                    "home_display": g.get("home_display"),
                    "away_display": g.get("away_display"),
                    "home_hit": home_hit,
                    "away_hit": away_hit,
                })
        out["candidates"] = cands[:15]
        return out
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/memory")
def memory_status():
    """Debug endpoint: current RSS + cache sizes, for spotting leaks
    or tuning memory pressure on Railway."""
    info = {}
    try:
        import resource, sys
        mem_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mb = mem_kb / 1024 if sys.platform != "darwin" else mem_kb / (1024 * 1024)
        info["rss_mb"] = round(mb, 1)
    except Exception as e:
        info["rss_error"] = str(e)
    try:
        cached = _cache.get("data") or []
        info["cached_records"] = len(cached)
    except Exception:
        info["cached_records"] = None
    try:
        from kalshi_ws import LIVE_PRICES as _lp
        info["live_prices"] = len(_lp)
    except Exception:
        info["live_prices"] = None
    try:
        from espn_feed import ESPN_GAMES as _eg
        info["espn_games"] = len(_eg)
    except Exception:
        info["espn_games"] = None
    return info

# ── Real-time price stream to connected browsers ──────────────────
@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    """Push real-time Kalshi price updates to the browser. Wire
    format:

      Client → Server (subscribe):
        {"action": "subscribe", "tickers": ["KX...-XXX", ...]}

      Client → Server (unsubscribe):
        {"action": "unsubscribe", "tickers": [...]}

      Server → Client (price delta):
        {"type": "price", "ticker": "KX...", "data": {
            "yes_bid": 87, "yes_ask": 88, "no_bid": 12, "no_ask": 13,
            "last_price": 87, "volume": 12345, ...
        }}

      Server → Client (hello):
        {"type": "hello", "ts": 1729300000000}
    """
    await websocket.accept()
    try:
        from kalshi_ws import (
            BrowserSubscriber, register_browser, unregister_browser,
            LIVE_PRICES, subscribe_ondemand, unsubscribe_ondemand,
        )
    except Exception as e:
        await websocket.close(code=1011)
        return
    sub = BrowserSubscriber()
    register_browser(sub)
    await websocket.send_json({"type": "hello", "ts": int(time.time() * 1000)})

    async def _reader():
        """Accept subscribe/unsubscribe messages from the client."""
        try:
            while True:
                msg = await websocket.receive_json()
                action = msg.get("action")
                tickers = msg.get("tickers") or []
                if action == "subscribe":
                    sub.subscribe([t.upper() for t in tickers if t])
                    snapshot = []
                    for t in tickers:
                        t = (t or "").upper()
                        if t in LIVE_PRICES:
                            snapshot.append({
                                "type": "price",
                                "ticker": t,
                                "data": LIVE_PRICES[t],
                            })
                    if snapshot:
                        await websocket.send_json({
                            "type": "snapshot",
                            "updates": snapshot,
                        })
                elif action == "unsubscribe":
                    sub.unsubscribe([t.upper() for t in tickers if t])
                elif action == "subscribe_channel":
                    # On-demand subscription to expensive channels
                    # (orderbook_delta, trade) for specific tickers.
                    channel = msg.get("channel", "")
                    if channel in ("orderbook_delta", "trade"):
                        for t in tickers:
                            t = (t or "").upper()
                            if t:
                                sub.subscribe([t])
                                await subscribe_ondemand(channel, t, id(sub))
                elif action == "unsubscribe_channel":
                    channel = msg.get("channel", "")
                    if channel in ("orderbook_delta", "trade"):
                        for t in tickers:
                            t = (t or "").upper()
                            if t:
                                await unsubscribe_ondemand(channel, t, id(sub))
                elif action == "ping":
                    await websocket.send_json({"type": "pong"})
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    async def _writer():
        """Forward broadcast messages from the subscriber queue."""
        try:
            while True:
                payload = await sub.queue.get()
                await websocket.send_json(payload)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    reader_task = asyncio.create_task(_reader())
    writer_task = asyncio.create_task(_writer())
    try:
        done, pending = await asyncio.wait(
            {reader_task, writer_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        unregister_browser(sub)
        # Clean up any on-demand channel subscriptions this browser had.
        try:
            from kalshi_ws import _ondemand_subs
            sub_id = id(sub)
            for key in list(_ondemand_subs.keys()):
                if sub_id in _ondemand_subs.get(key, set()):
                    channel, ticker = key
                    await unsubscribe_ondemand(channel, ticker, sub_id)
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            pass


# ── Serve frontend ─────────────────────────────────────────────────────────────
import os as _os

@app.get("/", response_class=HTMLResponse)
def root():
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "index.html")
    if not _os.path.exists(p):
        return HTMLResponse("<h1>static/index.html not found</h1><p>Make sure index.html is in the static/ folder</p>")
    # Read + substitute analytics snippet. Caches the rendered HTML
    # in memory so we don't re-read the file on every request — the
    # cache is invalidated on file mtime change so dev reloads work.
    global _INDEX_HTML_CACHE
    mtime = _os.path.getmtime(p)
    if _INDEX_HTML_CACHE.get("mtime") != mtime:
        with open(p, "r", encoding="utf-8") as f:
            html = f.read()
        html = html.replace("<!--__ANALYTICS__-->", _analytics_snippet())
        _INDEX_HTML_CACHE["html"] = html
        _INDEX_HTML_CACHE["mtime"] = mtime
    return HTMLResponse(
        _INDEX_HTML_CACHE["html"],
        headers={"Cache-Control": "public, max-age=60, must-revalidate"},
    )


_INDEX_HTML_CACHE = {"html": None, "mtime": None}


def _analytics_snippet() -> str:
    """Return the <script> tag to inject into index.html, based on
    environment variables. Currently supports Plausible (lightweight,
    privacy-friendly, GDPR-compliant without cookie banners).

    Set ANALYTICS_DOMAIN to your Plausible site domain (e.g.
    "stochverse.com") to enable. Self-hosted Plausible instances can
    point ANALYTICS_SCRIPT_URL at a custom script URL.
    """
    domain = os.environ.get("ANALYTICS_DOMAIN", "").strip()
    if not domain:
        return ""
    script_url = os.environ.get(
        "ANALYTICS_SCRIPT_URL",
        "https://plausible.io/js/script.js",
    )
    return (
        f'<script defer data-domain="{domain}" '
        f'src="{script_url}"></script>'
    )


@app.get("/api/market/{ticker}/orderbook")
def get_market_orderbook(ticker: str, depth: int = 100, debug: bool = False):
    """Fetch the full order book for a single market (outcome) ticker
    from Kalshi's /markets/{ticker}/orderbook endpoint. Returns
    structured asks + bids for both Trade Yes and Trade No views.

    Kalshi's orderbook response contains two arrays:
      - yes[]: [price, quantity] pairs representing offers to buy YES
      - no[]:  [price, quantity] pairs representing offers to buy NO

    For Trade Yes view:
      Asks (what you pay to buy YES)   = (100 - no_price, no_qty)
      Bids (what you receive for YES)  = yes entries as-is

    For Trade No view:
      Asks (what you pay to buy NO)    = (100 - yes_price, yes_qty)
      Bids (what you receive for NO)   = no entries as-is
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return {"error": "ticker required"}
    # Short-TTL cache shared across users. 3 s keeps the book fresh
    # enough for retail UX (it auto-refreshes on the client every 3 s
    # anyway) while ensuring we hit Kalshi at most once per ticker per
    # 3 s regardless of concurrent viewer count. Debug mode bypasses.
    cache_key = f"ob:{ticker}:{depth}"
    if not debug:
        cached = _mk_cache_get(cache_key)
        if cached is not None:
            return cached
    lock = _mk_cache_lock_for(cache_key)
    with lock:
        # Double-check after acquiring lock — another request may have
        # populated the cache while we were waiting.
        if not debug:
            cached = _mk_cache_get(cache_key)
            if cached is not None:
                return cached
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding
            import base64, httpx as _httpx
            key_str = os.environ.get("KALSHI_PRIVATE_KEY", "")
            key_id = os.environ.get("KALSHI_API_KEY_ID", "")
            if not key_str or not key_id:
                return {"error": "KALSHI credentials not configured"}
            private_key = serialization.load_pem_private_key(
                key_str.encode(), password=None,
            )
            ts_ms = str(int(time.time() * 1000))
            path = f"/trade-api/v2/markets/{ticker}/orderbook"
            msg = (ts_ms + "GET" + path).encode()
            sig = private_key.sign(
                msg,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            headers = {
                "KALSHI-ACCESS-KEY": key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts_ms,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
                "Accept": "application/json",
            }
            url = f"https://api.elections.kalshi.com{path}"
            with _httpx.Client(timeout=10.0) as client:
                r = client.get(url, headers=headers, params={"depth": depth})
                if r.status_code != 200:
                    return {
                        "error": f"Kalshi returned HTTP {r.status_code}",
                        "body": r.text[:400],
                    }
                data = r.json() or {}
                if debug:
                    return {
                        "ticker": ticker,
                        "status": r.status_code,
                        "raw_keys": list(data.keys()),
                        "raw_preview": str(data)[:1500],
                    }
                # Kalshi responses use orderbook_fp with *_dollars arrays
                # on newer API versions, and orderbook with yes/no arrays
                # on older. Accept both.
                ob = data.get("orderbook_fp") or data.get("orderbook") or {}
                yes_raw = ob.get("yes_dollars") or ob.get("yes") or []
                no_raw = ob.get("no_dollars") or ob.get("no") or []
                # Parse into [price, quantity] pairs in cents.
                # Dollar-format rows look like ["0.88", "337.25"] — convert
                # price to cents (int) and qty to float. Cent-format rows
                # (older API) look like [88, 337] — pass through.
                def _parse(levels):
                    out = []
                    for lv in levels or []:
                        if not isinstance(lv, (list, tuple)) or len(lv) < 2:
                            continue
                        try:
                            raw_p = lv[0]
                            if isinstance(raw_p, str):
                                # Dollar string → cents
                                p = int(round(float(raw_p) * 100))
                            else:
                                p = int(raw_p)
                            q = float(lv[1])
                        except Exception:
                            continue
                        out.append({"price": p, "qty": q})
                    return out
                yes_levels = _parse(yes_raw)
                no_levels = _parse(no_raw)
                # Build Trade Yes view.
                yes_asks = [{"price": 100 - lv["price"], "qty": lv["qty"]}
                            for lv in no_levels if lv["price"] < 100]
                yes_asks.sort(key=lambda x: x["price"])  # ascending
                yes_bids = sorted(yes_levels, key=lambda x: -x["price"])
                # Build Trade No view.
                no_asks = [{"price": 100 - lv["price"], "qty": lv["qty"]}
                           for lv in yes_levels if lv["price"] < 100]
                no_asks.sort(key=lambda x: x["price"])
                no_bids = sorted(no_levels, key=lambda x: -x["price"])
                # Add cumulative totals for display.
                def _add_totals(levels):
                    running_qty = 0.0
                    for lv in levels:
                        running_qty += lv["qty"]
                        # Total cost in dollars (price is cents).
                        lv["total"] = round(running_qty * lv["price"] / 100.0, 2)
                        lv["cum_qty"] = round(running_qty, 2)
                    return levels
                result = {
                    "ticker": ticker,
                    "yes": {
                        "asks": _add_totals(yes_asks),
                        "bids": _add_totals(yes_bids),
                    },
                    "no": {
                        "asks": _add_totals(no_asks),
                        "bids": _add_totals(no_bids),
                    },
                }
                _mk_cache_set(cache_key, result, ttl_seconds=1)
                return result
        except Exception as e:
            return {"error": str(e)}


@app.get("/api/market/{ticker}/trades")
def get_market_trades(ticker: str, limit: int = 10000, min_amount: float = 1000, hours: int = 0, debug: bool = False):
    """Fetch recent trades for a market from Kalshi and return
    large-capital trades (>= min_amount dollars) with YES/NO split
    for sentiment. Stats reflect whale-sized trades only so the
    totals match the displayed list."""
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return {"error": "ticker required"}
    # Trades are expensive: pagination can fetch up to 50k rows from
    # Kalshi (10+ seconds of work). Cache for 10 s so N simultaneous
    # viewers of the same market hit Kalshi once, not N times.
    cache_key = f"tr:{ticker}:{int(min_amount)}:{int(hours)}:{int(limit)}"
    if not debug:
        cached = _mk_cache_get(cache_key)
        if cached is not None:
            return cached
    lock = _mk_cache_lock_for(cache_key)
    with lock:
        if not debug:
            cached = _mk_cache_get(cache_key)
            if cached is not None:
                return cached
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding as _pad
            import base64, httpx as _httpx
            key_str = os.environ.get("KALSHI_PRIVATE_KEY", "")
            key_id = os.environ.get("KALSHI_API_KEY_ID", "")
            if not key_str or not key_id:
                return {"error": "KALSHI credentials not configured"}
            private_key = serialization.load_pem_private_key(
                key_str.encode(), password=None,
            )
            path = f"/trade-api/v2/markets/trades"
            ts_ms = str(int(time.time() * 1000))
            msg = (ts_ms + "GET" + path).encode()
            sig = private_key.sign(
                msg,
                _pad.PSS(
                    mgf=_pad.MGF1(hashes.SHA256()),
                    salt_length=_pad.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            headers = {
                "KALSHI-ACCESS-KEY": key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts_ms,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
                "Accept": "application/json",
            }
            url = f"https://api.elections.kalshi.com{path}"
            # Paginate through up to `limit` trades. Kalshi caps a single
            # page at 1000, so we loop on the cursor for events with heavy
            # flow.
            trades_raw = []
            cursor = None
            remaining = max(1, min(int(limit), 50000))
            # Optional time floor (epoch seconds). When > 0 we cap paging
            # once trades drift older than the window.
            min_ts = 0
            if hours and hours > 0:
                min_ts = int(time.time()) - (hours * 3600)
            with _httpx.Client(timeout=20.0) as client:
                while remaining > 0:
                    params = {"ticker": ticker, "limit": min(remaining, 1000)}
                    if cursor:
                        params["cursor"] = cursor
                    if min_ts:
                        params["min_ts"] = min_ts
                    # Re-sign per request (timestamps must be fresh).
                    ts_ms = str(int(time.time() * 1000))
                    msg = (ts_ms + "GET" + path).encode()
                    sig = private_key.sign(
                        msg,
                        _pad.PSS(
                            mgf=_pad.MGF1(hashes.SHA256()),
                            salt_length=_pad.PSS.DIGEST_LENGTH,
                        ),
                        hashes.SHA256(),
                    )
                    headers["KALSHI-ACCESS-TIMESTAMP"] = ts_ms
                    headers["KALSHI-ACCESS-SIGNATURE"] = base64.b64encode(sig).decode()
                    r = client.get(url, headers=headers, params=params)
                    if r.status_code != 200:
                        if not trades_raw:
                            return {"error": f"Kalshi returned HTTP {r.status_code}",
                                    "body": r.text[:400]}
                        break
                    data = r.json() or {}
                    if debug and not trades_raw:
                        return {
                            "ticker": ticker,
                            "status": r.status_code,
                            "raw_keys": list(data.keys()),
                            "raw_preview": str(data)[:2000],
                        }
                    page = data.get("trades") or []
                    if not page:
                        break
                    trades_raw.extend(page)
                    remaining -= len(page)
                    # When a time floor is set, stop once the oldest
                    # trade on this page pre-dates the window — even if
                    # Kalshi returns a cursor.
                    if min_ts:
                        last_ts = page[-1].get("created_time", "")
                        if last_ts:
                            try:
                                # created_time is ISO-8601 UTC with Z.
                                from datetime import datetime, timezone
                                dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                                if dt.timestamp() < min_ts:
                                    break
                            except Exception:
                                pass
                    cursor = data.get("cursor")
                    if not cursor:
                        break
            whale_trades = []
            yes_volume = 0.0
            no_volume = 0.0
            yes_count = 0
            no_count = 0
            total_all_trades_volume = 0.0
            total_all_trades_count = len(trades_raw)
            for t in trades_raw:
                # Kalshi newer API: count_fp (string), *_dollars (strings).
                # Older API: count (int), yes_price/no_price (cents int).
                count = t.get("count_fp") or t.get("count") or 0
                if isinstance(count, str):
                    count = float(count)
                yes_price = t.get("yes_price_dollars")
                if yes_price is None:
                    yes_price = t.get("yes_price", 0)
                no_price = t.get("no_price_dollars")
                if no_price is None:
                    no_price = t.get("no_price", 0)
                taker_side = t.get("taker_side", "")
                created = t.get("created_time", "")
                if isinstance(yes_price, str):
                    yes_price = float(yes_price)
                if isinstance(no_price, str):
                    no_price = float(no_price)
                # If the price looks like cents (integer > 1), convert.
                if yes_price > 1:
                    yes_price = yes_price / 100.0
                if no_price > 1:
                    no_price = no_price / 100.0
                if taker_side == "yes":
                    cost = yes_price * count
                    side = "YES"
                    price_cents = int(round(yes_price * 100))
                else:
                    cost = no_price * count
                    side = "NO"
                    price_cents = int(round(no_price * 100))
                total_all_trades_volume += cost
                if cost < min_amount:
                    continue
                if side == "YES":
                    yes_volume += cost
                    yes_count += 1
                else:
                    no_volume += cost
                    no_count += 1
                whale_trades.append({
                    "side": side,
                    "price": price_cents,
                    "contracts": count,
                    "cost": round(cost, 2),
                    "time": created,
                })
            total = yes_volume + no_volume
            result = {
                "ticker": ticker,
                "total_volume": round(total, 2),
                "yes_volume": round(yes_volume, 2),
                "no_volume": round(no_volume, 2),
                "yes_count": yes_count,
                "no_count": no_count,
                "whale_count": len(whale_trades),
                "total_trades_scanned": total_all_trades_count,
                "total_volume_all": round(total_all_trades_volume, 2),
                "sentiment": "Bullish" if yes_volume > no_volume else "Bearish" if no_volume > yes_volume else "Neutral",
                "trades": whale_trades,
            }
            _mk_cache_set(cache_key, result, ttl_seconds=10)
            return result
        except Exception as e:
            return {"error": str(e)}


@app.get("/api/event/{ticker}/history")
def get_event_history(ticker: str, hours: int = 24, period: int = 60, debug: bool = False):
    """Fetch historical candlestick data from Kalshi's REST API
    for every market under an event. Returns the same {series}
    shape as /api/event/{ticker}/prices so the frontend can use
    either endpoint interchangeably.

    Used for longer timeframes (24H, 7D, 30D) where our DB has
    no data (6h retention on Neon free tier). Short timeframes
    (1H, 6H) still use the /prices endpoint for real-time WS
    data.

    Params:
      hours   lookback window (default 24, max 8760 / 1 year)
      period  candle interval in minutes (default 60)
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return {"error": "ticker required", "series": []}
    hours = max(1, min(int(hours), 8760))
    # Kalshi only accepts specific period_interval values. Snap the
    # requested period to the nearest valid option.
    VALID_PERIODS = [1, 5, 15, 60, 1440]
    period = min(VALID_PERIODS, key=lambda p: abs(p - int(period)))
    # Find market tickers from the cache.
    get_data()
    records_all = _cache.get("data_all") or []
    records_grouped = _cache.get("data") or []
    market_tickers = []
    market_labels = {}
    for r in records_all:
        if r.get("event_ticker") == ticker:
            for o in r.get("outcomes", []):
                tk = o.get("ticker")
                if tk:
                    market_tickers.append(tk)
                    market_labels[tk] = o.get("label", tk)
            break
    if not market_tickers:
        for r in records_grouped:
            if r.get("event_ticker") == ticker:
                for o in r.get("outcomes", []):
                    tk = o.get("ticker")
                    if tk:
                        market_tickers.append(tk)
                        market_labels[tk] = o.get("label", tk)
                break
    if not market_tickers:
        return {"error": f"event {ticker!r} not found in cache", "series": []}
    # Build Kalshi API auth headers (same signing as WS).
    try:
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        import base64, httpx as _httpx
        key_str = os.environ.get("KALSHI_PRIVATE_KEY", "")
        key_id = os.environ.get("KALSHI_API_KEY_ID", "")
        if not key_str or not key_id:
            return {"error": "KALSHI credentials not configured", "series": []}
        private_key = serialization.load_pem_private_key(
            key_str.encode(), password=None
        )
        now = _dt.now(_tz.utc)
        start_ts = int((now - _td(hours=hours)).timestamp())
        end_ts = int(now.timestamp())
        api_base = "https://api.elections.kalshi.com"
        out_series = []
        debug_info = [] if debug else None
        # Try the first market ticker to find the correct API path.
        # Kalshi's API version/path may differ from what we expect.
        # Common patterns: /trade-api/v2, /v1, /v2, etc.
        # Kalshi's candlestick endpoint requires the series ticker in
        # the path. period_interval must be one of the allowed values
        # (1, 5, 15, 60, 1440 — minutes). Values outside this set
        # return 400 "PeriodInterval failed on 'oneof' tag".
        _CANDIDATE_PATHS = [
            "/trade-api/v2/series/{series}/markets/{mk}/candlesticks",
        ]
        def _sign_get(path_str):
            ts_ms = str(int(time.time() * 1000))
            msg = (ts_ms + "GET" + path_str).encode()
            sig = private_key.sign(
                msg,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            return {
                "KALSHI-ACCESS-KEY": key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts_ms,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
                "Accept": "application/json",
            }
        # Detect the event's series_ticker for paths that need it.
        series_ticker = ""
        for r_cache in records_all:
            if r_cache.get("event_ticker") == ticker:
                series_ticker = r_cache.get("series_ticker", "")
                break
        with _httpx.Client(timeout=15.0) as client:
            # In debug mode, try all candidate paths on the first
            # ticker so we can see which ones Kalshi actually serves.
            if debug:
                mk0 = market_tickers[0]
                for path_tpl in _CANDIDATE_PATHS:
                    p = path_tpl.format(mk=mk0, series=series_ticker)
                    h = _sign_get(p)
                    u = api_base + p
                    rr = client.get(u, headers=h, params={
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "period_interval": period,
                    })
                    debug_info.append({
                        "path_template": path_tpl,
                        "url": u,
                        "status": rr.status_code,
                        "body_preview": rr.text[:800],
                    })
                # Also try the SDK's method if available.
                try:
                    sdk_client = get_client()
                    sdk_methods = [m for m in dir(sdk_client) if any(
                        k in m.lower() for k in ('candle', 'history', 'trade', 'series')
                    ) and not m.startswith('_')]
                    debug_info.append({"sdk_methods": sdk_methods})
                except Exception as e:
                    debug_info.append({"sdk_error": str(e)})
            # Find the working path — try each until one returns 200.
            working_path_tpl = None
            mk_test = market_tickers[0]
            for path_tpl in _CANDIDATE_PATHS:
                p = path_tpl.format(mk=mk_test, series=series_ticker)
                h = _sign_get(p)
                u = api_base + p
                rr = client.get(u, headers=h, params={
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "period_interval": period,
                })
                if rr.status_code == 200:
                    working_path_tpl = path_tpl
                    break
            if not working_path_tpl:
                return {
                    "series": [],
                    "hours": hours,
                    "error": "no working Kalshi candlestick API path found",
                    "debug": debug_info,
                    "market_tickers_checked": market_tickers[:5],
                }
            for mk in market_tickers:
                p = working_path_tpl.format(mk=mk, series=series_ticker)
                hdrs = _sign_get(p)
                params = {
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "period_interval": period,
                }
                url = api_base + p
                r = client.get(url, headers=hdrs, params=params)
                if r.status_code != 200:
                    continue
                data = r.json() or {}
                candles = data.get("candlesticks") or []
                if not candles:
                    continue
                points = []
                for c in candles:
                    # Timestamp: unix seconds (integer).
                    ts_raw = c.get("end_period_ts")
                    if ts_raw is None:
                        continue
                    try:
                        t_ms = int(float(ts_raw) * 1000)
                    except Exception:
                        continue
                    # Price: Kalshi returns dollar strings like
                    # "0.2000" under price.close_dollars. Convert
                    # to cents (0-100 scale) for our chart.
                    price_obj = c.get("price") or {}
                    price_str = price_obj.get("close_dollars")
                    if price_str is None:
                        # Fallback: try yes_bid close as proxy.
                        yb = c.get("yes_bid") or {}
                        price_str = yb.get("close_dollars")
                    if price_str is None:
                        continue
                    try:
                        p = float(price_str) * 100  # dollars → cents
                    except Exception:
                        continue
                    # Volume per candle.
                    vol = None
                    vol_str = c.get("volume_fp")
                    if vol_str is not None:
                        try:
                            vol = float(vol_str)
                        except Exception:
                            pass
                    points.append({
                        "t": t_ms,
                        "p": round(p, 2),
                    })
                if points:
                    points.sort(key=lambda x: x["t"])
                    out_series.append({
                        "market_ticker": mk,
                        "label": market_labels.get(mk, mk),
                        "points": points,
                        "min": min(pt["p"] for pt in points),
                        "max": max(pt["p"] for pt in points),
                        "first": points[0]["p"],
                        "last": points[-1]["p"],
                    })
        result = {
            "series": out_series,
            "hours": hours,
            "period_minutes": period,
            "source": "kalshi_api",
            "market_tickers_checked": market_tickers[:5],
        }
        if debug and debug_info:
            result["debug"] = debug_info
        return result
    except Exception as e:
        logging.getLogger("stochverse").warning("history fetch failed: %s", e)
        return {"series": [], "error": str(e)}


@app.get("/api/event/{ticker}/stats")
def get_event_stats(ticker: str, debug: bool = False):
    """Fetch live game statistics (shots, possession, cards, etc.)
    from SofaScore for the matched game. Returns a symmetric
    home/away structure suitable for rendering a stats panel.

    On-demand — only called when a user opens the event detail
    page. Doesn't poll or cache beyond the request lifecycle.
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return {"error": "ticker required"}
    # Find the event in our cache to get sport + title.
    get_data()
    records = _cache.get("data_all") or _cache.get("data") or []
    found = None
    for r in records:
        if r.get("event_ticker") == ticker:
            found = r
            break
    if not found:
        return {"error": f"event {ticker!r} not found in cache"}
    sport = found.get("_sport", "")
    title = found.get("title", "")
    if not sport or not title:
        return {"error": "event has no sport or title"}
    # Find the matched game — try SofaScore first (richest stats).
    sofa_id = None
    home_name = ""
    away_name = ""
    try:
        from sofascore_feed import match_game as sofa_match, SOFASCORE_GAMES
        sg = sofa_match(title, sport)
        if sg:
            sofa_id = sg.get("_sofa_event_id")
            home_name = sg.get("home_display", "")
            away_name = sg.get("away_display", "")
    except Exception:
        pass
    # If SofaScore didn't match or no ID, try searching.
    if not sofa_id:
        try:
            import re as _re
            parts = _re.split(r'\s+(?:vs\.?|v|at)\s+', title, maxsplit=1, flags=_re.IGNORECASE)
            if len(parts) == 2:
                from sofascore_feed import lookup_aggregate_sync
                agg = lookup_aggregate_sync(parts[0].strip(), parts[1].strip())
                if agg and agg.get("_sofa_event_id"):
                    sofa_id = agg["_sofa_event_id"]
                    if not home_name:
                        home_name = parts[0].strip()
                    if not away_name:
                        away_name = parts[1].strip()
        except Exception:
            pass
    # Last-resort fallback: direct SofaScore search-events with loose
    # matching. lookup_aggregate_sync is 2-leg-tie specific and can
    # miss regular fixtures.
    if not sofa_id:
        try:
            import re as _re2
            import httpx as _httpx2
            parts = _re2.split(r'\s+(?:vs\.?|v|at)\s+', title, maxsplit=1, flags=_re2.IGNORECASE)
            if len(parts) == 2:
                t_home = parts[0].strip().lower()
                t_away = parts[1].strip().lower()
                search_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/125.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.sofascore.com/",
                    "Origin": "https://www.sofascore.com",
                }
                q = (parts[0].strip() + " " + parts[1].strip()).strip()
                with _httpx2.Client(headers=search_headers, timeout=10.0,
                                    follow_redirects=True) as sc:
                    sr = sc.get("https://api.sofascore.com/api/v1/search/events",
                                params={"q": q, "page": 0})
                    if sr.status_code == 200:
                        srd = sr.json() or {}
                        for item in (srd.get("results") or []):
                            if not isinstance(item, dict):
                                continue
                            if item.get("type") != "event":
                                continue
                            ent = item.get("entity") or {}
                            hn = (ent.get("homeTeam") or {}).get("name", "").lower()
                            an = (ent.get("awayTeam") or {}).get("name", "").lower()
                            # Loose match: each Kalshi team name should
                            # appear in ONE of the SofaScore team names
                            # (handles "Brentford" vs "Brentford FC"
                            # and order-swapped fixtures).
                            h_hit = (t_home in hn) or (t_home in an) or (hn in t_home) or (an in t_home)
                            a_hit = (t_away in hn) or (t_away in an) or (hn in t_away) or (an in t_away)
                            if h_hit and a_hit:
                                sofa_id = ent.get("id")
                                if not home_name:
                                    home_name = (ent.get("homeTeam") or {}).get("name") or parts[0].strip()
                                if not away_name:
                                    away_name = (ent.get("awayTeam") or {}).get("name") or parts[1].strip()
                                break
        except Exception:
            pass
    if not sofa_id:
        out_err = {"error": "no SofaScore match found for this event", "sport": sport, "title": title}
        if debug:
            try:
                import re as _re3
                import httpx as _httpx3
                parts3 = _re3.split(r'\s+(?:vs\.?|v|at)\s+', title, maxsplit=1, flags=_re3.IGNORECASE)
                out_err["title_parts"] = parts3
                if len(parts3) == 2:
                    dbg_headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/125.0.0.0 Safari/537.36",
                        "Accept": "application/json, text/plain, */*",
                        "Referer": "https://www.sofascore.com/",
                        "Origin": "https://www.sofascore.com",
                    }
                    q3 = (parts3[0].strip() + " " + parts3[1].strip()).strip()
                    with _httpx3.Client(headers=dbg_headers, timeout=10.0,
                                        follow_redirects=True) as sc3:
                        sr3 = sc3.get("https://api.sofascore.com/api/v1/search/events",
                                      params={"q": q3, "page": 0})
                        out_err["search_status"] = sr3.status_code
                        out_err["search_query"] = q3
                        if sr3.status_code == 200:
                            srd3 = sr3.json() or {}
                            out_err["search_results"] = [
                                {
                                    "type": it.get("type"),
                                    "home": (it.get("entity") or {}).get("homeTeam", {}).get("name"),
                                    "away": (it.get("entity") or {}).get("awayTeam", {}).get("name"),
                                    "id":   (it.get("entity") or {}).get("id"),
                                }
                                for it in (srd3.get("results") or [])[:10]
                            ]
                        else:
                            out_err["search_body"] = sr3.text[:400]
            except Exception as _e3:
                out_err["debug_err"] = str(_e3)
        return out_err
    # Fetch statistics from SofaScore.
    try:
        import httpx as _httpx
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.sofascore.com/",
            "Origin": "https://www.sofascore.com",
        }
        base = f"https://api.sofascore.com/api/v1/event/{sofa_id}"
        with _httpx.Client(headers=headers, timeout=10.0) as client:
            # ── Statistics ──────────────────────────────────────
            stats = []
            try:
                r = client.get(base + "/statistics")
                if r.status_code == 200:
                    raw_stats = (r.json() or {}).get("statistics") or []
                    all_period = None
                    for period in raw_stats:
                        if period.get("period") == "ALL":
                            all_period = period
                            break
                    if not all_period and raw_stats:
                        all_period = raw_stats[0]
                    if all_period:
                        for group in all_period.get("groups") or []:
                            for item in group.get("statisticsItems") or []:
                                stats.append({
                                    "name":  item.get("name", ""),
                                    "home":  str(item.get("home", "")),
                                    "away":  str(item.get("away", "")),
                                    "group": group.get("groupName", ""),
                                })
            except Exception:
                pass
            # ── Incidents (timeline) ────────────────────────────
            incidents = []
            try:
                r2 = client.get(base + "/incidents")
                if r2.status_code == 200:
                    raw_inc = (r2.json() or {}).get("incidents") or []
                    for inc in raw_inc:
                        itype = inc.get("incidentType") or ""
                        entry = {
                            "type": itype,
                            "time": inc.get("time"),
                            "addedTime": inc.get("addedTime"),
                            "isHome": inc.get("isHome"),
                            "text": inc.get("text") or "",
                        }
                        # Player info for goals, cards, subs.
                        player = inc.get("player") or {}
                        entry["player"] = player.get("shortName") or player.get("name") or ""
                        # Assist for goals.
                        assist = inc.get("assist1") or inc.get("assist") or {}
                        if isinstance(assist, dict):
                            entry["assist"] = assist.get("shortName") or assist.get("name") or ""
                        else:
                            entry["assist"] = ""
                        # Sub: player in / out.
                        pin = inc.get("playerIn") or {}
                        pout = inc.get("playerOut") or {}
                        if isinstance(pin, dict):
                            entry["playerIn"] = pin.get("shortName") or pin.get("name") or ""
                        if isinstance(pout, dict):
                            entry["playerOut"] = pout.get("shortName") or pout.get("name") or ""
                        # Card color.
                        entry["incidentClass"] = inc.get("incidentClass") or ""
                        # Goal details.
                        entry["goalType"] = inc.get("incidentClass") or ""
                        # Score after goal.
                        entry["homeScore"] = inc.get("homeScore")
                        entry["awayScore"] = inc.get("awayScore")
                        # Injury time length (minutes added).
                        entry["length"] = inc.get("length")
                        incidents.append(entry)
            except Exception:
                pass
            # ── Lineups ─────────────────────────────────────────
            lineups = {}
            try:
                r3 = client.get(base + "/lineups")
                if r3.status_code == 200:
                    raw_lin = r3.json() or {}
                    for side in ("home", "away"):
                        team = raw_lin.get(side) or {}
                        players_arr = team.get("players") or []
                        formation = team.get("formation") or ""
                        parsed_players = []
                        for p in players_arr:
                            pl = p.get("player") or {}
                            parsed_players.append({
                                "name": pl.get("shortName") or pl.get("name") or "",
                                "position": p.get("position") or pl.get("position") or "",
                                "jerseyNumber": pl.get("jerseyNumber") or p.get("jerseyNumber"),
                                "substitute": p.get("substitute", False),
                                "captain": p.get("captain", False),
                            })
                        # Manager / head coach — SofaScore nests it
                        # under various field names depending on the
                        # endpoint version: "manager", "headCoach",
                        # "coach". Try all.
                        manager_name = ""
                        for _mk in ("manager", "headCoach", "coach"):
                            mobj = team.get(_mk)
                            if isinstance(mobj, dict):
                                manager_name = (
                                    mobj.get("shortName")
                                    or mobj.get("name")
                                    or ""
                                )
                                if manager_name:
                                    break
                            elif isinstance(mobj, str) and mobj:
                                manager_name = mobj
                                break
                        lineups[side] = {
                            "formation": formation,
                            "players": parsed_players,
                            "manager": manager_name,
                        }
            except Exception:
                pass
            # If lineups lack manager names, try the event detail
            # endpoint where SofaScore nests them under
            # homeTeam.manager / awayTeam.manager.
            if lineups and (not lineups.get("home", {}).get("manager") or
                            not lineups.get("away", {}).get("manager")):
                try:
                    evr = client.get(base)
                    if evr.status_code == 200:
                        evd = (evr.json() or {}).get("event") or {}
                        for _side, _tkey in [("home","homeTeam"),("away","awayTeam")]:
                            if lineups.get(_side) and not lineups[_side].get("manager"):
                                mgr = (evd.get(_tkey) or {}).get("manager") or {}
                                if isinstance(mgr, dict):
                                    mn = mgr.get("shortName") or mgr.get("name") or ""
                                elif isinstance(mgr, str):
                                    mn = mgr
                                else:
                                    mn = ""
                                if mn:
                                    lineups[_side]["manager"] = mn
                except Exception:
                    pass
            return {
                "stats": stats,
                "incidents": incidents,
                "lineups": lineups,
                "home": home_name,
                "away": away_name,
                "sofa_event_id": sofa_id,
                "sport": sport,
            }
    except Exception as e:
        return {"error": str(e), "sofa_event_id": sofa_id}


@app.get("/api/prune")
async def prune_prices():
    """Manually trigger pruning of old price rows. Returns the
    number of rows deleted. Safe to call repeatedly — only deletes
    rows older than PRICE_RETENTION_HOURS (default 6h)."""
    try:
        from db import prune_old_prices, PRICE_RETENTION_HOURS
        deleted = await prune_old_prices()
        return {
            "deleted": deleted,
            "retention_hours": PRICE_RETENTION_HOURS,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/db_health")
async def db_health():
    """Dedicated DB probe — runs SELECT 1 against Postgres and
    reports latency + any error. Use this to tell whether a
    price-history failure is the DB itself or our connection pool.

    The endpoint also disposes + retries once, so just running it
    tends to heal a stale pool as a side effect."""
    from db import DATABASE_URL, async_session, engine as _eng
    out = {"database_url_set": bool(DATABASE_URL)}
    if not DATABASE_URL or async_session is None:
        out["ok"] = False
        out["error"] = "database not configured"
        return out
    async def _probe():
        from sqlalchemy import text
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
    t0 = time.time()
    last_err = None
    for attempt in range(3):
        try:
            await _probe()
            out["ok"] = True
            out["latency_ms"] = int((time.time() - t0) * 1000)
            out["attempts"] = attempt + 1
            return out
        except Exception as e:
            last_err = e
            try:
                if _eng is not None:
                    await _eng.dispose()
            except Exception:
                pass
            import asyncio as _a
            await _a.sleep(0.3 * (2 ** attempt))
    out["ok"] = False
    out["error"] = str(last_err)
    out["error_type"] = type(last_err).__name__ if last_err else None
    out["transient"] = _is_transient_db_error(last_err) if last_err else False
    out["latency_ms"] = int((time.time() - t0) * 1000)
    return out


@app.get("/healthz")
def healthz():
    """Cheap liveness probe. Used by keep-warm pingers (Railway
    cron, UptimeRobot) to prevent the container from parking
    between user visits, and by monitoring to detect outages.
    Returns 200 with a compact payload — no DB or Kalshi calls."""
    return {
        "ok": True,
        "cache_age_s": int(time.time() - _cache.get("ts", 0)) if _cache.get("ts") else None,
        "cache_ready": _cache.get("data") is not None,
    }
