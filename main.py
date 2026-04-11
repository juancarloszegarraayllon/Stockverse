from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import os, time, tempfile, functools, asyncio, threading, logging
from datetime import date, timedelta, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="OddsIQ API")


def _all_market_tickers():
    """Return every market ticker from the current REST snapshot, used
    by the Kalshi WebSocket client to know what to subscribe to."""
    records = _cache.get("data") or []
    seen = set()
    out = []
    for r in records:
        for o in r.get("outcomes", []):
            tk = o.get("ticker")
            if tk and tk not in seen:
                seen.add(tk)
                out.append(tk)
    return out


@app.on_event("startup")
async def startup_event():
    global _cache
    _cache = {"data": None, "ts": 0}
    # Build the REST snapshot eagerly in a thread so the WS client has
    # tickers to subscribe to without waiting for a first user request.
    threading.Thread(target=get_data, daemon=True).start()
    # Launch the Kalshi WebSocket client as an asyncio background task.
    try:
        from kalshi_ws import run_ws_client
        asyncio.create_task(run_ws_client(_all_market_tickers))
    except Exception as e:
        logging.getLogger("oddsiq").warning("failed to start ws client: %s", e)
    try:
        from espn_feed import run_espn_feed
        asyncio.create_task(run_espn_feed())
    except Exception as e:
        logging.getLogger("oddsiq").warning("failed to start espn feed: %s", e)
    try:
        from sportsdb_feed import run_sportsdb_feed
        asyncio.create_task(run_sportsdb_feed())
    except Exception as e:
        logging.getLogger("oddsiq").warning("failed to start sportsdb feed: %s", e)
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
        logging.getLogger("oddsiq").warning("failed to start sofascore feed: %s", e)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
"Soccer":["KXEPLGAME","KXEPL1H","KXEPLSPREAD","KXEPLTOTAL","KXEPLBTTS","KXEPLTOP4","KXEPLTOP2","KXEPLTOP6","KXEPLRELEGATION","KXPREMIERLEAGUE","KXARSENALCUPS","KXWINSTREAKMANU","KXNEXTMANAGERMANU","KXPFAPOY","KXLAMINEYAMAL","KXUCLGAME","KXUCL1H","KXUCLSPREAD","KXUCLTOTAL","KXUCLBTTS","KXUCL","KXUCLFINALIST","KXUCLRO4","KXUCLW","KXLEADERUCLGOALS","KXTEAMSINUCL","KXUELGAME","KXUELSPREAD","KXUELTOTAL","KXUEL","KXUECL","KXUECLGAME","KXLALIGAGAME","KXLALIGA1H","KXLALIGASPREAD","KXLALIGATOTAL","KXLALIGABTTS","KXLALIGA","KXLALIGATOP4","KXLALIGARELEGATION","KXLALIGA2GAME","KXSERIEAGAME","KXSERIEA1H","KXSERIEASPREAD","KXSERIEATOTAL","KXSERIEABTTS","KXSERIEA","KXSERIEATOP4","KXSERIEARELEGATION","KXSERIEBGAME","KXBUNDESLIGAGAME","KXBUNDESLIGA1H","KXBUNDESLIGASPREAD","KXBUNDESLIGATOTAL","KXBUNDESLIGABTTS","KXBUNDESLIGA","KXBUNDESLIGATOP4","KXBUNDESLIGARELEGATION","KXBUNDESLIGA2GAME","KXLIGUE1GAME","KXLIGUE11H","KXLIGUE1SPREAD","KXLIGUE1TOTAL","KXLIGUE1BTTS","KXLIGUE1","KXLIGUE1TOP4","KXLIGUE1RELEGATION","KXMLSGAME","KXMLSSPREAD","KXMLSTOTAL","KXMLSBTTS","KXMLSCUP","KXMLSEAST","KXMLSWEST","KXLIGAMXGAME","KXLIGAMXSPREAD","KXLIGAMXTOTAL","KXLIGAMX","KXBRASILEIROGAME","KXBRASILEIROSPREAD","KXBRASILEIROTOTAL","KXBRASILEIRO","KXBRASILEIROTOPX","KXWCGAME","KXWCROUND","KXWCGROUPWIN","KXWCGROUPQUAL","KXWCGOALLEADER","KXWCMESSIRONALDO","KXWCLOCATION","KXWCIRAN","KXWCSQUAD","KXMENWORLDCUP","KXSOCCERPLAYMESSI","KXSOCCERPLAYCRON","KXFIFAUSPULL","KXFIFAUSPULLGAME","KXSAUDIPLGAME","KXSAUDIPLSPREAD","KXSAUDIPLTOTAL","KXLIGAPORTUGALGAME","KXLIGAPORTUGAL","KXEREDIVISIEGAME","KXEREDIVISIE","KXCOPADELREY","KXDFBPOKAL","KXFACUP","KXCOPPAITALIA","KXEFLCHAMPIONSHIPGAME","KXEFLCHAMPIONSHIP","KXEFLPROMO","KXSUPERLIGGAME","KXSUPERLIG","KXCONCACAFCCUPGAME","KXCONMEBOLLIBGAME","KXCONMEBOLSUDGAME","KXUSLGAME","KXUSL","KXSCOTTISHPREMGAME","KXEKSTRAKLASAGAME","KXEKSTRAKLASA","KXALEAGUEGAME","KXALEAGUESPREAD","KXALEAGUETOTAL","KXKLEAGUEGAME","KXKLEAGUE","KXJLEAGUEGAME","KXCHNSLGAME","KXCHNSL","KXALLSVENSKANGAME","KXDENSUPERLIGAGAME","KXDENSUPERLIGA","KXSWISSLEAGUEGAME","KXARGPREMDIVGAME","KXDIMAYORGAME","KXURYPDGAME","KXURYPD","KXECULPGAME","KXECULP","KXVENFUTVEGAME","KXVENFUTVE","KXCHLLDPGAME","KXCHLLDP","KXAPFDDHGAME","KXAPFDDH","KXBALLERLEAGUEGAME","KXSLGREECE","KXTHAIL1GAME","KXTHAIL1","KXEGYPLGAME","KXHNLGAME","KXBELGIANPLGAME","KXBELGIANPL","KXPERLIGA1","KXKNVBCUP","KXSOCCERTRANSFER","KXJOINLEAGUE","KXJOINRONALDO","KXJOINCLUB","KXBALLONDOR"],
"Basketball":["KXNBAGAME","KXNBASPREAD","KXNBATOTAL","KXNBATEAMTOTAL","KXNBA1HWINNER","KXNBA1HSPREAD","KXNBA1HTOTAL","KXNBA2HWINNER","KXNBA2D","KXNBA3D","KXNBA3PT","KXNBAPTS","KXNBAREB","KXNBAAST","KXNBABLK","KXNBASTL","KXNBA","KXNBAEAST","KXNBAWEST","KXNBAPLAYOFF","KXNBAPLAYIN","KXNBAATLANTIC","KXNBACENTRAL","KXNBASOUTHEAST","KXNBANORTHWEST","KXNBAPACIFIC","KXNBASOUTHWEST","KXNBAEAST1SEED","KXNBAWEST1SEED","KXTEAMSINNBAF","KXTEAMSINNBAEF","KXTEAMSINNBAWF","KXNBAMATCHUP","KXNBAWINS","KXRECORDNBABEST","KXNBAMVP","KXNBAROY","KXNBACOY","KXNBADPOY","KXNBASIXTH","KXNBAMIMP","KXNBACLUTCH","KXNBAFINMVP","KXNBAWFINMVP","KXNBAEFINMVP","KXNBA1STTEAM","KXNBA2NDTEAM","KXNBA3RDTEAM","KXNBA1STTEAMDEF","KXNBA2NDTEAMDEF","KXLEADERNBAPTS","KXLEADERNBAREB","KXLEADERNBAAST","KXLEADERNBABLK","KXLEADERNBASTL","KXLEADERNBA3PT","KXNBADRAFT1","KXNBADRAFTPICK","KXNBADRAFTTOP","KXNBADRAFTCAT","KXNBADRAFTCOMP","KXNBATOPPICK","KXNBALOTTERYODDS","KXNBATOP5ROTY","KXNBATEAM","KXNBASEATTLE","KXCITYNBAEXPAND","KXSONICS","KXNEXTTEAMNBA","KXLBJRETIRE","KXSPORTSOWNERLBJ","KXSTEPHDEAL","KXQUADRUPLEDOUBLE","KXSHAI20PTREC","KXNBA2KCOVER","KXWNBADRAFT1","KXWNBADRAFTTOP3","KXWNBADELAY","KXWNBAGAMESPLAYED","KXMARMAD","KXNCAAMBNEXTCOACH","KXEUROLEAGUEGAME","KXBSLGAME","KXBBLGAME","KXACBGAME","KXISLGAME","KXABAGAME","KXCBAGAME","KXBBSERIEAGAME","KXJBLEAGUEGAME","KXLNBELITEGAME","KXARGLNBGAME","KXVTBGAME"],
"Baseball":["KXMLBGAME","KXMLBRFI","KXMLBSPREAD","KXMLBTOTAL","KXMLBTEAMTOTAL","KXMLBF5","KXMLBF5SPREAD","KXMLBF5TOTAL","KXMLBHIT","KXMLBHR","KXMLBHRR","KXMLBKS","KXMLBTB","KXMLB","KXMLBAL","KXMLBNL","KXMLBALEAST","KXMLBALWEST","KXMLBALCENT","KXMLBNLEAST","KXMLBNLWEST","KXMLBNLCENT","KXMLBPLAYOFFS","KXTEAMSINWS","KXMLBBESTRECORD","KXMLBWORSTRECORD","KXMLBLSTREAK","KXMLBWSTREAK","KXMLBALMVP","KXMLBNLMVP","KXMLBALCY","KXMLBNLCY","KXMLBALROTY","KXMLBNLROTY","KXMLBEOTY","KXMLBALMOTY","KXMLBNLMOTY","KXMLBALHAARON","KXMLBNLHAARON","KXMLBALCPOTY","KXMLBNLCPOTY","KXMLBALRELOTY","KXMLBNLRELOTY","KXMLBSTAT","KXMLBSTATCOUNT","KXMLBSEASONHR","KXLEADERMLBAVG","KXLEADERMLBDOUBLES","KXLEADERMLBERA","KXLEADERMLBHITS","KXLEADERMLBHR","KXLEADERMLBKS","KXLEADERMLBOPS","KXLEADERMLBRBI","KXLEADERMLBRUNS","KXLEADERMLBSTEALS","KXLEADERMLBTRIPLES","KXLEADERMLBWAR","KXLEADERMLBWINS","KXMLBTRADE","KXWSOPENTRANTS","KXNPBGAME","KXKBOGAME","KXNCAABBGAME","KXNCAABASEBALL","KXNCAABBGS"],
"Football":["KXUFLGAME","KXSB","KXNFLPLAYOFF","KXNFLAFCCHAMP","KXNFLNFCCHAMP","KXNFLAFCEAST","KXNFLAFCWEST","KXNFLAFCNORTH","KXNFLAFCSOUTH","KXNFLNFCEAST","KXNFLNFCWEST","KXNFLNFCNORTH","KXNFLNFCSOUTH","KXNFLMVP","KXNFLOPOTY","KXNFLDPOTY","KXNFLOROTY","KXNFLDROTY","KXNFLCOTY","KXNFLDRAFT1","KXNFLDRAFT1ST","KXNFLDRAFTPICK","KXNFLDRAFTTOP","KXNFLDRAFTWR","KXNFLDRAFTDB","KXNFLDRAFTTE","KXNFLDRAFTQB","KXNFLDRAFTOL","KXNFLDRAFTEDGE","KXNFLDRAFTLB","KXNFLDRAFTRB","KXNFLDRAFTDT","KXNFLDRAFTTEAM","KXLEADERNFLSACKS","KXLEADERNFLINT","KXLEADERNFLPINT","KXLEADERNFLPTDS","KXLEADERNFLPYDS","KXLEADERNFLRTDS","KXLEADERNFLRUSHTDS","KXLEADERNFLRUSHYDS","KXLEADERNFLRYDS","KXNFLTEAM1POS","KXNFLPRIMETIME","KXNFLTRADE","KXNEXTTEAMNFL","KXRECORDNFLBEST","KXRECORDNFLWORST","KXKELCERETIRE","KXSTARTINGQBWEEK1","KXCOACHOUTNFL","KXCOACHOUTNCAAFB","KXARODGRETIRE","KXRELOCATIONCHI","KX1STHOMEGAME","KXSORONDO","KXNCAAF","KXHEISMAN","KXNCAAFCONF","KXNCAAFACC","KXNCAAFB10","KXNCAAFB12","KXNCAAFSEC","KXNCAAFAAC","KXNCAAFSBELT","KXNCAAFMWC","KXNCAAFMAC","KXNCAAFCUSA","KXNCAAFPAC12","KXNCAAFPLAYOFF","KXNCAAFFINALIST","KXNCAAFUNDEFEATED","KXNCAAFCOTY","KXNCAAFAPRANK","KXNDJOINCONF","KXCOVEREA","KXDONATEMRBEAST"],
"Hockey":["KXNHLGAME","KXNHLSPREAD","KXNHLTOTAL","KXNHL","KXNHLPLAYOFF","KXTEAMSINSC","KXNHLPRES","KXNHLEAST","KXNHLWEST","KXNHLADAMS","KXNHLCENTRAL","KXNHLATLANTIC","KXNHLMETROPOLITAN","KXNHLPACIFIC","KXNHLHART","KXNHLNORRIS","KXNHLVEZINA","KXNHLCALDER","KXNHLROSS","KXNHLRICHARD","KXAHLGAME","KXCANADACUP","KXNCAAHOCKEY","KXNCAAHOCKEYGAME","KXKHLGAME","KXSHLGAME","KXLIIGAGAME","KXELHGAME","KXNLGAME","KXDELGAME"],
"Tennis":["KXATPMATCH","KXATPSETWINNER","KXATPCHALLENGERMATCH","KXATPGRANDSLAM","KXATPGRANDSLAMFIELD","KXATP1RANK","KXMCMMEN","KXFOMEN","KXWTAMATCH","KXWTAGRANDSLAM","KXWTASERENA","KXFOWOMEN","KXGRANDSLAM","KXGRANDSLAMJFONSECA","KXGOLFTENNISMAJORS"],
"Golf":["KXPGATOUR","KXPGAH2H","KXPGA3BALL","KXPGA5BALL","KXPGAR1LEAD","KXPGAR1TOP5","KXPGAR1TOP10","KXPGAR1TOP20","KXPGAR2LEAD","KXPGAR2TOP5","KXPGAR2TOP10","KXPGAR3LEAD","KXPGAR3TOP5","KXPGAR3TOP10","KXPGATOP5","KXPGATOP10","KXPGATOP20","KXPGATOP40","KXPGAPLAYOFF","KXPGACUTLINE","KXPGAMAKECUT","KXPGAAGECUT","KXPGAWINNERREGION","KXPGALOWSCORE","KXPGASTROKEMARGIN","KXPGAWINNINGSCORE","KXPGAPLAYERCAT","KXPGABIRDIES","KXPGAROUNDSCORE","KXPGAEAGLE","KXPGAHOLEINONE","KXPGABOGEYFREE","KXPGAMAJORTOP10","KXPGAMAJORWIN","KXPGAMASTERS","KXGOLFMAJORS","KXGOLFTENNISMAJORS","KXPGARYDER","KXPGASOLHEIM","KXRYDERCUPCAPTAIN","KXPGACURRY","KXPGATIGER","KXBRYSONCOURSERECORDS","KXSCOTTIESLAM"],
"MMA":["KXUFCFIGHT","KXUFCHEAVYWEIGHTTITLE","KXUFCLHEAVYWEIGHTTITLE","KXUFCMIDDLEWEIGHTTITLE","KXUFCWELTERWEIGHTTITLE","KXUFCLIGHTWEIGHTTITLE","KXUFCFEATHERWEIGHTTITLE","KXUFCBANTAMWEIGHTTITLE","KXUFCFLYWEIGHTTITLE","KXMCGREGORFIGHTNEXT","KXCARDPRESENCEUFCWH","KXUFCWHITEHOUSE"],
"Cricket":["KXIPLGAME","KXIPL","KXIPLFOUR","KXIPLSIX","KXIPLTEAMTOTAL","KXPSLGAME","KXPSL","KXT20MATCH"],
"Esports":["KXVALORANTMAP","KXVALORANTGAME","KXLOLGAME","KXLOLMAP","KXLOLTOTALMAPS","KXR6GAME","KXCS2GAME","KXCS2MAP","KXCS2TOTALMAPS","KXDOTA2GAME","KXDOTA2MAP","KXOWGAME"],
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
    "KXSLGREECE":"Greek SL",
    "KXTHAIL1GAME":"Thai League","KXTHAIL1":"Thai League",
    "KXEGYPLGAME":"Egyptian PL",
    "KXHNLGAME":"HNL Croatia",
    "KXBELGIANPLGAME":"Belgian Pro","KXBELGIANPL":"Belgian Pro",
    "KXPERLIGA1":"Peruvian L1","KXKNVBCUP":"KNVB Cup",
    "KXSOCCERTRANSFER":"Transfers/News","KXJOINLEAGUE":"Transfers/News",
    "KXJOINRONALDO":"Transfers/News","KXJOINCLUB":"Transfers/News","KXBALLONDOR":"Transfers/News",
}

SERIES_SPORT = {}
for sport, series_list in _SPORT_SERIES.items():
    for s in series_list:
        SERIES_SPORT[s] = sport

def get_sport(series_ticker):
    return SERIES_SPORT.get(str(series_ticker).upper(), "")

# ── Sport sub-tabs ─────────────────────────────────────────────────────────────
SPORT_SUBTABS = {
"Basketball":[("NBA Games",["KXNBAGAME","KXNBASPREAD","KXNBATOTAL","KXNBATEAMTOTAL","KXNBA1HWINNER","KXNBA1HSPREAD","KXNBA1HTOTAL","KXNBA2HWINNER","KXNBA2D","KXNBA3D","KXNBA3PT","KXNBAPTS","KXNBAREB","KXNBAAST","KXNBABLK","KXNBASTL"]),("NBA Season",["KXNBA","KXNBAEAST","KXNBAWEST","KXNBAPLAYOFF","KXNBAPLAYIN","KXNBAATLANTIC","KXNBACENTRAL","KXNBASOUTHEAST","KXNBANORTHWEST","KXNBAPACIFIC","KXNBASOUTHWEST","KXNBAEAST1SEED","KXNBAWEST1SEED","KXTEAMSINNBAF","KXTEAMSINNBAEF","KXTEAMSINNBAWF","KXNBAMATCHUP","KXNBAWINS","KXRECORDNBABEST"]),("NBA Awards",["KXNBAMVP","KXNBAROY","KXNBACOY","KXNBADPOY","KXNBASIXTH","KXNBAMIMP","KXNBACLUTCH","KXNBAFINMVP","KXNBAWFINMVP","KXNBAEFINMVP","KXNBA1STTEAM","KXNBA2NDTEAM","KXNBA3RDTEAM","KXNBA1STTEAMDEF","KXNBA2NDTEAMDEF"]),("NBA Stats",["KXLEADERNBAPTS","KXLEADERNBAREB","KXLEADERNBAAST","KXLEADERNBABLK","KXLEADERNBASTL","KXLEADERNBA3PT"]),("NBA Draft",["KXNBADRAFT1","KXNBADRAFTPICK","KXNBADRAFTTOP","KXNBADRAFTCAT","KXNBADRAFTCOMP","KXNBATOPPICK","KXNBALOTTERYODDS","KXNBATOP5ROTY"]),("NBA Other",["KXNBATEAM","KXNBASEATTLE","KXCITYNBAEXPAND","KXSONICS","KXNEXTTEAMNBA","KXLBJRETIRE","KXSPORTSOWNERLBJ","KXSTEPHDEAL","KXQUADRUPLEDOUBLE","KXSHAI20PTREC","KXNBA2KCOVER"]),("WNBA",["KXWNBADRAFT1","KXWNBADRAFTTOP3","KXWNBADELAY","KXWNBAGAMESPLAYED"]),("NCAAB",["KXMARMAD","KXNCAAMBNEXTCOACH"]),("International",["KXEUROLEAGUEGAME","KXBSLGAME","KXBBLGAME","KXACBGAME","KXISLGAME","KXABAGAME","KXCBAGAME","KXBBSERIEAGAME","KXJBLEAGUEGAME","KXLNBELITEGAME","KXARGLNBGAME","KXVTBGAME"]),],
"Baseball":[("MLB Games",["KXMLBGAME","KXMLBRFI","KXMLBSPREAD","KXMLBTOTAL","KXMLBTEAMTOTAL","KXMLBF5","KXMLBF5SPREAD","KXMLBF5TOTAL","KXMLBHIT","KXMLBHR","KXMLBHRR","KXMLBKS","KXMLBTB"]),("MLB Season",["KXMLB","KXMLBAL","KXMLBNL","KXMLBALEAST","KXMLBALWEST","KXMLBALCENT","KXMLBNLEAST","KXMLBNLWEST","KXMLBNLCENT","KXMLBPLAYOFFS","KXTEAMSINWS","KXMLBBESTRECORD","KXMLBWORSTRECORD","KXMLBLSTREAK","KXMLBWSTREAK"]),("MLB Awards",["KXMLBALMVP","KXMLBNLMVP","KXMLBALCY","KXMLBNLCY","KXMLBALROTY","KXMLBNLROTY","KXMLBEOTY","KXMLBALMOTY","KXMLBNLMOTY","KXMLBALHAARON","KXMLBNLHAARON","KXMLBALCPOTY","KXMLBNLCPOTY","KXMLBALRELOTY","KXMLBNLRELOTY"]),("MLB Stats",["KXMLBSTAT","KXMLBSTATCOUNT","KXMLBSEASONHR","KXLEADERMLBAVG","KXLEADERMLBDOUBLES","KXLEADERMLBERA","KXLEADERMLBHITS","KXLEADERMLBHR","KXLEADERMLBKS","KXLEADERMLBOPS","KXLEADERMLBRBI","KXLEADERMLBRUNS","KXLEADERMLBSTEALS","KXLEADERMLBTRIPLES","KXLEADERMLBWAR","KXLEADERMLBWINS"]),("MLB Other",["KXMLBTRADE","KXWSOPENTRANTS"]),("International",["KXNPBGAME","KXKBOGAME","KXNCAABBGAME"]),("NCAA",["KXNCAABASEBALL","KXNCAABBGS"]),],
"Football":[("NFL Games",["KXUFLGAME"]),("NFL Season",["KXSB","KXNFLPLAYOFF","KXNFLAFCCHAMP","KXNFLNFCCHAMP","KXNFLAFCEAST","KXNFLAFCWEST","KXNFLAFCNORTH","KXNFLAFCSOUTH","KXNFLNFCEAST","KXNFLNFCWEST","KXNFLNFCNORTH","KXNFLNFCSOUTH","KXRECORDNFLBEST","KXRECORDNFLWORST"]),("NFL Awards",["KXNFLMVP","KXNFLOPOTY","KXNFLDPOTY","KXNFLOROTY","KXNFLDROTY","KXNFLCOTY"]),("NFL Draft",["KXNFLDRAFT1","KXNFLDRAFT1ST","KXNFLDRAFTPICK","KXNFLDRAFTTOP","KXNFLDRAFTWR","KXNFLDRAFTDB","KXNFLDRAFTTE","KXNFLDRAFTQB","KXNFLDRAFTOL","KXNFLDRAFTEDGE","KXNFLDRAFTLB","KXNFLDRAFTRB","KXNFLDRAFTDT","KXNFLDRAFTTEAM"]),("NFL Stats",["KXLEADERNFLSACKS","KXLEADERNFLINT","KXLEADERNFLPINT","KXLEADERNFLPTDS","KXLEADERNFLPYDS","KXLEADERNFLRTDS","KXLEADERNFLRUSHTDS","KXLEADERNFLRUSHYDS","KXLEADERNFLRYDS","KXNFLTEAM1POS","KXNFLPRIMETIME"]),("NFL Other",["KXNFLTRADE","KXNEXTTEAMNFL","KXKELCERETIRE","KXSTARTINGQBWEEK1","KXCOACHOUTNFL","KXCOACHOUTNCAAFB","KXARODGRETIRE","KXRELOCATIONCHI","KX1STHOMEGAME","KXSORONDO","KXDONATEMRBEAST"]),("NCAAF",["KXNCAAF","KXHEISMAN","KXNCAAFCONF","KXNCAAFACC","KXNCAAFB10","KXNCAAFB12","KXNCAAFSEC","KXNCAAFAAC","KXNCAAFSBELT","KXNCAAFMWC","KXNCAAFMAC","KXNCAAFCUSA","KXNCAAFPAC12","KXNCAAFPLAYOFF","KXNCAAFFINALIST","KXNCAAFUNDEFEATED","KXNCAAFCOTY","KXNCAAFAPRANK"]),("Other",["KXNDJOINCONF","KXCOVEREA"]),],
"Hockey":[("NHL Games",["KXNHLGAME","KXNHLSPREAD","KXNHLTOTAL"]),("NHL Season",["KXNHL","KXNHLPLAYOFF","KXTEAMSINSC","KXNHLPRES","KXNHLEAST","KXNHLWEST","KXNHLADAMS","KXNHLCENTRAL","KXNHLATLANTIC","KXNHLMETROPOLITAN","KXNHLPACIFIC"]),("NHL Awards",["KXNHLHART","KXNHLNORRIS","KXNHLVEZINA","KXNHLCALDER","KXNHLROSS","KXNHLRICHARD"]),("AHL",["KXAHLGAME"]),("International",["KXKHLGAME","KXSHLGAME","KXLIIGAGAME","KXELHGAME","KXNLGAME","KXDELGAME"]),("Other",["KXCANADACUP","KXNCAAHOCKEY","KXNCAAHOCKEYGAME"]),],
"Tennis":[("ATP Matches",["KXATPMATCH","KXATPSETWINNER","KXATPCHALLENGERMATCH","KXMCMMEN","KXFOMEN"]),("WTA Matches",["KXWTAMATCH","KXFOWOMEN"]),("Grand Slams",["KXGRANDSLAM","KXATPGRANDSLAM","KXWTAGRANDSLAM","KXATPGRANDSLAMFIELD","KXGRANDSLAMJFONSECA"]),("Rankings",["KXATP1RANK"]),("Other",["KXWTASERENA","KXGOLFTENNISMAJORS"]),],
"Golf":[("Tour Events",["KXPGATOUR","KXPGAH2H","KXPGA3BALL","KXPGA5BALL","KXPGAR1LEAD","KXPGAR1TOP5","KXPGAR1TOP10","KXPGAR1TOP20","KXPGAR2LEAD","KXPGAR2TOP5","KXPGAR2TOP10","KXPGAR3LEAD","KXPGAR3TOP5","KXPGAR3TOP10","KXPGATOP5","KXPGATOP10","KXPGATOP20","KXPGATOP40","KXPGAPLAYOFF","KXPGACUTLINE","KXPGAMAKECUT","KXPGAAGECUT","KXPGAWINNERREGION","KXPGALOWSCORE","KXPGASTROKEMARGIN","KXPGAWINNINGSCORE","KXPGAPLAYERCAT","KXPGABIRDIES","KXPGAROUNDSCORE","KXPGAEAGLE","KXPGAHOLEINONE","KXPGABOGEYFREE","KXPGAMASTERS"]),("Majors",["KXPGAMAJORTOP10","KXPGAMAJORWIN","KXGOLFMAJORS"]),("Ryder Cup",["KXPGARYDER","KXPGASOLHEIM","KXRYDERCUPCAPTAIN"]),("Player Props",["KXPGACURRY","KXPGATIGER","KXBRYSONCOURSERECORDS","KXSCOTTIESLAM","KXGOLFTENNISMAJORS"]),],
"MMA":[("UFC Fights",["KXUFCFIGHT"]),("UFC Titles",["KXUFCHEAVYWEIGHTTITLE","KXUFCLHEAVYWEIGHTTITLE","KXUFCMIDDLEWEIGHTTITLE","KXUFCWELTERWEIGHTTITLE","KXUFCLIGHTWEIGHTTITLE","KXUFCFEATHERWEIGHTTITLE","KXUFCBANTAMWEIGHTTITLE","KXUFCFLYWEIGHTTITLE"]),("UFC Other",["KXMCGREGORFIGHTNEXT","KXCARDPRESENCEUFCWH","KXUFCWHITEHOUSE"]),],
"Cricket":[("IPL",["KXIPLGAME","KXIPL","KXIPLFOUR","KXIPLSIX","KXIPLTEAMTOTAL"]),("PSL",["KXPSLGAME","KXPSL"]),("Other",["KXT20MATCH"]),],
"Esports":[("Valorant",["KXVALORANTMAP","KXVALORANTGAME"]),("League of Legends",["KXLOLGAME","KXLOLMAP","KXLOLTOTALMAPS"]),("CS2",["KXCS2GAME","KXCS2MAP","KXCS2TOTALMAPS"]),("Rainbow Six",["KXR6GAME"]),("Dota 2",["KXDOTA2GAME","KXDOTA2MAP"]),("Overwatch",["KXOWGAME"]),],
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
    overlaying live WebSocket prices from LIVE_PRICES where available."""
    try:
        from kalshi_ws import LIVE_PRICES
    except Exception:
        LIVE_PRICES = {}
    out = []
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
        chance_c, yes_c, no_c = _midprice_and_ask(yb, ya, nb, na)
        out.append({
            "label":  o.get("label", ""),
            "ticker": tk,
            "chance": f"{int(round(chance_c))}%" if chance_c is not None else "—",
            "yes":    f"{int(round(yes_c))}¢"    if yes_c    is not None else "—",
            "no":     f"{int(round(no_c))}¢"     if no_c     is not None else "—",
        })
    return out


# ── Cache with TTL ─────────────────────────────────────────────────────────────
_cache = {"data": None, "ts": 0}  # cache cleared on startup
CACHE_TTL = 1800

def get_data():
    global _cache
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < CACHE_TTL:
        return _cache["data"]

    all_ev = paginate(with_markets=True, max_pages=30)
    if not all_ev:
        return []

    # Rough "exp_dt − kickoff" window per sport. Kalshi's
    # expected_expiration_time is set to the final-whistle + some
    # settlement buffer, so these values are slightly longer than
    # real game length. Used only for the wall-clock minute estimate
    # when ESPN/SofaScore don't match the event.
    DURATION = {
        "Soccer": timedelta(hours=2, minutes=15),
        "Baseball": timedelta(hours=3),
        "Basketball": timedelta(hours=2, minutes=30),
        "Hockey": timedelta(hours=2, minutes=30),
        "Football": timedelta(hours=3, minutes=30),
        "Cricket": timedelta(hours=4),
        "Tennis": timedelta(hours=2, minutes=30),
        "Golf": timedelta(hours=4),
        "MMA": timedelta(hours=3),
        "Esports": timedelta(hours=2),
        "Motorsport": timedelta(hours=3),
        "Rugby": timedelta(hours=2),
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
            label = str(mk.get("yes_sub_title") or "").strip()
            if not label:
                t = str(mk.get("ticker") or "")
                parts = t.rsplit("-", 1)
                label = parts[-1] if len(parts) > 1 else t
            yb = _cents_from(mk, "yes_bid_dollars", "yes_bid")
            ya = _cents_from(mk, "yes_ask_dollars", "yes_ask")
            nb = _cents_from(mk, "no_bid_dollars",  "no_bid")
            na = _cents_from(mk, "no_ask_dollars",  "no_ask")
            # Store raw cents + market ticker. The chance/yes/no display
            # strings are computed per-request by _format_outcomes() so
            # live WebSocket updates flow through without rebuilding the
            # REST snapshot cache.
            outcomes.append({
                "label":  label[:35],
                "ticker": str(mk.get("ticker","")),
                "_yb": yb, "_ya": ya, "_nb": nb, "_na": na,
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
        return sort_dt, sort_ts_dt, game_date, kickoff_dt, exp_dt, display, outcomes

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
            _is_sport = bool(_sport)
            _soccer_comp = SOCCER_COMP.get(series, "Other") if _sport == "Soccer" else ""
            mkts = ev.get("markets")
            if not isinstance(mkts, list):
                mkts = []
            # Stuff into the event dict so extract() can read them.
            ev["category"] = category
            ev["_sport"] = _sport
            ev["_is_sport"] = _is_sport
            ev["_soccer_comp"] = _soccer_comp
            ev["markets"] = mkts

            sort_dt, sort_ts_dt, game_date, kickoff_dt, game_end_dt, display_dt, outcomes = extract(ev)

            if _sport == "Soccer" and _soccer_comp and _soccer_comp not in ("Other", ""):
                _subcat = _soccer_comp
            elif _sport and _sport != "Soccer":
                _subcat = SERIES_TO_SUBTAB.get(_sport, {}).get(series, "")
            else:
                _subcat = ""

            r = {
                "event_ticker": str(ev.get("event_ticker", "")),
                "title": str(ev.get("title", ""))[:90],
                "category": category,
                "series_ticker": str(series_ticker_raw),
                "_sport": _sport,
                "_soccer_comp": _soccer_comp if _soccer_comp != "Other" else "",
                "_subcat": _subcat,
                "_is_sport": _is_sport,
                "_display_dt": display_dt,
                "_kickoff_dt": kickoff_dt.isoformat() if kickoff_dt else None,
                "_game_end_dt": game_end_dt.isoformat() if (kickoff_dt and game_end_dt) else None,
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
    sport_count = sum(1 for r in records if r.get("_is_sport"))
    kickoff_count = sum(1 for r in records if r.get("_kickoff_dt"))
    logging.getLogger("oddsiq").info(
        "get_data: raw=%d records=%d sport=%d kickoff=%d",
        raw_count, len(records), sport_count, kickoff_count,
    )
    _cache["data"] = records
    _cache["ts"] = now
    return records

# ── API routes ─────────────────────────────────────────────────────────────────
@app.get("/api/events")
def get_events(
    category: Optional[str] = None,
    sport: Optional[str] = None,
    soccer_comp: Optional[str] = None,
    search: Optional[str] = None,
    date_filter: Optional[str] = "all",
    sort: Optional[str] = "earliest",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    offset: int = 0,
    limit: int = 24,
):
    from datetime import date as _date
    records = get_data()
    today = _date.today()
    from datetime import datetime as _dt
    now_utc = _dt.now(timezone.utc)

    # Filter
    results = []
    for r in records:
        # Category filter
        if search:
            pass  # when searching, show all categories
        elif category and category != "All":
            if category == "Live":
                # Keep only sport events currently in progress.
                kdt = r.get("_kickoff_dt")
                gdt = r.get("_game_end_dt")
                if not (kdt and gdt):
                    continue
                try:
                    k = _dt.fromisoformat(kdt)
                    g = _dt.fromisoformat(gdt)
                    if not (k <= now_utc < g):
                        continue
                except Exception:
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

        results.append(r)

    # Sort by precise timestamp (kickoff → expiration → close → game date).
    # Undated events always go to the end, regardless of direction.
    dated = [r for r in results if r.get("_sort_ts")]
    undated = [r for r in results if not r.get("_sort_ts")]
    dated.sort(key=lambda r: r["_sort_ts"], reverse=(sort == "latest"))
    results = dated + undated

    total = len(results)
    page  = results[offset:offset+limit]
    # Overlay live WebSocket prices on the paginated page, and attach
    # live game state from ESPN first, falling back to TheSportsDB
    # for leagues ESPN doesn't cover (J2, K League, etc.).
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

    def _score_display(title: str, g: dict) -> str:
        """Build an ordered score string whose team order matches how
        the teams appear in the Kalshi event title. ESPN phrases are
        already accent-stripped/lowercased, so normalize the title
        the same way before searching."""
        if not g:
            return ""
        hs = g.get("home_score", "")
        as_ = g.get("away_score", "")
        if hs == "" or as_ == "":
            return ""
        ha = g.get("home_abbr", "") or "HOME"
        aa = g.get("away_abbr", "") or "AWAY"
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
            return f"{ha} {hs} - {aa} {as_}"
        return f"{aa} {as_} - {ha} {hs}"

    formatted = []
    for r in page:
        rc = dict(r)
        rc["outcomes"] = _format_outcomes(r.get("outcomes", []))
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
        if g:
            rc["_live_state"] = {
                "label":          compact_label(g) if compact_label else "",
                "state":          g.get("state", ""),
                "short_detail":   g.get("short_detail", ""),
                "display_clock":  g.get("display_clock", ""),
                "period":         g.get("period", 0),
                "league":         g.get("league", ""),
                "captured_at_ms": g.get("captured_at_ms", 0),
                "clock_running":  g.get("clock_running", True),
                "home_abbr":      g.get("home_abbr", ""),
                "away_abbr":      g.get("away_abbr", ""),
                "home_score":     g.get("home_score", ""),
                "away_score":     g.get("away_score", ""),
                "score_display":  _score_display(title, g),
            }
        formatted.append(rc)
    return {"total": total, "offset": offset, "limit": limit, "events": formatted}

@app.get("/api/sports")
def get_sports(live: bool = False):
    records = get_data()
    if live:
        from datetime import datetime as _dt
        now_utc = _dt.now(timezone.utc)
        filtered = []
        for r in records:
            kdt = r.get("_kickoff_dt")
            gdt = r.get("_game_end_dt")
            if not (kdt and gdt):
                continue
            try:
                k = _dt.fromisoformat(kdt)
                g = _dt.fromisoformat(gdt)
                if k <= now_utc < g:
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
    return {"sports": sports, "soccer_comps": sorted(soccer_comps)}

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
    """Debug endpoint: reports the Kalshi WebSocket connection state
    and how many markets have received at least one live price tick."""
    try:
        from kalshi_ws import STATUS, LIVE_PRICES
        return {"status": dict(STATUS), "live_count": len(LIVE_PRICES)}
    except Exception as e:
        return {"status": None, "error": str(e)}

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

@app.get("/api/kalshi_event_raw")
def kalshi_event_raw(ticker: str = "", status: str = "open", prefer: str = "sport"):
    """Debug: fetches several pages of Kalshi events and returns
    the full raw structure of one of them — every field on the
    event and every field on each of its markets. Used to verify
    whether Kalshi's public API exposes live game state (score,
    clock, period, status, sub_title hints, etc.).

    Params:
      - ticker: return this exact event_ticker if found in the
        fetched pages, otherwise error out.
      - status: "open" (default) or "closed".
      - prefer: "sport" (default) returns the first event whose
        series_ticker matches a known sport; "any" returns the
        first event regardless.
    """
    try:
        client = get_client()
        events: List[Dict[str, Any]] = []
        cursor = None
        # Pull multiple pages because the first page is usually
        # dominated by political / longshot markets; sport events
        # come later in the unfiltered listing.
        for _ in range(6):
            kw = {"limit": 200, "status": status, "with_nested_markets": True}
            if cursor:
                kw["cursor"] = cursor
            resp = client.get_events(**kw).to_dict()
            events.extend(resp.get("events", []) or [])
            cursor = resp.get("cursor") or resp.get("next_cursor")
            if not cursor:
                break
            if ticker is None:
                break
        if not events:
            return {"error": "no events returned", "status": status}

        picked = None
        if ticker:
            for ev in events:
                if ev.get("event_ticker") == ticker:
                    picked = ev
                    break
            if picked is None:
                return {
                    "error": f"ticker {ticker!r} not in first {len(events)} {status} events",
                    "hint": "call without ?ticker= to get a sample sport event",
                }
        elif prefer == "sport":
            for ev in events:
                series = str(ev.get("series_ticker") or "").upper()
                if get_sport(series):
                    picked = ev
                    break
            if picked is None:
                picked = events[0]
        else:
            picked = events[0]

        markets = picked.get("markets") or []
        first_market = markets[0] if markets else {}
        all_market_fields = set()
        for mk in markets:
            if isinstance(mk, dict):
                all_market_fields.update(mk.keys())
        return {
            "event_ticker": picked.get("event_ticker"),
            "series_ticker": picked.get("series_ticker"),
            "derived_sport": get_sport(str(picked.get("series_ticker") or "").upper()),
            "event_fields": sorted(list(picked.keys())),
            "event": picked,
            "market_count": len(markets),
            "first_market_fields": sorted(list(first_market.keys())) if isinstance(first_market, dict) else None,
            "union_of_all_market_fields": sorted(list(all_market_fields)),
            "first_market": first_market,
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

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
        async with httpx.AsyncClient(headers={"User-Agent": "oddsiq/1.0"}) as client:
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
                status = (ev.get("status") or {}).get("type", {})
                out["sample_state"] = status.get("state")
                out["sample_detail"] = status.get("shortDetail")
                comps = (ev.get("competitions") or [{}])[0]
                cps = comps.get("competitors") or []
                out["sample_competitor_count"] = len(cps)
                if cps:
                    out["sample_competitor_0"] = {
                        "id": cps[0].get("id"),
                        "type": cps[0].get("type"),
                        "team": (cps[0].get("team") or {}).get("displayName"),
                        "athlete": (cps[0].get("athlete") or {}).get("displayName"),
                        "score": cps[0].get("score"),
                    }
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
async def sofascore_probe():
    """Debug: makes a fresh call to SofaScore's live football events
    endpoint and returns status, headers, and the first chunk of body
    so we can tell whether Cloudflare / the API is blocking us."""
    try:
        import httpx
        url = "https://api.sofascore.com/api/v1/sport/football/events/live"
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

# ── Serve frontend ─────────────────────────────────────────────────────────────
import os as _os

@app.get("/", response_class=HTMLResponse)  
def root():
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "index.html")
    if _os.path.exists(p):
        with open(p, "r") as f:
            return f.read()
    return "<h1>static/index.html not found</h1><p>Make sure index.html is in the static/ folder</p>"
