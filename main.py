from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os, time, tempfile, functools
from datetime import date, timedelta, timezone
from typing import Optional

app = FastAPI(title="OddsIQ API")

@app.on_event("startup")
async def startup_event():
    global _cache
    _cache = {"data": None, "ts": 0}
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
    try:
        if val is None or val == "": return None
        if isinstance(val, str) and val.strip() in ("", "NaT", "None", "nan"): return None
        ts = pd.to_datetime(val, utc=True)
        if pd.isna(ts): return None
        return ts.to_pydatetime().astimezone(UTC)
    except: return None

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

    df = pd.DataFrame(all_ev)
    df["category"] = df.get("category", pd.Series("Other", index=df.index)).fillna("Other").str.strip()
    df["_series"]  = df.get("series_ticker", pd.Series("", index=df.index)).fillna("").str.upper()
    df["_sport"]   = df["_series"].apply(get_sport)
    df["_is_sport"]= df["_sport"] != ""
    if "markets" not in df.columns:
        df["markets"] = [[] for _ in range(len(df))]
    df["markets"] = df["markets"].apply(lambda x: x if isinstance(x, list) else [])
    df["_soccer_comp"] = df.apply(
        lambda r: SOCCER_COMP.get(r["_series"],"Other") if r["_sport"]=="Soccer" else "", axis=1)

    DURATION = {
        "Soccer": timedelta(hours=3), "Baseball": timedelta(hours=3),
        "Basketball": timedelta(hours=2), "Hockey": timedelta(hours=2, minutes=30),
        "Football": timedelta(hours=3), "Cricket": timedelta(hours=4),
        "Tennis": timedelta(hours=2), "Golf": timedelta(hours=4),
        "MMA": timedelta(hours=3), "Esports": timedelta(hours=2),
        "Motorsport": timedelta(hours=3), "Rugby": timedelta(hours=2),
    }

    def extract(row):
        mkts = row.get("markets")
        if not isinstance(mkts, list) or not mkts:
            return None, None, None, "", []
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

        # For non-sport events with no game_date, use close_dt as the resolve date
        if not game_date and close_dt:
            game_date = close_dt.date()
        sort_dt = game_date if game_date else (exp_dt.date() if exp_dt else (close_dt.date() if close_dt else None))
        outcomes = []
        for mk in mkts:
            label = str(mk.get("yes_sub_title") or "").strip()
            if not label:
                t = str(mk.get("ticker") or "")
                parts = t.rsplit("-", 1)
                label = parts[-1] if len(parts) > 1 else t
            yf = nf = None
            try:
                yd = mk.get("yes_bid_dollars")
                nd = mk.get("no_bid_dollars")
                if yd is not None: yf = float(yd)
                if nd is not None: nf = float(nd)
                if yf is None:
                    yb = mk.get("yes_bid")
                    if yb is not None: yf = float(yb)/100
                if nf is None:
                    nb = mk.get("no_bid")
                    if nb is not None: nf = float(nb)/100
            except: pass
            chance = f"{int(round(yf*100))}%" if yf is not None else "—"
            yes    = f"{int(round(yf*100))}¢"  if yf is not None else "—"
            no     = f"{int(round(nf*100))}¢"  if nf is not None else "—"
            outcomes.append({"label":label[:35],"chance":chance,"yes":yes,"no":no})
        # Show date+time if we have a reliable kickoff/release time
        # For sports: kickoff_dt calculated from exp_dt - duration
        # For non-sports: use exp_dt directly if it's within 60 days of today
        display = ""
        if kickoff_dt:
            # Sport event with calculated kickoff
            try:
                import pytz as _pytz
                eastern = _pytz.timezone("US/Eastern")
                kt = kickoff_dt.astimezone(eastern)
                hour = kt.hour % 12 or 12
                ampm = "am" if kt.hour < 12 else "pm"
                tz_label = kt.strftime("%Z")
                display = f"{kt.strftime('%b')} {kt.day}, {hour}:{kt.strftime('%M')}{ampm} {tz_label}"
            except:
                display = ""
        elif exp_dt and not sport:
            # Non-sport event with a specific expiration date (e.g. CPI release)
            # Only show if exp_dt is a specific date/time (not years in future)
            try:
                from datetime import date as _date2
                import pytz as _pytz
                eastern = _pytz.timezone("US/Eastern")
                days_out = (exp_dt.date() - _date2.today()).days
                if 0 <= days_out <= 400:  # within ~1 year
                    kt = exp_dt.astimezone(eastern)
                    hour = kt.hour % 12 or 12
                    ampm = "am" if kt.hour < 12 else "pm"
                    tz_label = kt.strftime("%Z")
                    display = f"{kt.strftime('%b')} {kt.day}, {hour}:{kt.strftime('%M')}{ampm} {tz_label}"
            except:
                display = ""
        return sort_dt, game_date, kickoff_dt, display, outcomes

    records = []
    for _, row in df.iterrows():
        try:
            sort_dt, game_date, kickoff_dt, display_dt, outcomes = extract(row)
            _sport = str(row.get("_sport",""))
            _series = str(row.get("series_ticker","")).upper()
            _soccer_comp = str(row.get("_soccer_comp",""))
            if _sport == "Soccer" and _soccer_comp and _soccer_comp not in ("Other",""):
                _subcat = _soccer_comp
            elif _sport and _sport != "Soccer":
                _subcat = SERIES_TO_SUBTAB.get(_sport, {}).get(_series, "")
            else:
                _subcat = ""
            r = {
                "event_ticker": str(row.get("event_ticker","")),
                "title": str(row.get("title",""))[:90],
                "category": str(row.get("category","Other")),
                "series_ticker": str(row.get("series_ticker","")),
                "_sport": _sport,
                "_soccer_comp": _soccer_comp,
                "_subcat": _subcat,
                "_is_sport": bool(row.get("_is_sport",False)),
                "_display_dt": display_dt,
                "_kickoff_dt": kickoff_dt.isoformat() if kickoff_dt else None,
                "_sort_dt": sort_dt.isoformat() if sort_dt else None,
                "outcomes": outcomes,
            }
            records.append(r)
        except: pass

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

    # Filter
    results = []
    for r in records:
        # Category filter
        if search:
            pass  # when searching, show all categories
        elif category and category != "All":
            if category == "Sports":
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

        # Search
        if search:
            s = search.lower()
            if s not in r["title"].lower() and s not in r["event_ticker"].lower():
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

    # Sort
    def sort_key(r):
        s = r.get("_sort_dt")
        return s if s else "9999-99-99"

    results.sort(key=sort_key, reverse=(sort=="latest"))

    total = len(results)
    page  = results[offset:offset+limit]
    return {"total": total, "offset": offset, "limit": limit, "events": page}

@app.get("/api/sports")
def get_sports():
    records = get_data()
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

# ── Serve frontend ─────────────────────────────────────────────────────────────
import os as _os

@app.get("/", response_class=HTMLResponse)  
def root():
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "index.html")
    if _os.path.exists(p):
        with open(p, "r") as f:
            return f.read()
    return "<h1>static/index.html not found</h1><p>Make sure index.html is in the static/ folder</p>"
