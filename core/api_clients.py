"""
api_clients.py - API wrappers

Sources :
  - The Odds API   → fixtures + cotes
  - football-data.org → standings, résultats, forme récente, H2H
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

# Rate limiter football-data.org : 10 req/min plan gratuit
_fd_last_call   = 0.0
FD_MIN_INTERVAL = 6.5  # secondes minimum entre chaque appel FD

# ── Tracker tokens Odds API ──────────────────
_odds_tokens = {"remaining": None, "used": None, "last_update": None}

def get_odds_quota() -> dict:
    """Retourne les tokens Odds API restants depuis le cache."""
    return dict(_odds_tokens)

def _update_odds_quota(headers: dict):
    """Met à jour le cache depuis les headers de réponse Odds API."""
    try:
        rem = headers.get("x-requests-remaining")
        used = headers.get("x-requests-used")
        if rem is not None:
            _odds_tokens["remaining"] = int(rem)
        if used is not None:
            _odds_tokens["used"] = int(used)
        from datetime import datetime, timezone
        _odds_tokens["last_update"] = datetime.now(timezone.utc).isoformat()
    except Exception:
        pass

def _fd_rate_limit():
    """Attend si nécessaire pour respecter la limite 10 req/min."""
    global _fd_last_call
    elapsed = time.time() - _fd_last_call
    if elapsed < FD_MIN_INTERVAL:
        time.sleep(FD_MIN_INTERVAL - elapsed)
    _fd_last_call = time.time()

def _fd_get(url: str, params: dict = None, retries: int = 3) -> dict:
    """
    Wrapper GET football-data.org avec retry intelligent.
    - 429 : attend 65-125s et reessaie (rate limit temporaire)
    - 403 : raise immediat sans retry (acces refuse = definitif, plan gratuit limite a 2 saisons)
    - 404 : raise immediat sans retry
    - Autres erreurs reseau : retry avec pause 10s
    """
    for attempt in range(retries):
        _fd_rate_limit()
        try:
            resp = requests.get(url, headers=_fd_headers(), params=params or {}, timeout=20)

            if resp.status_code == 403:
                raise requests.HTTPError(
                    f"403 Acces refuse (saison archivee ? plan gratuit = 2 saisons recentes max)",
                    response=resp
                )
            if resp.status_code == 404:
                raise requests.HTTPError(f"404 Not Found: {url}", response=resp)
            if resp.status_code == 429:
                wait = 65 + attempt * 30
                print(f"[FD] 429 Rate limit — attente {wait}s avant retry {attempt+1}/{retries}...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", 0) if e.response is not None else 0
            if status in (403, 404):
                raise  # pas de retry
            if attempt == retries - 1:
                raise
            print(f"[FD] Erreur HTTP attempt {attempt+1}: {e} — retry dans 10s")
            time.sleep(10)

        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"[FD] Erreur attempt {attempt+1}: {e} — retry dans 10s")
            time.sleep(10)

    return {}


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

ODDS_API_BASE     = "https://api.the-odds-api.com/v4"
FOOTBALLDATA_BASE = "https://api.football-data.org/v4"

LEAGUE_SPORT_MAP = {
    39:  "soccer_epl",
    61:  "soccer_france_ligue_one",
    78:  "soccer_germany_bundesliga",
    135: "soccer_italy_serie_a",
    140: "soccer_spain_la_liga",
    88:  "soccer_netherlands_eredivisie",
    94:  "soccer_portugal_primeira_liga",
    71:  "soccer_brazil_campeonato",
    40:  "soccer_efl_champ",
    2:   "soccer_uefa_champs_league",
    144: "soccer_belgium_first_div",
    203: "soccer_turkey_super_league",
    179: "soccer_spl",
    262: "soccer_mexico_ligamx",
    3:   "soccer_uefa_europa_league",
}

FOOTBALLDATA_LEAGUE_MAP = {
    39:  "PL",
    61:  "FL1",
    78:  "BL1",
    135: "SA",
    140: "PD",
    88:  "DED",
    94:  "PPL",
    71:  "BSA",
    40:  "ELC",
    2:   "CL",
}

FD_COMPETITION_TO_LEAGUE = {
    "Premier League":           39,
    "Ligue 1":                  61,
    "Bundesliga":               78,
    "Serie A":                  135,
    "Primera Division":         140,
    "Eredivisie":               88,
    "Primeira Liga":            94,
    "Championship":             40,
    "UEFA Champions League":    2,
    "UEFA Europa League":       3,
    "Belgian First Division A": 144,
    "Super Lig":                203,
    "Scottish Premiership":     179,
    "Série A":                  71,
    "Liga MX":                  262,
}

PREFERRED_BOOKMAKERS = [
    "Betclic (FR)", "Betclic",
    "Winamax (FR)", "Winamax",
    "Unibet (FR)", "Unibet",
]

TEAM_NAME_MAP = {
    # Ligue 1
    "fc nantes": "nantes", "angers sco": "angers", "aj auxerre": "auxerre",
    "rc strasbourg alsace": "strasbourg", "toulouse fc": "toulouse",
    "olympique de marseille": "marseille", "olympique lyonnais": "lyon",
    "paris saint-germain fc": "paris saint germain", "stade rennais fc": "rennes",
    "losc lille": "lille", "ogc nice": "nice", "as monaco fc": "as monaco",
    "rc lens": "rc lens", "stade brestois 29": "brest", "stade de reims": "reims",
    "le havre ac": "le havre", "montpellier hsc": "montpellier",
    "as saint-etienne": "saint-etienne", "paris fc": "paris fc",
    # Premier League
    "manchester city fc": "manchester city", "arsenal fc": "arsenal",
    "liverpool fc": "liverpool", "chelsea fc": "chelsea",
    "tottenham hotspur fc": "tottenham hotspur", "manchester united fc": "manchester united",
    "newcastle united fc": "newcastle united", "aston villa fc": "aston villa",
    "west ham united fc": "west ham united", "brighton & hove albion fc": "brighton",
    "wolverhampton wanderers fc": "wolverhampton wanderers", "fulham fc": "fulham",
    "brentford fc": "brentford", "crystal palace fc": "crystal palace",
    "everton fc": "everton", "nottingham forest fc": "nottingham forest",
    "bournemouth afc": "bournemouth", "ipswich town fc": "ipswich town",
    "leicester city fc": "leicester city", "sunderland afc": "sunderland",
    # Bundesliga
    "fc bayern münchen": "bayern munich", "fc bayern munchen": "bayern munich",
    "borussia dortmund": "borussia dortmund", "bayer 04 leverkusen": "bayer leverkusen",
    "rb leipzig": "rb leipzig", "eintracht frankfurt": "eintracht frankfurt",
    "vfb stuttgart": "vfb stuttgart", "sc freiburg": "sc freiburg",
    "vfl wolfsburg": "wolfsburg", "1. fsv mainz 05": "mainz",
    "tsg 1899 hoffenheim": "hoffenheim", "1. fc köln": "koln",
    "fc augsburg": "augsburg", "sv werder bremen": "werder bremen",
    "1. fc union berlin": "union berlin",
    "borussia mönchengladbach": "borussia monchengladbach",
    "1. fc heidenheim 1846": "heidenheim",
    # Serie A
    "fc internazionale milano": "inter milan", "ac milan": "ac milan",
    "juventus fc": "juventus", "ssc napoli": "napoli", "as roma": "as roma",
    "ss lazio": "lazio", "atalanta bc": "atalanta", "acf fiorentina": "fiorentina",
    "bologna fc 1909": "bologna", "torino fc": "torino", "udinese calcio": "udinese",
    "cagliari calcio": "cagliari", "como 1907": "como",
    # La Liga
    "real madrid cf": "real madrid", "fc barcelona": "barcelona",
    "club atletico de madrid": "atletico madrid", "athletic club": "athletic bilbao",
    "real sociedad de futbol": "real sociedad", "villarreal cf": "villarreal",
    "sevilla fc": "sevilla", "real betis balompie": "real betis",
    "ca osasuna": "osasuna", "rcd mallorca": "mallorca", "girona fc": "girona",
    # Eredivisie
    "psv eindhoven": "psv", "afc ajax": "ajax", "feyenoord": "feyenoord",
    "az alkmaar": "az", "fc groningen": "groningen",
    # Primeira Liga
    "sporting clube de portugal": "sporting cp", "fc porto": "porto",
    "sl benfica": "benfica", "sporting clube de braga": "braga",
    "moreirense fc": "moreirense", "cd nacional": "nacional", "casa pia ac": "casa pia",
}

# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

def normalize_team_name(name: str) -> str:
    n = name.lower().strip()
    if n in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[n]
    for suffix in [" fc", " afc", " sc", " cf", " ac", " rc", " as", " us", " ssc", " ss"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def _fd_headers() -> dict:
    return {"X-Auth-Token": os.getenv("FOOTBALLDATA_KEY", "")}


# ─────────────────────────────────────────────
# FIXTURES — The Odds API
# ─────────────────────────────────────────────

def get_fixtures(league_id: int, season: int, days_ahead: int = 10) -> list:
    sport_key = LEAGUE_SPORT_MAP.get(league_id)
    if not sport_key:
        return []

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey":     os.getenv("ODDS_API_KEY", ""),
        "regions":    "eu,fr",
        "markets":    "h2h",
        "oddsFormat": "decimal",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        _update_odds_quota(resp.headers)
        events = resp.json()
    except Exception as e:
        print(f"[get_fixtures] Erreur league {league_id}: {e}")
        return []

    now    = datetime.now(timezone.utc)
    cutoff = now.timestamp() + days_ahead * 86400
    fixtures = []

    for i, event in enumerate(events):
        try:
            ts = datetime.fromisoformat(
                event.get("commence_time", "").replace("Z", "+00:00")
            ).timestamp()
        except Exception:
            continue
        if ts < now.timestamp() - 10800 or ts > cutoff:
            continue
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        fixtures.append({
            "fixture_id":     event.get("id", f"odds_{i}"),
            "date":           event.get("commence_time", "")[:10],
            "home_team_name": home,
            "away_team_name": away,
            "home_team_id":   abs(hash(home)) % 100000,
            "away_team_id":   abs(hash(away)) % 100000,
            "league_id":      league_id,
        })
    return fixtures


# ─────────────────────────────────────────────
# ODDS — The Odds API
# ─────────────────────────────────────────────

def get_odds(league_id: int) -> list:
    sport_key = LEAGUE_SPORT_MAP.get(league_id)
    if not sport_key:
        return []

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey":     os.getenv("ODDS_API_KEY", ""),
        "regions":    "eu,fr",
        "markets":    "h2h,totals",
        "oddsFormat": "decimal",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        _update_odds_quota(resp.headers)
        events = resp.json()
    except Exception as e:
        print(f"[get_odds] Erreur league {league_id}: {e}")
        return []

    results = []
    for event in events:
        home = event.get("home_team")
        away = event.get("away_team")
        all_bk = {}
        pref_bk = {}

        for bk in event.get("bookmakers", []):
            bk_name = bk.get("title", bk.get("key", "Unknown"))
            entry = {}
            for mkt in bk.get("markets", []):
                mkt_key = mkt.get("key")
                if mkt_key == "h2h":
                    outcomes = {o["name"]: o["price"] for o in mkt.get("outcomes", [])}
                    entry["home_win"] = outcomes.get(home)
                    entry["draw"]     = outcomes.get("Draw")
                    entry["away_win"] = outcomes.get(away)
                elif mkt_key == "totals":
                    for o in mkt.get("outcomes", []):
                        point = o.get("point")
                        name  = o.get("name", "")
                        price = o.get("price")
                        if point is None or price is None:
                            continue
                        suffix = str(float(point)).replace(".", "_")
                        if name.lower() == "over":
                            entry[f"over_{suffix}"] = price
                        elif name.lower() == "under":
                            entry[f"under_{suffix}"] = price
            if entry:
                all_bk[bk_name] = entry
                bk_key = bk.get("key", "")
                is_pref = (bk_name in PREFERRED_BOOKMAKERS or
                           any(p.lower() in bk_name.lower() for p in PREFERRED_BOOKMAKERS) or
                           bk_key in ("winamax_fr", "betclic_fr", "unibet_fr"))
                if is_pref:
                    pref_bk[bk_name] = entry

        final = pref_bk if pref_bk else all_bk
        if not final:
            continue

        results.append({
            "date":      event.get("commence_time", "")[:10],
            "home_team": home,
            "away_team": away,
            "odds":      final,
            "event_id":  event.get("id"),
            "league_id": league_id,
        })
    return results


# ─────────────────────────────────────────────
# TEAM STANDINGS — football-data.org + fallback
# ─────────────────────────────────────────────

FALLBACK_STATS = {
    78: [  # Bundesliga
        ("Bayern Munich",30,8,12,10),("Bayer Leverkusen",26,12,14,14),
        ("Borussia Dortmund",24,14,14,16),("RB Leipzig",24,12,14,14),
        ("Eintracht Frankfurt",22,14,14,16),("VfB Stuttgart",22,14,12,16),
        ("SC Freiburg",18,16,12,18),("Union Berlin",16,18,12,20),
        ("Werder Bremen",18,18,12,18),("Borussia Monchengladbach",18,18,10,20),
        ("Wolfsburg",16,18,10,20),("Mainz",16,18,10,20),("Augsburg",14,20,10,22),
        ("Hoffenheim",14,20,10,22),("Heidenheim",12,22,8,24),("Koln",10,24,6,26),
        ("Bochum",10,24,6,26),("Hamburger SV",12,22,8,24),
    ],
    135: [  # Serie A
        ("Inter Milan",30,10,14,12),("AC Milan",26,12,14,14),("Juventus",26,12,12,14),
        ("Napoli",24,12,14,14),("AS Roma",22,14,14,16),("Lazio",22,14,12,16),
        ("Atalanta",22,12,14,14),("Fiorentina",20,14,12,16),("Bologna",18,16,12,18),
        ("Torino",16,18,10,20),("Udinese",14,18,10,20),("Genoa",14,20,10,22),
        ("Cagliari",12,20,8,22),("Lecce",12,22,8,24),("Empoli",12,22,8,24),
        ("Como",12,22,8,24),("Verona",10,22,6,24),("Monza",14,20,10,22),
        ("Parma",10,22,6,24),("Venezia",8,26,4,28),
    ],
    140: [  # La Liga
        ("Real Madrid",36,8,14,10),("Barcelona",34,10,14,12),
        ("Atletico Madrid",26,12,14,14),("Athletic Bilbao",24,12,14,14),
        ("Real Sociedad",22,14,14,16),("Villarreal",22,14,12,16),
        ("Sevilla",20,16,12,18),("Real Betis",18,16,12,18),("Osasuna",16,18,10,20),
        ("Mallorca",14,20,10,22),("Rayo Vallecano",14,18,10,20),
        ("Celta Vigo",14,20,10,22),("Getafe",12,20,8,22),("Girona",16,16,12,18),
        ("Alaves",10,22,6,24),("Las Palmas",10,22,6,24),("Valladolid",8,24,4,26),
        ("Leganes",8,24,4,26),("Espanyol",10,22,6,24),("Levante",12,22,8,24),
    ],
    88: [  # Eredivisie
        ("PSV",32,8,14,10),("Ajax",28,12,14,12),("Feyenoord",26,12,14,14),
        ("AZ",24,12,12,14),("Twente",22,14,12,16),("Utrecht",20,16,12,18),
        ("Heerenveen",16,18,10,20),("Heracles",14,18,10,20),
        ("NEC Nijmegen",14,20,10,22),("Sparta Rotterdam",12,22,8,24),
        ("Excelsior",10,22,6,24),("Groningen",10,22,6,24),
        ("Go Ahead Eagles",12,20,8,22),("Fortuna Sittard",10,24,6,26),
        ("PEC Zwolle",8,24,4,26),("Almere City",10,24,6,26),
    ],
    94: [  # Primeira Liga
        ("Sporting CP",30,8,14,10),("Porto",28,10,14,12),("Benfica",28,10,12,12),
        ("Braga",22,14,12,16),("Vitoria Guimaraes",18,16,10,18),
        ("Estoril",14,18,10,20),("Famalicao",12,20,8,22),("Boavista",12,20,8,22),
        ("Casa Pia",12,22,8,24),("Gil Vicente",10,22,6,24),
        ("Moreirense",10,22,6,24),("Arouca",10,22,6,24),("Rio Ave",10,24,6,26),
        ("Nacional",10,22,6,24),("Alverca",8,24,4,26),("AVS",8,24,4,26),
    ],
    144: [  # Belgium
        ("Club Brugge",28,10,12,12),("Anderlecht",24,12,12,14),
        ("Genk",22,14,12,16),("Gent",22,14,10,16),("Standard Liege",18,16,10,18),
        ("Antwerp",18,16,10,18),("Union SG",20,14,12,16),("OH Leuven",14,18,10,20),
        ("Cercle Brugge",14,18,8,20),("Westerlo",12,20,8,22),
        ("Mechelen",12,20,8,22),("Charleroi",12,20,8,22),("Eupen",8,24,4,26),
        ("RWDM",10,22,6,24),("Beerschot",8,26,4,28),("Dender",8,26,4,28),
    ],
    203: [  # Turkey
        ("Galatasaray",30,10,14,12),("Fenerbahce",28,10,12,12),
        ("Besiktas",24,12,12,14),("Trabzonspor",20,14,12,16),
        ("Basaksehir",18,16,10,18),("Sivasspor",14,18,10,20),
        ("Antalyaspor",14,18,8,20),("Konyaspor",12,20,8,22),
        ("Kayserispor",10,22,6,24),("Kasimpasa",10,22,6,24),
        ("Alanyaspor",12,20,8,22),("Rizespor",8,24,4,26),
        ("Gaziantep",8,24,4,26),("Samsunspor",10,22,6,24),
        ("Ankaraguco",10,22,6,24),("Eyupspor",10,22,6,24),
    ],
    179: [  # Scottish Prem
        ("Celtic",32,6,14,8),("Rangers",28,10,12,12),("Hearts",20,14,12,16),
        ("Aberdeen",18,16,10,18),("Hibernian",18,16,10,18),
        ("Motherwell",14,18,8,20),("Livingston",12,20,8,22),
        ("St Mirren",12,20,6,22),("Kilmarnock",10,22,6,24),
        ("Ross County",8,24,4,26),("St Johnstone",8,24,4,26),("Dundee",8,26,4,28),
    ],
    40: [  # Championship
        ("Sunderland",28,10,12,12),("Leeds United",26,10,12,12),
        ("Burnley",24,12,12,14),("Sheffield United",22,12,10,14),
        ("West Brom",20,14,10,16),("Norwich City",18,14,10,16),
        ("Middlesbrough",18,14,8,16),("Millwall",16,14,8,16),
        ("Swansea City",14,16,8,18),("Hull City",12,16,8,18),
        ("Coventry City",12,18,6,20),("Stoke City",12,18,6,20),
        ("Bristol City",10,18,6,20),("Preston",10,18,6,20),
        ("Watford",12,16,8,18),("Blackburn",10,20,6,22),
        ("Cardiff City",10,20,6,22),("QPR",8,22,4,24),
        ("Derby County",10,20,6,22),("Plymouth Argyle",8,22,4,24),
        ("Portsmouth",8,22,4,24),("Sheffield Wednesday",8,24,4,26),
    ],
    2: [  # Champions League
        ("Real Madrid",36,6,14,8),("Manchester City",34,8,14,10),
        ("Bayern Munich",30,8,12,10),("Paris Saint Germain",28,10,12,12),
        ("Liverpool",28,10,12,10),("Arsenal",26,10,12,12),
        ("Inter Milan",24,10,12,12),("Atletico Madrid",22,12,12,14),
        ("Barcelona",26,10,12,12),("Borussia Dortmund",20,12,12,14),
        ("Napoli",20,12,10,14),("Porto",18,14,10,16),("Benfica",18,14,10,16),
        ("RB Leipzig",16,14,10,16),("PSV",16,14,8,16),
        ("Bayer Leverkusen",22,12,10,14),("Aston Villa",16,14,10,16),
        ("AC Milan",20,12,10,14),("Juventus",18,12,10,14),
        ("Feyenoord",14,16,8,18),("Sporting CP",18,12,8,14),
        ("Celtic",14,18,8,20),("Club Brugge",12,18,6,20),("Galatasaray",14,16,8,18),
    ],
    3: [  # Europa League
        ("Roma",22,14,10,16),("Sevilla",20,14,10,16),("Ajax",20,14,10,16),
        ("Manchester United",20,16,10,18),("Lyon",16,16,8,18),
        ("Fiorentina",16,16,8,18),("Fenerbahce",16,14,8,16),
        ("Olympiakos",14,16,8,18),("Rangers",16,16,8,18),
        ("Marseille",18,16,10,18),("Villarreal",18,14,10,16),
        ("Lazio",18,14,10,16),("Athletic Bilbao",16,14,8,16),
        ("Braga",14,16,8,18),("Anderlecht",14,16,8,18),
    ],
    71: [  # Brasileirao
        ("Flamengo",28,12,12,14),("Palmeiras",26,10,12,12),
        ("Atletico Mineiro",24,12,12,14),("Fluminense",20,14,10,16),
        ("Internacional",18,14,10,16),("Corinthians",18,16,10,18),
        ("Sao Paulo",18,14,10,16),("Botafogo",16,14,8,16),
        ("Santos",14,16,8,18),("Fortaleza",14,16,8,18),
        ("Bahia",12,18,6,20),("Vasco",12,18,6,20),("Cruzeiro",14,16,8,18),
        ("Gremio",18,14,10,16),("Bragantino",14,16,8,18),
        ("Atletico GO",12,18,8,20),("Juventude",10,20,6,22),
        ("Vitoria",8,22,4,24),("Criciuma",8,22,4,24),("Cuiaba",10,20,6,22),
    ],
    262: [  # Liga MX
        ("Club America",26,12,12,14),("Chivas",22,14,12,16),
        ("Cruz Azul",20,14,10,16),("Tigres",24,12,12,14),
        ("Monterrey",22,12,12,14),("Pumas",18,16,10,18),("Toluca",18,16,10,18),
        ("Leon",16,18,10,20),("Atlas",14,18,8,20),("Pachuca",16,16,10,18),
        ("Santos",14,20,8,22),("Queretaro",12,20,6,22),("Mazatlan",10,22,6,24),
        ("Necaxa",10,22,6,24),("Tijuana",10,22,6,24),("Puebla",12,20,8,22),
        ("Juarez",8,24,4,26),("San Luis",8,24,4,26),
    ],
}


def get_team_standings(league_id: int, season: int) -> list:
    """Standings via football-data.org avec fallback."""
    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    key = os.getenv("FOOTBALLDATA_KEY", "")

    if competition and key:
        try:
            url  = f"{FOOTBALLDATA_BASE}/competitions/{competition}/standings"
            data = _fd_get(url)

            table = None
            for s in data.get("standings", []):
                if s.get("type") == "TOTAL":
                    table = s.get("table", [])
                    break
            if not table:
                standings = data.get("standings", [])
                if standings:
                    table = standings[0].get("table", [])

            if table:
                teams = []
                for entry in table:
                    name   = entry.get("team", {}).get("name", "")
                    played = entry.get("playedGames", 0)
                    gf     = entry.get("goalsFor", 0)
                    ga     = entry.get("goalsAgainst", 0)
                    if not name or played == 0:
                        continue
                    hg = played // 2
                    ag = played - hg
                    teams.append({
                        "league_id":           league_id,
                        "season":              season,
                        "team_id":             abs(hash(normalize_team_name(name))) % 100000,
                        "team_name":           normalize_team_name(name),
                        "home_goals_scored":   int(gf * 0.55),
                        "home_goals_conceded": int(ga * 0.45),
                        "away_goals_scored":   gf - int(gf * 0.55),
                        "away_goals_conceded": ga - int(ga * 0.45),
                        "home_games":          hg,
                        "away_games":          ag,
                    })
                if teams:
                    print(f"[standings] {len(teams)} equipes via FD league {league_id}")
                    return teams
        except Exception as e:
            print(f"[standings] Erreur FD league {league_id}: {e}")

    return _fallback_stats(league_id, season)


def _fallback_stats(league_id: int, season: int) -> list:
    print(f"[fallback] Stats moyennes — league {league_id}")
    if league_id == 61:
        teams_data = [
            ("Paris Saint Germain",38,8,14,12),("Marseille",30,16,18,20),
            ("AS Monaco",28,14,16,18),("Lyon",26,16,16,20),("Lille",26,12,16,18),
            ("Nice",24,14,18,20),("RC Lens",24,12,16,18),("Rennes",22,16,18,22),
            ("Strasbourg",20,18,16,20),("Montpellier",18,20,14,22),
            ("Nantes",18,20,14,22),("Brest",20,18,16,20),("Reims",16,18,12,20),
            ("Toulouse",18,20,14,22),("Le Havre",14,22,10,24),
            ("Angers",12,24,8,26),("Paris FC",16,20,12,22),
            ("Auxerre",18,20,14,22),("Saint-Etienne",12,24,8,26),
        ]
    elif league_id == 39:
        teams_data = [
            ("Manchester City",40,10,14,12),("Arsenal",36,12,14,14),
            ("Liverpool",38,10,14,12),("Chelsea",30,14,16,16),
            ("Tottenham Hotspur",28,16,16,18),("Manchester United",26,16,14,18),
            ("Newcastle United",28,14,16,16),("Aston Villa",28,14,16,16),
            ("West Ham United",22,18,14,20),("Brighton",24,16,16,18),
            ("Wolverhampton",18,20,12,22),("Fulham",20,18,14,20),
            ("Brentford",20,18,12,20),("Crystal Palace",16,20,10,22),
            ("Everton",14,22,10,24),("Nottingham Forest",18,18,12,20),
            ("Bournemouth",18,18,12,20),("Ipswich Town",14,22,10,24),
            ("Leicester City",16,20,12,22),("Sunderland",14,22,10,24),
        ]
    else:
        teams_data = FALLBACK_STATS.get(league_id, [])
        if not teams_data:
            return []

    return [{
        "league_id":           league_id,
        "season":              season,
        "team_id":             abs(hash(name)) % 100000,
        "team_name":           name,
        "home_goals_scored":   hgf,
        "home_goals_conceded": hga,
        "away_goals_scored":   agf,
        "away_goals_conceded": aga,
        "home_games":          19,
        "away_games":          19,
    } for name, hgf, hga, agf, aga in teams_data]


# ─────────────────────────────────────────────
# FORME RECENTE — football-data.org
# ─────────────────────────────────────────────

_form_cache = {}

def get_recent_form(league_id: int, season: int) -> dict:
    """
    Récupère les 10 derniers matchs de chaque équipe pour calculer :
      - Forme récente pondérée (matchs récents × 2, anciens × 1)
      - Momentum (série victoires/défaites consécutives en cours)
      - Fatigue (jours depuis dernier match)

    Retourne dict : { team_name_norm: { "form_attack", "form_defense",
                                        "momentum", "rest_days" } }
    """
    cache_key = f"form_{league_id}_{season}"
    if cache_key in _form_cache:
        return _form_cache[cache_key]

    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not competition or not key:
        return {}

    # Fetch les 60 derniers jours de matchs
    today     = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=60)).isoformat()
    date_to   = today.isoformat()

    try:
        url    = f"{FOOTBALLDATA_BASE}/competitions/{competition}/matches"
        params = {"dateFrom": date_from, "dateTo": date_to, "status": "FINISHED"}
        data    = _fd_get(url, params)
        matches = data.get("matches", [])
    except Exception as e:
        print(f"[get_recent_form] Erreur FD league {league_id}: {e}")
        return {}

    if not matches:
        return {}

    # Groupe par equipe — dernier match en premier
    from collections import defaultdict
    team_matches = defaultdict(list)

    for m in sorted(matches, key=lambda x: x.get("utcDate", ""), reverse=True):
        if m.get("status") != "FINISHED":
            continue
        ft = m.get("score", {}).get("fullTime", {})
        hg = ft.get("home")
        ag = ft.get("away")
        if hg is None or ag is None:
            continue

        date_str  = m.get("utcDate", "")[:10]
        home_name = normalize_team_name(m.get("homeTeam", {}).get("name", ""))
        away_name = normalize_team_name(m.get("awayTeam", {}).get("name", ""))

        team_matches[home_name].append({
            "date": date_str, "scored": hg, "conceded": ag,
            "result": "W" if hg > ag else ("D" if hg == ag else "L"),
            "is_home": True,
        })
        team_matches[away_name].append({
            "date": date_str, "scored": ag, "conceded": hg,
            "result": "W" if ag > hg else ("D" if ag == hg else "L"),
            "is_home": False,
        })

    today_dt = datetime.now(timezone.utc).date()
    result   = {}

    for team_name, team_m in team_matches.items():
        # Garde les 10 derniers matchs max
        recent = team_m[:10]
        if not recent:
            continue

        # Forme pondérée : matchs récents (idx 0-4) × poids 2, anciens × poids 1
        total_weight    = 0
        weighted_scored = 0.0
        weighted_conceded = 0.0

        for i, m in enumerate(recent):
            weight = 2 if i < 5 else 1
            weighted_scored   += m["scored"]   * weight
            weighted_conceded += m["conceded"] * weight
            total_weight += weight

        avg_scored    = weighted_scored   / total_weight if total_weight else 1.2
        avg_conceded  = weighted_conceded / total_weight if total_weight else 1.0

        # Momentum : série en cours (victoires ou défaites consécutives)
        momentum = 0
        last_result = recent[0]["result"] if recent else None
        for m in recent:
            if m["result"] == last_result and last_result in ("W", "L"):
                momentum += (1 if last_result == "W" else -1)
            else:
                break

        # Fatigue : jours depuis dernier match
        try:
            last_date = datetime.strptime(recent[0]["date"], "%Y-%m-%d").date()
            rest_days = (today_dt - last_date).days
        except Exception:
            rest_days = 7  # valeur neutre

        result[team_name] = {
            "avg_scored":   round(avg_scored, 3),
            "avg_conceded": round(avg_conceded, 3),
            "momentum":     momentum,      # positif = serie victoires, négatif = serie défaites
            "rest_days":    rest_days,     # jours de repos depuis dernier match
            "games_played": len(recent),
        }

    print(f"[get_recent_form] {len(result)} equipes — forme récente league {league_id}")
    _form_cache[cache_key] = result
    return result


def clear_form_cache():
    global _form_cache
    _form_cache = {}


# ─────────────────────────────────────────────
# H2H — football-data.org (batch par ligue)
# ─────────────────────────────────────────────

# Cache global : tous les matchs de la saison par ligue
# Structure : { league_id: [ {home_id, away_id, home_name, away_name, date, hg, ag}, ... ] }
_season_matches_cache = {}  # cache mémoire intra-run


def _parse_fd_matches(raw_matches: list, competition: str, season: int) -> list:
    """Parse les matchs bruts football-data.org en format interne."""
    result = []
    for m in raw_matches:
        ft = m.get("score", {}).get("fullTime", {})
        hg = ft.get("home")
        ag = ft.get("away")
        if hg is None or ag is None:
            continue
        home_raw = m.get("homeTeam", {}).get("name", "")
        away_raw = m.get("awayTeam", {}).get("name", "")
        result.append({
            "date":       m.get("utcDate", "")[:10],
            "home_id":    m.get("homeTeam", {}).get("id"),
            "away_id":    m.get("awayTeam", {}).get("id"),
            "home_name":  home_raw,
            "away_name":  away_raw,
            "home_norm":  normalize_team_name(home_raw),
            "away_norm":  normalize_team_name(away_raw),
            "home_goals": hg,
            "away_goals": ag,
        })
    return result


def prefetch_season_matches(league_id: int, seasons: list) -> list:
    """
    Retourne les matchs terminés d'une ligue sur plusieurs saisons.

    Stratégie cache à 3 niveaux :
      1. Cache mémoire intra-run (dict Python) → 0 ms
      2. Cache DB persistant (table h2h_cache, TTL 7 jours) → 0 appel API
      3. Appel football-data.org → stocké en DB pour les prochains runs

    Plan gratuit FD = 2 saisons récentes max. Les 403 sont ignorés proprement.
    """
    from database import get_h2h_cache, set_h2h_cache

    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    cache_key   = f"season_{league_id}_{'_'.join(str(s) for s in seasons)}"

    # Niveau 1 : cache mémoire intra-run
    if cache_key in _season_matches_cache:
        return _season_matches_cache[cache_key]

    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not competition or not key:
        return []

    all_matches = []
    for season in seasons:
        # Niveau 2 : cache DB
        cached = get_h2h_cache(league_id, season)
        if cached is not None:
            print(f"[prefetch] {competition} saison {season}: {len(cached)} matchs depuis cache DB")
            all_matches.extend(cached)
            continue

        # Niveau 3 : appel API football-data.org
        try:
            url    = f"{FOOTBALLDATA_BASE}/competitions/{competition}/matches"
            params = {"season": season, "status": "FINISHED"}
            data   = _fd_get(url, params)
            parsed = _parse_fd_matches(data.get("matches", []), competition, season)
            set_h2h_cache(league_id, season, parsed)  # stocke en DB
            all_matches.extend(parsed)
            print(f"[prefetch] {competition} saison {season}: {len(parsed)} matchs fetchés + mis en cache DB")
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", 0) if e.response is not None else 0
            if status == 403:
                print(f"[prefetch] {competition} saison {season}: 403 archive — ignoré (plan gratuit = 2 saisons recentes)")
            else:
                print(f"[prefetch] Erreur {competition} saison {season}: {e}")
        except Exception as e:
            print(f"[prefetch] Erreur {competition} saison {season}: {e}")

    _season_matches_cache[cache_key] = all_matches
    return all_matches


def get_h2h(league_id: int, home_team: str, away_team: str,
            match_date: str = None, seasons: list = None) -> Optional[dict]:
    """
    Calcule le H2H entre deux équipes depuis les données pré-fetchées.
    Filtre uniquement les matchs AVANT match_date pour éviter les matchs retour.

    Retourne dict avec win_rate_home, win_rate_away, total.
    Retourne None si moins de 5 matchs historiques.
    """
    h_norm = normalize_team_name(home_team)
    a_norm = normalize_team_name(away_team)

    # Seasons par défaut : 3 saisons pour max d'historique
    if seasons is None:
        current_season = int(os.getenv("SEASON", 2025))
        seasons = [current_season, current_season - 1]  # plan gratuit FD : 2 saisons recentes max

    # Pré-fetch si pas encore en cache
    all_matches = prefetch_season_matches(league_id, seasons)
    if not all_matches:
        return None

    # Filtre les matchs entre ces deux équipes avant la date du match
    h2h_matches = []
    for m in all_matches:
        fd_home = m.get("home_norm") or normalize_team_name(m["home_name"])
        fd_away = m.get("away_norm") or normalize_team_name(m["away_name"])

        is_normal  = (fd_home == h_norm or h_norm in fd_home or fd_home in h_norm) and                      (fd_away == a_norm or a_norm in fd_away or fd_away in a_norm)
        is_reverse = (fd_home == a_norm or a_norm in fd_home or fd_home in a_norm) and                      (fd_away == h_norm or h_norm in fd_away or fd_away in h_norm)

        if not (is_normal or is_reverse):
            continue

        # Filtre par date — uniquement avant le match prédit
        if match_date and m["date"] >= match_date:
            continue

        h2h_matches.append({**m, "reversed": is_reverse})

    if len(h2h_matches) < 5:
        return None

    # Calcule les stats du point de vue de l'équipe domicile du match actuel
    home_wins = away_wins = draws = 0
    for m in h2h_matches:
        hg = m["home_goals"]
        ag = m["away_goals"]
        if m["reversed"]:
            # Dans ce match H2H, notre équipe "home" jouait en extérieur
            hg, ag = ag, hg

        if hg > ag:
            home_wins += 1
        elif ag > hg:
            away_wins += 1
        else:
            draws += 1

    total = home_wins + away_wins + draws
    if total == 0:
        return None

    result = {
        "total":         total,
        "home_wins":     home_wins,
        "away_wins":     away_wins,
        "draws":         draws,
        "win_rate_home": round(home_wins / total, 3),
        "win_rate_away": round(away_wins / total, 3),
    }
    print(
        f"[H2H] {home_team} vs {away_team} | "
        f"{home_wins}W-{draws}D-{away_wins}L sur {total} matchs | "
        f"home={result['win_rate_home']:.0%} away={result['win_rate_away']:.0%}"
    )
    return result


def clear_h2h_cache():
    global _season_matches_cache
    _season_matches_cache = {}



def get_odds_api_usage() -> dict:
    """Retourne le nombre de requêtes utilisées/restantes sur The Odds API."""
    key = os.getenv("ODDS_API_KEY", "")
    if not key:
        return {"used": 0, "remaining": 0, "error": "Clé manquante"}
    try:
        # Appel léger sur sports pour récupérer les headers X-Requests-*
        url  = f"{ODDS_API_BASE}/sports"
        resp = requests.get(url, params={"apiKey": key}, timeout=10)
        used      = int(resp.headers.get("x-requests-used", 0))
        remaining = int(resp.headers.get("x-requests-remaining", 0))
        return {"used": used, "remaining": remaining, "total": used + remaining}
    except Exception as e:
        return {"used": 0, "remaining": 0, "error": str(e)}

# ─────────────────────────────────────────────
# RESULTATS — football-data.org
# ─────────────────────────────────────────────

def get_fixtures_results_batch(league_id: int, season: int, date: str) -> dict:
    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    if not competition:
        return {}
    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not key:
        return {}

    try:
        url    = f"{FOOTBALLDATA_BASE}/competitions/{competition}/matches"
        params = {"dateFrom": date, "dateTo": date}
        data = _fd_get(url, params)
    except Exception as e:
        print(f"[results_batch] Erreur: {e}")
        return {}

    results = {}
    for m in data.get("matches", []):
        if m.get("status") != "FINISHED":
            continue
        home = m.get("homeTeam", {}).get("name", "")
        away = m.get("awayTeam", {}).get("name", "")
        ft   = m.get("score", {}).get("fullTime", {})
        hg   = ft.get("home")
        ag   = ft.get("away")
        if hg is None or ag is None:
            continue
        r = {"home_goals": hg, "away_goals": ag, "total_goals": hg + ag,
             "status": "FINISHED", "score": f"{hg}-{ag}"}
        results[(home.lower(), away.lower())] = r
        results[(normalize_team_name(home), normalize_team_name(away))] = r

    return results


def get_all_results_today(date: str) -> dict:
    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not key:
        return {}
    try:
        url    = f"{FOOTBALLDATA_BASE}/matches"
        params = {"dateFrom": date, "dateTo": date}
        data = _fd_get(url, params)
    except Exception as e:
        print(f"[all_results] Erreur: {e}")
        return {}

    results = {}
    for m in data.get("matches", []):
        if m.get("status") != "FINISHED":
            continue
        comp      = m.get("competition", {}).get("name", "")
        league_id = FD_COMPETITION_TO_LEAGUE.get(comp)
        if league_id is None:
            continue
        home = m.get("homeTeam", {}).get("name", "")
        away = m.get("awayTeam", {}).get("name", "")
        ft   = m.get("score", {}).get("fullTime", {})
        hg   = ft.get("home")
        ag   = ft.get("away")
        if hg is None or ag is None:
            continue
        r = {"home_goals": hg, "away_goals": ag, "total_goals": hg + ag,
             "status": "FINISHED", "score": f"{hg}-{ag}", "league_id": league_id}
        results[(home.lower(), away.lower())] = r
        results[(normalize_team_name(home), normalize_team_name(away))] = r

    print(f"[all_results] {len(results)//2} résultats pour le {date}")
    return results