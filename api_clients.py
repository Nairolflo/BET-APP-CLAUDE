"""
api_clients.py - API wrappers

- get_fixtures()          → The Odds API
- get_team_standings()    → football-data.org standings réels + fallback
- get_odds()              → The Odds API h2h + totals
- get_all_results_today() → football-data.org résultats du jour
- get_fixtures_results_batch() → résultats par ligue + date
- normalize_team_name()   → normalisation noms équipes
"""

import os
import requests
from datetime import datetime, timezone
from typing import Optional


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

ODDS_API_BASE      = "https://api.the-odds-api.com/v4"
FOOTBALLDATA_BASE  = "https://api.football-data.org/v4"

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

# league_id → code football-data.org pour standings + résultats
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

# Mapping nom competition football-data.org → league_id
FD_COMPETITION_TO_LEAGUE = {
    "Premier League":          39,
    "Ligue 1":                 61,
    "Bundesliga":              78,
    "Serie A":                 135,
    "Primera Division":        140,
    "Eredivisie":              88,
    "Primeira Liga":           94,
    "Championship":            40,
    "UEFA Champions League":   2,
    "UEFA Europa League":      3,
    "Belgian First Division A": 144,
    "Super Lig":               203,
    "Scottish Premiership":    179,
    "Série A":                 71,
    "Liga MX":                 262,
}

PREFERRED_BOOKMAKERS = ["Betclic (FR)", "Winamax (FR)"]

# Mapping noms football-data.org → noms The Odds API
TEAM_NAME_MAP = {
    # Ligue 1
    "fc nantes": "nantes",
    "angers sco": "angers",
    "aj auxerre": "auxerre",
    "rc strasbourg alsace": "strasbourg",
    "toulouse fc": "toulouse",
    "olympique de marseille": "marseille",
    "olympique lyonnais": "lyon",
    "paris saint-germain fc": "paris saint germain",
    "stade rennais fc": "rennes",
    "losc lille": "lille",
    "ogc nice": "nice",
    "as monaco fc": "as monaco",
    "rc lens": "rc lens",
    "stade brestois 29": "brest",
    "stade de reims": "reims",
    "le havre ac": "le havre",
    "montpellier hsc": "montpellier",
    "as saint-etienne": "saint-etienne",
    # Premier League
    "manchester city fc": "manchester city",
    "arsenal fc": "arsenal",
    "liverpool fc": "liverpool",
    "chelsea fc": "chelsea",
    "tottenham hotspur fc": "tottenham hotspur",
    "manchester united fc": "manchester united",
    "newcastle united fc": "newcastle united",
    "aston villa fc": "aston villa",
    "west ham united fc": "west ham united",
    "brighton & hove albion fc": "brighton",
    "wolverhampton wanderers fc": "wolverhampton wanderers",
    "fulham fc": "fulham",
    "brentford fc": "brentford",
    "crystal palace fc": "crystal palace",
    "everton fc": "everton",
    "nottingham forest fc": "nottingham forest",
    "bournemouth afc": "bournemouth",
    "ipswich town fc": "ipswich town",
    "leicester city fc": "leicester city",
    "sunderland afc": "sunderland",
    # Bundesliga
    "fc bayern münchen": "bayern munich",
    "fc bayern munchen": "bayern munich",
    "borussia dortmund": "borussia dortmund",
    "bayer 04 leverkusen": "bayer leverkusen",
    "rb leipzig": "rb leipzig",
    "eintracht frankfurt": "eintracht frankfurt",
    "vfb stuttgart": "vfb stuttgart",
    "sc freiburg": "sc freiburg",
    "vfl wolfsburg": "wolfsburg",
    "1. fsv mainz 05": "mainz",
    "tsg 1899 hoffenheim": "hoffenheim",
    "1. fc köln": "koln",
    "fc augsburg": "augsburg",
    "sv werder bremen": "werder bremen",
    "1. fc union berlin": "union berlin",
    "borussia mönchengladbach": "borussia monchengladbach",
    "hamburger sv": "hamburger sv",
    "1. fc heidenheim 1846": "heidenheim",
    # Serie A
    "fc internazionale milano": "inter milan",
    "ac milan": "ac milan",
    "juventus fc": "juventus",
    "ssc napoli": "napoli",
    "as roma": "as roma",
    "ss lazio": "lazio",
    "atalanta bc": "atalanta",
    "acf fiorentina": "fiorentina",
    "bologna fc 1909": "bologna",
    "torino fc": "torino",
    "udinese calcio": "udinese",
    "cagliari calcio": "cagliari",
    "como 1907": "como",
    "ac pisa 1909": "pisa",
    # La Liga
    "real madrid cf": "real madrid",
    "fc barcelona": "barcelona",
    "club atletico de madrid": "atletico madrid",
    "athletic club": "athletic bilbao",
    "real sociedad de futbol": "real sociedad",
    "villarreal cf": "villarreal",
    "sevilla fc": "sevilla",
    "real betis balompie": "real betis",
    "ca osasuna": "osasuna",
    "rcd mallorca": "mallorca",
    "levante ud": "levante",
    "girona fc": "girona",
    # Eredivisie
    "psv eindhoven": "psv",
    "afc ajax": "ajax",
    "feyenoord": "feyenoord",
    "az alkmaar": "az",
    "fc groningen": "groningen",
    "sbv excelsior": "excelsior",
    "sc heerenveen": "heerenveen",
    # Primeira Liga
    "sporting clube de portugal": "sporting cp",
    "fc porto": "porto",
    "sl benfica": "benfica",
    "sporting clube de braga": "braga",
    "gd estoril praia": "estoril",
    "fc alverca": "alverca",
    "avs": "avs",
    "moreirense fc": "moreirense",
    "cd nacional": "nacional",
    "casa pia ac": "casa pia",
}


# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

def normalize_team_name(name: str) -> str:
    """Normalise un nom d'equipe pour le matching."""
    n = name.lower().strip()
    if n in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[n]
    for suffix in [" fc", " afc", " sc", " cf", " ac", " rc", " as", " us", " ssc", " ss"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def _footballdata_headers() -> dict:
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
        "regions":    "eu",
        "markets":    "h2h",
        "oddsFormat": "decimal",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"[get_fixtures] Erreur Odds API league {league_id}: {e}")
        return []

    now    = datetime.now(timezone.utc)
    cutoff = now.timestamp() + days_ahead * 86400

    fixtures = []
    for i, event in enumerate(events):
        commence_str = event.get("commence_time", "")
        try:
            commence_dt = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
            commence_ts = commence_dt.timestamp()
        except Exception:
            continue

        # Inclut matchs jusqu'a 3h passes (en cours) et jusqu'a cutoff
        if commence_ts < now.timestamp() - 10800 or commence_ts > cutoff:
            continue

        home = event.get("home_team", "")
        away = event.get("away_team", "")

        fixtures.append({
            "fixture_id":     event.get("id", f"odds_{i}"),
            "date":           commence_str[:10],
            "home_team_id":   hash(home) % 100000,
            "home_team_name": home,
            "away_team_id":   hash(away) % 100000,
            "away_team_name": away,
            "league_id":      league_id,
        })

    return fixtures


# ─────────────────────────────────────────────
# TEAM STANDINGS — football-data.org + fallback
# ─────────────────────────────────────────────

# Stats de fallback par league_id
FALLBACK_STATS = {
    78: [  # Bundesliga
        ("Bayern Munich",30,8,12,10),("Bayer Leverkusen",26,12,14,14),("Borussia Dortmund",24,14,14,16),
        ("RB Leipzig",24,12,14,14),("Eintracht Frankfurt",22,14,14,16),("VfB Stuttgart",22,14,12,16),
        ("SC Freiburg",18,16,12,18),("Union Berlin",16,18,12,20),("Werder Bremen",18,18,12,18),
        ("Borussia Monchengladbach",18,18,10,20),("Wolfsburg",16,18,10,20),("Mainz",16,18,10,20),
        ("Augsburg",14,20,10,22),("Hoffenheim",14,20,10,22),("Heidenheim",12,22,8,24),
        ("Hamburger SV",12,22,8,24),("Koln",10,24,6,26),("Bochum",10,24,6,26),
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
        ("Real Madrid",36,8,14,10),("Barcelona",34,10,14,12),("Atletico Madrid",26,12,14,14),
        ("Athletic Bilbao",24,12,14,14),("Real Sociedad",22,14,14,16),("Villarreal",22,14,12,16),
        ("Sevilla",20,16,12,18),("Real Betis",18,16,12,18),("Osasuna",16,18,10,20),
        ("Mallorca",14,20,10,22),("Rayo Vallecano",14,18,10,20),("Celta Vigo",14,20,10,22),
        ("Getafe",12,20,8,22),("Girona",16,16,12,18),("Levante",12,22,8,24),
        ("Alaves",10,22,6,24),("Las Palmas",10,22,6,24),("Valladolid",8,24,4,26),
        ("Leganes",8,24,4,26),("Espanyol",10,22,6,24),
    ],
    88: [  # Eredivisie
        ("PSV",32,8,14,10),("Ajax",28,12,14,12),("Feyenoord",26,12,14,14),
        ("AZ",24,12,12,14),("Twente",22,14,12,16),("Utrecht",20,16,12,18),
        ("Heerenveen",16,18,10,20),("Heracles",14,18,10,20),("NEC Nijmegen",14,20,10,22),
        ("Sparta Rotterdam",12,22,8,24),("Excelsior",10,22,6,24),("Groningen",10,22,6,24),
        ("Go Ahead Eagles",12,20,8,22),("Fortuna Sittard",10,24,6,26),
        ("PEC Zwolle",8,24,4,26),("Almere City",10,24,6,26),
    ],
    94: [  # Primeira Liga
        ("Sporting CP",30,8,14,10),("Porto",28,10,14,12),("Benfica",28,10,12,12),
        ("Braga",22,14,12,16),("Vitoria Guimaraes",18,16,10,18),("Estoril",14,18,10,20),
        ("Famalicao",12,20,8,22),("Boavista",12,20,8,22),("Casa Pia",12,22,8,24),
        ("Gil Vicente",10,22,6,24),("Moreirense",10,22,6,24),("Arouca",10,22,6,24),
        ("Rio Ave",10,24,6,26),("Nacional",10,22,6,24),("Alverca",8,24,4,26),
        ("AVS",8,24,4,26),("Estrela Amadora",8,26,4,28),("Chaves",8,26,4,28),
    ],
    144: [  # Belgium
        ("Club Brugge",28,10,12,12),("Anderlecht",24,12,12,14),("Genk",22,14,12,16),
        ("Gent",22,14,10,16),("Standard Liege",18,16,10,18),("Antwerp",18,16,10,18),
        ("Union SG",20,14,12,16),("OH Leuven",14,18,10,20),("Cercle Brugge",14,18,8,20),
        ("Westerlo",12,20,8,22),("Mechelen",12,20,8,22),("Charleroi",12,20,8,22),
        ("Eupen",8,24,4,26),("RWDM",10,22,6,24),("Beerschot",8,26,4,28),
        ("Dender",8,26,4,28),
    ],
    203: [  # Turkey
        ("Galatasaray",30,10,14,12),("Fenerbahce",28,10,12,12),("Besiktas",24,12,12,14),
        ("Trabzonspor",20,14,12,16),("Basaksehir",18,16,10,18),("Sivasspor",14,18,10,20),
        ("Antalyaspor",14,18,8,20),("Konyaspor",12,20,8,22),("Kayserispor",10,22,6,24),
        ("Kasimpasa",10,22,6,24),("Alanyaspor",12,20,8,22),("Rizespor",8,24,4,26),
        ("Gaziantep",8,24,4,26),("Samsunspor",10,22,6,24),("Ankaraguco",10,22,6,24),
        ("Eyupspor",10,22,6,24),("Bodrum",8,24,4,26),("Hatayspor",8,24,4,26),
    ],
    179: [  # Scottish Prem
        ("Celtic",32,6,14,8),("Rangers",28,10,12,12),("Hearts",20,14,12,16),
        ("Aberdeen",18,16,10,18),("Hibernian",18,16,10,18),("Motherwell",14,18,8,20),
        ("Livingston",12,20,8,22),("St Mirren",12,20,6,22),("Kilmarnock",10,22,6,24),
        ("Ross County",8,24,4,26),("St Johnstone",8,24,4,26),("Dundee",8,26,4,28),
    ],
    262: [  # Liga MX
        ("Club America",26,12,12,14),("Chivas",22,14,12,16),("Cruz Azul",20,14,10,16),
        ("Tigres",24,12,12,14),("Monterrey",22,12,12,14),("Pumas",18,16,10,18),
        ("Toluca",18,16,10,18),("Leon",16,18,10,20),("Atlas",14,18,8,20),
        ("Pachuca",16,16,10,18),("Santos",14,20,8,22),("Queretaro",12,20,6,22),
        ("Mazatlan",10,22,6,24),("Necaxa",10,22,6,24),("Tijuana",10,22,6,24),
        ("Puebla",12,20,8,22),("Juarez",8,24,4,26),("San Luis",8,24,4,26),
    ],
    2: [  # Champions League
        ("Real Madrid",36,6,14,8),("Manchester City",34,8,14,10),("Bayern Munich",30,8,12,10),
        ("Paris Saint Germain",28,10,12,12),("Liverpool",28,10,12,10),("Arsenal",26,10,12,12),
        ("Inter Milan",24,10,12,12),("Atletico Madrid",22,12,12,14),("Barcelona",26,10,12,12),
        ("Borussia Dortmund",20,12,12,14),("Napoli",20,12,10,14),("Porto",18,14,10,16),
        ("Benfica",18,14,10,16),("RB Leipzig",16,14,10,16),("PSV",16,14,8,16),
        ("Bayer Leverkusen",22,12,10,14),("Aston Villa",16,14,10,16),("AC Milan",20,12,10,14),
        ("Juventus",18,12,10,14),("Feyenoord",14,16,8,18),("Sporting CP",18,12,8,14),
        ("Celtic",14,18,8,20),("Club Brugge",12,18,6,20),("Galatasaray",14,16,8,18),
    ],
    3: [  # Europa League
        ("Roma",22,14,10,16),("Sevilla",20,14,10,16),("Ajax",20,14,10,16),
        ("Real Sociedad",18,14,10,16),("Manchester United",20,16,10,18),
        ("Lyon",16,16,8,18),("Fiorentina",16,16,8,18),("Fenerbahce",16,14,8,16),
        ("Olympiakos",14,16,8,18),("Rangers",16,16,8,18),("Marseille",18,16,10,18),
        ("Villarreal",18,14,10,16),("Lazio",18,14,10,16),("Eintracht Frankfurt",18,14,10,16),
        ("Athletic Bilbao",16,14,8,16),("Braga",14,16,8,18),("PAOK",12,16,6,18),
        ("Anderlecht",14,16,8,18),
    ],
    71: [  # Brasileirao
        ("Flamengo",28,12,12,14),("Palmeiras",26,10,12,12),("Atletico Mineiro",24,12,12,14),
        ("Fluminense",20,14,10,16),("Gremio",18,14,10,16),("Internacional",18,14,10,16),
        ("Corinthians",18,16,10,18),("Sao Paulo",18,14,10,16),("Botafogo",16,14,8,16),
        ("Santos",14,16,8,18),("Atletico GO",12,18,8,20),("Bragantino",14,16,8,18),
        ("Fortaleza",14,16,8,18),("Bahia",12,18,6,20),("Vasco",12,18,6,20),
        ("Cruzeiro",14,16,8,18),("Juventude",10,20,6,22),("Vitoria",8,22,4,24),
        ("Criciuma",8,22,4,24),("Cuiaba",10,20,6,22),
    ],
    40: [  # Championship
        ("Sunderland",28,10,12,12),("Leeds United",26,10,12,12),("Burnley",24,12,12,14),
        ("Sheffield United",22,12,10,14),("West Brom",20,14,10,16),("Norwich City",18,14,10,16),
        ("Middlesbrough",18,14,8,16),("Millwall",16,14,8,16),("Swansea City",14,16,8,18),
        ("Hull City",12,16,8,18),("Coventry City",12,18,6,20),("Stoke City",12,18,6,20),
        ("Bristol City",10,18,6,20),("Preston",10,18,6,20),("Watford",12,16,8,18),
        ("Blackburn",10,20,6,22),("Cardiff City",10,20,6,22),("QPR",8,22,4,24),
        ("Derby County",10,20,6,22),("Plymouth Argyle",8,22,4,24),
        ("Portsmouth",8,22,4,24),("Sheffield Wednesday",8,24,4,26),
        ("Ipswich Town",14,18,8,18),("Leicester City",16,16,10,16),
    ],
}


def get_team_standings(league_id: int, season: int) -> list:
    """Standings via football-data.org. Fallback sur stats moyennes."""
    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    key = os.getenv("FOOTBALLDATA_KEY", "")

    if competition and key:
        try:
            url = f"{FOOTBALLDATA_BASE}/competitions/{competition}/standings"
            resp = requests.get(url, headers={"X-Auth-Token": key}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            standings = data.get("standings", [])

            table = None
            for s in standings:
                if s.get("type") == "TOTAL":
                    table = s.get("table", [])
                    break
            if not table and standings:
                table = standings[0].get("table", [])

            if table:
                teams = []
                for entry in table:
                    team_name     = entry.get("team", {}).get("name", "")
                    if not team_name:
                        continue
                    games_played  = entry.get("playedGames", 0)
                    goals_for     = entry.get("goalsFor", 0)
                    goals_against = entry.get("goalsAgainst", 0)
                    if games_played == 0:
                        continue
                    home_games = games_played // 2
                    away_games = games_played - home_games
                    home_gf    = int(goals_for * 0.55)
                    away_gf    = goals_for - home_gf
                    home_ga    = int(goals_against * 0.45)
                    away_ga    = goals_against - home_ga
                    norm_name  = normalize_team_name(team_name)
                    teams.append({
                        "league_id":           league_id,
                        "season":              season,
                        "team_id":             abs(hash(norm_name)) % 100000,
                        "team_name":           norm_name,
                        "home_goals_scored":   home_gf,
                        "home_goals_conceded": home_ga,
                        "away_goals_scored":   away_gf,
                        "away_goals_conceded": away_ga,
                        "home_games":          home_games,
                        "away_games":          away_games,
                    })
                if teams:
                    print(f"[get_team_standings] {len(teams)} equipes via football-data.org league {league_id}")
                    return teams
        except Exception as e:
            print(f"[get_team_standings] Erreur football-data.org league {league_id}: {e}")

    return _fallback_stats(league_id, season)


def _fallback_stats(league_id: int, season: int) -> list:
    print(f"[fallback_stats] Stats moyennes — league {league_id}")

    if league_id == 61:
        teams_data = [
            ("Paris Saint Germain",38,8,14,12),("Marseille",30,16,18,20),
            ("AS Monaco",28,14,16,18),("Lyon",26,16,16,20),("Lille",26,12,16,18),
            ("Nice",24,14,18,20),("RC Lens",24,12,16,18),("Rennes",22,16,18,22),
            ("Strasbourg",20,18,16,20),("Montpellier",18,20,14,22),("Nantes",18,20,14,22),
            ("Brest",20,18,16,20),("Reims",16,18,12,20),("Toulouse",18,20,14,22),
            ("Le Havre",14,22,10,24),("Angers",12,24,8,26),("Paris FC",16,20,12,22),
            ("Auxerre",18,20,14,22),("Saint-Etienne",12,24,8,26),("Havre AC",14,22,10,24),
        ]
    elif league_id == 39:
        teams_data = [
            ("Manchester City",40,10,14,12),("Arsenal",36,12,14,14),("Liverpool",38,10,14,12),
            ("Chelsea",30,14,16,16),("Tottenham Hotspur",28,16,16,18),("Manchester United",26,16,14,18),
            ("Newcastle United",28,14,16,16),("Aston Villa",28,14,16,16),("West Ham United",22,18,14,20),
            ("Brighton",24,16,16,18),("Wolverhampton",18,20,12,22),("Fulham",20,18,14,20),
            ("Brentford",20,18,12,20),("Crystal Palace",16,20,10,22),("Everton",14,22,10,24),
            ("Nottingham Forest",18,18,12,20),("Bournemouth",18,18,12,20),("Ipswich Town",14,22,10,24),
            ("Leicester City",16,20,12,22),("Sunderland",14,22,10,24),
        ]
    else:
        teams_data = FALLBACK_STATS.get(league_id, [])
        if not teams_data:
            print(f"[fallback_stats] Aucune donnee pour league {league_id}")
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
# ODDS — The Odds API
# ─────────────────────────────────────────────

def get_odds(league_id: int) -> list:
    """Cotes h2h + totals depuis The Odds API."""
    sport_key = LEAGUE_SPORT_MAP.get(league_id)
    if not sport_key:
        return []

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey":     os.getenv("ODDS_API_KEY", ""),
        "regions":    "eu",
        "markets":    "h2h,totals",
        "oddsFormat": "decimal",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"[get_odds] Erreur league {league_id}: {e}")
        return []

    results = []
    for event in events:
        home     = event.get("home_team")
        away     = event.get("away_team")
        commence = event.get("commence_time", "")[:10]

        all_bookmakers  = {}
        pref_bookmakers = {}

        for bk in event.get("bookmakers", []):
            bk_name = bk.get("title", bk.get("key", "Unknown"))
            entry   = {}

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
                        key_suffix = str(float(point)).replace(".", "_")
                        if name.lower() == "over":
                            entry[f"over_{key_suffix}"]  = price
                        elif name.lower() == "under":
                            entry[f"under_{key_suffix}"] = price

            if entry:
                all_bookmakers[bk_name] = entry
                if bk_name in PREFERRED_BOOKMAKERS:
                    pref_bookmakers[bk_name] = entry

        final_odds = pref_bookmakers if pref_bookmakers else all_bookmakers
        if not final_odds:
            continue

        results.append({
            "date":      commence,
            "home_team": home,
            "away_team": away,
            "odds":      final_odds,
            "event_id":  event.get("id"),
            "league_id": league_id,
        })

    return results


# ─────────────────────────────────────────────
# RESULTATS — football-data.org
# ─────────────────────────────────────────────

def get_fixtures_results_batch(league_id: int, season: int, date: str) -> dict:
    """Resultats d'une journee pour une ligue via football-data.org."""
    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    if not competition:
        return {}

    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not key:
        print("[get_fixtures_results_batch] FOOTBALLDATA_KEY manquante")
        return {}

    url    = f"{FOOTBALLDATA_BASE}/competitions/{competition}/matches"
    params = {"dateFrom": date, "dateTo": date}

    try:
        resp = requests.get(url, headers=_footballdata_headers(), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[get_fixtures_results_batch] Erreur: {e}")
        return {}

    results = {}
    for match in data.get("matches", []):
        if match.get("status") != "FINISHED":
            continue
        home = match.get("homeTeam", {}).get("name", "")
        away = match.get("awayTeam", {}).get("name", "")
        ft   = match.get("score", {}).get("fullTime", {})
        hg   = ft.get("home")
        ag   = ft.get("away")
        if hg is None or ag is None:
            continue
        result = {
            "home_goals": hg, "away_goals": ag, "total_goals": hg + ag,
            "status": "FINISHED", "score": f"{hg}-{ag}",
            "home_name": home, "away_name": away,
        }
        results[(home.lower(), away.lower())] = result
        results[(normalize_team_name(home), normalize_team_name(away))] = result

    print(f"[get_fixtures_results_batch] {len(results)//2} resultats pour {competition} le {date}")
    return results


def get_all_results_today(date: str) -> dict:
    """
    Tous les resultats du jour toutes ligues via football-data.org.
    Filtre uniquement les competitions connues dans FD_COMPETITION_TO_LEAGUE.
    """
    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not key:
        return {}

    url    = f"{FOOTBALLDATA_BASE}/matches"
    params = {"dateFrom": date, "dateTo": date}

    try:
        resp = requests.get(url, headers=_footballdata_headers(), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[get_all_results_today] Erreur: {e}")
        return {}

    results = {}
    skipped = 0
    for match in data.get("matches", []):
        if match.get("status") != "FINISHED":
            continue
        comp_name = match.get("competition", {}).get("name", "")
        league_id = FD_COMPETITION_TO_LEAGUE.get(comp_name)
        if league_id is None:
            skipped += 1
            continue
        home = match.get("homeTeam", {}).get("name", "")
        away = match.get("awayTeam", {}).get("name", "")
        ft   = match.get("score", {}).get("fullTime", {})
        hg   = ft.get("home")
        ag   = ft.get("away")
        if hg is None or ag is None:
            continue
        result = {
            "home_goals": hg, "away_goals": ag, "total_goals": hg + ag,
            "status": "FINISHED", "score": f"{hg}-{ag}",
            "home_name": home, "away_name": away, "league_id": league_id,
        }
        results[(home.lower(), away.lower())] = result
        results[(normalize_team_name(home), normalize_team_name(away))] = result

    print(f"[get_all_results_today] {len(results)//2} resultats valides, {skipped} ignores pour le {date}")
    return results


def get_fixture_result(fixture_id) -> Optional[dict]:
    """Non utilise — on passe par get_fixtures_results_batch."""
    return None