"""
api_clients.py - API wrappers

- get_fixtures()       → The Odds API (fixtures extraits des cotes)
- get_team_standings() → FBref scraping + fallback stats
- get_odds()           → The Odds API — Betclic/Winamax FR en priorité,
                         fallback tous bookmakers EU
                         Marchés : h2h + totals (tous seuils)
"""

import os
import requests
from datetime import datetime, timezone
from typing import Optional
from bs4 import BeautifulSoup, Comment


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

LEAGUE_SPORT_MAP = {
    61: "soccer_france_ligue_one",
    39: "soccer_epl",
}

FBREF_LEAGUE_MAP = {
    61: "https://fbref.com/en/comps/13/Ligue-1-Stats",
    39: "https://fbref.com/en/comps/9/Premier-League-Stats",
}

FBREF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

PREFERRED_BOOKMAKERS = ["Betclic (FR)", "Winamax (FR)"]


# ─────────────────────────────────────────────
# FIXTURES — depuis The Odds API
# ─────────────────────────────────────────────

def get_fixtures(league_id: int, season: int, days_ahead: int = 3) -> list:
    sport_key = LEAGUE_SPORT_MAP.get(league_id)
    if not sport_key:
        return []

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey":      os.getenv("ODDS_API_KEY", ""),
        "regions":     "eu",
        "markets":     "h2h",
        "oddsFormat":  "decimal",
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

        if commence_ts < now.timestamp() or commence_ts > cutoff:
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
# TEAM STATS — FBref + fallback
# ─────────────────────────────────────────────

def get_team_standings(league_id: int, season: int) -> list:
    url = FBREF_LEAGUE_MAP.get(league_id)
    if not url:
        return _fallback_stats(league_id, season)

    try:
        resp = requests.get(url, headers=FBREF_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"[get_team_standings] Erreur FBref league {league_id}: {e}")
        return _fallback_stats(league_id, season)

    soup = BeautifulSoup(resp.text, "lxml")

    table = None
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if "stats_squads_standard_for" in comment:
            inner = BeautifulSoup(comment, "lxml")
            table = inner.find("table", {"id": "stats_squads_standard_for"})
            if table:
                break

    if not table:
        table = soup.find("table", {"id": "stats_squads_standard_for"})

    if not table:
        print(f"[get_team_standings] Table FBref introuvable — fallback league {league_id}")
        return _fallback_stats(league_id, season)

    teams = []
    tbody = table.find("tbody")
    if not tbody:
        return _fallback_stats(league_id, season)

    for row in tbody.find_all("tr"):
        if "thead" in row.get("class", []):
            continue

        team_cell = row.find("td", {"data-stat": "team"})
        if not team_cell:
            continue

        team_name = team_cell.get_text(strip=True)
        if not team_name:
            continue

        def safe_int(stat_name):
            cell = row.find("td", {"data-stat": stat_name})
            if not cell:
                return 0
            try:
                return int(float(cell.get_text(strip=True) or 0))
            except Exception:
                return 0

        games         = safe_int("games")
        goals_for     = safe_int("goals")
        goals_against = safe_int("goals_against")

        if games == 0:
            continue

        home_games = games // 2
        away_games = games - home_games
        home_gf    = int(goals_for * 0.55)
        away_gf    = goals_for - home_gf
        home_ga    = int(goals_against * 0.45)
        away_ga    = goals_against - home_ga

        teams.append({
            "league_id":           league_id,
            "season":              season,
            "team_id":             abs(hash(team_name)) % 100000,
            "team_name":           team_name,
            "home_goals_scored":   home_gf,
            "home_goals_conceded": home_ga,
            "away_goals_scored":   away_gf,
            "away_goals_conceded": away_ga,
            "home_games":          home_games,
            "away_games":          away_games,
        })

    return teams if teams else _fallback_stats(league_id, season)


def _fallback_stats(league_id: int, season: int) -> list:
    print(f"[fallback_stats] Stats moyennes — league {league_id}")

    if league_id == 61:
        teams_data = [
            ("Paris Saint Germain", 38, 8,  14, 12),
            ("Marseille",           30, 16, 18, 20),
            ("AS Monaco",           28, 14, 16, 18),
            ("Lyon",                26, 16, 16, 20),
            ("Lille",               26, 12, 16, 18),
            ("Nice",                24, 14, 18, 20),
            ("RC Lens",             24, 12, 16, 18),
            ("Rennes",              22, 16, 18, 22),
            ("Strasbourg",          20, 18, 16, 20),
            ("Montpellier",         18, 20, 14, 22),
            ("Nantes",              18, 20, 14, 22),
            ("Brest",               20, 18, 16, 20),
            ("Reims",               16, 18, 12, 20),
            ("Toulouse",            18, 20, 14, 22),
            ("Le Havre",            14, 22, 10, 24),
            ("Angers",              12, 24, 8,  26),
            ("Metz",                12, 24, 8,  26),
            ("Lorient",             10, 26, 6,  28),
            ("Paris FC",            16, 20, 12, 22),
            ("Auxerre",             18, 20, 14, 22),
        ]
    else:
        teams_data = [
            ("Manchester City",    40, 10, 14, 12),
            ("Arsenal",            36, 12, 14, 14),
            ("Liverpool",          38, 10, 14, 12),
            ("Chelsea",            30, 14, 16, 16),
            ("Tottenham Hotspur",  28, 16, 16, 18),
            ("Manchester United",  26, 16, 14, 18),
            ("Newcastle United",   28, 14, 16, 16),
            ("Aston Villa",        28, 14, 16, 16),
            ("West Ham United",    22, 18, 14, 20),
            ("Brighton",           24, 16, 16, 18),
            ("Wolverhampton",      18, 20, 12, 22),
            ("Fulham",             20, 18, 14, 20),
            ("Brentford",          20, 18, 12, 20),
            ("Crystal Palace",     16, 20, 10, 22),
            ("Everton",            14, 22, 10, 24),
            ("Nottingham Forest",  18, 18, 12, 20),
            ("Luton Town",         12, 26, 8,  30),
            ("Burnley",            10, 28, 6,  32),
            ("Sheffield United",   8,  30, 4,  34),
            ("Bournemouth",        18, 18, 12, 20),
            ("Ipswich Town",       14, 22, 10, 24),
            ("Leicester City",     16, 20, 12, 22),
        ]

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
# ODDS — The Odds API (h2h + totals tous seuils)
# ─────────────────────────────────────────────

def get_odds(league_id: int) -> list:
    """
    Fetch cotes depuis The Odds API.
    Marchés : h2h (1X2) + totals (Over/Under tous seuils disponibles).
    Priorité Betclic FR / Winamax FR, fallback tous bookmakers EU.
    """
    sport_key = LEAGUE_SPORT_MAP.get(league_id)
    if not sport_key:
        return []

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey":     os.getenv("ODDS_API_KEY", ""),
        "regions":    "eu",
        "markets":    "h2h,totals",   # ← h2h + tous les Over/Under
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
                        # Clé ex: over_2_5 / under_2_5 / over_3_5 etc.
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
# FIXTURE RESULT
# ─────────────────────────────────────────────

def get_fixture_result(fixture_id) -> Optional[dict]:
    return None