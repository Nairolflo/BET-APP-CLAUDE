"""
api_clients.py - API wrappers

- get_fixtures()         → The Odds API (fixtures extraits des cotes)
- get_team_standings()   → football-data.org standings réels + fallback stats
- get_odds()             → The Odds API — Betclic/Winamax FR en priorité,
                           fallback tous bookmakers EU
                           Marchés : h2h + totals (tous seuils)
- get_all_results_today()→ football-data.org résultats du jour
- normalize_team_name()  → normalisation noms équipes pour matching
"""

import os
import requests
from datetime import datetime, timezone
from typing import Optional


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

LEAGUE_SPORT_MAP = {
    61: "soccer_france_ligue_one",
    39: "soccer_epl",
}

PREFERRED_BOOKMAKERS = ["Betclic (FR)", "Winamax (FR)"]

FOOTBALLDATA_BASE = "https://api.football-data.org/v4"

FOOTBALLDATA_LEAGUE_MAP = {
    39: "PL",
    61: "FL1",
}

TEAM_NAME_MAP = {
    'fc nantes': 'nantes',
    'angers sco': 'angers',
    'aj auxerre': 'auxerre',
    'rc strasbourg alsace': 'strasbourg',
    'toulouse fc': 'toulouse',
    'olympique de marseille': 'marseille',
    'olympique lyonnais': 'lyon',
    'paris saint-germain fc': 'paris saint germain',
    'stade rennais fc': 'rennes',
    'losc lille': 'lille',
    'ogc nice': 'nice',
    'as monaco fc': 'as monaco',
    'rc lens': 'rc lens',
    'stade brestois 29': 'brest',
    'stade de reims': 'reims',
    'le havre ac': 'le havre',
    'montpellier hsc': 'montpellier',
    'manchester city fc': 'manchester city',
    'arsenal fc': 'arsenal',
    'liverpool fc': 'liverpool',
    'chelsea fc': 'chelsea',
    'tottenham hotspur fc': 'tottenham hotspur',
    'manchester united fc': 'manchester united',
    'newcastle united fc': 'newcastle united',
    'aston villa fc': 'aston villa',
    'west ham united fc': 'west ham united',
    'brighton & hove albion fc': 'brighton',
    'wolverhampton wanderers fc': 'wolverhampton wanderers',
    'fulham fc': 'fulham',
    'brentford fc': 'brentford',
    'crystal palace fc': 'crystal palace',
    'everton fc': 'everton',
    'nottingham forest fc': 'nottingham forest',
    'bournemouth afc': 'bournemouth',
    'ipswich town fc': 'ipswich town',
    'leicester city fc': 'leicester city',
    'sunderland afc': 'sunderland',
}


# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

def normalize_team_name(name: str) -> str:
    """Normalise un nom d'equipe pour le matching."""
    n = name.lower().strip()
    if n in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[n]
    for suffix in [' fc', ' afc', ' sc', ' cf', ' ac', ' rc', ' as', ' us']:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def _footballdata_headers() -> dict:
    return {"X-Auth-Token": os.getenv("FOOTBALLDATA_KEY", "")}


# ─────────────────────────────────────────────
# FIXTURES
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
# TEAM STANDINGS
# ─────────────────────────────────────────────

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
                    team_name = entry.get("team", {}).get("name", "")
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
                    normalized_name = normalize_team_name(team_name)
                    teams.append({
                        "league_id":           league_id,
                        "season":              season,
                        "team_id":             abs(hash(normalized_name)) % 100000,
                        "team_name":           normalized_name,
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
# ODDS
# ─────────────────────────────────────────────

def get_odds(league_id: int) -> list:
    """Cotes depuis The Odds API. Marches h2h + totals (tous seuils O/U)."""
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
    """Resultats d'une journee par ligue via football-data.org."""
    competition = FOOTBALLDATA_LEAGUE_MAP.get(league_id)
    if not competition:
        return {}

    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not key:
        print("[get_fixtures_results_batch] FOOTBALLDATA_KEY manquante")
        return {}

    url = f"{FOOTBALLDATA_BASE}/competitions/{competition}/matches"
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
            "home_goals": hg, "away_goals": ag,
            "total_goals": hg + ag, "status": "FINISHED",
            "score": f"{hg}-{ag}", "home_name": home, "away_name": away,
        }
        results[(home.lower(), away.lower())] = result
        results[(normalize_team_name(home), normalize_team_name(away))] = result

    print(f"[get_fixtures_results_batch] {len(results)//2} resultats pour {competition} le {date}")
    return results


def get_all_results_today(date: str) -> dict:
    """Tous les resultats du jour toutes ligues via football-data.org."""
    key = os.getenv("FOOTBALLDATA_KEY", "")
    if not key:
        return {}

    url = f"{FOOTBALLDATA_BASE}/matches"
    params = {"dateFrom": date, "dateTo": date}

    try:
        resp = requests.get(url, headers=_footballdata_headers(), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[get_all_results_today] Erreur: {e}")
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
            "home_goals": hg, "away_goals": ag,
            "total_goals": hg + ag, "status": "FINISHED",
            "score": f"{hg}-{ag}", "home_name": home, "away_name": away,
        }
        results[(home.lower(), away.lower())] = result
        results[(normalize_team_name(home), normalize_team_name(away))] = result

    print(f"[get_all_results_today] {len(results)//2} resultats pour le {date}")
    return results


def get_fixture_result(fixture_id) -> Optional[dict]:
    """Non utilise — on passe par get_fixtures_results_batch."""
    return None