"""
api_clients.py - API wrappers for The Odds API and API-Sports
"""

import os
import requests
from datetime import datetime, timedelta
from typing import Optional


# ─────────────────────────────────────────────
# API-SPORTS  (fixtures + team stats)
# ─────────────────────────────────────────────

APISPORTS_BASE = "https://v3.football.api-sports.io"


def _apisports_headers():
    return {
        "x-apisports-key": os.getenv("APISPORTS_KEY", ""),
    }


def get_fixtures(league_id: int, season: int, days_ahead: int = 3) -> list:
    """
    Fetch upcoming fixtures for a league within the next N days.
    Returns list of fixture dicts.
    """
    today = datetime.utcnow().date()
    date_from = today.strftime("%Y-%m-%d")
    date_to = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    url = f"{APISPORTS_BASE}/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "from": date_from,
        "to": date_to,
        "status": "NS",  # Not Started
        "timezone": "UTC",
    }

    resp = requests.get(url, headers=_apisports_headers(), params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    fixtures = []
    for item in data.get("response", []):
        fix = item.get("fixture", {})
        teams = item.get("teams", {})
        fixtures.append({
            "fixture_id": fix.get("id"),
            "date": fix.get("date", "")[:10],
            "home_team_id": teams.get("home", {}).get("id"),
            "home_team_name": teams.get("home", {}).get("name"),
            "away_team_id": teams.get("away", {}).get("id"),
            "away_team_name": teams.get("away", {}).get("name"),
            "league_id": league_id,
        })
    return fixtures


def get_fixture_result(fixture_id: int) -> Optional[dict]:
    """Fetch result of a finished fixture."""
    url = f"{APISPORTS_BASE}/fixtures"
    params = {"id": fixture_id}
    resp = requests.get(url, headers=_apisports_headers(), params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    response = data.get("response", [])
    if not response:
        return None

    item = response[0]
    goals = item.get("goals", {})
    fixture_status = item.get("fixture", {}).get("status", {}).get("short", "")

    if fixture_status not in ("FT", "AET", "PEN"):
        return None  # Not finished

    return {
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "status": fixture_status,
        "score": f"{goals.get('home')}-{goals.get('away')}",
    }


def get_team_standings(league_id: int, season: int) -> list:
    """
    Fetch team statistics (goals scored/conceded home/away) for the season.
    Uses the /teams/statistics endpoint for each team found in standings.
    Returns list of team stat dicts.
    """
    # Step 1: get teams from standings
    url = f"{APISPORTS_BASE}/standings"
    params = {"league": league_id, "season": season}
    resp = requests.get(url, headers=_apisports_headers(), params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    teams = []
    for group in data.get("response", []):
        for league_data in group.get("league", {}).get("standings", []):
            for entry in league_data:
                team = entry.get("team", {})
                home = entry.get("home", {})
                away = entry.get("away", {})
                teams.append({
                    "league_id": league_id,
                    "season": season,
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "home_goals_scored": home.get("goals", {}).get("for", 0) or 0,
                    "home_goals_conceded": home.get("goals", {}).get("against", 0) or 0,
                    "away_goals_scored": away.get("goals", {}).get("for", 0) or 0,
                    "away_goals_conceded": away.get("goals", {}).get("against", 0) or 0,
                    "home_games": (home.get("win", 0) or 0) + (home.get("draw", 0) or 0) + (home.get("lose", 0) or 0),
                    "away_games": (away.get("win", 0) or 0) + (away.get("draw", 0) or 0) + (away.get("lose", 0) or 0),
                })
    return teams


# ─────────────────────────────────────────────
# THE ODDS API  (bookmaker odds)
# ─────────────────────────────────────────────

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Map league_id to Odds API sport key
LEAGUE_SPORT_MAP = {
    61: "soccer_france_ligue_one",
    39: "soccer_england_premier_league",
}

# Bookmakers to focus on (France + UK)
TARGET_BOOKMAKERS = ["winamax", "betclic", "bet365", "williamhill", "unibet"]


def get_odds(league_id: int) -> list:
    """
    Fetch upcoming match odds from The Odds API.
    Returns list of match odds dicts.
    """
    sport_key = LEAGUE_SPORT_MAP.get(league_id)
    if not sport_key:
        return []

    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": os.getenv("ODDS_API_KEY", ""),
        "regions": "eu",
        "markets": "h2h",  # 1X2
        "oddsFormat": "decimal",
        "bookmakers": ",".join(TARGET_BOOKMAKERS),
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    events = resp.json()

    results = []
    for event in events:
        home = event.get("home_team")
        away = event.get("away_team")
        commence = event.get("commence_time", "")[:10]
        bookmakers_odds = {}

        for bk in event.get("bookmakers", []):
            bk_name = bk.get("title", bk.get("key", "Unknown"))
            for mkt in bk.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue
                outcomes = {o["name"]: o["price"] for o in mkt.get("outcomes", [])}
                bookmakers_odds[bk_name] = {
                    "home_win": outcomes.get(home),
                    "draw": outcomes.get("Draw"),
                    "away_win": outcomes.get(away),
                }

        results.append({
            "date": commence,
            "home_team": home,
            "away_team": away,
            "odds": bookmakers_odds,
            "event_id": event.get("id"),
            "league_id": league_id,
        })

    return results
