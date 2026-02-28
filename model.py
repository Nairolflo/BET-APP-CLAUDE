"""
model.py - Poisson-based football match prediction model

Calculates expected goals (xG proxies via attack/defense strength),
builds score probability matrices, and derives 1X2/over-under probabilities.

References:
  - Smarkets Poisson tutorial
  - Paul Riley xG zone method (adapted to available stats)
"""

import math
from typing import Optional


def factorial(n: int) -> int:
    return math.factorial(n)


def poisson_prob(lam: float, k: int) -> float:
    """P(X=k) for Poisson distribution with rate lambda."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k * math.exp(-lam)) / factorial(k)


def build_score_matrix(lambda_home: float, lambda_away: float, max_goals: int = 8):
    """
    Build a (max_goals+1) x (max_goals+1) matrix of P(home=i, away=j).
    Assumes independence between home and away goals.
    """
    matrix = []
    for i in range(max_goals + 1):
        row = []
        for j in range(max_goals + 1):
            row.append(poisson_prob(lambda_home, i) * poisson_prob(lambda_away, j))
        matrix.append(row)
    return matrix


def calc_1x2(matrix) -> dict:
    """Calculate Home Win / Draw / Away Win probabilities from score matrix."""
    home_win = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i > j)
    draw = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i == j)
    away_win = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i < j)
    return {"home_win": home_win, "draw": draw, "away_win": away_win}


def calc_over_under(matrix, threshold: float = 2.5) -> dict:
    """Calculate Over/Under 2.5 goals probabilities."""
    over = sum(
        matrix[i][j]
        for i in range(len(matrix))
        for j in range(len(matrix[i]))
        if (i + j) > threshold
    )
    under = 1 - over
    return {"over_2_5": over, "under_2_5": under}


def calc_btts(matrix) -> dict:
    """Both Teams To Score probability."""
    btts = sum(
        matrix[i][j]
        for i in range(1, len(matrix))
        for j in range(1, len(matrix[i]))
    )
    return {"btts_yes": btts, "btts_no": 1 - btts}


def calc_attack_defense_strength(team_stats: dict, league_avg_home: float, league_avg_away: float):
    """
    Calculate attack/defense strength multipliers for each team.

    attack_strength  = team avg goals scored / league average
    defense_strength = team avg goals conceded / league average

    Returns dict: {team_id: {"att_home", "def_home", "att_away", "def_away"}}
    """
    strengths = {}
    for tid, s in team_stats.items():
        h_games = max(s["home_games"], 1)
        a_games = max(s["away_games"], 1)

        home_scored_avg = s["home_goals_scored"] / h_games
        home_conceded_avg = s["home_goals_conceded"] / h_games
        away_scored_avg = s["away_goals_scored"] / a_games
        away_conceded_avg = s["away_goals_conceded"] / a_games

        strengths[tid] = {
            "att_home": home_scored_avg / max(league_avg_home, 0.01),
            "def_home": home_conceded_avg / max(league_avg_away, 0.01),
            "att_away": away_scored_avg / max(league_avg_away, 0.01),
            "def_away": away_conceded_avg / max(league_avg_home, 0.01),
            "name": s["team_name"],
        }
    return strengths


def calc_league_averages(team_stats: dict):
    """Compute league-wide home/away average goals per game."""
    total_home_scored = sum(s["home_goals_scored"] for s in team_stats.values())
    total_away_scored = sum(s["away_goals_scored"] for s in team_stats.values())
    total_home_games = sum(s["home_games"] for s in team_stats.values())
    total_away_games = sum(s["away_games"] for s in team_stats.values())

    avg_home = total_home_scored / max(total_home_games, 1)
    avg_away = total_away_scored / max(total_away_games, 1)
    return avg_home, avg_away


def predict_match(home_team_id: int, away_team_id: int, strengths: dict, league_avg_home: float, league_avg_away: float):
    """
    Predict a match using Dixon-Coles-style Poisson.

    Returns dict with lambda_home, lambda_away, and all probabilities.
    """
    h = strengths.get(home_team_id)
    a = strengths.get(away_team_id)

    if not h or not a:
        return None

    lambda_home = h["att_home"] * a["def_away"] * league_avg_home
    lambda_away = a["att_away"] * h["def_home"] * league_avg_away

    # Clamp lambdas to realistic range
    lambda_home = max(0.3, min(lambda_home, 6.0))
    lambda_away = max(0.3, min(lambda_away, 6.0))

    matrix = build_score_matrix(lambda_home, lambda_away)
    probs_1x2 = calc_1x2(matrix)
    probs_ou = calc_over_under(matrix)
    probs_btts = calc_btts(matrix)

    return {
        "lambda_home": round(lambda_home, 3),
        "lambda_away": round(lambda_away, 3),
        "home_win": round(probs_1x2["home_win"], 4),
        "draw": round(probs_1x2["draw"], 4),
        "away_win": round(probs_1x2["away_win"], 4),
        "over_2_5": round(probs_ou["over_2_5"], 4),
        "under_2_5": round(probs_ou["under_2_5"], 4),
        "btts_yes": round(probs_btts["btts_yes"], 4),
        "btts_no": round(probs_btts["btts_no"], 4),
    }


def find_value_bets(predictions: dict, odds: dict, value_threshold: float = 0.05, min_prob: float = 0.55):
    """
    Compare model probabilities vs bookmaker odds to find value bets.

    predictions: {"home_win": p, "draw": p, "away_win": p, ...}
    odds: {"Winamax": {"home_win": 1.85, "draw": 3.4, "away_win": 4.5}, ...}

    Returns list of value bets sorted by value descending.
    """
    market_map = {
        "home_win": "Home Win",
        "draw": "Draw",
        "away_win": "Away Win",
        "over_2_5": "Over 2.5",
        "under_2_5": "Under 2.5",
    }

    value_bets = []

    for bk_name, bk_odds in odds.items():
        for market_key, market_label in market_map.items():
            prob = predictions.get(market_key)
            bk_odd = bk_odds.get(market_key)

            if prob is None or bk_odd is None:
                continue
            if prob < min_prob:
                continue

            value = (bk_odd * prob) - 1
            if value > value_threshold:
                model_odds = round(1 / prob, 3)
                value_bets.append({
                    "market": market_label,
                    "bookmaker": bk_name,
                    "bk_odds": round(bk_odd, 3),
                    "model_odds": model_odds,
                    "probability": round(prob, 4),
                    "value": round(value, 4),
                })

    value_bets.sort(key=lambda x: x["value"], reverse=True)
    return value_bets
