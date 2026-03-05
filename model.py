"""
model.py - Poisson-based football match prediction model

Supporte tous les seuils Over/Under (1.5, 2.5, 3.5, 4.5...)
retournés par The Odds API.
"""

import math


def poisson_prob(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k * math.exp(-lam)) / math.factorial(k)


def build_score_matrix(lambda_home: float, lambda_away: float, max_goals: int = 10):
    return [
        [poisson_prob(lambda_home, i) * poisson_prob(lambda_away, j)
         for j in range(max_goals + 1)]
        for i in range(max_goals + 1)
    ]


def calc_1x2(matrix) -> dict:
    home_win = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i > j)
    draw     = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i == j)
    away_win = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i < j)
    return {"home_win": home_win, "draw": draw, "away_win": away_win}


def calc_over_under_threshold(matrix, threshold: float) -> tuple:
    """
    Calcule les probabilités Over/Under pour un seuil donné.
    Retourne (prob_over, prob_under).
    """
    over = sum(
        matrix[i][j]
        for i in range(len(matrix))
        for j in range(len(matrix[i]))
        if (i + j) > threshold
    )
    return round(over, 4), round(1 - over, 4)


def calc_btts(matrix) -> dict:
    btts = sum(
        matrix[i][j]
        for i in range(1, len(matrix))
        for j in range(1, len(matrix[i]))
    )
    return {"btts_yes": round(btts, 4), "btts_no": round(1 - btts, 4)}


def calc_league_averages(team_stats: dict):
    total_home_scored = sum(s["home_goals_scored"] for s in team_stats.values())
    total_away_scored = sum(s["away_goals_scored"] for s in team_stats.values())
    total_home_games  = sum(s["home_games"] for s in team_stats.values())
    total_away_games  = sum(s["away_games"] for s in team_stats.values())
    avg_home = total_home_scored / max(total_home_games, 1)
    avg_away = total_away_scored / max(total_away_games, 1)
    return avg_home, avg_away


def calc_attack_defense_strength(team_stats: dict, league_avg_home: float, league_avg_away: float):
    strengths = {}
    for tid, s in team_stats.items():
        h_games = max(s["home_games"], 1)
        a_games = max(s["away_games"], 1)

        home_scored_avg   = s["home_goals_scored"]   / h_games
        home_conceded_avg = s["home_goals_conceded"]  / h_games
        away_scored_avg   = s["away_goals_scored"]    / a_games
        away_conceded_avg = s["away_goals_conceded"]  / a_games

        name = s["team_name"]
        entry = {
            "att_home": home_scored_avg   / max(league_avg_home, 0.01),
            "def_home": home_conceded_avg / max(league_avg_away, 0.01),
            "att_away": away_scored_avg   / max(league_avg_away, 0.01),
            "def_away": away_conceded_avg / max(league_avg_home, 0.01),
        }
        strengths[name]         = entry
        strengths[name.lower()] = entry

    return strengths


def _fuzzy_get(strengths: dict, name: str):
    if name in strengths:
        return strengths[name]
    name_lower = name.lower()
    if name_lower in strengths:
        return strengths[name_lower]
    for key in strengths:
        if isinstance(key, str) and (key.lower() in name_lower or name_lower in key.lower()):
            return strengths[key]
    return None


def predict_match(home_name: str, away_name: str, strengths: dict,
                  league_avg_home: float, league_avg_away: float,
                  ou_thresholds: list = None):
    """
    Prédit un match via Poisson.
    ou_thresholds : liste de seuils Over/Under à calculer
                    ex: [1.5, 2.5, 3.5, 4.5]
                    Si None, calcule 1.5, 2.5, 3.5, 4.5 par défaut.
    """
    h = _fuzzy_get(strengths, home_name)
    a = _fuzzy_get(strengths, away_name)

    if not h or not a:
        return None

    lambda_home = h["att_home"] * a["def_away"] * league_avg_home
    lambda_away = a["att_away"] * h["def_home"] * league_avg_away

    lambda_home = max(0.3, min(lambda_home, 6.0))
    lambda_away = max(0.3, min(lambda_away, 6.0))

    matrix     = build_score_matrix(lambda_home, lambda_away)
    probs_1x2  = calc_1x2(matrix)
    probs_btts = calc_btts(matrix)

    result = {
        "lambda_home": round(lambda_home, 3),
        "lambda_away": round(lambda_away, 3),
        "home_win":    round(probs_1x2["home_win"], 4),
        "draw":        round(probs_1x2["draw"], 4),
        "away_win":    round(probs_1x2["away_win"], 4),
        "btts_yes":    probs_btts["btts_yes"],
        "btts_no":     probs_btts["btts_no"],
    }

    # Calcul Over/Under pour tous les seuils demandés
    thresholds = ou_thresholds if ou_thresholds else [1.5, 2.5, 3.5, 4.5]
    for t in thresholds:
        key = str(t).replace(".", "_")
        over, under = calc_over_under_threshold(matrix, t)
        result[f"over_{key}"]  = over
        result[f"under_{key}"] = under

    return result


def find_value_bets(predictions: dict, odds: dict,
                    value_threshold: float = 0.05, min_prob: float = 0.55):
    """
    Compare les probabilités du modèle aux cotes bookmakers.
    Gère les marchés 1X2 ET tous les Over/Under dynamiquement.
    Retourne UN seul bet par marché — le bookmaker avec la meilleure cote.
    """
    # Marchés fixes 1X2
    fixed_markets = {
        "home_win": "Home Win",
        "draw":     "Draw",
        "away_win": "Away Win",
    }

    best_per_market = {}

    def check_market(market_key, market_label, prob, bk_odd, bk_name):
        if prob is None or bk_odd is None:
            return
        if prob < min_prob:
            return
        value = (bk_odd * prob) - 1
        if value <= value_threshold:
            return
        existing = best_per_market.get(market_key)
        if existing is None or bk_odd > existing["bk_odds"]:
            best_per_market[market_key] = {
                "market":      market_label,
                "bookmaker":   bk_name,
                "bk_odds":     round(bk_odd, 3),
                "model_odds":  round(1 / prob, 3),
                "probability": round(prob, 4),
                "value":       round(value, 4),
            }

    for bk_name, bk_odds in odds.items():
        # 1X2
        for market_key, market_label in fixed_markets.items():
            check_market(market_key, market_label,
                         predictions.get(market_key),
                         bk_odds.get(market_key),
                         bk_name)

        # Over/Under dynamiques — parcourt toutes les clés de la forme over_X_X / under_X_X
        for bk_key, bk_odd in bk_odds.items():
            if not (bk_key.startswith("over_") or bk_key.startswith("under_")):
                continue

            # bk_key ex: "over_2_5" → threshold_str = "2.5"
            parts = bk_key.split("_", 1)  # ["over", "2_5"]
            direction = parts[0]           # "over" ou "under"
            threshold_str = parts[1].replace("_", ".")  # "2.5"

            try:
                threshold = float(threshold_str)
            except ValueError:
                continue

            # Clé dans predictions
            pred_key    = f"{direction}_{parts[1]}"
            prob        = predictions.get(pred_key)
            market_label = f"{'Over' if direction == 'over' else 'Under'} {threshold}"

            check_market(pred_key, market_label, prob, bk_odd, bk_name)

    return sorted(best_per_market.values(), key=lambda x: x["value"], reverse=True)