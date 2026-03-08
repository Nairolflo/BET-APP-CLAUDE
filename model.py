"""
model.py - Poisson + Market Combined Prediction Model

Combine deux sources de probabilité :
  1. Modèle Poisson (50%) — basé sur stats attaque/défense saison
  2. Probabilité implicite des cotes (50%) — intègre forme, blessés, H2H, etc.

Bonus bête noire :
  - +4% si équipe domine H2H à 70%+ sur 5+ matchs
  - +6% si 80%+ sur 8+ matchs
  - +8% si 90%+ sur 10+ matchs
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

        name  = s["team_name"]
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


def remove_bookmaker_margin(odds_dict: dict) -> dict:
    """Retire la marge bookmaker — normalisation simple."""
    result = {}

    # 1X2
    h2h_keys = ["home_win", "draw", "away_win"]
    h2h_odds = {k: odds_dict[k] for k in h2h_keys if k in odds_dict and odds_dict[k]}
    if len(h2h_odds) >= 2:
        raw_probs = {k: 1 / v for k, v in h2h_odds.items()}
        total = sum(raw_probs.values())
        for k, p in raw_probs.items():
            result[k] = p / total

    # Over/Under par seuil
    thresholds_seen = set()
    for key in odds_dict:
        if key.startswith("over_"):
            thresholds_seen.add(key[5:])

    for suffix in thresholds_seen:
        over_key  = f"over_{suffix}"
        under_key = f"under_{suffix}"
        o_odd = odds_dict.get(over_key)
        u_odd = odds_dict.get(under_key)
        if o_odd and u_odd:
            raw_over  = 1 / o_odd
            raw_under = 1 / u_odd
            total     = raw_over + raw_under
            result[over_key]  = raw_over  / total
            result[under_key] = raw_under / total

    return result


# ─────────────────────────────────────────────
# BETE NOIRE
# ─────────────────────────────────────────────

def calc_bete_noire_bonus(market_key: str, h2h: dict) -> float:
    """
    Calcule le bonus bête noire basé sur l'historique H2H.

    Structure h2h attendue (retournée par get_h2h()) :
      {
        "total":         10,   # nb matchs H2H analysés
        "home_wins":      7,   # victoires equipe domicile du match actuel
        "away_wins":      2,   # victoires equipe extérieure
        "draws":          1,
        "win_rate_home":  0.70,
        "win_rate_away":  0.20,
      }

    Seuils bonus :
      - 70%+ sur 5+ matchs  → +4%
      - 80%+ sur 8+ matchs  → +6%
      - 90%+ sur 10+ matchs → +8%
    """
    if not h2h:
        return 0.0

    total = h2h.get("total", 0)
    if total < 5:
        return 0.0

    if market_key == "home_win":
        win_rate = h2h.get("win_rate_home", 0)
    elif market_key == "away_win":
        win_rate = h2h.get("win_rate_away", 0)
    else:
        return 0.0  # pas de bonus bete noire sur Over/Under

    if win_rate >= 0.90 and total >= 10:
        bonus = 0.08
    elif win_rate >= 0.80 and total >= 8:
        bonus = 0.06
    elif win_rate >= 0.70 and total >= 5:
        bonus = 0.04
    else:
        bonus = 0.0

    if bonus > 0:
        side = "HOME" if market_key == "home_win" else "AWAY"
        print(
            f"[BETE NOIRE] {side} | win_rate={win_rate:.0%} sur {total} matchs "
            f"→ bonus +{bonus*100:.0f}%"
        )

    return bonus


# ─────────────────────────────────────────────
# PREDICTION
# ─────────────────────────────────────────────

def predict_match(home_name: str, away_name: str, strengths: dict,
                  league_avg_home: float, league_avg_away: float,
                  ou_thresholds: list = None):
    """Prédit un match via Poisson."""
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

    thresholds = ou_thresholds if ou_thresholds else [1.5, 2.5, 3.5, 4.5]
    for t in thresholds:
        key = str(t).replace(".", "_")
        over, under = calc_over_under_threshold(matrix, t)
        result[f"over_{key}"]  = over
        result[f"under_{key}"] = under

    return result


def combine_probabilities(poisson_p: float, market_p: float,
                           poisson_weight: float = 0.50) -> float:
    """Combine probabilité Poisson (50%) et marché (50%)."""
    return poisson_p * poisson_weight + market_p * (1.0 - poisson_weight)


# ─────────────────────────────────────────────
# FIND VALUE BETS
# ─────────────────────────────────────────────

def find_value_bets(predictions: dict, odds: dict,
                    value_threshold: float = 0.02, min_prob: float = 0.55,
                    poisson_weight: float = 0.50, h2h: dict = None):
    """
    Détecte les value bets de haute qualité.

    Critères :
      - Cote 1.40–2.30 (favoris clairs)
      - Pas de nul
      - Pas d'Under
      - Over 2.5 et Over 3.5 uniquement
      - Ecart Poisson/marché < 15%
      - Bonus bête noire si h2h favorable
    """
    fixed_markets = {
        "home_win": "Home Win",
        "away_win": "Away Win",
    }

    best_per_market = {}
    MIN_ODDS = 1.40
    MAX_ODDS = 2.30

    for bk_name, bk_odds in odds.items():
        market_probs = remove_bookmaker_margin(bk_odds)

        def check_market(market_key, market_label, poisson_p, bk_odd, market_p):
            if poisson_p is None or bk_odd is None or market_p is None:
                return

            if bk_odd < MIN_ODDS or bk_odd > MAX_ODDS:
                return

            combined_p = combine_probabilities(poisson_p, market_p, poisson_weight)

            # Bonus bête noire H2H
            bn_bonus = calc_bete_noire_bonus(market_key, h2h)
            if bn_bonus > 0:
                combined_p = min(combined_p + bn_bonus, 0.97)

            if combined_p < min_prob:
                return

            # Les deux modèles doivent être d'accord
            if abs(poisson_p - market_p) > 0.15:
                return

            value = (bk_odd * combined_p) - 1
            if value <= value_threshold:
                return

            existing = best_per_market.get(market_key)
            if existing is None or value > existing["value"]:
                best_per_market[market_key] = {
                    "market":          market_label,
                    "bookmaker":       bk_name,
                    "bk_odds":         round(bk_odd, 3),
                    "model_odds":      round(1 / combined_p, 3),
                    "probability":     round(combined_p, 4),
                    "poisson_prob":    round(poisson_p, 4),
                    "market_prob":     round(market_p, 4),
                    "value":           round(value, 4),
                    "bete_noire":      bn_bonus > 0,
                    "bete_noire_rate": round(
                        h2h.get("win_rate_home" if market_key == "home_win" else "win_rate_away", 0), 3
                    ) if h2h and bn_bonus > 0 else 0,
                }

        # 1X2 sans Draw
        for market_key, market_label in fixed_markets.items():
            check_market(
                market_key, market_label,
                predictions.get(market_key),
                bk_odds.get(market_key),
                market_probs.get(market_key),
            )

        # Over 2.5 et Over 3.5 uniquement
        for bk_key, bk_odd in bk_odds.items():
            if not bk_key.startswith("over_"):
                continue
            parts         = bk_key.split("_", 1)
            threshold_str = parts[1].replace("_", ".")
            try:
                threshold = float(threshold_str)
            except ValueError:
                continue

            if threshold not in (2.5, 3.5):
                continue

            pred_key     = f"over_{parts[1]}"
            market_label = f"Over {threshold}"

            check_market(
                pred_key, market_label,
                predictions.get(pred_key),
                bk_odd,
                market_probs.get(pred_key),
            )

    return sorted(best_per_market.values(), key=lambda x: x["value"], reverse=True)