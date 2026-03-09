"""
model.py - Modèle Poisson + Marché + Forme récente + Bête noire

Pipeline complet :
  1. Poisson (40%) — lambda calculé depuis stats saison + forme récente
  2. Marché (60%) — probabilité implicite des cotes sans marge bookie
  3. Bonus bête noire H2H — +4% à +8% si domination historique

Améliorations forme récente :
  - Moyenne pondérée 10 derniers matchs (récents × 2, anciens × 1)
  - Momentum : série victoires/défaites consécutives → multiplicateur lambda
  - Fatigue : < 3 jours de repos → malus lambda -8%
"""

import math


# ─────────────────────────────────────────────
# POISSON DE BASE
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# STATS LIGUE
# ─────────────────────────────────────────────

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
        name    = s["team_name"]
        entry   = {
            "att_home": (s["home_goals_scored"]   / h_games) / max(league_avg_home, 0.01),
            "def_home": (s["home_goals_conceded"]  / h_games) / max(league_avg_away, 0.01),
            "att_away": (s["away_goals_scored"]    / a_games) / max(league_avg_away, 0.01),
            "def_away": (s["away_goals_conceded"]  / a_games) / max(league_avg_home, 0.01),
        }
        strengths[name]         = entry
        strengths[name.lower()] = entry
    return strengths


def _fuzzy_get(strengths: dict, name: str):
    if name in strengths:
        return strengths[name]
    nl = name.lower()
    if nl in strengths:
        return strengths[nl]
    for key in strengths:
        if isinstance(key, str) and (key.lower() in nl or nl in key.lower()):
            return strengths[key]
    return None


# ─────────────────────────────────────────────
# FORME RECENTE — multiplicateurs lambda
# ─────────────────────────────────────────────

def calc_form_multiplier(form: dict, is_home: bool) -> tuple:
    """
    Calcule les multiplicateurs d'attaque et de défense basés sur la forme récente.

    form dict (depuis get_recent_form()) :
      { avg_scored, avg_conceded, momentum, rest_days, games_played }

    Retourne (att_mult, def_mult) à appliquer sur lambda Poisson.

    Règles :
      - Si peu de matchs récents → multiplicateur neutre 1.0
      - Momentum +3 victoires  → +8% attaque
      - Momentum -3 défaites   → -8% attaque, +8% buts encaissés
      - Fatigue < 3 jours      → -8% attaque et défense
      - Fatigue > 10 jours     → -3% (rouille)
    """
    if not form or form.get("games_played", 0) < 3:
        return 1.0, 1.0

    att_mult = 1.0
    def_mult = 1.0

    # Momentum
    momentum = form.get("momentum", 0)
    if momentum >= 4:
        att_mult += 0.10   # grande série victoires → en confiance
    elif momentum >= 3:
        att_mult += 0.08
    elif momentum >= 2:
        att_mult += 0.04
    elif momentum <= -4:
        att_mult -= 0.10   # grande série défaites → en crise
        def_mult += 0.10   # concède plus
    elif momentum <= -3:
        att_mult -= 0.08
        def_mult += 0.08
    elif momentum <= -2:
        att_mult -= 0.04
        def_mult += 0.04

    # Fatigue
    rest_days = form.get("rest_days", 7)
    if rest_days <= 2:
        att_mult -= 0.08  # match dans 3 jours ou moins → fatigué
        def_mult += 0.05
    elif rest_days <= 3:
        att_mult -= 0.04
    elif rest_days > 10:
        att_mult -= 0.03  # trop de repos → rouille légère

    # Cap les multiplicateurs entre 0.75 et 1.30
    att_mult = max(0.75, min(att_mult, 1.30))
    def_mult = max(0.75, min(def_mult, 1.30))

    return round(att_mult, 3), round(def_mult, 3)


# ─────────────────────────────────────────────
# MARGE BOOKMAKER
# ─────────────────────────────────────────────

def remove_bookmaker_margin(odds_dict: dict) -> dict:
    """Retire la marge bookmaker — normalisation simple."""
    result = {}

    h2h_odds = {k: odds_dict[k] for k in ["home_win", "draw", "away_win"]
                if k in odds_dict and odds_dict[k]}
    if len(h2h_odds) >= 2:
        raw   = {k: 1 / v for k, v in h2h_odds.items()}
        total = sum(raw.values())
        for k, p in raw.items():
            result[k] = p / total

    seen = set()
    for key in odds_dict:
        if key.startswith("over_"):
            seen.add(key[5:])

    for suffix in seen:
        o_odd = odds_dict.get(f"over_{suffix}")
        u_odd = odds_dict.get(f"under_{suffix}")
        if o_odd and u_odd:
            ro = 1 / o_odd
            ru = 1 / u_odd
            t  = ro + ru
            result[f"over_{suffix}"]  = ro / t
            result[f"under_{suffix}"] = ru / t

    return result


# ─────────────────────────────────────────────
# BETE NOIRE
# ─────────────────────────────────────────────

def calc_bete_noire_bonus(market_key: str, h2h: dict) -> float:
    """
    Bonus bête noire basé sur l'historique H2H.

    Seuils :
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
        return 0.0

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
        print(f"[BETE NOIRE] {side} | {win_rate:.0%} sur {total} matchs → +{bonus*100:.0f}%")

    return bonus


# ─────────────────────────────────────────────
# PREDICTION
# ─────────────────────────────────────────────

def predict_match(home_name: str, away_name: str, strengths: dict,
                  league_avg_home: float, league_avg_away: float,
                  ou_thresholds: list = None,
                  home_form: dict = None, away_form: dict = None):
    """
    Prédit un match via Poisson avec forme récente intégrée.

    home_form / away_form : dicts depuis get_recent_form()
    """
    h = _fuzzy_get(strengths, home_name)
    a = _fuzzy_get(strengths, away_name)

    if not h or not a:
        return None

    # Lambda de base depuis stats saison
    lambda_home = h["att_home"] * a["def_away"] * league_avg_home
    lambda_away = a["att_away"] * h["def_home"] * league_avg_away

    # Ajustement forme récente
    if home_form:
        h_att, h_def = calc_form_multiplier(home_form, is_home=True)
        lambda_home *= h_att
        lambda_away *= h_def   # la défense de l'équipe dom affecte les buts de l'équipe ext
        if h_att != 1.0 or h_def != 1.0:
            print(f"  [forme] {home_name}: att×{h_att} def×{h_def} "
                  f"(momentum={home_form.get('momentum',0)}, repos={home_form.get('rest_days',7)}j)")

    if away_form:
        a_att, a_def = calc_form_multiplier(away_form, is_home=False)
        lambda_away *= a_att
        lambda_home *= a_def
        if a_att != 1.0 or a_def != 1.0:
            print(f"  [forme] {away_name}: att×{a_att} def×{a_def} "
                  f"(momentum={away_form.get('momentum',0)}, repos={away_form.get('rest_days',7)}j)")

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

    for t in (ou_thresholds or [1.5, 2.5, 3.5, 4.5]):
        key = str(t).replace(".", "_")
        over, under = calc_over_under_threshold(matrix, t)
        result[f"over_{key}"]  = over
        result[f"under_{key}"] = under

    return result


def combine_probabilities(poisson_p: float, market_p: float,
                           poisson_weight: float = 0.40) -> float:
    """Combine Poisson (40%) + marché (60%)."""
    return poisson_p * poisson_weight + market_p * (1.0 - poisson_weight)


# ─────────────────────────────────────────────
# FIND VALUE BETS
# ─────────────────────────────────────────────

def find_value_bets(predictions: dict, odds: dict,
                    value_threshold: float = 0.02, min_prob: float = 0.55,
                    poisson_weight: float = 0.40, h2h: dict = None):
    """
    Détecte les value bets de haute qualité.

    Critères :
      - Cote 1.40–2.30 (favoris clairs)
      - Pas de nul
      - Tous les Over/Under disponibles (0.5, 1.5, 2.5, 3.5...)
      - Ecart Poisson/marché < 15%
      - Bonus bête noire H2H si applicable
      - Poisson weight 40% (marché plus fiable)
    """
    fixed_markets = {"home_win": "Home Win", "away_win": "Away Win"}
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

            # Bonus bête noire
            bn_bonus = calc_bete_noire_bonus(market_key, h2h)
            if bn_bonus > 0:
                combined_p = min(combined_p + bn_bonus, 0.97)

            if combined_p < min_prob:
                return
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

        # Tous les Over/Under disponibles (0.5, 1.5, 2.5, 3.5, 4.5...)
        for bk_key, bk_odd in bk_odds.items():
            if not (bk_key.startswith("over_") or bk_key.startswith("under_")):
                continue
            direction = "over" if bk_key.startswith("over_") else "under"
            suffix = bk_key.split("_", 1)[1]
            try:
                threshold = float(suffix.replace("_", "."))
            except ValueError:
                continue
            pred_key = bk_key  # ex: "over_2_5" ou "under_1_5"
            check_market(
                pred_key, f"{'Over' if direction == 'over' else 'Under'} {threshold}",
                predictions.get(pred_key),
                bk_odd,
                market_probs.get(pred_key),
            )

    return sorted(best_per_market.values(), key=lambda x: x["value"], reverse=True)