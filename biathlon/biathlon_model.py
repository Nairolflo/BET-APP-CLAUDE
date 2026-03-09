"""
biathlon_model.py
-----------------
Modèle de prédiction biathlon.

Pipeline :
  1. Calcul des features de forme par athlète + format de course
  2. Rating continu (ski speed + shooting + recency)
  3. H2H : P(A devant B) via logistique calibrée
  4. Podium : Monte Carlo 1M simulations → P(Top 3)
  5. Value bet : comparaison avec cotes implicites bookmakers

Pondérations par format :
  Sprint      → ski 45% / tir couché 25% / tir debout 20% / forme 10%
  Poursuite   → ski 50% / tir 35% / forme 15%
  Individuelle→ ski 40% / tir 45% / forme 15%
  Mass Start  → ski 55% / tir 30% / forme 15%
  Relais      → ski 50% / tir 40% / forme 10%
"""

import math
import random
import logging
from typing import Optional
from biathlon_client import (
    get_athlete_results, get_results, get_analytic_results,
    parse_shooting_string, time_to_seconds,
    CURRENT_SEASON, PREV_SEASON
)

log = logging.getLogger(__name__)

# ─── Pondérations par format ──────────────────
WEIGHTS = {
    "SR": {"ski": 0.45, "prone": 0.25, "standing": 0.20, "form": 0.10},  # Sprint
    "PU": {"ski": 0.50, "prone": 0.175,"standing": 0.175,"form": 0.15},  # Poursuite
    "IN": {"ski": 0.40, "prone": 0.225,"standing": 0.225,"form": 0.15},  # Individuelle
    "MS": {"ski": 0.55, "prone": 0.15, "standing": 0.15, "form": 0.15},  # Mass Start
    "RL": {"ski": 0.50, "prone": 0.20, "standing": 0.20, "form": 0.10},  # Relais
    "MX": {"ski": 0.50, "prone": 0.20, "standing": 0.20, "form": 0.10},  # Relais Mixte
}
WEIGHTS["SM"] = WEIGHTS["MX"]  # Single Mixed

# Paramètre logistique H2H (calibré sur données historiques IBU)
# Plus k est grand, plus les différences de rating sont amplifiées
K_LOGISTIC = 1.5  # calibré : delta 0.04 → ~65%, delta 0.25 → ~98%

# Variance Monte Carlo par format (le biathlon est très aléatoire à cause du tir)
SIGMA = {
    "SR": 1.2,   # Sprint : très variable
    "PU": 1.0,   # Poursuite : moins variable (partie sur résultat sprint)
    "IN": 1.4,   # Individuelle : très variable (4 séances de tir)
    "MS": 1.1,   # Mass Start
    "RL": 0.9,   # Relais : moyennée sur 4 athlètes
    "MX": 0.9,
    "SM": 1.0,
}

# Fenêtres de forme
FORM_WINDOW_SHORT = 5   # dernières 5 courses du même format
FORM_WINDOW_LONG  = 15  # dernières 15 courses toutes disciplines

# Seuil value bet minimum (5% validé par sportindepth)
VALUE_THRESHOLD = 0.05


def build_athlete_features(ibu_id: str, race_format: str,
                            seasons: list = None) -> Optional[dict]:
    """
    Calcule les features de forme d'un athlète pour un format donné.

    Retourne :
    {
      "ibu_id": str,
      "name": str,
      "nat": str,
      "avg_rank_short": float,       # rang moyen sur 5 dernières (même format)
      "avg_rank_long": float,        # rang moyen sur 15 dernières (tous formats)
      "rank_std": float,             # écart-type (volatilité)
      "top3_rate": float,            # % podiums sur la saison
      "top10_rate": float,
      "prone_accuracy": float,       # taux tir couché
      "standing_accuracy": float,    # taux tir debout
      "ski_speed_score": float,      # score vitesse ski (0-1, plus élevé = plus rapide)
      "fatigue": float,              # malus si course dans les 2 derniers jours
      "n_races": int,                # nb de courses analysées
      "format_n_races": int,         # nb de courses dans ce format
      "last_race_date": str,
    }
    """
    if seasons is None:
        seasons = [CURRENT_SEASON, PREV_SEASON]

    all_results = []
    for season in seasons:
        results = get_athlete_results(ibu_id, season)
        all_results.extend(results)

    if not all_results:
        return None

    # Tri par date décroissante
    all_results.sort(key=lambda r: r.get("StartTime", ""), reverse=True)

    # Sépare les résultats par format
    format_results = [r for r in all_results if r.get("RaceTypeId") == race_format]

    # Features de rang
    def avg_rank(results_list, window):
        ranks = [r.get("Rank") for r in results_list[:window] if r.get("Rank")]
        return sum(ranks) / len(ranks) if ranks else None

    avg_rank_short  = avg_rank(format_results, FORM_WINDOW_SHORT)
    avg_rank_long   = avg_rank(all_results, FORM_WINDOW_LONG)

    # Écart-type de rang (volatilité)
    recent_ranks = [r.get("Rank") for r in format_results[:FORM_WINDOW_LONG] if r.get("Rank")]
    if len(recent_ranks) >= 2:
        mean_r = sum(recent_ranks) / len(recent_ranks)
        rank_std = math.sqrt(sum((r - mean_r)**2 for r in recent_ranks) / len(recent_ranks))
    else:
        rank_std = 10.0  # inconnu = volatilité élevée

    # Top 3 / Top 10
    season_results = [r for r in all_results if seasons[0] in r.get("RaceId", "")]
    def rate_top(res_list, threshold):
        ranks = [r.get("Rank") for r in res_list if r.get("Rank")]
        if not ranks:
            return 0.0
        return len([r for r in ranks if r <= threshold]) / len(ranks)

    top3_rate  = rate_top(season_results, 3)
    top10_rate = rate_top(season_results, 10)

    # Stats de tir
    prone_hits, prone_total       = 0, 0
    standing_hits, standing_total = 0, 0

    for r in format_results[:FORM_WINDOW_LONG]:
        shooting_str = r.get("Shootings", "") or r.get("ShootingString", "")
        stats = parse_shooting_string(shooting_str)
        if stats["prone_accuracy"] is not None:
            prone_hits  += round(stats["prone_accuracy"] * 5)
            prone_total += 5
        if stats["standing_accuracy"] is not None:
            standing_hits  += round(stats["standing_accuracy"] * 5)
            standing_total += 5

    prone_accuracy    = prone_hits / prone_total if prone_total else 0.80
    standing_accuracy = standing_hits / standing_total if standing_total else 0.75

    # Vitesse ski : utilise le rang moyen comme proxy (les vrais temps ski
    # nécessitent get_analytic_results course par course — trop d'appels)
    # Score 0-1 : rang 1 = 1.0, rang 50 = 0.0
    ski_speed_score = max(0, 1 - (avg_rank_short or 25) / 50) if avg_rank_short else 0.5

    # Fatigue : malus si course dans les 2 derniers jours
    fatigue = 0.0
    if all_results:
        from datetime import datetime, timezone, timedelta
        last_date_str = all_results[0].get("StartTime", "")
        try:
            last_date = datetime.fromisoformat(last_date_str.replace("Z", "+00:00")).date()
            today = datetime.now(timezone.utc).date()
            days_ago = (today - last_date).days
            if days_ago <= 1:
                fatigue = 0.08  # -8% perf si course hier
            elif days_ago == 2:
                fatigue = 0.05  # -5% si avant-hier
        except Exception:
            pass

    return {
        "ibu_id":             ibu_id,
        "name":               all_results[0].get("Name", ibu_id) if all_results else ibu_id,
        "nat":                all_results[0].get("Nat", "") if all_results else "",
        "avg_rank_short":     avg_rank_short,
        "avg_rank_long":      avg_rank_long,
        "rank_std":           rank_std,
        "top3_rate":          top3_rate,
        "top10_rate":         top10_rate,
        "prone_accuracy":     prone_accuracy,
        "standing_accuracy":  standing_accuracy,
        "ski_speed_score":    ski_speed_score,
        "fatigue":            fatigue,
        "n_races":            len(all_results),
        "format_n_races":     len(format_results),
        "last_race_date":     all_results[0].get("StartTime", "")[:10] if all_results else "",
    }


def calc_rating(features: dict, race_format: str) -> float:
    """
    Calcule un rating continu [0-1] pour un athlète sur un format donné.
    Plus élevé = meilleur.

    Le rating est une combinaison pondérée des features normalisées.
    """
    w = WEIGHTS.get(race_format, WEIGHTS["SR"])

    # Composante ski : ski_speed_score déjà normalisé [0-1]
    ski = features.get("ski_speed_score", 0.5)

    # Composante tir couché [0-1]
    prone = features.get("prone_accuracy", 0.80)

    # Composante tir debout [0-1]
    standing = features.get("standing_accuracy", 0.75)

    # Composante forme : basée sur rang moyen normalisé + top3_rate
    avg_rank = features.get("avg_rank_short") or features.get("avg_rank_long") or 25
    rank_score = max(0, 1 - avg_rank / 50)  # rang 1 = 1.0, rang 50 = 0
    form_score = 0.6 * rank_score + 0.4 * features.get("top3_rate", 0)

    # Rating brut
    rating = (
        w["ski"]     * ski +
        w["prone"]   * prone +
        w["standing"]* standing +
        w["form"]    * form_score
    )

    # Malus fatigue
    rating *= (1 - features.get("fatigue", 0))

    # Bonus expérience : si peu de données, on réduit vers la moyenne
    n = features.get("format_n_races", 0)
    confidence = min(1.0, n / 10)  # pleine confiance à 10+ courses dans ce format
    rating = rating * confidence + 0.5 * (1 - confidence)

    return round(rating, 4)


def h2h_probability(rating_a: float, rating_b: float) -> float:
    """
    P(A finit devant B) via modèle logistique calibré.
    P = 1 / (1 + e^(-k * delta))
    """
    delta = rating_a - rating_b
    return round(1 / (1 + math.exp(-K_LOGISTIC * delta * 10)), 4)


def predict_h2h(ibu_id_a: str, ibu_id_b: str, race_format: str) -> Optional[dict]:
    """
    Prédit le H2H entre deux athlètes sur un format donné.

    Retourne :
    {
      "athlete_a": {...features},
      "athlete_b": {...features},
      "rating_a": float,
      "rating_b": float,
      "prob_a_wins": float,   # P(A devant B)
      "prob_b_wins": float,
      "favorite": "A" ou "B",
      "edge": float,          # différence de rating
    }
    """
    feat_a = build_athlete_features(ibu_id_a, race_format)
    feat_b = build_athlete_features(ibu_id_b, race_format)

    if not feat_a or not feat_b:
        log.warning(f"[H2H] Données manquantes pour {ibu_id_a} ou {ibu_id_b}")
        return None

    rating_a = calc_rating(feat_a, race_format)
    rating_b = calc_rating(feat_b, race_format)

    prob_a = h2h_probability(rating_a, rating_b)
    prob_b = 1 - prob_a

    return {
        "athlete_a":    feat_a,
        "athlete_b":    feat_b,
        "rating_a":     rating_a,
        "rating_b":     rating_b,
        "prob_a_wins":  prob_a,
        "prob_b_wins":  prob_b,
        "favorite":     "A" if prob_a > 0.5 else "B",
        "edge":         round(abs(rating_a - rating_b), 4),
        "race_format":  race_format,
    }


def simulate_race(athletes: list, race_format: str, n_simulations: int = 1_000_000) -> dict:
    """
    Monte Carlo : simule N fois la course et calcule P(Top 3) pour chaque athlète.

    athletes : liste de dicts {"ibu_id": str, "name": str, "rating": float}
    Retourne : {ibu_id: {"name": str, "p_win": float, "p_top3": float, "p_top5": float}}
    """
    n = len(athletes)
    if n == 0:
        return {}

    sigma = SIGMA.get(race_format, 1.2)
    ratings = [a["rating"] for a in athletes]

    counts = {a["ibu_id"]: {"win": 0, "top3": 0, "top5": 0} for a in athletes}

    # Pour optimiser : on simule en batch plutôt qu'un par un
    # Chaque simulation = scores = rating + bruit gaussien → classement
    sim_batch = max(1, n_simulations // 100)  # batches de 10k
    n_batches = n_simulations // sim_batch

    for _ in range(n_batches):
        # scores : matrice (sim_batch × n_athletes)
        batch_scores = [
            [r + random.gauss(0, sigma) for r in ratings]
            for _ in range(sim_batch)
        ]
        for scores in batch_scores:
            ranked = sorted(zip(scores, athletes), key=lambda x: -x[0])
            winner_id = ranked[0][1]["ibu_id"]
            counts[winner_id]["win"] += 1
            for i in range(min(3, len(ranked))):
                counts[ranked[i][1]["ibu_id"]]["top3"] += 1
            for i in range(min(5, len(ranked))):
                counts[ranked[i][1]["ibu_id"]]["top5"] += 1

    total = n_batches * sim_batch
    results = {}
    for a in athletes:
        aid = a["ibu_id"]
        results[aid] = {
            "name":   a["name"],
            "nat":    a.get("nat", ""),
            "rating": a["rating"],
            "p_win":  round(counts[aid]["win"]  / total, 4),
            "p_top3": round(counts[aid]["top3"] / total, 4),
            "p_top5": round(counts[aid]["top5"] / total, 4),
        }
    return results


def predict_podium(race_id: str, race_format: str) -> list:
    """
    Prédit le podium d'une course à partir de la liste des partants.
    Utilise les derniers résultats connus pour calculer les ratings.

    Retourne la liste des athlètes triée par P(Top 3) décroissant.
    """
    # Récupère les partants depuis les derniers résultats de cette course
    # (si la course n'a pas encore eu lieu, on prend la start list si disponible)
    from biathlon_client import get_results
    results = get_results(race_id)
    if not results:
        log.warning(f"[Podium] Aucun résultat pour {race_id}")
        return []

    athletes_data = []
    for r in results:
        ibu_id = r.get("IBU_ID", "")
        name   = r.get("Name", "")
        nat    = r.get("Nat", "")
        if not ibu_id:
            continue

        feat = build_athlete_features(ibu_id, race_format)
        if not feat:
            # Athlète sans historique → rating moyen
            athletes_data.append({"ibu_id": ibu_id, "name": name, "nat": nat, "rating": 0.45})
        else:
            rating = calc_rating(feat, race_format)
            athletes_data.append({"ibu_id": ibu_id, "name": name, "nat": nat, "rating": rating})

    simulation = simulate_race(athletes_data, race_format, n_simulations=200_000)

    # Tri par P(Top 3)
    sorted_results = sorted(simulation.values(), key=lambda x: -x["p_top3"])
    return sorted_results


def detect_h2h_value(prob_model: float, odd_bk: float, threshold: float = VALUE_THRESHOLD) -> dict:
    """
    Calcule la value d'un pari H2H.

    prob_model : probabilité calculée par notre modèle
    odd_bk     : cote décimale du bookmaker
    threshold  : seuil minimum de value (5% par défaut)

    Retourne :
    {
      "prob_model": float,
      "prob_implied": float,  # probabilité implicite bookmaker
      "odd_fair": float,      # cote juste selon le modèle
      "odd_bk": float,
      "value": float,         # edge en % (positif = value bet)
      "is_value": bool,
      "kelly": float,         # fraction Kelly recommandée
    }
    """
    prob_implied = 1 / odd_bk if odd_bk > 1 else 0
    value        = prob_model - prob_implied
    odd_fair     = round(1 / prob_model, 3) if prob_model > 0 else 0

    # Fraction Kelly = (prob * odd - 1) / (odd - 1)
    kelly = 0.0
    if odd_bk > 1 and prob_model > 0:
        kelly = (prob_model * odd_bk - 1) / (odd_bk - 1)
        kelly = max(0, round(kelly, 4))

    # Kelly conservateur : 1/4 du Kelly théorique
    kelly_conservative = round(kelly / 4, 4)

    return {
        "prob_model":        round(prob_model, 4),
        "prob_implied":      round(prob_implied, 4),
        "odd_fair":          odd_fair,
        "odd_bk":            odd_bk,
        "value":             round(value, 4),
        "value_pct":         round(value * 100, 2),
        "is_value":          value >= threshold,
        "kelly":             kelly,
        "kelly_conservative": kelly_conservative,
    }


def relay_team_rating(team_ibu_ids: list, race_format: str = "RL") -> float:
    """
    Rating d'une équipe de relais = moyenne des ratings des 4 athlètes.
    Les équipes avec 4 bons skieurs dominent même si tir moyen.
    """
    ratings = []
    for ibu_id in team_ibu_ids:
        feat = build_athlete_features(ibu_id, race_format)
        if feat:
            ratings.append(calc_rating(feat, race_format))
    return round(sum(ratings) / len(ratings), 4) if ratings else 0.5