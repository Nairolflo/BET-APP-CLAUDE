"""
biathlon_odds.py
----------------
Récupération des cotes biathlon depuis The Odds API.
Détection automatique des value bets H2H et podium.

Sports keys Odds API pour le biathlon :
  "biathlon"  (marchés H2H principalement sur grandes compétitions)

Note : les cotes biathlon sont disponibles surtout sur les grandes
épreuves (JO, Championnats du Monde, finales CdM).
Sur les étapes CdM régulières, les marchés sont plus limités.
"""

import os
import time
import logging
import requests
from typing import Optional
from biathlon_model import detect_h2h_value, VALUE_THRESHOLD

log = logging.getLogger(__name__)

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
BIATHLON_SPORT_KEY = "biathlon"

# Cache cotes 2h (les cotes biathlon bougent peu avant la course)
_odds_cache: dict = {}
ODDS_CACHE_TTL = 2 * 3600


def _get_odds_api(endpoint: str, params: dict = None) -> Optional[dict | list]:
    """Appel The Odds API avec gestion quota."""
    api_key = os.getenv("ODDS_API_KEY", "")
    if not api_key:
        log.warning("[Biathlon Odds] ODDS_API_KEY manquant")
        return None

    url = f"{ODDS_API_BASE}/{endpoint}"
    all_params = {"apiKey": api_key, **(params or {})}

    cache_key = url + str(sorted(all_params.items()))
    if cache_key in _odds_cache:
        data, ts = _odds_cache[cache_key]
        if time.time() - ts < ODDS_CACHE_TTL:
            return data

    try:
        resp = requests.get(url, params=all_params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        _odds_cache[cache_key] = (data, time.time())
        log.info(f"[Biathlon Odds] Restantes: {resp.headers.get('x-requests-remaining', '?')}")
        return data
    except Exception as e:
        log.warning(f"[Biathlon Odds] {endpoint}: {e}")
        return None


def get_biathlon_events() -> list:
    """
    Retourne les événements biathlon disponibles avec cotes.
    """
    data = _get_odds_api(
        f"sports/{BIATHLON_SPORT_KEY}/odds",
        {
            "regions":    "eu",
            "markets":    "h2h,outrights",
            "oddsFormat": "decimal",
        }
    )
    return data or []


def parse_h2h_odds(events: list) -> list:
    """
    Extrait les marchés H2H (face à face) des événements.

    Retourne :
    [
      {
        "event_id": str,
        "event": "Jacquelin vs Boe",
        "sport": "biathlon",
        "commence_time": str,
        "markets": {
          "athlete_a": {"name": str, "best_odd": float, "bookmaker": str},
          "athlete_b": {"name": str, "best_odd": float, "bookmaker": str},
        }
      }, ...
    ]
    """
    h2h_markets = []

    for event in events:
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        if not home or not away:
            continue

        best_odds = {home: 0.0, away: 0.0}
        best_bk   = {home: "", away: ""}

        for bk in event.get("bookmakers", []):
            bk_name = bk.get("title", bk.get("key", ""))
            for market in bk.get("markets", []):
                if market.get("key") != "h2h":
                    continue
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name", "")
                    odd  = outcome.get("price", 0)
                    if name in best_odds and odd > best_odds[name]:
                        best_odds[name] = odd
                        best_bk[name]   = bk_name

        if best_odds[home] > 1 and best_odds[away] > 1:
            h2h_markets.append({
                "event_id":      event.get("id", ""),
                "event":         f"{home} vs {away}",
                "commence_time": event.get("commence_time", ""),
                "athlete_a":     {"name": home, "best_odd": best_odds[home], "bookmaker": best_bk[home]},
                "athlete_b":     {"name": away, "best_odd": best_odds[away], "bookmaker": best_bk[away]},
            })

    return h2h_markets


def parse_outright_odds(events: list) -> list:
    """
    Extrait les marchés Outright (vainqueur/podium) des événements.
    """
    outrights = []
    for event in events:
        for bk in event.get("bookmakers", []):
            for market in bk.get("markets", []):
                if market.get("key") == "outrights":
                    for outcome in market.get("outcomes", []):
                        outrights.append({
                            "event_id":  event.get("id", ""),
                            "event":     event.get("sport_title", ""),
                            "athlete":   outcome.get("name", ""),
                            "odd":       outcome.get("price", 0),
                            "bookmaker": bk.get("title", ""),
                        })
    return outrights


def find_value_bets(h2h_markets: list, h2h_predictions: dict) -> list:
    """
    Compare les cotes bookmakers aux probabilités du modèle.
    Retourne les value bets avec edge ≥ VALUE_THRESHOLD.

    h2h_predictions : {(nom_a, nom_b): {"prob_a": float, "prob_b": float}}
    (clés normalisées en minuscules)
    """
    value_bets = []

    for market in h2h_markets:
        name_a = market["athlete_a"]["name"]
        name_b = market["athlete_b"]["name"]
        odd_a  = market["athlete_a"]["best_odd"]
        odd_b  = market["athlete_b"]["best_odd"]

        # Cherche la prédiction dans le dict (clé normalisée)
        key = (name_a.lower(), name_b.lower())
        pred = h2h_predictions.get(key) or h2h_predictions.get((name_b.lower(), name_a.lower()))
        if not pred:
            continue

        prob_a = pred.get("prob_a", 0.5)
        prob_b = 1 - prob_a

        # Check value pour A
        value_a = detect_h2h_value(prob_a, odd_a)
        if value_a["is_value"]:
            value_bets.append({
                "type":      "H2H",
                "pick":      name_a,
                "opponent":  name_b,
                "odd":       odd_a,
                "bookmaker": market["athlete_a"]["bookmaker"],
                "value_pct": value_a["value_pct"],
                "prob_model": value_a["prob_model"],
                "prob_implied": value_a["prob_implied"],
                "odd_fair":  value_a["odd_fair"],
                "kelly_conservative": value_a["kelly_conservative"],
                "event":     market["event"],
                "commence_time": market["commence_time"],
            })

        # Check value pour B
        value_b = detect_h2h_value(prob_b, odd_b)
        if value_b["is_value"]:
            value_bets.append({
                "type":      "H2H",
                "pick":      name_b,
                "opponent":  name_a,
                "odd":       odd_b,
                "bookmaker": market["athlete_b"]["bookmaker"],
                "value_pct": value_b["value_pct"],
                "prob_model": value_b["prob_model"],
                "prob_implied": value_b["prob_implied"],
                "odd_fair":  value_b["odd_fair"],
                "kelly_conservative": value_b["kelly_conservative"],
                "event":     market["event"],
                "commence_time": market["commence_time"],
            })

    return sorted(value_bets, key=lambda x: -x["value_pct"])