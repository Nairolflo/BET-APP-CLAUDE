"""
sports/biathlon/pinnacle_client.py
Récupère les cotes H2H biathlon depuis l'API guest Pinnacle (sans compte).
Endpoints : guest.api.arcadia.pinnacle.com
"""
import requests
import logging
import time

log = logging.getLogger(__name__)

BASE     = "https://guest.api.arcadia.pinnacle.com/0.1"
HEADERS  = {
    "x-api-key": "CmX2KcMrXuFmNg6YFbmTxE0y9CblE4Ql",   # clé publique guest connue
    "Accept":    "application/json",
    "Origin":    "https://www.pinnacle.com",
    "Referer":   "https://www.pinnacle.com/",
}

# sportId biathlon Pinnacle (à confirmer via /sports)
BIATHLON_SPORT_ID = 20  # Winter Sports — biathlon dedans

_cache: dict = {}
_TTL = 300  # 5 min


def _get(url: str, ttl: int = _TTL) -> dict | list | None:
    now = time.time()
    if url in _cache:
        data, ts = _cache[url]
        if now - ts < ttl:
            return data
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        _cache[url] = (data, now)
        return data
    except Exception as e:
        log.warning(f"[Pinnacle] {url}: {e}")
        return None


def get_biathlon_leagues() -> list:
    """Retourne les ligues biathlon disponibles chez Pinnacle."""
    url  = f"{BASE}/sports/{BIATHLON_SPORT_ID}/leagues?all=false"
    data = _get(url)
    if not data:
        # Fallback : essayer Winter Sports ID alternatifs
        for sport_id in [20, 19, 21, 33]:
            url  = f"{BASE}/sports/{sport_id}/leagues?all=false"
            data = _get(url, ttl=60)
            if data:
                # Chercher "biathlon" dans les noms
                biathlons = [l for l in data
                             if "biathlon" in l.get("name", "").lower()]
                if biathlons:
                    log.info(f"[Pinnacle] Biathlon trouvé sportId={sport_id}: {[l['name'] for l in biathlons]}")
                    return biathlons
        return []
    return [l for l in data if "biathlon" in l.get("name", "").lower()]


def get_matchups(league_id: int) -> list:
    """Retourne les matchups (H2H) d'une ligue."""
    url  = f"{BASE}/leagues/{league_id}/matchups"
    data = _get(url)
    return data if isinstance(data, list) else []


def get_straight_odds(league_id: int) -> list:
    """Retourne les cotes straight (H2H) d'une ligue."""
    url  = f"{BASE}/leagues/{league_id}/markets/straight"
    data = _get(url)
    return data if isinstance(data, list) else []


def get_h2h_odds() -> list:
    """
    Point d'entrée principal.
    Retourne liste de matchups H2H biathlon avec cotes :
    [{
        "matchup_id": int,
        "name": "Athlete A vs Athlete B",
        "athlete_a": str,
        "athlete_b": str,
        "odd_a": float,
        "odd_b": float,
        "start_time": str,
        "league": str,
    }]
    """
    leagues = get_biathlon_leagues()
    if not leagues:
        log.warning("[Pinnacle] Aucune ligue biathlon trouvée")
        return []

    results = []
    for league in leagues:
        league_id   = league.get("id")
        league_name = league.get("name", "")
        if not league_id:
            continue

        matchups     = get_matchups(league_id)
        straight     = get_straight_odds(league_id)

        # Index odds par matchupId + side
        odds_index = {}  # matchupId → {home_price, away_price}
        for mkt in straight:
            mid  = mkt.get("matchupId")
            side = mkt.get("side")  # "home" ou "away"
            price = mkt.get("price")
            if mid and side and price:
                if mid not in odds_index:
                    odds_index[mid] = {}
                odds_index[mid][side] = price

        for m in matchups:
            mid   = m.get("id")
            parts = m.get("participants", [])
            if len(parts) < 2:
                continue

            name_a = parts[0].get("name", "")
            name_b = parts[1].get("name", "")
            ods    = odds_index.get(mid, {})
            odd_a  = ods.get("home") or ods.get("over")
            odd_b  = ods.get("away") or ods.get("under")

            if not odd_a or not odd_b:
                continue

            # Convertir américain → décimal si nécessaire
            def to_decimal(o):
                if o is None:
                    return None
                o = float(o)
                if o > 0:
                    return round(o / 100 + 1, 3)
                elif o < 0:
                    return round(100 / abs(o) + 1, 3)
                return o  # déjà décimal si > 1

            # Pinnacle guest retourne en décimal directement
            odd_a_dec = odd_a if odd_a > 1.5 else to_decimal(odd_a)
            odd_b_dec = odd_b if odd_b > 1.5 else to_decimal(odd_b)

            results.append({
                "matchup_id": mid,
                "name":       f"{name_a} vs {name_b}",
                "athlete_a":  name_a,
                "athlete_b":  name_b,
                "odd_a":      round(float(odd_a_dec), 2),
                "odd_b":      round(float(odd_b_dec), 2),
                "start_time": m.get("startTime", ""),
                "league":     league_name,
            })

    log.info(f"[Pinnacle] {len(results)} H2H biathlon trouvés")
    return results
