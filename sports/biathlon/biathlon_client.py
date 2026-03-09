"""
biathlon_client.py
------------------
Client pour l'API biathlonresults.com.
Base URL : http://biathlonresults.com/modules/sportapi/api/

Endpoints :
  GET /Events?SeasonId=2425&Level=1          → liste des étapes CdM
  GET /Competitions?EventId=BT2425SWRLCP01   → courses d'une étape
  GET /Results?RaceId=BT2425SWRLCP01SMSP     → résultats d'une course
  GET /CupResults?CupId=BT2425SWRLCP__SMTS  → classement CdM général
  GET /Seasons                                → saisons disponibles
"""

import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)

IBU_BASE = "http://biathlonresults.com/modules/sportapi/api"

# Cache mémoire (TTL en secondes)
_cache: dict = {}

# Saisons
CURRENT_SEASON = "2526"
PREV_SEASON    = "2425"

# Formats de course
RACE_FORMATS = {
    "SP": "Sprint",
    "PU": "Poursuite",
    "SI": "Short Individuelle",
    "IN": "Individuelle",
    "MS": "Mass Start",
    "RL": "Relais",
    "MX": "Relais Mixte",
    "SR": "Single Mixed Relay",
}

# Level 1 = BMW IBU World Cup
WC_LEVEL = "1"


def _get(endpoint: str, params: dict = None, ttl: int = 3600) -> Optional[dict]:
    url = f"{IBU_BASE}/{endpoint}"
    cache_key = url + str(sorted((params or {}).items()))

    if cache_key in _cache:
        data, ts = _cache[cache_key]
        if time.time() - ts < ttl:
            return data

    try:
        resp = requests.get(url, params=params or {}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        _cache[cache_key] = (data, time.time())
        return data
    except Exception as e:
        log.warning(f"[IBU] {endpoint} : {e}")
        return None


def get_events(season: str = CURRENT_SEASON) -> list:
    """Retourne les étapes CdM de la saison."""
    data = _get("Events", {"SeasonId": season, "Level": WC_LEVEL}, ttl=3600)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Events", [])


def get_competitions(event_id: str) -> list:
    """Retourne les courses d'une étape (sprint, poursuite, etc.)."""
    data = _get("Competitions", {"EventId": event_id}, ttl=3600)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Competitions", [])


def get_results(race_id: str) -> list:
    """Résultats d'une course. Champs : Rank, IBUId, Name, Nat, ShortName."""
    data = _get("Results", {"RaceId": race_id}, ttl=86400)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Results", [])


def get_cup_results(cup_id: str) -> list:
    """Classement CdM. cup_id ex: BT2526SWRLCP__SMTS (hommes total score)."""
    data = _get("CupResults", {"CupId": cup_id}, ttl=3600)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Rows", [])


def get_cups(season: str = CURRENT_SEASON) -> list:
    """Liste des coupes disponibles pour la saison."""
    data = _get("Cups", {"SeasonId": season}, ttl=3600)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Cups", [])


def get_cup_standings(season: str = CURRENT_SEASON, gender: str = "M") -> list:
    """
    Classement général CdM.
    gender: "M" (hommes) ou "W" (femmes).
    Champs retournés : Rank, IBUId, Name, Nat, Score.
    """
    suffix = "SW" if gender == "W" else "SM"
    cup_id = f"BT{season}SWRLCP__{suffix}TS"
    rows = get_cup_results(cup_id)
    log.info(f"[IBU] CupResults {cup_id}: {len(rows)} rows")
    if rows and len(rows) > 0 and isinstance(rows[0], dict):
        log.info(f"[IBU] CupResults[0] keys: {list(rows[0].keys())}")
    return rows


def get_upcoming_races(days_ahead: int = 10) -> list:
    """
    Retourne les prochaines courses CdM dans les N jours.
    Structure retournée : race_id, description, location, date, format, format_name, gender
    """
    today   = datetime.now(timezone.utc).date()
    cutoff  = today + timedelta(days=days_ahead)
    upcoming = []

    for season in [CURRENT_SEASON, PREV_SEASON]:
        events = get_events(season)
        log.info(f"[IBU] Events saison {season}: {len(events)}")
        if not events:
            continue

        for event in events:
            event_id  = event.get("EventId", "")
            location  = event.get("ShortDescription", event.get("Location", ""))
            start_raw = event.get("StartDate", "")
            end_raw   = event.get("EndDate", "")

            if not event_id:
                continue

            # Vérifier si l'événement est dans la fenêtre
            try:
                end_date = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).date()
                if end_date < today:
                    continue
                start_date = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).date()
                if start_date > cutoff:
                    continue
            except Exception:
                continue

            # Récupérer les courses de cet événement
            races = get_competitions(event_id)
            for r in races:
                race_id  = r.get("RaceId", "")
                desc     = r.get("ShortDescription", r.get("Description", ""))
                start_r  = r.get("StartTime", "")
                fmt_code = r.get("RaceTypeId", r.get("RaceType", "SP"))
                status   = r.get("Status", "")

                # Ignorer les courses déjà officielles
                if status == "Official":
                    continue

                if not race_id:
                    continue

                try:
                    race_date = datetime.fromisoformat(start_r.replace("Z", "+00:00")).date()
                    if not (today <= race_date <= cutoff):
                        continue
                except Exception:
                    race_date = start_date

                # Détection genre depuis le RaceId et la description
                is_women = (
                    "Women" in desc
                    or "SW" in race_id.split("SWRLCP")[-1][:3] if "SWRLCP" in race_id else False
                    or desc.startswith("W")
                )
                gender = "W" if is_women else "M"

                upcoming.append({
                    "race_id":     race_id,
                    "description": desc,
                    "location":    location,
                    "date":        race_date.isoformat(),
                    "format":      fmt_code,
                    "format_name": RACE_FORMATS.get(fmt_code, fmt_code),
                    "gender":      gender,
                    "event_id":    event_id,
                })

        if upcoming:
            break  # Ne pas chercher la saison précédente si on a trouvé

    return sorted(upcoming, key=lambda x: x["date"])


def get_recent_race_ids(gender: str = "M", fmt_code: str = "SP",
                        season: str = PREV_SEASON, n: int = 5) -> list:
    """
    Retourne les RaceId des N dernières courses officielles du format/genre donné.
    Utilisé pour récupérer les athlètes performants récemment.
    """
    events = get_events(season)
    race_ids = []
    for event in events:
        event_id = event.get("EventId", "")
        if not event_id:
            continue
        races = get_competitions(event_id)
        for r in races:
            race_id  = r.get("RaceId", "")
            desc     = r.get("ShortDescription", "")
            fmt      = r.get("RaceTypeId", r.get("RaceType", ""))
            status   = r.get("Status", "")

            if status != "Official":
                continue
            if fmt != fmt_code:
                continue

            is_women = "Women" in desc or ("SWRLCP" in race_id and "SW" in race_id.split("SWRLCP")[-1][:3])
            r_gender = "W" if is_women else "M"
            if r_gender != gender:
                continue

            race_ids.append((r.get("StartTime",""), race_id))

    # Trier par date décroissante et prendre les N derniers
    race_ids.sort(reverse=True)
    return [rid for _, rid in race_ids[:n]]


def clear_cache():
    global _cache
    _cache = {}
    log.info("[IBU] Cache vidé.")


def get_athlete_results(ibu_id: str, season: str = None) -> list:
    """Tous les résultats d'un athlète. Champs: RaceId, Comp, Season, Place, Rank."""
    params = {"IBUId": ibu_id}
    if season:
        params["SeasonId"] = season
    data = _get("Results", params, ttl=3600)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Results", [])


def get_analytic_results(race_id: str) -> list:
    """Résultats analytiques (temps ski, tir, etc.)."""
    data = _get("AnalyticResults", {"RaceId": race_id}, ttl=86400)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Results", [])


def time_to_seconds(time_str: str):
    """Convertit '00:23:45.2' en secondes."""
    if not time_str:
        return None
    try:
        s = time_str.lstrip("+").strip()
        parts = s.split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        return float(s)
    except Exception:
        return None


def parse_shooting_string(shootings_str: str) -> dict:
    """Parse la chaîne de tirs IBU : '1 0 1 0 1 1 0 1 0 1'."""
    if not shootings_str:
        return {"total_shots": 0, "hits": 0, "misses": 0, "accuracy": None}
    normalized = shootings_str.replace(" ", "").replace("/", "")
    shots = [int(c) for c in normalized if c in "01"]
    total = len(shots)
    hits  = sum(shots)
    return {
        "total_shots": total,
        "hits":        hits,
        "misses":      total - hits,
        "accuracy":    hits / total if total else None,
    }