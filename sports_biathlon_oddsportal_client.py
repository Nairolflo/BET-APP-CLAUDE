"""
sports/biathlon/oddsportal_client.py
Récupère l'historique des cotes H2H biathlon sur OddsPortal.
OddsPortal charge ses données via une API JSON interne — pas besoin de Selenium.
"""
import re
import time
import logging
import requests
from datetime import datetime, timezone

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer":         "https://www.oddsportal.com/",
    "x-requested-with": "XMLHttpRequest",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Bookmakers à surveiller (IDs OddsPortal)
TARGET_BOOKMAKERS = {
    "Winamax": ["winamax", "winamax-fr"],
    "Betclic":  ["betclic", "betclic-fr"],
    "Unibet":   ["unibet", "unibet-fr"],
}

_cache: dict = {}
_TTL = 1800  # 30 min


def _get_json(url: str, ttl: int = _TTL) -> dict | list | None:
    now = time.time()
    if url in _cache:
        data, ts = _cache[url]
        if now - ts < ttl:
            return data
    try:
        time.sleep(0.5)  # politesse
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        _cache[url] = (data, now)
        return data
    except Exception as e:
        log.warning(f"[OddsPortal] {url}: {e}")
        return None


def _get_html(url: str, ttl: int = _TTL) -> str | None:
    now = time.time()
    cache_key = f"html_{url}"
    if cache_key in _cache:
        data, ts = _cache[cache_key]
        if now - ts < ttl:
            return data
    try:
        time.sleep(0.5)
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        _cache[cache_key] = (r.text, now)
        return r.text
    except Exception as e:
        log.warning(f"[OddsPortal] HTML {url}: {e}")
        return None


def get_biathlon_h2h_history(n_last: int = 5) -> list:
    """
    Scrape OddsPortal pour récupérer les cotes H2H des dernières
    courses de biathlon (Winamax, Betclic, Unibet).

    Retourne liste de dicts :
    {
        race_name, race_date, format,
        h2h: [{athlete_a, athlete_b, odd_a_winamax, odd_b_winamax,
                odd_a_betclic, odd_b_betclic}]
    }
    """
    # OddsPortal URL biathlon CdM
    base_url = "https://www.oddsportal.com/biathlon/world/ibu-world-cup/"

    html = _get_html(base_url)
    if not html:
        log.warning("[OddsPortal] Impossible de charger la page biathlon")
        return []

    # Chercher l'ID sport/tournoi dans le JS de la page
    # OddsPortal injecte les données dans window.pageProps ou un script JSON
    sport_id_match = re.search(r'"sportId"\s*:\s*(\d+)', html)
    tournament_match = re.search(r'"tournamentId"\s*:\s*(\d+)', html)

    if sport_id_match and tournament_match:
        sport_id      = sport_id_match.group(1)
        tournament_id = tournament_match.group(1)
        log.info(f"[OddsPortal] sportId={sport_id} tournamentId={tournament_id}")

        # Appel API interne OddsPortal
        api_url = (f"https://www.oddsportal.com/api/v2/tournaments/{tournament_id}"
                   f"/events/?sport={sport_id}&limit={n_last}&offset=0&status=2")
        data = _get_json(api_url)
        if data:
            return _parse_events(data, n_last)

    # Fallback : parser le HTML directement
    log.info("[OddsPortal] Fallback HTML parsing")
    return _parse_html_results(html, n_last)


def _parse_events(data: dict, n_last: int) -> list:
    """Parse la réponse API OddsPortal."""
    events = data.get("data", {}).get("rows", data.get("rows", []))
    results = []

    for evt in events[:n_last]:
        race_name = evt.get("name", evt.get("home-name", ""))
        race_date = evt.get("date-start-timestamp", "")
        if race_date:
            try:
                race_date = datetime.fromtimestamp(int(race_date), tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                pass

        # Chercher les odds dans l'événement
        odds = evt.get("odds", {})
        h2h_list = []

        # Structure odds : {bookmaker_id: [odd_home, odd_draw, odd_away]}
        winamax_odds = None
        betclic_odds = None
        for bk_key, bk_odds in odds.items():
            bk_lower = str(bk_key).lower()
            if "winamax" in bk_lower and isinstance(bk_odds, list) and len(bk_odds) >= 2:
                winamax_odds = bk_odds
            if "betclic" in bk_lower and isinstance(bk_odds, list) and len(bk_odds) >= 2:
                betclic_odds = bk_odds

        if winamax_odds or betclic_odds:
            h2h_list.append({
                "athlete_a":      evt.get("home-name", "Athlète A"),
                "athlete_b":      evt.get("away-name", "Athlète B"),
                "odd_a_winamax":  winamax_odds[0] if winamax_odds else None,
                "odd_b_winamax":  winamax_odds[1] if winamax_odds else None,
                "odd_a_betclic":  betclic_odds[0] if betclic_odds else None,
                "odd_b_betclic":  betclic_odds[1] if betclic_odds else None,
            })

        if h2h_list:
            results.append({
                "race_name": race_name,
                "race_date": race_date,
                "h2h":       h2h_list,
            })

    return results


def _parse_html_results(html: str, n_last: int) -> list:
    """
    Fallback : extraction des liens de courses depuis le HTML OddsPortal.
    Cherche les URLs /biathlon/world/ibu-world-cup/[course]/ et scrape chacune.
    """
    # Trouver les URLs de courses récentes
    race_urls = re.findall(
        r'href="(/biathlon/world/ibu-world-cup/[^"]+/)"',
        html
    )
    # Dédoublonner et prendre les N dernières
    seen = set()
    unique_urls = []
    for u in race_urls:
        if u not in seen and "results" not in u:
            seen.add(u)
            unique_urls.append(u)

    log.info(f"[OddsPortal] {len(unique_urls)} courses trouvées")
    results = []

    for url_path in unique_urls[:n_last]:
        full_url = f"https://www.oddsportal.com{url_path}"
        race_html = _get_html(full_url, ttl=3600)
        if not race_html:
            continue

        parsed = _parse_race_page(race_html, url_path)
        if parsed:
            results.append(parsed)

    return results


def _parse_race_page(html: str, url_path: str) -> dict | None:
    """Parse une page de course OddsPortal pour extraire les cotes H2H."""
    # Nom de la course depuis l'URL
    race_name = url_path.strip("/").split("/")[-1].replace("-", " ").title()

    # Chercher les données JSON injectées dans la page
    json_match = re.search(r'window\.__NEXT_DATA__\s*=\s*(\{.+?\});?\s*</script>', html, re.DOTALL)
    if not json_match:
        return None

    try:
        import json
        page_data = json.loads(json_match.group(1))
        props = page_data.get("props", {}).get("pageProps", {})

        # Extraire les matchups H2H
        event = props.get("event", {})
        odds  = props.get("odds", {})

        race_date = event.get("startDate", "")[:10]
        h2h_list  = []

        for matchup_id, matchup_odds in odds.items():
            participants = matchup_odds.get("participants", [])
            if len(participants) < 2:
                continue

            name_a = participants[0].get("name", "")
            name_b = participants[1].get("name", "")
            bk_odds = matchup_odds.get("bookmakers", {})

            odd_a_win = odd_b_win = odd_a_bet = odd_b_bet = None
            for bk_name, bk_data in bk_odds.items():
                bk_lower = bk_name.lower()
                o = bk_data.get("odds", [])
                if len(o) >= 2:
                    if "winamax" in bk_lower:
                        odd_a_win, odd_b_win = o[0], o[1]
                    elif "betclic" in bk_lower:
                        odd_a_bet, odd_b_bet = o[0], o[1]

            if any([odd_a_win, odd_a_bet]):
                h2h_list.append({
                    "athlete_a":     name_a,
                    "athlete_b":     name_b,
                    "odd_a_winamax": odd_a_win,
                    "odd_b_winamax": odd_b_win,
                    "odd_a_betclic": odd_a_bet,
                    "odd_b_betclic": odd_b_bet,
                })

        if not h2h_list:
            return None

        return {"race_name": race_name, "race_date": race_date, "h2h": h2h_list}

    except Exception as e:
        log.warning(f"[OddsPortal] Parse {url_path}: {e}")
        return None


def get_avg_h2h_odds(n_last: int = 5) -> dict:
    """
    Calcule les cotes H2H moyennes des N dernières courses.
    Retourne {(athlete_a, athlete_b): {avg_odd_a, avg_odd_b, bookmaker, n_samples}}
    """
    history = get_biathlon_h2h_history(n_last)
    if not history:
        return {}

    accumulated: dict = {}
    for race in history:
        for h2h in race.get("h2h", []):
            key = tuple(sorted([h2h["athlete_a"], h2h["athlete_b"]]))
            if key not in accumulated:
                accumulated[key] = {
                    "name_a":   h2h["athlete_a"],
                    "name_b":   h2h["athlete_b"],
                    "odds_a_w": [],
                    "odds_b_w": [],
                    "odds_a_b": [],
                    "odds_b_b": [],
                }
            a = accumulated[key]
            if h2h.get("odd_a_winamax"): a["odds_a_w"].append(h2h["odd_a_winamax"])
            if h2h.get("odd_b_winamax"): a["odds_b_w"].append(h2h["odd_b_winamax"])
            if h2h.get("odd_a_betclic"): a["odds_a_b"].append(h2h["odd_a_betclic"])
            if h2h.get("odd_b_betclic"): a["odds_b_b"].append(h2h["odd_b_betclic"])

    result = {}
    for key, a in accumulated.items():
        avg_a = (sum(a["odds_a_w"])/len(a["odds_a_w"]) if a["odds_a_w"] else
                 sum(a["odds_a_b"])/len(a["odds_a_b"]) if a["odds_a_b"] else None)
        avg_b = (sum(a["odds_b_w"])/len(a["odds_b_w"]) if a["odds_b_w"] else
                 sum(a["odds_b_b"])/len(a["odds_b_b"]) if a["odds_b_b"] else None)
        bk    = "Winamax" if a["odds_a_w"] else "Betclic" if a["odds_a_b"] else None
        n     = max(len(a["odds_a_w"]), len(a["odds_a_b"]))
        if avg_a and avg_b:
            result[key] = {
                "name_a":   a["name_a"],
                "name_b":   a["name_b"],
                "avg_odd_a": round(avg_a, 2),
                "avg_odd_b": round(avg_b, 2),
                "bookmaker": bk,
                "n_samples": n,
            }

    log.info(f"[OddsPortal] {len(result)} paires H2H avec cotes historiques")
    return result
