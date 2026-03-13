"""
winamax_ws_client.py
--------------------
Client WebSocket Socket.IO pour récupérer les cotes Winamax biathlon en temps réel.

Winamax utilise : wss://sports-eu-west-3.winamax.fr/uof-sports-server/socket.io/
Protocole       : Engine.IO v3 + Socket.IO v3 (WebSocket pur)
Dépendance      : pip install "python-socketio[asyncio_client]" aiohttp
"""

import asyncio
import json
import re
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ─── Cache des cotes ─────────────────────────────────────────────────────────

_ws_odds: dict = {}   # { "Athlete A vs Athlete B": {"home": 1.55, "away": 2.00} }
_connected = False

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _is_biathlon(txt: str) -> bool:
    return bool(re.search(r'biathlon|biathl', txt, re.IGNORECASE))

def _extract_matches(data) -> list:
    """
    Tente d'extraire des matchs H2H depuis un payload Socket.IO Winamax.
    Structure observée : liste de matches avec competitors[] et odds[].
    """
    matches = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("matches", data.get("events", data.get("competitions", [])))
    else:
        return matches

    for item in items:
        if not isinstance(item, dict):
            continue
        competitors = item.get("competitors", item.get("teams", []))
        odds_list   = item.get("odds", item.get("selections", []))
        name        = item.get("name", item.get("label", ""))

        if not competitors or not odds_list:
            continue

        try:
            home = competitors[0].get("name", "")
            away = competitors[1].get("name", "") if len(competitors) > 1 else ""
            h_odd = float(odds_list[0].get("price", odds_list[0].get("odds", 0)))
            a_odd = float(odds_list[1].get("price", odds_list[1].get("odds", 0))) if len(odds_list) > 1 else 0

            if home and away and h_odd > 1:
                key = f"{home} vs {away}"
                matches.append({"label": key, "home": home, "away": away,
                                 "home_odd": h_odd, "away_odd": a_odd,
                                 "raw_name": name})
        except (IndexError, TypeError, ValueError):
            continue

    return matches


# ─── Client Socket.IO ────────────────────────────────────────────────────────

async def _run_client(timeout: int = 20):
    global _connected, _ws_odds

    try:
        import socketio
    except ImportError:
        log.error("[Winamax WS] python-socketio non installé. "
                  "pip install 'python-socketio[asyncio_client]' aiohttp")
        return

    sio = socketio.AsyncClient(ssl_verify=False, logger=False, engineio_logger=False)

    @sio.event
    async def connect():
        global _connected
        _connected = True
        log.info("[Winamax WS] Connecté")

    @sio.event
    async def disconnect():
        global _connected
        _connected = False
        log.info("[Winamax WS] Déconnecté")

    @sio.on("*")
    async def on_any(event, data):
        txt = json.dumps(data) if not isinstance(data, str) else data
        if _is_biathlon(txt):
            log.info(f"[Winamax WS] Event biathlon '{event}' ({len(txt)} chars)")
            matches = _extract_matches(data)
            for m in matches:
                _ws_odds[m["label"]] = m
                log.info(f"  → {m['label']} : {m['home_odd']} / {m['away_odd']}")

    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/121 Safari/537.36"),
        "Origin":  "https://www.winamax.fr",
        "Referer": "https://www.winamax.fr/paris-sportifs/sports/biathlon",
    }

    try:
        await sio.connect(
            "https://sports-eu-west-3.winamax.fr",
            socketio_path="/uof-sports-server/socket.io/",
            headers=headers,
            transports=["websocket"],
            wait_timeout=10,
        )
        await asyncio.sleep(timeout)
        await sio.disconnect()
    except Exception as e:
        log.warning(f"[Winamax WS] Connexion échouée : {e}")


# ─── API publique ─────────────────────────────────────────────────────────────

def fetch_biathlon_odds(timeout: int = 20) -> dict:
    """
    Lance le client WS, attend `timeout` secondes, retourne les cotes biathlon.
    Retour : { "Athlete A vs Athlete B": {"home": 1.55, "away": 2.00, ...}, ... }
    """
    global _ws_odds
    _ws_odds = {}

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Dans un contexte Flask/Thread — crée un nouveau loop dans un thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, _run_client(timeout))
                future.result(timeout=timeout + 5)
        else:
            loop.run_until_complete(_run_client(timeout))
    except Exception as e:
        log.warning(f"[Winamax WS] fetch_biathlon_odds: {e}")

    log.info(f"[Winamax WS] {len(_ws_odds)} H2H biathlon récupérés")
    return dict(_ws_odds)


def get_winamax_odd_for(athlete_a: str, athlete_b: str) -> Optional[dict]:
    """
    Cherche dans le cache WS les cotes pour un duel donné.
    Matching souple (lastname).
    """
    def lastname(n): return n.split()[-1].lower()
    la, lb = lastname(athlete_a), lastname(athlete_b)

    for key, val in _ws_odds.items():
        kla = lastname(val.get("home", ""))
        klb = lastname(val.get("away", ""))
        if (la in kla or kla in la) and (lb in klb or klb in lb):
            return val
        if (lb in kla or kla in lb) and (la in klb or klb in la):
            # inversé
            return {"home": val["away_odd"], "away": val["home_odd"],
                    "home_odd": val["away_odd"], "away_odd": val["home_odd"],
                    "label": f"{athlete_b} vs {athlete_a}"}
    return None
