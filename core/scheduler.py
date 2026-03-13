"""
core/scheduler.py — Scheduler global Le Loup de Wall Bet

- Un seul polling Telegram
- Menus interactifs avec boutons inline
- Jobs APScheduler foot + biathlon
- Ajout d'un nouveau sport = 10 lignes
"""

import os
import sys
import logging
import threading
import time
import requests
from datetime import datetime, timezone

log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")


# ─────────────────────────────────────────────
# DISPATCH CALLBACKS (boutons inline)
# ─────────────────────────────────────────────

def handle_callback(callback_query: dict):
    """Route les callbacks des boutons inline vers les bons handlers."""
    from core.telegram import (
        answer_callback, send_menu_principal, send_menu_foot, send_menu_biathlon
    )

    cid      = callback_query.get("id", "")
    data     = callback_query.get("data", "")
    log.info(f"[Callback] data={repr(data)}")
    chat_id  = str(callback_query.get("message", {}).get("chat", {}).get("id", ""))
    msg_id   = callback_query.get("message", {}).get("message_id")

    answer_callback(cid)

    # ── Menus ──
    if data == "menu_main":
        send_menu_principal()
    elif data == "menu_foot":
        send_menu_foot()
    elif data == "menu_biathlon":
        send_menu_biathlon()

    # ── Football ──
    elif data == "foot_bets":
        from sports.football.handlers import handle_bets
        threading.Thread(target=handle_bets, daemon=True).start()
    elif data == "foot_today":
        from sports.football.handlers import handle_today
        threading.Thread(target=handle_today, daemon=True).start()
    elif data == "foot_stats":
        from sports.football.handlers import handle_stats
        threading.Thread(target=handle_stats, daemon=True).start()
    elif data == "foot_pourcent":
        from sports.football.handlers import handle_pourcent
        threading.Thread(target=handle_pourcent, daemon=True).start()
    elif data == "foot_run":
        from sports.football.handlers import handle_run
        threading.Thread(target=handle_run, daemon=True).start()
    elif data == "foot_results":
        from sports.football.handlers import handle_results
        threading.Thread(target=handle_results, daemon=True).start()
    elif data == "foot_refresh":
        from sports.football.handlers import handle_refresh
        threading.Thread(target=handle_refresh, daemon=True).start()
    elif data == "foot_api":
        from sports.football.handlers import handle_api
        threading.Thread(target=handle_api, daemon=True).start()
    elif data == "foot_h2h":
        from sports.football.handlers import handle_h2h
        threading.Thread(target=handle_h2h, daemon=True).start()
    elif data == "foot_refreshh2h":
        from sports.football.handlers import handle_refresh_h2h
        threading.Thread(target=handle_refresh_h2h, daemon=True).start()
    elif data == "foot_reset":
        from sports.football.handlers import handle_reset
        threading.Thread(target=handle_reset, daemon=True).start()

    # ── Biathlon ──
    elif data == "biat_status":
        from sports.biathlon.handlers import handle_status
        threading.Thread(target=handle_status, daemon=True).start()
    elif data == "biat_run":
        from sports.biathlon.handlers import handle_run
        threading.Thread(target=handle_run, daemon=True).start()
    elif data == "biat_results":
        from sports.biathlon.handlers import handle_results
        threading.Thread(target=handle_results, daemon=True).start()
    elif data == "biat_stats":
        from sports.biathlon.handlers import handle_stats
        threading.Thread(target=handle_stats, daemon=True).start()
    elif data == "biat_h2h_menu":
        from sports.biathlon.handlers import handle_h2h_menu
        threading.Thread(target=handle_h2h_menu, daemon=True).start()
    elif data.startswith("biat_race|"):
        race_id = data.split("|", 1)[1]
        from sports.biathlon.handlers import handle_race_menu
        threading.Thread(target=handle_race_menu, args=(race_id,), daemon=True).start()
    elif data.startswith("biat_h2hp|"):        # page N athlètes
        _, rid, page = data.split("|")
        from sports.biathlon.handlers import handle_h2h_athletes
        threading.Thread(target=handle_h2h_athletes, args=(rid, int(page), chat_id), daemon=True).start()
    elif data.startswith("biat_h2h|"):         # page 0 athlètes
        race_id = data.split("|", 1)[1]
        from sports.biathlon.handlers import handle_h2h_athletes
        threading.Thread(target=handle_h2h_athletes, args=(race_id, 0, chat_id), daemon=True).start()
    elif data.startswith("biat_selb|"):         # page N athlètes B — AVANT biat_sel|
        _, race_id, ibu_a, page = data.split("|")
        from sports.biathlon.handlers import handle_select_b_page
        threading.Thread(target=handle_select_b_page, args=(race_id, ibu_a, int(page), chat_id), daemon=True).start()
    elif data.startswith("biat_sel|"):          # sélection athlète A
        _, race_id, ibu_a = data.split("|")
        from sports.biathlon.handlers import handle_select_a
        threading.Thread(target=handle_select_a, args=(race_id, ibu_a, chat_id), daemon=True).start()
    elif data.startswith("biat_vs|"):           # duel final
        _, race_id, ibu_a, ibu_b = data.split("|")
        from sports.biathlon.handlers import handle_duel
        threading.Thread(target=handle_duel, args=(race_id, ibu_a, ibu_b, chat_id), daemon=True).start()
    elif data == "noop":
        pass
    elif data.startswith("biat_pod|"):
        race_id = data.split("|", 1)[1]
        from sports.biathlon.handlers import handle_podium
        threading.Thread(target=handle_podium, args=(race_id,), daemon=True).start()

    # ── Global ──
    elif data == "stats_global":
        threading.Thread(target=handle_global_stats, daemon=True).start()
    elif data == "web":
        from core.telegram import send_message
        url = os.getenv("WEB_URL", "")
        send_message(f"🌐 <b>Interface Web</b>\n\n{url}" if url else "⚠️ WEB_URL non configurée.")

    else:
        log.warning(f"Callback inconnu : {data}")


# ─────────────────────────────────────────────
# COMMANDES TEXTE (fallback / raccourcis)
# ─────────────────────────────────────────────

def handle_start():
    from core.telegram import send_menu_principal
    send_menu_principal()


def handle_global_stats():
    """Stats combinées foot + biathlon."""
    from core.telegram import send_message
    from core.database import get_stats

    stats = get_stats()
    o     = stats["overall"]
    roi   = o.get("roi") or 0
    sign  = "+" if roi >= 0 else ""

    msg = (
        f"📊 <b>Stats globales — Le Loup de Wall Bet</b>\n\n"
        f"⚽ <b>Football</b>\n"
        f"Total : {o.get('total') or 0} · ✅ {o.get('wins') or 0} · ❌ {o.get('losses') or 0}\n"
        f"Win rate : {o.get('win_rate') or 0}% · ROI : {sign}{roi}%\n"
    )

    # Biathlon si dispo
    try:
        from core.database import get_connection, row_to_dict
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN result=1 THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result=0 THEN 1 ELSE 0 END) as losses
            FROM biathlon_bets WHERE result != -1
        """)
        rb = row_to_dict(cur, cur.fetchone())
        conn.close()
        bw = rb.get("wins") or 0
        bl = rb.get("losses") or 0
        bt = rb.get("total") or 0
        if bt > 0:
            bwr  = round(bw / bt * 100, 1)
            broi = round((bw - bl) / bt * 100, 1)
            brs  = "+" if broi >= 0 else ""
            msg += (
                f"\n🎿 <b>Biathlon</b>\n"
                f"Total : {bt} · ✅ {bw} · ❌ {bl}\n"
                f"Win rate : {bwr}% · ROI : {brs}{broi}%\n"
            )
    except Exception:
        pass

    send_message(msg)


def handle_redeploy():
    from core.telegram import send_message
    token  = os.getenv("RAILWAY_API_TOKEN", "")
    svc_id = os.getenv("RAILWAY_SERVICE_ID", "")

    if not token or not svc_id:
        send_message(
            "⚠️ <b>Redeploy impossible</b>\n"
            "RAILWAY_API_TOKEN ou RAILWAY_SERVICE_ID manquant."
        )
        return
    try:
        resp = requests.post(
            "https://backboard.railway.app/graphql/v2",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"query": f'mutation {{ serviceInstanceRedeploy(serviceId: "{svc_id}") }}'},
            timeout=10
        )
        data = resp.json()
        if data.get("data", {}).get("serviceInstanceRedeploy"):
            send_message("🚀 <b>Redeploy lancé !</b>\nBot indisponible ~30s.")
        else:
            send_message(f"❌ Redeploy échoué : {data.get('errors', [{}])[0].get('message', '?')}")
    except Exception as e:
        send_message(f"❌ Redeploy échoué : {e}")


# Commandes texte disponibles (en plus des boutons)
TEXT_COMMANDS = {
    "/start":     handle_start,
    "/menu":      handle_start,
    "/redeploy":  handle_redeploy,
    "/stats":     handle_global_stats,
    # Raccourcis foot
    "/run":       lambda: threading.Thread(target=__import__('sports.football.handlers', fromlist=['handle_run']).handle_run, daemon=True).start(),
    "/bets":      lambda: threading.Thread(target=__import__('sports.football.handlers', fromlist=['handle_bets']).handle_bets, daemon=True).start(),
    "/results":   lambda: threading.Thread(target=__import__('sports.football.handlers', fromlist=['handle_results']).handle_results, daemon=True).start(),
    "/today":     lambda: threading.Thread(target=__import__('sports.football.handlers', fromlist=['handle_today']).handle_today, daemon=True).start(),
    # Raccourcis biathlon
    "/biathlon":  lambda: threading.Thread(target=__import__('sports.biathlon.handlers', fromlist=['handle_status']).handle_status, daemon=True).start(),
    "/biathlonrun": lambda: threading.Thread(target=__import__('sports.biathlon.handlers', fromlist=['handle_run']).handle_run, daemon=True).start(),
}


# ─────────────────────────────────────────────
# POLLING TELEGRAM
# ─────────────────────────────────────────────

def telegram_polling():
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN manquant — polling désactivé.")
        return

    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    offset   = None

    log.info(f"📲 Telegram polling démarré (chat_id: {TELEGRAM_CHAT})")

    # Skip anciens messages
    try:
        resp = requests.get(f"{base_url}/getUpdates", params={"offset": -1}, timeout=10)
        results = resp.json().get("result", [])
        if results:
            offset = results[-1]["update_id"] + 1
    except Exception as e:
        log.error(f"Init polling: {e}")

    while True:
        try:
            params = {"timeout": 5, "allowed_updates": ["message", "callback_query"]}
            if offset:
                params["offset"] = offset

            resp    = requests.get(f"{base_url}/getUpdates", params=params, timeout=10)
            updates = resp.json().get("result", [])

            for update in updates:
                offset = update["update_id"] + 1

                # ── Boutons inline ──
                if "callback_query" in update:
                    cb      = update["callback_query"]
                    chat_id = str(cb.get("message", {}).get("chat", {}).get("id", ""))
                    if TELEGRAM_CHAT and chat_id != TELEGRAM_CHAT:
                        continue
                    try:
                        handle_callback(cb)
                    except Exception as e:
                        log.error(f"Callback error: {e}")
                    continue

                # ── Messages texte ──
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().split()[0].lower()
                from_id = str(msg.get("chat", {}).get("id", ""))

                if TELEGRAM_CHAT and from_id != TELEGRAM_CHAT:
                    continue

                if text in TEXT_COMMANDS:
                    log.info(f"Commande: {text}")
                    try:
                        TEXT_COMMANDS[text]()
                    except Exception as e:
                        log.error(f"Commande {text}: {e}")
                        from core.telegram import send_message
                        send_message(f"❌ Erreur {text} : {e}")
                elif text.startswith("/"):
                    log.info(f"Commande inconnue ignorée : {text}")

        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(3)


# ─────────────────────────────────────────────
# SCHEDULER PRINCIPAL
# ─────────────────────────────────────────────

def run_scheduler():
    from apscheduler.schedulers.blocking import BlockingScheduler
    from core.database import init_db
    from core.telegram import send_message
    from sports.football.jobs import (
        refresh_team_stats, smart_run, check_results as foot_results,
        SCHEDULER_HOUR, state as foot_state
    )
    from sports.biathlon.jobs import (
        init_db as init_biathlon_db,
        run as biat_run, check_results as biat_results,
        ANALYSIS_HOUR, RESULTS_HOUR
    )

    # Init DB
    init_db()
    init_biathlon_db()

    foot_state["started_at"] = datetime.now(timezone.utc)

    # Polling Telegram
    threading.Thread(target=telegram_polling, daemon=True).start()
    log.info("📲 Polling Telegram démarré")

    # Scheduler
    scheduler = BlockingScheduler(timezone="UTC")

    # ── Jobs foot ──
    scheduler.add_job(refresh_team_stats, "cron", hour=6,  minute=0,  id="foot_refresh",  kwargs={"silent": True})
    scheduler.add_job(smart_run,          "cron", hour=SCHEDULER_HOUR, minute=0, id="foot_run")
    scheduler.add_job(foot_results,       "cron", hour=23, minute=0,  id="foot_results",  kwargs={"silent": False})

    # ── Jobs biathlon ──
    scheduler.add_job(biat_run,     "cron", hour=ANALYSIS_HOUR, minute=30, id="biat_run",     kwargs={"silent": False})
    scheduler.add_job(biat_results, "cron", hour=RESULTS_HOUR,  minute=0,  id="biat_results", kwargs={"silent": False})

    log.info(f"⏰ Foot: refresh 06h · analyse {SCHEDULER_HOUR:02d}h · résultats 23h UTC")
    log.info(f"⏰ Biathlon: analyse {ANALYSIS_HOUR:02d}h30 · résultats {RESULTS_HOUR:02d}h UTC")

    send_message(
        f"🐺 <b>Le Loup de Wall Bet est lancé</b> — "
        f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        f"⚽ Foot · 🎿 Biathlon\n"
        f"Tapez /menu pour commencer"
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        send_message("🛑 <b>Worker arrêté.</b>")