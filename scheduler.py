"""
scheduler.py - Value bet engine + Bot Telegram interactif

Commandes Telegram :
  /help    → liste des commandes
  /status  → état du worker
  /bets    → paris du jour
  /stats   → win rate + ROI
  /refresh → forcer refresh stats équipes
  /run     → lancer l'analyse maintenant

Commandes CLI :
  python scheduler.py run       → exécution immédiate
  python scheduler.py refresh   → mise à jour stats équipes
  python scheduler.py schedule  → démarrer worker + bot (Railway)
"""

import os
import logging
import threading
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from database import init_db, save_bet, save_team_stats, get_team_stats, get_all_bets, get_stats
from api_clients import get_fixtures, get_odds, get_team_standings
from model import calc_league_averages, calc_attack_defense_strength, predict_match, find_value_bets
from telegram_bot import send_message, send_daily_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LEAGUE_NAMES    = {61: "Ligue 1", 39: "Premier League"}
SEASON          = int(os.getenv("SEASON", 2024))
LEAGUES         = [int(x) for x in os.getenv("LEAGUES", "61,39").split(",")]
VALUE_THRESHOLD = float(os.getenv("VALUE_THRESHOLD", 0.05))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", 0.55))
DAYS_AHEAD      = int(os.getenv("SCHEDULER_DAYS_AHEAD", 3))
SCHEDULER_HOUR  = int(os.getenv("SCHEDULER_HOUR", 8))
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

# État global du worker
worker_state = {
    "started_at":   None,
    "last_run":     None,
    "last_refresh": None,
    "bets_today":   0,
    "running":      False,
}


# ─────────────────────────────────────────────
# MOTEUR VALUE BET
# ─────────────────────────────────────────────

def refresh_team_stats(silent=False):
    """Mise à jour des stats équipes depuis API-Sports → DB."""
    log.info("🔄 Refresh stats équipes...")
    results = []
    for league_id in LEAGUES:
        try:
            teams = get_team_standings(league_id, SEASON)
            for t in teams:
                save_team_stats(t)
            msg = f"✅ {LEAGUE_NAMES.get(league_id)} : {len(teams)} équipes"
            log.info(f"  {msg}")
            results.append(msg)
        except Exception as e:
            msg = f"❌ {LEAGUE_NAMES.get(league_id)} : {e}"
            log.error(f"  {msg}")
            results.append(msg)

    worker_state["last_refresh"] = datetime.now(timezone.utc)

    if not silent:
        send_message("🔄 <b>Refresh stats terminé</b>\n\n" + "\n".join(results))

    return results


def run_value_bet_engine(silent=False):
    """Moteur principal — entièrement autonome."""
    if worker_state["running"]:
        send_message("⏳ Une analyse est déjà en cours, patientez...")
        return

    worker_state["running"] = True
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info("=" * 60)
    log.info(f"⚽ VALUE BET ENGINE — {now}")
    log.info("=" * 60)

    if not silent:
        send_message(f"🚀 <b>Analyse démarrée</b>\n📅 {now}\n🔍 Calcul en cours...")

    all_value_bets = []
    errors = []

    for league_id in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        log.info(f"\n[{league_name}]")

        # 1. Fixtures
        try:
            fixtures = get_fixtures(league_id, SEASON, DAYS_AHEAD)
            log.info(f"  {len(fixtures)} matchs trouvés.")
        except Exception as e:
            errors.append(f"Fixtures {league_name}: {e}")
            continue

        if not fixtures:
            log.info(f"  Aucun match à venir.")
            continue

        # 2. Stats équipes (avec auto-refresh si vides)
        team_stats = get_team_stats(league_id, SEASON)
        if not team_stats:
            log.warning(f"  Pas de stats — auto-refresh...")
            try:
                teams = get_team_standings(league_id, SEASON)
                for t in teams:
                    save_team_stats(t)
                team_stats = get_team_stats(league_id, SEASON)
                log.info(f"  Auto-refresh OK : {len(team_stats)} équipes.")
            except Exception as e:
                errors.append(f"Auto-refresh {league_name}: {e}")
                continue

        avg_home, avg_away = calc_league_averages(team_stats)
        strengths = calc_attack_defense_strength(team_stats, avg_home, avg_away)
        log.info(f"  Moy. buts: dom={avg_home:.2f} ext={avg_away:.2f}")

        # 3. Cotes bookmakers
        try:
            odds_events = get_odds(league_id)
            log.info(f"  {len(odds_events)} événements avec cotes.")
        except Exception as e:
            errors.append(f"Cotes {league_name}: {e}")
            odds_events = []

        odds_lookup = {}
        for ev in odds_events:
            key = (ev["home_team"].lower(), ev["away_team"].lower())
            odds_lookup[key] = ev["odds"]

        # 4. Prédiction + value pour chaque match
        for fix in fixtures:
            home_id, away_id     = fix["home_team_id"], fix["away_team_id"]
            home_name, away_name = fix["home_team_name"], fix["away_team_name"]

            prediction = predict_match(home_id, away_id, strengths, avg_home, avg_away)
            if not prediction:
                continue

            odds = odds_lookup.get((home_name.lower(), away_name.lower()), {})
            if not odds:
                for (h_key, a_key), o in odds_lookup.items():
                    if h_key in home_name.lower() or home_name.lower() in h_key:
                        if a_key in away_name.lower() or away_name.lower() in a_key:
                            odds = o
                            break

            if not odds:
                continue

            value_bets = find_value_bets(prediction, odds, VALUE_THRESHOLD, MIN_PROBABILITY)
            match_info = {
                "date": fix["date"], "home_team": home_name,
                "away_team": away_name, "league": league_name,
            }

            for bet in value_bets:
                try:
                    bet_id = save_bet({
                        "match_date": fix["date"], "league": league_name,
                        "home_team": home_name, "away_team": away_name, **bet,
                    })
                    log.info(
                        f"  ✅ BET #{bet_id}: {home_name} vs {away_name} | "
                        f"{bet['market']} @ {bet['bk_odds']} | +{bet['value']*100:.1f}%"
                    )
                    all_value_bets.append((bet, match_info))
                except Exception as e:
                    log.error(f"  save_bet: {e}")

    worker_state["last_run"]   = datetime.now(timezone.utc)
    worker_state["bets_today"] = len(all_value_bets)
    worker_state["running"]    = False

    send_daily_summary(all_value_bets, {})

    if errors:
        send_message("⚠️ <b>Erreurs durant l'analyse :</b>\n" + "\n".join(f"• {e}" for e in errors))

    log.info(f"✅ Analyse terminée — {len(all_value_bets)} value bets.")


# ─────────────────────────────────────────────
# COMMANDES TELEGRAM
# ─────────────────────────────────────────────

def handle_help():
    send_message(
        "🤖 <b>ValueBet Bot — Commandes</b>\n\n"
        "❓ /help    — Ce message\n"
        "📡 /status  — État du worker\n"
        "⚽ /bets    — Paris du jour\n"
        "📊 /stats   — Win rate + ROI\n"
        "⚡ /run     — Lancer une analyse\n"
        "🔄 /refresh — Refresh stats équipes\n\n"
        f"<i>Analyse auto : {SCHEDULER_HOUR:02d}h00 UTC chaque jour</i>"
    )


def handle_status():
    started      = worker_state["started_at"]
    last_run     = worker_state["last_run"]
    last_refresh = worker_state["last_refresh"]

    uptime = "N/A"
    if started:
        delta = datetime.now(timezone.utc) - started
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m = rem // 60
        uptime = f"{h}h {m:02d}m"

    etat = "🔄 Analyse en cours..." if worker_state["running"] else "🟢 En attente"

    send_message(
        f"📡 <b>Status du Worker</b>\n\n"
        f"État : {etat}\n"
        f"⏱ Uptime : {uptime}\n"
        f"📅 Démarré : {started.strftime('%Y-%m-%d %H:%M UTC') if started else 'N/A'}\n"
        f"🕐 Prochaine analyse : {SCHEDULER_HOUR:02d}h00 UTC\n"
        f"⚽ Dernière analyse : {last_run.strftime('%Y-%m-%d %H:%M UTC') if last_run else 'Aucune'}\n"
        f"🔄 Dernier refresh : {last_refresh.strftime('%Y-%m-%d %H:%M UTC') if last_refresh else 'Aucun'}\n"
        f"🎯 Bets dernière analyse : {worker_state['bets_today']}"
    )


def handle_bets():
    today = datetime.now(timezone.utc).date().isoformat()
    bets  = get_all_bets(limit=200)
    today_bets = [b for b in bets if b["match_date"] == today]

    if not today_bets:
        send_message(
            f"📭 <b>Aucun value bet aujourd'hui</b> ({today})\n"
            f"Prochaine analyse : {SCHEDULER_HOUR:02d}h00 UTC\n\n"
            f"💡 Tapez /run pour lancer une analyse maintenant."
        )
        return

    msg = f"⚽ <b>Value bets du {today}</b> — {len(today_bets)} sélection(s)\n{'─'*30}\n\n"
    for b in today_bets[:10]:
        status = "✅" if b["success"] == 1 else "❌" if b["success"] == 0 else "⏳"
        msg += (
            f"{status} <b>{b['home_team']} vs {b['away_team']}</b>\n"
            f"   📌 {b['market']} @ <b>{b['bk_odds']}</b>\n"
            f"   💎 Value : <b>+{b['value']*100:.1f}%</b> | "
            f"Proba : {b['probability']*100:.0f}%\n"
            f"   🏦 {b['bookmaker']}\n\n"
        )
    send_message(msg)


def handle_stats():
    stats     = get_stats()
    o         = stats["overall"]
    by_league = stats.get("by_league", [])

    league_lines = ""
    for row in by_league:
        wins  = row.get("wins") or 0
        total = row.get("total") or 0
        wr    = round(wins / total * 100, 1) if total > 0 else 0
        league_lines += (
            f"\n  • {row['league']} : {wins}/{total} ({wr}%) "
            f"| Value moy. +{row.get('avg_value') or 0}%"
        )

    roi      = o.get("roi") or 0
    wr       = o.get("win_rate") or 0
    roi_sign = "+" if roi >= 0 else ""

    send_message(
        f"📊 <b>Statistiques ValueBet</b>\n\n"
        f"🎯 Paris totaux : <b>{o.get('total') or 0}</b>\n"
        f"✅ Gagnés : <b>{o.get('wins') or 0}</b>\n"
        f"❌ Perdus : <b>{o.get('losses') or 0}</b>\n"
        f"⏳ En attente : <b>{o.get('pending') or 0}</b>\n\n"
        f"📈 Taux de réussite : <b>{wr}%</b>\n"
        f"💰 ROI : <b>{roi_sign}{roi}%</b>\n"
        f"📉 Value moyenne : <b>+{o.get('avg_value_pct') or 0}%</b>\n"
        f"\n<b>Par ligue :</b>{league_lines or ' Pas encore de données'}"
    )


def handle_run():
    send_message(
        "⚡ <b>Analyse manuelle lancée !</b>\n"
        "Résultats dans quelques secondes...\n\n"
        "💡 Tapez /bets après pour voir les sélections."
    )
    t = threading.Thread(target=run_value_bet_engine, daemon=True)
    t.start()


def handle_refresh():
    send_message("🔄 <b>Refresh des stats en cours...</b>")
    t = threading.Thread(target=refresh_team_stats, daemon=True)
    t.start()


COMMANDS = {
    "/help":    handle_help,
    "/status":  handle_status,
    "/bets":    handle_bets,
    "/stats":   handle_stats,
    "/run":     handle_run,
    "/refresh": handle_refresh,
}


# ─────────────────────────────────────────────
# POLLING TELEGRAM (corrigé)
# ─────────────────────────────────────────────

def telegram_polling():
    """Écoute les messages Telegram — short polling robuste."""
    if not TELEGRAM_TOKEN:
        log.warning("⚠️ TELEGRAM_BOT_TOKEN manquant — polling désactivé.")
        return

    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    offset   = None

    log.info(f"📲 Telegram polling démarré — chat_id autorisé : {TELEGRAM_CHAT}")

    # Vider les anciens messages au démarrage
    try:
        resp = requests.get(f"{base_url}/getUpdates", params={"offset": -1}, timeout=10)
        results = resp.json().get("result", [])
        if results:
            offset = results[-1]["update_id"] + 1
            log.info(f"  {len(results)} anciens messages ignorés, offset={offset}")
    except Exception as e:
        log.error(f"  Erreur init polling: {e}")

    while True:
        try:
            params = {"timeout": 5, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset

            resp    = requests.get(f"{base_url}/getUpdates", params=params, timeout=10)
            updates = resp.json().get("result", [])

            for update in updates:
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().split()[0].lower()
                from_id = str(msg.get("chat", {}).get("id", ""))

                log.info(f"📩 Message reçu : '{text}' de {from_id}")

                # Sécurité : uniquement votre chat_id
                if TELEGRAM_CHAT and from_id != TELEGRAM_CHAT:
                    log.warning(f"  Ignoré — chat_id non autorisé : {from_id}")
                    continue

                if text in COMMANDS:
                    log.info(f"  → Exécution commande : {text}")
                    try:
                        COMMANDS[text]()
                    except Exception as e:
                        log.error(f"  Erreur commande {text}: {e}")
                        send_message(f"❌ Erreur commande {text} : {e}")
                elif text.startswith("/"):
                    handle_help()

        except requests.exceptions.Timeout:
            pass  # Normal avec short polling
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(3)


# ─────────────────────────────────────────────
# SCHEDULER PRINCIPAL
# ─────────────────────────────────────────────

def run_scheduler():
    """Démarre APScheduler + polling Telegram en parallèle."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    worker_state["started_at"] = datetime.now(timezone.utc)

    # Polling Telegram dans un thread séparé
    log.info("Démarrage thread polling Telegram...")
    poll_thread = threading.Thread(target=telegram_polling, daemon=True)
    poll_thread.start()
    log.info("Thread polling démarré ✅")

    # Scheduler cron
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        refresh_team_stats, "cron",
        hour=6, minute=0, id="refresh_stats",
        kwargs={"silent": True}
    )
    scheduler.add_job(
        run_value_bet_engine, "cron",
        hour=SCHEDULER_HOUR, minute=0, id="daily_value_bets",
        kwargs={"silent": False}
    )

    log.info(f"⏰ Scheduler démarré — refresh 06h UTC, analyse {SCHEDULER_HOUR:02d}h UTC")

    send_message(
        f"✅ <b>Worker ValueBet démarré !</b>\n\n"
        f"⏰ Refresh stats : 06h00 UTC\n"
        f"⚽ Analyse value bets : {SCHEDULER_HOUR:02d}h00 UTC\n"
        f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"💬 Tapez /help pour voir les commandes."
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler arrêté.")
        send_message("🛑 <b>Worker ValueBet arrêté.</b>")


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    init_db()

    command = sys.argv[1] if len(sys.argv) > 1 else "run"

    if command == "refresh":
        refresh_team_stats()
    elif command == "schedule":
        run_scheduler()
    elif command == "run":
        run_value_bet_engine()
    else:
        print(f"Commande inconnue : {command}")
        print("Usage: python scheduler.py [run|refresh|schedule]")