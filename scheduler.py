"""
scheduler.py - Daily value bet engine (100% autonome, sans dépendance au web)

Commandes:
  python scheduler.py run       → exécution immédiate
  python scheduler.py refresh   → mise à jour stats équipes
  python scheduler.py schedule  → démarrer le cron (Railway worker)
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from database import init_db, save_bet, save_team_stats, get_team_stats, get_all_bets
from api_clients import get_fixtures, get_odds, get_team_standings
from model import calc_league_averages, calc_attack_defense_strength, predict_match, find_value_bets
from telegram_bot import send_daily_summary, send_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LEAGUE_NAMES    = {61: "Ligue 1", 39: "Premier League"}
SEASON          = int(os.getenv("SEASON", 2024))
LEAGUES         = [int(x) for x in os.getenv("LEAGUES", "61,39").split(",")]
VALUE_THRESHOLD = float(os.getenv("VALUE_THRESHOLD", 0.05))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", 0.55))
DAYS_AHEAD      = int(os.getenv("SCHEDULER_DAYS_AHEAD", 3))


def refresh_team_stats():
    """Mise à jour des stats équipes depuis API-Sports → DB."""
    log.info("🔄 Refresh stats équipes...")
    for league_id in LEAGUES:
        try:
            teams = get_team_standings(league_id, SEASON)
            for t in teams:
                save_team_stats(t)
            log.info(f"  [{LEAGUE_NAMES.get(league_id)}] {len(teams)} équipes mises à jour.")
        except Exception as e:
            log.error(f"  Erreur standings league {league_id}: {e}")
            send_message(f"⚠️ <b>Erreur refresh stats</b>\nLeague {league_id}: {e}")


def run_value_bet_engine():
    """
    Moteur principal — entièrement autonome :
    1. Récupère les fixtures J+1 à J+3
    2. Calcule les prédictions Poisson
    3. Compare aux cotes bookmakers
    4. Sauvegarde en DB
    5. Envoie les value bets via Telegram
    """
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    log.info("=" * 60)
    log.info(f"⚽ VALUE BET ENGINE — {now}")
    log.info("=" * 60)

    send_message(f"🚀 <b>ValueBet Bot démarré</b>\n📅 {now}\n🔍 Analyse en cours...")

    all_value_bets = []
    errors = []

    for league_id in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        log.info(f"\n[{league_name}] Récupération des fixtures...")

        # 1. Fixtures
        try:
            fixtures = get_fixtures(league_id, SEASON, DAYS_AHEAD)
            log.info(f"  {len(fixtures)} matchs trouvés.")
        except Exception as e:
            msg = f"Erreur fixtures {league_name}: {e}"
            log.error(f"  {msg}")
            errors.append(msg)
            continue

        if not fixtures:
            log.info(f"  Aucun match à venir pour {league_name}.")
            continue

        # 2. Stats équipes depuis DB (avec auto-refresh si vides)
        team_stats = get_team_stats(league_id, SEASON)
        if not team_stats:
            log.warning(f"  Pas de stats en DB pour {league_name} — auto-refresh...")
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
        log.info(f"  Moy. buts: domicile={avg_home:.2f}, extérieur={avg_away:.2f}")

        # 3. Cotes bookmakers
        try:
            odds_events = get_odds(league_id)
            log.info(f"  {len(odds_events)} événements avec cotes.")
        except Exception as e:
            msg = f"Erreur cotes {league_name}: {e}"
            log.error(f"  {msg}")
            errors.append(msg)
            odds_events = []

        # Index cotes par (home, away)
        odds_lookup = {}
        for ev in odds_events:
            key = (ev["home_team"].lower(), ev["away_team"].lower())
            odds_lookup[key] = ev["odds"]

        # 4. Prédiction + value pour chaque match
        for fix in fixtures:
            home_id   = fix["home_team_id"]
            away_id   = fix["away_team_id"]
            home_name = fix["home_team_name"]
            away_name = fix["away_team_name"]

            prediction = predict_match(home_id, away_id, strengths, avg_home, avg_away)
            if not prediction:
                log.debug(f"  Skip {home_name} vs {away_name} — stats manquantes")
                continue

            # Recherche cotes (exacte puis partielle)
            odds = odds_lookup.get((home_name.lower(), away_name.lower()), {})
            if not odds:
                for (h_key, a_key), o in odds_lookup.items():
                    if h_key in home_name.lower() or home_name.lower() in h_key:
                        if a_key in away_name.lower() or away_name.lower() in a_key:
                            odds = o
                            break

            if not odds:
                log.debug(f"  Pas de cotes pour {home_name} vs {away_name}")
                continue

            value_bets = find_value_bets(prediction, odds, VALUE_THRESHOLD, MIN_PROBABILITY)

            match_info = {
                "date":      fix["date"],
                "home_team": home_name,
                "away_team": away_name,
                "league":    league_name,
            }

            for bet in value_bets:
                bet_record = {
                    "match_date": fix["date"],
                    "league":     league_name,
                    "home_team":  home_name,
                    "away_team":  away_name,
                    **bet,
                }
                try:
                    bet_id = save_bet(bet_record)
                    log.info(
                        f"  ✅ BET #{bet_id}: {home_name} vs {away_name} | "
                        f"{bet['market']} @ {bet['bk_odds']} | Value: +{bet['value']*100:.1f}%"
                    )
                    all_value_bets.append((bet, match_info))
                except Exception as e:
                    log.error(f"  Erreur save_bet: {e}")

    # 5. Envoi Telegram
    log.info(f"\n📊 Total value bets : {len(all_value_bets)}")
    send_daily_summary(all_value_bets, {})

    if errors:
        err_msg = "⚠️ <b>Erreurs durant l'analyse :</b>\n" + "\n".join(f"• {e}" for e in errors)
        send_message(err_msg)

    log.info("✅ Job quotidien terminé.")


def run_scheduler():
    """Démarre APScheduler en mode bloquant pour Railway worker."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    hour = int(os.getenv("SCHEDULER_HOUR", 8))

    scheduler.add_job(refresh_team_stats,   "cron", hour=6,    minute=0, id="refresh_stats")
    scheduler.add_job(run_value_bet_engine, "cron", hour=hour, minute=0, id="daily_value_bets")

    log.info(f"⏰ Scheduler démarré — refresh 6h UTC, analyse {hour}h UTC")

    # Message Telegram de confirmation au démarrage
    send_message(
        f"✅ <b>Worker ValueBet démarré</b>\n"
        f"⏰ Refresh stats : 06h00 UTC\n"
        f"⚽ Analyse value bets : {hour:02d}h00 UTC\n"
        f"📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler arrêté.")
        send_message("🛑 <b>Worker ValueBet arrêté.</b>")


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