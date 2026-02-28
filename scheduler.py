"""
scheduler.py - Daily value bet engine

Run standalone: python scheduler.py
Or integrated with APScheduler for Railway deployment.
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from database import init_db, save_bet, save_team_stats, get_team_stats, get_all_bets, update_bet_result
from api_clients import get_fixtures, get_odds, get_team_standings, get_fixture_result
from model import (
    calc_league_averages, calc_attack_defense_strength,
    predict_match, find_value_bets
)
from telegram_bot import send_daily_summary, send_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LEAGUE_NAMES = {61: "Ligue 1", 39: "Premier League"}
SEASON = int(os.getenv("SEASON", 2024))
LEAGUES = [int(x) for x in os.getenv("LEAGUES", "61,39").split(",")]
VALUE_THRESHOLD = float(os.getenv("VALUE_THRESHOLD", 0.05))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", 0.55))
DAYS_AHEAD = int(os.getenv("SCHEDULER_DAYS_AHEAD", 3))


def refresh_team_stats():
    """Pull team standings/stats from API-Sports and persist to DB."""
    log.info("Refreshing team statistics...")
    for league_id in LEAGUES:
        try:
            teams = get_team_standings(league_id, SEASON)
            for t in teams:
                save_team_stats(t)
            log.info(f"  [{LEAGUE_NAMES.get(league_id)}] {len(teams)} teams updated.")
        except Exception as e:
            log.error(f"  Error fetching standings for league {league_id}: {e}")


def run_value_bet_engine():
    """Main daily job: compute value bets, store, notify."""
    log.info("=" * 60)
    log.info(f"VALUE BET ENGINE — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    all_value_bets = []  # list of (bet_dict, match_info_dict)

    for league_id in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        log.info(f"\n[{league_name}] Fetching fixtures...")

        # 1. Get upcoming fixtures
        try:
            fixtures = get_fixtures(league_id, SEASON, DAYS_AHEAD)
            log.info(f"  {len(fixtures)} fixtures found.")
        except Exception as e:
            log.error(f"  Error fetching fixtures: {e}")
            continue

        if not fixtures:
            continue

        # 2. Get team stats from DB
        team_stats = get_team_stats(league_id, SEASON)
        if not team_stats:
            log.warning(f"  No team stats in DB for {league_name}. Run refresh_team_stats() first.")
            continue

        avg_home, avg_away = calc_league_averages(team_stats)
        strengths = calc_attack_defense_strength(team_stats, avg_home, avg_away)
        log.info(f"  League avg goals: home={avg_home:.2f}, away={avg_away:.2f}")

        # 3. Get bookmaker odds for this league
        try:
            odds_events = get_odds(league_id)
            log.info(f"  {len(odds_events)} odds events fetched.")
        except Exception as e:
            log.error(f"  Error fetching odds: {e}")
            odds_events = []

        # Build odds lookup: (home_team, away_team) -> odds dict
        odds_lookup = {}
        for ev in odds_events:
            key = (ev["home_team"].lower(), ev["away_team"].lower())
            odds_lookup[key] = ev["odds"]

        # 4. For each fixture, predict & find value
        for fix in fixtures:
            home_id = fix["home_team_id"]
            away_id = fix["away_team_id"]
            home_name = fix["home_team_name"]
            away_name = fix["away_team_name"]

            prediction = predict_match(home_id, away_id, strengths, avg_home, avg_away)
            if not prediction:
                log.debug(f"  Skipping {home_name} vs {away_name} — no stats")
                continue

            # Match odds to fixture (fuzzy key match)
            odds = odds_lookup.get((home_name.lower(), away_name.lower()), {})
            if not odds:
                # Try partial name match
                for (h_key, a_key), o in odds_lookup.items():
                    if h_key in home_name.lower() or home_name.lower() in h_key:
                        if a_key in away_name.lower() or away_name.lower() in a_key:
                            odds = o
                            break

            if not odds:
                log.debug(f"  No odds found for {home_name} vs {away_name}")
                continue

            value_bets = find_value_bets(prediction, odds, VALUE_THRESHOLD, MIN_PROBABILITY)

            match_info = {
                "date": fix["date"],
                "home_team": home_name,
                "away_team": away_name,
                "league": league_name,
                "fixture_id": fix["fixture_id"],
            }

            for bet in value_bets:
                bet_record = {
                    "match_date": fix["date"],
                    "league": league_name,
                    "home_team": home_name,
                    "away_team": away_name,
                    **bet,
                }
                bet_id = save_bet(bet_record)
                log.info(
                    f"  ✅ VALUE BET #{bet_id}: {home_name} vs {away_name} | "
                    f"{bet['market']} @ {bet['bk_odds']} | Value: +{bet['value']*100:.1f}%"
                )
                all_value_bets.append((bet, match_info))

    # 5. Send top bets via Telegram
    log.info(f"\nTotal value bets found: {len(all_value_bets)}")
    send_daily_summary(all_value_bets, {})
    log.info("Daily job complete.")


def update_past_results():
    """Check pending bets and update with actual results."""
    log.info("Updating past results...")
    bets = get_all_bets()
    today = datetime.utcnow().date().isoformat()

    pending = [b for b in bets if b["success"] is None and b["match_date"] < today]
    log.info(f"  {len(pending)} pending bets to check.")

    for bet in pending:
        fixture_id = None
        # We don't store fixture_id in bets table currently.
        # This is a best-effort update — in production, store fixture_id in bets.
        # Skipping for now; implement fixture_id tracking as enhancement.
        pass


def run_scheduler():
    """Start APScheduler for Railway deployment."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    hour = int(os.getenv("SCHEDULER_HOUR", 8))

    scheduler.add_job(run_value_bet_engine, "cron", hour=hour, minute=0, id="daily_value_bets")
    scheduler.add_job(refresh_team_stats, "cron", hour=6, minute=0, id="refresh_stats")

    log.info(f"Scheduler started. Daily run at {hour}:00 UTC.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")


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
        print(f"Unknown command: {command}")
        print("Usage: python scheduler.py [run|refresh|schedule]")
