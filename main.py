"""
main.py — Point d'entrée unique du Loup de Wall Bet
"""
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Ajoute la racine du projet au path
sys.path.insert(0, os.path.dirname(__file__))

from database import init_db

if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "schedule"

    init_db()

    if command == "schedule":
        from core.scheduler import run_scheduler
        run_scheduler()

    elif command == "run":
        from sports.football.jobs import run
        run(silent=False)

    elif command == "refresh":
        from sports.football.jobs import refresh_team_stats
        refresh_team_stats(silent=False)

    elif command == "results":
        from sports.football.jobs import check_results
        check_results(silent=False)

    elif command == "biathlon":
        from sports.biathlon.jobs import run
        run(silent=False)

    elif command == "biathlon_results":
        from sports.biathlon.jobs import check_results
        check_results(silent=False)

    else:
        print("Usage: python main.py [schedule|run|refresh|results|biathlon|biathlon_results]")
