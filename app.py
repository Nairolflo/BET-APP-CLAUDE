"""
app.py - Flask web interface ValueBet Bot
"""
import os
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv
load_dotenv()

from api_clients import get_odds_api_usage
from database import (
    init_db, get_unique_bets, get_stats,
    get_stats_by_market, get_stats_by_league_detailed,
    get_bete_noire_bets, get_roi_over_time, get_streak,
)
from api_clients import get_odds_api_usage

app = Flask(__name__)

@app.before_request
def setup():
    init_db()

# ─────────────────────────────────────────────
# PAGES
# ─────────────────────────────────────────────

@app.route("/")
def index():
    stats    = get_stats()
    streak   = get_streak()
    roi_time = get_roi_over_time()
    return render_template("index.html", stats=stats, streak=streak, roi_time=roi_time)

@app.route("/history")
def history():
    bets = get_unique_bets(limit=500)
    # Collecte toutes les ligues disponibles pour le filtre
    leagues = sorted(set(b["league"] for b in bets if b.get("league")))
    return render_template("history.html", bets=bets, leagues=leagues)

@app.route("/stats")
def stats_page():
    stats      = get_stats()
    by_market  = get_stats_by_market()
    by_league  = get_stats_by_league_detailed()
    roi_time   = get_roi_over_time()
    streak     = get_streak()
    # Best market
    resolved    = [m for m in by_market if (m.get("total",0) - m.get("pending",0)) >= 3]
    best_market = max(resolved, key=lambda x: x.get("roi", -999)) if resolved else None
    # Bête noire stats
    bn_bets = get_bete_noire_bets(limit=500)
    bn_wins    = sum(1 for b in bn_bets if b.get("success") == 1)
    bn_losses  = sum(1 for b in bn_bets if b.get("success") == 0)
    bn_pending = sum(1 for b in bn_bets if b.get("success") == -1)
    bn_settled = max(bn_wins + bn_losses, 1)
    bn_stats = {
        "total":    len(bn_bets),
        "wins":     bn_wins,
        "losses":   bn_losses,
        "pending":  bn_pending,
        "win_rate": round(bn_wins / bn_settled * 100, 1),
        "roi":      round((bn_wins - bn_losses) / bn_settled * 100, 1),
    }
    return render_template(
        "stats.html",
        stats=stats,
        by_market=by_market,
        by_league=by_league,
        roi_time=roi_time,
        streak=streak,
        best_market=best_market,
        bn_stats=bn_stats,
    )

# bete_noire page merged into /stats

@app.route("/live")
def live():
    from datetime import datetime
    today     = datetime.utcnow().date().isoformat()
    bets      = get_unique_bets(limit=500)
    today_bets = [b for b in bets if b.get("match_date") == today]
    return render_template("live.html", bets=today_bets, today=today)

@app.route("/config")
def config_page():
    config = {
        "value_threshold":      float(os.getenv("VALUE_THRESHOLD", 0.02)),
        "min_probability":      float(os.getenv("MIN_PROBABILITY", 0.55)),
        "poisson_weight":       0.40,
        "days_ahead":           int(os.getenv("SCHEDULER_DAYS_AHEAD", 10)),
        "season":               int(os.getenv("SEASON", 2025)),
        "scheduler_hour":       int(os.getenv("SCHEDULER_HOUR", 8)),
        "h2h_seasons":          3,
        "has_odds_key":         bool(os.getenv("ODDS_API_KEY")),
        "has_footballdata_key": bool(os.getenv("FOOTBALLDATA_KEY")),
        "has_telegram_token":   bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "leagues": [
            {"id": 39,  "name": "Premier League",      "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "h2h": True,  "form": True},
            {"id": 61,  "name": "Ligue 1",              "flag": "🇫🇷", "h2h": True,  "form": True},
            {"id": 78,  "name": "Bundesliga",           "flag": "🇩🇪", "h2h": True,  "form": True},
            {"id": 135, "name": "Serie A",              "flag": "🇮🇹", "h2h": True,  "form": True},
            {"id": 140, "name": "La Liga",              "flag": "🇪🇸", "h2h": True,  "form": True},
            {"id": 88,  "name": "Eredivisie",           "flag": "🇳🇱", "h2h": True,  "form": True},
            {"id": 94,  "name": "Primeira Liga",        "flag": "🇵🇹", "h2h": True,  "form": True},
            {"id": 40,  "name": "Championship",         "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "h2h": True,  "form": True},
            {"id": 2,   "name": "Champions League",     "flag": "🏆",  "h2h": True,  "form": True},
            {"id": 3,   "name": "Europa League",        "flag": "🇪🇺", "h2h": False, "form": False},
            {"id": 144, "name": "Belgium First Div",    "flag": "🇧🇪", "h2h": False, "form": False},
            {"id": 203, "name": "Turkey Super League",  "flag": "🇹🇷", "h2h": False, "form": False},
            {"id": 179, "name": "Scottish Premiership", "flag": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "h2h": False, "form": False},
        ],
    }
    quota = get_odds_api_usage()
    return render_template("config.html", config=config, quota=quota)

# ─────────────────────────────────────────────
# API JSON
# ─────────────────────────────────────────────

@app.route("/api/bets")
def api_bets():
    return jsonify(get_unique_bets(limit=500))

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())

@app.route("/api/stats/market")
def api_stats_market():
    return jsonify(get_stats_by_market())

@app.route("/api/stats/league")
def api_stats_league():
    return jsonify(get_stats_by_league_detailed())

@app.route("/api/roi-time")
def api_roi_time():
    return jsonify(get_roi_over_time())

@app.route("/api/live")
def api_live():
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    bets  = get_unique_bets(limit=500)
    return jsonify([b for b in bets if b.get("match_date") == today])

@app.route("/api/quota")
def api_quota():
    return jsonify(get_odds_api_usage())




if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)