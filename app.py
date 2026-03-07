"""
app.py - Flask web interface for ValueBet Bot
"""
import os
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv
load_dotenv()
from database import init_db, get_unique_bets, get_stats

app = Flask(__name__)

@app.before_request
def setup():
    init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/history")
def history():
    bets = get_unique_bets(limit=200)
    return render_template("history.html", bets=bets)

@app.route("/stats")
def stats_page():
    stats = get_stats()
    return render_template("stats.html", stats=stats)

@app.route("/api/bets")
def api_bets():
    bets = get_unique_bets(limit=200)
    return jsonify(bets)

@app.route("/api/stats")
def api_stats():
    stats = get_stats()
    return jsonify(stats)

@app.route("/api/live")
def api_live():
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    bets = get_unique_bets(limit=200)
    today_bets = [b for b in bets if b["match_date"] == today]
    return jsonify(today_bets)

@app.route("/live")
def live():
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    bets = get_unique_bets(limit=200)
    today_bets = [b for b in bets if b["match_date"] == today]
    return render_template("live.html", bets=today_bets, today=today)


@app.route("/config")
def config_page():
    import os
    config = {
        "value_threshold":    float(os.getenv("VALUE_THRESHOLD", 0.02)),
        "min_probability":    float(os.getenv("MIN_PROBABILITY", 0.55)),
        "poisson_weight":     0.50,
        "days_ahead":         int(os.getenv("SCHEDULER_DAYS_AHEAD", 10)),
        "top_bets_count":     int(os.getenv("TOP_BETS_COUNT", 10)),
        "season":             int(os.getenv("SEASON", 2025)),
        "scheduler_hour":     int(os.getenv("SCHEDULER_HOUR", 8)),
        "has_odds_key":       bool(os.getenv("ODDS_API_KEY")),
        "has_footballdata_key": bool(os.getenv("FOOTBALLDATA_KEY")),
        "has_telegram_token": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "leagues": [
            {"id": 39,  "name": "Premier League",      "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
            {"id": 61,  "name": "Ligue 1",              "flag": "🇫🇷"},
            {"id": 78,  "name": "Bundesliga",           "flag": "🇩🇪"},
            {"id": 135, "name": "Serie A",              "flag": "🇮🇹"},
            {"id": 140, "name": "La Liga",              "flag": "🇪🇸"},
            {"id": 88,  "name": "Eredivisie",           "flag": "🇳🇱"},
            {"id": 94,  "name": "Primeira Liga",        "flag": "🇵🇹"},
            {"id": 40,  "name": "Championship",         "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
            {"id": 2,   "name": "Champions League",     "flag": "🇪🇺"},
            {"id": 3,   "name": "Europa League",        "flag": "🇪🇺"},
            {"id": 144, "name": "Belgium First Div",    "flag": "🇧🇪"},
            {"id": 203, "name": "Turkey Super League",  "flag": "🇹🇷"},
            {"id": 179, "name": "Scottish Premiership", "flag": "🏴󠁧󠁢󠁳󠁣󠁴󠁿"},
            {"id": 3,   "name": "Europa League",        "flag": "🇪🇺"},
        ],
    }
    return render_template("config.html", config=config)

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)