"""
app.py - Flask web interface for ValueBet Bot
"""

import os
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

load_dotenv()

from database import init_db, get_all_bets, get_stats

app = Flask(__name__)


@app.before_request
def setup():
    """Ensure DB exists on first request."""
    init_db()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/history")
def history():
    bets = get_all_bets(limit=200)
    return render_template("history.html", bets=bets)


@app.route("/stats")
def stats_page():
    stats = get_stats()
    return render_template("stats.html", stats=stats)


@app.route("/api/bets")
def api_bets():
    bets = get_all_bets(limit=200)
    return jsonify(bets)


@app.route("/api/stats")
def api_stats():
    stats = get_stats()
    return jsonify(stats)


@app.route("/api/live")
def api_live():
    """Return today's pending bets."""
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    bets = get_all_bets(limit=200)
    today_bets = [b for b in bets if b["match_date"] == today]
    return jsonify(today_bets)


@app.route("/live")
def live():
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    bets = get_all_bets(limit=200)
    today_bets = [b for b in bets if b["match_date"] == today]
    return render_template("live.html", bets=today_bets, today=today)


if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
