"""
app.py - Flask web interface ValueBet Bot
"""
import os
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
load_dotenv()

from api_clients import get_odds_api_usage
from database import (
    init_db, get_unique_bets, get_stats,
    get_stats_by_market, get_stats_by_league_detailed,
    get_bete_noire_bets, get_roi_over_time, get_streak,
    update_bet_result,
)

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
    bn_bets = get_bete_noire_bets(limit=500)
    return render_template(
        "stats.html",
        stats=stats,
        by_market=by_market,
        by_league=by_league,
        roi_time=roi_time,
        streak=streak,
        best_market=best_market,
        bn_bets=bn_bets,
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


# ─── Biathlon ────────────────────────────────────────────────────────────────

@app.route("/biathlon")
def biathlon_page():
    return render_template("biathlon.html")

@app.route("/api/biathlon/races")
def api_biathlon_races():
    from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON, PREV_SEASON
    try:
        preload_competitions(CURRENT_SEASON)
        preload_competitions(PREV_SEASON)
        races = [r for r in get_upcoming_races(days_ahead=21)
                 if r.get("format") not in ("RL","SR","MX")]
        return jsonify({"races": races, "count": len(races)})
    except Exception as e:
        return jsonify({"races": [], "error": str(e), "count": 0})

@app.route("/api/biathlon/athletes")
def api_biathlon_athletes():
    from sports.biathlon.handlers import _get_race_stats
    race_id = request.args.get("race_id","")
    try:
        cached = _get_race_stats(race_id)
        stats  = cached["stats"]
        athletes = [
            {"ibu": ibu, "name": s["name"], "nat": s["nat"],
             "avg_rank": round(s["avg_rank"], 1),
             "prone_acc": s["prone_acc"], "standing_acc": s["standing_acc"],
             "top3_rate": s["top3_rate"], "n_races": s["n_races"]}
            for ibu, s in sorted(stats.items(), key=lambda x: x[1]["avg_rank"])
        ]
        return jsonify({"athletes": athletes})
    except Exception as e:
        return jsonify({"athletes": [], "error": str(e)})

@app.route("/api/biathlon/duel")
def api_biathlon_duel():
    from sports.biathlon.handlers import _get_race_stats, _calc
    race_id = request.args.get("race_id","")
    ibu_a   = request.args.get("ibu_a","")
    ibu_b   = request.args.get("ibu_b","")
    try:
        cached = _get_race_stats(race_id)
        stats  = cached["stats"]
        fmt    = cached["fmt"]
        sa, sb = stats[ibu_a], stats[ibu_b]
        pa, pb = _calc(sa, sb, fmt)
        return jsonify({
            "name_a": sa["name"], "nat_a": sa["nat"],
            "name_b": sb["name"], "nat_b": sb["nat"],
            "prob_a": round(pa, 4), "prob_b": round(pb, 4),
            "cote_a": round(1/pa, 2), "cote_b": round(1/pb, 2),
            "prone_a": sa["prone_acc"], "prone_b": sb["prone_acc"],
            "stand_a": sa["standing_acc"], "stand_b": sb["standing_acc"],
            "rank_a": round(sa["avg_rank"], 1), "rank_b": round(sb["avg_rank"], 1),
            "top3_a": sa["top3_rate"], "top3_b": sb["top3_rate"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/biathlon/podium")
def api_biathlon_podium():
    from sports.biathlon.handlers import _get_race_stats
    from sports.biathlon.jobs import calc_rating
    race_id = request.args.get("race_id","")
    try:
        cached = _get_race_stats(race_id)
        stats  = cached["stats"]
        fmt    = cached["fmt"]
        top    = sorted(stats.items(), key=lambda x: -calc_rating(x[1], fmt))[:8]
        total  = sum(calc_rating(s, fmt) for _, s in top)
        podium = [{
            "name": s["name"], "nat": s["nat"],
            "pct": round(calc_rating(s, fmt)/total*100),
            "rank": round(s["avg_rank"], 1),
            "prone": s["prone_acc"], "stand": s["standing_acc"],
            "top3": s["top3_rate"],
        } for _, s in top]
        return jsonify({"podium": podium})
    except Exception as e:
        return jsonify({"podium": [], "error": str(e)}), 500

@app.route("/api/bets/<int:bet_id>/result", methods=["POST"])
def api_update_bet_result(bet_id):
    """Correction manuelle d'un résultat : 1=gagné, 0=perdu, -1=en attente."""
    data   = request.get_json(silent=True) or {}
    result = data.get("result")
    if result not in (0, 1, -1):
        return jsonify({"error": "result doit être 0, 1 ou -1"}), 400
    try:
        update_bet_result(bet_id, result)
        return jsonify({"ok": True, "bet_id": bet_id, "result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500