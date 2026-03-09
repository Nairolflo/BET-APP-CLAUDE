"""
sports/biathlon/handlers.py — Handlers Telegram biathlon
"""
import threading
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def handle_status():
    """Affiche les prochaines courses + statut."""
    from core.telegram import send_message
    from sports.biathlon.jobs import state, BIATHLON_DAYS_AHEAD, ANALYSIS_HOUR

    try:
        from sports.biathlon.biathlon_client import get_upcoming_races
        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)
    except Exception as e:
        send_message(f"❌ Impossible de contacter l'API IBU : {e}")
        return

    last_run = state["last_run"]
    msg = (
        f"🎿 <b>Biathlon — Statut</b>\n\n"
        f"Dernière analyse : {last_run.strftime('%Y-%m-%d %H:%M UTC') if last_run else 'Aucune'}\n"
        f"Analyse auto : {ANALYSIS_HOUR:02d}h30 UTC\n\n"
    )

    if not races:
        msg += "📭 Aucune course dans les prochains jours."
    else:
        msg += f"📅 <b>{len(races)} course(s) à venir :</b>\n"
        for r in races[:5]:
            date     = r.get("date", "")
            name     = r.get("description", "?")
            loc      = r.get("location", "")
            fmt_name = r.get("format_name", "")
            gender   = "♀️" if r.get("gender") == "W" else "♂️"
            msg += f"  • {gender} {date} · <b>{name}</b>"
            if loc:
                msg += f" — {loc}"
            if fmt_name:
                msg += f" ({fmt_name})"
            msg += "\n"

    send_message(msg)


def handle_run():
    from sports.biathlon.jobs import run
    from core.telegram import send_message
    send_message("⚡ <b>Analyse biathlon lancée !</b>")
    threading.Thread(target=run, daemon=True).start()


def handle_results():
    from sports.biathlon.jobs import check_results
    from core.telegram import send_message
    send_message("🔍 <b>Vérification résultats biathlon...</b>")
    threading.Thread(target=check_results, daemon=True).start()


def handle_stats():
    from core.telegram import send_message
    from core.database import get_connection, rows_to_dicts

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result::text = '1'  THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result::text = '0'  THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result::text = '-1' THEN 1 ELSE 0 END) as pending,
                AVG(CASE WHEN result::text != '-1' THEN value_pct ELSE NULL END) as avg_value,
                AVG(CASE WHEN result::text != '-1' THEN odd ELSE NULL END) as avg_odd
            FROM biathlon_bets
        """)
        from core.database import row_to_dict
        r = row_to_dict(cur, cur.fetchone())
    finally:
        conn.close()

    total   = r.get("total") or 0
    wins    = r.get("wins") or 0
    losses  = r.get("losses") or 0
    pending = r.get("pending") or 0
    settled = total - pending

    if total == 0:
        send_message("🎿 <b>Stats biathlon</b>\n\nAucun bet enregistré.")
        return

    win_rate  = round(wins / max(settled, 1) * 100, 1)
    roi       = round((wins - losses) / max(settled, 1) * 100, 1)
    roi_sign  = "+" if roi >= 0 else ""
    avg_value = round((r.get("avg_value") or 0) * 100, 1)

    send_message(
        f"🎿 <b>Stats Biathlon</b>\n\n"
        f"Total : <b>{total}</b> · ✅ {wins} · ❌ {losses} · ⏳ {pending}\n"
        f"Win rate : <b>{win_rate}%</b>\n"
        f"ROI : <b>{roi_sign}{roi}%</b>\n"
        f"Value moy. : <b>+{avg_value}%</b>"
    )