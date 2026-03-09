"""
sports/biathlon/jobs.py — Jobs biathlon (analyse, résultats)
"""
import os
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BIATHLON_VALUE_THRESHOLD = float(os.getenv("BIATHLON_VALUE_THRESHOLD", 0.05))
BIATHLON_DAYS_AHEAD      = int(os.getenv("BIATHLON_DAYS_AHEAD", 3))
ANALYSIS_HOUR            = int(os.getenv("BIATHLON_ANALYSIS_HOUR", 7))
RESULTS_HOUR             = int(os.getenv("BIATHLON_RESULTS_HOUR", 22))

state = {
    "last_run":     None,
    "last_results": None,
    "running":      False,
}


def init_db():
    """Crée les tables biathlon si elles n'existent pas."""
    from database import get_connection, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_bets (
                    id           SERIAL PRIMARY KEY,
                    race_id      TEXT,
                    race_name    TEXT,
                    race_date    TEXT,
                    race_format  TEXT,
                    bet_type     TEXT,
                    pick         TEXT,
                    opponent     TEXT,
                    odd          REAL,
                    bookmaker    TEXT,
                    prob_model   REAL,
                    prob_implied REAL,
                    value_pct    REAL,
                    kelly        REAL,
                    result       INTEGER DEFAULT -1,
                    created_at   TIMESTAMP DEFAULT NOW(),
                    resolved_at  TIMESTAMP
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_bets (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id      TEXT,
                    race_name    TEXT,
                    race_date    TEXT,
                    race_format  TEXT,
                    bet_type     TEXT,
                    pick         TEXT,
                    opponent     TEXT,
                    odd          REAL,
                    bookmaker    TEXT,
                    prob_model   REAL,
                    prob_implied REAL,
                    value_pct    REAL,
                    kelly        REAL,
                    result       INTEGER DEFAULT -1,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at  TIMESTAMP
                )
            """)
        conn.commit()
        log.info("[Biathlon] Tables DB initialisées")
    finally:
        conn.close()


def save_bet(bet: dict) -> int:
    from database import get_connection, is_postgres, ph
    conn = get_connection()
    try:
        cur  = conn.cursor()
        p    = ph()
        # Anti-doublon
        cur.execute(f"""
            SELECT id FROM biathlon_bets
            WHERE race_id = {p} AND bet_type = {p} AND pick = {p}
        """, (bet.get("race_id"), bet.get("bet_type"), bet.get("pick")))
        existing = cur.fetchone()
        if existing:
            return existing[0]

        if is_postgres():
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id, race_name, race_date, race_format, bet_type,
                     pick, opponent, odd, bookmaker, prob_model, prob_implied,
                     value_pct, kelly)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                bet.get("race_id"), bet.get("race_name"), bet.get("race_date"),
                bet.get("race_format"), bet.get("bet_type"), bet.get("pick"),
                bet.get("opponent"), bet.get("odd"), bet.get("bookmaker"),
                bet.get("prob_model"), bet.get("prob_implied"),
                bet.get("value_pct"), bet.get("kelly"),
            ))
            return cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id, race_name, race_date, race_format, bet_type,
                     pick, opponent, odd, bookmaker, prob_model, prob_implied,
                     value_pct, kelly)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                bet.get("race_id"), bet.get("race_name"), bet.get("race_date"),
                bet.get("race_format"), bet.get("bet_type"), bet.get("pick"),
                bet.get("opponent"), bet.get("odd"), bet.get("bookmaker"),
                bet.get("prob_model"), bet.get("prob_implied"),
                bet.get("value_pct"), bet.get("kelly"),
            ))
            return cur.lastrowid
    finally:
        conn.commit()
        conn.close()


def get_pending_bets() -> list:
    from database import get_connection, rows_to_dicts
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM biathlon_bets WHERE result = -1
            ORDER BY race_date ASC
        """)
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()


def update_result(bet_id: int, result: int):
    from database import get_connection, ph
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        if hasattr(conn, 'autocommit'):  # postgres
            cur.execute(f"""
                UPDATE biathlon_bets
                SET result = {p}, resolved_at = NOW()
                WHERE id = {p}
            """, (result, bet_id))
        else:
            cur.execute(f"""
                UPDATE biathlon_bets
                SET result = {p}, resolved_at = CURRENT_TIMESTAMP
                WHERE id = {p}
            """, (result, bet_id))
        conn.commit()
    finally:
        conn.close()


def run(silent=False):
    """Analyse principale biathlon : courses → cotes → value bets → DB → Telegram."""
    from core.telegram import send_message

    if state["running"]:
        send_message("⏳ Analyse biathlon déjà en cours...")
        return

    state["running"] = True
    log.info("[Biathlon] Analyse démarrée")

    try:
        # Import des modules biathlon existants
        from biathlon.biathlon_client import get_upcoming_races
        from biathlon.biathlon_odds   import get_biathlon_events, parse_h2h_odds, find_value_bets
        from biathlon.biathlon_model  import predict_h2h

        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)
        if not races:
            if not silent:
                send_message("🎿 <b>Biathlon</b> : Aucune course dans les prochains jours.")
            state["running"] = False
            return

        if not silent:
            send_message(
                f"🎿 <b>Analyse biathlon démarrée</b>\n"
                f"{len(races)} course(s) trouvée(s)"
            )

        # Cotes
        events   = get_biathlon_events()
        h2h_mkt  = parse_h2h_odds(events)
        value_bets = find_value_bets(h2h_mkt, {}, threshold=BIATHLON_VALUE_THRESHOLD)

        saved = 0
        msg_bets = ""
        for vb in value_bets:
            try:
                race = next((r for r in races if r.get("RaceId") == vb.get("race_id")), {})
                bet_id = save_bet({
                    "race_id":     vb.get("race_id", ""),
                    "race_name":   race.get("ShortDescription", "Course"),
                    "race_date":   race.get("StartTime", "")[:10],
                    "race_format": race.get("RaceTypeId", ""),
                    "bet_type":    "H2H",
                    "pick":        vb.get("athlete_a", ""),
                    "opponent":    vb.get("athlete_b", ""),
                    "odd":         vb.get("odd", 0),
                    "bookmaker":   vb.get("bookmaker", ""),
                    "prob_model":  vb.get("prob_model", 0),
                    "prob_implied": vb.get("prob_implied", 0),
                    "value_pct":   vb.get("value_pct", 0),
                    "kelly":       vb.get("kelly", 0),
                })
                saved += 1
                msg_bets += (
                    f"  <b>{vb.get('athlete_a')} vs {vb.get('athlete_b')}</b>\n"
                    f"  @ <b>{vb.get('odd')}</b> · +{vb.get('value_pct',0)*100:.1f}% · "
                    f"Modèle {vb.get('prob_model',0)*100:.0f}%\n\n"
                )
            except Exception as e:
                log.error(f"[Biathlon] save_bet: {e}")

        state["last_run"] = datetime.now(timezone.utc)
        state["running"]  = False

        if not silent:
            if saved > 0:
                send_message(
                    f"🎯 <b>Biathlon — {saved} value bet(s)</b>\n\n"
                    + msg_bets +
                    "⚠️ <i>Pariez de façon responsable.</i>"
                )
            else:
                send_message("🎿 <b>Biathlon</b> : Aucun value bet trouvé.")

    except Exception as e:
        state["running"] = False
        log.error(f"[Biathlon] run error: {e}")
        if not silent:
            send_message(f"❌ <b>Erreur analyse biathlon</b> : {e}")


def check_results(silent=False):
    """Vérifie les résultats des bets biathlon en attente."""
    from core.telegram import send_message

    pending = get_pending_bets()
    if not pending:
        if not silent:
            send_message("🎿 Aucun bet biathlon en attente.")
        return

    try:
        from biathlon.biathlon_client import get_competitions, get_results
    except ImportError as e:
        log.error(f"[Biathlon] Import error: {e}")
        return

    won, lost = [], []

    for bet in pending:
        try:
            results = get_results(bet["race_id"])
            if not results:
                continue
            # Cherche la position du pick dans les résultats
            pick_pos = next(
                (r.get("Rank") for r in results
                 if bet["pick"].lower() in r.get("Name", "").lower()),
                None
            )
            opp_pos = next(
                (r.get("Rank") for r in results
                 if bet.get("opponent", "").lower() in r.get("Name", "").lower()),
                None
            )
            if pick_pos is None or opp_pos is None:
                continue

            success = 1 if pick_pos < opp_pos else 0
            update_result(bet["id"], success)
            (won if success == 1 else lost).append(bet)
        except Exception as e:
            log.warning(f"[Biathlon] check_result bet {bet['id']}: {e}")

    state["last_results"] = datetime.now(timezone.utc)

    if not won and not lost:
        if not silent:
            send_message("⏳ Résultats biathlon pas encore disponibles.")
        return

    msg = "🎿 <b>Résultats biathlon</b>\n\n"
    if won:
        msg += f"✅ <b>Gagnés ({len(won)})</b>\n"
        for b in won:
            msg += f"  • {b['pick']} vs {b.get('opponent','')} · {b['race_name']}\n"
    if lost:
        msg += f"\n❌ <b>Perdus ({len(lost)})</b>\n"
        for b in lost:
            msg += f"  • {b['pick']} vs {b.get('opponent','')} · {b['race_name']}\n"

    if not silent:
        send_message(msg)
