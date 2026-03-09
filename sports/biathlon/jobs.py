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
    from core.database import get_connection, is_postgres
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
    from core.database import get_connection, is_postgres, ph
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
    from core.database import get_connection, rows_to_dicts
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
    from core.database import get_connection, ph
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
    """
    Analyse biathlon — mode prédiction pure (sans cotes externes).
    Récupère le classement CdM, génère les H2H entre top athlètes,
    et envoie les prédictions sur Telegram.
    """
    from core.telegram import send_message

    if state["running"]:
        send_message("⏳ Analyse biathlon déjà en cours...")
        return

    state["running"] = True
    log.info("[Biathlon] Analyse démarrée (mode prédiction pure)")

    try:
        from sports.biathlon.biathlon_client import get_upcoming_races, get_cup_standings, RACE_FORMATS
        from sports.biathlon.biathlon_model import predict_h2h

        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)
        if not races:
            if not silent:
                send_message("🎿 <b>Biathlon</b> : Aucune course dans les prochains jours.")
            state["running"] = False
            return

        msg = "🎿 <b>Prédictions Biathlon</b>\n\n"

        for race in races[:3]:
            race_id     = race.get("race_id", "")
            description = race.get("description", "Course")
            race_date   = race.get("date", "")
            fmt_code    = race.get("format", "SR")
            fmt_name    = race.get("format_name") or RACE_FORMATS.get(fmt_code, fmt_code)
            location    = race.get("location", "")
            gender      = race.get("gender", "M")

            # Titre de la course
            gender_icon = "♀️" if gender == "W" else "♂️"
            msg += f"{gender_icon} <b>{description}</b>\n"
            msg += f"📅 {race_date}"
            if location:
                msg += f" · {location}"
            if fmt_name:
                msg += f" · {fmt_name}"
            msg += "\n"

            # Top athlètes depuis le classement CdM
            standings = get_cup_standings(gender=gender)
            top_athletes = []
            for row in standings[:12]:
                ibu_id = row.get("IBU_ID") or row.get("Id") or row.get("ibu_id", "")
                name   = row.get("Name") or row.get("name", "")
                nat    = row.get("Nat") or row.get("nat", "")
                if ibu_id and name:
                    top_athletes.append({"ibu_id": ibu_id, "name": name, "nat": nat})

            if len(top_athletes) < 2:
                msg += "<i>Classement CdM non disponible</i>\n\n"
                continue

            # H2H entre le top 1 et les suivants (max 4 duels)
            msg += "\n⚔️ <b>H2H favoris</b>\n"
            predicted = 0
            for i in range(min(4, len(top_athletes) - 1)):
                a = top_athletes[i]
                b = top_athletes[i + 1]
                try:
                    h2h = predict_h2h(a["ibu_id"], b["ibu_id"], fmt_code)
                    if not h2h:
                        continue
                    prob_a = h2h["prob_a_wins"]
                    prob_b = h2h["prob_b_wins"]
                    fav    = a if prob_a >= prob_b else b
                    fav_p  = max(prob_a, prob_b)
                    und    = b if prob_a >= prob_b else a
                    und_p  = min(prob_a, prob_b)

                    msg += (
                        f"  • <b>{fav['name']}</b> {fav.get('nat','')} "
                        f"<b>{round(fav_p*100)}%</b> "
                        f"vs {und['name']} {und.get('nat','')} {round(und_p*100)}%\n"
                    )

                    # Sauvegarde en DB
                    save_bet({
                        "race_id":      race_id,
                        "race_name":    description,
                        "race_date":    race_date,
                        "race_format":  fmt_code,
                        "bet_type":     "H2H",
                        "pick":         fav["name"],
                        "opponent":     und["name"],
                        "odd":          0,
                        "bookmaker":    "IBU Model",
                        "prob_model":   fav_p,
                        "prob_implied": 0,
                        "value_pct":    0,
                        "kelly":        0,
                    })
                    predicted += 1
                except Exception as e:
                    log.warning(f"[Biathlon] H2H {a['name']} vs {b['name']}: {e}")

            if predicted == 0:
                msg += "<i>Données insuffisantes pour les prédictions</i>\n"

            msg += "<i>💡 Consultez Unibet/Betclic pour les cotes</i>\n\n"

        state["last_run"] = datetime.now(timezone.utc)
        state["running"]  = False

        if not silent:
            send_message(msg)

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
        from sports.biathlon.biathlon_client import get_competitions, get_results
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