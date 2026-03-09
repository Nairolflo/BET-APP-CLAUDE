"""
biathlon/scheduler.py
---------------------
Worker dédié biathlon — tourne en service Railway indépendant.

Start command Railway : python biathlon/scheduler.py

Partage :
  - DATABASE_URL (même Postgres que le bot foot)
  - ODDS_API_KEY (même quota)
  - TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

Planning automatique :
  07:30 UTC — Analyse H2H + podium pour les courses du jour
  22:00 UTC — Vérification résultats post-course
  Chaque jour — Mise à jour calendrier IBU
"""

import os
import sys
import logging
import threading
from datetime import datetime, timezone, timedelta

# Ajoute le dossier parent au path pour accéder à database.py, telegram_bot.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from apscheduler.schedulers.blocking import BlockingScheduler
from biathlon_client import get_upcoming_races, get_results, CURRENT_SEASON, RACE_FORMATS
from biathlon_model  import predict_h2h, calc_rating, build_athlete_features, detect_h2h_value
from biathlon_odds   import get_biathlon_events, parse_h2h_odds, parse_outright_odds, find_value_bets
from biathlon_bot    import run_biathlon_analysis, predict_h2h_by_name, format_podium_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BIATHLON] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
VALUE_THRESHOLD  = float(os.getenv("BIATHLON_VALUE_THRESHOLD", "0.05"))
DAYS_AHEAD       = int(os.getenv("BIATHLON_DAYS_AHEAD", "3"))
ANALYSIS_HOUR    = int(os.getenv("BIATHLON_ANALYSIS_HOUR", "7"))
RESULTS_HOUR     = int(os.getenv("BIATHLON_RESULTS_HOUR", "22"))

# État worker
worker_state = {
    "running":   False,
    "last_run":  None,
    "bets_today": 0,
}

# ─── Telegram ────────────────────────────────

def send_message(text: str):
    """Envoie un message Telegram (réutilise telegram_bot.py du projet parent)."""
    try:
        from telegram_bot import send_message as _send
        _send(text)
    except ImportError:
        import requests
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10
            )


# ─── Base de données biathlon ─────────────────

def init_biathlon_db():
    """Crée les tables biathlon dans la DB partagée."""
    try:
        import database as db
        conn = db.get_connection()
        cur  = conn.cursor()
        p    = db.ph()

        # Table des bets biathlon
        if db.is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_bets (
                    id              SERIAL PRIMARY KEY,
                    race_id         TEXT,
                    race_name       TEXT,
                    race_date       TEXT,
                    race_format     TEXT,
                    bet_type        TEXT,       -- 'H2H' ou 'TOP3'
                    pick            TEXT,       -- athlète à gagner
                    opponent        TEXT,       -- adversaire (H2H) ou NULL (podium)
                    odd             REAL,
                    bookmaker       TEXT,
                    prob_model      REAL,
                    prob_implied    REAL,
                    value_pct       REAL,
                    kelly           REAL,
                    result          TEXT DEFAULT 'PENDING',  -- WIN/LOSS/VOID/PENDING
                    created_at      TIMESTAMP DEFAULT NOW(),
                    resolved_at     TIMESTAMP
                )
            """)
            # Index pour les requêtes fréquentes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_biathlon_bets_date
                ON biathlon_bets(race_date)
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_bets (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id         TEXT,
                    race_name       TEXT,
                    race_date       TEXT,
                    race_format     TEXT,
                    bet_type        TEXT,
                    pick            TEXT,
                    opponent        TEXT,
                    odd             REAL,
                    bookmaker       TEXT,
                    prob_model      REAL,
                    prob_implied    REAL,
                    value_pct       REAL,
                    kelly           REAL,
                    result          TEXT DEFAULT 'PENDING',
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at     TIMESTAMP
                )
            """)

        # Table cache athlètes (nom → IBU_ID)
        if db.is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_athletes (
                    ibu_id      TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    name_lower  TEXT,
                    nat         TEXT,
                    gender      TEXT,
                    active      BOOLEAN DEFAULT TRUE,
                    updated_at  TIMESTAMP DEFAULT NOW()
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_athletes (
                    ibu_id      TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    name_lower  TEXT,
                    nat         TEXT,
                    gender      TEXT,
                    active      INTEGER DEFAULT 1,
                    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        conn.commit()
        log.info("[DB] Tables biathlon initialisées.")
    except Exception as e:
        log.error(f"[DB] init_biathlon_db: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def save_biathlon_bet(bet: dict):
    """Sauvegarde un value bet biathlon en DB."""
    try:
        import database as db
        conn = db.get_connection()
        cur  = conn.cursor()
        p    = db.ph()

        cur.execute(f"""
            INSERT INTO biathlon_bets
            (race_id, race_name, race_date, race_format, bet_type,
             pick, opponent, odd, bookmaker, prob_model, prob_implied,
             value_pct, kelly)
            VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
        """, (
            bet.get("race_id", ""),
            bet.get("race_name", ""),
            bet.get("race_date", ""),
            bet.get("race_format", ""),
            bet.get("bet_type", "H2H"),
            bet.get("pick", ""),
            bet.get("opponent", ""),
            bet.get("odd", 0),
            bet.get("bookmaker", ""),
            bet.get("prob_model", 0),
            bet.get("prob_implied", 0),
            bet.get("value_pct", 0),
            bet.get("kelly_conservative", 0),
        ))
        conn.commit()
    except Exception as e:
        log.error(f"[DB] save_biathlon_bet: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_pending_biathlon_bets() -> list:
    """Retourne les bets biathlon en attente de résultat."""
    try:
        import database as db
        conn = db.get_connection()
        cur  = conn.cursor()
        p    = db.ph()
        cur.execute(f"""
            SELECT id, race_id, race_name, race_date, race_format,
                   bet_type, pick, opponent, odd, prob_model, value_pct
            FROM biathlon_bets
            WHERE result = {p}
            ORDER BY race_date DESC
        """, ("PENDING",))
        return db.rows_to_dicts(cur, cur.fetchall())
    except Exception as e:
        log.error(f"[DB] get_pending_biathlon_bets: {e}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_biathlon_bet_result(bet_id: int, result: str):
    """Met à jour le résultat d'un bet (WIN/LOSS/VOID)."""
    try:
        import database as db
        conn = db.get_connection()
        cur  = conn.cursor()
        p    = db.ph()
        if db.is_postgres():
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
    except Exception as e:
        log.error(f"[DB] update_biathlon_bet_result: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── Analyse principale ───────────────────────

def run_biathlon_full_analysis(silent: bool = False):
    """
    Analyse complète :
    1. Courses dans les DAYS_AHEAD prochains jours
    2. Cotes H2H disponibles
    3. Calcul probas modèle
    4. Détection value bets
    5. Prédiction podiums
    6. Sauvegarde en DB + envoi Telegram
    """
    if worker_state["running"]:
        send_message("⏳ Analyse biathlon déjà en cours...")
        return

    worker_state["running"] = True
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info(f"🎿 BIATHLON ANALYSIS — {now}")

    if not silent:
        send_message(f"🎿 <b>Analyse Biathlon démarrée</b>\n📅 {now}")

    try:
        # 1. Prochaines courses
        upcoming = get_upcoming_races(days_ahead=DAYS_AHEAD)
        today_races = [
            r for r in upcoming
            if r["date"] == datetime.now(timezone.utc).date().isoformat()
        ]
        log.info(f"  {len(upcoming)} course(s) dans {DAYS_AHEAD} jours, {len(today_races)} aujourd'hui")

        if not upcoming:
            if not silent:
                send_message("📭 <b>Biathlon</b> : aucune course dans les prochains jours.")
            return

        # 2. Cotes H2H
        events      = get_biathlon_events()
        h2h_markets = parse_h2h_odds(events)
        outrights   = parse_outright_odds(events)
        log.info(f"  {len(h2h_markets)} marché(s) H2H, {len(outrights)} outright(s)")

        all_value_bets = []

        # 3. H2H value bets
        if h2h_markets:
            h2h_predictions = {}
            for market in h2h_markets:
                name_a = market["athlete_a"]["name"]
                name_b = market["athlete_b"]["name"]

                # Trouve le format depuis les courses proches
                race_format = _guess_format_from_market(market, upcoming)
                pred = predict_h2h_by_name(name_a, name_b, race_format)
                if pred:
                    h2h_predictions[(name_a.lower(), name_b.lower())] = {
                        "prob_a": pred["prob_a_wins"],
                        "prob_b": pred["prob_b_wins"],
                    }

            value_bets = find_value_bets(h2h_markets, h2h_predictions)
            log.info(f"  {len(value_bets)} value bet(s) H2H détecté(s)")

            for vb in value_bets:
                # Enrichit avec infos de course
                race_info = _find_race_for_market(vb["event"], upcoming)
                vb["race_id"]     = race_info.get("race_id", "")
                vb["race_name"]   = race_info.get("description", vb["event"])
                vb["race_date"]   = race_info.get("date", "")
                vb["race_format"] = race_info.get("format", "SR")
                vb["bet_type"]    = "H2H"
                save_biathlon_bet(vb)

            all_value_bets.extend(value_bets)

        # 4. Podiums (si courses aujourd'hui)
        podium_predictions = []
        for race in today_races:
            if race["format"] in ("SR", "MS", "IN"):  # formats individuels
                log.info(f"  Prédiction podium : {race['description']}")
                try:
                    from biathlon_model import predict_podium
                    podium = predict_podium(race["race_id"], race["format"])
                    if podium:
                        podium_predictions.append({
                            "race": race,
                            "podium": podium[:8]
                        })
                except Exception as e:
                    log.warning(f"  Podium {race['race_id']}: {e}")

        # 5. Envoi résumé
        worker_state["bets_today"] = len(all_value_bets)
        if not silent:
            _send_full_summary(upcoming, all_value_bets, podium_predictions)

    except Exception as e:
        log.error(f"[run_biathlon] {e}", exc_info=True)
        send_message(f"❌ <b>Erreur analyse biathlon</b>\n{e}")
    finally:
        worker_state["running"] = False
        worker_state["last_run"] = datetime.now(timezone.utc)


def check_biathlon_results(silent: bool = False):
    """
    Vérifie les résultats des bets en attente après les courses.
    Lance en soirée (22h UTC) pour attraper toutes les courses de la journée.
    """
    pending = get_pending_biathlon_bets()
    if not pending:
        log.info("[Results] Aucun bet biathlon en attente.")
        return

    log.info(f"[Results] {len(pending)} bet(s) en attente à vérifier")
    resolved_wins  = []
    resolved_losses = []

    for bet in pending:
        race_id     = bet.get("race_id")
        pick        = bet.get("pick", "").upper()
        bet_type    = bet.get("bet_type", "H2H")
        opponent    = bet.get("opponent", "").upper()

        if not race_id:
            continue

        try:
            results = get_results(race_id)
            if not results:
                continue  # Course pas encore officielle

            # Cherche le classement des athlètes concernés
            rank_pick     = None
            rank_opponent = None

            for r in results:
                name = r.get("Name", "").upper()
                if pick in name or name in pick:
                    rank_pick = r.get("Rank")
                if opponent and (opponent in name or name in opponent):
                    rank_opponent = r.get("Rank")

            if rank_pick is None:
                continue  # Pas encore trouvé

            if bet_type == "H2H":
                if rank_opponent is None:
                    continue
                if rank_pick < rank_opponent:
                    update_biathlon_bet_result(bet["id"], "WIN")
                    resolved_wins.append(bet)
                else:
                    update_biathlon_bet_result(bet["id"], "LOSS")
                    resolved_losses.append(bet)

            elif bet_type == "TOP3":
                if rank_pick <= 3:
                    update_biathlon_bet_result(bet["id"], "WIN")
                    resolved_wins.append(bet)
                else:
                    update_biathlon_bet_result(bet["id"], "LOSS")
                    resolved_losses.append(bet)

        except Exception as e:
            log.warning(f"[Results] Bet {bet['id']}: {e}")

    if not silent and (resolved_wins or resolved_losses):
        _send_results_summary(resolved_wins, resolved_losses)


# ─── Helpers ─────────────────────────────────

def _guess_format_from_market(market: dict, upcoming: list) -> str:
    """Devine le format de course depuis le nom du marché."""
    event_str = market.get("event", "").lower()
    for race in upcoming:
        if any(kw in event_str for kw in [
            race["location"].lower()[:5],
            race["description"].lower()[:8],
        ]):
            return race["format"]
    # Heuristique sur le nom
    if "sprint" in event_str:    return "SR"
    if "pursuit" in event_str:   return "PU"
    if "mass" in event_str:      return "MS"
    if "relay" in event_str:     return "RL"
    if "individual" in event_str: return "IN"
    return "SR"  # défaut


def _find_race_for_market(event_str: str, upcoming: list) -> dict:
    """Associe un marché Odds API à une course IBU."""
    event_lower = event_str.lower()
    for race in upcoming:
        if any(kw in event_lower for kw in [
            race["location"].lower()[:5],
            race["description"].lower()[:5],
        ]):
            return race
    return {}


def _send_full_summary(upcoming: list, value_bets: list, podiums: list):
    """Formate et envoie le résumé complet."""
    lines = [f"🎿 <b>BIATHLON — {datetime.now(timezone.utc).strftime('%d/%m/%Y')}</b>\n"]

    # Prochaines courses
    lines.append(f"📅 <b>{len(upcoming)} prochaine(s) course(s) :</b>")
    for r in upcoming[:4]:
        fmt    = RACE_FORMATS.get(r["format"], r["format"])
        gender = "♀️" if r["gender"] == "W" else "♂️"
        lines.append(f"  {gender} {r['date']} · <b>{fmt}</b> — {r['location']}")

    lines.append("")

    # Value bets H2H
    if value_bets:
        lines.append(f"🎯 <b>{len(value_bets)} VALUE BET(S) DÉTECTÉ(S) :</b>")
        for vb in value_bets[:6]:
            emoji = "🔥" if vb["value_pct"] >= 10 else "✅"
            lines.append(
                f"\n{emoji} <b>{vb['pick']}</b> vs {vb['opponent']}\n"
                f"   💰 Cote {vb['odd']} · Value <b>+{vb['value_pct']:.1f}%</b>\n"
                f"   📊 Modèle {vb['prob_model']*100:.0f}% vs BK {vb['prob_implied']*100:.0f}%\n"
                f"   📐 Kelly: {vb['kelly_conservative']*100:.1f}%"
            )
    else:
        lines.append("📭 Aucun value bet H2H détecté (seuil 5%)")

    # Podiums
    if podiums:
        lines.append("\n")
        for pred in podiums[:2]:
            race = pred["race"]
            fmt  = RACE_FORMATS.get(race["format"], race["format"])
            gender = "♀️" if race["gender"] == "W" else "♂️"
            lines.append(f"🏆 <b>Podium prédit — {gender} {fmt} {race['location']}</b>")
            for i, a in enumerate(pred["podium"][:3]):
                medals = ["🥇", "🥈", "🥉"]
                lines.append(
                    f"  {medals[i]} {a['name']} ({a.get('nat','')}) "
                    f"P={a['p_top3']*100:.0f}%"
                )

    lines.append("\n⚠️ <i>Pariez de façon responsable.</i>")
    send_message("\n".join(lines))


def _send_results_summary(wins: list, losses: list):
    """Envoie le résumé des résultats post-course."""
    total   = len(wins) + len(losses)
    win_rate = len(wins) / total * 100 if total else 0

    lines = [f"📊 <b>Résultats Biathlon</b>\n"]
    lines.append(f"✅ {len(wins)} gagné(s) · ❌ {len(losses)} perdu(s) · Win rate: {win_rate:.0f}%\n")

    for w in wins:
        lines.append(f"✅ <b>{w['pick']}</b> bat {w['opponent']} @ {w['odd']}")
    for l in losses:
        lines.append(f"❌ {l['pick']} bat {l['opponent']} @ {l['odd']}")

    send_message("\n".join(lines))


# ─── Commandes Telegram ───────────────────────

COMMANDS = {
    "/biathlon":    lambda: threading.Thread(
        target=handle_biathlon_status, daemon=True).start(),
    "/biathlonrun": lambda: threading.Thread(
        target=run_biathlon_full_analysis, kwargs={"silent": False}, daemon=True).start(),
    "/biathlonresults": lambda: threading.Thread(
        target=check_biathlon_results, kwargs={"silent": False}, daemon=True).start(),
    "/biathlonstats": handle_biathlon_stats,
}


def handle_biathlon_status():
    """Affiche l'état du worker + prochaines courses."""
    last_run = worker_state.get("last_run")
    last_str = last_run.strftime("%d/%m %H:%M UTC") if last_run else "jamais"

    upcoming = get_upcoming_races(days_ahead=7)
    lines = [
        "🎿 <b>Worker Biathlon</b>\n",
        f"✅ En ligne · Dernier run : {last_str}",
        f"📊 Bets aujourd'hui : {worker_state.get('bets_today', 0)}",
        f"📅 Analyse auto : {ANALYSIS_HOUR:02d}h30 UTC",
        f"📋 Résultats auto : {RESULTS_HOUR:02d}h00 UTC\n",
    ]
    if upcoming:
        lines.append(f"📅 <b>{len(upcoming)} prochaine(s) course(s) :</b>")
        for r in upcoming[:5]:
            fmt    = RACE_FORMATS.get(r["format"], r["format"])
            gender = "♀️" if r["gender"] == "W" else "♀️"
            lines.append(f"  {gender} {r['date']} · {fmt} — {r['location']}")
    else:
        lines.append("📭 Aucune course dans les 7 prochains jours")

    lines.append("\n💬 /biathlonrun · /biathlonresults")
    send_message("\n".join(lines))


def handle_biathlon_stats():
    """Affiche les stats des bets biathlon (ROI global)."""
    try:
        import database as db
        conn = db.get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result='PENDING' THEN 1 ELSE 0 END) as pending,
                AVG(odd) as avg_odd,
                AVG(value_pct) as avg_value,
                SUM(CASE WHEN result='WIN' THEN odd - 1
                         WHEN result='LOSS' THEN -1
                         ELSE 0 END) as profit
            FROM biathlon_bets
        """)
        row = cur.fetchone()
        if not row or row[0] == 0:
            send_message("📭 <b>Biathlon</b> : aucun bet enregistré pour l'instant.")
            return

        total, wins, losses, pending, avg_odd, avg_value, profit = row
        resolved = (wins or 0) + (losses or 0)
        win_rate = (wins or 0) / resolved * 100 if resolved else 0
        roi      = (profit or 0) / resolved * 100 if resolved else 0

        send_message(
            f"📊 <b>Stats Biathlon</b>\n\n"
            f"Total : {total} bets ({pending} en attente)\n"
            f"✅ {wins} gagné(s) · ❌ {losses} perdu(s)\n"
            f"Win rate : <b>{win_rate:.1f}%</b>\n"
            f"ROI : <b>{roi:+.1f}%</b>\n"
            f"Cote moyenne : {avg_odd:.2f}\n"
            f"Value moyenne : +{avg_value:.1f}%\n"
            f"Profit : <b>{profit:+.2f} u</b>"
        )
    except Exception as e:
        send_message(f"❌ Stats biathlon: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── Polling Telegram ────────────────────────

def start_telegram_polling():
    """Polling des commandes Telegram (thread séparé)."""
    import requests as req
    offset = None
    log.info("📱 Telegram polling démarré")

    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset

            resp = req.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params=params, timeout=35
            )
            data = resp.json()

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg    = update.get("message", {})
                text   = msg.get("text", "").strip().split()[0].lower()
                chat   = str(msg.get("chat", {}).get("id", ""))

                if chat != TELEGRAM_CHAT_ID:
                    continue

                if text in COMMANDS:
                    log.info(f"📱 Commande reçue : {text}")
                    try:
                        COMMANDS[text]()
                    except Exception as e:
                        log.error(f"Commande {text}: {e}")

        except Exception as e:
            log.warning(f"[Telegram polling] {e}")
            import time; time.sleep(5)


# ─── Main ─────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 55)
    log.info("🎿 BIATHLON WORKER — Démarrage")
    log.info("=" * 55)

    # Init DB
    init_biathlon_db()

    # Démarrage Telegram polling en thread
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        tg_thread = threading.Thread(target=start_telegram_polling, daemon=True)
        tg_thread.start()

    # Scheduler
    scheduler = BlockingScheduler(timezone="UTC")

    # Analyse H2H + podium chaque matin
    scheduler.add_job(
        run_biathlon_full_analysis, "cron",
        hour=ANALYSIS_HOUR, minute=30, id="biathlon_analysis",
        kwargs={"silent": False}
    )

    # Vérification résultats chaque soir
    scheduler.add_job(
        check_biathlon_results, "cron",
        hour=RESULTS_HOUR, minute=0, id="biathlon_results",
        kwargs={"silent": False}
    )

    # Message de démarrage
    send_message(
        f"🎿 <b>Biathlon Worker démarré</b> — "
        f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        f"📅 Analyse : {ANALYSIS_HOUR:02d}h30 · Résultats : {RESULTS_HOUR:02d}h00\n"
        f"💬 /biathlon pour le statut"
    )

    log.info(f"⏰ Analyse : {ANALYSIS_HOUR:02d}h30 UTC · Résultats : {RESULTS_HOUR:02d}h00 UTC")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Worker biathlon arrêté.")