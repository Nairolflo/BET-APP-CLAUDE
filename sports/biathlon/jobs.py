"""
sports/biathlon/jobs.py
Prédictions H2H biathlon : modèle IBU + cotes Pinnacle guest.
Message Telegram : uniquement les H2H avec cote + probabilité modèle.
"""
import os
import math
import random
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BIATHLON_DAYS_AHEAD = int(os.getenv("BIATHLON_DAYS_AHEAD", 5))
N_RECENT_RACES      = int(os.getenv("BIATHLON_RECENT_RACES", 8))
VALUE_THRESHOLD     = float(os.getenv("VALUE_THRESHOLD", 0.02))

state = {
    "last_run":     None,
    "last_results": None,
    "running":      False,
}


# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────

def init_db():
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
                    odd          REAL DEFAULT 0,
                    bookmaker    TEXT DEFAULT 'Pinnacle',
                    prob_model   REAL,
                    prob_implied REAL DEFAULT 0,
                    value_pct    REAL DEFAULT 0,
                    kelly        REAL DEFAULT 0,
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
                    odd          REAL DEFAULT 0,
                    bookmaker    TEXT DEFAULT 'Pinnacle',
                    prob_model   REAL,
                    prob_implied REAL DEFAULT 0,
                    value_pct    REAL DEFAULT 0,
                    kelly        REAL DEFAULT 0,
                    result       INTEGER DEFAULT -1,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at  TIMESTAMP
                )
            """)
        conn.commit()
    finally:
        conn.close()


def save_bet(bet: dict) -> int:
    from core.database import get_connection, is_postgres, ph
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        cur.execute(f"""
            SELECT id FROM biathlon_bets
            WHERE race_id = {p} AND bet_type = {p} AND pick = {p}
        """, (bet.get("race_id"), bet.get("bet_type"), bet.get("pick")))
        if cur.fetchone():
            return -1
        if is_postgres():
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id,race_name,race_date,race_format,bet_type,
                     pick,opponent,odd,bookmaker,prob_model,prob_implied,value_pct,kelly)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                bet.get("race_id"), bet.get("race_name"), bet.get("race_date"),
                bet.get("race_format"), bet.get("bet_type"), bet.get("pick"),
                bet.get("opponent",""), bet.get("odd",0), bet.get("bookmaker","Pinnacle"),
                bet.get("prob_model",0), bet.get("prob_implied",0),
                bet.get("value_pct",0), bet.get("kelly",0),
            ))
            return cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id,race_name,race_date,race_format,bet_type,
                     pick,opponent,odd,bookmaker,prob_model,prob_implied,value_pct,kelly)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                bet.get("race_id"), bet.get("race_name"), bet.get("race_date"),
                bet.get("race_format"), bet.get("bet_type"), bet.get("pick"),
                bet.get("opponent",""), bet.get("odd",0), bet.get("bookmaker","Pinnacle"),
                bet.get("prob_model",0), bet.get("prob_implied",0),
                bet.get("value_pct",0), bet.get("kelly",0),
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
        cur.execute("SELECT * FROM biathlon_bets WHERE result = -1 ORDER BY race_date")
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()


def update_result(bet_id: int, result: int):
    from core.database import get_connection, ph, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        ts  = "NOW()" if is_postgres() else "CURRENT_TIMESTAMP"
        cur.execute(
            f"UPDATE biathlon_bets SET result={p}, resolved_at={ts} WHERE id={p}",
            (result, bet_id)
        )
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# STATS IBU depuis courses récentes
# ─────────────────────────────────────────────

def _parse_shooting(s: str) -> dict:
    digits = [int(c) for c in s.replace(" ", "") if c in "01"] if s else []
    if not digits:
        return {"acc": None, "prone": None, "standing": None}
    half = len(digits) // 2
    return {
        "acc":      sum(digits) / len(digits),
        "prone":    sum(digits[:half]) / half if half else None,
        "standing": sum(digits[half:]) / (len(digits)-half) if (len(digits)-half) > 0 else None,
    }


def _time_to_sec(t: str):
    if not t:
        return None
    try:
        t = t.lstrip("+").strip()
        parts = t.split(":")
        if len(parts) == 3:
            return int(parts[0])*3600 + int(parts[1])*60 + float(parts[2])
        elif len(parts) == 2:
            return int(parts[0])*60 + float(parts[1])
    except Exception:
        return None


def build_athlete_stats(race_ids: list) -> dict:
    """Stats par athlète depuis les résultats des courses récentes."""
    from sports.biathlon.biathlon_client import get_results
    data = {}
    for race_id in race_ids:
        try:
            results  = get_results(race_id)
            n_finish = len([r for r in results if r.get("Rank")])
            for r in results:
                ibu_id = r.get("IBUId", "")
                if not ibu_id or r.get("IRM") or not r.get("Rank"):
                    continue
                shoot = _parse_shooting(r.get("Shootings", ""))
                if ibu_id not in data:
                    data[ibu_id] = {"name": r.get("Name",""), "nat": r.get("Nat",""), "res": []}
                data[ibu_id]["res"].append({
                    "rank":     int(r["Rank"]),
                    "n_fin":    n_finish,
                    "prone":    shoot["prone"],
                    "standing": shoot["standing"],
                    "acc":      shoot["acc"],
                    "run_sec":  _time_to_sec(r.get("RunTime","")),
                })
        except Exception as e:
            log.warning(f"[Biathlon] stats {race_id}: {e}")

    stats = {}
    for ibu_id, d in data.items():
        res = d["res"]
        if not res:
            continue
        n      = len(res)
        ranks  = [r["rank"] for r in res]
        n_fins = [r["n_fin"] for r in res]
        prones    = [r["prone"]    for r in res if r["prone"]    is not None]
        standings = [r["standing"] for r in res if r["standing"] is not None]
        runs      = [r["run_sec"]  for r in res if r["run_sec"]  is not None]

        rel_ranks = [rk/max(nf,1) for rk,nf in zip(ranks,n_fins)]
        stats[ibu_id] = {
            "name":         d["name"],
            "nat":          d["nat"],
            "n_races":      n,
            "avg_rank":     sum(ranks)/n,
            "avg_rel_rank": sum(rel_ranks)/n,
            "top3_rate":    sum(1 for rk in ranks if rk<=3)/n,
            "win_rate":     sum(1 for rk in ranks if rk==1)/n,
            "prone_acc":    sum(prones)/len(prones)       if prones    else 0.82,
            "standing_acc": sum(standings)/len(standings) if standings else 0.78,
            "avg_run_sec":  sum(runs)/len(runs)           if runs      else None,
        }
    return stats


def calc_rating(s: dict, fmt: str) -> float:
    """Score composite 0-1 depuis stats IBU."""
    w = {
        "SP": (0.45, 0.40, 0.15),
        "PU": (0.50, 0.35, 0.15),
        "IN": (0.38, 0.47, 0.15),
        "MS": (0.55, 0.30, 0.15),
    }.get(fmt, (0.45, 0.40, 0.15))
    ski_score   = max(0, 1.0 - s.get("avg_rel_rank", 0.5))
    shoot_score = s.get("prone_acc", 0.82)*0.5 + s.get("standing_acc", 0.78)*0.5
    form_score  = s.get("top3_rate", 0.1)
    return w[0]*ski_score + w[1]*shoot_score + w[2]*form_score


def h2h_prob(ra: float, rb: float) -> float:
    return 1 / (1 + math.exp(-15 * (ra - rb)))


# ─────────────────────────────────────────────
# MATCHING noms IBU ↔ Pinnacle
# ─────────────────────────────────────────────

def _normalize(name: str) -> str:
    """Normalise un nom pour la comparaison : minuscules, sans accents."""
    import unicodedata
    name = unicodedata.normalize("NFD", name.lower())
    return "".join(c for c in name if unicodedata.category(c) != "Mn")


def match_pinnacle_to_ibu(pin_name: str, stats: dict) -> tuple[str, str] | None:
    """
    Cherche l'IBU id correspondant au nom Pinnacle.
    Pinnacle = "Boe J." ou "Boe Johannes" ou "Johannes Boe"
    IBU = "Johannes Thingnes Boe" ou "BOE Johannes Thingnes"
    Retourne (ibu_id, ibu_name) ou None.
    """
    pin_norm  = _normalize(pin_name)
    pin_parts = set(pin_norm.split())

    best_id, best_name, best_score = None, None, 0
    for ibu_id, s in stats.items():
        ibu_norm  = _normalize(s["name"])
        ibu_parts = set(ibu_norm.split())
        common    = pin_parts & ibu_parts
        score     = len(common)
        # Bonus si le nom de famille correspond (token le plus long)
        if score > 0:
            pin_long = max(pin_parts, key=len)
            ibu_long = max(ibu_parts, key=len)
            if pin_long == ibu_long or pin_long in ibu_norm or ibu_long in pin_norm:
                score += 2
        if score > best_score:
            best_score = score
            best_id    = ibu_id
            best_name  = s["name"]

    return (best_id, best_name) if best_score >= 1 else None


# ─────────────────────────────────────────────
# RUN PRINCIPAL
# ─────────────────────────────────────────────

def run(silent=False):
    from core.telegram import send_message

    if state["running"]:
        if not silent:
            send_message("⏳ Analyse biathlon déjà en cours...")
        return

    state["running"] = True
    log.info("[Biathlon] Analyse H2H démarrée")

    try:
        from sports.biathlon.pinnacle_client import get_h2h_odds
        from sports.biathlon.biathlon_client import (
            get_upcoming_races, RACE_FORMATS,
            preload_competitions, CURRENT_SEASON, PREV_SEASON,
            get_recent_race_ids,
        )

        # ── 1. Cotes H2H Pinnacle ──
        log.info("[Biathlon] Récupération cotes Pinnacle...")
        h2h_pinnacle = get_h2h_odds()
        log.info(f"[Biathlon] {len(h2h_pinnacle)} H2H Pinnacle disponibles")

        # ── 2. Courses à venir ──
        preload_competitions(CURRENT_SEASON)
        preload_competitions(PREV_SEASON)
        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)

        if not races and not h2h_pinnacle:
            if not silent:
                send_message("🎿 <b>Biathlon</b> : Aucune course ni cote disponible.")
            state["running"] = False
            return

        # ── 3. Stats IBU (une seule fois pour toutes les courses) ──
        # Prendre les 8 derniers sprints H+F pour avoir les stats générales
        all_race_ids = []
        for gender in ["M", "W"]:
            for fmt in ["SP", "PU", "IN"]:
                ids = get_recent_race_ids(gender=gender, fmt_code=fmt,
                                          season=CURRENT_SEASON, n=4)
                all_race_ids.extend(ids)
        all_race_ids = list(dict.fromkeys(all_race_ids))  # dédoublonner
        log.info(f"[Biathlon] Calcul stats sur {len(all_race_ids)} courses récentes")
        ibu_stats = build_athlete_stats(all_race_ids)
        log.info(f"[Biathlon] Stats calculées pour {len(ibu_stats)} athlètes")

        # ── 4. Message : H2H Pinnacle enrichis avec modèle IBU ──
        msg = "🎿 <b>Biathlon — H2H Pinnacle</b>\n\n"

        if h2h_pinnacle:
            value_found = False
            for h2h in h2h_pinnacle:
                odd_a = h2h["odd_a"]
                odd_b = h2h["odd_b"]

                # Probabilité implicite (avec marge bookmaker)
                prob_imp_a = 1 / odd_a
                prob_imp_b = 1 / odd_b

                # Chercher les stats IBU pour chaque athlète
                match_a = match_pinnacle_to_ibu(h2h["athlete_a"], ibu_stats)
                match_b = match_pinnacle_to_ibu(h2h["athlete_b"], ibu_stats)

                if match_a and match_b:
                    sa = ibu_stats[match_a[0]]
                    sb = ibu_stats[match_b[0]]
                    fmt = "SP"  # format par défaut pour le rating
                    ra  = calc_rating(sa, fmt)
                    rb  = calc_rating(sb, fmt)
                    prob_model_a = h2h_prob(ra, rb)
                    prob_model_b = 1 - prob_model_a

                    value_a = (prob_model_a * odd_a) - 1
                    value_b = (prob_model_b * odd_b) - 1

                    # Afficher uniquement si value bet ou toujours ?
                    # On affiche tout mais on met ✅ sur les values
                    fav_idx  = "a" if prob_model_a >= prob_model_b else "b"
                    fav_name = h2h["athlete_a"] if fav_idx == "a" else h2h["athlete_b"]
                    fav_prob = prob_model_a if fav_idx == "a" else prob_model_b
                    fav_odd  = odd_a if fav_idx == "a" else odd_b
                    fav_val  = value_a if fav_idx == "a" else value_b
                    und_name = h2h["athlete_b"] if fav_idx == "a" else h2h["athlete_a"]
                    und_prob = prob_model_b if fav_idx == "a" else prob_model_a
                    und_odd  = odd_b if fav_idx == "a" else odd_a
                    und_val  = value_b if fav_idx == "a" else value_a

                    is_value = fav_val > VALUE_THRESHOLD
                    value_icon = "✅" if is_value else "•"
                    if is_value:
                        value_found = True

                    shoot_a = sa.get("prone_acc",0)*50 + sa.get("standing_acc",0)*50
                    shoot_b = sb.get("prone_acc",0)*50 + sb.get("standing_acc",0)*50

                    msg += (
                        f"{value_icon} <b>{fav_name}</b> {sa.get('nat','')} "
                        f"<b>{round(fav_prob*100)}%</b> @ <b>{fav_odd}</b>\n"
                        f"   vs {und_name} {sb.get('nat','')} "
                        f"{round(und_prob*100)}% @ {und_odd}\n"
                        f"   🎯 {round(shoot_a)}% vs {round(shoot_b)}%"
                        f" · {sa.get('n_races',0)} vs {sb.get('n_races',0)} courses\n"
                    )
                    if is_value:
                        msg += f"   📈 Edge: +{round(fav_val*100, 1)}%\n"
                    msg += "\n"

                    save_bet({
                        "race_id":      str(h2h["matchup_id"]),
                        "race_name":    h2h["league"],
                        "race_date":    h2h["start_time"][:10] if h2h.get("start_time") else "",
                        "race_format":  "H2H",
                        "bet_type":     "H2H",
                        "pick":         fav_name,
                        "opponent":     und_name,
                        "odd":          fav_odd,
                        "bookmaker":    "Pinnacle",
                        "prob_model":   round(fav_prob, 4),
                        "prob_implied": round(1/fav_odd, 4),
                        "value_pct":    round(fav_val*100, 2),
                    })

                else:
                    # Pas de stats IBU — afficher juste les cotes Pinnacle
                    msg += (
                        f"• <b>{h2h['athlete_a']}</b> @ {odd_a}"
                        f" vs <b>{h2h['athlete_b']}</b> @ {odd_b}\n"
                        f"  <i>(pas de stats IBU)</i>\n\n"
                    )

            if not value_found:
                msg += "💡 <i>Aucun value bet détecté — cotes Pinnacle alignées avec le modèle</i>\n"

        else:
            # Pas de cotes Pinnacle — mode prédictions IBU seules
            msg += "⚠️ <i>Pas de H2H Pinnacle disponibles actuellement</i>\n"
            msg += "<i>Courses à venir :</i>\n"
            for race in races[:5]:
                msg += f"  • {race.get('description','')} — {race.get('date','')}\n"

        msg += "\n🏦 <i>Cotes Pinnacle — référence de marché</i>"

        state["last_run"] = datetime.now(timezone.utc)
        if not silent:
            send_message(msg)

    except Exception as e:
        log.error(f"[Biathlon] run error: {e}", exc_info=True)
        if not silent:
            send_message(f"❌ <b>Erreur biathlon</b> : {e}")
    finally:
        state["running"] = False


# ─────────────────────────────────────────────
# CHECK RESULTS
# ─────────────────────────────────────────────

def check_results(silent=False):
    from core.telegram import send_message
    from sports.biathlon.biathlon_client import get_results

    pending = get_pending_bets()
    if not pending:
        if not silent:
            send_message("🎿 Aucun bet biathlon en attente.")
        return

    won, lost = [], []
    for bet in pending:
        try:
            results = get_results(bet["race_id"])
            if not results:
                continue
            pick_rank = next((int(r["Rank"]) for r in results
                              if bet["pick"].lower() in r.get("Name","").lower()
                              and r.get("Rank")), None)
            opp_rank  = next((int(r["Rank"]) for r in results
                              if bet.get("opponent","").lower() in r.get("Name","").lower()
                              and r.get("Rank")), None)
            if pick_rank is None or opp_rank is None:
                continue
            success = 1 if pick_rank < opp_rank else 0
            update_result(bet["id"], success)
            (won if success else lost).append(bet)
        except Exception as e:
            log.warning(f"[Biathlon] check_result {bet['id']}: {e}")

    state["last_results"] = datetime.now(timezone.utc)

    if not won and not lost:
        if not silent:
            send_message("⏳ Résultats biathlon pas encore disponibles.")
        return

    msg = "🎿 <b>Résultats biathlon</b>\n\n"
    if won:
        msg += f"✅ <b>Gagnés ({len(won)})</b>\n"
        for b in won:
            msg += f"  • {b['pick']} vs {b['opponent']} — {b['race_name']}\n"
    if lost:
        msg += f"\n❌ <b>Perdus ({len(lost)})</b>\n"
        for b in lost:
            msg += f"  • {b['pick']} vs {b['opponent']} — {b['race_name']}\n"

    if not silent:
        send_message(msg)

# Constantes attendues par le scheduler
ANALYSIS_HOUR = int(os.getenv("BIATHLON_ANALYSIS_HOUR", 7))
RESULTS_HOUR  = int(os.getenv("BIATHLON_RESULTS_HOUR", 22))