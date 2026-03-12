"""
sports/biathlon/jobs.py
Prédictions H2H biathlon : modèle IBU + cotes historiques OddsPortal.
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
                    bookmaker    TEXT DEFAULT 'Winamax',
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
                    bookmaker    TEXT DEFAULT 'Winamax',
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
                bet.get("opponent",""), bet.get("odd",0), bet.get("bookmaker","Winamax"),
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
                bet.get("opponent",""), bet.get("odd",0), bet.get("bookmaker","Winamax"),
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
        # OddsPortal importé plus haut
        from sports.biathlon.biathlon_client import (
            get_upcoming_races, RACE_FORMATS,
            preload_competitions, CURRENT_SEASON, PREV_SEASON,
            get_recent_race_ids,
        )

        # ── 1. Cotes H2H historiques OddsPortal (Winamax/Betclic) ──
        log.info("[Biathlon] Récupération cotes OddsPortal...")
        from sports.biathlon.oddsportal_client import (
            get_biathlon_h2h_history, get_avg_h2h_odds
        )
        h2h_history  = get_biathlon_h2h_history(n_last=5)
        h2h_avg_odds = get_avg_h2h_odds(n_last=5)
        log.info(f"[Biathlon] {len(h2h_history)} courses historiques, "
                 f"{len(h2h_avg_odds)} paires H2H avec cotes")

        # ── 2. Courses à venir ──
        preload_competitions(CURRENT_SEASON)
        preload_competitions(PREV_SEASON)
        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)

        if not races and not h2h_avg_odds:
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
        msg = "🎿 <b>Biathlon — H2H Prédictions</b>\n\n"

        # ── 4. Message : H2H avec cotes OddsPortal + modèle IBU ──
        # Construire les H2H depuis les courses IBU à venir + cotes historiques
        h2h_to_show = []

        # Générer H2H depuis le top des athlètes par stats
        top_athletes = sorted(ibu_stats.items(),
                              key=lambda x: x[1].get("avg_rank", 99))[:12]

        for i in range(min(5, len(top_athletes)-1)):
            ibu_id_a, sa = top_athletes[i]
            ibu_id_b, sb = top_athletes[i+1]

            ra = calc_rating(sa, "SP")
            rb = calc_rating(sb, "SP")
            prob_a = h2h_prob(ra, rb)
            prob_b = 1 - prob_a

            # Chercher cotes historiques OddsPortal pour cette paire
            from sports.biathlon.oddsportal_client import _normalize
            key_sorted = tuple(sorted([sa["name"], sb["name"]]))
            hist = h2h_avg_odds.get(key_sorted)

            # Cote juste calculée
            fair_a = round(1/prob_a, 2) if prob_a > 0.01 else 99.0
            fair_b = round(1/prob_b, 2) if prob_b > 0.01 else 99.0

            h2h_to_show.append({
                "name_a":      sa["name"],
                "name_b":      sb["name"],
                "nat_a":       sa.get("nat",""),
                "nat_b":       sb.get("nat",""),
                "prob_a":      prob_a,
                "prob_b":      prob_b,
                "fair_a":      fair_a,
                "fair_b":      fair_b,
                "hist_odd_a":  hist["avg_odd_a"] if hist else None,
                "hist_odd_b":  hist["avg_odd_b"] if hist else None,
                "bookmaker":   hist["bookmaker"] if hist else None,
                "n_samples":   hist["n_samples"] if hist else 0,
                "shoot_a":     sa.get("prone_acc",0)*50 + sa.get("standing_acc",0)*50,
                "shoot_b":     sb.get("prone_acc",0)*50 + sb.get("standing_acc",0)*50,
                "n_races_a":   sa.get("n_races",0),
                "n_races_b":   sb.get("n_races",0),
            })

        # ── 4. Message final — H2H avec cotes historiques et modèle ──
        upcoming_names = ", ".join(r.get("description","") for r in races[:3]) if races else "—"
        msg += f"📅 <i>Prochaines courses : {upcoming_names}</i>\n\n"
        msg += "⚔️ <b>H2H — Prédictions modèle</b>\n\n"

        value_found = False
        for h2h in h2h_to_show:
            prob_a = h2h["prob_a"]
            prob_b = h2h["prob_b"]
            fav_is_a = prob_a >= prob_b

            fav_name  = h2h["name_a"] if fav_is_a else h2h["name_b"]
            fav_nat   = h2h["nat_a"]  if fav_is_a else h2h["nat_b"]
            fav_prob  = prob_a if fav_is_a else prob_b
            fav_fair  = h2h["fair_a"] if fav_is_a else h2h["fair_b"]
            fav_hist  = h2h["hist_odd_a"] if fav_is_a else h2h["hist_odd_b"]
            fav_shoot = h2h["shoot_a"] if fav_is_a else h2h["shoot_b"]

            und_name  = h2h["name_b"] if fav_is_a else h2h["name_a"]
            und_nat   = h2h["nat_b"]  if fav_is_a else h2h["nat_a"]
            und_prob  = prob_b if fav_is_a else prob_a
            und_fair  = h2h["fair_b"] if fav_is_a else h2h["fair_a"]
            und_hist  = h2h["hist_odd_b"] if fav_is_a else h2h["hist_odd_a"]
            und_shoot = h2h["shoot_b"] if fav_is_a else h2h["shoot_a"]

            # Value bet : cote historique bookmaker > cote juste modèle
            is_value = fav_hist and fav_hist > fav_fair * (1 + VALUE_THRESHOLD)
            icon = "✅" if is_value else "•"
            if is_value:
                value_found = True

            # Ligne principale
            msg += f"{icon} <b>{fav_name}</b> {fav_nat} <b>{round(fav_prob*100)}%</b>"
            msg += f" — cote juste ~{fav_fair}\n"
            msg += f"   vs {und_name} {und_nat} {round(und_prob*100)}%"
            msg += f" — cote juste ~{und_fair}\n"

            # Cotes historiques si disponibles
            if fav_hist and und_hist:
                bk = h2h.get("bookmaker","Winamax")
                n  = h2h.get("n_samples", 0)
                msg += f"   📊 Hist. {bk}: {fav_hist} vs {und_hist}"
                msg += f" <i>({n} course{'s' if n>1 else ''})</i>\n"
                if is_value:
                    edge = round((fav_hist / fav_fair - 1) * 100, 1)
                    msg += f"   📈 Edge estimé: +{edge}%\n"
            else:
                msg += f"   📊 <i>Pas de cotes historiques disponibles</i>\n"

            # Stats tir
            msg += (f"   🎯 {round(fav_shoot)}% vs {round(und_shoot)}%"
                    f" · {h2h['n_races_a']} vs {h2h['n_races_b']} courses\n\n")

            # Sauvegarder en DB
            race_upcoming = races[0] if races else {}
            save_bet({
                "race_id":      race_upcoming.get("race_id", "upcoming"),
                "race_name":    race_upcoming.get("description", "Prochaine course"),
                "race_date":    race_upcoming.get("date", ""),
                "race_format":  race_upcoming.get("format", "SP"),
                "bet_type":     "H2H",
                "pick":         fav_name,
                "opponent":     und_name,
                "odd":          fav_hist or fav_fair,
                "bookmaker":    h2h.get("bookmaker", "IBU Model"),
                "prob_model":   round(fav_prob, 4),
                "prob_implied": round(1/fav_hist, 4) if fav_hist else 0,
                "value_pct":    round((fav_hist/fav_fair - 1)*100, 2) if fav_hist else 0,
            })

        if not value_found:
            msg += "💡 <i>Aucun value bet détecté ce jour</i>\n"

        msg += "\n📊 <i>Cotes historiques Winamax/Betclic via OddsPortal · Modèle IBU</i>"

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