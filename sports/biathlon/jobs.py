"""
sports/biathlon/jobs.py
H2H biathlon par course — genre et format corrects.
"""
import os, math, logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BIATHLON_DAYS_AHEAD = int(os.getenv("BIATHLON_DAYS_AHEAD", 5))
VALUE_THRESHOLD     = float(os.getenv("VALUE_THRESHOLD", 0.02))
ANALYSIS_HOUR       = int(os.getenv("BIATHLON_ANALYSIS_HOUR", 7))
RESULTS_HOUR        = int(os.getenv("BIATHLON_RESULTS_HOUR", 22))

state = {"last_run": None, "last_results": None, "running": False}

# ─── DB ──────────────────────────────────────

def init_db():
    from core.database import get_connection, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""CREATE TABLE IF NOT EXISTS biathlon_bets (
                id SERIAL PRIMARY KEY, race_id TEXT, race_name TEXT,
                race_date TEXT, race_format TEXT, bet_type TEXT,
                pick TEXT, opponent TEXT, odd REAL DEFAULT 0,
                bookmaker TEXT DEFAULT 'IBU Model', prob_model REAL,
                prob_implied REAL DEFAULT 0, value_pct REAL DEFAULT 0,
                kelly REAL DEFAULT 0, result INTEGER DEFAULT -1,
                created_at TIMESTAMP DEFAULT NOW(), resolved_at TIMESTAMP)""")
        else:
            cur.execute("""CREATE TABLE IF NOT EXISTS biathlon_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT, race_id TEXT, race_name TEXT,
                race_date TEXT, race_format TEXT, bet_type TEXT,
                pick TEXT, opponent TEXT, odd REAL DEFAULT 0,
                bookmaker TEXT DEFAULT 'IBU Model', prob_model REAL,
                prob_implied REAL DEFAULT 0, value_pct REAL DEFAULT 0,
                kelly REAL DEFAULT 0, result INTEGER DEFAULT -1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, resolved_at TIMESTAMP)""")
        conn.commit()
    finally:
        conn.close()

def save_bet(bet: dict):
    from core.database import get_connection, is_postgres, ph
    conn = get_connection()
    try:
        cur = conn.cursor(); p = ph()
        cur.execute(
            f"SELECT id FROM biathlon_bets WHERE race_id={p} AND bet_type={p} AND pick={p}",
            (bet["race_id"], bet["bet_type"], bet["pick"]))
        if cur.fetchone(): return
        vals = (bet["race_id"], bet["race_name"], bet["race_date"], bet["race_format"],
                bet["bet_type"], bet["pick"], bet.get("opponent",""), bet.get("odd",0),
                bet.get("bookmaker","IBU Model"), bet.get("prob_model",0), 0, 0, 0)
        if is_postgres():
            cur.execute("""INSERT INTO biathlon_bets
                (race_id,race_name,race_date,race_format,bet_type,pick,opponent,
                 odd,bookmaker,prob_model,prob_implied,value_pct,kelly)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", vals)
        else:
            cur.execute("""INSERT INTO biathlon_bets
                (race_id,race_name,race_date,race_format,bet_type,pick,opponent,
                 odd,bookmaker,prob_model,prob_implied,value_pct,kelly)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", vals)
        conn.commit()
    finally:
        conn.close()

def get_pending_bets() -> list:
    from core.database import get_connection, rows_to_dicts
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM biathlon_bets WHERE result=-1 ORDER BY race_date")
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()

def update_result(bet_id: int, result: int):
    from core.database import get_connection, ph, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor(); p = ph()
        ts = "NOW()" if is_postgres() else "CURRENT_TIMESTAMP"
        cur.execute(f"UPDATE biathlon_bets SET result={p},resolved_at={ts} WHERE id={p}",
                    (result, bet_id))
        conn.commit()
    finally:
        conn.close()

# ─── STATS IBU ───────────────────────────────

def _parse_shooting(s: str) -> dict:
    digits = [int(c) for c in s.replace(" ","") if c in "01"] if s else []
    if not digits: return {"prone": None, "standing": None}
    half = len(digits) // 2
    return {
        "prone":    sum(digits[:half])/half if half else None,
        "standing": sum(digits[half:])/(len(digits)-half) if len(digits)-half else None,
    }

def _time_to_sec(t: str):
    if not t: return None
    try:
        t = t.lstrip("+").strip(); p = t.split(":")
        return int(p[0])*3600+int(p[1])*60+float(p[2]) if len(p)==3 else int(p[0])*60+float(p[1])
    except: return None

def build_stats_for(gender: str, fmt_code: str, n: int = 10) -> dict:
    """
    Stats depuis les N dernières courses (même genre + format).
    - Rang relatif (rank/nb_finishers)
    - Pondération récence exponentielle (course récente pèse plus)
    - Ratio ski : temps_athlète / temps_winner
    """
    from sports.biathlon.biathlon_client import get_results, get_recent_race_ids, \
        CURRENT_SEASON, PREV_SEASON

    race_ids = get_recent_race_ids(gender=gender, fmt_code=fmt_code,
                                    season=CURRENT_SEASON, n=n)
    if len(race_ids) < 3:
        race_ids += get_recent_race_ids(gender=gender, fmt_code=fmt_code,
                                         season=PREV_SEASON, n=n-len(race_ids))
    race_ids = race_ids[:n]

    if not race_ids:
        log.warning(f"[Biathlon] Aucune course récente {fmt_code}/{gender}")
        return {}

    log.info(f"[Biathlon] Stats {fmt_code}/{gender} sur {len(race_ids)} courses")

    data = {}
    for race_idx, race_id in enumerate(race_ids):
        recency_w = 0.85 ** race_idx  # plus récent = poids plus élevé
        try:
            results  = get_results(race_id)
            finished = [r for r in results if r.get("Rank") and not r.get("IRM")]
            n_fin    = len(finished)
            if n_fin < 5:
                continue

            # Temps du vainqueur
            win_time = None
            for r in finished:
                if int(r["Rank"]) == 1:
                    win_time = _time_to_sec(r.get("RunTime", ""))
                    break

            for r in finished:
                ibu  = r.get("IBUId", "")
                if not ibu: continue
                sh   = _parse_shooting(r.get("Shootings", ""))
                rank = int(r["Rank"])
                run_t = _time_to_sec(r.get("RunTime", ""))
                ski_ratio = (win_time / run_t) if (run_t and win_time and run_t > 0) else None

                if ibu not in data:
                    data[ibu] = {"name": r.get("Name",""), "nat": r.get("Nat",""), "res": []}
                data[ibu]["res"].append({
                    "rank":      rank,
                    "n_fin":     n_fin,
                    "rel_rank":  rank / n_fin,
                    "ski_ratio": ski_ratio,
                    "prone":     sh["prone"],
                    "standing":  sh["standing"],
                    "weight":    recency_w,
                })
        except Exception as e:
            log.warning(f"[Biathlon] stats {race_id}: {e}")

    stats = {}
    for ibu, d in data.items():
        res = d["res"]
        if not res: continue
        total_w = sum(r["weight"] for r in res)

        avg_rel_rank = sum(r["rel_rank"]  * r["weight"] for r in res) / total_w
        avg_rank     = sum(r["rank"]      * r["weight"] for r in res) / total_w
        top3_rate    = sum(r["weight"] for r in res if r["rank"] <= 3) / total_w

        prones    = [(r["prone"],     r["weight"]) for r in res if r["prone"]     is not None]
        standings = [(r["standing"],  r["weight"]) for r in res if r["standing"]  is not None]
        ski_rats  = [(r["ski_ratio"], r["weight"]) for r in res if r["ski_ratio"] is not None]

        prone_acc    = sum(v*w for v,w in prones)    / sum(w for _,w in prones)    if prones    else 0.82
        standing_acc = sum(v*w for v,w in standings) / sum(w for _,w in standings) if standings else 0.78
        ski_score    = sum(v*w for v,w in ski_rats)  / sum(w for _,w in ski_rats)  if ski_rats  else (1 - avg_rel_rank)

        stats[ibu] = {
            "name":         d["name"], "nat": d["nat"],
            "n_races":      len(res),
            "avg_rank":     round(avg_rank, 2),
            "avg_rel_rank": round(avg_rel_rank, 4),
            "top3_rate":    round(top3_rate, 4),
            "prone_acc":    round(prone_acc, 4),
            "standing_acc": round(standing_acc, 4),
            "ski_score":    round(ski_score, 4),
        }
    return stats

def calc_rating(s: dict, fmt: str) -> float:
    """Score composite — ski basé sur ratio temps, tir, forme récente."""
    w = {
        "SP": (0.42, 0.40, 0.18),
        "PU": (0.48, 0.35, 0.17),
        "IN": (0.35, 0.48, 0.17),
        "MS": (0.52, 0.30, 0.18),
    }.get(fmt, (0.44, 0.38, 0.18))
    ski   = min(s.get("ski_score", 1 - s.get("avg_rel_rank", 0.5)), 1.0)
    shoot = s.get("prone_acc", 0.82) * 0.5 + s.get("standing_acc", 0.78) * 0.5
    form  = s.get("top3_rate", 0.1)
    return w[0]*ski + w[1]*shoot + w[2]*form

def h2h_prob(ra: float, rb: float) -> float:
    return 1 / (1 + math.exp(-15*(ra-rb)))

def _gender_icon(g: str) -> str:
    return "♀️" if g == "W" else "♂️"

def _fmt_name(fmt: str) -> str:
    return {"SP":"Sprint","PU":"Poursuite","IN":"Individuelle",
            "MS":"Mass Start","RL":"Relais","SR":"Relais Mixte"}.get(fmt, fmt)

# ─── RUN ─────────────────────────────────────

def run(silent=False):
    from core.telegram import send_message

    if state["running"]:
        if not silent: send_message("⏳ Analyse biathlon déjà en cours...")
        return
    state["running"] = True

    try:
        from sports.biathlon.biathlon_client import (
            get_upcoming_races, preload_competitions, CURRENT_SEASON, PREV_SEASON
        )

        preload_competitions(CURRENT_SEASON)
        preload_competitions(PREV_SEASON)
        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)

        # Filtrer : pas de relais (on ne peut pas faire de H2H individuel)
        races = [r for r in races if r.get("format") not in ("RL","SR","MX","SI")]

        if not races:
            if not silent: send_message("🎿 Biathlon : aucune course individuelle à venir.")
            state["running"] = False
            return

        msg = "🎿 <b>Biathlon — H2H par course</b>\n\n"

        _winamax_available = False
        def get_winamax_odd_for(a, b): return None

        # Cache stats par (gender, fmt) pour ne pas recalculer
        stats_cache = {}

        for race in races[:4]:
            race_id  = race.get("race_id","")
            desc     = race.get("description","")
            date     = race.get("date","")
            fmt      = race.get("format","SP")
            gender   = race.get("gender","M")
            location = race.get("location","")

            cache_key = (gender, fmt)
            if cache_key not in stats_cache:
                stats_cache[cache_key] = build_stats_for(gender, fmt, n=6)
            ibu_stats = stats_cache[cache_key]

            if len(ibu_stats) < 4:
                log.warning(f"[Biathlon] Pas assez de stats pour {fmt}/{gender} ({len(ibu_stats)} athlètes)")
                continue

            log.info(f"[Biathlon] {len(ibu_stats)} athlètes pour {fmt}/{gender}, construction H2H...")

            # Top athlètes de CE genre ET format — triés par avg_rank
            top = sorted(ibu_stats.items(), key=lambda x: x[1]["avg_rank"])[:10]

            g_icon = _gender_icon(gender)
            f_name = _fmt_name(fmt)
            msg += f"{g_icon} <b>{desc}</b>\n"
            msg += f"📅 {date}"
            if location: msg += f" · {location}"
            msg += f" · {f_name}\n\n"

            for i in range(min(4, len(top)-1)):
                ibu_a, sa = top[i]
                ibu_b, sb = top[i+1]
                ra = calc_rating(sa, fmt)
                rb = calc_rating(sb, fmt)
                pa = h2h_prob(ra, rb)
                pb = 1 - pa

                fa = round(1/pa, 2)  # cote juste A
                fb = round(1/pb, 2)  # cote juste B

                # Cotes Winamax réelles si dispo
                wm = get_winamax_odd_for(sa["name"], sb["name"])
                if wm:
                    wm_a = wm.get("home_odd", wm.get("home", 0))
                    wm_b = wm.get("away_odd", wm.get("away", 0))
                    val_a = (pa * wm_a - 1) if wm_a > 1 else 0
                    val_b = (pb * wm_b - 1) if wm_b > 1 else 0
                    vbet_a = f" ✅ +{val_a*100:.1f}%" if val_a > VALUE_THRESHOLD else ""
                    vbet_b = f" ✅ +{val_b*100:.1f}%" if val_b > VALUE_THRESHOLD else ""
                    odds_line = (
                        f"  💰 Winamax : <b>{wm_a}</b>{vbet_a} / {wm_b}{vbet_b}"
                        f"  (c.j. {fa} / {fb})\n"
                    )
                else:
                    odds_line = f"  💰 Cote juste modèle : {fa} / {fb}\n"

                msg += (
                    f"  <b>{sa['name']}</b> {sa['nat']} <b>{round(pa*100)}%</b>"
                    f" vs <b>{sb['name']}</b> {sb['nat']} {round(pb*100)}%\n"
                    + odds_line +
                    f"  🎯 C:{round(sa['prone_acc']*100)}%/D:{round(sa['standing_acc']*100)}%"
                    f" vs C:{round(sb['prone_acc']*100)}%/D:{round(sb['standing_acc']*100)}%"
                    f"  · {sa['n_races']} vs {sb['n_races']} courses\n\n"
                )

                save_bet({
                    "race_id":    race_id, "race_name": desc,
                    "race_date":  date,    "race_format": fmt,
                    "bet_type":   "H2H",   "pick": sa["name"],
                    "opponent":   sb["name"], "odd": fa,
                    "prob_model": round(pa, 4),
                })

        msg += "💡 <i>c.j. = cote juste modèle IBU · Comparer sur Winamax</i>"

        state["last_run"] = datetime.now(timezone.utc)
        log.info(f"[Biathlon] Message prêt ({len(msg)} chars), envoi Telegram...")
        if not silent: send_message(msg)

    except Exception as e:
        log.error(f"[Biathlon] run error: {e}", exc_info=True)
        if not silent: send_message(f"❌ <b>Erreur biathlon</b> : {e}")
    finally:
        state["running"] = False

# ─── CHECK RESULTS ────────────────────────────

def check_results(silent=False):
    from core.telegram import send_message
    from sports.biathlon.biathlon_client import get_results

    pending = get_pending_bets()
    if not pending:
        if not silent: send_message("🎿 Aucun bet biathlon en attente.")
        return

    won, lost = [], []
    for bet in pending:
        try:
            results = get_results(bet["race_id"])
            if not results: continue
            pr = next((int(r["Rank"]) for r in results
                       if bet["pick"].lower() in r.get("Name","").lower()
                       and r.get("Rank")), None)
            or_ = next((int(r["Rank"]) for r in results
                        if bet.get("opponent","").lower() in r.get("Name","").lower()
                        and r.get("Rank")), None)
            if pr is None or or_ is None: continue
            ok = 1 if pr < or_ else 0
            update_result(bet["id"], ok)
            (won if ok else lost).append(bet)
        except Exception as e:
            log.warning(f"[Biathlon] check_result {bet['id']}: {e}")

    state["last_results"] = datetime.now(timezone.utc)
    if not won and not lost:
        if not silent: send_message("⏳ Résultats biathlon pas encore disponibles.")
        return

    msg = "🎿 <b>Résultats biathlon</b>\n\n"
    if won:
        msg += f"✅ <b>Gagnés ({len(won)})</b>\n"
        for b in won: msg += f"  • {b['pick']} vs {b['opponent']} [{b['race_format']}]\n"
    if lost:
        msg += f"\n❌ <b>Perdus ({len(lost)})</b>\n"
        for b in lost: msg += f"  • {b['pick']} vs {b['opponent']} [{b['race_format']}]\n"
    if not silent: send_message(msg)