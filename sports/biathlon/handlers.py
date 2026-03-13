"""
sports/biathlon/handlers.py — Handlers Telegram biathlon
Flow interactif H2H :
  biat_h2h               → liste courses à venir
  biat_race_{race_id}    → choix type (H2H / Podium)
  biat_h2h_{race_id}     → liste athlètes (page 0)
  biat_h2hp_{race_id}_{p}→ page suivante athlètes
  biat_sel_{race_id}_{ibu_a} → athlète A sélectionné, choisir B
  biat_vs_{race_id}_{a}_{b}  → fiche duel finale
  biat_pod_{race_id}     → top podium de la course
"""
import threading
import logging
import math
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# État session H2H : garde le choix de l'athlète A entre deux callbacks
_session: dict = {}   # chat_id → {"race_id": ..., "ibu_a": ..., "stats": ...}


# ─── Helpers ────────────────────────────────────────────────────────────────

def _fmt_name(fmt: str) -> str:
    return {"SP":"Sprint","PU":"Poursuite","IN":"Individuelle",
            "MS":"Mass Start","RL":"Relais","SR":"Relais Mixte"}.get(fmt, fmt)

def _gender_icon(g: str) -> str:
    return "♀️" if g == "W" else "♂️"

def _build_stats(gender, fmt, n=6):
    from sports.biathlon.jobs import build_stats_for
    return build_stats_for(gender, fmt, n)

def _calc(sa, sb, fmt):
    from sports.biathlon.jobs import calc_rating, h2h_prob
    ra = calc_rating(sa, fmt)
    rb = calc_rating(sb, fmt)
    pa = h2h_prob(ra, rb)
    return pa, 1-pa

def _make_keyboard(buttons):
    """buttons = [[(text, callback_data), ...], ...]"""
    from core.telegram import make_keyboard
    return make_keyboard([[{"text": t, "callback_data": d} for t,d in row] for row in buttons])


# ─── Handlers principaux ────────────────────────────────────────────────────

def handle_status():
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
        msg += "Aucune course prévue dans les prochains jours."
        send_message(msg)
        return

    for r in races[:8]:
        g = _gender_icon(r.get("gender","M"))
        msg += f"{g} {r.get('description','')} · {r.get('date','')} · {_fmt_name(r.get('format',''))}\n"

    send_message(msg)


def handle_run():
    from core.telegram import send_message
    from sports.biathlon.jobs import run
    send_message("⏳ Analyse biathlon en cours...")
    threading.Thread(target=run, daemon=True).start()


def handle_results():
    from sports.biathlon.jobs import check_results
    threading.Thread(target=check_results, daemon=True).start()


def handle_stats():
    from core.telegram import send_message
    from sports.biathlon.jobs import get_pending_bets
    bets = get_pending_bets()
    send_message(f"🎿 {len(bets)} paris biathlon en attente.")


# ─── Flow interactif H2H ────────────────────────────────────────────────────

def handle_h2h_menu():
    """Étape 1 : affiche les courses à venir comme boutons."""
    from core.telegram import send_message, make_keyboard
    from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON

    try:
        preload_competitions(CURRENT_SEASON)
        races = get_upcoming_races(days_ahead=7)
        races = [r for r in races if r.get("format") not in ("RL","SR","MX")]
    except Exception as e:
        send_message(f"❌ IBU API : {e}")
        return

    if not races:
        send_message("🎿 Aucune course individuelle à venir.")
        return

    rows = []
    for r in races[:8]:
        g = _gender_icon(r.get("gender","M"))
        label = f"{g} {r.get('description','')} · {r.get('date','')}"
        rid = r.get("race_id","")
        rows.append([(label, f"biat_race_{rid}")])
    rows.append([("◀️ Menu", "menu_biathlon")])

    kb = make_keyboard([[{"text": t, "callback_data": d} for t,d in row] for row in rows])
    send_message("🎿 <b>Choisir une course :</b>", reply_markup=kb)


def handle_race_menu(race_id: str):
    """Étape 2 : H2H ou Podium ?"""
    from core.telegram import send_message, make_keyboard
    from sports.biathlon.biathlon_client import get_upcoming_races, CURRENT_SEASON, preload_competitions

    try:
        preload_competitions(CURRENT_SEASON)
        races = get_upcoming_races(days_ahead=7)
        race = next((r for r in races if r.get("race_id") == race_id), None)
    except Exception:
        race = None

    desc = race.get("description","Course") if race else race_id
    kb = make_keyboard([
        [{"text": "⚔️ H2H — Choisir deux athlètes", "callback_data": f"biat_h2h_{race_id}"}],
        [{"text": "🏆 Podium — Top favoris",         "callback_data": f"biat_pod_{race_id}"}],
        [{"text": "◀️ Retour",                        "callback_data": "biat_h2h_menu"}],
    ])
    send_message(f"🎿 <b>{desc}</b>\n\nQue veux-tu analyser ?", reply_markup=kb)


def handle_h2h_athletes(race_id: str, page: int = 0, chat_id: str = None):
    """Étape 3 : liste paginée des athlètes pour choisir A."""
    from core.telegram import send_message, make_keyboard
    from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON

    try:
        preload_competitions(CURRENT_SEASON)
        races = get_upcoming_races(days_ahead=7)
        race = next((r for r in races if r.get("race_id") == race_id), None)
    except Exception:
        race = None

    if not race:
        send_message("❌ Course introuvable.")
        return

    gender = race.get("gender","M")
    fmt    = race.get("format","SP")
    desc   = race.get("description","Course")

    stats = _build_stats(gender, fmt, n=6)
    if not stats:
        send_message("❌ Pas de stats disponibles pour cette course.")
        return

    top = sorted(stats.items(), key=lambda x: x[1]["avg_rank"])
    PER_PAGE = 8
    total_pages = math.ceil(len(top) / PER_PAGE)
    slice_ = top[page*PER_PAGE:(page+1)*PER_PAGE]

    rows = []
    for ibu, s in slice_:
        label = f"{s['name']} {s['nat']} (#{round(s['avg_rank'],1)})"
        rows.append([{"text": label, "callback_data": f"biat_sel_{race_id}_{ibu}"}])

    # Navigation pages
    nav = []
    if page > 0:
        nav.append({"text": "◀️", "callback_data": f"biat_h2hp_{race_id}_{page-1}"})
    if page < total_pages - 1:
        nav.append({"text": "▶️", "callback_data": f"biat_h2hp_{race_id}_{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": "◀️ Retour", "callback_data": f"biat_race_{race_id}"}])

    kb = make_keyboard(rows)
    send_message(
        f"🎿 <b>{desc}</b> — Choisir l'athlète A\n"
        f"<i>(page {page+1}/{total_pages})</i>",
        reply_markup=kb
    )

    # Sauvegarde stats en session
    if chat_id:
        _session[chat_id] = {"race_id": race_id, "gender": gender, "fmt": fmt,
                              "desc": desc, "stats": stats, "ibu_a": None}


def handle_select_a(race_id: str, ibu_a: str, chat_id: str):
    """Étape 4 : A sélectionné, choisir B."""
    from core.telegram import send_message, make_keyboard

    sess = _session.get(chat_id, {})
    if not sess or sess.get("race_id") != race_id:
        # Recharge stats
        from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON
        preload_competitions(CURRENT_SEASON)
        races = get_upcoming_races(days_ahead=7)
        race  = next((r for r in races if r.get("race_id") == race_id), {})
        gender, fmt = race.get("gender","M"), race.get("format","SP")
        stats = _build_stats(gender, fmt)
        sess  = {"race_id": race_id, "gender": gender, "fmt": fmt,
                 "desc": race.get("description",""), "stats": stats}
        _session[chat_id] = sess

    sess["ibu_a"] = ibu_a
    stats = sess.get("stats", {})
    sa    = stats.get(ibu_a, {})
    name_a = sa.get("name", ibu_a)
    fmt   = sess.get("fmt","SP")
    desc  = sess.get("desc","")

    # Liste B (tous sauf A) triés par avg_rank
    top = [(ibu, s) for ibu, s in sorted(stats.items(), key=lambda x: x[1]["avg_rank"]) if ibu != ibu_a]

    rows = []
    for ibu_b, sb in top[:12]:
        label = f"{sb['name']} {sb['nat']} (#{round(sb['avg_rank'],1)})"
        rows.append([{"text": label, "callback_data": f"biat_vs_{race_id}_{ibu_a}_{ibu_b}"}])
    rows.append([{"text": "◀️ Rechoisir A", "callback_data": f"biat_h2h_{race_id}"}])

    kb = make_keyboard(rows)
    send_message(
        f"🎿 <b>{desc}</b>\n"
        f"⚔️ <b>{name_a}</b> vs ...\n\n"
        f"Choisir l'adversaire :",
        reply_markup=kb
    )


def handle_duel(race_id: str, ibu_a: str, ibu_b: str, chat_id: str):
    """Étape 5 : fiche duel complète."""
    from core.telegram import send_message, make_keyboard

    sess = _session.get(chat_id, {})
    stats = sess.get("stats") if sess.get("race_id") == race_id else None
    fmt   = sess.get("fmt","SP") if sess else "SP"
    desc  = sess.get("desc","") if sess else ""

    if not stats:
        from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON
        preload_competitions(CURRENT_SEASON)
        races = get_upcoming_races(days_ahead=7)
        race  = next((r for r in races if r.get("race_id") == race_id), {})
        fmt   = race.get("format","SP")
        desc  = race.get("description","")
        gender = race.get("gender","M")
        stats  = _build_stats(gender, fmt)

    sa = stats.get(ibu_a)
    sb = stats.get(ibu_b)
    if not sa or not sb:
        send_message("❌ Athlètes introuvables dans les stats.")
        return

    pa, pb = _calc(sa, sb, fmt)
    fa = round(1/pa, 2)
    fb = round(1/pb, 2)

    winner = sa if pa > pb else sb
    loser  = sb if pa > pb else sa
    pw = max(pa, pb)

    msg = (
        f"⚔️ <b>{sa['name']} vs {sb['name']}</b>\n"
        f"🎿 {desc} · {_fmt_name(fmt)}\n\n"
        f"📊 <b>Probabilités modèle IBU</b>\n"
        f"  {sa['name']} : <b>{round(pa*100)}%</b> → c.j. {fa}\n"
        f"  {sb['name']} : <b>{round(pb*100)}%</b> → c.j. {fb}\n\n"
        f"🏆 Favori : <b>{winner['name']}</b> ({round(pw*100)}%)\n\n"
        f"🎯 <b>Stats tir</b>\n"
        f"  {sa['name']} : Couché {round(sa['prone_acc']*100)}% · Debout {round(sa['standing_acc']*100)}%\n"
        f"  {sb['name']} : Couché {round(sb['prone_acc']*100)}% · Debout {round(sb['standing_acc']*100)}%\n\n"
        f"⛷️ <b>Forme</b>\n"
        f"  {sa['name']} : Rang moy. #{round(sa['avg_rank'],1)} · Top3 {round(sa['top3_rate']*100)}% sur {sa['n_races']} courses\n"
        f"  {sb['name']} : Rang moy. #{round(sb['avg_rank'],1)} · Top3 {round(sb['top3_rate']*100)}% sur {sb['n_races']} courses\n\n"
        f"💡 <i>Comparer avec Winamax — si cote > {fa} sur {sa['name']} → value bet</i>"
    )

    kb = make_keyboard([
        [{"text": "🔄 Changer adversaire", "callback_data": f"biat_sel_{race_id}_{ibu_a}"}],
        [{"text": "◀️ Retour courses",     "callback_data": "biat_h2h_menu"}],
    ])
    send_message(msg, reply_markup=kb)


def handle_podium(race_id: str):
    """Podium — top 8 favoris de la course."""
    from core.telegram import send_message, make_keyboard
    from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON
    from sports.biathlon.jobs import calc_rating

    try:
        preload_competitions(CURRENT_SEASON)
        races = get_upcoming_races(days_ahead=7)
        race  = next((r for r in races if r.get("race_id") == race_id), None)
    except Exception as e:
        send_message(f"❌ {e}")
        return

    if not race:
        send_message("❌ Course introuvable.")
        return

    gender = race.get("gender","M")
    fmt    = race.get("format","SP")
    desc   = race.get("description","")

    stats = _build_stats(gender, fmt)
    if not stats:
        send_message("❌ Pas de stats disponibles.")
        return

    top = sorted(stats.items(), key=lambda x: -calc_rating(x[1], fmt))[:8]
    total_rating = sum(calc_rating(s, fmt) for _, s in top)

    msg = f"🏆 <b>Podium favori — {desc}</b>\n🎿 {_fmt_name(fmt)}\n\n"
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣"]
    for i, (ibu, s) in enumerate(top):
        r  = calc_rating(s, fmt)
        pct = round(r / total_rating * 100)
        msg += (
            f"{medals[i]} <b>{s['name']}</b> {s['nat']} — {pct}%\n"
            f"   Rang moy. #{round(s['avg_rank'],1)} · "
            f"C:{round(s['prone_acc']*100)}% D:{round(s['standing_acc']*100)}% · "
            f"Top3: {round(s['top3_rate']*100)}%\n"
        )

    kb = make_keyboard([
        [{"text": "⚔️ Voir H2H", "callback_data": f"biat_h2h_{race_id}"}],
        [{"text": "◀️ Retour",   "callback_data": f"biat_race_{race_id}"}],
    ])
    send_message(msg, reply_markup=kb)