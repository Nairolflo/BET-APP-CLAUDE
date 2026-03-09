"""
sports/football/handlers.py — Handlers des commandes Telegram foot
Chaque fonction est appelée depuis le scheduler central.
"""
import os
import threading
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def handle_bets():
    from database import get_unique_bets
    from core.telegram import send_message, send_menu_foot

    bets    = get_unique_bets(limit=200)
    pending = [b for b in bets if b["success"] == -1]

    if not pending:
        send_message("📭 <b>Aucun paris en attente.</b>\nUtilisez ⚡ Lancer analyse pour en trouver.")
        return

    home_bets = [b for b in pending if b["market"] == "Home Win"]
    away_bets = [b for b in pending if b["market"] == "Away Win"]
    over_bets = [b for b in pending if b["market"] not in ("Home Win", "Away Win")]
    bn_bets   = [b for b in pending if b.get("bete_noire")]

    def fmt(b):
        bn = " 🔥" if b.get("bete_noire") else ""
        return (
            f"  <b>{b['home_team']} vs {b['away_team']}</b>{bn}\n"
            f"  📅 {b['match_date']} · {b.get('league', '')}"
            f" · @ <b>{b['bk_odds']}</b> · +{b['value']*100:.1f}% · {b['probability']*100:.0f}%\n"
        )

    msg = f"⏳ <b>Paris en attente — {len(pending)} sélections</b>\n"
    if home_bets:
        msg += f"\n🏠 <b>Domicile ({len(home_bets)})</b>\n"
        for b in home_bets[:8]: msg += fmt(b)
    if away_bets:
        msg += f"\n✈️ <b>Extérieur ({len(away_bets)})</b>\n"
        for b in away_bets[:8]: msg += fmt(b)
    if over_bets:
        msg += f"\n⚽ <b>Over/Under ({len(over_bets)})</b>\n"
        for b in over_bets[:8]: msg += fmt(b)
    if bn_bets:
        msg += f"\n🔥 <b>Bête Noire ({len(bn_bets)})</b>\n"
        for b in bn_bets[:5]: msg += fmt(b)
    send_message(msg)


def handle_today():
    from database import get_unique_bets
    from core.telegram import send_message

    today      = datetime.now(timezone.utc).date().isoformat()
    bets       = get_unique_bets(limit=200)
    pending    = [b for b in bets if b.get("match_date") == today and b["success"] == -1]

    if not pending:
        from sports.football.jobs import SCHEDULER_HOUR
        send_message(f"📅 <b>Aucun paris pour aujourd'hui ({today})</b>\n"
                     f"Analyse auto à {SCHEDULER_HOUR:02d}h00 UTC.")
        return

    home_b = [b for b in pending if b["market"] == "Home Win"]
    away_b = [b for b in pending if b["market"] == "Away Win"]
    over_b = [b for b in pending if b["market"] not in ("Home Win", "Away Win")]
    bn_b   = [b for b in pending if b.get("bete_noire")]

    def fmt(b):
        bn = " 🔥" if b.get("bete_noire") else ""
        return f"  <b>{b['home_team']} vs {b['away_team']}</b>{bn} · {b['market']} @ <b>{b['bk_odds']}</b> · +{b['value']*100:.1f}%\n"

    msg = f"📅 <b>Paris du {today} — {len(pending)} sélections</b>\n"
    if home_b:
        msg += f"\n🏠 <b>Domicile ({len(home_b)})</b>\n"
        for b in home_b: msg += fmt(b)
    if away_b:
        msg += f"\n✈️ <b>Extérieur ({len(away_b)})</b>\n"
        for b in away_b: msg += fmt(b)
    if over_b:
        msg += f"\n⚽ <b>Over/Under ({len(over_b)})</b>\n"
        for b in over_b: msg += fmt(b)
    if bn_b:
        msg += f"\n🔥 <b>Bête Noire ({len(bn_b)})</b>\n"
        for b in bn_b: msg += fmt(b)
    send_message(msg)


def handle_stats():
    from database import get_stats, get_stats_by_market, get_stats_by_league_detailed, get_streak
    from core.telegram import send_message

    stats     = get_stats()
    o         = stats["overall"]
    by_market = get_stats_by_market()
    by_league = get_stats_by_league_detailed()
    streak    = get_streak()
    roi       = o.get("roi") or 0
    roi_sign  = "+" if roi >= 0 else ""

    streak_line = ""
    if streak and streak.get("count", 0) > 1:
        emoji = "🔥" if streak["type"] == "win" else "❄️"
        label = "victoires" if streak["type"] == "win" else "défaites"
        streak_line = f"\n{emoji} Série : <b>{streak['count']} {label} consécutives</b>"

    resolved  = [m for m in by_market if (m.get("total", 0) - m.get("pending", 0)) >= 3]
    best      = max(resolved, key=lambda x: x.get("roi", -999)) if resolved else None
    best_line = f"\n🏆 Meilleur : <b>{best['market']}</b> (+{best['roi']}% ROI)" if best else ""

    msg = (
        f"📊 <b>Stats Football</b>\n\n"
        f"Total : <b>{o.get('total') or 0}</b> · ✅ {o.get('wins') or 0} · ❌ {o.get('losses') or 0} · ⏳ {o.get('pending') or 0}\n"
        f"Win rate : <b>{o.get('win_rate') or 0}%</b> · ROI : <b>{roi_sign}{roi}%</b>\n"
        f"Value moy. : <b>+{o.get('avg_value_pct') or 0}%</b>"
        f"{streak_line}{best_line}\n"
    )
    if by_market:
        msg += "\n<b>Par marché :</b>\n"
        for m in by_market:
            s = "+" if m.get("roi", 0) >= 0 else ""
            msg += f"  {m['market']} · {m.get('wins',0)}W/{m.get('losses',0)}L · ROI {s}{m.get('roi',0)}%\n"
    if by_league:
        msg += "\n<b>Par ligue (top 5) :</b>\n"
        for l in by_league[:5]:
            s = "+" if l.get("roi", 0) >= 0 else ""
            msg += f"  {l['league']} · {l.get('wins',0)}W/{l.get('losses',0)}L · ROI {s}{l.get('roi',0)}%\n"
    send_message(msg)


def handle_pourcent():
    from database import get_stats
    from core.telegram import send_message

    o       = get_stats()["overall"]
    wins    = o.get("wins") or 0
    losses  = o.get("losses") or 0
    pending = o.get("pending") or 0
    settled = (o.get("total") or 0) - pending
    if settled == 0:
        send_message("📊 Aucun pari résolu.\nTapez /results pour mettre à jour.")
        return
    win_rate = round(wins / settled * 100, 1)
    roi      = round((wins - losses) / settled * 100, 1)
    roi_sign = "+" if roi >= 0 else ""
    send_message(
        f"📈 <b>Taux de réussite</b>\n\n"
        f"✅ Gagnés : <b>{wins}</b>\n"
        f"❌ Perdus : <b>{losses}</b>\n"
        f"⏳ En attente : <b>{pending}</b>\n"
        f"🎯 Taux : <b>{win_rate}%</b>\n"
        f"💰 ROI : <b>{roi_sign}{roi}%</b>"
    )


def handle_run():
    from sports.football.jobs import run
    from core.telegram import send_message
    send_message("⚡ <b>Analyse football lancée !</b>")
    threading.Thread(target=run, daemon=True).start()


def handle_refresh():
    from sports.football.jobs import refresh_team_stats
    from core.telegram import send_message
    send_message("🔄 <b>Refresh stats en cours...</b>")
    threading.Thread(target=refresh_team_stats, daemon=True).start()


def handle_results():
    from sports.football.jobs import check_results
    from core.telegram import send_message
    send_message("🔍 <b>Vérification résultats...</b>")
    threading.Thread(target=check_results, daemon=True).start()


def handle_reset():
    from database import reset_all_bets
    from core.telegram import send_message
    count = reset_all_bets()
    send_message(f"🗑 <b>Reset effectué</b> — {count} paris supprimés.")


def handle_api():
    from api_clients import get_odds_quota
    from core.telegram import send_message
    from sports.football.jobs import LEAGUES

    quota     = get_odds_quota()
    remaining = quota.get("remaining")
    used      = quota.get("used")
    updated   = quota.get("last_update", "")[:16].replace("T", " ") if quota.get("last_update") else "jamais"

    if remaining is None:
        send_message("📡 <b>Quotas API</b>\n\nPas encore de données — lancez une analyse d'abord.")
        return

    total = 500
    pct   = round(remaining / total * 100)
    bar   = "█" * round(pct / 10) + "░" * (10 - round(pct / 10))
    color = "🟢" if pct > 40 else "🟡" if pct > 15 else "🔴"

    send_message(
        f"📡 <b>Quotas API</b>\n\n"
        f"<b>The Odds API</b> (500/mois)\n"
        f"{color} [{bar}] {pct}%\n"
        f"  ✅ Utilisées : <b>{used}</b>\n"
        f"  💚 Restantes : <b>{remaining}</b>\n"
        f"  📅 Mis à jour : {updated}\n\n"
        f"<b>Football-Data.org</b>\n"
        f"  ⏱ Rate limit : 10 req/min (auto-géré)"
    )


def handle_status():
    from sports.football.jobs import state, SCHEDULER_HOUR
    from core.telegram import send_message

    started      = state["started_at"]
    last_run     = state["last_run"]
    last_refresh = state["last_refresh"]
    uptime = "N/A"
    if started:
        delta = datetime.now(timezone.utc) - started
        h, rem = divmod(int(delta.total_seconds()), 3600)
        uptime = f"{h}h {rem//60:02d}m"
    etat = "🔄 En cours..." if state["running"] else "🟢 En attente"
    send_message(
        f"📡 <b>Status Football</b>\n\n"
        f"État : {etat}\n"
        f"⏱ Uptime : {uptime}\n"
        f"🕐 Prochaine analyse : {SCHEDULER_HOUR:02d}h00 UTC\n"
        f"⚽ Dernière analyse : {last_run.strftime('%Y-%m-%d %H:%M UTC') if last_run else 'Aucune'}\n"
        f"🔄 Dernier refresh : {last_refresh.strftime('%Y-%m-%d %H:%M UTC') if last_refresh else 'Aucun'}\n"
        f"🎯 Bets dernière analyse : {state['bets_today']}"
    )


def handle_h2h():
    from database import get_h2h_cache_status
    from core.telegram import send_message
    from sports.football.jobs import LEAGUE_NAMES
    from collections import defaultdict

    rows = get_h2h_cache_status()
    if not rows:
        send_message("📭 <b>Cache H2H vide</b>\nLancez une analyse pour le remplir.")
        return

    by_league = defaultdict(list)
    for r in rows:
        name = LEAGUE_NAMES.get(r.get("league_id"), str(r.get("league_id")))
        by_league[name].append(r)

    msg = "🔥 <b>Cache H2H</b>\n\n"
    for league_name, entries in sorted(by_league.items()):
        msg += f"<b>{league_name}</b>\n"
        for e in sorted(entries, key=lambda x: x.get("season", 0), reverse=True):
            age       = e.get("age_days", "?")
            freshness = "✅" if (age or 99) <= 7 else "⚠️ expiré"
            msg += f"  {e.get('season')} : {e.get('match_count', '?')} matchs · {age}j {freshness}\n"
        msg += "\n"
    msg += "<i>TTL : 7 jours</i>"
    send_message(msg)


def handle_refresh_h2h():
    from core.telegram import send_message
    import database as db_module
    from api_clients import clear_h2h_cache, prefetch_season_matches, FOOTBALLDATA_LEAGUE_MAP
    from sports.football.jobs import LEAGUES, SEASON

    send_message("🔄 <b>Refresh H2H démarré...</b>")

    conn = db_module.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM h2h_cache")
        conn.commit()
    finally:
        conn.close()

    clear_h2h_cache()
    seasons = [SEASON, SEASON - 1]
    fetched, errors = 0, 0

    for league_id in LEAGUES:
        if not FOOTBALLDATA_LEAGUE_MAP.get(league_id):
            continue
        try:
            prefetch_season_matches(league_id, seasons)
            fetched += 1
        except Exception:
            errors += 1

    send_message(
        f"✅ <b>Refresh H2H terminé</b>\n"
        f"{fetched} ligues · {'❌ ' + str(errors) + ' erreurs' if errors else '✅ Sans erreur'}\n"
        f"<i>Cache valide 7 jours</i>"
    )
