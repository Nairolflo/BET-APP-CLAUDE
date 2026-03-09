"""
scheduler.py - Value bet engine + Bot Telegram interactif

Jobs automatiques :
  06h00 UTC → refresh stats equipes
  08h00 UTC → analyse value bets
  23h00 UTC → verification resultats

Commandes Telegram :
  /help /status /bets /stats /pourcent /run /refresh /results /reset /web
"""

import os
import logging
import threading
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from database import (
    init_db, save_bet, save_team_stats, get_team_stats,
    get_stats, get_unique_bets, is_bet_notified, mark_bet_notified,
    delete_today_pending_bets, update_bet_result, get_pending_bets, reset_all_bets,
)
from api_clients import (
    get_odds_quota, odds_quota_ok, clear_odds_cache,
    get_fixtures, get_odds, get_team_standings,
    get_fixtures_results_batch, get_all_results_today,
    normalize_team_name, get_h2h, clear_h2h_cache,
    get_recent_form, clear_form_cache, FOOTBALLDATA_LEAGUE_MAP,
)
from model import (
    calc_league_averages, calc_attack_defense_strength,
    predict_match, find_value_bets,
)
from telegram_bot import send_message, send_daily_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

LEAGUE_NAMES = {
    39:  "Premier League",
    61:  "Ligue 1",
    78:  "Bundesliga",
    135: "Serie A",
    140: "La Liga",
    88:  "Eredivisie",
    94:  "Primeira Liga",
    71:  "Brasileirao",
    40:  "Championship",
    2:   "Champions League",
    144: "Belgium First Div",
    203: "Turkey Super League",
    179: "Scottish Premiership",
    262: "Liga MX",
    3:   "Europa League",
}

SEASON          = int(os.getenv("SEASON", 2025))
DEFAULT_LEAGUES = "39,61,78,135,140,88,94,40,2,144,203,179,3"
LEAGUES         = [int(x) for x in os.getenv("LEAGUES", DEFAULT_LEAGUES).split(",")]
VALUE_THRESHOLD = float(os.getenv("VALUE_THRESHOLD", 0.02))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", 0.55))
DAYS_AHEAD      = int(os.getenv("SCHEDULER_DAYS_AHEAD", 10))
SCHEDULER_HOUR  = int(os.getenv("SCHEDULER_HOUR", 8))
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

worker_state = {
    "started_at":   None,
    "last_run":     None,
    "last_refresh": None,
    "bets_today":   0,
    "running":      False,
}


# ─────────────────────────────────────────────
# MOTEUR VALUE BET
# ─────────────────────────────────────────────

def refresh_team_stats(silent=False):
    log.info("Refresh stats equipes...")
    results = []
    for league_id in LEAGUES:
        try:
            teams = get_team_standings(league_id, SEASON)
            for t in teams:
                save_team_stats(t)
            msg = f"✅ {LEAGUE_NAMES.get(league_id, league_id)} : {len(teams)} equipes"
            log.info(f"  {msg}")
            results.append(msg)
        except Exception as e:
            msg = f"❌ {LEAGUE_NAMES.get(league_id, league_id)} : {e}"
            log.error(f"  {msg}")
            results.append(msg)

    worker_state["last_refresh"] = datetime.now(timezone.utc)
    if not silent:
        send_message("🔄 <b>Refresh stats terminé</b>\n\n" + "\n".join(results))
    return results


def smart_run_value_bet_engine():
    """
    Mode intelligent : vérifie d'abord si des matchs existent dans les 3 prochains jours.
    Si aucune ligue n'a de matchs proches → run annulé, quota préservé.
    Si au moins une ligue a des matchs → run normal.
    Le run manuel /run ignore ce check et tourne toujours.
    """
    from datetime import timedelta
    today      = datetime.now(timezone.utc).date()
    near_dates = {(today + timedelta(days=i)).isoformat() for i in range(3)}

    leagues_with_fixtures = []
    for league_id in LEAGUES:
        try:
            fixtures = get_fixtures(league_id, SEASON, 3)  # seulement 3 jours
            near = [f for f in fixtures if f.get("date", "")[:10] in near_dates]
            if near:
                leagues_with_fixtures.append(LEAGUE_NAMES.get(league_id, str(league_id)))
        except Exception:
            pass  # si erreur on inclut quand même

    if not leagues_with_fixtures:
        log.info("⏭️  Smart run : aucun match dans 3 jours — analyse annulée (quota préservé)")
        send_message(
            f"⏭️ <b>Analyse auto annulée</b>\n"
            f"Aucun match dans les 3 prochains jours.\n"
            f"<i>Quota Odds API préservé. /run pour forcer.</i>"
        )
        return

    log.info(f"✅ Smart run : {len(leagues_with_fixtures)} ligue(s) avec matchs → analyse lancée")
    run_value_bet_engine(silent=False)


def run_value_bet_engine(silent=False):
    if worker_state["running"]:
        send_message("⏳ Une analyse est déjà en cours, patientez...")
        return

    worker_state["running"] = True

    # Vérif quota avant de démarrer (sauf run forcé via /run)
    if not silent:  # silent=True = run normal schedulé
        if not odds_quota_ok(required=len(LEAGUES)):
            rem = get_odds_quota().get("remaining", "?")
            send_message(
                f"⚠️ <b>Run annulé — quota insuffisant</b>\n\n"
                f"📡 Restantes : <b>{rem}</b> / seuil sécurité : {30}\n"
                f"<i>Utilisez /run pour forcer malgré tout.</i>"
            )
            worker_state["running"] = False
            return

    clear_h2h_cache()
    clear_form_cache()
    delete_today_pending_bets()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info("=" * 60)
    log.info(f"⚽ VALUE BET ENGINE — {now}")
    log.info("=" * 60)

    if not silent:
        send_message(f"🚀 <b>Analyse démarrée</b>\n📅 {now}\n🔍 Calcul en cours...")

    new_value_bets = []
    errors = []

    for league_id in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        log.info(f"\n[{league_name}]")

        # 1. Fixtures
        try:
            fixtures = get_fixtures(league_id, SEASON, DAYS_AHEAD)
            log.info(f"  {len(fixtures)} matchs trouvés.")
        except Exception as e:
            errors.append(f"Fixtures {league_name}: {e}")
            continue

        if not fixtures:
            continue

        # 2. Stats equipes
        team_stats = get_team_stats(league_id, SEASON)
        if not team_stats:
            log.warning(f"  Pas de stats — auto-refresh...")
            try:
                teams = get_team_standings(league_id, SEASON)
                for t in teams:
                    save_team_stats(t)
                team_stats = get_team_stats(league_id, SEASON)
                log.info(f"  Auto-refresh OK : {len(team_stats)} equipes.")
            except Exception as e:
                errors.append(f"Auto-refresh {league_name}: {e}")
                continue

        avg_home, avg_away = calc_league_averages(team_stats)
        strengths = calc_attack_defense_strength(team_stats, avg_home, avg_away)
        log.info(f"  Moy. buts: dom={avg_home:.2f} ext={avg_away:.2f}")

        # 3. Forme récente (seulement pour ligues avec FD)
        recent_form = {}
        if FOOTBALLDATA_LEAGUE_MAP.get(league_id):
            try:
                recent_form = get_recent_form(league_id, SEASON)
                if recent_form:
                    log.info(f"  Forme récente: {len(recent_form)} equipes")
            except Exception as e:
                log.warning(f"  Forme récente indisponible: {e}")

        # 4. Cotes bookmakers — mode intelligent : skip si aucun match dans 3 jours
        from datetime import timedelta
        today      = datetime.now(timezone.utc).date()
        near_dates = {(today + timedelta(days=i)).isoformat() for i in range(3)}
        near_fixtures = [f for f in fixtures if f.get("date", "")[:10] in near_dates]
        if not near_fixtures:
            log.info(f"  ⏭️  Aucun match dans 3 jours — appel Odds API ignoré (économie crédit)")
            odds_events = []
        else:
            log.info(f"  {len(near_fixtures)} match(s) dans 3 jours → appel Odds API")
            try:
                odds_events = get_odds(league_id)
                log.info(f"  {len(odds_events)} evenements avec cotes.")
            except Exception as e:
                errors.append(f"Cotes {league_name}: {e}")
                odds_events = []

        odds_lookup = {}
        for ev in odds_events:
            key = (ev["home_team"].lower(), ev["away_team"].lower())
            odds_lookup[key] = ev["odds"]

        # 5. Prédiction + value pour chaque match
        for fix in fixtures:
            home_name = fix["home_team_name"]
            away_name = fix["away_team_name"]

            # Seuils Over/Under disponibles
            ou_thresholds = set()
            for bk_odds in odds_lookup.values():
                for k in bk_odds.keys():
                    if k.startswith("over_"):
                        try:
                            ou_thresholds.add(float(k.replace("over_", "").replace("_", ".")))
                        except ValueError:
                            pass
            ou_thresholds = sorted(ou_thresholds) or [1.5, 2.5, 3.5, 4.5]

            # Forme récente des deux equipes
            h_norm    = normalize_team_name(home_name)
            a_norm    = normalize_team_name(away_name)
            home_form = recent_form.get(h_norm) or recent_form.get(home_name.lower())
            away_form = recent_form.get(a_norm) or recent_form.get(away_name.lower())

            prediction = predict_match(
                home_name, away_name, strengths, avg_home, avg_away,
                ou_thresholds, home_form=home_form, away_form=away_form
            )
            if not prediction:
                continue

            # Cherche les cotes — exact puis fuzzy
            odds = odds_lookup.get((home_name.lower(), away_name.lower()), {})
            if not odds:
                for (h_key, a_key), o in odds_lookup.items():
                    if (h_key == h_norm or h_norm in h_key or h_key in h_norm) and \
                       (a_key == a_norm or a_norm in a_key or a_key in a_norm):
                        odds = o
                        break

            if not odds:
                continue

            # H2H bête noire — batch, pas d'appel API supplémentaire par match
            h2h = None
            if FOOTBALLDATA_LEAGUE_MAP.get(league_id):
                try:
                    h2h = get_h2h(league_id, home_name, away_name, match_date=fix["date"])
                    if h2h:
                        log.info(
                            f"  🔥 H2H {home_name} vs {away_name}: "
                            f"{h2h['home_wins']}W-{h2h['draws']}D-{h2h['away_wins']}L "
                            f"(home={h2h['win_rate_home']:.0%})"
                        )
                except Exception as e:
                    log.warning(f"  H2H indisponible: {e}")

            value_bets = find_value_bets(prediction, odds, VALUE_THRESHOLD, MIN_PROBABILITY, h2h=h2h)
            match_info = {
                "date":      fix["date"],
                "home_team": home_name,
                "away_team": away_name,
                "league":    league_name,
            }

            for bet in value_bets:
                try:
                    bet_id = save_bet({
                        "match_date": fix["date"],
                        "league":     league_name,
                        "home_team":  home_name,
                        "away_team":  away_name,
                        **bet,
                    })
                    if not is_bet_notified(bet_id):
                        log.info(
                            f"  ✅ NOUVEAU BET #{bet_id}: {home_name} vs {away_name} | "
                            f"{bet['market']} @ {bet['bk_odds']} | +{bet['value']*100:.1f}%"
                            + (" 🔥 BETE NOIRE" if bet.get("bete_noire") else "")
                        )
                        new_value_bets.append((bet, match_info))
                        mark_bet_notified(bet_id)
                    else:
                        log.info(f"  ⏭ BET #{bet_id} deja notifie: {home_name} vs {away_name}")
                except Exception as e:
                    log.error(f"  save_bet: {e}")

    worker_state["last_run"]   = datetime.now(timezone.utc)
    worker_state["bets_today"] = len(new_value_bets)
    worker_state["running"]    = False

    # Quota restant après le run
    try:
        from api_clients import get_odds_api_usage
        usage     = get_odds_api_usage()
        remaining = usage.get("remaining", "?")
        used_run  = usage.get("used", "?")
        quota_line = f"\n📡 Quota Odds API : <b>{remaining}</b> req. restantes"
    except Exception:
        quota_line = ""

    send_daily_summary(new_value_bets, extra=quota_line)

    if errors:
        send_message("⚠️ <b>Erreurs durant l'analyse :</b>\n" + "\n".join(f"• {e}" for e in errors))

    log.info(f"✅ Analyse terminée — {len(new_value_bets)} nouveaux value bets.")


# ─────────────────────────────────────────────
# VERIFICATION RESULTATS
# ─────────────────────────────────────────────

def check_results(silent=False):
    pending = get_pending_bets()
    if not pending:
        if not silent:
            send_message("📭 Aucun bet en attente à vérifier.")
        return

    log.info(f"🔍 Vérification de {len(pending)} bets en attente...")
    updated_won  = []
    updated_lost = []

    from collections import defaultdict
    by_date = defaultdict(list)
    for bet in pending:
        by_date[bet["match_date"]].append(bet)

    league_name_to_id = {v: k for k, v in LEAGUE_NAMES.items()}

    def fuzzy_match(n1: str, n2: str) -> bool:
        a = normalize_team_name(n1)
        b = normalize_team_name(n2)
        if a == b or a in b or b in a:
            return True
        for w in ["fc", "af", "sc", "afc", "cf", "rc", "as", "ac", "us", "oc"]:
            a = a.replace(f" {w}", "").replace(f"{w} ", "")
            b = b.replace(f" {w}", "").replace(f"{w} ", "")
        return a.strip() == b.strip() or a.strip() in b.strip() or b.strip() in a.strip()

    def find_result(results: dict, home_bet: str, away_bet: str):
        for (h_key, a_key), result in results.items():
            if fuzzy_match(home_bet, h_key) and fuzzy_match(away_bet, a_key):
                return result
        return None

    for match_date, bets in by_date.items():
        all_results = get_all_results_today(match_date)
        for bet in bets:
            league_id = league_name_to_id.get(bet.get("league", ""))
            if league_id:
                extra = get_fixtures_results_batch(league_id, SEASON, match_date)
                all_results.update(extra)

        if not all_results:
            continue

        for bet in bets:
            result = find_result(all_results, bet["home_team"], bet["away_team"])
            if not result:
                continue

            hg      = result["home_goals"]
            ag      = result["away_goals"]
            total   = result["total_goals"]
            market  = bet["market"]
            success = None

            if market == "Home Win":
                success = 1 if hg > ag else 0
            elif market == "Away Win":
                success = 1 if ag > hg else 0
            elif market == "Draw":
                success = 1 if hg == ag else 0
            elif market.startswith("Over "):
                try:
                    success = 1 if total > float(market.split(" ")[1]) else 0
                except ValueError:
                    pass
            elif market.startswith("Under "):
                try:
                    success = 1 if total < float(market.split(" ")[1]) else 0
                except ValueError:
                    pass

            if success is not None:
                update_bet_result(bet["id"], success)
                score_str = f"{hg}-{ag}"
                if success == 1:
                    updated_won.append({**bet, "score": score_str})
                    log.info(f"  ✅ GAGNÉ: {bet['home_team']} vs {bet['away_team']} | {market} ({score_str})")
                else:
                    updated_lost.append({**bet, "score": score_str})
                    log.info(f"  ❌ PERDU: {bet['home_team']} vs {bet['away_team']} | {market} ({score_str})")

    if not updated_won and not updated_lost:
        if not silent:
            send_message("⏳ Résultats pas encore disponibles.")
        return

    msg = "📊 <b>Résultats mis à jour</b>\n\n"
    if updated_won:
        msg += f"✅ <b>Gagnés ({len(updated_won)}) :</b>\n"
        for b in updated_won:
            msg += f"  • {b['home_team']} vs {b['away_team']} — {b['market']} @ {b['bk_odds']} ({b['score']})\n"
    if updated_lost:
        msg += f"\n❌ <b>Perdus ({len(updated_lost)}) :</b>\n"
        for b in updated_lost:
            msg += f"  • {b['home_team']} vs {b['away_team']} — {b['market']} @ {b['bk_odds']} ({b['score']})\n"

    if not silent:
        send_message(msg)
    log.info(f"✅ {len(updated_won)} gagnés, {len(updated_lost)} perdus.")


# ─────────────────────────────────────────────
# COMMANDES TELEGRAM
# ─────────────────────────────────────────────

def handle_help():
    send_message(
        "⚽ <b>Le Loup de Wall Bet — FOOT</b>\n\n"
        "❓ /help       — Ce message\n"
        "📡 /status     — État du worker\n"
        "⚽ /bets       — Paris en attente\n"
        "📊 /stats      — Win rate + ROI\n"
        "📈 /pourcent   — Taux de réussite rapide\n"
        "⚡ /run        — Lancer une analyse\n"
        "🔄 /refresh    — Refresh stats équipes\n"
        "🏆 /results    — Vérifier les résultats\n"
        "📅 /today      — Paris du jour\n"
        "📡 /api        — Quota Odds API\n"
        "🔥 /h2h        — Cache H2H\n"
        "🔄 /refreshh2h — Forcer refresh H2H\n"
        "🗑 /reset      — Effacer tous les paris\n"
        "🌐 /web        — Lien page web\n\n"
        f"<i>⏰ Analyse auto : {SCHEDULER_HOUR:02d}h00 UTC · /helpbiathlon pour le biathlon</i>"
    )


def handle_status():
    started      = worker_state["started_at"]
    last_run     = worker_state["last_run"]
    last_refresh = worker_state["last_refresh"]
    uptime = "N/A"
    if started:
        delta = datetime.now(timezone.utc) - started
        h, rem = divmod(int(delta.total_seconds()), 3600)
        uptime = f"{h}h {rem//60:02d}m"
    etat = "🔄 Analyse en cours..." if worker_state["running"] else "🟢 En attente"
    send_message(
        f"📡 <b>Status du Worker</b>\n\n"
        f"État : {etat}\n"
        f"⏱ Uptime : {uptime}\n"
        f"📅 Démarré : {started.strftime('%Y-%m-%d %H:%M UTC') if started else 'N/A'}\n"
        f"🕐 Prochaine analyse : {SCHEDULER_HOUR:02d}h00 UTC\n"
        f"⚽ Dernière analyse : {last_run.strftime('%Y-%m-%d %H:%M UTC') if last_run else 'Aucune'}\n"
        f"🔄 Dernier refresh : {last_refresh.strftime('%Y-%m-%d %H:%M UTC') if last_refresh else 'Aucun'}\n"
        f"🎯 Nouveaux bets dernière analyse : {worker_state['bets_today']}"
    )


def handle_bets():
    bets    = get_unique_bets(limit=200)
    pending = [b for b in bets if b["success"] == -1]

    if not pending:
        send_message("📭 <b>Aucun paris en attente.</b>\n💡 /run pour lancer une analyse.")
        return

    home_bets = [b for b in pending if b["market"] == "Home Win"]
    away_bets = [b for b in pending if b["market"] == "Away Win"]
    over_bets = [b for b in pending if b["market"] not in ("Home Win", "Away Win")]
    bn_bets   = [b for b in pending if b.get("bete_noire")]

    def fmt_bet(b):
        bn = " 🔥" if b.get("bete_noire") else ""
        return (
            f"  <b>{b['home_team']} vs {b['away_team']}</b>{bn}\n"
            f"  📅 {b['match_date']} · {b.get('league', '')}"
            f" · @ <b>{b['bk_odds']}</b> · +{b['value']*100:.1f}% · {b['probability']*100:.0f}%\n"
        )

    msg = f"⏳ <b>Paris en attente — {len(pending)} sélections</b>\n"
    if home_bets:
        msg += f"\n🏠 <b>Domicile ({len(home_bets)})</b>\n"
        for b in home_bets[:8]: msg += fmt_bet(b)
    if away_bets:
        msg += f"\n✈️ <b>Extérieur ({len(away_bets)})</b>\n"
        for b in away_bets[:8]: msg += fmt_bet(b)
    if over_bets:
        msg += f"\n⚽ <b>Over/Under ({len(over_bets)})</b>\n"
        for b in over_bets[:8]: msg += fmt_bet(b)
    if bn_bets:
        msg += f"\n🔥 <b>Bête Noire ({len(bn_bets)})</b>\n"
        for b in bn_bets[:5]: msg += fmt_bet(b)
    send_message(msg)


def handle_stats():
    from database import get_stats_by_market, get_stats_by_league_detailed, get_streak
    stats      = get_stats()
    o          = stats["overall"]
    by_market  = get_stats_by_market()
    by_league  = get_stats_by_league_detailed()
    streak     = get_streak()
    roi        = o.get("roi") or 0
    roi_sign   = "+" if roi >= 0 else ""

    # Streak
    streak_line = ""
    if streak and streak.get("count", 0) > 1:
        emoji = "🔥" if streak["type"] == "win" else "❄️"
        label = "victoires" if streak["type"] == "win" else "défaites"
        streak_line = f"\n{emoji} Série : <b>{streak['count']} {label} consécutives</b>"

    # Meilleur marché
    resolved = [m for m in by_market if (m.get("total",0) - m.get("pending",0)) >= 3]
    best = max(resolved, key=lambda x: x.get("roi", -999)) if resolved else None
    best_line = f"\n🏆 Meilleur marché : <b>{best['market']}</b> (+{best['roi']}% ROI)" if best else ""

    msg = (
        f"📊 <b>Statistiques ValueBet</b>\n\n"
        f"🎯 Total : <b>{o.get('total') or 0}</b> · ✅ {o.get('wins') or 0} · ❌ {o.get('losses') or 0} · ⏳ {o.get('pending') or 0}\n"
        f"📈 Win rate : <b>{o.get('win_rate') or 0}%</b> · ROI : <b>{roi_sign}{roi}%</b>\n"
        f"💎 Value moy. : <b>+{o.get('avg_value_pct') or 0}%</b>"
        f"{streak_line}{best_line}\n"
    )

    # Par marché
    if by_market:
        msg += "\n<b>Par marché :</b>\n"
        for m in by_market:
            roi_m = m.get("roi", 0)
            s = "+" if roi_m >= 0 else ""
            msg += f"  {m['market']} · {m.get('wins',0)}W/{m.get('losses',0)}L · WR {m.get('win_rate',0)}% · ROI {s}{roi_m}%\n"

    # Par ligue (top 5)
    if by_league:
        msg += "\n<b>Par ligue (top 5) :</b>\n"
        for l in by_league[:5]:
            roi_l = l.get("roi", 0)
            s = "+" if roi_l >= 0 else ""
            msg += f"  {l['league']} · {l.get('wins',0)}W/{l.get('losses',0)}L · ROI {s}{roi_l}%\n"

    send_message(msg)


def handle_pourcent():
    stats   = get_stats()
    o       = stats["overall"]
    total   = o.get("total") or 0
    wins    = o.get("wins") or 0
    losses  = o.get("losses") or 0
    pending = o.get("pending") or 0
    settled = total - pending
    if settled == 0:
        send_message("📊 Aucun pari résolu.\n💡 Tapez /results pour mettre à jour.")
        return
    win_rate = round(wins / settled * 100, 1)
    roi      = round((wins - losses) / settled * 100, 1)
    roi_sign = "+" if roi >= 0 else ""
    send_message(
        f"📈 <b>Taux de réussite</b>\n\n"
        f"✅ Gagnés : <b>{wins}</b>\n"
        f"❌ Perdus : <b>{losses}</b>\n"
        f"⏳ En attente : <b>{pending}</b>\n"
        f"📊 Total résolu : <b>{settled}</b>\n\n"
        f"🎯 Taux de réussite : <b>{win_rate}%</b>\n"
        f"💰 ROI : <b>{roi_sign}{roi}%</b>"
    )


def handle_run():
    send_message("⚡ <b>Analyse manuelle lancée !</b>\n💡 Tapez /bets après pour voir les sélections.")
    threading.Thread(target=run_value_bet_engine, daemon=True).start()


def handle_refresh():
    send_message("🔄 <b>Refresh des stats en cours...</b>")
    threading.Thread(target=refresh_team_stats, daemon=True).start()


def handle_results():
    send_message("🔍 <b>Vérification des résultats en cours...</b>")
    threading.Thread(target=check_results, daemon=True).start()


def handle_reset():
    count = reset_all_bets()
    send_message(f"🗑 <b>Reset effectué</b> — {count} paris supprimés.\n\nBase de données vierge ✅")


def handle_web():
    url = os.getenv("WEB_URL", "")
    if url:
        send_message(f"🌐 <b>Interface Web ValueBet</b>\n\n👉 {url}")
    else:
        send_message("⚠️ Variable WEB_URL non configurée dans Railway.")


def handle_api():
    from api_clients import get_odds_api_usage
    usage = get_odds_api_usage()
    if usage.get("error"):
        send_message(f"❌ <b>Erreur API</b> : {usage['error']}")
        return
    used      = usage.get("used", 0)
    remaining = usage.get("remaining", 0)
    total     = usage.get("total", used + remaining)
    pct_used  = round(used / max(total, 1) * 100, 1)
    bar_filled = int(pct_used / 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)
    send_message(
        f"📡 <b>Quota The Odds API</b>\n\n"
        f"[{bar}] {pct_used}%\n\n"
        f"✅ Utilisées : <b>{used}</b>\n"
        f"🟢 Restantes : <b>{remaining}</b>\n"
        f"📊 Total : <b>{total}</b>/mois\n\n"
        f"<i>Chaque run utilise ~{len([39,61,78,135,140,88,94,40,2,144,203,179,3]) * 2} requêtes</i>"
    )


def handle_today():
    from datetime import datetime
    today = datetime.now(timezone.utc).date().isoformat()
    bets  = get_unique_bets(limit=200)
    today_bets = [b for b in bets if b.get("match_date") == today]
    pending    = [b for b in today_bets if b["success"] == -1]
    if not pending:
        send_message(f"📅 <b>Aucun paris pour aujourd'hui ({today})</b>\n💡 L'analyse tourne à {SCHEDULER_HOUR:02d}h00 UTC.")
        return
    home_b = [b for b in pending if b["market"] == "Home Win"]
    away_b = [b for b in pending if b["market"] == "Away Win"]
    over_b = [b for b in pending if b["market"] not in ("Home Win","Away Win")]
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


def handle_api():
    from api_clients import get_odds_quota
    quota = get_odds_quota()
    remaining = quota.get("remaining")
    used      = quota.get("used")
    updated   = quota.get("last_update", "")[:16].replace("T", " ") if quota.get("last_update") else "jamais"

    if remaining is None:
        send_message(
            "📡 <b>Quotas API</b>\n\n"
            "⚠️ Pas encore de données — lancez /run pour initialiser.\n\n"
            "<b>The Odds API</b> : plan gratuit = 500 req/mois\n"
            "<b>Football-Data.org</b> : plan gratuit = 10 req/min"
        )
        return

    total = 500
    pct   = round(remaining / total * 100)
    bar_filled = round(pct / 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)

    color_hint = "🟢" if pct > 40 else "🟡" if pct > 15 else "🔴"

    send_message(
        f"📡 <b>Quotas API</b>\n\n"
        f"<b>The Odds API</b> (500/mois)\n"
        f"{color_hint} {bar} {pct}%\n"
        f"  ✅ Utilisées : <b>{used}</b>\n"
        f"  💚 Restantes : <b>{remaining}</b>\n"
        f"  📅 Mis à jour : {updated}\n\n"
        f"<b>Football-Data.org</b>\n"
        f"  ⏱ Rate limit : 10 req/min (auto-géré)\n"
        f"  🔄 Retry automatique sur 429"
    )


def handle_h2h():
    """Affiche le statut du cache H2H en DB et propose un refresh."""
    from database import get_h2h_cache_status
    rows = get_h2h_cache_status()
    if not rows:
        send_message(
            "📭 <b>Cache H2H vide</b>\n\n"
            "Les données H2H seront fetchées automatiquement au prochain run.\n"
            "Ou tapez /refreshh2h pour les charger maintenant."
        )
        return

    from collections import defaultdict
    by_league = defaultdict(list)
    for r in rows:
        name = LEAGUE_NAMES.get(r.get("league_id"), str(r.get("league_id")))
        by_league[name].append(r)

    msg = "🔥 <b>Cache H2H en base de données</b>\n\n"
    for league_name, entries in sorted(by_league.items()):
        msg += f"<b>{league_name}</b>\n"
        for e in sorted(entries, key=lambda x: x.get("season", 0), reverse=True):
            age = e.get("age_days", "?")
            count = e.get("match_count", "?")
            freshness = "✅" if (age or 99) <= 7 else "⚠️ expiré"
            msg += f"  {e.get('season')} : {count} matchs · {age}j {freshness}\n"
        msg += "\n"

    msg += "<i>TTL : 7 jours · /refreshh2h pour forcer la mise à jour</i>"
    send_message(msg)


def handle_refresh_h2h():
    """Force le re-fetch de toutes les données H2H depuis football-data.org."""
    from database import get_h2h_cache_status
    import database as db_module
    import psycopg2, sqlite3

    send_message("🔄 <b>Refresh H2H démarré...</b>\nCela peut prendre quelques minutes (rate limit FD).")

    # Efface le cache DB H2H pour forcer le re-fetch
    conn = db_module.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM h2h_cache")
        conn.commit()
    finally:
        conn.close()

    # Efface aussi le cache mémoire
    from api_clients import clear_h2h_cache
    clear_h2h_cache()

    # Re-fetch pour toutes les ligues configurées
    from api_clients import prefetch_season_matches, FOOTBALLDATA_LEAGUE_MAP
    current_season = int(os.getenv("SEASON", 2025))
    seasons = [current_season, current_season - 1]
    fetched = 0
    errors  = 0

    for league_id in LEAGUES:
        if not FOOTBALLDATA_LEAGUE_MAP.get(league_id):
            continue
        try:
            matches = prefetch_season_matches(league_id, seasons)
            fetched += 1
        except Exception as e:
            errors += 1

    send_message(
        f"✅ <b>Refresh H2H terminé</b>\n\n"
        f"✅ {fetched} ligues mises à jour\n"
        f"{'❌ ' + str(errors) + ' erreurs' if errors else ''}\n"
        f"<i>Cache valide 7 jours — 0 appel API au prochain run</i>"
    )


BIATHLON_WORKER_URL = os.getenv("BIATHLON_WORKER_URL", "")  # ex: http://biathlon-worker.railway.internal:5001

def dispatch_biathlon(cmd: str):
    """Dispatch une commande /biathlon* vers le worker biathlon via HTTP interne."""
    if not BIATHLON_WORKER_URL:
        send_message("⚠️ Worker biathlon non configuré (BIATHLON_WORKER_URL manquant)")
        return
    try:
        import requests as req
        req.get(f"{BIATHLON_WORKER_URL}", params={"cmd": cmd}, timeout=5)
    except Exception as e:
        send_message(f"⚠️ Worker biathlon injoignable : {e}")

COMMANDS = {
    "/help":          handle_help,
    "/status":        handle_status,
    "/bets":          handle_bets,
    "/stats":         handle_stats,
    "/pourcent":      handle_pourcent,
    "/run":           handle_run,
    "/refresh":       handle_refresh,
    "/results":       handle_results,
    "/reset":         handle_reset,
    "/today":         handle_today,
    "/api":           handle_api,
    "/h2h":           handle_h2h,
    "/refreshh2h":    handle_refresh_h2h,
    "/web":           handle_web,
    "/helpbiathlon":  lambda: dispatch_biathlon("/biathlon"),
    # Biathlon — dispatchés vers le worker dédié
    "/biathlon":         lambda: dispatch_biathlon("/biathlon"),
    "/biathlonrun":      lambda: dispatch_biathlon("/biathlonrun"),
    "/biathlonresults":  lambda: dispatch_biathlon("/biathlonresults"),
    "/biathlonstats":    lambda: dispatch_biathlon("/biathlonstats"),
}


# ─────────────────────────────────────────────
# POLLING TELEGRAM
# ─────────────────────────────────────────────

def telegram_polling():
    if not TELEGRAM_TOKEN:
        log.warning("⚠️ TELEGRAM_BOT_TOKEN manquant — polling désactivé.")
        return

    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    offset   = None

    log.info(f"📲 Telegram polling démarré — chat_id autorisé : {TELEGRAM_CHAT}")

    try:
        resp    = requests.get(f"{base_url}/getUpdates", params={"offset": -1}, timeout=10)
        results = resp.json().get("result", [])
        if results:
            offset = results[-1]["update_id"] + 1
            log.info(f"  {len(results)} anciens messages ignorés, offset={offset}")
    except Exception as e:
        log.error(f"  Erreur init polling: {e}")

    while True:
        try:
            params = {"timeout": 5, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset
            resp    = requests.get(f"{base_url}/getUpdates", params=params, timeout=10)
            updates = resp.json().get("result", [])

            for update in updates:
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().split()[0].lower()
                from_id = str(msg.get("chat", {}).get("id", ""))

                log.info(f"📩 Message reçu : '{text}' de {from_id}")

                if TELEGRAM_CHAT and from_id != TELEGRAM_CHAT:
                    log.warning(f"  Ignoré — chat_id non autorisé : {from_id}")
                    continue

                if text in COMMANDS:
                    log.info(f"  → Exécution : {text}")
                    try:
                        COMMANDS[text]()
                    except Exception as e:
                        log.error(f"  Erreur {text}: {e}")
                        send_message(f"❌ Erreur {text} : {e}")
                elif text.startswith("/biathlon") or text.startswith("/b_"):
                    # Commande biathlon non reconnue → dispatch générique
                    dispatch_biathlon(text)
                elif text.startswith("/help") and "biathlon" in text:
                    # /helpbiathlon → dispatcher vers /biathlon
                    dispatch_biathlon("/biathlon")
                elif text.startswith("/"):
                    # Commande inconnue → silence (pas de flood help)
                    log.info(f"  Commande inconnue ignorée : {text}")

        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(3)


# ─────────────────────────────────────────────
# SCHEDULER PRINCIPAL
# ─────────────────────────────────────────────

def run_scheduler():
    from apscheduler.schedulers.blocking import BlockingScheduler

    worker_state["started_at"] = datetime.now(timezone.utc)

    log.info("Démarrage thread polling Telegram...")
    threading.Thread(target=telegram_polling, daemon=True).start()
    log.info("Thread polling démarré ✅")

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        refresh_team_stats, "cron",
        hour=6, minute=0, id="refresh_stats",
        kwargs={"silent": True}
    )
    scheduler.add_job(
        smart_run_value_bet_engine, "cron",
        hour=SCHEDULER_HOUR, minute=0, id="daily_value_bets",
    )
    scheduler.add_job(
        check_results, "cron",
        hour=23, minute=0, id="check_results",
        kwargs={"silent": False}
    )

    log.info(f"⏰ Scheduler : refresh 06h, analyse {SCHEDULER_HOUR:02d}h, résultats 23h UTC")

    send_message(
        f"🐺 <b>Le Loup de Wall Bet</b> — {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        f"⚽ Worker Foot <b>opérationnel</b>\n"
        f"💬 /help · /helpbiathlon pour le biathlon"
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler arrêté.")
        send_message("🛑 <b>Worker ValueBet arrêté.</b>")


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    init_db()
    command = sys.argv[1] if len(sys.argv) > 1 else "schedule"
    if command == "refresh":
        refresh_team_stats()
    elif command == "schedule":
        run_scheduler()
    elif command == "run":
        run_value_bet_engine()
    elif command == "results":
        check_results()
    else:
        print(f"Usage: python scheduler.py [run|refresh|schedule|results]")