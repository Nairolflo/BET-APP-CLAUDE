"""
sports/football/jobs.py — Jobs foot (refresh stats, analyse, résultats)
Identique à scheduler.py actuel, juste réorganisé en module indépendant.
"""
import os
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

# ── State partagé (mis à jour par les jobs, lu par les handlers) ──
state = {
    "started_at":   None,
    "last_run":     None,
    "last_refresh": None,
    "bets_today":   0,
    "running":      False,
}

LEAGUE_NAMES = {
    39:  "Premier League",
    61:  "Ligue 1",
    78:  "Bundesliga",
    135: "Serie A",
    140: "La Liga",
    88:  "Eredivisie",
    94:  "Primeira Liga",
    40:  "Championship",
    2:   "Champions League",
    144: "Belgium First Div",
    203: "Turkey Super League",
    179: "Scottish Premiership",
    3:   "Europa League",
}

SEASON          = int(os.getenv("SEASON", 2025))
DEFAULT_LEAGUES = "39,61,78,135,140,88,94,40,2,144,203,179,3"
LEAGUES         = [int(x) for x in os.getenv("LEAGUES", DEFAULT_LEAGUES).split(",")]
VALUE_THRESHOLD = float(os.getenv("VALUE_THRESHOLD", 0.02))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", 0.55))
DAYS_AHEAD      = int(os.getenv("SCHEDULER_DAYS_AHEAD", 10))
SCHEDULER_HOUR  = int(os.getenv("SCHEDULER_HOUR", 8))


def refresh_team_stats(silent=False):
    from database import save_team_stats
    from api_clients import get_team_standings
    from core.telegram import send_message

    log.info("Refresh stats equipes...")
    results = []
    for league_id in LEAGUES:
        try:
            teams = get_team_standings(league_id, SEASON)
            for t in teams:
                save_team_stats(t)
            msg = f"✅ {LEAGUE_NAMES.get(league_id, league_id)} : {len(teams)} équipes"
            results.append(msg)
        except Exception as e:
            msg = f"❌ {LEAGUE_NAMES.get(league_id, league_id)} : {e}"
            results.append(msg)

    state["last_refresh"] = datetime.now(timezone.utc)
    if not silent:
        send_message("🔄 <b>Refresh stats terminé</b>\n\n" + "\n".join(results))
    return results


def smart_run(silent=False):
    """Vérifie si des matchs existent avant de consommer le quota."""
    from api_clients import get_fixtures
    from core.telegram import send_message

    today      = datetime.now(timezone.utc).date()
    near_dates = {(today + timedelta(days=i)).isoformat() for i in range(3)}

    has_fixtures = False
    for league_id in LEAGUES:
        try:
            fixtures = get_fixtures(league_id, SEASON, 3)
            if any(f.get("date", "")[:10] in near_dates for f in fixtures):
                has_fixtures = True
                break
        except Exception:
            pass

    if not has_fixtures:
        log.info("Smart run : aucun match dans 3 jours — annulé")
        send_message(
            "⏭️ <b>Analyse auto annulée</b>\n"
            "Aucun match dans les 3 prochains jours.\n"
            "<i>Quota préservé. /run pour forcer.</i>"
        )
        return

    run(silent=silent)


def run(silent=False):
    """Moteur principal value bet foot."""
    from database import (
        save_bet, get_team_stats, save_team_stats,
        is_bet_notified, mark_bet_notified, delete_today_pending_bets,
    )
    from api_clients import (
        get_odds_quota, odds_quota_ok, clear_odds_cache,
        get_fixtures, get_odds, get_team_standings,
        normalize_team_name, get_h2h, clear_h2h_cache,
        get_recent_form, clear_form_cache, FOOTBALLDATA_LEAGUE_MAP,
    )
    from model import (
        calc_league_averages, calc_attack_defense_strength,
        predict_match, find_value_bets,
    )
    from core.telegram import send_message, send_daily_summary

    if state["running"]:
        send_message("⏳ Une analyse est déjà en cours...")
        return

    state["running"] = True

    if not silent:
        if not odds_quota_ok(required=len(LEAGUES)):
            rem = get_odds_quota().get("remaining", "?")
            send_message(
                f"⚠️ <b>Run annulé — quota insuffisant</b>\n"
                f"Restantes : <b>{rem}</b> / seuil : 30\n"
                f"<i>/run pour forcer.</i>"
            )
            state["running"] = False
            return

    clear_h2h_cache()
    clear_form_cache()
    clear_odds_cache()
    delete_today_pending_bets()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info(f"VALUE BET ENGINE — {now}")
    if not silent:
        send_message(f"🚀 <b>Analyse démarrée</b>\n📅 {now}\n🔍 Calcul en cours...")

    new_value_bets = []
    errors = []

    for league_id in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        log.info(f"[{league_name}]")

        try:
            fixtures = get_fixtures(league_id, SEASON, DAYS_AHEAD)
        except Exception as e:
            errors.append(f"Fixtures {league_name}: {e}")
            continue

        if not fixtures:
            continue

        team_stats = get_team_stats(league_id, SEASON)
        if not team_stats:
            try:
                teams = get_team_standings(league_id, SEASON)
                for t in teams:
                    save_team_stats(t)
                team_stats = get_team_stats(league_id, SEASON)
            except Exception as e:
                errors.append(f"Stats {league_name}: {e}")
                continue

        avg_home, avg_away = calc_league_averages(team_stats)
        strengths = calc_attack_defense_strength(team_stats, avg_home, avg_away)

        recent_form = {}
        if FOOTBALLDATA_LEAGUE_MAP.get(league_id):
            try:
                recent_form = get_recent_form(league_id, SEASON)
            except Exception:
                pass

        today      = datetime.now(timezone.utc).date()
        near_dates = {(today + timedelta(days=i)).isoformat() for i in range(3)}
        near_fix   = [f for f in fixtures if f.get("date", "")[:10] in near_dates]

        odds_events = []
        if near_fix:
            try:
                odds_events = get_odds(league_id)
            except Exception as e:
                errors.append(f"Cotes {league_name}: {e}")

        odds_lookup = {
            (ev["home_team"].lower(), ev["away_team"].lower()): ev["odds"]
            for ev in odds_events
        }

        for fix in fixtures:
            home_name = fix["home_team_name"]
            away_name = fix["away_team_name"]

            ou_thresholds = sorted({
                float(k.replace("over_", "").replace("_", "."))
                for bk in odds_lookup.values()
                for k in bk if k.startswith("over_")
            }) or [1.5, 2.5, 3.5, 4.5]

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

            odds = odds_lookup.get((home_name.lower(), away_name.lower()), {})
            if not odds:
                for (h_k, a_k), o in odds_lookup.items():
                    if (h_k == h_norm or h_norm in h_k or h_k in h_norm) and \
                       (a_k == a_norm or a_norm in a_k or a_k in a_norm):
                        odds = o
                        break
            if not odds:
                continue

            h2h = None
            if FOOTBALLDATA_LEAGUE_MAP.get(league_id):
                try:
                    h2h = get_h2h(league_id, home_name, away_name, match_date=fix["date"])
                except Exception:
                    pass

            value_bets = find_value_bets(
                prediction, odds, VALUE_THRESHOLD, MIN_PROBABILITY, h2h=h2h
            )
            match_info = {
                "date":      fix["date"],
                "home_team": home_name,
                "away_team": away_name,
                "league":    league_name,
            }

            for bet in value_bets:
                try:
                    from database import save_bet
                    bet_id = save_bet({
                        "match_date": fix["date"],
                        "league":     league_name,
                        "home_team":  home_name,
                        "away_team":  away_name,
                        **bet,
                    })
                    if not is_bet_notified(bet_id):
                        new_value_bets.append((bet, match_info))
                        mark_bet_notified(bet_id)
                except Exception as e:
                    log.error(f"save_bet: {e}")

    state["last_run"]   = datetime.now(timezone.utc)
    state["bets_today"] = len(new_value_bets)
    state["running"]    = False

    try:
        from api_clients import get_odds_api_usage
        remaining  = get_odds_api_usage().get("remaining", "?")
        quota_line = f"\n📡 Quota : <b>{remaining}</b> req. restantes"
    except Exception:
        quota_line = ""

    send_daily_summary(new_value_bets, extra=quota_line)
    if errors:
        send_message("⚠️ <b>Erreurs :</b>\n" + "\n".join(f"• {e}" for e in errors))


def check_results(silent=False):
    """Vérifie les résultats des bets en attente."""
    from database import get_pending_bets, update_bet_result
    from api_clients import (
        get_all_results_today, get_fixtures_results_batch, normalize_team_name,
    )
    from core.telegram import send_message

    pending = get_pending_bets()
    if not pending:
        if not silent:
            send_message("📭 Aucun bet en attente.")
        return

    from collections import defaultdict
    by_date = defaultdict(list)
    for bet in pending:
        by_date[bet["match_date"]].append(bet)

    league_name_to_id = {v: k for k, v in LEAGUE_NAMES.items()}

    def fuzzy(n1, n2):
        a, b = normalize_team_name(n1), normalize_team_name(n2)
        if a == b or a in b or b in a:
            return True
        for w in ["fc", "af", "sc", "afc", "cf", "rc", "as", "ac", "us", "oc"]:
            a = a.replace(f" {w}", "").replace(f"{w} ", "")
            b = b.replace(f" {w}", "").replace(f"{w} ", "")
        return a.strip() == b.strip()

    won, lost = [], []

    for match_date, bets in by_date.items():
        all_results = get_all_results_today(match_date)
        for bet in bets:
            lid = league_name_to_id.get(bet.get("league", ""))
            if lid:
                all_results.update(get_fixtures_results_batch(lid, SEASON, match_date))

        for bet in bets:
            # Cherche match normal (home=home, away=away)
            result   = None
            inverted = False
            for (h, a), r in all_results.items():
                if fuzzy(bet["home_team"], h) and fuzzy(bet["away_team"], a):
                    result = r
                    break
            # Fallback : football-data inverse parfois home/away
            if not result:
                for (h, a), r in all_results.items():
                    if fuzzy(bet["home_team"], a) and fuzzy(bet["away_team"], h):
                        result   = r
                        inverted = True
                        break
            if not result:
                continue

            # Si inversé, on swap les buts
            if inverted:
                hg = result["away_goals"]
                ag = result["home_goals"]
            else:
                hg = result["home_goals"]
                ag = result["away_goals"]
            total  = hg + ag
            market = bet["market"]
            success = None

            if market == "Home Win":    success = 1 if hg > ag else 0
            elif market == "Away Win":  success = 1 if ag > hg else 0
            elif market == "Draw":      success = 1 if hg == ag else 0
            elif market.startswith("Over "):
                try: success = 1 if total > float(market.split()[1]) else 0
                except ValueError: pass
            elif market.startswith("Under "):
                try: success = 1 if total < float(market.split()[1]) else 0
                except ValueError: pass

            if success is not None:
                update_bet_result(bet["id"], success)
                score = f"{hg}-{ag}"
                (won if success == 1 else lost).append({**bet, "score": score})

    if not won and not lost:
        if not silent:
            send_message("⏳ Résultats pas encore disponibles.")
        return

    msg = "📊 <b>Résultats foot</b>\n\n"
    if won:
        msg += f"✅ <b>Gagnés ({len(won)})</b>\n"
        for b in won:
            msg += f"  • {b['home_team']} vs {b['away_team']} — {b['market']} @ {b['bk_odds']} ({b['score']})\n"
    if lost:
        msg += f"\n❌ <b>Perdus ({len(lost)})</b>\n"
        for b in lost:
            msg += f"  • {b['home_team']} vs {b['away_team']} — {b['market']} @ {b['bk_odds']} ({b['score']})\n"

    if not silent:
        send_message(msg)