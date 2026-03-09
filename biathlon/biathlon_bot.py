"""
biathlon_bot.py
---------------
Commandes Telegram pour le module biathlon.
S'intègre dans le scheduler.py existant du ValueBet bot.

Nouvelles commandes :
  /biathlon      — prochaines courses + value bets H2H détectés
  /biathlonrun   — lance l'analyse manuelle
  /biathlonpodium <race_id> — prédit le podium d'une course
  /biathlonstats <nom>      — profil d'un athlète

Intégration dans scheduler.py :
  Ajouter dans COMMANDS :
    "/biathlon":       handle_biathlon,
    "/biathlonrun":    handle_biathlon_run,

  Ajouter dans init_scheduler() :
    scheduler.add_job(run_biathlon_analysis, "cron",
                      hour=7, minute=30, id="biathlon_daily")
"""

import os
import logging
import threading
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# Import lazy pour éviter les dépendances circulaires
def _send(msg):
    from telegram_bot import send_message
    send_message(msg)


def run_biathlon_analysis(silent: bool = False) -> list:
    """
    Lance l'analyse complète biathlon :
    1. Récupère les prochaines courses (7 jours)
    2. Récupère les cotes disponibles
    3. Calcule les ratings + H2H proba pour chaque marché
    4. Détecte les value bets
    5. Envoie le résumé Telegram
    """
    from biathlon_client import get_upcoming_races, get_biathlon_events_from_ibu
    from biathlon_odds   import get_biathlon_events, parse_h2h_odds, find_value_bets
    from biathlon_model  import predict_h2h, RACE_FORMATS

    if not silent:
        _send("🎯 <b>Analyse Biathlon démarrée...</b>")

    # 1. Prochaines courses
    upcoming = get_upcoming_races(days_ahead=7)
    if not upcoming:
        if not silent:
            _send("📭 Aucune course biathlon dans les 7 prochains jours.")
        return []

    # 2. Cotes disponibles
    events = get_biathlon_events()
    h2h_markets = parse_h2h_odds(events)

    if not h2h_markets:
        if not silent:
            _send(
                f"⚠️ <b>Biathlon</b> : aucune cote H2H disponible actuellement.\n"
                f"📅 {len(upcoming)} course(s) dans les 7 prochains jours.\n"
                f"<i>Les cotes H2H apparaissent ~48h avant les grandes épreuves.</i>"
            )
        return []

    # 3. Calcul des probas H2H pour chaque marché coté
    h2h_predictions = {}
    for market in h2h_markets:
        name_a = market["athlete_a"]["name"]
        name_b = market["athlete_b"]["name"]

        # On cherche le format de course depuis les prochaines épreuves
        race_format = "SR"  # défaut : Sprint
        for race in upcoming:
            if any(keyword in market["event"].lower()
                   for keyword in [race["location"].lower(), race["description"].lower()[:10]]):
                race_format = race["format"]
                break

        # Recherche IBU_ID par nom (approximatif — en prod on ferait un mapping)
        # Pour l'instant on utilise le nom comme identifiant proxy
        pred = predict_h2h_by_name(name_a, name_b, race_format)
        if pred:
            h2h_predictions[(name_a.lower(), name_b.lower())] = {
                "prob_a": pred["prob_a_wins"],
                "prob_b": pred["prob_b_wins"],
            }

    # 4. Value bets
    value_bets = find_value_bets(h2h_markets, h2h_predictions)

    # 5. Envoi Telegram
    if not silent:
        _send_biathlon_summary(upcoming, value_bets, h2h_markets)

    return value_bets


def predict_h2h_by_name(name_a: str, name_b: str, race_format: str) -> dict:
    """
    Prédit H2H à partir des noms (sans IBU_ID connu).
    Recherche les athlètes par nom dans les résultats récents.

    En prod : maintenir un mapping nom → IBU_ID en DB.
    """
    from biathlon_client import get_competitions, get_results, CURRENT_SEASON

    # Cherche les IBU_IDs dans les résultats récents
    ibu_id_a, ibu_id_b = None, None

    races = get_competitions(CURRENT_SEASON)
    # Prend les 5 dernières courses officielles pour la recherche
    recent_official = [r for r in races if r.get("Status") == "Official"][-5:]

    for race in recent_official:
        if ibu_id_a and ibu_id_b:
            break
        results = get_results(race.get("RaceId", ""))
        for r in results:
            athlete_name = r.get("Name", "").upper()
            if not ibu_id_a and name_a.upper() in athlete_name:
                ibu_id_a = r.get("IBU_ID")
            if not ibu_id_b and name_b.upper() in athlete_name:
                ibu_id_b = r.get("IBU_ID")

    if not ibu_id_a or not ibu_id_b:
        return None

    from biathlon_model import predict_h2h
    pred = predict_h2h(ibu_id_a, ibu_id_b, race_format)
    if not pred:
        return None

    return {
        "prob_a_wins": pred["prob_a_wins"],
        "prob_b_wins": pred["prob_b_wins"],
        "rating_a":    pred["rating_a"],
        "rating_b":    pred["rating_b"],
    }


def _send_biathlon_summary(upcoming: list, value_bets: list, h2h_markets: list):
    """Formate et envoie le résumé biathlon sur Telegram."""
    from biathlon_client import RACE_FORMATS

    lines = ["🎿 <b>BIATHLON — Analyse du jour</b>\n"]

    # Prochaines courses
    lines.append("📅 <b>Prochaines épreuves :</b>")
    for r in upcoming[:5]:
        fmt = RACE_FORMATS.get(r["format"], r["format"])
        gender = "♀️" if r["gender"] == "W" else "♂️"
        lines.append(f"  {gender} {r['date']} — {r['description']} ({r['location']})")
    lines.append("")

    if not value_bets:
        lines.append(f"📡 {len(h2h_markets)} marché(s) H2H analysé(s)")
        lines.append("📭 Aucun value bet détecté (seuil 5%)")
        _send("\n".join(lines))
        return

    # Value bets détectés
    lines.append(f"🎯 <b>{len(value_bets)} VALUE BET(S) BIATHLON :</b>")

    for vb in value_bets[:8]:  # max 8 bets
        value_emoji = "🔥" if vb["value_pct"] >= 10 else "✅"
        lines.append(
            f"\n{value_emoji} <b>{vb['pick']}</b> bat {vb['opponent']}\n"
            f"   💰 Cote: <b>{vb['odd']}</b> ({vb['bookmaker']})\n"
            f"   📊 Modèle: {vb['prob_model']*100:.1f}% vs implicite: {vb['prob_implied']*100:.1f}%\n"
            f"   📈 Value: <b>+{vb['value_pct']:.1f}%</b> | Kelly: {vb['kelly_conservative']*100:.1f}%"
        )

    lines.append("\n⚠️ <i>Pariez de façon responsable.</i>")
    _send("\n".join(lines))


def format_podium_message(race_name: str, podium_results: list, top_n: int = 8) -> str:
    """Formate le message de prédiction de podium."""
    lines = [f"🏆 <b>PRÉDICTION PODIUM</b>\n{race_name}\n"]

    medals = ["🥇", "🥈", "🥉"]
    for i, athlete in enumerate(podium_results[:top_n]):
        medal = medals[i] if i < 3 else f"  {i+1}."
        p_top3_str = f"{athlete['p_top3']*100:.1f}%"
        p_win_str  = f"{athlete['p_win']*100:.1f}%"
        lines.append(
            f"{medal} <b>{athlete['name']}</b> ({athlete.get('nat','')})\n"
            f"   P(victoire): {p_win_str} | P(podium): <b>{p_top3_str}</b>"
        )

    lines.append(
        f"\n<i>Monte Carlo 1M simulations · "
        f"Modèle: ski {45}% / tir {45}% / forme {10}%</i>"
    )
    return "\n".join(lines)


# ─── Handlers Telegram ───────────────────────

def handle_biathlon():
    """Affiche les prochaines courses + value bets en cache."""
    from biathlon_client import get_upcoming_races
    from biathlon_odds import get_biathlon_events, parse_h2h_odds

    upcoming = get_upcoming_races(days_ahead=10)
    events   = get_biathlon_events()
    h2h      = parse_h2h_odds(events)

    if not upcoming:
        _send(
            "🎿 <b>Biathlon</b>\n\n"
            "Aucune course dans les 10 prochains jours.\n"
            "<i>Fin de saison CdM ou pause internationale.</i>"
        )
        return

    from biathlon_client import RACE_FORMATS
    lines = ["🎿 <b>BIATHLON — Calendrier + Marchés</b>\n"]
    lines.append(f"📅 <b>{len(upcoming)} prochaine(s) épreuve(s) :</b>")
    for r in upcoming:
        fmt    = RACE_FORMATS.get(r["format"], r["format"])
        gender = "♀️" if r["gender"] == "W" else "♂️"
        lines.append(f"  {gender} {r['date']} · {fmt} — {r['location']}")

    lines.append("")
    if h2h:
        lines.append(f"📡 <b>{len(h2h)} marché(s) H2H disponible(s)</b>")
        for m in h2h[:5]:
            lines.append(
                f"  ⚔️ {m['athlete_a']['name']} ({m['athlete_a']['best_odd']}) "
                f"vs {m['athlete_b']['name']} ({m['athlete_b']['best_odd']})"
            )
        lines.append("")
        lines.append("💡 /biathlonrun pour analyser les value bets")
    else:
        lines.append("📭 Aucune cote H2H disponible pour l'instant")
        lines.append("<i>Les cotes apparaissent ~48h avant les grandes épreuves</i>")

    _send("\n".join(lines))


def handle_biathlon_run():
    """Lance l'analyse biathlon en thread."""
    _send("🎯 Analyse biathlon lancée...")
    threading.Thread(
        target=run_biathlon_analysis,
        kwargs={"silent": False},
        daemon=True
    ).start()


# ─── À ajouter dans scheduler.py ──────────────
BIATHLON_COMMANDS = {
    "/biathlon":    handle_biathlon,
    "/biathlonrun": handle_biathlon_run,
}