"""
telegram_bot.py - Telegram notification for value bets
"""

import os
import requests


TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def send_message(text: str, parse_mode: str = "HTML") -> bool:
    """Send a message via Telegram Bot API."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if not token or not chat_id:
        print("[Telegram] Missing BOT_TOKEN or CHAT_ID — skipping notification.")
        return False

    url = TELEGRAM_API.format(token=token, method="sendMessage")
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] Error sending message: {e}")
        return False


def format_value_bet_message(bet: dict, match_info: dict) -> str:
    """Format a value bet as a Telegram HTML message."""
    value_pct = round(bet["value"] * 100, 1)
    prob_pct = round(bet["probability"] * 100, 1)

    emoji = "🟢" if value_pct >= 10 else "🟡"

    return (
        f"{emoji} <b>VALUE BET DÉTECTÉ</b>\n\n"
        f"⚽ <b>{match_info['home_team']} vs {match_info['away_team']}</b>\n"
        f"📅 {match_info['date']} — {match_info.get('league', '')}\n\n"
        f"📊 <b>Marché :</b> {bet['market']}\n"
        f"🏦 <b>Bookmaker :</b> {bet['bookmaker']}\n"
        f"💰 <b>Cote BK :</b> {bet['bk_odds']}\n"
        f"🧮 <b>Cote modèle :</b> {bet['model_odds']}\n"
        f"📈 <b>Probabilité :</b> {prob_pct}%\n"
        f"✨ <b>Value :</b> +{value_pct}%"
    )


def send_daily_summary(value_bets: list, match_infos: dict):
    """
    Send the top N value bets to Telegram.
    value_bets: list of (bet_dict, match_info_dict) tuples
    """
    if not value_bets:
        send_message("📭 <b>Aucun value bet trouvé aujourd'hui.</b>\nLa chasse continue demain ! ⚽")
        return

    top_count = int(os.getenv("TOP_BETS_COUNT", 5))
    top_bets = value_bets[:top_count]

    # Header
    header = (
        f"🎯 <b>VALUE BETS DU JOUR</b> — {len(top_bets)} sélection(s)\n"
        f"{'─' * 30}\n"
    )
    send_message(header)

    # Individual bets
    for bet, match_info in top_bets:
        msg = format_value_bet_message(bet, match_info)
        send_message(msg)

    # Footer disclaimer
    footer = (
        "\n⚠️ <i>Ces paris sont générés automatiquement par un modèle statistique. "
        "Pariez de façon responsable. Les performances passées ne garantissent pas les résultats futurs.</i>"
    )
    send_message(footer)
