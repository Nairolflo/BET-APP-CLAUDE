"""
core/telegram.py — Envoi de messages et boutons Telegram
Gère : send_message, send_buttons (inline keyboard), send_daily_summary
"""
import os
import requests
import logging

log = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def _token():
    return os.getenv("TELEGRAM_BOT_TOKEN", "")

def _chat():
    return os.getenv("TELEGRAM_CHAT_ID", "")


def send_message(text: str, parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
    token   = _token()
    chat_id = _chat()
    if not token or not chat_id:
        log.warning("[Telegram] BOT_TOKEN ou CHAT_ID manquant")
        return False
    url     = TELEGRAM_API.format(token=token, method="sendMessage")
    payload = {
        "chat_id":    chat_id,
        "text":       text[:4096],
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"[Telegram] send_message error: {e}")
        return False


def edit_message(chat_id: str, message_id: int, text: str,
                 parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
    """Édite un message existant (pour les menus boutons)."""
    token = _token()
    if not token:
        return False
    url     = TELEGRAM_API.format(token=token, method="editMessageText")
    payload = {
        "chat_id":    chat_id,
        "message_id": message_id,
        "text":       text[:4096],
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"[Telegram] edit_message error: {e}")
        return False


def answer_callback(callback_query_id: str, text: str = "") -> bool:
    """Répond à un callback (enlève le loading sur le bouton)."""
    token = _token()
    if not token:
        return False
    url = TELEGRAM_API.format(token=token, method="answerCallbackQuery")
    try:
        requests.post(url, json={
            "callback_query_id": callback_query_id,
            "text": text,
        }, timeout=5)
        return True
    except Exception as e:
        log.error(f"[Telegram] answer_callback error: {e}")
        return False


def make_keyboard(buttons: list[list[dict]]) -> dict:
    """
    Crée un inline keyboard.
    buttons = [[{"text": "⚽ Foot", "callback_data": "menu_foot"}, ...], ...]
    """
    return {"inline_keyboard": buttons}


def send_menu_principal():
    """Menu principal — choix du sport."""
    keyboard = make_keyboard([
        [
            {"text": "⚽ Foot",     "callback_data": "menu_foot"},
            {"text": "🎿 Biathlon", "callback_data": "menu_biathlon"},
        ],
        [
            {"text": "📊 Stats globales", "callback_data": "stats_global"},
            {"text": "🌐 Web",            "callback_data": "web"},
        ],
    ])
    send_message(
        "🐺 <b>Le Loup de Wall Bet</b>\n\nChoisissez un sport :",
        reply_markup=keyboard
    )


def send_menu_foot():
    """Menu foot."""
    keyboard = make_keyboard([
        [
            {"text": "⏳ Paris en attente", "callback_data": "foot_bets"},
            {"text": "📅 Aujourd'hui",      "callback_data": "foot_today"},
        ],
        [
            {"text": "📊 Stats",    "callback_data": "foot_stats"},
            {"text": "📈 % Succès", "callback_data": "foot_pourcent"},
        ],
        [
            {"text": "⚡ Lancer analyse", "callback_data": "foot_run"},
            {"text": "🏆 Résultats",      "callback_data": "foot_results"},
        ],
        [
            {"text": "🔄 Refresh stats", "callback_data": "foot_refresh"},
            {"text": "📡 Quota API",     "callback_data": "foot_api"},
        ],
        [
            {"text": "🔥 Cache H2H",    "callback_data": "foot_h2h"},
            {"text": "🔄 Refresh H2H",  "callback_data": "foot_refreshh2h"},
        ],
        [
            {"text": "🗑 Reset",       "callback_data": "foot_reset"},
            {"text": "◀️ Menu principal", "callback_data": "menu_main"},
        ],
    ])
    send_message("⚽ <b>Football</b> — que voulez-vous faire ?", reply_markup=keyboard)


def send_menu_biathlon():
    """Menu biathlon."""
    keyboard = make_keyboard([
        [
            {"text": "📋 Statut courses",    "callback_data": "biat_status"},
            {"text": "⚡ Lancer analyse",     "callback_data": "biat_run"},
        ],
        [
            {"text": "🏆 Résultats",  "callback_data": "biat_results"},
            {"text": "📊 Stats",      "callback_data": "biat_stats"},
        ],
        [
            {"text": "◀️ Menu principal", "callback_data": "menu_main"},
        ],
    ])
    send_message("🎿 <b>Biathlon</b> — que voulez-vous faire ?", reply_markup=keyboard)


def send_daily_summary(value_bets: list, extra: str = ""):
    """Résumé foot groupé par catégorie."""
    if not value_bets:
        send_message("📭 <b>Aucun nouveau value bet.</b> La chasse continue ⚽" + extra)
        return

    top_count = int(os.getenv("TOP_BETS_COUNT", 10))
    bets      = value_bets[:top_count]

    home_bets = [(b, m) for b, m in bets if b["market"] == "Home Win"]
    away_bets = [(b, m) for b, m in bets if b["market"] == "Away Win"]
    over_bets = [(b, m) for b, m in bets if b["market"] not in ("Home Win", "Away Win")]
    bn_bets   = [(b, m) for b, m in bets if b.get("bete_noire")]

    def fmt(bet, match_info):
        vp = round(bet["value"] * 100, 1)
        pp = round(bet["probability"] * 100, 0)
        bn = " 🔥" if bet.get("bete_noire") else ""
        return (
            f"  <b>{match_info['home_team']} vs {match_info['away_team']}</b>{bn}\n"
            f"  📅 {match_info['date']} · {match_info.get('league','')}"
            f" · @ <b>{bet['bk_odds']}</b> · +{vp}% · {pp:.0f}% · {bet['bookmaker']}\n"
        )

    msg = f"🎯 <b>NOUVEAUX VALUE BETS — {len(bets)} sélection(s)</b>\n"
    if home_bets:
        msg += f"\n🏠 <b>Domicile ({len(home_bets)})</b>\n"
        for b, m in home_bets: msg += fmt(b, m)
    if away_bets:
        msg += f"\n✈️ <b>Extérieur ({len(away_bets)})</b>\n"
        for b, m in away_bets: msg += fmt(b, m)
    if over_bets:
        msg += f"\n⚽ <b>Over/Under ({len(over_bets)})</b>\n"
        for b, m in over_bets: msg += fmt(b, m)
    if bn_bets:
        msg += f"\n🔥 <b>Bête Noire ({len(bn_bets)})</b>\n"
        for b, m in bn_bets: msg += fmt(b, m)

    msg += "\n⚠️ <i>Pariez de façon responsable.</i>"
    if extra:
        msg += extra
    send_message(msg)

    # Alerte séparée bête noire
    if bn_bets:
        alert = "🔥🔥 <b>ALERTE BÊTE NOIRE</b> 🔥🔥\n\n"
        for bet, match_info in bn_bets:
            rate = round((bet.get("bete_noire_rate") or 0) * 100)
            alert += (
                f"<b>{match_info['home_team']} vs {match_info['away_team']}</b>\n"
                f"📌 {bet['market']} @ <b>{bet['bk_odds']}</b>\n"
                f"🔥 Domination H2H : <b>{rate}%</b>\n"
                f"💎 Value : <b>+{round(bet['value']*100,1)}%</b> | "
                f"Proba : {round(bet['probability']*100,0):.0f}%\n\n"
            )
        send_message(alert)
