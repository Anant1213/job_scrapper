import os, requests

BOT = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT = os.environ.get("TELEGRAM_CHAT_ID")

def send(msg: str):
    if not BOT or not CHAT:
        print("Telegram not configured; skipping send.")
        return
    url = f"https://api.telegram.org/bot{BOT}/sendMessage"
    data = {"chat_id": CHAT, "text": msg, "disable_web_page_preview": True, "parse_mode": "HTML"}
    r = requests.post(url, data=data, timeout=20)
    r.raise_for_status()
