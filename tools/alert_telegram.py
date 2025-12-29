# tools/alert_telegram.py
"""
Telegram notification module for job alerts.
"""
import os
import requests
from typing import Optional


BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def send(msg: str, parse_mode: str = "HTML") -> bool:
    """
    Send a message via Telegram bot.
    
    Args:
        msg: Message text (supports HTML formatting)
        parse_mode: Telegram parse mode (HTML or Markdown)
        
    Returns:
        True if message was sent successfully, False otherwise
    """
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured; skipping send.")
        print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.")
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": msg,
        "disable_web_page_preview": True,
        "parse_mode": parse_mode,
    }
    
    try:
        r = requests.post(url, data=data, timeout=20)
        r.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Telegram send error: {e}")
        return False
