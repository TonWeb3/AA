import json
import asyncio
from datetime import datetime
from typing import Dict, Any
from .config import settings
from .state import state, log_message, save_telegram_subscribers, load_telegram_subscribers
from .net_utils import get_proxy_url_for

def add_telegram_subscriber(chat: dict) -> bool:
    """Add a chat (user/group/channel) to the subscriber list. Returns True if new."""
    cid = str(chat.get("id"))
    if not cid or cid == "None":
        return False
    subs = state.get("telegram_subscribers", {})
    if cid in subs:
        return False
    subs[cid] = {
        "name": chat.get("username") or chat.get("title") or chat.get("first_name") or cid,
        "type": chat.get("type"),
        "added": datetime.now().isoformat(),
    }
    state["telegram_subscribers"] = subs
    save_telegram_subscribers()
    return True

def remove_telegram_subscriber(chat_id: str) -> bool:
    subs = state.get("telegram_subscribers", {})
    cid = str(chat_id)
    if cid in subs:
        del subs[cid]
        state["telegram_subscribers"] = subs
        save_telegram_subscribers()
        return True
    return False

async def send_telegram(text: str):
    """Best-effort Telegram alert, BROADCAST to every saved subscriber. No-op unless
    enabled, a bot token is set, and there is at least one subscriber. Never raises —
    a failed alert must not affect trading."""
    subs = state.get("telegram_subscribers", {})
    ids = list(subs.keys())
    if not (settings.TELEGRAM_ENABLED and settings.TELEGRAM_BOT_TOKEN and ids):
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        import httpx
        proxy = get_proxy_url_for(url)
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
            for cid in ids:
                try:
                    resp = await client.post(url, json={
                        "chat_id": cid,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    })
                    if resp.status_code != 200:
                        log_message(f"Telegram alert to {cid} failed: HTTP {resp.status_code} {resp.text[:100]}")
                except Exception as e:
                    log_message(f"Telegram alert to {cid} error: {e}")
    except Exception as e:
        log_message(f"Telegram alert error: {e}")

async def send_telegram_to(chat_id: str, text: str):
    """Send one message to a single chat id (used for subscribe/unsubscribe replies)."""
    if not settings.TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        import httpx
        proxy = get_proxy_url_for(url)
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
    except Exception:
        pass

async def telegram_poller():
    """Continuously read the bot's incoming messages (long-poll getUpdates) and
    auto-subscribe anyone who messages it. `/stop` (or `/unsubscribe`) removes them.
    Runs for the whole app lifetime; idle while Telegram is disabled or has no token."""
    offset = None
    while True:
        try:
            if not (settings.TELEGRAM_ENABLED and settings.TELEGRAM_BOT_TOKEN):
                await asyncio.sleep(5)
                continue
            import httpx
            url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/getUpdates"
            proxy = get_proxy_url_for(url)
            params = {"timeout": 25, "allowed_updates": json.dumps(["message", "my_chat_member", "channel_post"])}
            if offset is not None:
                params["offset"] = offset
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=35.0) as client:
                data = (await client.get(url, params=params)).json()
            if not data.get("ok"):
                await asyncio.sleep(5)
                continue
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                obj = u.get("message") or u.get("channel_post") or u.get("my_chat_member") or {}
                chat = obj.get("chat") or {}
                if chat.get("id") is None:
                    continue
                text = (obj.get("text") or "").strip().lower()
                if text in ("/stop", "/unsubscribe"):
                    if remove_telegram_subscriber(str(chat["id"])):
                        log_message(f"Telegram: {chat.get('username') or chat['id']} unsubscribed")
                        await send_telegram_to(str(chat["id"]), "🔕 You've unsubscribed from withdrawal alerts.")
                else:
                    if add_telegram_subscriber(chat):
                        log_message(f"Telegram: new subscriber {chat.get('username') or chat.get('title') or chat['id']} ({chat.get('type')})")
                        await send_telegram_to(str(chat["id"]), "🔔 Subscribed — you'll receive withdrawal alerts here. Send /stop to unsubscribe.")
        except Exception as e:
            log_message(f"Telegram poller error: {e}")
            await asyncio.sleep(5)
