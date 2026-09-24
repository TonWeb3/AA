import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from fastapi import APIRouter, Request, WebSocket, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates

from .config import settings, normalize_private_key, inspect_key_or_mnemonic
from .state import state, log_message
from .clob_trader import clob_trader
from .telegram_service import send_telegram, remove_telegram_subscriber
from . import data

router = APIRouter()
templates = Jinja2Templates(directory="templates")

_ws_clients = set()
_on_symbol_change_hook: Optional[Callable] = None

def set_symbol_change_hook(hook: Callable):
    global _on_symbol_change_hook
    _on_symbol_change_hook = hook

async def broadcast_state():
    """Push the current snapshot to every connected dashboard."""
    if not _ws_clients or not state.get("latest_data"):
        return
    latest = state["latest_data"]
    latest["log_seq"] = state.get("log_seq", 0)
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send_json(latest)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)

def reflect_running_now():
    ts = state.get("latest_data", {}).get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]


@router.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@router.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@router.get("/api/latest")
async def get_latest():
    return state.get("latest_data", {})

@router.websocket("/ws")
async def dashboard_ws(ws: WebSocket):
    await ws.accept()
    try:
        if state.get("latest_data"):
            await ws.send_json(state["latest_data"])
        _ws_clients.add(ws)
        while True:
            await ws.receive_text()
    except Exception:
        pass
    finally:
        _ws_clients.discard(ws)

@router.get("/api/logs")
async def get_logs():
    return state.get("logs", [])

DOWNLOADABLE = {
    "signals": ("logs/signals.csv", "text/csv"),
    "trades": ("state_data.json", "application/json"),
}

@router.get("/api/files")
async def list_files():
    out = []
    for key, (path, _) in DOWNLOADABLE.items():
        exists = os.path.exists(path)
        out.append({
            "key": key,
            "name": os.path.basename(path),
            "exists": exists,
            "size": os.path.getsize(path) if exists else 0,
            "rows": (max(0, sum(1 for _ in open(path, encoding="utf-8", errors="ignore")) - 1)
                     if exists and path.endswith(".csv") else None),
            "modified": (datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                         if exists else None),
        })
    return out

@router.get("/api/download/{key}")
async def download_file(key: str):
    entry = DOWNLOADABLE.get(key)
    if not entry:
        return JSONResponse({"error": "unknown_file"}, status_code=404)
    path, media = entry
    if not os.path.exists(path):
        return JSONResponse({"error": "not_generated_yet", "path": path}, status_code=404)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(os.path.basename(path))
    return FileResponse(path, media_type=media, filename=f"15m-{base}-{stamp}{ext}")

@router.post("/api/start")
async def start_trading():
    state["running"] = True
    reflect_running_now()
    log_message("Trading STARTED by user")
    await broadcast_state()
    return {"ok": True, "running": True}

@router.post("/api/stop")
async def stop_trading():
    state["running"] = False
    reflect_running_now()
    log_message("Trading STOPPED by user")
    await broadcast_state()
    return {"ok": True, "running": False}

@router.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_15m_series()

@router.get("/api/settings")
async def get_settings():
    def mask(v: str) -> str:
        return v[:6] + "..." + v[-4:] if v and len(v) > 10 else v

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": mask(settings.PRIVATE_KEY),
        "live": {
            "relayer_api_key": mask(settings.RELAYER_API_KEY),
            "alchemy_api_key": mask(settings.ALCHEMY_API_KEY),
            "max_slippage": settings.CLOB_MAX_SLIPPAGE,
            "entry_max_retries": settings.ENTRY_MAX_RETRIES,
            "entry_retry_delay_ms": settings.ENTRY_RETRY_DELAY_MS,
            "exit_max_retries": settings.EXIT_MAX_RETRIES
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD,
            "min_seconds_left": settings.MIN_SECONDS_LEFT
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "recipient_address": settings.WITHDRAW_ADDRESS,
            "auto_resume": settings.WITHDRAW_AUTO_RESUME,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER,
            "default_destination": (clob_trader.get_eoa_address() if clob_trader else "") or ""
        },
        "telegram": {
            "enabled": settings.TELEGRAM_ENABLED,
            "bot_token": "set" if settings.TELEGRAM_BOT_TOKEN else ""
        }
    }

@router.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    old_symbol = settings.SYMBOL

    if isinstance(new_settings.get("telegram"), dict) and new_settings["telegram"].get("bot_token") == "set":
        new_settings["telegram"].pop("bot_token", None)

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        try:
            settings.PRIVATE_KEY = normalize_private_key(new_pk)
            new_settings["private_key"] = settings.PRIVATE_KEY
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid private key or seed phrase: {e}")

    existing_cfg = {}
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged_cfg = deep_merge(existing_cfg, new_settings)
    with open("config.json", "w", encoding="utf-8") as f:
        json.dump(merged_cfg, f, indent=2)

    settings.MODE = new_settings.get("mode", settings.MODE)
    settings.PAPER_BALANCE_USD = float(new_settings.get("paper_balance_usd", settings.PAPER_BALANCE_USD))

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))
        settings.MIN_SECONDS_LEFT = float(e.get("min_seconds_left", settings.MIN_SECONDS_LEFT))

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        if "withdraw_address" in ce: settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
        if "auto_resume_after_withdrawal" in ce: settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]

    if "telegram" in new_settings:
        tg = new_settings["telegram"]
        if "enabled" in tg: settings.TELEGRAM_ENABLED = bool(tg["enabled"])
        if "bot_token" in tg: settings.TELEGRAM_BOT_TOKEN = tg["bot_token"]

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "live" in new_settings:
        lv = new_settings["live"]
        if "max_slippage" in lv:
            settings.CLOB_MAX_SLIPPAGE = float(lv["max_slippage"])
        if "entry_max_retries" in lv:
            settings.ENTRY_MAX_RETRIES = int(lv["entry_max_retries"])
        if "entry_retry_delay_ms" in lv:
            settings.ENTRY_RETRY_DELAY_MS = int(lv["entry_retry_delay_ms"])
        if "exit_max_retries" in lv:
            settings.EXIT_MAX_RETRIES = int(lv["exit_max_retries"])
        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    clob_trader.reset()
    state["trading_mode"] = settings.MODE
    if settings.MODE == "live" and settings.PRIVATE_KEY:
        real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
        if real_bal is not None:
            state["paper_balance"] = real_bal
        else:
            state["paper_balance"] = 0.0
    else:
        state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol and _on_symbol_change_hook:
        await _on_symbol_change_hook(old_symbol, settings.SYMBOL)

    return {"status": "ok"}

@router.post("/api/validate-key")
async def validate_key(body: Dict[str, Any]):
    secret = str(body.get("private_key", "")).strip()
    return inspect_key_or_mnemonic(secret)

@router.post("/api/setup-wallet")
async def setup_wallet(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from .config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    rk = body.get("relayer_api_key")
    if rk and "..." not in rk:
        settings.RELAYER_API_KEY = rk
    ak = body.get("alchemy_api_key")
    if ak and "..." not in ak:
        settings.ALCHEMY_API_KEY = ak
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.ensure_setup)
        if result.get("ok"):
            if result.get("skipped"):
                log_message("Wallet setup: already done this session")
            else:
                log_message(f"Wallet setup complete ({result.get('approvals', 0)} approvals)")
        else:
            log_message(f"Wallet setup failed: {result.get('error')}")
        return result
    except Exception as e:
        log_message(f"Wallet setup error: {e}")
        return {"ok": False, "error": str(e)}

@router.post("/api/test-connection")
async def test_connection(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from .config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    rk = body.get("relayer_api_key")
    if rk and "..." not in rk:
        settings.RELAYER_API_KEY = rk
    ak = body.get("alchemy_api_key")
    if ak and "..." not in ak:
        settings.ALCHEMY_API_KEY = ak
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.test_connection)
        if result.get("ok"):
            log_message(f"Connection OK — EOA {result.get('eoa')}, trading from "
                        f"{result.get('funder')} (sig type {result.get('chosen_signature_type')})")
        else:
            log_message(f"Connection test failed: {result.get('error')}")
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.post("/api/enable-auto-redeem")
async def enable_auto_redeem(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from .config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.enable_auto_redeem)
        log_message("Auto-redeem enabled" if result.get("ok")
                    else f"Auto-redeem failed: {result.get('error')}")
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/api/telegram-subscribers")
async def get_telegram_subscribers():
    subs = state.get("telegram_subscribers", {})
    sub_list = [{"chat_id": cid, **info} for cid, info in subs.items()]
    return {"count": len(sub_list), "subscribers": sub_list}

@router.post("/api/telegram-unsubscribe")
async def telegram_unsubscribe(body: Dict[str, Any]):
    cid = str(body.get("chat_id", ""))
    removed = remove_telegram_subscriber(cid)
    if removed:
        log_message(f"Telegram subscriber removed by user: {cid}")
    return {"ok": removed, "count": len(state.get("telegram_subscribers", {}))}

@router.post("/api/test-telegram")
async def test_telegram():
    if not settings.TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "missing_bot_token"}
    if not settings.TELEGRAM_ENABLED:
        return {"ok": False, "error": "telegram_alerts_disabled"}
    subs = state.get("telegram_subscribers", {})
    if not subs:
        return {"ok": False, "error": "no_subscribers_yet — send /start to your bot first"}
    await send_telegram("✅ <b>Test alert</b>\nThis chat will receive withdrawal alerts from your "
                        "Polymarket BTC 15m bot.")
    log_message(f"Telegram test alert broadcast to {len(subs)} subscriber(s)")
    return {"ok": True, "count": len(subs)}

@router.get("/health")
async def health():
    return {
        "status": "ok",
        "last_update": state.get("last_update_ts", 0),
        "mode": state.get("trading_mode", settings.MODE),
        "running": state.get("running", False)
    }

@router.get("/history")
async def get_history():
    return state.get("trade_history", [])
