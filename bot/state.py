import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from .config import settings

STATE_FILE = "state_data.json"
CONFIG_FILE = "config.json"
SUBSCRIBERS_FILE = "telegram_subscribers.json"

class StateManager:
    """Thread-safe, atomic state container with crash-resilient disk persistence."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._data: Dict[str, Any] = {
            "latest_data": {},
            "last_update_ts": 0,
            "trading_mode": settings.MODE,
            "paper_balance": settings.PAPER_BALANCE_USD,
            "active_trades": [],
            "trade_history": [],
            "logs": [],
            "last_trade_side": None,
            "last_balance_refresh": 0,
            "running": False,
            "market_opens": {},
            "last_window_start": None,
            "last_seen_price": None,
            "withdraw_state": "ARMED",
            "last_withdrawal": None,
            "withdraw_flat_since": None,
            "withdraw_submitted_at": 0,
            "withdraw_locked_market": None,
            "telegram_subscribers": {},
            "trade_ctx": {},
            "event_exec": None,
            "log_seq": 0,
        }

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    # ── Dict-compatible accessors ─────────────────────────────────────────────
    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any):
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any):
        self._data[key] = value

    def update(self, mapping: Dict[str, Any]):
        self._data.update(mapping)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def as_dict(self) -> Dict[str, Any]:
        return self._data

    # ── Logging ───────────────────────────────────────────────────────────────
    def log_message(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {msg}"
        print(formatted)
        self._data["logs"].append(formatted)
        if len(self._data["logs"]) > 100:
            self._data["logs"].pop(0)
        self._data["log_seq"] += 1

    # ── Persistence ───────────────────────────────────────────────────────────
    def save_state(self):
        """Atomic persistence: writes to temporary file then renames to avoid corruption."""
        try:
            data_to_save = {
                "paper_balance": self._data.get("paper_balance", settings.PAPER_BALANCE_USD),
                "active_trades": self._data.get("active_trades", []),
                "trade_history": self._data.get("trade_history", []),
                "last_trade_side": self._data.get("last_trade_side")
            }
            tmp_file = f"{STATE_FILE}.tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(data_to_save, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, STATE_FILE)

            if os.path.exists(CONFIG_FILE):
                try:
                    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                    cfg["paper_balance_usd"] = self._data.get("paper_balance")
                    cfg_tmp = f"{CONFIG_FILE}.tmp"
                    with open(cfg_tmp, "w", encoding="utf-8") as f:
                        json.dump(cfg, f, indent=2)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(cfg_tmp, CONFIG_FILE)
                except Exception as e:
                    print(f"Error syncing config.json: {e}")
        except Exception as e:
            print(f"Error saving state: {e}")

    def load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    self._data["paper_balance"] = loaded.get("paper_balance", settings.PAPER_BALANCE_USD)
                    self._data["active_trades"] = loaded.get("active_trades", [])
                    self._data["trade_history"] = loaded.get("trade_history", [])
                    self._data["last_trade_side"] = loaded.get("last_trade_side")
                    self.log_message("State loaded from state_data.json")
        except Exception as e:
            print(f"Error loading state: {e}")

    # ── Telegram Subscribers ──────────────────────────────────────────────────
    def load_telegram_subscribers(self):
        try:
            if os.path.exists(SUBSCRIBERS_FILE):
                with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
                    subs = json.load(f)
                if isinstance(subs, dict):
                    self._data["telegram_subscribers"] = {str(k): v for k, v in subs.items()}
        except Exception as e:
            print(f"Error loading telegram subscribers: {e}")

    def save_telegram_subscribers(self):
        try:
            subs = self._data.get("telegram_subscribers", {})
            tmp_file = f"{SUBSCRIBERS_FILE}.tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(subs, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, SUBSCRIBERS_FILE)
        except Exception as e:
            print(f"Error saving telegram subscribers: {e}")


state_manager = StateManager()
state = state_manager
log_message = state_manager.log_message
save_state = state_manager.save_state
load_state = state_manager.load_state
load_telegram_subscribers = state_manager.load_telegram_subscribers
save_telegram_subscribers = state_manager.save_telegram_subscribers
