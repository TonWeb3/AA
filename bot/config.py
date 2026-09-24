import os
import json
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Any, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    MODE: str = "paper"  # "paper" or "live"
    PAPER_BALANCE_USD: float = 1000.0
    PRIVATE_KEY: str = ""

    # ── Live trading (Polymarket CLOB V2) ───────────────────────────────────────
    # The wallet is DERIVED from PRIVATE_KEY (hex key or 12/24-word seed phrase) and
    # auto-detected: deposit-wallet (V2, signature_type 3) first, then legacy proxy /
    # safe — whichever actually holds pUSD. Nothing to pick by hand.
    CLOB_MAX_SLIPPAGE: float = 0.02  # marketable-limit buffer above the quote (probability units)
    RELAYER_API_KEY: str = ""        # Polymarket relayer API key (sponsors gasless on-chain setup)
    ALCHEMY_API_KEY: str = ""        # optional: dedicated Polygon RPC for chain reads
    ENTRY_MAX_RETRIES: int = 6       # retries for a failed/killed entry order
    ENTRY_RETRY_DELAY_MS: int = 250  # delay between entry retries in milliseconds (rate-limit safe)
    EXIT_MAX_RETRIES: int = 3        # bounded retries for a failed early-exit sell

    # ── Auto-withdrawal (capital extractor) ─────────────────────────────────────
    # LIVE mode only. Once equity (cash + open position value) reaches the trigger,
    # pause entries, close any open trade, withdraw AMOUNT of pUSD to your own wallet
    # (the key/seed EOA unless an address is set) and auto-resume next window.
    AUTO_WITHDRAW_ENABLED: bool = False
    WITHDRAW_TRIGGER_BALANCE: float = 2000.0  # withdraw once equity reaches this
    WITHDRAW_AMOUNT: float = 1000.0           # amount of pUSD to withdraw each time
    WITHDRAW_ADDRESS: str = ""                # destination (blank = your own key/seed EOA)
    WITHDRAW_AUTO_RESUME: bool = True         # resume trading after the withdrawal
    WITHDRAW_RESUME_AFTER: str = "submitted"  # "submitted" or "confirmed"

    # ── Telegram alerts ─────────────────────────────────────────────────────────
    # When enabled, a message is broadcast every time a withdrawal completes. Anyone
    # who sends the bot /start (or any message) is saved to telegram_subscribers.json
    # and receives every alert — no chat IDs to copy by hand.
    TELEGRAM_ENABLED: bool = False
    TELEGRAM_BOT_TOKEN: str = ""   # from @BotFather

    SYMBOL: str = "BTCUSDT"
    BINANCE_BASE_URL: str = "https://api.binance.com"
    GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    CLOB_BASE_URL: str = "https://clob.polymarket.com"

    POLL_INTERVAL_MS: int = 1000
    CANDLE_WINDOW_MINUTES: int = 15

    # Risk per trade: "percent" = RISK_VALUE% of balance; "fixed" = RISK_VALUE dollars.
    RISK_TYPE: str = "percent"
    RISK_VALUE: float = 10.0

    # ── Latency-arb entry engine ────────────────────────────────────────────────
    # Fair probability (fast, from Binance spot) vs Polymarket's implied price.
    # Enter when EV = fair - ask_price clears EV_THRESHOLD (the book looks stale).
    EV_THRESHOLD: float = 0.04          # require >= this expected value per $1 share (after price)
    MIN_PROB_EV: float = 0.55           # don't bet near-coinflips even if EV looks positive
    MIN_BOOK_LIQUIDITY_USD: float = 20.0  # skip if the ask side can't absorb the stake
    # Near expiry the model is near-certain, so EV vs any stale quote looks huge — but a
    # FOK order into a closing book is an unreliable fill. Stop entering this many
    # seconds before the window ends (900s window, so 30s = the last 3.3%).
    MIN_SECONDS_LEFT: float = 30.0

    # After a window expires, wait this long for Polymarket to publish its OFFICIAL
    # outcome before falling back to our own close-vs-open comparison. Without this the
    # local fallback always won the race and the authoritative result was never used.
    AUTHORITATIVE_SETTLE_WAIT_S: float = 90.0

    # Close-and-flip the open position on a strong opposite signal
    FLIP_ENABLED: bool = False
    FLIP_MIN_CONVICTION: float = 0.80   # opposite side's adjusted prob must be >= this
    FLIP_MIN_MINUTES_LEFT: float = 9.0  # and at least this much time left in the window

    RSI_PERIOD: int = 14
    RSI_OVERBOUGHT: float = 80.0
    RSI_OVERSOLD: float = 20.0
    EMA_15M_PERIOD: int = 20

    # Polymarket
    POLYMARKET_SLUG: str = os.getenv("POLYMARKET_SLUG", "")
    POLYMARKET_SERIES_ID: str = os.getenv("POLYMARKET_SERIES_ID", "10192")
    POLYMARKET_SERIES_SLUG: str = os.getenv("POLYMARKET_SERIES_SLUG", "btc-up-or-down-15m")
    POLYMARKET_AUTO_SELECT_LATEST: bool = os.getenv("POLYMARKET_AUTO_SELECT_LATEST", "true").lower() == "true"
    POLYMARKET_LIVE_DATA_WS_URL: str = os.getenv("POLYMARKET_LIVE_WS_URL", "wss://ws-live-data.polymarket.com")
    # CLOB market websocket — live order books for the active tokens, replacing the
    # per-tick REST /book + /price poll. Set MAX_BOOK_AGE_S to 0 to disable the WS book
    # entirely and go back to pure REST.
    POLYMARKET_CLOB_WS_URL: str = os.getenv("POLYMARKET_CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
    MAX_BOOK_AGE_S: float = 15.0   # older than this => distrust the socket, use REST
    POLYMARKET_UP_LABEL: str = os.getenv("POLYMARKET_UP_LABEL", "Up")
    POLYMARKET_DOWN_LABEL: str = os.getenv("POLYMARKET_DOWN_LABEL", "Down")

    # Chainlink
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon.drpc.org")
    POLYGON_RPC_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_RPC_URLS", "").split(",") if url.strip()]
    POLYGON_WSS_URL: str = os.getenv("POLYGON_WSS_URL", "wss://polygon-bor-rpc.publicnode.com")
    POLYGON_WSS_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_WSS_URLS", "").split(",") if url.strip()]
    CHAINLINK_BTC_USD_AGGREGATOR: str = os.getenv("CHAINLINK_BTC_USD_AGGREGATOR", "0xc907E116054Ad103354f2D350FD2514433D57F6f")

    CHAINLINK_AGGREGATORS: Dict[str, str] = {
        "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "SOL": "0x39771505D18301D239916F4C88367A6010F7D2e3",
        "XRP": "0x3454796324D6469C3110996E2E10972688045F19",
        "DOGE": "0xbAf93Ba318f77363f82E8896a2E830206121D506",
        "BNB": "0x82a6C67606bdc0409f959f60608226064223A57c"
    }

    def alchemy_rpc_url(self) -> str:
        """Dedicated Polygon RPC for chain reads (pUSD balance, wallet derivation).
        Empty string => the library's default public RPC."""
        return f"https://polygon-mainnet.g.alchemy.com/v2/{self.ALCHEMY_API_KEY}" if self.ALCHEMY_API_KEY else ""

    def get_aggregator(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith("USDT"): s = s[:-4]
        return self.CHAINLINK_AGGREGATORS.get(s, self.CHAINLINK_BTC_USD_AGGREGATOR)

    # Proxy
    HTTP_PROXY: str = os.getenv("HTTP_PROXY", os.getenv("http_proxy", ""))
    HTTPS_PROXY: str = os.getenv("HTTPS_PROXY", os.getenv("https_proxy", ""))
    ALL_PROXY: str = os.getenv("ALL_PROXY", os.getenv("all_proxy", ""))

def normalize_private_key(secret: str) -> str:
    """Accept either a raw hex private key or a 12/15/18/21/24-word seed phrase and return a
    hex private key. EOA only — the trading wallet is derived from this secret and
    nothing else. Returns "" for empty input. Raises if a seed phrase can't be parsed."""
    secret = (secret or "").strip().strip('"\'')
    if not secret:
        return ""
    words = secret.split()
    if len(words) in (12, 15, 18, 21, 24) or (len(words) >= 12 and not secret.startswith("0x")):
        from eth_account import Account
        Account.enable_unaudited_hdwallet_features()
        clean_mnemonic = " ".join(words)
        key = Account.from_mnemonic(clean_mnemonic).key.hex()
        return key if key.startswith("0x") else "0x" + key

    cleaned_hex = secret if secret.startswith("0x") else "0x" + secret
    return cleaned_hex


def inspect_key_or_mnemonic(secret: str) -> Dict[str, Any]:
    """Inspect and validate a private key or mnemonic without mutating settings."""
    secret = (secret or "").strip().strip('"\'')
    if not secret:
        return {"valid": False, "type": "empty", "message": "No key or seed phrase entered"}
    words = secret.split()
    if len(words) >= 12 and not secret.startswith("0x"):
        if len(words) not in (12, 15, 18, 21, 24):
            return {
                "valid": False,
                "type": "seed_phrase",
                "word_count": len(words),
                "message": f"Seed phrase has {len(words)} words (expected 12 or 24)"
            }
        try:
            from eth_account import Account
            Account.enable_unaudited_hdwallet_features()
            clean_mnemonic = " ".join(words)
            acc = Account.from_mnemonic(clean_mnemonic)
            return {
                "valid": True,
                "type": "seed_phrase",
                "word_count": len(words),
                "eoa": acc.address,
                "message": f"Valid {len(words)}-word seed phrase"
            }
        except Exception as e:
            return {
                "valid": False,
                "type": "seed_phrase",
                "word_count": len(words),
                "message": f"Invalid seed phrase: {e}"
            }
    else:
        try:
            from eth_account import Account
            raw = secret if secret.startswith("0x") else "0x" + secret
            acc = Account.from_key(raw)
            return {
                "valid": True,
                "type": "private_key",
                "eoa": acc.address,
                "message": "Valid private key"
            }
        except Exception as e:
            return {
                "valid": False,
                "type": "private_key",
                "message": f"Invalid private key: {e}"
            }



def load_settings():
    base_settings = Settings()
    config_path = "config.json"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)

            if "mode" in config_data: base_settings.MODE = config_data["mode"]
            if "paper_balance_usd" in config_data: base_settings.PAPER_BALANCE_USD = config_data["paper_balance_usd"]
            if "private_key" in config_data:
                base_settings.PRIVATE_KEY = normalize_private_key(config_data["private_key"])

            if "relayer" in config_data:
                rl = config_data["relayer"]
                if "api_key" in rl: base_settings.RELAYER_API_KEY = rl["api_key"]

            if "live" in config_data:
                live = config_data["live"]
                if "max_slippage" in live: base_settings.CLOB_MAX_SLIPPAGE = float(live["max_slippage"])
                if "entry_max_retries" in live: base_settings.ENTRY_MAX_RETRIES = int(live["entry_max_retries"])
                if "entry_retry_delay_ms" in live: base_settings.ENTRY_RETRY_DELAY_MS = int(live["entry_retry_delay_ms"])
                if "exit_max_retries" in live: base_settings.EXIT_MAX_RETRIES = int(live["exit_max_retries"])

            if "capital_extractor" in config_data:
                ce = config_data["capital_extractor"]
                if "enabled" in ce: base_settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
                if "trigger_balance" in ce: base_settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
                if "withdraw_amount" in ce: base_settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
                if "withdraw_address" in ce: base_settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
                if "auto_resume_after_withdrawal" in ce: base_settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
                if "resume_after" in ce: base_settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]

            if "telegram" in config_data:
                tg = config_data["telegram"]
                if "enabled" in tg: base_settings.TELEGRAM_ENABLED = bool(tg["enabled"])
                if "bot_token" in tg: base_settings.TELEGRAM_BOT_TOKEN = tg["bot_token"]

            if "polymarket" in config_data:
                poly = config_data["polymarket"]
                if "gamma_base_url" in poly: base_settings.GAMMA_BASE_URL = poly["gamma_base_url"]
                if "clob_base_url" in poly: base_settings.CLOB_BASE_URL = poly["clob_base_url"]
                if "live_ws_url" in poly: base_settings.POLYMARKET_LIVE_DATA_WS_URL = poly["live_ws_url"]
                if "clob_ws_url" in poly: base_settings.POLYMARKET_CLOB_WS_URL = poly["clob_ws_url"]
                if "max_book_age_s" in poly: base_settings.MAX_BOOK_AGE_S = float(poly["max_book_age_s"])
                if "series_id" in poly: base_settings.POLYMARKET_SERIES_ID = poly["series_id"]
                if "series_slug" in poly: base_settings.POLYMARKET_SERIES_SLUG = poly["series_slug"]
                if "auto_select_latest" in poly: base_settings.POLYMARKET_AUTO_SELECT_LATEST = poly["auto_select_latest"]
                if "up_label" in poly: base_settings.POLYMARKET_UP_LABEL = poly["up_label"]
                if "down_label" in poly: base_settings.POLYMARKET_DOWN_LABEL = poly["down_label"]

            if "trading" in config_data:
                trading = config_data["trading"]
                if "symbol" in trading: base_settings.SYMBOL = trading["symbol"]
                if "binance_base_url" in trading: base_settings.BINANCE_BASE_URL = trading["binance_base_url"]
                if "candle_window_minutes" in trading: base_settings.CANDLE_WINDOW_MINUTES = trading["candle_window_minutes"]
                if "poll_interval_ms" in trading: base_settings.POLL_INTERVAL_MS = trading["poll_interval_ms"]
                if "risk_type" in trading: base_settings.RISK_TYPE = trading["risk_type"]
                if "risk_value" in trading: base_settings.RISK_VALUE = trading["risk_value"]

            if "ev" in config_data:
                ev = config_data["ev"]
                if "ev_threshold" in ev: base_settings.EV_THRESHOLD = float(ev["ev_threshold"])
                if "min_prob" in ev: base_settings.MIN_PROB_EV = float(ev["min_prob"])
                if "min_book_liquidity_usd" in ev: base_settings.MIN_BOOK_LIQUIDITY_USD = float(ev["min_book_liquidity_usd"])
                if "min_seconds_left" in ev: base_settings.MIN_SECONDS_LEFT = float(ev["min_seconds_left"])

            if "settlement" in config_data:
                st = config_data["settlement"]
                if "authoritative_settle_wait_s" in st:
                    base_settings.AUTHORITATIVE_SETTLE_WAIT_S = float(st["authoritative_settle_wait_s"])

            if "flip" in config_data:
                flip = config_data["flip"]
                if "enabled" in flip: base_settings.FLIP_ENABLED = bool(flip["enabled"])
                if "min_conviction" in flip: base_settings.FLIP_MIN_CONVICTION = float(flip["min_conviction"])
                if "min_minutes_left" in flip: base_settings.FLIP_MIN_MINUTES_LEFT = float(flip["min_minutes_left"])

            if "chainlink" in config_data:
                cl = config_data["chainlink"]
                if "polygon_rpc_url" in cl: base_settings.POLYGON_RPC_URL = cl["polygon_rpc_url"]
                if "polygon_wss_url" in cl: base_settings.POLYGON_WSS_URL = cl["polygon_wss_url"]
                if "btc_usd_aggregator" in cl: base_settings.CHAINLINK_BTC_USD_AGGREGATOR = cl["btc_usd_aggregator"]
                if "alchemy_api_key" in cl: base_settings.ALCHEMY_API_KEY = cl["alchemy_api_key"]

            if "indicators" in config_data:
                ind = config_data["indicators"]
                if "rsi_period" in ind: base_settings.RSI_PERIOD = int(ind["rsi_period"])
                if "rsi_overbought" in ind: base_settings.RSI_OVERBOUGHT = float(ind["rsi_overbought"])
                if "rsi_oversold" in ind: base_settings.RSI_OVERSOLD = float(ind["rsi_oversold"])
                if "ema_15m_period" in ind: base_settings.EMA_15M_PERIOD = int(ind["ema_15m_period"])

        except Exception as e:
            print(f"Warning: Failed to load config.json: {e}")

    return base_settings

settings = load_settings()
