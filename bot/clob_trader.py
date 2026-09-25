"""
Live trading on Polymarket CLOB V2 via `polymarket-apis`.

Polymarket migrated to CLOB V2. New wallets trade through the gasless
**deposit-wallet** flow (signature_type=3 / POLY_1271):

  - Funds live in a **deposit wallet** derived from your EOA (deposit via Polymarket).
  - Your **private key / seed (EOA)** signs every order — it holds no funds or gas.
  - A **relayer API key** sponsors on-chain setup (wallet deploy + token approvals),
    so you never pay gas.

Legacy accounts (Polymarket proxy / Gnosis safe) are auto-detected and used as-is:
we pick whichever wallet actually holds pUSD. The EOA is derived from PRIVATE_KEY
(hex key or 12/24-word seed phrase), so there is no signature-type/funder to choose
by hand any more.

All clients are synchronous — call from the event loop via `asyncio.to_thread`.
"""

import threading
from typing import Optional, Dict, Any, List, Tuple
from .config import settings


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _patch_clob_models():
    """Relax over-strict fields in polymarket-apis' models. The live CLOB API omits
    some fields the library marks required (e.g. blockaid_check_enabled), which would
    otherwise crash order placement with a pydantic ValidationError."""
    try:
        import polymarket_apis.types.clob_types as ct
        changed = False
        for name in ("blockaid_check_enabled",):
            f = ct.ClobMarketInfo.model_fields.get(name)
            if f is not None and f.is_required():
                f.default = False
                changed = True
        if changed:
            ct.ClobMarketInfo.model_rebuild(force=True)
    except Exception:
        pass


def _patch_order_builder():
    """Polymarket CLOB V2 strictly enforces:
    - Market buy maker amount: max 2 decimals (collateral/USDC spent)
    - Market buy taker amount: max 4 decimals (tokens/shares received)
    - Market sell maker amount: max 4 decimals (tokens/shares sold)
    - Market sell taker amount: max 2 decimals (collateral/USDC received)
    The polymarket-apis library's default ROUNDING_CONFIG specifies amount=5 for 0.001
    and amount=6 for 0.0001 tick sizes, which produces taker amounts with > 4 decimals.
    Polymarket rejects these with:
      HTTP 400 - invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals.
    This patch clamps ROUNDING_CONFIG to a maximum of 4 decimals and ensures exact integer divisibility:
      BUY:  maker amount snapped to 10,000 (10^(6-2) -> max 2 decimals USDC)
            taker amount snapped to 100 (10^(6-4) -> max 4 decimals shares)
      SELL: maker amount snapped to 100 (10^(6-4) -> max 4 decimals shares)
            taker amount snapped to 10,000 (10^(6-2) -> max 2 decimals USDC)
    """
    try:
        from polymarket_apis.utilities.order_builder.builder import OrderBuilder, ROUNDING_CONFIG, RoundConfig
        from polymarket_apis.utilities.order_builder.helpers import to_token_decimals, round_down

        for k, rc in list(ROUNDING_CONFIG.items()):
            if rc.amount > 4:
                ROUNDING_CONFIG[k] = RoundConfig(price=rc.price, size=rc.size, amount=min(4, rc.amount))

        orig_get_market = OrderBuilder.get_market_order_amounts

        def safe_get_market_order_amounts(self, side: str, amount: float, price: float, round_config):
            s = str(side).upper()
            raw_price = round_down(price, round_config.price)

            if s in ("BUY", "0"):
                # Market BUY: Maker is USDC (max 2 decimals), Taker is shares (max 4 decimals)
                res_side, maker, taker = orig_get_market(self, "BUY", amount, price, round_config)
                maker = (maker // 10_000) * 10_000
                taker = (taker // 100) * 100
                return res_side, maker, taker

            elif s in ("SELL", "1"):
                # Market SELL: Maker is shares (up to 4 decimals), Taker is USDC (max 2 decimals).
                # orig_get_market would truncate amount to round_config.size (2 decimals).
                # We calculate directly with up to 4 decimals to avoid leaving dust behind.
                raw_maker_amt = round_down(amount, 4)
                raw_taker_amt = round_down(raw_maker_amt * raw_price, 2)
                maker_amount = to_token_decimals(raw_maker_amt)
                taker_amount = to_token_decimals(raw_taker_amt)
                maker = (maker_amount // 100) * 100
                taker = (taker_amount // 10_000) * 10_000
                return 1, maker, taker

            msg = f"order_args.side must be 'BUY' or 'SELL'"
            raise ValueError(msg)

        OrderBuilder.get_market_order_amounts = safe_get_market_order_amounts
    except Exception as e:
        print(f"Warning: Failed to patch OrderBuilder: {e}")


_patch_clob_models()
_patch_order_builder()



class ClobTrader:
    _FILLED_STATUSES = ("matched", "delayed")
    _PARTIAL_FILL_HINTS = (
        "fully filled", "could not be filled", "fill or kill", "killed",
        "unfilled", "insufficient liquidity", "not filled"
    )

    def __init__(self):
        self.clob = None          # PolymarketClobClient — order placement
        self.gasless = None       # PolymarketGaslessWeb3Client — relayer on-chain ops
        self.funder: Optional[str] = None         # funded wallet (deposit/proxy/safe)
        self.signature_type: Optional[int] = None
        self.ready = False
        self.last_error: Optional[str] = None
        self._approvals_done = False
        self._api_creds = None
        self._lock = threading.Lock()

    def reset(self):
        """Drop cached clients so the next call re-initialises with fresh
        credentials (call after the key / relayer settings change)."""
        with self._lock:
            self.clob = None
            self.gasless = None
            self.funder = None
            self.signature_type = None
            self.ready = False
            self.last_error = None
            self._approvals_done = False
            self._api_creds = None

    # ── wallet derivation / detection ───────────────────────────────────────────
    def _derive_creds(self):
        """CLOB API creds derived from the key (L1 auth). Cached. Doubles as the
        gasless client's builder_creds when no relayer key is set."""
        if self._api_creds is not None:
            return self._api_creds
        from polymarket_apis.clients.clob_client import PolymarketClobClient
        from eth_account import Account
        eoa = Account.from_key(settings.PRIVATE_KEY).address
        c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa,
                                 chain_id=137, signature_type=3)
        self._api_creds = c.create_or_derive_api_creds()
        return self._api_creds

    def _new_gasless(self, signature_type: int):
        # Gasless on-chain client. Two distinct roles:
        #   - relayer key (or builder creds): SUBMITS txs and pays the gas
        #   - rpc_url: READS chain state (pUSD balance, wallet derivation). Use Alchemy
        #     when configured, else the library default.
        from polymarket_apis.clients.web3_client import PolymarketGaslessWeb3Client
        kwargs = {"private_key": settings.PRIVATE_KEY, "signature_type": signature_type}
        if settings.RELAYER_API_KEY:
            kwargs["relayer_api_key"] = settings.RELAYER_API_KEY
        else:
            kwargs["builder_creds"] = self._derive_creds()
        if settings.alchemy_rpc_url():
            kwargs["rpc_url"] = settings.alchemy_rpc_url()
        return PolymarketGaslessWeb3Client(**kwargs)

    def _candidate_wallets(self, gasless) -> List[Tuple[int, str]]:
        """(signature_type, address) candidates derived from the EOA, deposit-wallet
        (V2) first, then legacy proxy / safe, and previous active maker address."""
        out: List[Tuple[int, str]] = []

        # 1. Local create2 derivation (deposit wallet V2, proxy, safe) — instantaneous (<10ms)
        for st, getter in (
            (3, gasless.get_expected_deposit_wallet),
            (1, gasless.get_poly_proxy_wallet_address),
            (2, gasless.get_safe_proxy_wallet_address),
        ):
            try:
                addr = getter()
                if not any(a.lower() == addr.lower() for _, a in out):
                    out.append((st, addr))
            except Exception:
                pass

        # 2. Check previous active maker address if creds are already cached
        if self._api_creds:
            try:
                from polymarket_apis.clients.clob_client import PolymarketClobClient
                from eth_account import Account
                eoa = Account.from_key(settings.PRIVATE_KEY).address
                c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa, chain_id=137, signature_type=0)
                c.set_api_creds(self._api_creds)
                trades = c.get_trades()
                if trades:
                    first = trades[0]
                    m_addr = getattr(first, "maker_address", None) or (first.get("maker_address") if isinstance(first, dict) else None)
                    if m_addr and not any(a.lower() == m_addr.lower() for _, a in out):
                        out.append((3, m_addr))
            except Exception:
                pass

        return out

    def _pick_funded_wallet(self, gasless) -> Tuple[int, str]:
        """Trade from where the money actually is: pick the candidate holding pUSD.
        Falls back to the deposit wallet when every balance reads 0."""
        try:
            from polymarket_apis.clients.clob_client import PolymarketClobClient
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
            c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa, chain_id=137, signature_type=3)
            c.set_api_creds(self._derive_creds())
            clob_bal = float(c.get_pusd_balance())
            if clob_bal > 0:
                trades = c.get_trades()
                if trades:
                    first = trades[0]
                    m_addr = getattr(first, "maker_address", None) or (first.get("maker_address") if isinstance(first, dict) else None)
                    if m_addr:
                        return 3, m_addr
        except Exception:
            pass

        best = None  # (sig_type, addr, balance)
        for st, addr in self._candidate_wallets(gasless):
            try:
                bal = float(gasless.get_pusd_balance(address=addr))
            except Exception:
                bal = 0.0
            if bal > 0:
                return st, addr
            if best is None or bal > best[2]:
                best = (st, addr, bal)
        if best:
            return best[0], best[1]
        return 3, gasless.get_expected_deposit_wallet()

    def _init_clients(self):
        from polymarket_apis.clients.clob_client import PolymarketClobClient

        # Any gasless client can derive every candidate address; probe with deposit.
        probe = self._new_gasless(3)
        sig_type, funder = self._pick_funded_wallet(probe)

        gasless = probe if sig_type == 3 else self._new_gasless(sig_type)

        clob = PolymarketClobClient(
            private_key=settings.PRIVATE_KEY,
            address=funder,
            chain_id=137,
            signature_type=sig_type,
        )
        clob.set_api_creds(clob.create_or_derive_api_creds())

        self.gasless = gasless
        self.clob = clob
        self.funder = funder
        self.signature_type = sig_type
        self.ready = True
        self.last_error = None

    def ensure_ready(self) -> bool:
        if self.ready and self.clob is not None:
            return True
        with self._lock:
            if self.ready and self.clob is not None:
                return True
            if not settings.PRIVATE_KEY:
                self.last_error = "missing_private_key"
                return False
            try:
                self._init_clients()
                return True
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.ready = False
                self.clob = None
                return False

    def ensure_setup(self) -> Dict[str, Any]:
        """One-time gasless on-chain setup: deploy the deposit wallet (if needed) and
        set token approvals, sponsored by the relayer key. Required once before the
        first live order on a fresh deposit wallet."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if self._approvals_done:
            return {"ok": True, "skipped": "already_done"}
        if not settings.RELAYER_API_KEY:
            return {"ok": False, "error": "missing_relayer_api_key"}
        try:
            receipts = self.gasless.set_all_approvals()
            self._approvals_done = True
            return {"ok": True, "approvals": len(receipts or [])}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── orders ──────────────────────────────────────────────────────────────────
    def _market_order(self, token_id, amount, side: str, price, order_type=None) -> Dict[str, Any]:
        from polymarket_apis.types.clob_types import MarketOrderArgs, OrderType
        ot = order_type if order_type is not None else OrderType.FAK
        order_amount = round(float(amount), 2) if side == "BUY" else round(float(amount), 4)
        args = MarketOrderArgs(
            token_id=str(token_id),
            amount=order_amount,              # BUY: USDC to spend (2 dec); SELL: shares to sell (up to 4 dec)
            side=side,                        # "BUY" / "SELL"
            price=round(float(price), 4) if price else 0,
            order_type=ot,
        )
        resp = self.clob.create_and_post_market_order(args)
        if resp is None:
            return {"ok": False, "error": "no_response_from_clob", "response": {}}

        data = resp.model_dump() if hasattr(resp, "model_dump") else dict(resp)
        order_id = (data.get("order_id") or data.get("orderID")
                    or data.get("orderId") or data.get("id"))
        status = str(data.get("status", "")).lower()
        err = data.get("error_msg") or data.get("errorMsg")

        # making/taking are the two legs of the match:
        #   BUY  -> making = USDC paid,   taking = shares received
        #   SELL -> making = shares sold, taking = USDC received
        making = _to_float(data.get("making_amount") or data.get("makingAmount"))
        taking = _to_float(data.get("taking_amount") or data.get("takingAmount"))

        if side == "BUY":
            usd, shares = making, taking
        else:
            shares, usd = making, taking

        filled = (data.get("success") is not False
                  and status in self._FILLED_STATUSES
                  and shares is not None and shares > 0
                  and usd is not None and usd > 0)

        if not filled:
            return {"ok": False, "response": data, "order_id": order_id,
                    "status": status,
                    "error": err or f"not_filled(status={status or 'unknown'})"}

        return {
            "ok": True, "response": data, "order_id": order_id, "status": status,
            # REAL fill economics — never the quote we asked for.
            "fill_price": (usd / shares) if shares else None,
            "fill_size": shares,
            "fill_usd": usd,
        }

    def place_market_buy(self, token_id: str, usdc_amount: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Fill-And-Kill (FAK) marketable BUY for `usdc_amount` USDC of `token_id`. `price`
        is the current quote; the limit is quote + slippage buffer (capped < $1).
        Executes strictly as FAK to fill available liquidity immediately without rejection.
        On success returns the ACTUAL fill_price / fill_size / fill_usd."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        # Ensure the deposit wallet is deployed + approved before the first order if relayer key is present.
        if not self._approvals_done and settings.RELAYER_API_KEY:
            try:
                self.ensure_setup()
            except Exception:
                pass
        try:
            from polymarket_apis.types.clob_types import OrderType
            limit = min(0.99, float(price) + settings.CLOB_MAX_SLIPPAGE) if price and price > 0 else 0
            return self._market_order(token_id, usdc_amount, "BUY", limit, order_type=OrderType.FAK)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def place_market_sell(self, token_id: str, size: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Fill-And-Kill (FAK) marketable SELL of `size` shares (used to exit / flip). Limit
        is quote − slippage buffer (floored at 1¢). On success returns the ACTUAL
        fill_price / fill_size / fill_usd."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            from polymarket_apis.types.clob_types import OrderType
            limit = max(0.01, float(price) - settings.CLOB_MAX_SLIPPAGE) if price and price > 0 else 0
            return self._market_order(token_id, size, "SELL", limit, order_type=OrderType.FAK)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def get_last_fill(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Most recent on-chain trade for this token from the CLOB's own record —
        an independent confirmation that a reported fill really happened."""
        if not self.ensure_ready():
            return None
        try:
            trades = self.clob.get_trades(token_id=str(token_id))
        except Exception:
            return None
        if not trades:
            return None
        t = trades[0]
        d = t.model_dump() if hasattr(t, "model_dump") else dict(t)
        return {"trade_id": d.get("trade_id"), "side": d.get("side"),
                "price": _to_float(d.get("price")), "size": _to_float(d.get("size")),
                "status": d.get("status"), "match_time": d.get("match_time")}

    # ── redemption ──────────────────────────────────────────────────────────────
    def enable_auto_redeem(self) -> Dict[str, Any]:
        """Ask Polymarket to auto-redeem resolved positions. Best-effort: if the
        account doesn't support it we fall back to redeeming explicitly."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not hasattr(self.gasless, "auto_redeem_enable"):
            return {"ok": False, "error": "auto_redeem_unsupported"}
        try:
            res = self.gasless.auto_redeem_enable()
            return {"ok": True, "result": str(res)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def redeem(self, condition_id: str, amounts, neg_risk: bool = False) -> Dict[str, Any]:
        """Redeem a resolved position into pUSD.

        `amounts` is the per-outcome share list the CTF expects ([up, down]); the
        losing leg is simply zero.
        """
        if not condition_id:
            return {"ok": False, "error": "missing_condition_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not hasattr(self.gasless, "redeem_position"):
            return {"ok": False, "error": "redeem_unsupported_by_client"}
        try:
            amts = [float(a) for a in (amounts if isinstance(amounts, (list, tuple)) else [amounts])]
            tx = self.gasless.redeem_position(condition_id, amts, neg_risk)
            return {"ok": True, "tx": str(tx)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── capital extractor / withdrawal ─────────────────────────────────────────
    def get_funder(self) -> Optional[str]:
        """Return the active funded wallet address (deposit/proxy/safe)."""
        if self.funder:
            return self.funder
        if not settings.PRIVATE_KEY:
            return None
        try:
            probe = self._new_gasless(3)
            _, funder = self._pick_funded_wallet(probe)
            return funder
        except Exception:
            return None

    def withdraw_pusd(self, recipient: str, amount: float) -> Dict[str, Any]:
        """Transfer pUSD from the funded deposit wallet to `recipient` via gasless relayer."""
        if not recipient:
            return {"ok": False, "error": "missing_recipient_address"}
        try:
            from web3 import Web3
            recipient = Web3.to_checksum_address(recipient)
        except Exception as e:
            return {"ok": False, "error": f"invalid_recipient_address: {e}"}
        if amount <= 0:
            return {"ok": False, "error": "invalid_withdraw_amount"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            if hasattr(self.gasless, "withdraw_pusd"):
                tx = self.gasless.withdraw_pusd(recipient, amount)
            elif hasattr(self.gasless, "transfer_pusd"):
                tx = self.gasless.transfer_pusd(recipient, amount)
            else:
                return {"ok": False, "error": "withdraw_unsupported_by_client"}
            return {"ok": True, "tx": str(tx)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def is_tx_confirmed(self, tx_hash: str) -> bool:
        """Check whether an on-chain transaction has confirmed on Polygon."""
        if not tx_hash:
            return False
        try:
            if self.gasless and hasattr(self.gasless, "web3") and self.gasless.web3:
                receipt = self.gasless.web3.eth.get_transaction_receipt(tx_hash)
                return receipt is not None and receipt.get("status") == 1
            from web3 import Web3
            rpc = settings.alchemy_rpc_url() or settings.POLYGON_RPC_URL
            w3 = Web3(Web3.HTTPProvider(rpc))
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            return receipt is not None and receipt.get("status") == 1
        except Exception:
            return False

    # ── diagnostics / balance ───────────────────────────────────────────────────
    def get_eoa_address(self) -> Optional[str]:
        """The EOA address derived from PRIVATE_KEY — the wallet you control (the
        key/seed owner). It signs orders but holds no funds or gas."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            from eth_account import Account
            return Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            return None

    def test_connection(self) -> Dict[str, Any]:
        """Derive the EOA + its candidate wallets and report pUSD balances — shows
        which wallet holds the funds and which signature type will be used. Read-only
        (no relayer key required)."""
        if not settings.PRIVATE_KEY:
            return {"ok": False, "error": "missing_private_key"}
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
        except Exception as e:
            return {"ok": False, "error": f"invalid_key: {type(e).__name__}: {e}"}
        try:
            probe = self._new_gasless(3)
            wallets = []
            for st, addr in self._candidate_wallets(probe):
                try:
                    bal = float(probe.get_pusd_balance(address=addr))
                except Exception:
                    bal = None
                wallets.append({"signature_type": st, "address": addr, "pusd_balance": bal})
            funded = [w for w in wallets if (w.get("pusd_balance") or 0) > 0]
            if funded:
                sig_type, funder = funded[0]["signature_type"], funded[0]["address"]
            else:
                sig_type = 3
                try:
                    funder = probe.get_expected_deposit_wallet()
                except Exception:
                    funder = eoa

            return {
                "ok": True,
                "eoa": eoa,
                "chosen_signature_type": sig_type,
                "funder": funder,
                "relayer_key_set": bool(settings.RELAYER_API_KEY),
                "wallets": wallets,
            }
        except Exception as e:
            return {"ok": False, "eoa": eoa, "error": f"{type(e).__name__}: {e}"}

    def get_usdc_balance(self) -> Optional[float]:
        """pUSD balance of the funded wallet (dollars), or None. pUSD is Polymarket's
        V2 collateral; this is the deposit wallet's tradeable balance."""
        if not settings.PRIVATE_KEY:
            return None
        # First priority: Query Polymarket CLOB API directly (accurate for CLOB deposits)
        try:
            if self.ensure_ready() and self.clob is not None:
                c_bal = float(self.clob.get_pusd_balance())
                if c_bal is not None and c_bal >= 0:
                    return c_bal
        except Exception:
            pass

        # Fallback to gasless / on-chain query
        try:
            if self.ready and self.gasless is not None and self.funder:
                return float(self.gasless.get_pusd_balance(address=self.funder))
            probe = self._new_gasless(3)
            _, funder = self._pick_funded_wallet(probe)
            return float(probe.get_pusd_balance(address=funder))
        except Exception:
            return None


clob_trader = ClobTrader()
