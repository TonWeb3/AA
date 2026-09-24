import time
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from .config import settings
from .state import state, log_message, save_state
from .chainlink import chainlink_fetcher
from .execution import _archive, _redeem_win
from . import data

POLY_WS_MAX_AGE_MS = 10_000
ONCHAIN_MAX_AGE_MS = 15 * 60_000

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

def closed_candles(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop the still-developing candle from a kline buffer."""
    if not candles:
        return []
    now_ms = time.time() * 1000
    out = []
    for c in candles:
        if c.get("isClosed") is True:
            out.append(c)
        elif c.get("isClosed") is None and c.get("closeTime", 0) < now_ms:
            out.append(c)
    return out

async def mark_window_open_with_recovery(start_ms: int, window_ms: int,
                                        current_price: Optional[float],
                                        spot_price: Optional[float],
                                        price_source: Optional[str],
                                        price_is_fresh: bool = True,
                                        klines_5m: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """Latch window OPEN at eventStartTime using the EXACT on-chain Chainlink round.

    Always uses the historical Chainlink aggregator round whose updatedAt is the latest
    tick at or before eventStartTime — this is exactly the same value Polymarket's oracle
    resolver reads, so the strike matches 1:1 with what the market settles against.

    The WebSocket live price is NEVER used for the strike: it carries timing jitter
    (socket lag, OS scheduling) that can land 1-30s away from the real on-chain tick,
    causing strike mismatches on near-the-money trades.

    The on-chain call is cached per window so it only fires once per 15m window (~12 RPC
    calls via binary search), not once per tick. Subsequent ticks find `chainlink` already
    populated and return immediately.

    `binance` (the model's reference open) is still sourced from the 5m kline that opens
    exactly at start_ms, so the fair-prob model keeps measuring Binance move since the open
    correctly without any Chainlink/Binance cross-feed offset.
    """
    opens = state["market_opens"]
    prev_ws = state.get("last_window_start")

    # Freeze the prior window's close price when the window rolls
    if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
        if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
            opens[prev_ws]["close"] = state["last_seen_price"]
    if current_price:
        state["last_seen_price"] = current_price

    if start_ms not in opens:
        opens[start_ms] = {
            "chainlink": None,   # Settlement strike — always on-chain
            "binance": None,     # Model reference open — 5m kline
            "close": None,
            "genuine": True,     # Always genuine: we always recover the on-chain value
            "strike_source": None,
            "strike_round_id": None,
            "_rpc_attempted": False,  # Ensure we only try the RPC once per window
        }
        for k in list(opens.keys()):
            if k < start_ms - 4 * window_ms:
                del opens[k]

    win = opens[start_ms]
    since_start = time.time() * 1000 - start_ms

    # ── Always use on-chain Chainlink historical round as the settlement strike ──────────
    # Only attempt once per window (cached). The eventStartTime must be in the past for the
    # round to exist on-chain (since_start >= 0).
    if win["chainlink"] is None and not win.get("_rpc_attempted") and since_start >= 0:
        win["_rpc_attempted"] = True
        target_ts = int(start_ms / 1000)
        try:
            hist = await chainlink_fetcher.fetch_round_at_timestamp(target_ts, symbol=settings.SYMBOL)
            if hist and hist.get("price"):
                win["chainlink"] = hist["price"]
                win["strike_round_id"] = hist.get("roundId")
                win["strike_source"] = "chainlink_on_chain"

                # Model reference open: prefer 5m kline that opens exactly at start_ms
                if klines_5m:
                    for c in reversed(klines_5m):
                        if c.get("openTime") == start_ms:
                            win["binance"] = c.get("open")
                            break
                        if c.get("openTime") < start_ms:
                            break
                if win.get("binance") is None:
                    win["binance"] = spot_price  # fallback: Binance spot at this moment

                rpc_diff = target_ts - int(hist["updatedAt"] / 1000)
                log_message(
                    f"✅ Strike latched via on-chain Chainlink round {hist.get('roundId')}: "
                    f"${hist['price']:.2f} at eventStartTime (round is {rpc_diff}s before open) "
                    f"/ Binance ref {win['binance']}"
                )
            else:
                log_message(
                    f"⚠️ On-chain Chainlink round lookup returned no price for "
                    f"eventStartTime {target_ts}. No strike this window."
                )
        except Exception as e:
            log_message(f"⚠️ On-chain strike fetch error: {e}. Will retry next tick.")
            # Allow retry: reset the flag so the next tick tries again
            win["_rpc_attempted"] = False

    state["last_window_start"] = start_ms
    return win


async def update_trades(current_prices: Dict[str, Any]):
    """Settle expired trades against official Polymarket outcomes or frozen close vs strike.
    Non-blocking: trades waiting for settlement never prevent opening new positions in new windows."""
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    SETTLEMENT_GRACE_SECONDS = 300

    async with state.lock:
        for trade in state["active_trades"]:
            if cur_price:
                trade["last_price"] = cur_price

            end_ts = trade.get("end_ts", 0)
            if not end_ts:
                try:
                    end_ts = datetime.fromisoformat(trade["entry_time"]).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
                except Exception:
                    end_ts = now_ts
            expired = now_ts >= end_ts
            if expired and trade.get("status") == "OPEN":
                trade["status"] = "RESOLVING"

            if expired and trade.get("close_price") is None:
                frozen_close = cur_price or trade.get("last_price")
                if frozen_close:
                    trade["close_price"] = frozen_close

            market = trade.get("_market")
            poll_every = 3.0 if expired else 30.0
            if trade.get("last_api_check", 0) < now_ts - poll_every:
                try:
                    fetched = await data.fetch_market_by_slug(trade["market_slug"])
                except Exception:
                    fetched = None
                trade["last_api_check"] = now_ts
                if fetched is not None:
                    market = fetched
                    trade["_market"] = fetched
                    trade["_market_closed"] = bool(fetched.get("closed"))
            market_closed = trade.get("_market_closed", False)

            if not expired and not market_closed:
                remaining_active.append(trade)
                continue

            # Determine the winning outcome
            outcomes = []
            outcome_prices = []
            if market:
                outcomes = market.get("outcomes", [])
                if isinstance(outcomes, str):
                    outcomes = json.loads(outcomes)
                outcome_prices = market.get("outcomePrices", [])
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)
            if not outcomes:
                outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

            up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
            down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

            winning_index = -1
            resolution = None

            # 1) Authoritative resolution
            resolved = market_closed or str((market or {}).get("umaResolutionStatus", "")).lower() == "resolved"
            for i, p in enumerate(outcome_prices if resolved else []):
                try:
                    if float(p) >= 0.99:
                        winning_index = i
                        resolution = "polymarket_settled"
                        break
                except Exception:
                    pass

            # 2) Fallback: frozen CLOSE vs STRIKE (open)
            strike = trade.get("strike_price")
            settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                                or trade.get("last_price") or cur_price)
            if winning_index == -1 and (expired or market_closed):
                if trade.get("expired_at") is None:
                    trade["expired_at"] = now_ts
                waited = now_ts - trade["expired_at"]
                if waited < settings.AUTHORITATIVE_SETTLE_WAIT_S:
                    remaining_active.append(trade)
                    continue
                if strike and settlement_price:
                    trade["settlement_price_at_expiry"] = settlement_price
                    winning_index = up_index if settlement_price > strike else down_index
                    resolution = "close_vs_open"
                    trade["settle_wait_s"] = round(waited, 1)

            if winning_index == -1:
                first_seen = trade.get("unresolved_since")
                if first_seen is None:
                    trade["unresolved_since"] = now_ts
                    remaining_active.append(trade)
                    continue
                if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                    remaining_active.append(trade)
                    continue
                trade["status"] = "VOID"
                trade["exit_reason"] = "void"
                trade["exit_time"] = datetime.now().isoformat()
                trade["profit_loss"] = 0.0
                if trade.get("mode", "paper") == "paper":
                    state["paper_balance"] += trade["amount"]
                state["trade_history"].append(_archive(trade))
                trades_changed = True
                log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
                continue

            won = ((trade["side"] == "UP" and winning_index == up_index) or
                   (trade["side"] == "DOWN" and winning_index == down_index))

            open_px = strike
            close_px = trade.get("close_price") or settlement_price
            trade["open_price"] = open_px
            trade["close_price"] = close_px
            trade["resolution"] = resolution or "unknown"
            if open_px and close_px:
                move_side = "UP" if close_px > open_px else "DOWN"
                dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
            else:
                dir_txt = f"open {open_px} -> close {close_px}"

            if won:
                payout = trade["shares"] * 1.0
                if trade.get("mode", "paper") == "paper":
                    state["paper_balance"] += payout
                trade["profit_loss"] = payout - trade["amount"]
                log_message(f"WIN: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Profit: ${trade['profit_loss']:.2f}")
                if trade.get("mode") == "live":
                    await _redeem_win(trade, market, up_index, down_index, winning_index)
            else:
                trade["profit_loss"] = -trade["amount"]
                log_message(f"LOSS: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Loss: ${trade['profit_loss']:.2f}")

            trade["status"] = "CLOSED"
            trade["exit_reason"] = trade.get("exit_reason") or "settled"
            trade["exit_time"] = datetime.now().isoformat()
            trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
            trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
            state["trade_history"].append(_archive(trade))
            trades_changed = True

        state["active_trades"] = remaining_active
        if trades_changed:
            save_state()
