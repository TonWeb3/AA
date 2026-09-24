import time
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from .config import settings
from .state import state, log_message, save_state
from .clob_trader import clob_trader
from . import data

def _archive(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Strip bulky/internal scratch keys before a trade goes into trade_history.

    `_market` caches a whole Gamma market payload while the trade is open; writing
    that into state_data.json every settle would bloat the file for no benefit.
    """
    for k in ("_market", "_market_closed", "order_response"):
        trade.pop(k, None)
    return trade


def exit_price_for(poly_snapshot: Dict[str, Any], side: str, shares: float) -> Tuple[Optional[float], float, float]:
    """The all-in price selling `shares` would fetch on the bid side, by walking the
    book rather than valuing the whole position at the touch. Returns
    (avg_price, shares_sellable, proceeds)."""
    key = "up" if side == "UP" else "down"
    ob = (poly_snapshot.get("orderbook") or {}).get(key) or {}
    levels = ob.get("bidLevels") or []
    avg, sold, proceeds = data.sweep_sell(levels, shares)
    if avg is None:
        return ob.get("bestBid"), 0.0, 0.0
    return avg, sold, proceeds


async def close_open_position(poly_snapshot: Dict[str, Any], reason: str) -> Optional[Dict[str, Any]]:
    """Sell the open position into the bid side and book the realized P/L. Used by the
    auto-withdrawal to go flat before extracting funds. Returns
    {"side","exit_price","pl"} on success, else None.

    Live sells are RETRY-BOUNDED (`EXIT_MAX_RETRIES`): without a bound, a rejected FOK
    becomes a fresh sell order on every tick, chasing the book down. When the retries
    are spent the position simply settles at expiry as it normally would.
    """
    async with state.lock:
        if not state["active_trades"] or not poly_snapshot.get("ok"):
            return None
        market = poly_snapshot["market"]
        cur_market_id = str(market.get("id"))
        trade = next((t for t in state["active_trades"] if str(t.get("market_id")) == cur_market_id and t.get("status") == "OPEN"), None)
        if not trade:
            return None  # no active open position in this market

        attempts = trade.get("exit_attempts", {}).get(reason, 0)
        if settings.EXIT_MAX_RETRIES > 0 and attempts >= settings.EXIT_MAX_RETRIES:
            return None  # give up on this exit; the position settles at expiry

        token_ids = poly_snapshot.get("token_ids", {})
        held_key = "up" if trade["side"] == "UP" else "down"
        exit_price, sellable, _ = exit_price_for(poly_snapshot, trade["side"], trade["shares"])
        if not exit_price or exit_price <= 0:
            return None

        # A Fill-Or-Kill sell of the whole position is KILLED outright if the bid side
        # cannot absorb it.
        if sellable < trade["shares"] * 0.999:
            return None

        if state["trading_mode"] == "live":
            token_id = token_ids.get(held_key)
            result = await asyncio.to_thread(clob_trader.place_market_sell, token_id,
                                             trade["shares"], exit_price)
            if not result.get("ok"):
                trade.setdefault("exit_attempts", {})[reason] = attempts + 1
                left = max(0, settings.EXIT_MAX_RETRIES - (attempts + 1))
                log_message(f"{reason} sell FAILED ({trade['side']}): {result.get('error')} "
                            f"— {left} attempt(s) left")
                if left == 0:
                    log_message(f"{reason}: giving up on the early exit; holding {trade['side']} to expiry")
                save_state()
                return None
            # Book the REAL fill, not the quote we aimed at.
            if result.get("fill_price") and result.get("fill_size"):
                exit_price = float(result["fill_price"])
                proceeds = result.get("fill_usd") or (result["fill_size"] * exit_price)
            else:
                proceeds = trade["shares"] * exit_price
            trade["exit_order_id"] = result.get("order_id")
            state["last_balance_refresh"] = 0   # re-read the on-chain balance next tick
        else:
            proceeds = trade["shares"] * exit_price
            state["paper_balance"] += proceeds

        pl = proceeds - trade["amount"]
        side = trade["side"]
        trade["status"] = "CLOSED"
        trade["exit_time"] = datetime.now().isoformat()
        trade["exit_reason"] = reason
        trade["resolution"] = "early_exit"
        trade["settlement_price_at_expiry"] = exit_price
        trade["profit_loss"] = pl
        trade["exit_proceeds"] = proceeds
        trade["open_price"] = trade.get("strike_price")
        trade["close_price"] = state.get("last_seen_price")
        state["trade_history"].append(_archive(trade))
        state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
        state["last_trade_side"] = None
        save_state()
        return {"side": side, "exit_price": exit_price, "pl": pl}


async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    """Close the open position early and flip when a STRONG opposite signal appears.

    Opt-in (FLIP_ENABLED). Guards: the new side must clear FLIP_MIN_CONVICTION and at
    least FLIP_MIN_MINUTES_LEFT must remain, and we only flip within the same market.
    After closing here, execute_trade() opens the new side (slot is now free).
    """
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    cur_market_id = str(market.get("id"))
    async with state.lock:
        trade = next((t for t in state["active_trades"] if str(t.get("market_id")) == cur_market_id and t.get("status") == "OPEN"), None)
        if not trade:
            return
        if trade["side"] == new_side:
            return  # already on the signalled side

        held_key = "up" if trade["side"] == "UP" else "down"
        ob = orderbook.get(held_key) or {}
        exit_price = ob.get("bestBid") or prices.get(held_key)
        if not exit_price or exit_price <= 0:
            log_message(f"FLIP aborted: no exit price for {trade['side']}")
            return

        if state["trading_mode"] == "live":
            token_id = token_ids.get(held_key)
            result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
            if not result.get("ok"):
                log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')} — position kept")
                return
            if result.get("fill_price"):
                exit_price = float(result["fill_price"])
            trade["exit_order_id"] = result.get("order_id")
        else:
            state["paper_balance"] += trade["shares"] * exit_price

        trade["status"] = "CLOSED"
        trade["exit_time"] = datetime.now().isoformat()
        trade["exit_reason"] = "flip"
        trade["resolution"] = "flip_exit"
        trade["settlement_price_at_expiry"] = exit_price
        trade["open_price"] = trade.get("strike_price")
        trade["close_price"] = state.get("last_seen_price")
        trade["profit_loss"] = (trade["shares"] * exit_price) - trade["amount"]
        state["trade_history"].append(_archive(trade))
        state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
        state["last_trade_side"] = None
        save_state()
        log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side}")
        return new_side


async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any],
                        strike_open: Optional[float], token_ids: Dict[str, Any],
                        orderbook: Optional[Dict[str, Any]] = None,
                        strike_source: str = "chainlink_ws", window_start_ms: Optional[int] = None,
                        open_reason: str = "ev_entry") -> str:
    """Execute entry decision either on paper balance or Polymarket CLOB.
    Maintains single active trade per market window without blocking subsequent windows."""
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    async with state.lock:
        cur_market_id = str(market.get("id"))
        # Non-blocking resolution: only OPEN or PENDING trades in THIS market block entry
        if any(str(t.get("market_id")) == cur_market_id and t.get("status") in ("OPEN", "PENDING") for t in state["active_trades"]):
            return "slot_busy"

        if strike_open is None:
            return "no_strike"

        side = decision["side"]
        price = market_prices["up"] if side == "UP" else market_prices["down"]
        if price is None:
            return "no_price"

        balance = state["paper_balance"]
        risk_type = (settings.RISK_TYPE or "percent").lower()
        if risk_type == "fixed":
            amount_to_risk = float(settings.RISK_VALUE)
        else:
            amount_to_risk = (float(settings.RISK_VALUE) / 100.0) * balance

        if amount_to_risk <= 0:
            return "stake_zero"

        ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
        ask_levels = ob.get("askLevels") or []
        fill_limit = min(0.99, price + settings.CLOB_MAX_SLIPPAGE)
        ask_liq_usd = 0.90 * sum(p * s for p, s in ask_levels if p <= fill_limit)
        if not ask_levels or ask_liq_usd < settings.MIN_BOOK_LIQUIDITY_USD:
            log_message(f"Skip {side}: thin book (${ask_liq_usd:.2f} reachable at {fill_limit:.3f})")
            return "thin_book"
        amount_to_risk = min(amount_to_risk, ask_liq_usd)

        if balance < amount_to_risk or amount_to_risk <= 0:
            print(f"Insufficient paper balance ({balance}) or invalid risk amount ({amount_to_risk})")
            return "insufficient_balance"

        end_date_str = market.get("endDate")
        end_ts = 0
        if end_date_str:
            try:
                end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
            except Exception:
                pass
        if not end_ts:
            end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

        trade = {
            "market_id": market["id"],
            "market_slug": market.get("slug"),
            "side": side,
            "entry_price": price,
            "amount": amount_to_risk,
            "shares": amount_to_risk / price,
            "entry_time": datetime.now().isoformat(),
            "status": "OPEN",
            "settlement_price": None,
            "profit_loss": None,
            "strike_price": strike_open,
            "strike_source": strike_source,
            "window_start_ms": int(window_start_ms) if window_start_ms is not None else None,
            "open_reason": open_reason,
            "close_price": None,
            "end_ts": end_ts,
            "mode": state["trading_mode"]
        }

        if state["trading_mode"] == "paper":
            state["paper_balance"] -= amount_to_risk
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            save_state()
            log_message(f"Executed PAPER trade: {side} @ {price} for {market.get('slug')} (Amount: ${amount_to_risk:.2f})")
            return "entered"
        else:
            token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
            if not token_id:
                log_message(f"LIVE trade aborted: missing token_id for side {side}")
                return "missing_token_id"

            max_retries = max(1, int(getattr(settings, "ENTRY_MAX_RETRIES", 6)))
            retry_delay_ms = max(50, int(getattr(settings, "ENTRY_RETRY_DELAY_MS", 250)))
            current_price = float(price)
            current_amount = float(amount_to_risk)
            last_error = None
            pre_fill = None

            # Capture latest fill before attempts to guard against ghost fills
            try:
                pre_fill = await asyncio.to_thread(clob_trader.get_last_fill, token_id)
            except Exception:
                pre_fill = None
            pre_trade_id = (pre_fill or {}).get("trade_id")

            for attempt in range(1, max_retries + 1):
                # Guard 1: check if user stopped the bot while retrying
                if not state.get("running"):
                    log_message(f"LIVE trade aborted on attempt {attempt}: bot stopped")
                    return "stopped"

                # Guard 2: check if slot was filled
                if any(str(t.get("market_id")) == cur_market_id and t.get("status") in ("OPEN", "PENDING") for t in state["active_trades"]):
                    log_message(f"LIVE trade aborted on attempt {attempt}: market slot already active")
                    return "slot_busy"

                # Guard 3: check remaining window seconds
                time_left_s = end_ts - time.time()
                if time_left_s < settings.MIN_SECONDS_LEFT:
                    log_message(f"LIVE trade aborted on attempt {attempt}: window near expiry ({time_left_s:.1f}s left < {settings.MIN_SECONDS_LEFT}s)")
                    return "near_expiry"

                # Dynamic quote & book refresh for retries (attempt > 1)
                if attempt > 1:
                    refreshed_ask = None
                    refreshed_liq = None

                    # Check in-memory CLOB websocket stream first (zero latency)
                    clob_ws = state.get("clob_ws")
                    if clob_ws:
                        try:
                            summary = clob_ws.get_summary(str(token_id))
                            if summary:
                                refreshed_ask = summary.get("bestAsk")
                                ask_levels = summary.get("askLevels") or []
                                if ask_levels and refreshed_ask:
                                    fill_lim = min(0.99, float(refreshed_ask) + settings.CLOB_MAX_SLIPPAGE)
                                    refreshed_liq = 0.90 * sum(p * s for p, s in ask_levels if p <= fill_lim)
                        except Exception:
                            pass

                    # Fallback to REST price if WS was unavailable
                    if refreshed_ask is None:
                        try:
                            refreshed_ask = await data.fetch_clob_price(token_id, "sell")
                        except Exception:
                            pass

                    if refreshed_ask is not None and 0 < float(refreshed_ask) < 0.99:
                        old_p = current_price
                        current_price = float(refreshed_ask)
                        if refreshed_liq and refreshed_liq >= settings.MIN_BOOK_LIQUIDITY_USD:
                            current_amount = min(amount_to_risk, refreshed_liq)
                        log_message(
                            f"Refreshed quote for attempt {attempt}/{max_retries} ({side}): "
                            f"{old_p:.4f} -> {current_price:.4f}"
                            + (f" (depth ${refreshed_liq:.2f})" if refreshed_liq else "")
                        )

                # Submit the market buy order
                result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, current_amount, current_price)
                if result.get("ok"):
                    trade["order_id"] = result.get("order_id")
                    trade["order_response"] = result.get("response") or {}
                    trade["token_id"] = token_id
                    fill_size = result.get("fill_size")
                    fill_price = result.get("fill_price")
                    fill_usd = result.get("fill_usd")
                    if fill_size and fill_price:
                        trade["shares"] = float(fill_size)
                        trade["entry_price"] = float(fill_price)
                        trade["amount"] = float(fill_usd if fill_usd else fill_size * fill_price)
                        trade["quoted_price"] = current_price
                        trade["slippage"] = float(fill_price) - float(current_price) if current_price else None
                    state["active_trades"].append(trade)
                    state["last_trade_side"] = side
                    save_state()
                    attempt_str = f" (attempt {attempt}/{max_retries})" if attempt > 1 else ""
                    log_message(
                        f"Executed LIVE trade{attempt_str}: {side} ${trade['amount']:.2f} on {market.get('slug')} "
                        f"— {trade['shares']:.2f} shares @ {trade['entry_price']:.4f} "
                        f"(quote {current_price}, order {trade['order_id']})"
                    )
                    return "entered"

                last_error = result.get("error", "unknown_error")

                # Check for fatal unrecoverable errors (fail fast without burning retries)
                err_lower = str(last_error).lower()
                if any(fatal in err_lower for fatal in ("missing_private_key", "client_not_ready", "insufficient_balance", "missing_token_id")):
                    log_message(f"LIVE trade aborted ({side}): fatal error '{last_error}' — will not retry")
                    return "live_order_failed"

                # Ghost fill check: Did the order actually fill despite error/delayed response?
                try:
                    post_fill = await asyncio.to_thread(clob_trader.get_last_fill, token_id)
                    post_trade_id = (post_fill or {}).get("trade_id")
                    post_side = str((post_fill or {}).get("side", "")).upper()
                    if post_fill and post_trade_id and post_trade_id != pre_trade_id and post_side == "BUY":
                        fill_sz = post_fill.get("size")
                        fill_pr = post_fill.get("price")
                        if fill_sz and fill_pr and fill_sz > 0:
                            trade["order_id"] = post_trade_id
                            trade["token_id"] = token_id
                            trade["shares"] = float(fill_sz)
                            trade["entry_price"] = float(fill_pr)
                            trade["amount"] = float(fill_sz * fill_pr)
                            trade["quoted_price"] = current_price
                            trade["slippage"] = float(fill_pr) - float(current_price)
                            state["active_trades"].append(trade)
                            state["last_trade_side"] = side
                            save_state()
                            log_message(
                                f"Executed LIVE trade via ghost-fill confirmation on attempt {attempt}: "
                                f"{side} ${trade['amount']:.2f} — {trade['shares']:.2f} shares @ {trade['entry_price']:.4f} "
                                f"(trade_id {post_trade_id})"
                            )
                            return "entered"
                except Exception:
                    pass

                if attempt < max_retries:
                    # Rate limit backoff: Polymarket CLOB limit is 10 req/s.
                    # Standard retry delay is 250ms (~4 req/s max).
                    # If 429/rate-limit hit, back off 1.0 second.
                    if "429" in err_lower or "rate" in err_lower or "too many" in err_lower:
                        backoff_s = 1.0
                        log_message(f"Rate limit detected on attempt {attempt} ({side}) — backing off {backoff_s:.1f}s")
                    else:
                        backoff_s = (retry_delay_ms / 1000.0) * (1.0 + 0.1 * (attempt - 1))

                    log_message(
                        f"LIVE trade attempt {attempt}/{max_retries} FAILED ({side}): {last_error} "
                        f"— retrying in {backoff_s*1000:.0f}ms..."
                    )
                    await asyncio.sleep(backoff_s)

            log_message(f"LIVE trade FAILED ({side}) after {max_retries} attempts: {last_error}")
            return "live_order_failed"


async def _redeem_win(trade: Dict[str, Any], market: Optional[Dict[str, Any]],
                      up_index: int, down_index: int, winning_index: int):
    """Redeem a winning LIVE position into pUSD."""
    condition_id = (market or {}).get("conditionId") or (market or {}).get("condition_id")
    if not condition_id:
        trade["redeem"] = {"ok": False, "error": "missing_condition_id"}
        log_message(f"REDEEM skipped for {trade['market_slug']}: no conditionId on the market")
        return

    amounts = [0.0, 0.0]
    idx = up_index if winning_index == up_index else down_index
    if 0 <= idx < len(amounts):
        amounts[idx] = float(trade.get("shares") or 0.0)

    neg_risk = bool((market or {}).get("negRisk") or (market or {}).get("neg_risk") or False)
    try:
        res = await asyncio.to_thread(clob_trader.redeem, condition_id, amounts, neg_risk)
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    trade["redeem"] = res
    if res.get("ok"):
        log_message(f"REDEEM ok for {trade['market_slug']}: {amounts[idx]:.2f} shares (tx {res.get('tx')})")
    else:
        log_message(f"REDEEM FAILED for {trade['market_slug']}: {res.get('error')} — redeem manually on Polymarket")
