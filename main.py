import os
import json
import time
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI
import uvicorn

from bot.config import settings
from bot.state import state, log_message, save_state, load_state, load_telegram_subscribers
from bot.telegram_service import telegram_poller
from bot.execution import execute_trade, maybe_flip_position
from bot.capital_extractor import maybe_auto_withdraw
from bot.settlement import (
    update_trades,
    mark_window_open_with_recovery,
    get_candle_window_timing,
    POLY_WS_MAX_AGE_MS,
    ONCHAIN_MAX_AGE_MS
)
from bot.routers import router, broadcast_state, reflect_running_now, set_symbol_change_hook
from bot import data, ws_data, indicators, engines, utils, chainlink
from bot.clob_trader import clob_trader

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# ── Event-driven entry primitives ────────────────────────────────────────────
MIN_EVAL_INTERVAL_S = 0.05
CTX_MAX_AGE_S = 5.0

_market_event = asyncio.Event()
_entry_lock = asyncio.Lock()
_last_eval_ts = 0.0

def _wake_entry(_payload=None):
    _market_event.set()

# Background stream instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
binance_kline_15m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="15m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
polymarket_clob_ws = ws_data.PolymarketClobBookStream(
    ws_url=settings.POLYMARKET_CLOB_WS_URL,
    on_update=_wake_entry
)
state["clob_ws"] = polymarket_clob_ws

async def seed_kline_buffers():
    try:
        k1m, k5m, k15m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200),
            data.fetch_klines(settings.SYMBOL, "15m", 200)
        )
        binance_kline_1m.set_candles(k1m)
        binance_kline_5m.set_candles(k5m)
        binance_kline_15m.set_candles(k15m)
        log_message(f"Seeded Binance kline buffers (1m/5m/15m) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

async def handle_symbol_change(old_symbol: str, new_symbol: str):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m, binance_kline_15m
    binance_stream.close()
    binance_stream = ws_data.BinanceTradeStream(symbol=new_symbol, on_update=_wake_entry)
    asyncio.create_task(binance_stream.start())

    binance_kline_1m.close()
    binance_kline_1m = ws_data.BinanceKlineStream(symbol=new_symbol, interval="1m", limit=240)
    asyncio.create_task(binance_kline_1m.start())

    binance_kline_5m.close()
    binance_kline_5m = ws_data.BinanceKlineStream(symbol=new_symbol, interval="5m", limit=200)
    asyncio.create_task(binance_kline_5m.start())

    binance_kline_15m.close()
    binance_kline_15m = ws_data.BinanceKlineStream(symbol=new_symbol, interval="15m", limit=200)
    asyncio.create_task(binance_kline_15m.start())

    await seed_kline_buffers()

    polymarket_ws_stream.close()
    polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
        ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
        symbol_includes=get_ws_symbol_filter(new_symbol)
    )
    asyncio.create_task(polymarket_ws_stream.start())

    chainlink_ws_stream.close()
    chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(new_symbol))
    asyncio.create_task(chainlink_ws_stream.start())

set_symbol_change_hook(handle_symbol_change)

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = None
    if settings.POLYMARKET_SLUG:
        market = await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    elif settings.POLYMARKET_AUTO_SELECT_LATEST:
        events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
        markets = data.flatten_event_markets(events)
        now = time.time() * 1000
        live_markets = [
            m for m in markets
            if m.get("endDate") and datetime.fromisoformat(m["endDate"].replace('Z', '+00:00')).timestamp() * 1000 > now
        ]
        if live_markets:
            live_markets.sort(key=lambda x: x["endDate"])
            market = live_markets[0]

    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id:
            continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if 0 <= up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if 0 <= down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    polymarket_clob_ws.update_assets([up_token_id, down_token_id])
    max_age = settings.MAX_BOOK_AGE_S
    up_book_summary = polymarket_clob_ws.get_summary(up_token_id, max_age_s=max_age) if max_age else None
    down_book_summary = polymarket_clob_ws.get_summary(down_token_id, max_age_s=max_age) if max_age else None

    ws_up, ws_down = up_book_summary is not None, down_book_summary is not None
    book_source = "ws" if (ws_up and ws_down) else ("mixed" if (ws_up or ws_down) else "rest")

    up_sell = down_sell = None
    if not (ws_up and ws_down):
        try:
            up_sell, down_sell, up_book, down_book = await asyncio.gather(
                data.fetch_clob_price(up_token_id, "sell"),
                data.fetch_clob_price(down_token_id, "sell"),
                data.fetch_order_book(up_token_id) if up_book_summary is None else asyncio.sleep(0, result=None),
                data.fetch_order_book(down_token_id) if down_book_summary is None else asyncio.sleep(0, result=None)
            )
            if up_book_summary is None and up_book is not None:
                up_book_summary = data.summarize_order_book(up_book)
            if down_book_summary is None and down_book is not None:
                down_book_summary = data.summarize_order_book(down_book)
        except Exception:
            up_sell = None
            down_sell = None

    _empty = {"bestBid": None, "bestAsk": None, "spread": None,
              "bidLiquidity": None, "askLiquidity": None, "askLevels": [], "bidLevels": []}
    if up_book_summary is None:
        up_book_summary = dict(_empty)
    if down_book_summary is None:
        down_book_summary = dict(_empty)

    up_ask = up_book_summary.get("bestAsk") or up_sell or gamma_yes
    down_ask = down_book_summary.get("bestAsk") or down_sell or gamma_no

    return {
        "ok": True,
        "market": market,
        "prices": {"up": up_ask, "down": down_ask},
        "token_ids": {"up": up_token_id, "down": down_token_id},
        "orderbook": {"up": up_book_summary, "down": down_book_summary},
        "book_source": book_source
    }

def _live_prices_from_ws(ctx) -> Optional[Dict[str, Any]]:
    tids = ctx.get("token_ids") or {}
    up_id, down_id = tids.get("up"), tids.get("down")
    if not up_id or not down_id:
        return None
    max_age = settings.MAX_BOOK_AGE_S
    if not max_age:
        return None
    up = polymarket_clob_ws.get_summary(up_id, max_age_s=max_age)
    down = polymarket_clob_ws.get_summary(down_id, max_age_s=max_age)
    if not up or not down:
        return None
    if up.get("bestAsk") is None or down.get("bestAsk") is None:
        return None
    return {
        "prices": {"up": up["bestAsk"], "down": down["bestAsk"]},
        "orderbook": {"up": up, "down": down}
    }

async def evaluate_entry(trigger: str):
    """Re-run the entry decision against live spot + live book."""
    global _last_eval_ts
    ctx = state.get("trade_ctx") or {}
    if not ctx:
        return
    now = time.time()
    if now - _last_eval_ts < MIN_EVAL_INTERVAL_S:
        return
    if now - ctx.get("ts", 0) > CTX_MAX_AGE_S:
        return

    cur_market_id = str((ctx.get("market") or {}).get("id"))
    # Non-blocking: only block if this specific market already has an open trade
    has_trade_for_market = any(
        str(t.get("market_id")) == cur_market_id and t.get("status") in ("OPEN", "PENDING")
        for t in state["active_trades"]
    )
    if not state["running"] or has_trade_for_market:
        return
    if state["withdraw_state"] != "ARMED":
        return
    if state.get("withdraw_locked_market") and state["withdraw_locked_market"] == cur_market_id:
        return
    if ctx.get("strike_open") is None or ctx.get("target_open") is None:
        return

    live = _live_prices_from_ws(ctx)
    if not live:
        return

    spot = (binance_stream.get_last() or {}).get("price")
    if not spot:
        return

    settlement_ms = ctx.get("settlement_ms")
    time_left_min = ((settlement_ms - now * 1000) / 60_000) if settlement_ms else None
    if time_left_min is None or time_left_min <= 0:
        return

    fair_up = indicators.fair_prob_up(
        spot, ctx["target_open"], time_left_min,
        ctx.get("sigma_1m"), drift_per_minute=ctx.get("drift_1m") or 0.0
    )

    decision = engines.decide_ev({
        "mcProbUp": fair_up,
        "priceUp": live["prices"]["up"],
        "priceDown": live["prices"]["down"],
        "minProb": settings.MIN_PROB_EV,
        "evThreshold": settings.EV_THRESHOLD,
        "rsi": ctx.get("rsi"),
        "rsiOverbought": settings.RSI_OVERBOUGHT,
        "rsiOversold": settings.RSI_OVERSOLD,
        "ha1mColour": ctx.get("ha_1m_colour"),
        "ha5mColour": ctx.get("ha_5m_colour"),
        "ema15m": ctx.get("ema_15m"),
        "emaPeriod": settings.EMA_15M_PERIOD,
        "spotPrice": spot,
        "secondsLeft": time_left_min * 60,
        "minSecondsLeft": settings.MIN_SECONDS_LEFT,
    })
    _last_eval_ts = now
    if decision["action"] != "ENTER":
        return

    async with _entry_lock:
        has_trade_for_market = any(
            str(t.get("market_id")) == cur_market_id and t.get("status") in ("OPEN", "PENDING")
            for t in state["active_trades"]
        )
        if has_trade_for_market or not state["running"]:
            return
        result = await execute_trade(
            decision, live["prices"], ctx["market"], ctx["strike_open"],
            ctx.get("token_ids", {}), live["orderbook"],
            strike_source=ctx.get("strike_source", "chainlink_ws"),
            window_start_ms=ctx.get("window_start_ms"), open_reason="ev_entry"
        )

    if result == "entered":
        state["event_exec"] = f"entered_on_{trigger}"
        log_message(f"Entered on {trigger} tick (event-driven, {(now - ctx['ts']) * 1000:.0f}ms after last housekeeping tick)")
        ts = state["latest_data"].get("trading_state")
        if isinstance(ts, dict):
            ts["active_trades"] = state["active_trades"]
            ts["balance"] = state["paper_balance"]
        await broadcast_state()
    elif result not in (None, "slot_busy", "no_trade"):
        state["event_exec"] = f"{result}_on_{trigger}"

async def entry_watcher():
    while True:
        try:
            await _market_event.wait()
            _market_event.clear()
            await evaluate_entry("book" if polymarket_clob_ws.connected else "spot")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Entry watcher error: {e}")
        await asyncio.sleep(MIN_EVAL_INTERVAL_S)

async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            binance_ws = binance_stream.get_last()
            if not binance_ws.get("price"):
                poly_ws_last = polymarket_ws_stream.get_last()
                cl_ws_last = chainlink_ws_stream.get_last()
                binance_ws["price"] = poly_ws_last.get("price") or cl_ws_last.get("price")
            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd(),
                fetch_polymarket_snapshot(),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()
            klines_15m = binance_kline_15m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price

            now_ms_feed = time.time() * 1000

            def _age_ok(snap, max_age_ms):
                px = (snap or {}).get("price")
                if not px:
                    return None
                ts = (snap or {}).get("updatedAt")
                if ts and (now_ms_feed - float(ts)) > max_age_ms:
                    return None
                return px

            sources = (
                (poly_ws, "Polymarket WS", POLY_WS_MAX_AGE_MS),
                (cl_ws, "Chainlink RPC WS", ONCHAIN_MAX_AGE_MS),
                (chainlink_data, "Chainlink RPC REST", ONCHAIN_MAX_AGE_MS)
            )

            current_price = None
            price_source = None
            price_is_fresh = False
            for snap, label, max_age in sources:
                px = _age_ok(snap, max_age)
                if px:
                    current_price, price_source, price_is_fresh = px, label, True
                    break
            if current_price is None:
                for snap, label, _ in sources:
                    if (snap or {}).get("price"):
                        current_price, price_source = snap["price"], label + " (STALE)"
                        break

            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = None
            if poly_snapshot.get("ok"):
                _mkt = poly_snapshot["market"]
                _esr = _mkt.get("eventStartTime") or _mkt.get("gameStartTime")
                if _esr:
                    try:
                        event_start_ms = int(datetime.fromisoformat(str(_esr).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        event_start_ms = None
                if event_start_ms is None and _mkt.get("endDate"):
                    try:
                        event_start_ms = int(datetime.fromisoformat(_mkt["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - window_ms
                    except Exception:
                        event_start_ms = None
            if event_start_ms is None:
                event_start_ms = int(timing["startMs"])

            start_ms = event_start_ms
            # Strike is always the exact on-chain Chainlink round at eventStartTime
            win = await mark_window_open_with_recovery(
                start_ms, window_ms, current_price, spot_price,
                price_source, price_is_fresh, klines_5m
            )

            strike_open = win["chainlink"]
            strike_source = win.get("strike_source", "chainlink_on_chain")

            model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == start_ms:
                    model_open = c["open"]
                    break
                if c["openTime"] < start_ms:
                    break
            model_open = model_open or win.get("binance")
            target_open = model_open if strike_open is not None else None

            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]

            # Pure NumPy realized drift & vol + fair prob
            drift_1m, sigma_1m = indicators.realized_drift_vol(klines_5m, lookback=300, minutes_per_candle=5)
            fair_up = indicators.fair_prob_up(
                spot_price or 0, target_open or 0,
                time_left_min, sigma_1m,
                drift_per_minute=drift_1m or 0.0
            )
            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "minutes_left": time_left_min,
                "sigma_1m": sigma_1m,
            }

            closes = [c["close"] for c in klines_1m]
            rsi_now = indicators.compute_rsi(closes, settings.RSI_PERIOD)

            consec = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_1m))
            consec_5m = {"color": None, "count": 0}
            if len(klines_5m) >= 20:
                consec_5m = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_5m))

            # 15m EMA Macro Trend (20-period)
            ema_15m_data = indicators.evaluate_15m_ema(klines_15m, spot_price, period=settings.EMA_15M_PERIOD)

            market_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            market_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None

            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            ha_1m_colour = consec["color"]
            ha_5m_colour = consec_5m["color"]

            decision = engines.decide_ev({
                "mcProbUp": fair_up,
                "priceUp": market_up,
                "priceDown": market_down,
                "minProb": settings.MIN_PROB_EV,
                "evThreshold": settings.EV_THRESHOLD,
                "rsi": rsi_now,
                "rsiOverbought": settings.RSI_OVERBOUGHT,
                "rsiOversold": settings.RSI_OVERSOLD,
                "ha1mColour": ha_1m_colour,
                "ha5mColour": ha_5m_colour,
                "ema15m": ema_15m_data.get("ema"),
                "emaPeriod": settings.EMA_15M_PERIOD,
                "spotPrice": spot_price,
                "secondsLeft": time_left_min * 60 if time_left_min is not None else None,
                "minSecondsLeft": settings.MIN_SECONDS_LEFT,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            cur_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot["ok"] else None
            if state.get("withdraw_locked_market") and cur_market_id and state["withdraw_locked_market"] != cur_market_id:
                state["withdraw_locked_market"] = None

            withdraw_locked = (state.get("withdraw_locked_market") is not None and state["withdraw_locked_market"] == cur_market_id)
            entries_allowed = (state["running"] and state["withdraw_state"] == "ARMED" and not withdraw_locked)

            if poly_snapshot["ok"]:
                state["trade_ctx"] = {
                    "ts": time.time(),
                    "market": poly_snapshot["market"],
                    "token_ids": poly_snapshot.get("token_ids", {}),
                    "strike_open": strike_open,
                    "strike_source": strike_source,
                    "target_open": target_open,
                    "window_start_ms": start_ms,
                    "settlement_ms": settlement_ms,
                    "sigma_1m": sigma_1m,
                    "drift_1m": drift_1m,
                    "rsi": rsi_now,
                    "ha_1m_colour": ha_1m_colour,
                    "ha_5m_colour": ha_5m_colour,
                    "ema_15m": ema_15m_data.get("ema"),
                    "ema_15m_data": ema_15m_data,
                }
            else:
                state["trade_ctx"] = {}

            exec_result = None
            if poly_snapshot["ok"] and entries_allowed:
                flipped = await maybe_flip_position(decision, poly_snapshot, time_left_min)
                async with _entry_lock:
                    exec_result = await execute_trade(
                        decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open,
                        poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}),
                        strike_source=strike_source, window_start_ms=start_ms,
                        open_reason="flip_entry" if flipped else "ev_entry"
                    )
            elif not state["running"]:
                exec_result = "stopped"
            elif state["withdraw_state"] != "ARMED":
                exec_result = f"withdraw_{state['withdraw_state'].lower()}"
            elif withdraw_locked:
                exec_result = "withdraw_locked"

            await update_trades(current_prices_dict)

            open_value = 0.0
            for t in state["active_trades"]:
                mark = None
                if poly_snapshot["ok"] and str(t.get("market_id")) == str(poly_snapshot["market"].get("id")):
                    ob = (poly_snapshot.get("orderbook") or {}).get("up" if t["side"] == "UP" else "down") or {}
                    mark = ob.get("bestBid") or (market_up if t["side"] == "UP" else market_down)
                if mark:
                    t["mark_price"] = mark
                    t["unrealized_pl"] = (t["shares"] * mark) - t["amount"]
                    open_value += t["shares"] * mark
                else:
                    t["unrealized_pl"] = None
                    open_value += t["amount"]

            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 30:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts

            equity = state["paper_balance"] + open_value
            await maybe_auto_withdraw(equity, poly_snapshot, reflect_running_cb=reflect_running_now)
            if not state["active_trades"]:
                open_value = 0.0

            if state.get("event_exec"):
                exec_result = state["event_exec"]
                state["event_exec"] = None

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            utils.append_csv_row("./logs/signals.csv", csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, fair_up, 1 - fair_up, market_up, market_down,
                edge["edgeUp"], edge["edgeDown"],
                f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "log_seq": state["log_seq"],
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "running": state["running"],
                    "balance": state["paper_balance"],
                    "equity": state["paper_balance"] + open_value,
                    "open_value": open_value,
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "withdraw": {
                        "enabled": settings.AUTO_WITHDRAW_ENABLED,
                        "state": state["withdraw_state"],
                        "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
                        "amount": settings.WITHDRAW_AMOUNT,
                        "last": state["last_withdrawal"],
                    }
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "window_open": strike_open,
                    "window_open_source": strike_source,
                    "model_open": model_open,
                    "window_start_ms": start_ms,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot["ok"] else None
                },
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "ema_15m": ema_15m_data,
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view,
                    "edge": edge,
                    "decision": decision
                }
            }
            state["last_update_ts"] = time.time()
            await broadcast_state()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await asyncio.sleep(settings.POLL_INTERVAL_MS / 1000)

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    load_telegram_subscribers()
    await seed_kline_buffers()

    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(binance_kline_15m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(polymarket_clob_ws.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(telegram_poller()),
        asyncio.create_task(entry_watcher()),
        asyncio.create_task(update_loop())
    ]

    yield

    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    binance_kline_15m.close()
    polymarket_ws_stream.close()
    polymarket_clob_ws.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant", lifespan=lifespan)
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
