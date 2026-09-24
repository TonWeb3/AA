import asyncio
import json
import aiohttp
import time
import heapq
from typing import Optional, Callable, Dict, List
from .config import settings
from .net_utils import get_proxy_url_for

class BinanceTradeStream:
    """Fast spot-price feed from Binance @trade — the leading signal for fair_prob."""
    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        self.symbol = symbol.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_ts = None
        self.closed = False

    async def start(self):
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"

        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance WS: {self.symbol}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = json.loads(msg.data)
                                self._process_trade(float(data_msg.get("p")))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance trade WS failed: {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def _process_trade(self, p: float):
        self.last_price = p
        self.last_ts = time.time()

        # A SYNC callback is called inline — BTC trades arrive many times a second and
        # spawning a task per tick just to set a flag is pure churn. A coroutine
        # callback still gets a task, so existing async consumers are unaffected.
        if self.on_update:
            res = self.on_update({"price": self.last_price, "ts": self.last_ts})
            if asyncio.iscoroutine(res):
                asyncio.create_task(res)

    def get_last(self):
        return {"price": self.last_price, "ts": self.last_ts}

    def close(self):
        self.closed = True

class BinanceKlineStream:
    def __init__(self, symbol: str, interval: str, limit: int = 240):
        self.symbol = symbol.lower()
        self.interval = interval
        self.limit = limit
        self.candles = []
        self.closed = False

    async def start(self):
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"

        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Kline WS: {self.symbol} {self.interval}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = json.loads(msg.data)
                                k = data_msg.get("k", {})
                                candle = {
                                    "openTime": int(k.get("t")),
                                    "open": float(k.get("o")),
                                    "high": float(k.get("h")),
                                    "low": float(k.get("l")),
                                    "close": float(k.get("c")),
                                    "volume": float(k.get("v")),
                                    "closeTime": int(k.get("T")),
                                    "isClosed": k.get("x")
                                }
                                self._update_candle(candle)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance Kline WS failed: {e}")
                if not self.closed:
                    await asyncio.sleep(5)

    def _update_candle(self, candle: Dict):
        if not self.candles:
            self.candles.append(candle)
        else:
            if candle["openTime"] == self.candles[-1]["openTime"]:
                self.candles[-1] = candle
            else:
                self.candles.append(candle)
        if len(self.candles) > self.limit:
            self.candles.pop(0)

    def set_candles(self, candles: List[Dict]):
        self.candles = candles[-self.limit:]

    def get_candles(self):
        return self.candles

    def close(self):
        self.closed = True

class PolymarketChainlinkStream:
    def __init__(self, ws_url: str, symbol_includes: str = "btc", on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        self.symbol_includes = symbol_includes.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False

    async def start(self):
        if not self.ws_url:
            return

        async def ping_loop(ws):
            while not self.closed:
                try:
                    await ws.send_str("PING")
                    await asyncio.sleep(5)
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                async with aiohttp.ClientSession(headers=headers) as session:
                    print(f"Connecting to Polymarket WS: {self.ws_url}")
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Polymarket WS. Subscribing to topics...")

                        # Comprehensive topic subscription for maximum compatibility
                        topics = ["crypto_prices_chainlink", "price_chainlink", "settlement_prices"]
                        for t in topics:
                            await ws.send_json({"action": "subscribe", "topic": t})

                        asyncio.create_task(ping_loop(ws))

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_text = msg.data
                                if data_text in ("PONG", "OK", "PONG\n"):
                                    continue

                                try:
                                    data_msg = json.loads(data_text)
                                except:
                                    continue

                                topic = data_msg.get("topic")
                                if topic not in ("crypto_prices_chainlink", "price_chainlink", "settlement_prices"):
                                    continue

                                payload = data_msg.get("payload", {})
                                if isinstance(payload, str):
                                    try:
                                        payload = json.loads(payload)
                                    except:
                                        continue

                                updates = payload if isinstance(payload, list) else [payload]

                                for update in updates:
                                    if not isinstance(update, dict): continue

                                    sym = str(update.get("symbol") or update.get("pair") or update.get("ticker") or update.get("asset") or "").lower()
                                    # Normalize symbol btc-usd, btc/usd, bitcoin
                                    if self.symbol_includes:
                                        target = self.symbol_includes.lower()
                                        if target not in sym and not (target == "btc" and "bitcoin" in sym):
                                            continue

                                    try:
                                        price_val = update.get("price") or update.get("value") or update.get("current")
                                        if price_val is not None:
                                            self.last_price = float(price_val)
                                            ts_val = update.get("timestamp") or update.get("updated_at") or time.time()
                                            updated_at = float(ts_val)
                                            if updated_at < 10000000000: updated_at *= 1000
                                            self.last_updated_at = updated_at

                                            if self.on_update:
                                                await self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"})
                                    except:
                                        continue

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Polymarket): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"}

    def close(self):
        self.closed = True

class PolymarketClobBookStream:
    """Live CLOB order books over Polymarket's market websocket.

    Replaces the per-tick REST `/book` + `/price` polling for the two active tokens.
    The book is the half of the edge calculation the strategy is racing, so polling it
    at 1 Hz while the spot feed is pushed measures the race on the slower clock.

    Design notes, each one a bug avoided:

    - **Levels are held as {price: size} maps**, not the raw arrays, because a
      `price_change` is a DELTA. Applying deltas to a stored snapshot array (or, worse,
      only to a cached best-bid/best-ask scalar) leaves the depth frozen at the first
      snapshot while the quote moves — and the depth is what gates the stake.
    - **Best bid is max(price), best ask is min(price)** — never `[0]`. Polymarket
      returns bids ASCENDING and asks DESCENDING, so index 0 is the WORST level on both
      sides. (Same trap as `data._levels`.)
    - **Freshness is recorded per asset and enforced by the caller.** A silently dead
      socket keeps returning its last book forever; `get_summary` returns None past
      `max_age_s` so the caller falls back to REST instead of trading a frozen book.
    - **A keepalive is mandatory.** Without PING the connection is dropped server-side
      after ~30s idle, and aiohttp will not always surface that as an error.
    """

    def __init__(self, ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                 on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        # Called (synchronously, with the asset_id) whenever a book actually changes —
        # this is what makes the entry decision event-driven instead of polled.
        self.on_update = on_update
        self.asset_ids: List[str] = []
        # asset_id -> {"bids": {price: size}, "asks": {price: size}, "updated_at": float}
        self.books: Dict[str, Dict] = {}
        self.connected = False
        self.closed = False
        self.last_msg_ts = 0.0
        self._ws = None

    # ── subscription ────────────────────────────────────────────────────────────
    def update_assets(self, asset_ids: List[str]):
        """Point the stream at the current window's two tokens. Called every tick; a
        no-op unless the market actually rolled."""
        ids = sorted({str(a) for a in (asset_ids or []) if a})
        if ids == self.asset_ids:
            return
        self.asset_ids = ids
        for aid in list(self.books):        # drop books for tokens we no longer track
            if aid not in ids:
                self.books.pop(aid, None)
        if self._ws is not None and not self._ws.closed:
            asyncio.create_task(self._subscribe(self._ws))

    async def _subscribe(self, ws):
        if not self.asset_ids:
            return
        try:
            await ws.send_json({"assets_ids": self.asset_ids, "type": "market"})
            print(f"Subscribed to CLOB book WS for {len(self.asset_ids)} token(s)")
        except Exception as e:
            print(f"CLOB book WS subscribe failed: {e}")

    async def start(self):
        async def ping_loop(ws):
            # Idle connections are dropped server-side; aiohttp does not always raise.
            while not self.closed and not ws.closed:
                try:
                    await ws.send_str("PING")
                except Exception:
                    break
                await asyncio.sleep(10)

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None,
                                                  heartbeat=15) as ws:
                        self._ws = ws
                        self.connected = True
                        print(f"Connected to Polymarket CLOB book WS: {self.ws_url}")
                        await self._subscribe(ws)
                        ping = asyncio.create_task(ping_loop(ws))
                        try:
                            while not self.closed:
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    if msg.data in ("PONG", "PING", "OK"):
                                        continue
                                    try:
                                        self._process(json.loads(msg.data))
                                    except Exception:
                                        continue
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        finally:
                            ping.cancel()
            except Exception as e:
                print(f"CLOB book WS error: {e}")
            finally:
                self.connected = False
                self._ws = None
                # The books are now unmaintained. Drop them rather than let a caller
                # read a snapshot frozen at the moment the socket died — the staleness
                # check would catch it eventually, this makes it immediate.
                self.books.clear()
            if not self.closed:
                await asyncio.sleep(2)

    # ── message handling ────────────────────────────────────────────────────────
    def _process(self, payload):
        self.last_msg_ts = time.time()
        events = payload if isinstance(payload, list) else [payload]
        touched = set()
        for ev in events:
            if not isinstance(ev, dict):
                continue
            et = str(ev.get("event_type") or ev.get("type") or "")
            if et == "book" or ("bids" in ev and "asks" in ev):
                touched |= self._snapshot(ev)
            elif et.startswith("price_change") or ev.get("price_changes") or ev.get("changes"):
                touched |= self._delta(ev)
        # Fire once per frame, not once per level change: a frame carrying twenty
        # deltas is one book state, and the consumer only ever reads the result.
        if touched and self.on_update:
            try:
                self.on_update(touched)
            except Exception:
                pass

    def _empty(self):
        return {"bids": {}, "asks": {}, "updated_at": self.last_msg_ts, "_cached_summary": None, "_cached_depth": 0}

    @staticmethod
    def _levels_to_map(raw):
        out = {}
        for lvl in raw or []:
            try:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    p = float(lvl[0]); s = float(lvl[1])
                elif isinstance(lvl, dict):
                    p = float(lvl.get("price") or lvl.get("p"))
                    s = float(lvl.get("size") or lvl.get("s"))
                else:
                    continue
            except (KeyError, TypeError, ValueError):
                continue
            if s > 0:
                out[p] = s
        return out

    def _snapshot(self, ev):
        aid = str(ev.get("asset_id") or ev.get("assetId") or ev.get("token_id") or ev.get("market") or "")
        if not aid:
            return set()
        self.books[aid] = {
            "bids": self._levels_to_map(ev.get("bids")),
            "asks": self._levels_to_map(ev.get("asks")),
            "updated_at": self.last_msg_ts,
            "_cached_summary": None,
            "_cached_depth": 0,
        }
        return {aid}

    def _delta(self, ev):
        # Two shapes are seen in the wild: a flat `changes` list on an event that names
        # the asset, and a `price_changes` list whose entries name their own asset.
        changes = ev.get("price_changes") or ev.get("changes") or []
        touched = set()
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            aid = str(ch.get("asset_id") or ch.get("assetId") or ch.get("token_id") or ev.get("asset_id") or ev.get("assetId") or ev.get("token_id") or "")
            if not aid:
                continue
            touched.add(aid)
            try:
                price = float(ch.get("price") or ch.get("p")); size = float(ch.get("size") or ch.get("s"))
            except (KeyError, TypeError, ValueError):
                continue
            side = str(ch.get("side") or "").upper()
            key = "bids" if side in ("BUY", "BID", "BIDS") else "asks"
            book = self.books.setdefault(aid, self._empty())
            if size <= 0:
                book[key].pop(price, None)   # size 0 removes the level
            else:
                book[key][price] = size
            book["updated_at"] = self.last_msg_ts
            book["_cached_summary"] = None  # Invalidate summary cache on delta
        return touched

    # ── read side ───────────────────────────────────────────────────────────────
    def get_summary(self, asset_id: str, depth_levels: int = 20,
                    max_age_s: float = 15.0) -> Optional[Dict]:
        """The same shape `data.summarize_order_book` returns, or None when there is no
        usable book — unsubscribed, never delivered, or stale. None means "use REST"."""
        book = self.books.get(str(asset_id))
        if not book:
            return None
        if max_age_s and (time.time() - book.get("updated_at", 0)) > max_age_s:
            return None

        # Return cached top-N summary if still valid for the requested depth
        cached = book.get("_cached_summary")
        if cached is not None and book.get("_cached_depth", 0) >= depth_levels:
            return cached

        bids = book.get("bids")
        asks = book.get("asks")
        if not bids and not asks:
            return None

        # O(N log K) top-K selection via min/max heaps instead of O(N log N) full sort
        bid_levels = heapq.nlargest(depth_levels, bids.items(), key=lambda x: x[0]) if bids else []
        ask_levels = heapq.nsmallest(depth_levels, asks.items(), key=lambda x: x[0]) if asks else []

        if not bid_levels and not ask_levels:
            return None

        best_bid = bid_levels[0][0] if bid_levels else None
        best_ask = ask_levels[0][0] if ask_levels else None
        summary = {
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "spread": (best_ask - best_bid) if best_bid is not None and best_ask is not None else None,
            "bidLiquidity": sum(s for _, s in bid_levels[:depth_levels]),
            "askLiquidity": sum(s for _, s in ask_levels[:depth_levels]),
            "askLevels": ask_levels[:depth_levels],
            "bidLevels": bid_levels[:depth_levels],
        }
        book["_cached_summary"] = summary
        book["_cached_depth"] = depth_levels
        return summary

    def age(self, asset_id: str) -> Optional[float]:
        book = self.books.get(str(asset_id))
        if not book:
            return None
        return time.time() - book.get("updated_at", 0)

    def close(self):
        self.closed = True


class ChainlinkPriceStream:
    def __init__(self, aggregator: str, decimals: int = 8, on_update: Optional[Callable] = None):
        self.aggregator = aggregator
        self.decimals = decimals
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False
        self.wss_urls = settings.POLYGON_WSS_URLS + ([settings.POLYGON_WSS_URL] if settings.POLYGON_WSS_URL else [])

    async def start(self):
        if not self.wss_urls or not self.aggregator:
            return

        url_idx = 0
        while not self.closed:
            url = self.wss_urls[url_idx % len(self.wss_urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Chainlink RPC WS: {url}")
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {
                                    "address": self.aggregator,
                                    "topics": ["0x05598845ccd9c46647361c770d3023029a3514781ca1029c91d84f2913e79435"] # AnswerUpdated topic
                                }
                            ]
                        }
                        await ws.send_json(sub_msg)

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_res = json.loads(msg.data)
                                if data_res.get("method") == "eth_subscription":
                                    log = data_res.get("params", {}).get("result", {})
                                    topics = log.get("topics", [])
                                    if len(topics) >= 2:
                                        answer = int(topics[1], 16)
                                        if answer >= 2**255:
                                            answer -= 2**256

                                        self.last_price = answer / (10 ** self.decimals)
                                        data_hex = log.get("data", "0x")
                                        if len(data_hex) >= 66:
                                            self.last_updated_at = int(data_hex[2:66], 16) * 1000

                                        if self.on_update:
                                            await self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Chainlink RPC): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"}

    def close(self):
        self.closed = True
