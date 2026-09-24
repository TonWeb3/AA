import httpx
from .config import settings
from typing import List, Dict, Optional
from .net_utils import get_proxy_url_for

def to_number(x) -> Optional[float]:
    try:
        n = float(x)
        return n
    except (TypeError, ValueError):
        return None

async def fetch_klines(symbol: str, interval: str, limit: int) -> List[Dict]:
    url = f"{settings.BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
        return [{
            "openTime": int(k[0]),
            "open": to_number(k[1]),
            "high": to_number(k[2]),
            "low": to_number(k[3]),
            "close": to_number(k[4]),
            "volume": to_number(k[5]),
            "closeTime": int(k[6])
        } for k in data]

async def fetch_last_price(symbol: str) -> Optional[float]:
    url = f"{settings.BINANCE_BASE_URL}/api/v3/ticker/price"
    params = {"symbol": symbol}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
        return to_number(data.get("price"))

async def fetch_market_by_slug(slug: str) -> Optional[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/markets"
    params = {"slug": slug}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
    market = data[0] if isinstance(data, list) and data else data
    return market if market else None

async def fetch_live_events_by_series_id(series_id: str, limit: int = 20) -> List[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/events"
    params = {
        "series_id": series_id,
        "active": "true",
        "closed": "false",
        "limit": limit
    }
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
    return data if isinstance(data, list) else []

async def fetch_available_15m_series() -> List[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/events"
    params = {
        "active": "true",
        "closed": "false",
        "limit": 100,
        "tag_id": "102467" # 15M tag
    }
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        try:
            res = await client.get(url, params=params)
            res.raise_for_status()
            events = res.json()
        except:
            events = []

    series_map = {}
    defaults = {
        "10192": "Bitcoin Up or Down 15m",
        "10212": "Ethereum Up or Down 15m"
    }
    for sid, name in defaults.items():
        series_map[sid] = {"series_id": sid, "title": name}

    for e in events:
        series_slug = e.get("seriesSlug", "")
        if "up-or-down-15m" in series_slug:
            sid = e.get("series_id")
            if not sid and e.get("series") and isinstance(e["series"], list) and len(e["series"]) > 0:
                sid = e["series"][0].get("id")

            if sid:
                asset = series_slug.split("-")[0].upper()
                series_map[str(sid)] = {
                    "series_id": str(sid),
                    "title": f"{asset} Up or Down 15m",
                    "slug": series_slug
                }

    return sorted(list(series_map.values()), key=lambda x: x["title"])

def flatten_event_markets(events: List[Dict]) -> List[Dict]:
    out = []
    for e in events:
        markets = e.get("markets", [])
        if isinstance(markets, list):
            out.extend(markets)
    return out

async def fetch_clob_price(token_id: str, side: str) -> Optional[float]:
    url = f"{settings.CLOB_BASE_URL}/price"
    params = {"token_id": token_id, "side": side}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
    return to_number(data.get("price"))

async def fetch_order_book(token_id: str) -> Dict:
    url = f"{settings.CLOB_BASE_URL}/book"
    params = {"token_id": token_id}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        return res.json()

def _levels(raw, reverse: bool):
    """(price, size) pairs ordered from the TOUCH outward.

    Polymarket returns `bids` ASCENDING and `asks` DESCENDING by price, so the best
    price of each side is the LAST element, not the first. Slicing [:n] off the raw
    arrays therefore summed the n WORST levels — depth resting far from the touch, at
    prices we would never trade — and reported it as tradable liquidity. Sort here so
    every caller is measuring the book it can actually hit.
    """
    out = []
    for lvl in raw or []:
        if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            p = to_number(lvl[0])
            s = to_number(lvl[1])
        elif isinstance(lvl, dict):
            p = to_number(lvl.get("price") or lvl.get("p"))
            s = to_number(lvl.get("size") or lvl.get("s"))
        else:
            continue
        if p is not None and s is not None and s > 0:
            out.append((p, s))
    return sorted(out, key=lambda x: x[0], reverse=reverse)


def summarize_order_book(book: Dict, depth_levels: int = 20) -> Dict:
    bid_levels = _levels(book.get("bids"), reverse=True)    # highest price first (touch at [0])
    ask_levels = _levels(book.get("asks"), reverse=False)   # lowest price first (touch at [0])

    best_bid = bid_levels[0][0] if bid_levels else None
    best_ask = ask_levels[0][0] if ask_levels else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None

    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spread": spread,
        "bidLiquidity": sum(s for _, s in bid_levels[:depth_levels]),
        "askLiquidity": sum(s for _, s in ask_levels[:depth_levels]),
        # kept so a caller can price a real fill by walking the book instead of
        # assuming the whole stake clears at the touch
        "askLevels": ask_levels[:depth_levels],
        "bidLevels": bid_levels[:depth_levels],
    }


def sweep_sell(bid_levels, size: float):
    """Walk the BID side (best-first) selling up to `size` shares — the mirror of
    `fill_price_for_usd`. Returns (all_in_avg_price, shares_sold, usd_proceeds).

    Selling a whole position walks DOWN the bids exactly as buying walks up the asks,
    so valuing the exit at the best bid overstates what a real liquidation returns.
    `shares_sold < size` means the visible book cannot absorb the whole position — a
    Fill-Or-Kill sell of it would simply be killed.
    """
    if size <= 0 or not bid_levels:
        return None, 0.0, 0.0
    remaining = float(size)
    sold = 0.0
    proceeds = 0.0
    for price, avail in bid_levels:
        take = min(avail, remaining)
        if take <= 0:
            break
        sold += take
        proceeds += take * price
        remaining -= take
        if remaining <= 1e-9:
            break
    if sold <= 0:
        return None, 0.0, 0.0
    return proceeds / sold, sold, proceeds


def fill_price_for_usd(ask_levels, usd: float):
    """Average price paid to spend `usd` walking the asks, and the shares it buys.

    A marketable order eats level 1, then level 2, and so on; pricing the whole stake
    at the touch flatters every fill. Returns (avg_price, shares, filled_usd) — with
    filled_usd < usd when the visible book cannot absorb the whole order.
    """
    spend = 0.0
    shares = 0.0
    for price, size in ask_levels or []:
        if spend >= usd or price <= 0:
            break
        take_usd = min(usd - spend, price * size)
        spend += take_usd
        shares += take_usd / price
    if shares <= 0:
        return None, 0.0, 0.0
    return spend / shares, shares, spend
