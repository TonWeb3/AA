# Polymarket BTC 15m Assistant (Python FastAPI)

A real-time trading assistant for Polymarket **"Bitcoin Up or Down" 15-minute**
markets, ported to Python and FastAPI.

It runs a **latency-arbitrage** strategy: a fast closed-form fair probability from
Binance spot vs Polymarket's (possibly stale) implied price, traded on the gap, with
position size set by a simple percent-of-balance or fixed-dollar risk.

## The strategy in one screen

The model has **no predictive edge** over the trivial "is spot already above the 15m
open?" baseline — that signal is fully priced by the market. The only edge left is
**latency**: acting on a Binance spot move before Polymarket's thin book reprices. So
the entry decision is deliberately just *fair probability vs the market's ask*, and
every indicator survives only as a filter that can **block** a trade, never as one
that creates or reweights a signal.

The decision runs in four stages, in this order:

1. **15m 20-period EMA macro trend** — macro permission. The 20 EMA on the 15-minute
   timeframe decides whether a direction may be traded at all: spot price above the
   20 EMA permits UP only, while spot price below the 20 EMA permits DOWN only.
   Fails closed — no data means no trade.
2. **1m + 5m Heiken-Ashi direction** — micro confirmation. Both faster Heiken-Ashi
   candles must point the **same way** as the side permitted by the 15m 20 EMA: green
   confirms UP, red confirms DOWN. Together that aligns macro trend (15m 20 EMA) with
   micro momentum (5m and 1m HA running right now). These two read the **developing**
   candle on purpose (the 1m turning green *is* the move being raced). Fails closed
   on an unknown colour.
3. **RSI(14) extremes veto** — don't buy UP above 80 or DOWN below 20. This is the one
   counter-trend check in the stack; everything above it is confluence, so without it
   nothing can stop a fully-aligned signal.
4. **EV gate** — enter only when `fair_prob − ask_price` clears `ev_threshold`, the
   chosen side's probability clears `min_prob`, and at least `min_seconds_left`
   remains in the window.

The EMA, Heiken-Ashi, and RSI thresholds are defined in
[`bot/indicators.py`](bot/indicators.py) and [`bot/engines.py`](bot/engines.py)
and configurable in `config.json` and Settings.

> **The filters are restrictive by design.** The EV engine picks its side first, then
> checks the 15m 20 EMA macro trend for *that* side only — it never falls back to the
> second-best one, and the 1m and 5m must then both agree with it. Long stretches of
> "NO TRADE" with `spot_*_below_15m_20ema` or `ha1m_*` / `ha5m_*` reasons are the
> filters working, not a fault. Every tick's reason code is written to `logs/signals.csv`.
>
> **Non-blocking Window Transitions.** A trade from a prior 15m window that is waiting
> for authoritative resolution/settlement does not block the bot from opening a fresh
> position in the new window. Each market's trade is isolated by market ID and settles
> independently.

## Features

- Real-time Web Dashboard (FastAPI + Jinja2 + Alpine.js), pushed over a websocket
- Fast fair-probability model (closed-form GBM) + EV entry engine
- Three-layer filtering: 15m Heiken-Ashi parent shield, 1m + 5m Heiken-Ashi
  directional confirmation, RSI extremes veto
- Event-driven entries: the decision re-runs on a Binance trade tick or a CLOB book
  update, not on a 1-second timer
- Trade Execution: Paper Trading simulation vs Live Mode toggle
- Data Sources: Binance (trades + klines), Polymarket (Gamma / CLOB REST + book
  websocket), Chainlink (WebSocket + RPC)
- Proxy Support: Global HTTP/HTTPS/SOCKS proxy configuration

## Requirements

- Python **3.12+** — required, not preferred. `polymarket-apis` declares
  `Requires-Python >=3.12`; on 3.11 pip ignores every published version and fails with
  the confusing `Could not find a version ... (from versions: none)`.
- pip (comes with Python)

## Local Run

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Configure `config.json`

Set your trading mode, risk preferences, and optional private key in `config.json`.
Position size is set by `trading.risk_type` (`"percent"` = `risk_value`% of balance,
or `"fixed"` = `risk_value` dollars) and `trading.risk_value`. The entry engine is
tuned in the `ev` block:

```jsonc
"ev": {
  "ev_threshold": 0.04,          // enter only when fair prob − share price ≥ this (the edge gate)
  "min_prob": 0.55,              // never bet near-coinflips even if EV looks positive
  "min_book_liquidity_usd": 20.0, // skip if the ask side can't absorb the stake
  "min_seconds_left": 30          // stop entering this close to expiry
}
```

The fair-probability horizon is **continuous**: sigma and drift are normalised to
per-minute units and the model scales by `sqrt(minutes_left)`, so conviction tightens
smoothly as the window runs down rather than in 5-minute steps. That makes the model
sharp in the last minutes, which is why `min_seconds_left` exists — near the close EV
against a stale quote looks enormous, but a Fill-Or-Kill order into a closing book is
the least reliable fill of the window.

All of these are also editable live on the **Settings** page.

### 3) Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Access the dashboard at `http://localhost:8000`.

> Running `python main.py` directly instead starts uvicorn on port **8080**.

## Docker

```bash
docker build -t polymarket-assistant .
docker run -p 8000:8000 polymarket-assistant
```

## Deployment on Render

If you are seeing errors related to Node.js or `npm run start`, it is because Render is
auto-detecting the old environment. **You must manually set the runtime to Python.**

1. Create a **Web Service** on Render.
2. Under **Runtime**, explicitly select **Python 3**.
3. Set the following commands:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Add any necessary environment variables (optional).

Keep the worker count at **1**. The bot holds all its state in memory in one event
loop, so a second worker would run a second independent bot against the same wallet.

## Live Trading

Switching **Mode** to `live` makes the bot place real **Fill-Or-Kill market BUY**
orders on **Polymarket CLOB V2** via `polymarket-apis`, using the gasless
**deposit-wallet** flow.

Your trading wallet is **derived from your key** — there is no signature type or funder
address to choose. The bot checks the deposit, proxy and safe wallets and trades from
whichever actually holds **pUSD**.

1. Set a **private key or 12/24-word seed phrase** (Settings → Credentials, or
   `config.json`). It signs orders but holds no funds and needs no gas.
2. Set the **Relayer API key** (`config.json` → `relayer.api_key`). It sponsors the
   one-time on-chain setup, so you never pay gas. Optionally set an **Alchemy key** for
   a private Polygon RPC.
3. Click **Test Connection** — read-only, no relayer key needed. It lists every derived
   wallet with its pUSD balance and ticks the one that will be traded.
4. **Deposit pUSD** to that wallet through Polymarket.
5. Click **Setup Wallet (gasless)** to deploy + approve it. Once per fresh wallet.
6. *(Optional)* **Enable Auto-Redeem** so wins convert back to pUSD by themselves.
7. *(Optional)* **Auto-Withdrawal** (Settings → Capital Extractor) — see below.

Orders are **slippage-capped**: the limit is the quote plus `CLOB_MAX_SLIPPAGE`
(default 2¢), so if the book moves away the order is killed rather than filled badly. A
fill is only recorded when it is positively confirmed, and the trade is stamped with the
**actual** fill price and size. In live mode the dashboard balance is the real on-chain
pUSD balance (refreshed every 30s). Order failures appear in the Console Log.

**Press Start on the dashboard** — the bot does not trade until you do.

### Auto-Withdrawal (Capital Extractor)

Live mode only, off by default. Once **equity** (cash + the value of any open position)
reaches `capital_extractor.trigger_balance`, the bot:

1. pauses new entries,
2. sells any open position into the bid to go flat,
3. withdraws `withdraw_amount` of pUSD, gasless, to `withdraw_address` (blank = your own
   key/seed EOA),
4. resumes at the **next** 15m market — or stops entirely if
   `auto_resume_after_withdrawal` is off.

`resume_after` chooses whether to resume as soon as the transaction is `submitted` or
to wait up to 3 minutes for it to be `confirmed` on-chain.

Add a **Telegram** bot token (Settings → Telegram Alerts) to be alerted each time one
completes. Recipients subscribe themselves by sending the bot `/start` — there are no
chat IDs to copy by hand — and leave with `/stop`.

## Safety

This is not financial advice. Use at your own risk; live mode trades real funds.

**The edge is unproven.** The model has no predictive edge over "is spot already above
the open" — that signal is fully priced by the market. The only remaining edge is
latency (beating the book's repricing). Run in **paper mode** and confirm EV-positive
trades actually exist against the real book *before* risking capital.
