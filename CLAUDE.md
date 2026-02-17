# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

polyBot is a real-time Polymarket trading terminal for BTC 5-minute binary options markets. It streams order book data via WebSocket, detects arbitrage, runs paper trading simulations, and computes Black-Scholes fair prices — all rendered in a curses-based terminal UI.

## Running the Applications

```bash
# Activate the virtual environment (Python 3.13)
source venv/bin/activate

# Main app: real-time curses TUI with orderbooks, arbs, paper trading, BS pricing
./venv/bin/python polybot_ws.py

# Arb recorder: headless, writes arb opportunities to arbs.csv
./venv/bin/python arb_recorder.py

# Simple one-shot orderbook viewer (REST, no WebSocket)
./venv/bin/python polybot.py

# Install dependencies
./venv/bin/pip install requests websocket-client
```

## Architecture

### Three Entry Points

- **polybot_ws.py** — Main application. `OrderBook` class holds all state behind a single `threading.Lock()`. Four threads run concurrently: WebSocket consumer (`run_ws`), market rotation (`run_rotation`), BTC price poller (`run_btc_price`), and curses UI (`draw`). The UI renders 5 columns: Up book, Down book, trade tape, volume profile, and arb list, plus paper trading and market statistics panels below.

- **arb_recorder.py** — Lightweight alternative using `ArbRecorder` class. Same WebSocket/arb detection logic but outputs to CSV instead of curses. Useful for data collection.

- **polybot.py** — Simple REST-based utility. Fetches market + orderbooks once and prints side-by-side with ANSI colors. No WebSocket, no state.

### Threading Model (polybot_ws.py)

All shared state lives in `OrderBook` and is protected by a single lock. Background threads:
- `run_ws()` — WebSocket with auto-reconnect, handles `book` (snapshot) and `price_change` (delta) events. Initial message arrives as a **list**, not a dict.
- `run_rotation()` — Sleeps until market expiry + 2s, fetches next market, swaps WS subscriptions.
- `run_btc_price()` — Polls Binance every 1s, updates volatility and BS prices via `record_price()`.
- `draw()` — Curses loop, redraws every ~100ms. Calls thread-safe getters on `OrderBook`.

### Key Domain Logic

- **Market slug format**: `btc-updown-5m-{unix_ts_rounded_to_300}`
- **Arb detection**: BUY arb if `best_ask_up + best_ask_down < 1.0`; SELL arb if `best_bid_up + best_bid_down > 1.0`. Tracked as open/close events with duration.
- **Paper trading**: Places limit orders at 0.45 (buy) and 0.55 (sell) on both outcomes. Fills checked against orderbook on every update. Settles on rotation based on BTC vs strike. First market is skipped.
- **Black-Scholes**: Binary call = `N(d2)`, put = `1 - N(d2)`. Realized volatility from rolling 5-min log returns, annualized. Risk-free rate = 0.
- **Volume profile**: Persistent per-market accumulation split Up/Down. Down prices normalized via `1 - price` so both sides align.

### External APIs

| API | Base URL | Used For |
|-----|----------|----------|
| Polymarket Gamma | `https://gamma-api.polymarket.com` | Market metadata by slug |
| Polymarket CLOB | `https://clob.polymarket.com` | REST orderbook (polybot.py only) |
| Polymarket WS | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Real-time book/trade streaming |
| Binance | `https://api.binance.com/api/v3` | Current BTC price + historical klines for strike |

WebSocket requires PING every 10 seconds to stay alive. Subscribe with `{"assets_ids": [...], "type": "market"}`.
