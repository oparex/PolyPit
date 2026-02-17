# PolyScope

Real-time trading terminal for Polymarket BTC 5-minute binary options. Streams live order books via WebSocket, detects arbitrage opportunities, simulates paper trades, and computes Black-Scholes fair prices — all in a curses-based terminal UI.

![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue)

## Features

- **Live Order Books** — Side-by-side Up/Down books streamed over WebSocket with auto-reconnect
- **Arbitrage Detection** — Flags BUY arbs (asks sum < $1) and SELL arbs (bids sum > $1) with edge, size, and duration tracking
- **Black-Scholes Pricing** — Binary option fair values from realized volatility (rolling 5-min window)
- **Paper Trading** — Simulated limit orders at 0.45/0.55 with fill detection and per-market settlement
- **Trade Tape** — Real-time trades colored by outcome (Up = green, Down = red)
- **Volume Profile** — Split histogram showing Up and Down volume by price level
- **Market Statistics** — Per-market VWAP, volume, net P&L, strike/settle prices, and min/max ranges
- **Auto Rotation** — Seamlessly switches to the next 5-minute market on expiry

## Quick Start

```bash
# Clone and set up
git clone <repo-url> && cd polyBot
python3 -m venv venv
source venv/bin/activate
pip install requests websocket-client

# Run the main dashboard
python polybot_ws.py
```

Press `q` to quit. The terminal needs to be wide enough to show all panels (~200+ columns for full layout).

## Programs

| Script | Description |
|--------|-------------|
| `polybot_ws.py` | Main dashboard — full curses TUI with all features |
| `arb_recorder.py` | Headless arb logger — writes opportunities to `arbs.csv` |
| `polybot.py` | One-shot orderbook snapshot — REST-based, no WebSocket |

## Dashboard Layout

```
+------------------+----+------------------+---------------+---------------+------------------+
|   Up Order Book  | σ  | Down Order Book  |  Trade Tape   | Volume Profile|  Arb Opportunities|
|   asks (sell)    |    |   asks (sell)    |               |  Up | Dn      |                  |
|   ── spread ──   |vol%|   ── spread ──   |  HH:MM:SS ... |  ████|████    |  TIME TYPE EDGE  |
|   bids (buy)     |    |   bids (buy)     |               |     |         |                  |
+------------------+----+------------------+---------------+---------------+------------------+
| Paper Trading: filled/pending          | Market Statistics                                   |
| Market          Fills  P&L  Result     | Market       UpVol UpAvg UpNet DnVol DnAvg DnNet...|
+----------------------------------------+----------------------------------------------------+
```

## How It Works

**Market Rotation**: BTC up/down markets use the slug `btc-updown-5m-{timestamp}` where the timestamp is rounded down to 5-minute boundaries. The app fetches the current market on startup, subscribes to its WebSocket feed, and automatically rotates to the next market on expiry.

**Arbitrage**: In a binary market, Up + Down should equal $1.00. When the sum of best asks is below $1 (BUY arb) or the sum of best bids is above $1 (SELL arb), an opportunity exists. The app tracks when arbs open and close, recording their duration and maximum edge.

**Fair Pricing**: BTC price is polled from Binance every second. Realized volatility is computed from log returns over a rolling 5-minute window, then annualized. This feeds a Black-Scholes binary option pricer: `fair_call = N(d2)`, `fair_put = 1 - N(d2)`, displayed on the spread line between best bid and ask.

**Paper Trading**: On each new market (after the first), the system places four limit orders: BUY Up@0.45, SELL Up@0.55, BUY Down@0.45, SELL Down@0.55. Orders fill when the orderbook crosses their price. At market rotation, positions settle based on whether BTC finished above or below the strike price.

## APIs Used

| Service | Purpose |
|---------|---------|
| [Polymarket Gamma API](https://gamma-api.polymarket.com) | Market metadata (slug, token IDs, outcomes) |
| [Polymarket WebSocket](wss://ws-subscriptions-clob.polymarket.com/ws/market) | Real-time order book and trade streaming |
| [Binance API](https://api.binance.com/api/v3) | BTC/USDT spot price and historical klines |

## Dependencies

- `requests` — HTTP client for REST APIs
- `websocket-client` — WebSocket protocol support
- Python standard library: `curses`, `threading`, `math`, `collections`, `csv`, `json`
