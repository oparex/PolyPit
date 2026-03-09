"""Backtest: Market Making Strategy with Monte Carlo on Polymarket BTC 5-min Up/Down markets.

Replays historical Chainlink + Polymarket orderbook data from QuestDB,
uses precomputed MC paths for fast fair-price calculation, and tracks all trades with P&L.

Data and paths are cached to pickle files so re-runs with different EDGE/ORDER_SIZE/ORDER_DELAY_MS
skip the slow DB + precomputation steps.
"""

import os
import pickle
import time

import numpy as np
import requests

# ── Configuration ──────────────────────────────────────────────────────────────
QUESTDB_API = "http://localhost:9001"
DATE_FILTER = "2026-02-23T12:50;5m"
DATE_FILTER_RETURNS = "2026-02-22;2d"
MC_PATHS = 100_000       # precomputed paths per step count
MC_SAMPLE = 10_000       # paths sampled per MC run
EDGE = 0.02              # spread around fair price for bid/ask
ORDER_SIZE = 1.0          # notional per trade
ORDER_DELAY_MS = 0      # delay before new orders are active
MARKET_START_DELAY = 10   # seconds into market before placing orders
MARKET_STOP_BEFORE = 10   # seconds before expiry to stop placing orders
MIN_STEPS = MARKET_STOP_BEFORE   # 10
MAX_STEPS = 300 - MARKET_START_DELAY  # 290

CACHE_DIR = "cache"
DATA_CACHE = os.path.join(CACHE_DIR, f"data_{DATE_FILTER}.pkl")
PATHS_CACHE = os.path.join(CACHE_DIR, f"paths_{DATE_FILTER}_{MC_PATHS}.pkl")


# ── QuestDB helpers ────────────────────────────────────────────────────────────

def query_questdb(sql: str) -> list[dict]:
    resp = requests.get(
        f"{QUESTDB_API}/exec",
        params={"query": sql},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    cols = [c["name"] for c in data["columns"]]
    return [dict(zip(cols, row)) for row in data["dataset"]]


def ts_to_epoch(ts_str: str) -> float:
    """Convert QuestDB timestamp string to unix epoch seconds."""
    from datetime import datetime, timezone
    ts_str = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts_str)
    return dt.timestamp()


# ── Data Loading ───────────────────────────────────────────────────────────────

def load_chainlink_returns() -> list[dict]:
    """Load raw Chainlink prices for computing log returns (MC precomputation)."""
    sql = (
                f"WITH aligned AS ( "
                f"  ( "
                f"    SELECT "
                f"      exchange_time timestamp, "
                f"      ap_1 as price "
                f"    FROM "
                f"      orderbooks "
                f"    WHERE "
                f"      exchange_time IN '{DATE_FILTER_RETURNS}' "
                f"      AND exchange = 'chainlink' "
                f"      AND pair = 'BTCUSD' "
                f"  ) TIMESTAMP(timestamp) "
                f")"
                f"SELECT "
                f"  timestamp, "
                f"  last(price) as price "
                f"FROM "
                f"  aligned SAMPLE BY 1s FILL(PREV);"
    )
    rows = query_questdb(sql)
    for r in rows:
        r["ts"] = ts_to_epoch(r["timestamp"])
        r["price"] = float(r["price"])
    return rows


def load_chainlink_1s() -> list[dict]:
    """Load Chainlink BTC/USD 1-second sampled prices for loop iteration + MC fair price."""
    sql = (
        f"SELECT timestamp, last(ap_1) price FROM orderbooks "
        f"WHERE timestamp IN '{DATE_FILTER}' AND exchange='chainlink' AND pair='BTCUSD' SAMPLE BY 1s;"
    )
    rows = query_questdb(sql)
    for r in rows:
        r["ts"] = ts_to_epoch(r["timestamp"])
        r["price"] = float(r["price"])
    return rows


def load_polymarket_up() -> list[dict]:
    """Load Polymarket UP orderbook best bid/ask."""
    sql = (
        f"WITH lagged AS ("
        f"  SELECT"
        f"    timestamp,"
        f"    ap_1 ask,"
        f"    lag(ap_1) OVER (ORDER BY timestamp) prev_ask,"
        f"    bp_1 bid,"
        f"    lag(bp_1) OVER (ORDER BY timestamp) prev_bid"
        f"  FROM orderbooks"
        f"  WHERE"
        f"    timestamp IN '{DATE_FILTER}'"
        f"    AND exchange = 'polymarket'"
        f"    AND pair = 'BTCUP'"
        f") "
        f"SELECT timestamp, ask, bid FROM lagged"
        f" WHERE ask != prev_ask AND bid != prev_bid;"
    )
    rows = query_questdb(sql)
    for r in rows:
        r["ts"] = ts_to_epoch(r["timestamp"])
        r["ask"] = float(r["ask"]) if r["ask"] is not None else None
        r["bid"] = float(r["bid"]) if r["bid"] is not None else None
    return rows


def load_strike_settlement() -> list[dict]:
    """Load Chainlink exchange_time prices and derive strike/settlement per 5-min boundary."""
    sql = (
        f"SELECT exchange_time, ap_1 price FROM orderbooks "
        f"WHERE exchange_time IN '{DATE_FILTER}' AND exchange='chainlink' AND pair='BTCUSD' "
        f"ORDER BY exchange_time;"
    )
    rows = query_questdb(sql)
    # group by 5-min boundary on exchange_time
    buckets: dict[int, list[float]] = {}
    for r in rows:
        ts = ts_to_epoch(r["exchange_time"])
        boundary = int(ts) - int(ts) % 300
        buckets.setdefault(boundary, []).append(float(r["price"]))
    markets = []
    for boundary in sorted(buckets):
        prices = buckets[boundary]
        markets.append({
            "ts": float(boundary),
            "strike": prices[0],
            "settlement": prices[-1],
        })
    return markets


def load_all_data() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Load all data from QuestDB, using pickle cache if available."""
    if os.path.exists(DATA_CACHE):
        print(f"Loading cached data from {DATA_CACHE}...")
        with open(DATA_CACHE, "rb") as f:
            return pickle.load(f)

    print(f"Loading data from QuestDB for {DATE_FILTER}...")

    chainlink_raw = load_chainlink_returns()
    print(f"  Chainlink raw (for returns): {len(chainlink_raw)} prices")

    chainlink_1s = load_chainlink_1s()
    print(f"  Chainlink 1s (for loop/MC): {len(chainlink_1s)} prices")

    poly_up = load_polymarket_up()
    print(f"  Polymarket UP: {len(poly_up)} snapshots")

    markets = load_strike_settlement()
    print(f"  Markets (5-min, exchange_time): {len(markets)}")

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(DATA_CACHE, "wb") as f:
        pickle.dump((chainlink_raw, chainlink_1s, poly_up, markets), f)
    print(f"  Cached to {DATA_CACHE}")

    return chainlink_raw, chainlink_1s, poly_up, markets


# ── Precomputation ─────────────────────────────────────────────────────────────

def compute_log_returns(chainlink_data: list[dict]) -> np.ndarray:
    """Compute log returns from consecutive 1s chainlink prices."""
    prices = np.array([r["price"] for r in chainlink_data], dtype=np.float64)
    valid = (prices[:-1] > 0) & (prices[1:] > 0)
    log_rets = np.log(prices[1:] / prices[:-1])
    return log_rets[valid].astype(np.float32)


def load_or_compute_paths(log_returns: np.ndarray) -> dict[int, np.ndarray]:
    """Load precomputed paths from pickle cache, or compute and cache them."""
    if os.path.exists(PATHS_CACHE):
        print(f"Loading cached paths from {PATHS_CACHE}...")
        with open(PATHS_CACHE, "rb") as f:
            return pickle.load(f)

    paths = precompute_paths(log_returns)

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(PATHS_CACHE, "wb") as f:
        pickle.dump(paths, f)
    print(f"  Cached to {PATHS_CACHE}")

    return paths


def precompute_paths(log_returns: np.ndarray) -> dict[int, np.ndarray]:
    """Precompute MC_PATHS random walk paths for each step count MIN_STEPS..MAX_STEPS.

    paths[s] = array of shape (MC_PATHS,) containing cumulative log returns
    for walks of length s, where each step is a randomly sampled log return.
    """
    n_steps = MAX_STEPS - MIN_STEPS + 1
    print(f"Precomputing {n_steps} step counts ({MIN_STEPS}..{MAX_STEPS}) x {MC_PATHS:,} paths...")
    t0 = time.perf_counter()
    paths = {}
    n_returns = len(log_returns)
    for s in range(MIN_STEPS, MAX_STEPS + 1):
        indices = np.random.randint(0, n_returns, size=(MC_PATHS, s))
        sampled = log_returns[indices]  # (MC_PATHS, s)
        paths[s] = sampled.sum(axis=1)  # (MC_PATHS,) float32
    elapsed = time.perf_counter() - t0
    mem_mb = sum(a.nbytes for a in paths.values()) / 1e6
    print(f"  Done in {elapsed:.1f}s, {mem_mb:.0f} MB")
    return paths


# ── MC Fair Price ──────────────────────────────────────────────────────────────

def mc_fair_price(current_price: float, strike: float, steps_left: int,
                  paths: dict[int, np.ndarray]) -> float:
    """Compute MC fair price for UP outcome using precomputed paths."""
    if steps_left < MIN_STEPS:
        return 1.0 if current_price >= strike else 0.0
    if steps_left > MAX_STEPS:
        steps_left = MAX_STEPS
    all_paths = paths[steps_left]
    idx = np.random.randint(0, len(all_paths), size=MC_SAMPLE)
    terminal = current_price * np.exp(all_paths[idx])
    return float(np.mean(terminal >= strike))


# ── Backtest Engine ────────────────────────────────────────────────────────────

def run_backtest():
    # 1. Load data (from cache or DB)
    chainlink_raw, chainlink_1s, poly_up, markets = load_all_data()

    if not chainlink_1s:
        print("No chainlink data found!")
        return
    if len(markets) < 2:
        print("Not enough market data!")
        return

    # 2. Compute log returns from raw chainlink data and load/compute MC paths
    log_returns = compute_log_returns(chainlink_raw)
    print(f"  Log returns: {len(log_returns)}")
    if len(log_returns) < 10:
        print("Not enough log returns for MC!")
        return

    paths = load_or_compute_paths(log_returns)

    # Build lookup: chainlink 1s ts -> price (sorted by ts)
    cl_by_ts = {r["ts"]: r["price"] for r in chainlink_1s}
    cl_times = sorted(cl_by_ts.keys())

    # Build polymarket snapshots sorted by ts
    poly_up.sort(key=lambda r: r["ts"])
    poly_times = np.array([r["ts"] for r in poly_up])

    # ── Market loop ────────────────────────────────────────────────────────
    DEBUG = True  # verbose logs, exit after first market
    from datetime import datetime, timezone as tz

    def _ts(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=tz.utc).strftime("%H:%M:%S")

    trades = []
    total_markets = 0

    for mkt_idx in range(len(markets)):
        mkt = markets[mkt_idx]
        market_start = mkt["ts"]
        strike = mkt["strike"]
        settlement = mkt["settlement"]
        expires = market_start + 300
        resolution = "UP" if settlement >= strike else "DOWN"

        # find chainlink prices within tradeable window (skip first/last 10s)
        trade_start = market_start + MARKET_START_DELAY
        trade_end = expires - MARKET_STOP_BEFORE
        market_cl = [t for t in cl_times if trade_start <= t < trade_end]
        if len(market_cl) < 2:
            if DEBUG:
                print(f"[SKIP] Market {datetime.fromtimestamp(market_start, tz=tz.utc)} — {len(market_cl)} cl ticks")
            continue

        total_markets += 1
        if DEBUG:
            print(f"\n{'='*80}")
            print(f"MARKET #{total_markets}: {datetime.fromtimestamp(market_start, tz=tz.utc)}")
            print(f"  Strike: {strike:.2f}  Settlement: {settlement:.2f}  Resolution: {resolution}")
            print(f"  Window: {_ts(trade_start)} .. {_ts(trade_end)}  ({len(market_cl)} cl ticks)")
            p_s = np.searchsorted(poly_times, trade_start, side="left")
            p_e = np.searchsorted(poly_times, trade_end, side="left")
            print(f"  Polymarket snaps in window: {p_e - p_s}")
            print(f"{'='*80}")

        prev_bid_order = None
        prev_ask_order = None
        market_trade_count = 0

        for i, ct in enumerate(market_cl):
            current_price = cl_by_ts[ct]
            steps_left = int(expires - ct)
            if steps_left <= 0:
                continue

            fair_up = mc_fair_price(current_price, strike, steps_left, paths)

            new_bid = round(fair_up - EDGE, 4)
            new_ask = round(fair_up + EDGE, 4)
            order_active_ts = ct + ORDER_DELAY_MS / 1000.0

            if DEBUG:
                pb = f"{prev_bid_order['price']:.4f}" if prev_bid_order else "None"
                pa = f"{prev_ask_order['price']:.4f}" if prev_ask_order else "None"
                print(f"\n  [{_ts(ct)}] BTC={current_price:.2f}  steps={steps_left}  "
                      f"fair={fair_up:.4f}  new_bid={new_bid:.4f}  new_ask={new_ask:.4f}  "
                      f"prev_bid={pb}  prev_ask={pa}")

            next_ct = market_cl[i + 1] if i + 1 < len(market_cl) else expires

            p_start = np.searchsorted(poly_times, ct, side="left")
            p_end = np.searchsorted(poly_times, next_ct, side="left")

            if DEBUG:
                print(f"         poly snaps [{_ts(ct)}..{_ts(next_ct)}): {p_end - p_start}")

            for pi in range(p_start, p_end):
                snap = poly_up[pi]
                snap_ts = snap["ts"]
                snap_ask = snap["ask"]
                snap_bid = snap["bid"]

                if snap_ask is None and snap_bid is None:
                    continue

                phase = "PRE" if snap_ts < order_active_ts else "ACT"
                if DEBUG:
                    sa = f"{snap_ask:.4f}" if snap_ask is not None else "None  "
                    sb = f"{snap_bid:.4f}" if snap_bid is not None else "None  "
                    print(f"         {_ts(snap_ts)}  poly_ask={sa}  poly_bid={sb}  [{phase}]", end="")

                if snap_ts < order_active_ts:
                    if prev_bid_order is not None and snap_ask is not None:
                        if snap_ask <= prev_bid_order["price"]:
                            if DEBUG:
                                print(f"  >>> BUY FILL prev_bid={prev_bid_order['price']:.4f} >= poly_ask={snap_ask:.4f}", end="")
                            trades.append({
                                "timestamp": snap_ts,
                                "market_start": market_start,
                                "side": "BUY",
                                "price": prev_bid_order["price"],
                                "fair_price": prev_bid_order["fair"],
                                "strike": strike,
                                "settlement": settlement,
                                "resolution": resolution,
                                "profit": (1.0 if resolution == "UP" else 0.0) - prev_bid_order["price"],
                            })
                            market_trade_count += 1
                            prev_bid_order = None
                    if prev_ask_order is not None and snap_bid is not None:
                        if snap_bid >= prev_ask_order["price"]:
                            if DEBUG:
                                print(f"  >>> SELL FILL prev_ask={prev_ask_order['price']:.4f} <= poly_bid={snap_bid:.4f}", end="")
                            trades.append({
                                "timestamp": snap_ts,
                                "market_start": market_start,
                                "side": "SELL",
                                "price": prev_ask_order["price"],
                                "fair_price": prev_ask_order["fair"],
                                "strike": strike,
                                "settlement": settlement,
                                "resolution": resolution,
                                "profit": prev_ask_order["price"] - (1.0 if resolution == "UP" else 0.0),
                            })
                            market_trade_count += 1
                            prev_ask_order = None
                else:
                    if snap_ask is not None and 0 < new_bid < 1:
                        if snap_ask <= new_bid:
                            if DEBUG:
                                print(f"  >>> BUY FILL new_bid={new_bid:.4f} >= poly_ask={snap_ask:.4f}", end="")
                            trades.append({
                                "timestamp": snap_ts,
                                "market_start": market_start,
                                "side": "BUY",
                                "price": new_bid,
                                "fair_price": fair_up,
                                "strike": strike,
                                "settlement": settlement,
                                "resolution": resolution,
                                "profit": (1.0 if resolution == "UP" else 0.0) - new_bid,
                            })
                            market_trade_count += 1
                            new_bid = -1
                    if snap_bid is not None and 0 < new_ask < 1:
                        if snap_bid >= new_ask:
                            if DEBUG:
                                print(f"  >>> SELL FILL new_ask={new_ask:.4f} <= poly_bid={snap_bid:.4f}", end="")
                            trades.append({
                                "timestamp": snap_ts,
                                "market_start": market_start,
                                "side": "SELL",
                                "price": new_ask,
                                "fair_price": fair_up,
                                "strike": strike,
                                "settlement": settlement,
                                "resolution": resolution,
                                "profit": new_ask - (1.0 if resolution == "UP" else 0.0),
                            })
                            market_trade_count += 1
                            new_ask = 2

                if DEBUG:
                    print()  # newline after snap line

            # Set orders for next iteration
            if new_bid > 0:
                prev_bid_order = {"price": new_bid, "fair": fair_up}
            else:
                prev_bid_order = None
            if new_ask < 1:
                prev_ask_order = {"price": new_ask, "fair": fair_up}
            else:
                prev_ask_order = None

        if DEBUG:
            print(f"\n  Market #{total_markets} trades: {market_trade_count}")
            print(f"  Exiting after first market (DEBUG=True)")
            break

    # ── Output Statistics ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"BACKTEST RESULTS — {DATE_FILTER}")
    print(f"{'='*60}")
    print(f"Edge: {EDGE}  |  MC paths: {MC_PATHS:,}  |  MC sample: {MC_SAMPLE:,}")
    print(f"Markets replayed: {total_markets}")
    print(f"Total trades: {len(trades)}")

    if not trades:
        print("No trades generated.")
        return

    buys = [t for t in trades if t["side"] == "BUY"]
    sells = [t for t in trades if t["side"] == "SELL"]
    profits = [t["profit"] for t in trades]
    cum_pnl = sum(profits)
    wins = sum(1 for p in profits if p > 0)

    print(f"  Buys: {len(buys)}  |  Sells: {len(sells)}")
    print(f"  Win rate: {wins / len(trades) * 100:.1f}%")
    print(f"  Cumulative P&L: {cum_pnl:.4f}")
    print(f"  Avg profit/trade: {cum_pnl / len(trades):.4f}")

    # Sharpe-like ratio
    if len(profits) > 1:
        mean_p = np.mean(profits)
        std_p = np.std(profits, ddof=1)
        sharpe = mean_p / std_p if std_p > 0 else 0
        print(f"  Sharpe (per-trade): {sharpe:.3f}")

    # Max drawdown
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in profits:
        running += p
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd
    print(f"  Max drawdown: {max_dd:.4f}")

    # P&L per market
    print(f"\n{'─'*60}")
    print(f"{'Market Start':<22} {'Trades':>6} {'P&L':>10} {'Resolution':>10}")
    print(f"{'─'*60}")
    from datetime import datetime, timezone
    market_starts = sorted(set(t["market_start"] for t in trades))
    for ms in market_starts:
        mt = [t for t in trades if t["market_start"] == ms]
        mkt_pnl = sum(t["profit"] for t in mt)
        res = mt[0]["resolution"]
        dt_str = datetime.fromtimestamp(ms, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {dt_str:<20} {len(mt):>6} {mkt_pnl:>+10.4f} {res:>10}")
    print(f"{'─'*60}")
    print(f"  {'TOTAL':<20} {len(trades):>6} {cum_pnl:>+10.4f}")


if __name__ == "__main__":
    run_backtest()
