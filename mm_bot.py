"""
mm_bot.py — Live market-making bot for Polymarket BTC 5-min Up/Down markets.

Precomputes MC paths from historical Chainlink data (QuestDB), then streams
live BTC prices from Binance (BTCUSDC) and Chainlink (via Polymarket RTDS).
Chainlink is used only for strike detection. Binance provides fast price ticks;
each tick is adjusted by the rolling average Binance–Chainlink difference to
estimate the Chainlink-equivalent price, which feeds the MC fair-price engine.

Usage:
    ./venv/bin/python mm_bot.py

Requires:
    pip install py-clob-client numpy requests websocket-client

Environment variables:
    POLY_PRIVATE_KEY   — your private key (hex, with or without 0x prefix)
    POLY_FUNDER        — your funder wallet address (checksummed)
"""

import json
import os
import pickle
import signal
import threading
import time
from datetime import datetime, timezone

import numpy as np
import requests
import websocket

# ── Configuration ──────────────────────────────────────────────────────────────
QUESTDB_API = "http://localhost:9001"
DATE_FILTER_RETURNS = "2026-02-22;2d"  # historical data for log returns
MC_PATHS = 100_000
MC_SAMPLE = 10_000
EDGE = 0.02
SKEW_FACTOR = 0.01            # extra edge per share of position on that side
ORDER_SIZE = 5.0             # shares per order
MAX_POSITION = 4.0           # max abs(pos_up - pos_down) before skipping that side
DRY_RUN = False                # True = print only, no real orders
MARKET_START_DELAY = 10       # seconds into market before trading
MARKET_STOP_BEFORE = 10       # seconds before expiry to stop trading
MIN_STEPS = MARKET_STOP_BEFORE
MAX_STEPS = 300 - MARKET_START_DELAY

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CHAIN_ID = 137
RTDS_WS = "wss://ws-live-data.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdc@ticker"
PING_INTERVAL = 10
PRICE_CACHE_SECONDS = 300  # rolling window for Binance–Chainlink diff
BALANCE_SYNC_INTERVAL = 10  # seconds between exchange balance/position syncs

CACHE_DIR = "cache"
PATHS_CACHE = os.path.join(CACHE_DIR, f"paths_live_{DATE_FILTER_RETURNS}_{MC_PATHS}.pkl")


# ── QuestDB ────────────────────────────────────────────────────────────────────

def query_questdb(sql: str) -> list[dict]:
    resp = requests.get(f"{QUESTDB_API}/exec", params={"query": sql}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    cols = [c["name"] for c in data["columns"]]
    return [dict(zip(cols, row)) for row in data["dataset"]]


def load_chainlink_returns() -> list[dict]:
    sql = (
        f"SELECT exchange_time timestamp, ap_1 as price FROM orderbooks "
        f"WHERE exchange_time IN '{DATE_FILTER_RETURNS}' "
        f"AND exchange='chainlink' AND pair='BTCUSD' ORDER BY exchange_time;"
    )
    return query_questdb(sql)


# ── MC Precomputation ──────────────────────────────────────────────────────────

def compute_log_returns(rows: list[dict]) -> np.ndarray:
    prices = np.array([float(r["price"]) for r in rows], dtype=np.float64)
    valid = (prices[:-1] > 0) & (prices[1:] > 0)
    log_rets = np.log(prices[1:] / prices[:-1])
    return log_rets[valid].astype(np.float32)


def precompute_paths(log_returns: np.ndarray) -> dict[int, np.ndarray]:
    n_steps = MAX_STEPS - MIN_STEPS + 1
    print(f"[mc] Precomputing {n_steps} step counts ({MIN_STEPS}..{MAX_STEPS}) x {MC_PATHS:,} paths...")
    t0 = time.perf_counter()
    paths = {}
    n_returns = len(log_returns)
    for s in range(MIN_STEPS, MAX_STEPS + 1):
        indices = np.random.randint(0, n_returns, size=(MC_PATHS, s))
        sampled = log_returns[indices]
        paths[s] = sampled.sum(axis=1)
    elapsed = time.perf_counter() - t0
    mem_mb = sum(a.nbytes for a in paths.values()) / 1e6
    print(f"[mc] Done in {elapsed:.1f}s, {mem_mb:.0f} MB")
    return paths


def load_or_compute_paths() -> dict[int, np.ndarray]:
    if os.path.exists(PATHS_CACHE):
        print(f"[mc] Loading cached paths from {PATHS_CACHE}")
        with open(PATHS_CACHE, "rb") as f:
            return pickle.load(f)

    print("[mc] Loading chainlink data from QuestDB...")
    rows = load_chainlink_returns()
    print(f"[mc] {len(rows)} raw prices")
    log_returns = compute_log_returns(rows)
    print(f"[mc] {len(log_returns)} log returns")

    paths = precompute_paths(log_returns)
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(PATHS_CACHE, "wb") as f:
        pickle.dump(paths, f)
    print(f"[mc] Cached to {PATHS_CACHE}")
    return paths


def mc_fair_price(current_price: float, strike: float, steps_left: int,
                  paths: dict[int, np.ndarray]) -> float:
    if steps_left < MIN_STEPS:
        return 1.0 if current_price >= strike else 0.0
    if steps_left > MAX_STEPS:
        steps_left = MAX_STEPS
    all_paths = paths[steps_left]
    idx = np.random.randint(0, len(all_paths), size=MC_SAMPLE)
    terminal = current_price * np.exp(all_paths[idx])
    return float(np.mean(terminal >= strike))


# ── Polymarket helpers ─────────────────────────────────────────────────────────

def parse_json_list(raw) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip().strip('"') for x in raw]
    return [t.strip().strip('"') for t in raw.strip("[]").split(",") if t.strip()]


def get_current_5min_boundary() -> int:
    now = int(time.time())
    return now - (now % 300)


def fetch_market(slug: str) -> dict | None:
    resp = requests.get(f"{GAMMA_API}/markets/slug/{slug}", timeout=10)
    if resp.status_code != 200:
        return None
    return resp.json()


def get_creds() -> tuple[str, str]:
    key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
    funder = os.environ.get("POLY_FUNDER", "").strip()
    if not key:
        key = input("Private key (hex): ").strip()
    if not key.startswith("0x"):
        key = "0x" + key
    if not funder:
        funder = input("Funder address: ").strip()
    return key, funder


def init_clob_client():
    from py_clob_client.client import ClobClient
    key, funder = get_creds()
    print("[clob] Initialising client...")
    client = ClobClient(
        host=CLOB_API,
        chain_id=CHAIN_ID,
        key=key,
        signature_type=2,
        funder=funder,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    print("[clob] Client ready.")
    return client


def get_position(client, token_id: str) -> float:
    """Get current position (shares) for a token."""
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id, signature_type=2)
        )
        if isinstance(bal, dict):
            return float(bal.get("balance", 0)) / 1e6
    except Exception as e:
        print(f"[warn] get_position failed: {e}")
    return 0.0


def get_usdc_balance(client) -> float:
    """Get USDC cash balance."""
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2)
        )
        if isinstance(bal, dict):
            return float(bal.get("balance", 0)) / 1e6
    except Exception as e:
        print(f"[warn] get_usdc_balance failed: {e}")
    return 0.0


def cancel_all_orders(client):
    """Cancel all open orders."""
    try:
        resp = client.cancel_all()
        return resp
    except Exception as e:
        print(f"[warn] cancel_all failed: {e}")
        return None


def place_buy_order(client, token_id: str, price: float, size: float) -> str | None:
    """Place a GTC BUY limit order. Returns order ID or None."""
    from py_clob_client.clob_types import OrderArgs, OrderType
    if price <= 0.01 or price >= 0.99:
        return None
    price = round(price, 2)
    try:
        order_args = OrderArgs(token_id=token_id, price=price, size=size, side="BUY")
        signed = client.create_order(order_args)
        resp = client.post_order(signed, OrderType.GTC)
        order_id = resp.get("orderID") if isinstance(resp, dict) else None
        return order_id
    except Exception as e:
        print(f"[warn] place_buy_order failed: {e}")
        return None


def place_sell_order(client, token_id: str, price: float, size: float) -> str | None:
    """Place a GTC SELL limit order. Returns order ID or None."""
    from py_clob_client.clob_types import OrderArgs, OrderType
    if price <= 0.01 or price >= 0.99:
        return None
    price = round(price, 2)
    try:
        order_args = OrderArgs(token_id=token_id, price=price, size=size, side="SELL")
        signed = client.create_order(order_args)
        resp = client.post_order(signed, OrderType.GTC)
        order_id = resp.get("orderID") if isinstance(resp, dict) else None
        return order_id
    except Exception as e:
        print(f"[warn] place_sell_order failed: {e}")
        return None


# ── Bot State ──────────────────────────────────────────────────────────────────

class BotState:
    def __init__(self, client, paths: dict[int, np.ndarray]):
        self.client = client
        self.paths = paths

        # Market state
        self.market_boundary: int = 0
        self.expires: int = 0
        self.strike: float = 0.0
        self.slug: str = ""
        self.token_up: str = ""
        self.token_down: str = ""

        # Orderbook (updated from CLOB WS)
        self.lock = threading.Lock()
        self._asks_up: dict = {}
        self._asks_down: dict = {}
        self._bids_up: dict = {}
        self._bids_down: dict = {}
        self.best_ask_up: float | None = None
        self.best_ask_down: float | None = None
        self.best_bid_up: float | None = None
        self.best_bid_down: float | None = None
        self.clob_ws_app = None
        self.clob_connected = False
        self._loading_market = False

        # Cached boundary prices: {boundary_ts: price} for strike detection
        self._boundary_prices: dict[int, float] = {}

        # Price caches: list of (epoch_s, price) tuples, kept to PRICE_CACHE_SECONDS
        self._chainlink_cache: list[tuple[float, float]] = []
        self._binance_cache: list[tuple[float, float]] = []

        # Balance and position tracking
        self.cash: float = 0.0  # USDC balance, fetched on market rotation
        self.pos_up: float = 0.0
        self.pos_down: float = 0.0
        self.avg_price_up: float = 0.0   # volume-weighted avg buy price
        self.avg_price_down: float = 0.0

        # Orderbook version counter — incremented on every snapshot/delta
        self._book_version: int = 0

        # Active orders: track what we believe is live on Polymarket
        # Each entry: {"order_id": str, "price": float, "size": float,
        #              "fair_price": float, "book_version": int} or None
        self._order_buy_up: dict | None = None
        self._order_sell_up: dict | None = None
        self._order_buy_down: dict | None = None
        self._order_sell_down: dict | None = None

        # Order history: maps order_id -> entry for active slot clearing on fills
        # Each entry: {"order_id": str, "price": float, "size": float, "side": str, "token": str, "label": str}
        self._order_history: dict[str, dict] = {}

        # Trade IDs already processed, to avoid double-counting
        self._seen_trade_ids: set[str] = set()

        # Trade log
        self.trades: list[dict] = []
        self.total_pnl: float = 0.0

        # Periodic balance/position sync
        self._last_balance_sync: float = 0.0

    def load_market(self, boundary: int | None = None) -> bool:
        """Load a 5-min market. Uses server clock if boundary not provided."""
        if boundary is None:
            boundary = get_current_5min_boundary()
        if boundary == self.market_boundary:
            return True  # already loaded

        slug = f"btc-updown-5m-{boundary}"
        print(f"\n[market] Loading {slug}...")

        market = None
        for _ in range(5):
            market = fetch_market(slug)
            if market is not None:
                break
            time.sleep(2)

        if market is None:
            print(f"[market] Could not fetch {slug}")
            return False

        token_ids = parse_json_list(market.get("clobTokenIds", "[]"))
        outcomes = parse_json_list(market.get("outcomes", "[]"))

        if len(token_ids) < 2:
            print(f"[market] Expected 2 tokens, got {len(token_ids)}")
            return False

        # outcomes[0] = Up, outcomes[1] = Down (by convention)
        up_idx = 0
        for i, name in enumerate(outcomes):
            if "up" in name.lower():
                up_idx = i
                break

        down_idx = 1 - up_idx

        self.market_boundary = boundary
        self.expires = boundary + 300
        self.slug = slug
        old_token_up = self.token_up
        old_token_down = self.token_down
        self.token_up = token_ids[up_idx]
        self.token_down = token_ids[down_idx]
        self.strike = 0.0  # will be set from chainlink

        # reset orderbooks and tracked orders
        with self.lock:
            self._asks_up = {}
            self._asks_down = {}
            self._bids_up = {}
            self._bids_down = {}
            self.best_ask_up = None
            self.best_ask_down = None
            self.best_bid_up = None
            self.best_bid_down = None
        self._order_buy_up = None
        self._order_sell_up = None
        self._order_buy_down = None
        self._order_sell_down = None
        self._order_history = {}
        self._seen_trade_ids = set()
        self.avg_price_up = 0.0
        self.avg_price_down = 0.0

        # swap CLOB WS subscription
        old_ids = [t for t in [old_token_up, old_token_down] if t]
        new_ids = [self.token_up, self.token_down]
        self._swap_clob_subscription(old_ids, new_ids)

        # REST fallback: fetch orderbooks in case WS doesn't send a snapshot
        self._fetch_books_rest()

        # snapshot balance and positions
        if not DRY_RUN:
            self.cash = get_usdc_balance(self.client)
            self.pos_up = get_position(self.client, self.token_up)
            self.pos_down = get_position(self.client, self.token_down)

        title = market.get("title") or slug
        print(f"[market] {title}")
        print(f"[market] UP token:   {self.token_up[:12]}...")
        print(f"[market] DOWN token: {self.token_down[:12]}...")
        print(f"[market] Cash: ${self.cash:.2f}  UP: {self.pos_up:.2f}  DOWN: {self.pos_down:.2f}")
        return True

    def _do_load_market(self, boundary: int):
        """Wrapper for load_market that runs on a background thread."""
        try:
            self.load_market(boundary)
        finally:
            self._loading_market = False

    def _swap_clob_subscription(self, old_ids: list[str], new_ids: list[str]):
        """Unsubscribe old tokens and subscribe new ones on the CLOB WS."""
        ws = self.clob_ws_app
        if ws is None or not self.clob_connected:
            return
        try:
            if old_ids:
                ws.send(json.dumps({"assets_ids": old_ids, "operation": "unsubscribe"}))
            ws.send(json.dumps({"assets_ids": new_ids, "type": "market"}))
        except Exception:
            pass

    def _fetch_books_rest(self):
        """Fetch orderbooks via REST API as fallback for CLOB WS."""
        for token_id, label in [(self.token_up, "UP"), (self.token_down, "DOWN")]:
            if not token_id:
                continue
            try:
                resp = requests.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=5)
                if resp.status_code == 200:
                    book = resp.json()
                    self.set_book_snapshot(token_id, book.get("asks", []), book.get("bids", []))
            except Exception:
                pass

    def set_book_snapshot(self, asset_id: str, asks: list, bids: list | None = None):
        """Set full orderbook from snapshot. Called from CLOB WS thread."""
        ask_dict = {e["price"]: e["size"] for e in asks}
        bid_dict = {e["price"]: e["size"] for e in (bids or [])}
        with self.lock:
            self._book_version += 1
            if asset_id == self.token_up:
                self._asks_up = ask_dict
                self._bids_up = bid_dict
                self.best_ask_up = min((float(p) for p in ask_dict), default=None) if ask_dict else None
                self.best_bid_up = max((float(p) for p in bid_dict), default=None) if bid_dict else None
            elif asset_id == self.token_down:
                self._asks_down = ask_dict
                self._bids_down = bid_dict
                self.best_ask_down = min((float(p) for p in ask_dict), default=None) if ask_dict else None
                self.best_bid_down = max((float(p) for p in bid_dict), default=None) if bid_dict else None

    def apply_book_delta(self, asset_id: str, price: str, size: str, side: str):
        """Apply a price_change delta. Called from CLOB WS thread."""
        with self.lock:
            self._book_version += 1
            if side == "SELL":
                if asset_id == self.token_up:
                    book = self._asks_up
                elif asset_id == self.token_down:
                    book = self._asks_down
                else:
                    return
            else:
                if asset_id == self.token_up:
                    book = self._bids_up
                elif asset_id == self.token_down:
                    book = self._bids_down
                else:
                    return

            if float(size) == 0:
                book.pop(price, None)
            else:
                book[price] = size

            # Update best prices
            if asset_id == self.token_up:
                self.best_ask_up = min((float(p) for p in self._asks_up), default=None) if self._asks_up else None
                self.best_bid_up = max((float(p) for p in self._bids_up), default=None) if self._bids_up else None
            else:
                self.best_ask_down = min((float(p) for p in self._asks_down), default=None) if self._asks_down else None
                self.best_bid_down = max((float(p) for p in self._bids_down), default=None) if self._bids_down else None

    def _trim_cache(self, cache: list[tuple[float, float]], now: float):
        """Remove entries older than PRICE_CACHE_SECONDS from a price cache."""
        cutoff = now - PRICE_CACHE_SECONDS
        while cache and cache[0][0] < cutoff:
            cache.pop(0)

    def compute_avg_diff(self) -> float | None:
        """Compute average (Chainlink - Binance) difference over the last PRICE_CACHE_SECONDS.

        Both caches are sampled at 1-second intervals and aligned by integer second.
        Returns None if insufficient overlapping data.
        """
        with self.lock:
            cl = list(self._chainlink_cache)
            bn = list(self._binance_cache)

        if len(cl) < 2 or len(bn) < 2:
            return None

        # Build 1-second sampled dicts keyed by integer second
        # Chainlink is already ~1s; for both, take the last price at each second
        cl_by_sec: dict[int, float] = {}
        for ts, price in cl:
            cl_by_sec[int(ts)] = price

        bn_by_sec: dict[int, float] = {}
        for ts, price in bn:
            bn_by_sec[int(ts)] = price

        # Find overlapping seconds
        common_secs = sorted(set(cl_by_sec) & set(bn_by_sec))
        if len(common_secs) < 2:
            return None

        diffs = [cl_by_sec[s] - bn_by_sec[s] for s in common_secs]
        return sum(diffs) / len(diffs)

    def on_chainlink_price(self, price: float, oracle_ts_ms: int, snapshot: bool = False):
        """Called on every chainlink price update from RTDS.
        Handles strike detection, market rotation, and caches prices for diff calculation.
        No MC or order placement — that happens on Binance ticks."""
        oracle_s = int(oracle_ts_ms / 1000)
        oracle_ts_s = oracle_ts_ms / 1000.0
        ts_str = datetime.fromtimestamp(oracle_ts_s, tz=timezone.utc).strftime("%H:%M:%S.") + f"{oracle_ts_ms % 1000:03d}"

        # Cache chainlink price for diff calculation
        now = time.time()
        with self.lock:
            self._chainlink_cache.append((oracle_ts_s, price))
            self._trim_cache(self._chainlink_cache, now)

        # Derive which 5-min boundary this oracle tick belongs to
        oracle_boundary = oracle_s - (oracle_s % 300)

        # Cache any price that lands exactly on a 5-min boundary
        if oracle_s == oracle_boundary:
            self._boundary_prices[oracle_boundary] = price
            print(f"[chainlink] Cached boundary price: {oracle_boundary} -> {price:.2f}")
            # keep only last 5 boundaries
            if len(self._boundary_prices) > 5:
                oldest = min(self._boundary_prices)
                del self._boundary_prices[oldest]

        # Check if we need to rotate markets (based on oracle time)
        # Run on a separate thread so we don't block the RTDS callback
        if oracle_boundary != self.market_boundary:
            if not self._loading_market:
                self._loading_market = True
                threading.Thread(target=self._do_load_market, args=(oracle_boundary,), daemon=True).start()

        # Set strike from cache or live tick
        if self.strike == 0.0 and self.market_boundary > 0:
            # Check cache first (boundary price may have arrived before market loaded)
            if self.market_boundary in self._boundary_prices:
                self.strike = self._boundary_prices[self.market_boundary]
                print(f"[strike] Set strike={self.strike:.2f} from cached boundary price (boundary={self.market_boundary})")
            elif oracle_s == self.market_boundary:
                self.strike = price
                print(f"[strike] Set strike={price:.2f} from live oracle_ts={ts_str} (boundary={self.market_boundary})")
            else:
                seconds_into_market = oracle_s - self.market_boundary
                if seconds_into_market < 0:
                    return
                if seconds_into_market <= MARKET_START_DELAY:
                    if not snapshot:
                        print(f"[strike] Waiting for boundary match. oracle={ts_str} ({seconds_into_market}s into market)")

    def on_binance_price(self, price: float, trade_ts_ms: int):
        """Called on every Binance BTCUSDC trade. Adjusts price by avg Chainlink–Binance
        diff and runs MC fair pricing + order placement."""
        now = time.time()
        trade_ts_s = trade_ts_ms / 1000.0

        # Cache binance price for diff calculation
        with self.lock:
            self._binance_cache.append((trade_ts_s, price))
            self._trim_cache(self._binance_cache, now)

        # Need strike and market loaded
        if self.strike == 0.0 or self.market_boundary == 0:
            return

        # Compute adjusted price = binance + avg(chainlink - binance)
        avg_diff = self.compute_avg_diff()
        if avg_diff is None:
            return
        adjusted_price = price + avg_diff

        # Use server time for steps_left (Binance trade timestamps are exchange time)
        now_s = int(now)
        steps_left = self.expires - now_s
        if steps_left > MAX_STEPS or steps_left < MIN_STEPS:
            return

        ts_str = datetime.fromtimestamp(trade_ts_s, tz=timezone.utc).strftime("%H:%M:%S.") + f"{trade_ts_ms % 1000:03d}"

        # MC fair price using the adjusted (Chainlink-equivalent) price
        fair_up = mc_fair_price(adjusted_price, self.strike, steps_left, self.paths)
        fair_down = 1.0 - fair_up

        # Balance skew: widen the bid on the side where we hold position
        # to discourage buying more; ask stays tight to offload
        skew_up = SKEW_FACTOR * self.pos_up
        skew_down = SKEW_FACTOR * self.pos_down

        # Our desired bid/ask for each side
        our_bid_up = round(fair_up - EDGE - skew_up, 2)
        our_ask_up = round(fair_up + EDGE, 2)
        our_bid_down = round(fair_down - EDGE - skew_down, 2)
        our_ask_down = round(fair_down + EDGE, 2)

        # Snapshot orderbook best prices
        with self.lock:
            mkt_ask_up = self.best_ask_up
            mkt_bid_up = self.best_bid_up
            mkt_ask_down = self.best_ask_down
            mkt_bid_down = self.best_bid_down

        # Ensure all orders stay maker (never cross the spread)
        # Track which orders got throttled to best bid/ask
        throttled_buy_up = throttled_buy_down = False
        throttled_sell_up = throttled_sell_down = False
        # BUY: if our bid >= market ask, cap at market ask - 0.01
        if mkt_ask_up is not None and our_bid_up >= mkt_ask_up:
            our_bid_up = round(mkt_ask_up - 0.01, 2)
            throttled_buy_up = True
        if mkt_ask_down is not None and our_bid_down >= mkt_ask_down:
            our_bid_down = round(mkt_ask_down - 0.01, 2)
            throttled_buy_down = True
        # SELL: if our ask <= market bid, raise to market bid + 0.01
        if mkt_bid_up is not None and our_ask_up <= mkt_bid_up:
            our_ask_up = round(mkt_bid_up + 0.01, 2)
            throttled_sell_up = True
        if mkt_bid_down is not None and our_ask_down <= mkt_bid_down:
            our_ask_down = round(mkt_bid_down + 0.01, 2)
            throttled_sell_down = True

        def _fmt(v): return f"{v:.2f}" if v is not None else "None"

        print(f"[tick] {ts_str}  BN={price:.2f}  adj={adjusted_price:.2f}  diff={avg_diff:+.2f}  "
              f"strike={self.strike:.2f}  steps={steps_left}  "
              f"fair_up={fair_up:.4f}  fair_dn={fair_down:.4f}  "
              f"UP[bid={our_bid_up:.2f} ask={our_ask_up:.2f} mkt={_fmt(mkt_bid_up)}/{_fmt(mkt_ask_up)}]  "
              f"DN[bid={our_bid_down:.2f} ask={our_ask_down:.2f} mkt={_fmt(mkt_bid_down)}/{_fmt(mkt_ask_down)}]")

        if DRY_RUN:
            return

        # Check for new fills via get_trades
        self._check_trades()

        # Periodically sync balance/positions from exchange to correct drift
        self._sync_balance_if_due()

        # Determine desired orders (None = no order wanted)
        # Net position: positive = long UP, negative = long DOWN
        net_pos = self.pos_up - self.pos_down

        # If we hold UP tokens, stop buying DOWN (selling DOWN ≈ buying UP, redundant).
        # Instead, sell UP to exit. Vice versa for DOWN.
        want_buy_up = our_bid_up if our_bid_up > 0.01 and net_pos < MAX_POSITION else None
        want_sell_up = our_ask_up if our_ask_up < 0.99 and self.pos_up > 0 else None
        want_buy_down = our_bid_down if our_bid_down > 0.01 and net_pos > -MAX_POSITION else None
        want_sell_down = our_ask_down if our_ask_down < 0.99 and self.pos_down > 0 else None

        if self.pos_up > 0:
            want_buy_down = None   # don't bid opposite side; sell UP instead
        if self.pos_down > 0:
            want_buy_up = None     # don't bid opposite side; sell DOWN instead

        sell_size_up = min(ORDER_SIZE, self.pos_up) if want_sell_up else 0
        sell_size_down = min(ORDER_SIZE, self.pos_down) if want_sell_down else 0

        # Check cash for BUY orders — skip if insufficient
        # Cost of a BUY = price * size
        cost_buy_up = (want_buy_up * ORDER_SIZE) if want_buy_up else 0
        cost_buy_down = (want_buy_down * ORDER_SIZE) if want_buy_down else 0
        available = self.cash
        if cost_buy_up + cost_buy_down > available:
            # Not enough for both — try one at a time
            if cost_buy_up > available:
                want_buy_up = None
                cost_buy_up = 0
            if cost_buy_down > available - cost_buy_up:
                want_buy_down = None
            if want_buy_up is None and want_buy_down is None:
                print(f"[cash] Insufficient balance ${available:.2f} for BUY orders, skipping")

        # Update each order slot: cancel+replace only if price changed by >= 0.01
        self._update_order("BUY  UP  ", self.token_up, "BUY",
                           want_buy_up, ORDER_SIZE, "_order_buy_up", fair_up, throttled_buy_up)
        self._update_order("SELL UP  ", self.token_up, "SELL",
                           want_sell_up, sell_size_up, "_order_sell_up", fair_up, throttled_sell_up)
        self._update_order("BUY  DOWN", self.token_down, "BUY",
                           want_buy_down, ORDER_SIZE, "_order_buy_down", fair_down, throttled_buy_down)
        self._update_order("SELL DOWN", self.token_down, "SELL",
                           want_sell_down, sell_size_down, "_order_sell_down", fair_down, throttled_sell_down)

    def _sync_balance_if_due(self):
        """Re-fetch cash and positions from exchange every BALANCE_SYNC_INTERVAL seconds.
        Corrects drift from missed fills, failed order placements that went through, etc."""
        now = time.time()
        if now - self._last_balance_sync < BALANCE_SYNC_INTERVAL:
            return
        self._last_balance_sync = now

        try:
            cash = get_usdc_balance(self.client)
            pos_up = get_position(self.client, self.token_up)
            pos_down = get_position(self.client, self.token_down)
        except Exception as e:
            print(f"[warn] balance sync failed: {e}")
            return

        # Log if there's meaningful drift
        if (abs(cash - self.cash) > 0.01 or
                abs(pos_up - self.pos_up) > 0.01 or
                abs(pos_down - self.pos_down) > 0.01):
            print(f"[sync] cash ${self.cash:.2f}→${cash:.2f}  "
                  f"pos_up {self.pos_up:.2f}→{pos_up:.2f}  "
                  f"pos_down {self.pos_down:.2f}→{pos_down:.2f}")

        self.cash = cash
        self.pos_up = pos_up
        self.pos_down = pos_down

    def _check_trades(self):
        """Fetch recent trades from Polymarket and process new fills.

        Uses get_trades(after=market_boundary) to discover all executions for
        the current market. Deduplicates via _seen_trade_ids. Updates avg_price
        on BUY fills. Clears active order slots when their order gets filled.
        Cash and position are authoritative from _sync_balance_if_due — this
        method focuses on logging and avg_price tracking.
        """
        from py_clob_client.clob_types import TradeParams
        if self.market_boundary == 0:
            return

        try:
            trades = self.client.get_trades(
                TradeParams(after=self.market_boundary),
            )
        except Exception as e:
            print(f"[warn] get_trades failed: {e}")
            return

        if not isinstance(trades, list):
            return

        for trade in trades:
            if not isinstance(trade, dict):
                continue
            trade_id = trade.get("id", "")
            if trade_id in self._seen_trade_ids:
                continue
            self._seen_trade_ids.add(trade_id)

            print(f"[get_trades] raw: {trade}")

            asset_id = trade.get("asset_id", "")
            side = trade.get("side", "").upper()
            price = float(trade.get("price", 0))
            size = float(trade.get("size", 0))
            # Our orders are limit (maker) — check both fields to match
            order_id = trade.get("maker_order_id", "") or trade.get("taker_order_id", "")

            # Determine which side this trade is for
            if asset_id == self.token_up:
                is_up = True
            elif asset_id == self.token_down:
                is_up = False
            else:
                continue  # not our market

            label = f"{'BUY' if side == 'BUY' else 'SELL'} {'UP' if is_up else 'DOWN'}"

            # Update avg price on BUY fills
            if side == "BUY":
                if is_up and self.pos_up + size > 0:
                    self.avg_price_up = (self.avg_price_up * self.pos_up + price * size) / (self.pos_up + size)
                elif not is_up and self.pos_down + size > 0:
                    self.avg_price_down = (self.avg_price_down * self.pos_down + price * size) / (self.pos_down + size)

            print(f"[trade] {label} @ {price:.2f}  size={size}  id={trade_id[:8]}…  "
                  f"order={order_id[:8]}…")

            # Clear active order slot if this trade's order is still the active one
            for attr in ("_order_buy_up", "_order_sell_up", "_order_buy_down", "_order_sell_down"):
                cur = getattr(self, attr)
                if cur is not None and cur["order_id"] == order_id:
                    setattr(self, attr, None)

    def _verify_order_live(self, order: dict, label: str) -> bool:
        """Check if an order is still live on the exchange via get_order.
        Returns True if live, False if already filled/cancelled/gone."""
        try:
            resp = self.client.get_order(order["order_id"])
            status = resp.get("status", "") if isinstance(resp, dict) else ""
            if status in ("LIVE", "OPEN", "live", "open"):
                return True
            print(f"[verify] {label} id={order['order_id'][:8]}… status={status} — already gone")
            return False
        except Exception as e:
            print(f"[warn] get_order {label} failed: {e} — assuming gone")
            return False

    def _cancel_order(self, order: dict, label: str, side: str):
        """Cancel a single tracked order. Returns locked cash for BUY orders immediately
        so replacement orders can use it. Balance sync corrects any drift."""
        try:
            self.client.cancel(order_id=order["order_id"])
            print(f"[cancel] {label} @ {order['price']:.2f}  id={order['order_id']}")
        except Exception as e:
            print(f"[warn] cancel {label} failed: {e}")
        # Return locked cash immediately
        if side == "BUY":
            self.cash += order["price"] * order["size"]

    def _update_order(self, label: str, token_id: str, side: str,
                      want_price: float | None, want_size: float, attr: str,
                      fair_price: float = 0.0, throttled: bool = False):
        """Cancel+replace an order slot only if the price changed by >= 0.01.

        If the price hasn't changed (throttled to same best bid/ask) but the
        underlying fair price moved by > 0.01, the order is stale — cancel it
        and wait for a fresh orderbook update before re-placing.

        When in throttled mode (our model price is far from market, so we're
        at best bid/ask), verify the order is still live via get_order before
        cancelling, in case it was already filled.

        Cash locking for BUY orders happens at placement; cash return for
        cancelled/filled orders is handled by _check_all_order_fills.
        attr is the name of the self._order_* attribute for this slot.
        """
        current = getattr(self, attr)
        book_ver = self._book_version

        # No order wanted — cancel if one exists
        if want_price is None:
            if current is not None:
                if throttled and not self._verify_order_live(current, label):
                    setattr(self, attr, None)
                    return
                self._cancel_order(current, label, side)
                setattr(self, attr, None)
            return

        # Order wanted — check if current is close enough to skip
        if current is not None:
            if abs(current["price"] - want_price) < 0.01:
                # Price didn't move — but check if fair price shifted significantly
                if abs(current.get("fair_price", 0) - fair_price) > 0.01:
                    # Fair price moved but we're throttled to the same book level.
                    # In throttled mode, verify order is still live before cancelling.
                    if throttled and not self._verify_order_live(current, label):
                        setattr(self, attr, None)
                        return
                    # Cancel the stale order; only re-place once the book updates.
                    if book_ver == current.get("book_version", -1):
                        print(f"[stale] {label}: fair moved "
                              f"{current.get('fair_price', 0):.3f}→{fair_price:.3f} "
                              f"but book unchanged — cancel & wait")
                        self._cancel_order(current, label, side)
                        setattr(self, attr, None)
                        return
                    # Book has updated since placement — fall through to re-place
                    self._cancel_order(current, label, side)
                    setattr(self, attr, None)
                else:
                    return  # price hasn't moved enough, keep existing order
            else:
                # Price moved — cancel old order
                if throttled and not self._verify_order_live(current, label):
                    setattr(self, attr, None)
                else:
                    self._cancel_order(current, label, side)
                    setattr(self, attr, None)

        # Check cash for BUY orders
        if side == "BUY":
            cost = want_price * want_size
            if cost > self.cash:
                print(f"[cash] Skip {label}: need ${cost:.2f} but only ${self.cash:.2f} available")
                return

        # Place new order
        if side == "BUY":
            oid = place_buy_order(self.client, token_id, want_price, want_size)
        else:
            oid = place_sell_order(self.client, token_id, want_price, want_size)

        if oid:
            order_entry = {"order_id": oid, "price": want_price, "size": want_size,
                           "fair_price": fair_price, "book_version": book_ver}
            setattr(self, attr, order_entry)
            # Register in order history for active slot clearing on fills
            self._order_history[oid] = {
                "order_id": oid, "price": want_price, "size": want_size,
                "side": side, "token": token_id, "label": label,
            }
            # Lock cash for BUY orders
            if side == "BUY":
                self.cash -= want_price * want_size
            print(f"[order] {label} @ {want_price:.2f}  size={want_size}  id={oid}  cash=${self.cash:.2f}")
        else:
            setattr(self, attr, None)


# ── RTDS WebSocket (Chainlink — strike detection + diff cache) ────────────────

PRICE_STALE_TIMEOUT = 4  # seconds without update → force reconnect

def run_rtds(bot: BotState):
    """Connect to Polymarket RTDS and stream chainlink prices to the bot.
    Used for strike detection and Binance–Chainlink diff calculation only.
    Reconnects if no update received for PRICE_STALE_TIMEOUT seconds."""
    last_update = [time.time()]

    def on_open(ws):
        last_update[0] = time.time()
        print("[rtds] Connected")
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "{\"symbol\":\"btc/usd\"}",
            }],
        }))

    def on_message(ws, message):
        if not message:
            return
        try:
            data = json.loads(message)
        except (json.JSONDecodeError, ValueError):
            return

        if "crypto_prices" not in data.get("topic", ""):
            return

        payload = data.get("payload", {})
        if data.get("type") == "subscribe":
            # snapshot — strike detection only
            arr = payload.get("data", [])
            for item in arr:
                price = float(item.get("value", 0))
                ts_ms = int(item.get("timestamp", 0))
                if price > 0:
                    last_update[0] = time.time()
                    bot.on_chainlink_price(price, ts_ms, snapshot=True)
        else:
            # live update — strike detection + diff cache (no MC)
            price = float(payload.get("value", 0))
            ts_ms = int(payload.get("timestamp", 0))
            if price > 0:
                last_update[0] = time.time()
                bot.on_chainlink_price(price, ts_ms, snapshot=False)

    def stale_watchdog(ws_ref):
        """Close the WS if no price update for PRICE_STALE_TIMEOUT seconds."""
        while True:
            time.sleep(PRICE_STALE_TIMEOUT)
            if time.time() - last_update[0] > PRICE_STALE_TIMEOUT:
                print(f"[rtds] No update for {PRICE_STALE_TIMEOUT}s — reconnecting...")
                try:
                    ws_ref[0].close()
                except Exception:
                    pass
                break

    while not shutdown_event.is_set():
        ws_ref = [None]
        ws_app = websocket.WebSocketApp(
            RTDS_WS,
            on_open=on_open,
            on_message=on_message,
            on_error=lambda ws, e: None,
            on_close=lambda ws, c, m: None,
        )
        ws_ref[0] = ws_app
        threading.Thread(target=stale_watchdog, args=(ws_ref,), daemon=True).start()
        ws_app.run_forever()
        if not shutdown_event.is_set():
            time.sleep(2)


# ── Binance WebSocket (BTCUSDC aggTrade) ──────────────────────────────────────

BINANCE_STALE_TIMEOUT = 5  # seconds without Binance update → force reconnect


def run_binance(bot: BotState):
    """Stream BTCUSDC aggTrades from Binance. Each trade triggers MC + order logic."""
    last_update = [time.time()]

    def on_open(ws):
        last_update[0] = time.time()
        print("[binance] Connected")

    def on_message(ws, message):
        if not message:
            return
        try:
            data = json.loads(message)
        except (json.JSONDecodeError, ValueError):
            return
        bid = float(data.get("b", 0))
        ask = float(data.get("a", 0))
        event_ts = int(data.get("E", 0))
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            last_update[0] = time.time()
            bot.on_binance_price(mid, event_ts)

    def stale_watchdog(ws_ref):
        while True:
            time.sleep(BINANCE_STALE_TIMEOUT)
            if time.time() - last_update[0] > BINANCE_STALE_TIMEOUT:
                print(f"[binance] No update for {BINANCE_STALE_TIMEOUT}s — reconnecting...")
                try:
                    ws_ref[0].close()
                except Exception:
                    pass
                break

    while not shutdown_event.is_set():
        ws_ref = [None]
        ws_app = websocket.WebSocketApp(
            BINANCE_WS,
            on_open=on_open,
            on_message=on_message,
            on_error=lambda ws, e: None,
            on_close=lambda ws, c, m: None,
        )
        ws_ref[0] = ws_app
        threading.Thread(target=stale_watchdog, args=(ws_ref,), daemon=True).start()
        ws_app.run_forever()
        if not shutdown_event.is_set():
            time.sleep(2)


# ── CLOB WebSocket (orderbook best asks) ───────────────────────────────────────

CLOB_STALE_TIMEOUT = 10  # seconds without CLOB update → force reconnect


def run_clob_ws(bot: BotState):
    """Subscribe to CLOB orderbook for UP and DOWN tokens, track best asks.
    Reconnects if no update received for CLOB_STALE_TIMEOUT seconds."""
    last_update = [time.time()]

    def on_open(ws):
        last_update[0] = time.time()
        bot.clob_connected = True
        bot.clob_ws_app = ws
        ids = [t for t in [bot.token_up, bot.token_down] if t]
        if ids:
            ws.send(json.dumps({"assets_ids": ids, "type": "market"}))
            print(f"[clob-ws] Connected, subscribed to {len(ids)} tokens")
        else:
            print("[clob-ws] Connected, no tokens to subscribe")

    def handle_event(data: dict):
        evt = data.get("event_type")
        if evt == "book":
            last_update[0] = time.time()
            bot.set_book_snapshot(data["asset_id"], data.get("asks", []), data.get("bids", []))
        elif evt == "price_change":
            last_update[0] = time.time()
            for ch in data.get("price_changes", []):
                bot.apply_book_delta(ch["asset_id"], ch["price"], ch["size"], ch["side"])

    def on_message(ws, message):
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                handle_event(item)
        else:
            handle_event(data)

    def on_error(ws, error):
        bot.clob_connected = False

    def on_close(ws, code, msg):
        bot.clob_connected = False
        bot.clob_ws_app = None

    def ping_loop(ws_ref):
        while bot.clob_connected and not shutdown_event.is_set():
            try:
                ws_ref.send("PING")
            except Exception:
                break
            time.sleep(PING_INTERVAL)

    def stale_watchdog(ws_ref):
        """Close the WS if no book update for CLOB_STALE_TIMEOUT seconds."""
        while True:
            time.sleep(CLOB_STALE_TIMEOUT)
            if time.time() - last_update[0] > CLOB_STALE_TIMEOUT:
                print(f"[clob-ws] No update for {CLOB_STALE_TIMEOUT}s — reconnecting...")
                try:
                    ws_ref[0].close()
                except Exception:
                    pass
                break

    while not shutdown_event.is_set():
        ws_ref = [None]
        ws_app = websocket.WebSocketApp(
            CLOB_WS,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_ref[0] = ws_app
        threading.Thread(target=ping_loop, args=(ws_app,), daemon=True).start()
        threading.Thread(target=stale_watchdog, args=(ws_ref,), daemon=True).start()
        ws_app.run_forever()
        bot.clob_connected = False
        bot.clob_ws_app = None
        if not shutdown_event.is_set():
            time.sleep(2)


# ── Shutdown ───────────────────────────────────────────────────────────────────

shutdown_event = threading.Event()


def shutdown(bot: BotState):
    print("\n[bot] Shutting down...")
    shutdown_event.set()
    if not DRY_RUN and bot.client is not None:
        cancel_all_orders(bot.client)
        print("[bot] Cancelled all orders.")
    print("[bot] Goodbye.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Polymarket MM Bot — BTC 5-min Up/Down")
    print("=" * 60)

    # 1. Precompute MC paths
    paths = load_or_compute_paths()

    # 2. Init CLOB client (skip in dry-run)
    client = None
    if not DRY_RUN:
        client = init_clob_client()
    else:
        print("[bot] DRY RUN — no orders will be placed")

    # 3. Create bot state
    bot = BotState(client, paths)

    # 4. Register signal handlers for graceful shutdown
    def on_signal(signum, frame):
        shutdown(bot)
        os._exit(0)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    # 5. Load initial market
    bot.load_market()

    # 6. Start CLOB WS for orderbook best asks
    print("[bot] Starting CLOB orderbook stream...")
    threading.Thread(target=run_clob_ws, args=(bot,), daemon=True).start()

    # 7. Stream chainlink prices for strike detection + diff calculation
    print("[bot] Starting RTDS price stream (strike + diff)...")
    threading.Thread(target=run_rtds, args=(bot,), daemon=True).start()

    # 8. Stream Binance BTCUSDC for fast MC ticks
    print("[bot] Starting Binance BTCUSDC stream (MC + orders)... (Ctrl+C to stop)")
    threading.Thread(target=run_binance, args=(bot,), daemon=True).start()

    # Main thread waits for shutdown signal
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=1)
    except KeyboardInterrupt:
        shutdown(bot)


if __name__ == "__main__":
    main()
