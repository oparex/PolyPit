"""
mm_bot.py — Live market-making bot for Polymarket BTC 5-min Up/Down markets.

Precomputes MC paths from historical Chainlink data (QuestDB), then streams
live Chainlink prices via Polymarket RTDS. On each price update: compute MC
fair price, place BUY limit orders on both UP and DOWN around fair price with
edge buffer. Detect fills via balance changes, cancel+replace on each tick.

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
ORDER_SIZE = 10.0             # shares per order
MAX_POSITION = 50.0           # max abs(pos_up - pos_down) before skipping that side
DRY_RUN = True                # True = print only, no real orders
MARKET_START_DELAY = 10       # seconds into market before trading
MARKET_STOP_BEFORE = 10       # seconds before expiry to stop trading
MIN_STEPS = MARKET_STOP_BEFORE
MAX_STEPS = 300 - MARKET_START_DELAY

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CHAIN_ID = 137
RTDS_WS = "wss://ws-live-data.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10

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
        self.best_ask_up: float | None = None
        self.best_ask_down: float | None = None
        self.clob_ws_app = None
        self.clob_connected = False
        self._loading_market = False

        # Cached boundary prices: {boundary_ts: price} for strike detection
        self._boundary_prices: dict[int, float] = {}

        # Position tracking
        self.pos_up: float = 0.0
        self.pos_down: float = 0.0

        # Trade log
        self.trades: list[dict] = []
        self.total_pnl: float = 0.0

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

        # reset orderbooks
        with self.lock:
            self._asks_up = {}
            self._asks_down = {}
            self.best_ask_up = None
            self.best_ask_down = None

        # swap CLOB WS subscription
        old_ids = [t for t in [old_token_up, old_token_down] if t]
        new_ids = [self.token_up, self.token_down]
        self._swap_clob_subscription(old_ids, new_ids)

        # REST fallback: fetch orderbooks in case WS doesn't send a snapshot
        self._fetch_books_rest()

        # snapshot positions
        if not DRY_RUN:
            self.pos_up = get_position(self.client, self.token_up)
            self.pos_down = get_position(self.client, self.token_down)

        title = market.get("title") or slug
        print(f"[market] {title}")
        print(f"[market] UP token:   {self.token_up[:12]}...")
        print(f"[market] DOWN token: {self.token_down[:12]}...")
        print(f"[market] Positions — UP: {self.pos_up:.2f}  DOWN: {self.pos_down:.2f}")
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
                    asks = book.get("asks", [])
                    self.set_book_snapshot(token_id, asks)
            except Exception:
                pass

    def set_book_snapshot(self, asset_id: str, asks: list):
        """Set full ask book from snapshot. Called from CLOB WS thread."""
        ask_dict = {e["price"]: e["size"] for e in asks}
        with self.lock:
            if asset_id == self.token_up:
                self._asks_up = ask_dict
                self.best_ask_up = min((float(p) for p in ask_dict), default=None) if ask_dict else None
            elif asset_id == self.token_down:
                self._asks_down = ask_dict
                self.best_ask_down = min((float(p) for p in ask_dict), default=None) if ask_dict else None

    def apply_book_delta(self, asset_id: str, price: str, size: str, side: str):
        """Apply a price_change delta. Called from CLOB WS thread."""
        if side != "SELL":  # only track ask side
            return
        with self.lock:
            if asset_id == self.token_up:
                asks = self._asks_up
            elif asset_id == self.token_down:
                asks = self._asks_down
            else:
                return
            if float(size) == 0:
                asks.pop(price, None)
            else:
                asks[price] = size
            if asset_id == self.token_up:
                self.best_ask_up = min((float(p) for p in asks), default=None) if asks else None
            else:
                self.best_ask_down = min((float(p) for p in asks), default=None) if asks else None

    def on_chainlink_price(self, price: float, oracle_ts_ms: int, snapshot: bool = False):
        """Called on every chainlink price update from RTDS.
        snapshot=True means this is from the initial snapshot (strike detection only, no MC).
        All timing decisions use the oracle timestamp, not our server clock."""
        oracle_s = int(oracle_ts_ms / 1000)
        oracle_ts_s = oracle_ts_ms / 1000.0
        ts_str = datetime.fromtimestamp(oracle_ts_s, tz=timezone.utc).strftime("%H:%M:%S.") + f"{oracle_ts_ms % 1000:03d}"

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
                    return
                else:
                    return

        if self.strike == 0.0 or snapshot:
            return

        # Compute time to expiry from oracle timestamp
        steps_left = self.expires - oracle_s
        if steps_left > MAX_STEPS or steps_left < MIN_STEPS:
            if steps_left < MIN_STEPS:
                print(f"[skip] {ts_str} steps_left={steps_left} < {MIN_STEPS}, waiting for next market")
            return

        # MC fair price
        fair_up = mc_fair_price(price, self.strike, steps_left, self.paths)
        fair_down = 1.0 - fair_up

        bid_up = round(fair_up - EDGE, 2)
        bid_down = round(fair_down - EDGE, 2)

        # Cap bids at best_ask - 0.01 (never cross the spread)
        with self.lock:
            ask_up = self.best_ask_up
            ask_down = self.best_ask_down

        if ask_up is not None and bid_up >= ask_up:
            bid_up = round(ask_up - 0.01, 2)
        if ask_down is not None and bid_down >= ask_down:
            bid_down = round(ask_down - 0.01, 2)

        ask_up_s = f"{ask_up:.2f}" if ask_up is not None else "None"
        ask_down_s = f"{ask_down:.2f}" if ask_down is not None else "None"

        print(f"[tick] {ts_str}  BTC={price:.2f}  strike={self.strike:.2f}  "
              f"steps={steps_left}  fair_up={fair_up:.4f}  fair_dn={fair_down:.4f}  "
              f"bid_up={bid_up:.2f}  bid_dn={bid_down:.2f}  "
              f"ask_up={ask_up_s}  ask_dn={ask_down_s}")

        if DRY_RUN:
            return

        # Check for fills via position change
        new_pos_up = get_position(self.client, self.token_up)
        new_pos_down = get_position(self.client, self.token_down)

        if new_pos_up != self.pos_up:
            delta = new_pos_up - self.pos_up
            print(f"[fill] UP position changed: {self.pos_up:.2f} -> {new_pos_up:.2f} (delta={delta:+.2f})")
            self.pos_up = new_pos_up

        if new_pos_down != self.pos_down:
            delta = new_pos_down - self.pos_down
            print(f"[fill] DOWN position changed: {self.pos_down:.2f} -> {new_pos_down:.2f} (delta={delta:+.2f})")
            self.pos_down = new_pos_down

        # Cancel existing orders and place new ones
        cancel_all_orders(self.client)

        net = self.pos_up - self.pos_down  # positive = long UP, negative = long DOWN
        skip_up = net >= MAX_POSITION
        skip_down = -net >= MAX_POSITION

        if skip_up or skip_down:
            skipped = []
            if skip_up:
                skipped.append("UP")
            if skip_down:
                skipped.append("DOWN")
            print(f"[pos] net={net:+.2f} (up={self.pos_up:.2f} down={self.pos_down:.2f}) — skipping {', '.join(skipped)}")

        if bid_up > 0.01 and not skip_up:
            oid = place_buy_order(self.client, self.token_up, bid_up, ORDER_SIZE)
            if oid:
                print(f"[order] BUY UP   @ {bid_up:.2f}  size={ORDER_SIZE}  id={oid}")

        if bid_down > 0.01 and not skip_down:
            oid = place_buy_order(self.client, self.token_down, bid_down, ORDER_SIZE)
            if oid:
                print(f"[order] BUY DOWN @ {bid_down:.2f}  size={ORDER_SIZE}  id={oid}")


# ── RTDS WebSocket (reconnect-on-stale, same pattern as paper_strategy.py) ─────

PRICE_STALE_TIMEOUT = 4  # seconds without update → force reconnect

def run_rtds(bot: BotState):
    """Connect to Polymarket RTDS and stream chainlink prices to the bot.
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
            # snapshot — only use for strike detection, no MC
            arr = payload.get("data", [])
            for item in arr:
                price = float(item.get("value", 0))
                ts_ms = int(item.get("timestamp", 0))
                if price > 0:
                    last_update[0] = time.time()
                    bot.on_chainlink_price(price, ts_ms, snapshot=True)
        else:
            # live update — full MC + orders
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
            bot.set_book_snapshot(data["asset_id"], data.get("asks", []))
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

    # 7. Stream chainlink prices (runs in daemon thread)
    print("[bot] Starting RTDS price stream... (Ctrl+C to stop)")
    threading.Thread(target=run_rtds, args=(bot,), daemon=True).start()

    # Main thread waits for shutdown signal
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=1)
    except KeyboardInterrupt:
        shutdown(bot)


if __name__ == "__main__":
    main()
