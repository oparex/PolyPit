import collections
import curses
import json
import math
import threading
import time
from datetime import datetime, timezone

import requests
import websocket

GAMMA_API = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MAX_ROWS = 15
MAX_TRADES = 200
MAX_ARBS = 500
MAX_PAPER_ROWS = 50
PAPER_BUY_PRICE = 0.45
PAPER_SELL_PRICE = 0.55
PING_INTERVAL = 10
VOL_WINDOW = 300  # 5 minutes of price history for volatility
SECONDS_PER_YEAR = 365.25 * 24 * 3600


def norm_cdf(x: float) -> float:
    """Standard normal CDF using the error function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_binary_call(s: float, k: float, t: float, sigma: float) -> float:
    """Black-Scholes price for a binary (digital) call option.

    Returns probability that S(T) >= K under risk-neutral measure.
    s: current price, k: strike, t: time to expiry in years, sigma: annualized vol.
    """
    if t <= 0 or sigma <= 0:
        return 1.0 if s >= k else 0.0
    d2 = (math.log(s / k) + (-0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    return norm_cdf(d2)


def get_current_5min_timestamp():
    now = int(time.time())
    return now - (now % 300)


def parse_json_list(raw: str) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip().strip('"') for x in raw]
    return [t.strip().strip('"') for t in raw.strip("[]").split(",") if t.strip()]


def fetch_market(slug: str) -> dict | None:
    url = f"{GAMMA_API}/markets/slug/{slug}"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        return None
    return resp.json()


class OrderBook:
    def __init__(self):
        self.lock = threading.Lock()
        self.books: dict[str, dict] = {}
        self.asset_ids: list[str] = []
        self.names: dict[str, str] = {}
        self.title = ""
        self.slug = ""
        self.expires_ts = 0  # next 5-min boundary (unix seconds)
        self.last_update = 0.0
        self.msg_count = 0
        self.connected = False
        self.ws_app: websocket.WebSocketApp | None = None
        self.rotations = 0
        # BTC price tracking
        self.strike_price: float = 0.0  # BTC price at market start
        self.btc_price: float = 0.0     # current BTC price
        # trades: deque of (timestamp_s, outcome_name, side, price, size)
        self.trades: collections.deque = collections.deque(maxlen=MAX_TRADES)
        # persistent volume profiles split by side: {normalized_price_str: total_volume}
        self.volume_up: dict[str, float] = {}
        self.volume_down: dict[str, float] = {}
        # arb opportunities: list of (timestamp, type, price_up, price_down, cost, edge, max_size, duration_ms)
        # type is "BUY" (buy both asks) or "SELL" (sell both bids)
        self.arbs: list[tuple] = []
        # active arb tracking: {type: (start_time, price_up, price_down)}
        self._active_arbs: dict[str, tuple] = {}
        # price history for volatility: list of (timestamp, btc_price)
        self._price_history: list[tuple[float, float]] = []
        self.volatility: float = 0.0  # annualized vol
        self.bs_call: float = 0.5     # BS binary call (Up) fair price
        self.bs_put: float = 0.5      # BS binary put (Down) fair price
        # paper trading
        # orders: {(outcome, side): {"price": float, "filled": bool, "fill_time": float}}
        self._paper_orders: dict[tuple, dict] = {}
        self._paper_min_up: float = float("inf")  # min normalized Up price seen
        self._paper_max_up: float = 0.0            # max normalized Up price seen
        self._paper_min_btc: float = float("inf")
        self._paper_max_btc: float = 0.0
        # completed market results: list of dicts, newest first
        self.paper_results: list[dict] = []
        # per-market trade statistics: volume-weighted tracking
        self._market_up_vol: float = 0.0
        self._market_up_cost: float = 0.0   # sum(price * size) for VWAP
        self._market_down_vol: float = 0.0
        self._market_down_cost: float = 0.0
        self.market_stats: list[dict] = []

    def set_market(self, slug: str, title: str, asset_ids: list[str],
                   names: list[str], expires_ts: int):
        with self.lock:
            # settle paper trades from previous market
            if self._paper_orders:
                self._settle_paper()
            # save market stats from previous market
            if self.slug and self.rotations > 0:
                self._save_market_stats()
            old_ids = list(self.asset_ids)
            self.slug = slug
            self.title = title
            self.asset_ids = asset_ids
            self.names = {aid: n for aid, n in zip(asset_ids, names)}
            self.expires_ts = expires_ts
            self.books = {}
            self.msg_count = 0
            self.trades.clear()
            self.volume_up.clear()
            self.volume_down.clear()
            # reset per-market trade stats
            self._market_up_vol = 0.0
            self._market_up_cost = 0.0
            self._market_down_vol = 0.0
            self._market_down_cost = 0.0
            # close any active arbs from the old market before rotating
            now = time.time()
            for arb_type in list(self._active_arbs):
                start, p_up, p_down, cost, edge, size = self._active_arbs.pop(arb_type)
                duration_ms = int((now - start) * 1000)
                self.arbs.append((start, arb_type, p_up, p_down, cost, edge, size, duration_ms))
            # init paper orders for new market (skip first market)
            if self.rotations > 0:
                self._init_paper_orders()
        return old_ids

    def add_trade(self, asset_id: str, side: str, price: str, size: str, timestamp_ms: int):
        with self.lock:
            name = self.names.get(asset_id, "?")
            ts = timestamp_ms / 1000.0
            self.trades.appendleft((ts, name, side, price, size))
            # accumulate into persistent volume profiles (normalized to Up price)
            p = float(price)
            vol = float(size)
            if name == "Up":
                key = f"{p:.2f}"
                self.volume_up[key] = self.volume_up.get(key, 0.0) + vol
                self._market_up_vol += vol
                self._market_up_cost += p * vol
            elif name == "Down":
                key = f"{round(1.0 - p, 2):.2f}"
                self.volume_down[key] = self.volume_down.get(key, 0.0) + vol
                self._market_down_vol += vol
                self._market_down_cost += p * vol

    def get_trades(self, max_rows: int) -> list:
        with self.lock:
            return list(self.trades)[:max_rows]

    def get_volume_by_price(self) -> tuple[dict[str, float], dict[str, float]]:
        """Return persistent volume profiles split by Up/Down.

        Both keyed by normalized price (Down inverted via 1-price).
        Returns (volume_up, volume_down).
        """
        with self.lock:
            return dict(self.volume_up), dict(self.volume_down)

    def _close_arb(self, arb_type: str, now: float):
        """Close an active arb and record it with duration."""
        if arb_type in self._active_arbs:
            start, p_up, p_down, cost, edge, size = self._active_arbs.pop(arb_type)
            duration_ms = int((now - start) * 1000)
            self.arbs.append((start, arb_type, p_up, p_down, cost, edge, size, duration_ms))
            if len(self.arbs) > MAX_ARBS:
                self.arbs = self.arbs[-MAX_ARBS:]

    def check_arbs(self):
        """Check for arb opportunities across Up/Down books. Called with lock held."""
        if len(self.asset_ids) < 2:
            return
        aid_up = self.asset_ids[0]
        aid_down = self.asset_ids[1]
        if aid_up not in self.books or aid_down not in self.books:
            return

        now = time.time()
        book_up = self.books[aid_up]
        book_down = self.books[aid_down]

        # BUY arb: buy best ask Up + buy best ask Down < 1.0
        buy_arb = False
        asks_up = book_up["asks"]
        asks_down = book_down["asks"]
        if asks_up and asks_down:
            best_ask_up = min(asks_up.keys(), key=lambda p: float(p))
            best_ask_down = min(asks_down.keys(), key=lambda p: float(p))
            cost = float(best_ask_up) + float(best_ask_down)
            if cost < 1.0:
                buy_arb = True
                edge = 1.0 - cost
                size = min(float(asks_up[best_ask_up]), float(asks_down[best_ask_down]))
                if "BUY" not in self._active_arbs:
                    self._active_arbs["BUY"] = (now, best_ask_up, best_ask_down, cost, edge, size)
                else:
                    # update size/edge but keep original start time
                    start = self._active_arbs["BUY"][0]
                    self._active_arbs["BUY"] = (start, best_ask_up, best_ask_down, cost, edge, size)
        if not buy_arb:
            self._close_arb("BUY", now)

        # SELL arb: sell best bid Up + sell best bid Down > 1.0
        sell_arb = False
        bids_up = book_up["bids"]
        bids_down = book_down["bids"]
        if bids_up and bids_down:
            best_bid_up = max(bids_up.keys(), key=lambda p: float(p))
            best_bid_down = max(bids_down.keys(), key=lambda p: float(p))
            proceeds = float(best_bid_up) + float(best_bid_down)
            if proceeds > 1.0:
                sell_arb = True
                edge = proceeds - 1.0
                size = min(float(bids_up[best_bid_up]), float(bids_down[best_bid_down]))
                if "SELL" not in self._active_arbs:
                    self._active_arbs["SELL"] = (now, best_bid_up, best_bid_down, proceeds, edge, size)
                else:
                    start = self._active_arbs["SELL"][0]
                    self._active_arbs["SELL"] = (start, best_bid_up, best_bid_down, proceeds, edge, size)
        if not sell_arb:
            self._close_arb("SELL", now)

    def record_price(self, price: float):
        """Record a BTC price observation and update volatility + BS prices. Called with lock held."""
        now = time.time()
        self._price_history.append((now, price))

        # trim to rolling window
        cutoff = now - VOL_WINDOW
        while self._price_history and self._price_history[0][0] < cutoff:
            self._price_history.pop(0)

        # need at least 10 observations for meaningful vol
        if len(self._price_history) < 10:
            return

        # compute log returns and time deltas
        log_returns = []
        dts = []
        for i in range(1, len(self._price_history)):
            t0, p0 = self._price_history[i - 1]
            t1, p1 = self._price_history[i]
            if p0 > 0 and p1 > 0 and t1 > t0:
                log_returns.append(math.log(p1 / p0))
                dts.append(t1 - t0)

        if len(log_returns) < 5 or not dts:
            return

        # realized volatility: variance of returns / avg_dt, annualized
        mean_ret = sum(log_returns) / len(log_returns)
        var = sum((r - mean_ret) ** 2 for r in log_returns) / (len(log_returns) - 1)
        avg_dt = sum(dts) / len(dts)
        if avg_dt <= 0:
            return

        var_per_second = var / avg_dt
        self.volatility = math.sqrt(var_per_second * SECONDS_PER_YEAR)

        # compute BS binary prices
        if self.strike_price > 0 and self.btc_price > 0 and self.expires_ts > 0:
            t_remaining = max(self.expires_ts - time.time(), 0) / SECONDS_PER_YEAR
            self.bs_call = bs_binary_call(self.btc_price, self.strike_price, t_remaining, self.volatility)
            self.bs_put = 1.0 - self.bs_call

    def _init_paper_orders(self):
        """Place paper orders for the new market. Called with lock held."""
        self._paper_orders = {
            ("Up", "BUY"):  {"price": PAPER_BUY_PRICE, "filled": False, "fill_time": 0.0},
            ("Up", "SELL"): {"price": PAPER_SELL_PRICE, "filled": False, "fill_time": 0.0},
            ("Down", "BUY"):  {"price": PAPER_BUY_PRICE, "filled": False, "fill_time": 0.0},
            ("Down", "SELL"): {"price": PAPER_SELL_PRICE, "filled": False, "fill_time": 0.0},
        }
        self._paper_min_up = float("inf")
        self._paper_max_up = 0.0
        self._paper_min_btc = float("inf")
        self._paper_max_btc = 0.0

    def _check_paper_fills(self):
        """Check if any paper orders are filled. Called with lock held."""
        if len(self.asset_ids) < 2:
            return
        aid_up, aid_down = self.asset_ids[0], self.asset_ids[1]

        for outcome, aid in [("Up", aid_up), ("Down", aid_down)]:
            if aid not in self.books:
                continue
            book = self.books[aid]
            now = time.time()

            # check BUY order: filled if best ask <= our buy price
            buy_order = self._paper_orders.get((outcome, "BUY"))
            if buy_order and not buy_order["filled"] and book["asks"]:
                best_ask = min(book["asks"].keys(), key=lambda p: float(p))
                if float(best_ask) <= buy_order["price"]:
                    buy_order["filled"] = True
                    buy_order["fill_time"] = now

            # check SELL order: filled if best bid >= our sell price
            sell_order = self._paper_orders.get((outcome, "SELL"))
            if sell_order and not sell_order["filled"] and book["bids"]:
                best_bid = max(book["bids"].keys(), key=lambda p: float(p))
                if float(best_bid) >= sell_order["price"]:
                    sell_order["filled"] = True
                    sell_order["fill_time"] = now

        # track min/max normalized Up price (Up best mid, and 1-Down best mid)
        if aid_up in self.books:
            book = self.books[aid_up]
            if book["bids"] and book["asks"]:
                bb = float(max(book["bids"].keys(), key=lambda p: float(p)))
                ba = float(min(book["asks"].keys(), key=lambda p: float(p)))
                mid = (bb + ba) / 2
                self._paper_min_up = min(self._paper_min_up, mid)
                self._paper_max_up = max(self._paper_max_up, mid)

        if aid_down in self.books:
            book = self.books[aid_down]
            if book["bids"] and book["asks"]:
                bb = float(max(book["bids"].keys(), key=lambda p: float(p)))
                ba = float(min(book["asks"].keys(), key=lambda p: float(p)))
                mid_down = (bb + ba) / 2
                norm = 1.0 - mid_down
                self._paper_min_up = min(self._paper_min_up, norm)
                self._paper_max_up = max(self._paper_max_up, norm)

        # track min/max BTC price
        if self.btc_price > 0:
            self._paper_min_btc = min(self._paper_min_btc, self.btc_price)
            self._paper_max_btc = max(self._paper_max_btc, self.btc_price)

    def _settle_paper(self):
        """Settle paper orders for the ended market. Called with lock held."""
        if not self._paper_orders or self.strike_price == 0 or self.btc_price == 0:
            return

        up_won = self.btc_price >= self.strike_price

        num_fills = 0
        profit = 0.0

        for (outcome, side), order in self._paper_orders.items():
            if not order["filled"]:
                continue
            num_fills += 1
            outcome_won = (outcome == "Up" and up_won) or (outcome == "Down" and not up_won)

            if side == "BUY":
                # we paid order["price"], get $1 if right, $0 if wrong
                if outcome_won:
                    profit += 1.0 - order["price"]
                else:
                    profit += 0.0 - order["price"]
            else:  # SELL
                # we received order["price"], pay $0 if right, $1 if wrong
                if outcome_won:
                    # we sold a winner — we pay $1
                    profit += order["price"] - 1.0
                else:
                    # we sold a loser — we pay $0
                    profit += order["price"]

        result = {
            "slug": self.slug,
            "fills": num_fills,
            "profit": profit,
            "up_won": up_won,
        }
        self.paper_results.insert(0, result)
        if len(self.paper_results) > MAX_PAPER_ROWS:
            self.paper_results.pop()

    def get_paper_results(self) -> list[dict]:
        with self.lock:
            return list(self.paper_results)

    def _save_market_stats(self):
        """Save aggregated trade stats for the ending market. Called with lock held."""
        up_avg = (self._market_up_cost / self._market_up_vol) if self._market_up_vol > 0 else 0.0
        down_avg = (self._market_down_cost / self._market_down_vol) if self._market_down_vol > 0 else 0.0

        up_won = self.btc_price >= self.strike_price if (self.strike_price > 0 and self.btc_price > 0) else None

        # net profit: if outcome won, value = 1.0; if lost, value = 0.0
        # For Up trades: bought at avg price, settled at 1.0 or 0.0
        # Approximate net = volume * (settle_value - avg_price)
        up_settle = 1.0 if up_won else 0.0
        down_settle = 0.0 if up_won else 1.0
        up_net = self._market_up_vol * (up_settle - up_avg) if self._market_up_vol > 0 else 0.0
        down_net = self._market_down_vol * (down_settle - down_avg) if self._market_down_vol > 0 else 0.0

        stat = {
            "slug": self.slug,
            "up_vol": self._market_up_vol,
            "up_avg": up_avg,
            "up_net": up_net,
            "down_vol": self._market_down_vol,
            "down_avg": down_avg,
            "down_net": down_net,
            "strike": self.strike_price,
            "min_up": self._paper_min_up if self._paper_min_up != float("inf") else 0.0,
            "max_up": self._paper_max_up,
            "min_btc": self._paper_min_btc if self._paper_min_btc != float("inf") else 0.0,
            "max_btc": self._paper_max_btc,
            "up_won": up_won,
            "settle_btc": self.btc_price,
        }
        self.market_stats.insert(0, stat)
        if len(self.market_stats) > MAX_PAPER_ROWS:
            self.market_stats.pop()

    def get_market_stats(self) -> list[dict]:
        with self.lock:
            return list(self.market_stats)

    def get_arbs(self, max_rows: int) -> list[tuple]:
        """Return most recent arb opportunities, newest first.

        Includes currently active (open) arbs at the top with live duration.
        Tuple: (timestamp, type, p_up, p_down, cost, edge, size, duration_ms)
        """
        with self.lock:
            now = time.time()
            # active arbs shown first with live duration
            active = []
            for arb_type in ("BUY", "SELL"):
                if arb_type in self._active_arbs:
                    start, p_up, p_down, cost, edge, size = self._active_arbs[arb_type]
                    dur = int((now - start) * 1000)
                    active.append((start, arb_type, p_up, p_down, cost, edge, size, dur))
            closed = list(reversed(self.arbs))
            combined = active + closed
            return combined[:max_rows], set(self._active_arbs.keys())

    def set_snapshot(self, asset_id: str, bids: list, asks: list):
        with self.lock:
            if asset_id not in self.names:
                return
            self.books[asset_id] = {
                "bids": {e["price"]: e["size"] for e in bids},
                "asks": {e["price"]: e["size"] for e in asks},
            }
            self.last_update = time.time()
            self.msg_count += 1
            self.check_arbs()
            self._check_paper_fills()

    def apply_delta(self, asset_id: str, price: str, size: str, side: str):
        with self.lock:
            if asset_id not in self.books:
                return
            book_side = "bids" if side == "BUY" else "asks"
            if float(size) == 0:
                self.books[asset_id][book_side].pop(price, None)
            else:
                self.books[asset_id][book_side][price] = size
            self.last_update = time.time()
            self.msg_count += 1
            self.check_arbs()
            self._check_paper_fills()

    def get_levels(self, asset_id: str):
        with self.lock:
            if asset_id not in self.books:
                return [], []
            book = self.books[asset_id]
            bids = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:MAX_ROWS]
            asks = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:MAX_ROWS]
            return bids, asks

    def get_snapshot(self):
        with self.lock:
            return (
                list(self.asset_ids),
                self.title,
                self.slug,
                self.expires_ts,
                self.connected,
                self.msg_count,
                self.rotations,
                self.strike_price,
                self.btc_price,
                self.bs_call,
                self.bs_put,
                self.volatility,
            )


def fetch_btc_price() -> float:
    """Fetch current BTC/USDT price from Binance."""
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": "BTCUSDT"}, timeout=5)
        return float(r.json()["price"])
    except Exception:
        return 0.0


def fetch_btc_price_at(ts: int) -> float:
    """Fetch BTC open price at a given unix timestamp from Binance klines."""
    try:
        r = requests.get("https://api.binance.com/api/v3/klines", params={
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": ts * 1000,
            "limit": 1,
        }, timeout=5)
        kline = r.json()
        if kline:
            return float(kline[0][1])  # open price
    except Exception:
        pass
    return 0.0


def run_btc_price(ob: OrderBook):
    """Background thread that polls current BTC price every second."""
    while True:
        price = fetch_btc_price()
        if price > 0:
            with ob.lock:
                ob.btc_price = price
                ob.record_price(price)
        time.sleep(1)


def swap_subscription(ob: OrderBook, old_ids: list[str], new_ids: list[str]):
    """Unsubscribe old tokens and subscribe new ones on the live WS."""
    ws = ob.ws_app
    if ws is None or not ob.connected:
        return
    try:
        if old_ids:
            ws.send(json.dumps({"assets_ids": old_ids, "operation": "unsubscribe"}))
        ws.send(json.dumps({"assets_ids": new_ids, "operation": "subscribe"}))
    except Exception:
        pass


def load_market(ob: OrderBook) -> list[str]:
    """Fetch the current 5-min market and update the OrderBook. Returns new asset_ids."""
    ts = get_current_5min_timestamp()
    slug = f"btc-updown-5m-{ts}"
    expires_ts = ts + 300

    for attempt in range(5):
        market = fetch_market(slug)
        if market is not None:
            break
        time.sleep(2)
    else:
        return []

    title = market.get("title") or market.get("question", slug)
    tokens = parse_json_list(market.get("clobTokenIds", "[]"))
    outcomes = parse_json_list(market.get("outcomes", "[]"))
    if not outcomes:
        outcomes = [f"Outcome {i}" for i in range(len(tokens))]

    old_ids = ob.set_market(slug, title, tokens, outcomes, expires_ts)
    ob.rotations += 1
    swap_subscription(ob, old_ids, tokens)

    # fetch strike price (BTC price at market start = start of 5-min window)
    strike = fetch_btc_price_at(ts)
    with ob.lock:
        ob.strike_price = strike

    return tokens


def run_rotation(ob: OrderBook):
    """Background thread that rotates to the next market when the current one expires."""
    while True:
        now = int(time.time())
        expires = ob.expires_ts
        sleep_for = max(0, expires - now) + 2  # 2s grace for market to appear
        time.sleep(sleep_for)
        load_market(ob)


def run_ws(ob: OrderBook):
    def on_open(ws):
        ob.connected = True
        ob.ws_app = ws
        # subscribe to current asset_ids
        with ob.lock:
            ids = list(ob.asset_ids)
        if ids:
            ws.send(json.dumps({"assets_ids": ids, "type": "market"}))

    def handle_event(data: dict):
        evt = data.get("event_type")
        if evt == "book":
            ob.set_snapshot(data["asset_id"], data.get("bids", []), data.get("asks", []))
        elif evt == "price_change":
            for ch in data.get("price_changes", []):
                ob.apply_delta(ch["asset_id"], ch["price"], ch["size"], ch["side"])
        elif evt == "last_trade_price":
            try:
                ts_ms = int(data.get("timestamp", 0))
            except (ValueError, TypeError):
                ts_ms = int(time.time() * 1000)
            ob.add_trade(
                data.get("asset_id", ""),
                data.get("side", ""),
                data.get("price", "0"),
                data.get("size", "0"),
                ts_ms,
            )

    def on_message(ws, message):
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                handle_event(item)
        else:
            handle_event(data)

    def on_error(ws, error):
        ob.connected = False

    def on_close(ws, code, msg):
        ob.connected = False
        ob.ws_app = None

    def ping_loop(ws_app):
        while ob.connected:
            try:
                ws_app.send("PING")
            except Exception:
                break
            time.sleep(PING_INTERVAL)

    while True:
        ws_app = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        threading.Thread(target=ping_loop, args=(ws_app,), daemon=True).start()
        ws_app.run_forever()
        ob.connected = False
        ob.ws_app = None
        time.sleep(2)


def draw(stdscr, ob: OrderBook):
    curses.curs_set(0)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)
    curses.init_pair(2, curses.COLOR_RED, -1)
    curses.init_pair(3, curses.COLOR_CYAN, -1)
    curses.init_pair(4, curses.COLOR_YELLOW, -1)
    curses.init_pair(5, curses.COLOR_MAGENTA, -1)
    curses.init_pair(6, curses.COLOR_WHITE, -1)
    stdscr.nodelay(True)
    stdscr.timeout(200)

    GREEN = curses.color_pair(1)
    RED = curses.color_pair(2)
    CYAN = curses.color_pair(3)
    YELLOW = curses.color_pair(4)
    MAGENTA = curses.color_pair(5)
    WHITE = curses.color_pair(6)
    BOLD = curses.A_BOLD
    DIM = curses.A_DIM

    COL_W = 32
    GAP = 4
    TRADE_W = 36
    CHART_W = 38
    ARB_W = 50

    while True:
        key = stdscr.getch()
        if key == ord("q"):
            return

        asset_ids, title, slug, expires_ts, connected, msg_count, rotations, strike_price, btc_price, bs_call, bs_put, volatility = ob.get_snapshot()

        h, w = stdscr.getmaxyx()
        stdscr.erase()

        now_utc = datetime.now(timezone.utc)
        now_ts = int(time.time())
        now_str = now_utc.strftime("%H:%M:%S UTC")
        status = "LIVE" if connected else "CONNECTING..."
        status_color = GREEN | BOLD if connected else YELLOW | BOLD

        remaining = max(0, expires_ts - now_ts)
        mins, secs = divmod(remaining, 60)
        countdown = f"{mins}:{secs:02d}"

        row = 0
        stdscr.addnstr(row, 1, f" {title} ", w - 2, BOLD | CYAN)
        row += 1
        stdscr.addnstr(row, 1, f" {slug}", w - 2, DIM)
        status_str = f" [{status}]  msgs: {msg_count}  {now_str} "
        if len(status_str) < w - 2:
            stdscr.addnstr(row, w - len(status_str) - 1, status_str, len(status_str), status_color)
        row += 1

        # countdown bar
        countdown_color = MAGENTA | BOLD if remaining <= 30 else YELLOW | BOLD
        countdown_str = f" Expires in {countdown} "
        bar_width = min(w - 4, 40)
        filled = int(bar_width * remaining / 300) if expires_ts > 0 else 0
        bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
        stdscr.addnstr(row, 1, f" {countdown_str}", len(countdown_str) + 1, countdown_color)
        try:
            stdscr.addnstr(row, len(countdown_str) + 2, bar, bar_width, countdown_color)
        except curses.error:
            pass
        # BTC price info to the right of the bar
        after_bar_x = len(countdown_str) + bar_width + 3
        if strike_price > 0 and btc_price > 0:
            diff = btc_price - strike_price
            diff_sign = "+" if diff >= 0 else ""
            price_color = GREEN | BOLD if diff >= 0 else RED | BOLD
            btc_str = f" BTC ${btc_price:,.2f}"
            strike_str = f" Strike ${strike_price:,.2f}"
            diff_str = f" ({diff_sign}{diff:,.2f})"
            if after_bar_x + len(btc_str) + len(strike_str) + len(diff_str) + 2 < w:
                try:
                    stdscr.addnstr(row, after_bar_x, btc_str, len(btc_str), price_color)
                    stdscr.addnstr(row, after_bar_x + len(btc_str), strike_str, len(strike_str), DIM)
                    stdscr.addnstr(row, after_bar_x + len(btc_str) + len(strike_str), diff_str, len(diff_str), price_color)
                except curses.error:
                    pass
        elif btc_price > 0:
            btc_str = f" BTC ${btc_price:,.2f}"
            try:
                stdscr.addnstr(row, after_bar_x, btc_str, len(btc_str), YELLOW | BOLD)
            except curses.error:
                pass
        row += 2

        # column positions
        left_x = 2
        right_x = left_x + COL_W + GAP
        if right_x + COL_W > w:
            right_x = left_x + COL_W + 2

        # trade tape x position (right of both books)
        trade_x = right_x + COL_W + GAP
        show_trades = trade_x + TRADE_W <= w

        # volume chart x position (right of trade tape)
        chart_x = trade_x + TRADE_W + GAP
        show_chart = show_trades and chart_x + CHART_W <= w

        # arb column x position (right of volume chart)
        arb_x = chart_x + CHART_W + GAP
        show_arbs = show_chart and arb_x + ARB_W <= w

        if len(asset_ids) < 2:
            stdscr.addnstr(row, 2, "Waiting for market data...", w - 4, DIM)
            stdscr.refresh()
            continue

        book_start_row = row  # remember where the book starts for trade column

        # outcome headers
        for i, aid in enumerate(asset_ids):
            x = left_x if i == 0 else right_x
            name = ob.names.get(aid, f"Outcome {i}")
            stdscr.addnstr(row, x, f"{'':>4}{name:^24}", COL_W, BOLD | CYAN)
        if show_trades:
            stdscr.addnstr(row, trade_x, f"{'Recent Trades':^{TRADE_W}}", TRADE_W, BOLD | CYAN)
        if show_chart:
            stdscr.addnstr(row, chart_x, f"{'Volume by Price':^{CHART_W}}", CHART_W, BOLD | CYAN)
        if show_arbs:
            stdscr.addnstr(row, arb_x, f"{'Arb Opportunities':^{ARB_W}}", ARB_W, BOLD | CYAN)
        row += 1

        for i in range(2):
            x = left_x if i == 0 else right_x
            stdscr.addnstr(row, x, "\u2500" * COL_W, COL_W, DIM)
        if show_trades:
            stdscr.addnstr(row, trade_x, "\u2500" * TRADE_W, TRADE_W, DIM)
        if show_chart:
            stdscr.addnstr(row, chart_x, "\u2500" * CHART_W, CHART_W, DIM)
        if show_arbs:
            stdscr.addnstr(row, arb_x, "\u2500" * ARB_W, ARB_W, DIM)
        row += 1

        # asks header
        for i in range(2):
            x = left_x if i == 0 else right_x
            stdscr.addnstr(row, x, f"{'ASKS (sell)':^{COL_W}}", COL_W, DIM)
        if show_trades:
            hdr = "  TIME      SIDE  PRICE     SIZE"
            stdscr.addnstr(row, trade_x, hdr, TRADE_W, DIM)
        if show_chart:
            bar_max = (CHART_W - 8) // 2
            price_w = 6
            up_hdr = f"{'Up':>{bar_max}}"
            dn_hdr = f"{'Down':<{bar_max}}"
            stdscr.addnstr(row, chart_x, up_hdr, bar_max, GREEN | DIM)
            stdscr.addnstr(row, chart_x + bar_max, f"{'':^{price_w}}", price_w, DIM)
            stdscr.addnstr(row, chart_x + bar_max + price_w, dn_hdr, bar_max, RED | DIM)
        if show_arbs:
            stdscr.addnstr(row, arb_x, "  TIME         TYPE  EDGE   SIZE  DUR     Up/Dn", ARB_W, DIM)
        row += 1

        # get levels
        levels = []
        for aid in asset_ids:
            bids, asks = ob.get_levels(aid)
            levels.append((bids, asks))

        max_ask_rows = max((len(l[1]) for l in levels), default=0)
        max_ask_rows = min(max_ask_rows, MAX_ROWS)

        for r in range(max_ask_rows - 1, -1, -1):
            if row >= h - 3:
                break
            for i, (bids, asks) in enumerate(levels):
                x = left_x if i == 0 else right_x
                if r < len(asks):
                    price, size = asks[r]
                    line = f"  {float(price):>6.2f}  \u2502 {float(size):>10.2f}  "
                    try:
                        stdscr.addnstr(row, x, line, COL_W, RED)
                    except curses.error:
                        pass
            row += 1

        # spread line with BS fair price
        spread_row = None
        if row < h - 2:
            bs_prices = [bs_call, bs_put]  # Up=call, Down=put
            for i, (bids, asks) in enumerate(levels):
                x = left_x if i == 0 else right_x
                best_bid = f"{float(bids[0][0]):.2f}" if bids else "-.--"
                best_ask = f"{float(asks[0][0]):.2f}" if asks else "-.--"
                if volatility > 0:
                    fair = bs_prices[i]
                    spread_str = f" {best_bid} [{fair:.2f}] {best_ask} "
                else:
                    spread_str = f"  {best_bid} / {best_ask}  "
                padded = f"{spread_str:\u2500^{COL_W}}"
                try:
                    stdscr.addnstr(row, x, padded, COL_W, YELLOW | BOLD)
                except curses.error:
                    pass
            spread_row = row
            row += 1

        # bids header
        if row < h - 2:
            for i in range(2):
                x = left_x if i == 0 else right_x
                try:
                    stdscr.addnstr(row, x, f"{'BIDS (buy)':^{COL_W}}", COL_W, DIM)
                except curses.error:
                    pass
            row += 1

        max_bid_rows = max((len(l[0]) for l in levels), default=0)
        max_bid_rows = min(max_bid_rows, MAX_ROWS)

        for r in range(max_bid_rows):
            if row >= h - 2:
                break
            for i, (bids, asks) in enumerate(levels):
                x = left_x if i == 0 else right_x
                if r < len(bids):
                    price, size = bids[r]
                    line = f"  {float(price):>6.2f}  \u2502 {float(size):>10.2f}  "
                    try:
                        stdscr.addnstr(row, x, line, COL_W, GREEN)
                    except curses.error:
                        pass
            row += 1

        book_end_row = row

        # -- trade tape (drawn in the rows alongside the book) --
        if show_trades:
            # available rows for trades: from book_start_row+3 (after header/separator/col header)
            # to book_end_row - 1
            trade_row_start = book_start_row + 3
            trade_row_count = book_end_row - trade_row_start
            if trade_row_count > 0:
                trades = ob.get_trades(trade_row_count)
                for idx, trade in enumerate(trades):
                    tr = trade_row_start + idx
                    if tr >= h - 1:
                        break
                    ts_f, name, side, price, size = trade
                    t_str = datetime.fromtimestamp(ts_f, tz=timezone.utc).strftime("%H:%M:%S")
                    side_short = "BUY " if side == "BUY" else "SELL"
                    color = GREEN if name == "Up" else RED
                    # name tag: first char of outcome
                    tag = name[0] if name else "?"
                    line = f"  {t_str}  {tag} {side_short} {float(price):>5.2f} {float(size):>8.2f}"
                    try:
                        stdscr.addnstr(tr, trade_x, line, TRADE_W, color)
                    except curses.error:
                        pass

        # -- volatility between the two books on the spread row --
        if volatility > 0 and spread_row is not None:
            vol_str = f"\u03c3{volatility:.0%}"
            gap_start = left_x + COL_W
            gap_width = right_x - gap_start
            vol_x = gap_start + max(0, (gap_width - len(vol_str)) // 2)
            try:
                stdscr.addnstr(spread_row, vol_x, vol_str, gap_width, MAGENTA | BOLD)
            except curses.error:
                pass

        # -- volume by price chart (split Up left / price center / Down right) --
        if show_chart:
            chart_row_start = book_start_row + 3
            chart_row_count = book_end_row - chart_row_start
            if chart_row_count > 0:
                vol_up, vol_down = ob.get_volume_by_price()
                all_prices = sorted(set(vol_up) | set(vol_down), key=lambda p: float(p), reverse=True)
                all_prices = all_prices[:chart_row_count]
                if all_prices:
                    max_vol = max(
                        max((vol_up.get(p, 0) for p in all_prices), default=1),
                        max((vol_down.get(p, 0) for p in all_prices), default=1),
                    ) or 1.0
                    # layout: [up_bar 14] [price 6] [down_bar 14]  = ~34 + padding
                    bar_max = (CHART_W - 8) // 2  # bar width per side

                    for idx, price_key in enumerate(all_prices):
                        cr = chart_row_start + idx
                        if cr >= h - 1:
                            break
                        vu = vol_up.get(price_key, 0)
                        vd = vol_down.get(price_key, 0)
                        len_up = int(bar_max * vu / max_vol) if vu > 0 else 0
                        len_down = int(bar_max * vd / max_vol) if vd > 0 else 0
                        len_up = max(len_up, 1) if vu > 0 else 0
                        len_down = max(len_down, 1) if vd > 0 else 0

                        # Up bar: right-aligned, grows leftward
                        up_bar = "\u2588" * len_up
                        up_str = f"{up_bar:>{bar_max}}"
                        # Down bar: left-aligned, grows rightward
                        down_bar = "\u2588" * len_down
                        down_str = f"{down_bar:<{bar_max}}"

                        price_label = f" {price_key} "

                        try:
                            # Up bar (green, right-aligned)
                            stdscr.addnstr(cr, chart_x, up_str, bar_max, GREEN)
                            # price label (center)
                            stdscr.addnstr(cr, chart_x + bar_max, price_label, len(price_label), YELLOW | BOLD)
                            # Down bar (red, left-aligned)
                            stdscr.addnstr(cr, chart_x + bar_max + len(price_label), down_str, bar_max, RED)
                        except curses.error:
                            pass

        # -- arb opportunities (drawn alongside the book) --
        if show_arbs:
            arb_row_start = book_start_row + 3
            arb_row_count = book_end_row - arb_row_start
            if arb_row_count > 0:
                arbs, active_types = ob.get_arbs(arb_row_count)
                for idx, arb in enumerate(arbs):
                    ar = arb_row_start + idx
                    if ar >= h - 1:
                        break
                    ts_f, arb_type, p_up, p_down, cost, edge, size, dur_ms = arb
                    dt = datetime.fromtimestamp(ts_f, tz=timezone.utc)
                    ms = int((ts_f % 1) * 1000)
                    t_str = dt.strftime("%H:%M:%S") + f".{ms:03d}"
                    type_str = "BUY " if arb_type == "BUY" else "SELL"
                    # format duration
                    if dur_ms < 1000:
                        dur_str = f"{dur_ms}ms"
                    else:
                        dur_str = f"{dur_ms / 1000:.1f}s"
                    # active arbs shown brighter
                    is_active = arb_type in active_types
                    color = (GREEN | BOLD) if is_active else GREEN
                    marker = "\u25cf" if is_active else " "
                    line = (
                        f" {marker}{t_str}  {type_str}"
                        f" {edge:>5.2f}"
                        f" {size:>6.1f}"
                        f" {dur_str:>6}"
                        f"  {p_up}/{p_down}"
                    )
                    try:
                        stdscr.addnstr(ar, arb_x, line, ARB_W, color)
                    except curses.error:
                        pass

        # -- paper trading table (below the orderbook, left side) --
        PAPER_W = 68  # width of paper trading panel
        paper_row = book_end_row + 1
        if paper_row < h - 3:
            # current market paper status
            paper_orders = None
            with ob.lock:
                paper_orders = dict(ob._paper_orders)
            filled_count = sum(1 for o in paper_orders.values() if o["filled"])
            pending_count = sum(1 for o in paper_orders.values() if not o["filled"])
            paper_status = f" Paper: {filled_count} filled, {pending_count} pending"
            fills_detail = ""
            for (outcome, side), o in sorted(paper_orders.items()):
                marker = "\u2713" if o["filled"] else "\u00b7"
                fills_detail += f"  {marker}{outcome[0]}{side[0]}@{o['price']:.2f}"
            try:
                stdscr.addnstr(paper_row, 1, paper_status, PAPER_W, CYAN | BOLD)
                stdscr.addnstr(paper_row, len(paper_status) + 1, fills_detail, PAPER_W - len(paper_status), DIM)
            except curses.error:
                pass
            paper_row += 1

        results = ob.get_paper_results()
        if results and paper_row < h - 2:
            # header
            hdr = (
                f" {'Market':<28}"
                f" {'Fills':>5}"
                f" {'P&L':>7}"
                f" {'Result':>6}"
            )
            try:
                stdscr.addnstr(paper_row, 1, hdr, PAPER_W, CYAN | DIM)
            except curses.error:
                pass
            paper_row += 1
            try:
                stdscr.addnstr(paper_row, 1, "\u2500" * min(PAPER_W, len(hdr)), PAPER_W, DIM)
            except curses.error:
                pass
            paper_row += 1

            cumulative_pnl = sum(r["profit"] for r in results)

            for res in results:
                if paper_row >= h - 2:
                    break
                result_str = "UP" if res["up_won"] else "DN"
                result_color = GREEN if res["up_won"] else RED
                pnl_color = GREEN if res["profit"] >= 0 else RED
                line = (
                    f" {res['slug']:<28}"
                    f" {res['fills']:>5}"
                    f" {res['profit']:>+7.2f}"
                )
                result_part = f" {result_str:>6}"
                try:
                    stdscr.addnstr(paper_row, 1, line, len(line), pnl_color)
                    stdscr.addnstr(paper_row, 1 + len(line), result_part, len(result_part), result_color | BOLD)
                except curses.error:
                    pass
                paper_row += 1

            # cumulative P&L
            if paper_row < h - 1:
                cum_color = GREEN | BOLD if cumulative_pnl >= 0 else RED | BOLD
                cum_str = f" Cumulative P&L: {cumulative_pnl:+.2f}  ({len(results)} markets)"
                try:
                    stdscr.addnstr(paper_row, 1, cum_str, PAPER_W, cum_color)
                except curses.error:
                    pass

        # -- market statistics panel (right of paper trading) --
        STATS_X = PAPER_W + 4
        stats = ob.get_market_stats()
        stats_row = book_end_row + 1
        if stats and STATS_X < w - 10 and stats_row < h - 3:
            stats_hdr = (
                f" {'Market':<28}"
                f" {'UpVol':>8} {'UpAvg':>6} {'UpNet':>8}"
                f" {'DnVol':>8} {'DnAvg':>6} {'DnNet':>8}"
                f" {'Strike':>10} {'BTC':>10}"
                f" {'MinUp':>6} {'MaxUp':>6}"
                f" {'MinBTC':>10} {'MaxBTC':>10}"
            )
            try:
                stdscr.addnstr(stats_row, STATS_X, " Market Statistics", w - STATS_X - 1, CYAN | BOLD)
            except curses.error:
                pass
            stats_row += 1
            try:
                stdscr.addnstr(stats_row, STATS_X, stats_hdr, w - STATS_X - 1, CYAN | DIM)
            except curses.error:
                pass
            stats_row += 1
            try:
                stdscr.addnstr(stats_row, STATS_X, "\u2500" * min(w - STATS_X - 1, len(stats_hdr)), w - STATS_X - 1, DIM)
            except curses.error:
                pass
            stats_row += 1

            for st in stats:
                if stats_row >= h - 2:
                    break
                result_str = "UP" if st["up_won"] else "DN"
                result_color = GREEN if st["up_won"] else RED
                up_net_color = GREEN if st["up_net"] >= 0 else RED
                dn_net_color = GREEN if st["down_net"] >= 0 else RED
                base = f" {st['slug']:<28}"
                up_part = f" {st['up_vol']:>8.1f} {st['up_avg']:>6.3f} {st['up_net']:>+8.2f}"
                dn_part = f" {st['down_vol']:>8.1f} {st['down_avg']:>6.3f} {st['down_net']:>+8.2f}"
                price_part = (
                    f" {st['strike']:>10,.2f} {st['settle_btc']:>10,.2f}"
                    f" {st['min_up']:>6.2f} {st['max_up']:>6.2f}"
                    f" {st['min_btc']:>10,.2f} {st['max_btc']:>10,.2f}"
                )
                try:
                    cx = STATS_X
                    stdscr.addnstr(stats_row, cx, base, len(base), result_color)
                    cx += len(base)
                    stdscr.addnstr(stats_row, cx, up_part, len(up_part), up_net_color)
                    cx += len(up_part)
                    stdscr.addnstr(stats_row, cx, dn_part, len(dn_part), dn_net_color)
                    cx += len(dn_part)
                    stdscr.addnstr(stats_row, cx, price_part, len(price_part), DIM)
                except curses.error:
                    pass
                stats_row += 1

        # footer
        if h > 1:
            try:
                stdscr.addnstr(h - 1, 1, " Press 'q' to quit ", w - 2, DIM)
            except curses.error:
                pass

        stdscr.refresh()


def main():
    ob = OrderBook()

    # load initial market
    tokens = load_market(ob)
    if not tokens:
        print("Could not fetch initial market.")
        return

    # start websocket thread
    threading.Thread(target=run_ws, args=(ob,), daemon=True).start()

    # start rotation thread
    threading.Thread(target=run_rotation, args=(ob,), daemon=True).start()

    # start BTC price polling thread
    threading.Thread(target=run_btc_price, args=(ob,), daemon=True).start()

    curses.wrapper(lambda stdscr: draw(stdscr, ob))


if __name__ == "__main__":
    main()
