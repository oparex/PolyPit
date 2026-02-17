import collections
import csv
import json
import os
import threading
import time
from datetime import datetime, timezone

import requests
import websocket

GAMMA_API = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10
MAX_ARBS = 500
OUTPUT_FILE = "arbs.csv"


def get_current_5min_timestamp():
    now = int(time.time())
    return now - (now % 300)


def parse_json_list(raw):
    if isinstance(raw, list):
        return [str(x).strip().strip('"') for x in raw]
    return [t.strip().strip('"') for t in raw.strip("[]").split(",") if t.strip()]


def fetch_market(slug):
    url = f"{GAMMA_API}/markets/slug/{slug}"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        return None
    return resp.json()


def fetch_btc_price():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": "BTCUSDT"}, timeout=5)
        return float(r.json()["price"])
    except Exception:
        return 0.0


def fetch_btc_price_at(ts):
    try:
        r = requests.get("https://api.binance.com/api/v3/klines", params={
            "symbol": "BTCUSDT", "interval": "1m", "startTime": ts * 1000, "limit": 1,
        }, timeout=5)
        kline = r.json()
        if kline:
            return float(kline[0][1])
    except Exception:
        pass
    return 0.0


class ArbRecorder:
    def __init__(self, output_file):
        self.lock = threading.Lock()
        self.books: dict[str, dict] = {}
        self.asset_ids: list[str] = []
        self.names: dict[str, str] = {}
        self.slug = ""
        self.expires_ts = 0
        self.connected = False
        self.ws_app = None
        self.strike_price = 0.0
        self.btc_price = 0.0
        self._active_arbs: dict[str, tuple] = {}
        self.arb_count = 0
        self.msg_count = 0
        self.output_file = output_file
        self._writer = None
        self._csvfile = None
        self._init_csv()

    def _init_csv(self):
        file_exists = os.path.exists(self.output_file) and os.path.getsize(self.output_file) > 0
        self._csvfile = open(self.output_file, "a", newline="")
        self._writer = csv.writer(self._csvfile)
        if not file_exists:
            self._writer.writerow([
                "timestamp", "time_utc", "type", "price_up", "price_down",
                "cost", "edge", "size", "duration_ms", "slug",
                "strike_price", "btc_price",
            ])
            self._csvfile.flush()

    def write_arb(self, start, arb_type, p_up, p_down, cost, edge, size, duration_ms):
        dt = datetime.fromtimestamp(start, tz=timezone.utc)
        ms = int((start % 1) * 1000)
        t_str = dt.strftime("%H:%M:%S") + f".{ms:03d}"
        self._writer.writerow([
            f"{start:.3f}", t_str, arb_type, p_up, p_down,
            f"{cost:.4f}", f"{edge:.4f}", f"{size:.2f}", duration_ms,
            self.slug, f"{self.strike_price:.2f}", f"{self.btc_price:.2f}",
        ])
        self._csvfile.flush()
        self.arb_count += 1

        # also print to console
        dur = f"{duration_ms}ms" if duration_ms < 1000 else f"{duration_ms / 1000:.1f}s"
        print(
            f"  {t_str}  {arb_type:<4}"
            f"  edge={edge:.4f}  size={size:>8.2f}  dur={dur:>6}"
            f"  {p_up}/{p_down}"
            f"  btc=${self.btc_price:,.2f}"
        )

    def _close_arb(self, arb_type, now):
        if arb_type in self._active_arbs:
            start, p_up, p_down, cost, edge, size = self._active_arbs.pop(arb_type)
            duration_ms = int((now - start) * 1000)
            self.write_arb(start, arb_type, p_up, p_down, cost, edge, size, duration_ms)

    def check_arbs(self):
        if len(self.asset_ids) < 2:
            return
        aid_up, aid_down = self.asset_ids[0], self.asset_ids[1]
        if aid_up not in self.books or aid_down not in self.books:
            return

        now = time.time()
        book_up = self.books[aid_up]
        book_down = self.books[aid_down]

        # BUY arb
        buy_arb = False
        asks_up, asks_down = book_up["asks"], book_down["asks"]
        if asks_up and asks_down:
            best_ask_up = min(asks_up, key=lambda p: float(p))
            best_ask_down = min(asks_down, key=lambda p: float(p))
            cost = float(best_ask_up) + float(best_ask_down)
            if cost < 1.0:
                buy_arb = True
                edge = 1.0 - cost
                size = min(float(asks_up[best_ask_up]), float(asks_down[best_ask_down]))
                if "BUY" not in self._active_arbs:
                    self._active_arbs["BUY"] = (now, best_ask_up, best_ask_down, cost, edge, size)
                else:
                    start = self._active_arbs["BUY"][0]
                    self._active_arbs["BUY"] = (start, best_ask_up, best_ask_down, cost, edge, size)
        if not buy_arb:
            self._close_arb("BUY", now)

        # SELL arb
        sell_arb = False
        bids_up, bids_down = book_up["bids"], book_down["bids"]
        if bids_up and bids_down:
            best_bid_up = max(bids_up, key=lambda p: float(p))
            best_bid_down = max(bids_down, key=lambda p: float(p))
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

    def set_snapshot(self, asset_id, bids, asks):
        with self.lock:
            if asset_id not in self.names:
                return
            self.books[asset_id] = {
                "bids": {e["price"]: e["size"] for e in bids},
                "asks": {e["price"]: e["size"] for e in asks},
            }
            self.msg_count += 1
            self.check_arbs()

    def apply_delta(self, asset_id, price, size, side):
        with self.lock:
            if asset_id not in self.books:
                return
            book_side = "bids" if side == "BUY" else "asks"
            if float(size) == 0:
                self.books[asset_id][book_side].pop(price, None)
            else:
                self.books[asset_id][book_side][price] = size
            self.msg_count += 1
            self.check_arbs()

    def set_market(self, slug, asset_ids, names, expires_ts):
        with self.lock:
            # close active arbs from previous market
            now = time.time()
            for arb_type in list(self._active_arbs):
                self._close_arb(arb_type, now)
            self.slug = slug
            self.asset_ids = asset_ids
            self.names = {aid: n for aid, n in zip(asset_ids, names)}
            self.expires_ts = expires_ts
            self.books = {}
            self.msg_count = 0


def load_market(rec: ArbRecorder):
    ts = get_current_5min_timestamp()
    slug = f"btc-updown-5m-{ts}"
    expires_ts = ts + 300

    for _ in range(5):
        market = fetch_market(slug)
        if market is not None:
            break
        time.sleep(2)
    else:
        return []

    tokens = parse_json_list(market.get("clobTokenIds", "[]"))
    outcomes = parse_json_list(market.get("outcomes", "[]"))
    if not outcomes:
        outcomes = [f"Outcome {i}" for i in range(len(tokens))]

    title = market.get("title") or market.get("question", slug)
    rec.set_market(slug, tokens, outcomes, expires_ts)

    strike = fetch_btc_price_at(ts)
    with rec.lock:
        rec.strike_price = strike

    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"  {slug}  |  strike=${strike:,.2f}")
    print(f"{'='*70}")

    return tokens


def swap_subscription(rec: ArbRecorder, old_ids, new_ids):
    ws = rec.ws_app
    if ws is None or not rec.connected:
        return
    try:
        if old_ids:
            ws.send(json.dumps({"assets_ids": old_ids, "operation": "unsubscribe"}))
        ws.send(json.dumps({"assets_ids": new_ids, "operation": "subscribe"}))
    except Exception:
        pass


def run_rotation(rec: ArbRecorder):
    while True:
        now = int(time.time())
        sleep_for = max(0, rec.expires_ts - now) + 2
        time.sleep(sleep_for)
        old_ids = list(rec.asset_ids)
        tokens = load_market(rec)
        if tokens:
            swap_subscription(rec, old_ids, tokens)


def run_btc_price(rec: ArbRecorder):
    while True:
        price = fetch_btc_price()
        if price > 0:
            with rec.lock:
                rec.btc_price = price
        time.sleep(1)


def run_ws(rec: ArbRecorder):
    def on_open(ws):
        rec.connected = True
        rec.ws_app = ws
        with rec.lock:
            ids = list(rec.asset_ids)
        if ids:
            ws.send(json.dumps({"assets_ids": ids, "type": "market"}))

    def handle_event(data):
        evt = data.get("event_type")
        if evt == "book":
            rec.set_snapshot(data["asset_id"], data.get("bids", []), data.get("asks", []))
        elif evt == "price_change":
            for ch in data.get("price_changes", []):
                rec.apply_delta(ch["asset_id"], ch["price"], ch["size"], ch["side"])

    def on_message(ws, message):
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                handle_event(item)
        else:
            handle_event(data)

    def on_error(ws, error):
        rec.connected = False
        print(f"  [ws error] {error}")

    def on_close(ws, code, msg):
        rec.connected = False
        rec.ws_app = None
        print("  [ws closed]")

    def ping_loop(ws_app):
        while rec.connected:
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
        rec.connected = False
        rec.ws_app = None
        time.sleep(2)


def main():
    print(f"Arb Recorder — writing to {OUTPUT_FILE}")
    print(f"Press Ctrl+C to stop\n")

    rec = ArbRecorder(OUTPUT_FILE)

    tokens = load_market(rec)
    if not tokens:
        print("Could not fetch initial market.")
        return

    threading.Thread(target=run_ws, args=(rec,), daemon=True).start()
    threading.Thread(target=run_rotation, args=(rec,), daemon=True).start()
    threading.Thread(target=run_btc_price, args=(rec,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # close any remaining active arbs
        now = time.time()
        with rec.lock:
            for arb_type in list(rec._active_arbs):
                rec._close_arb(arb_type, now)
        print(f"\n\nStopped. {rec.arb_count} arbs recorded to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
