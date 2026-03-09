#!/usr/bin/env python3
"""Test script to stream Chainlink BTC/USD prices via Polymarket RTDS WebSocket."""

import json
import time
import websocket

RTDS_WS = "wss://ws-live-data.polymarket.com"


def main():

    print("Streaming Chainlink BTC/USD via RTDS WebSocket...\n")
    print(f"{'Time':<12} {'Price':>12} {'Change':>20}")
    print("-" * 46)

    def print_price(price: float, ts_ms: int = 0):
        from datetime import datetime
        now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        oracle_ts = datetime.fromtimestamp(ts_ms / 1000).strftime("%H:%M:%S.%f")[:-3] if ts_ms else "—"
        print(f"{now:<15} ${price:>11,.2f}   oracle={oracle_ts}")

    def on_open(ws):
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
            arr = payload.get("data", [])
            if arr:
                item = arr[-1]
                print_price(float(item.get("value", 0)), int(item.get("timestamp", 0)))
        else:
            price = float(payload.get("value", 0))
            ts_ms = int(payload.get("timestamp", 0))
            if price > 0:
                print_price(price, ts_ms)

    def on_error(ws, error):
        print(f"[error] {error}")

    def on_close(ws, code, msg):
        print(f"\nDisconnected (code={code}) — reconnecting...")

    while True:
        ws_app = websocket.WebSocketApp(
            RTDS_WS,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            ws_app.run_forever()
        except KeyboardInterrupt:
            print("\nStopped.")
            return
        time.sleep(2)


if __name__ == "__main__":
    main()
