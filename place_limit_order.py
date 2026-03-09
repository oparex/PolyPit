"""
place_limit_order.py — Place a GTC limit order on the current BTC 5-minute Up/Down market.

Usage:
    ./venv/bin/python place_limit_order.py

Requires:
    pip install py-clob-client

Environment variables (or prompts if missing):
    POLY_PRIVATE_KEY   — your private key (hex, with or without 0x prefix)
    POLY_FUNDER        — your funder wallet address (checksummed)
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet


# ---------------------------------------------------------------------------
# Market slug + market fetch
# ---------------------------------------------------------------------------

def get_current_5min_timestamp() -> int:
    now = int(time.time())
    return now - (now % 300)


def build_slug(ts: int) -> str:
    return f"btc-updown-5m-{ts}"


def parse_json_list(raw) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip().strip('"') for x in raw]
    return [t.strip().strip('"') for t in raw.strip("[]").split(",") if t.strip()]


def fetch_market(slug: str) -> dict | None:
    url = f"{GAMMA_API}/markets/slug/{slug}"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        print(f"[error] GET {url} -> {resp.status_code}")
        return None
    return resp.json()


def fetch_orderbook(token_id: str) -> dict | None:
    url = f"{CLOB_API}/book"
    resp = requests.get(url, params={"token_id": token_id}, timeout=10)
    if resp.status_code != 200:
        print(f"[error] GET {url} -> {resp.status_code}")
        return None
    return resp.json()


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def best_bid_ask(book: dict) -> tuple[float | None, float | None]:
    """Return (best_bid, best_ask) from a CLOB REST book response."""
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = max((float(e["price"]) for e in bids), default=None) if bids else None
    best_ask = min((float(e["price"]) for e in asks), default=None) if asks else None
    return best_bid, best_ask


def print_books(names: list[str], token_ids: list[str], books: list[dict]):
    print()
    print(f"  {'Outcome':<10}  {'Token ID (truncated)':<20}  {'Best Bid':>9}  {'Best Ask':>9}  {'Spread':>8}")
    print(f"  {'─'*10}  {'─'*20}  {'─'*9}  {'─'*9}  {'─'*8}")
    for name, token_id, book in zip(names, token_ids, books):
        bid, ask = best_bid_ask(book)
        bid_s = f"{bid:.4f}" if bid is not None else "  -.----"
        ask_s = f"{ask:.4f}" if ask is not None else "  -.----"
        spread = f"{ask - bid:.4f}" if (bid is not None and ask is not None) else "  -.----"
        tid_short = token_id[:8] + "..." + token_id[-6:]
        print(f"  {name:<10}  {tid_short:<20}  {bid_s:>9}  {ask_s:>9}  {spread:>8}")
    print()


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # 1. Compile slug
    ts = get_current_5min_timestamp()
    slug = build_slug(ts)
    expires_ts = ts + 300
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    remaining = max(0, expires_ts - int(time.time()))
    mins, secs = divmod(remaining, 60)

    print(f"\nSlug    : {slug}")
    print(f"Opens   : {dt}")
    print(f"Expires : {mins}m {secs:02d}s from now")

    # 2. Fetch market
    print(f"\nFetching market...")
    market = fetch_market(slug)
    if market is None:
        print("Could not fetch market. Exiting.")
        return

    title = market.get("title") or market.get("question", slug)
    print(f"Market  : {title}")

    token_ids = parse_json_list(market.get("clobTokenIds", "[]"))
    outcomes = parse_json_list(market.get("outcomes", "[]"))
    if not outcomes:
        outcomes = [f"Outcome {i}" for i in range(len(token_ids))]

    if len(token_ids) < 2:
        print("Expected 2 tokens (Up + Down). Got:", token_ids)
        return

    # 3. Fetch and print orderbooks
    print("\nFetching orderbooks...")
    books = []
    for tid in token_ids:
        book = fetch_orderbook(tid)
        books.append(book if book else {"bids": [], "asks": []})

    print_books(outcomes, token_ids, books)

    # Show token index for selection
    for i, (name, tid) in enumerate(zip(outcomes, token_ids)):
        print(f"  [{i}] {name}  —  {tid}")
    print()

    # 4. Init CLOB client
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
    except ImportError:
        print("[error] py-clob-client not installed. Run:")
        print("  ./venv/bin/pip install py-clob-client")
        return

    key, funder = get_creds()

    print("\nInitialising CLOB client...")
    client = ClobClient(
        host=CLOB_API,
        chain_id=CHAIN_ID,
        key=key,
        signature_type=2,
        funder=funder,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    print("Client ready.")

    # Fetch and print balance
    print("\nFetching balance...")
    try:
        bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2))
        if isinstance(bal, dict):
            usdc = float(bal.get("balance", 0)) / 1e6  # USDC has 6 decimals
            allowance = float(bal.get("allowance", 0)) / 1e6
            print(f"  USDC Balance  : ${usdc:,.2f}")
            print(f"  CLOB Allowance: ${allowance:,.2f}")
        else:
            print(f"  Balance: {bal}")
    except Exception as exc:
        print(f"  [warn] Could not fetch balance: {exc}")

    for name, tid in zip(outcomes, token_ids):
        try:
            bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid, signature_type=2))
            if isinstance(bal, dict):
                shares = float(bal.get("balance", 0)) / 1e6
                allowance = float(bal.get("allowance", 0)) / 1e6
                print(f"  {name} Balance  : {shares:,.2f} shares")
                print(f"  {name} Allowance: {allowance:,.2f}")
            else:
                print(f"  {name} Balance: {bal}")
        except Exception as exc:
            print(f"  [warn] Could not fetch {name} balance: {exc}")
    print()

    # 5. Prompt for trade parameters
    while True:
        idx_raw = input(f"Which outcome to trade? [0={outcomes[0]}, 1={outcomes[1]}]: ").strip()
        if idx_raw in ("0", "1"):
            idx = int(idx_raw)
            break
        print("  Enter 0 or 1.")

    token_id = token_ids[idx]
    outcome_name = outcomes[idx]

    while True:
        side = input("Side [BUY/SELL]: ").strip().upper()
        if side in ("BUY", "SELL"):
            break
        print("  Enter BUY or SELL.")

    while True:
        size_raw = input("Size (shares): ").strip()
        try:
            size = float(size_raw)
            if size > 0:
                break
        except ValueError:
            pass
        print("  Enter a positive number.")

    bid, ask = best_bid_ask(books[idx])
    suggested = ask if side == "BUY" else bid
    suggested_str = f" [{suggested:.4f}]" if suggested is not None else ""
    while True:
        price_raw = input(f"Limit price (0.01 – 0.99){suggested_str}: ").strip()
        try:
            limit_price = float(price_raw)
            if 0.01 <= limit_price <= 0.99:
                break
        except ValueError:
            pass
        print("  Enter a price between 0.01 and 0.99.")

    # Confirm
    print(f"\n  Outcome    : {outcome_name}")
    print(f"  Token ID   : {token_id}")
    print(f"  Side       : {side}")
    print(f"  Size       : {size} shares")
    print(f"  Limit price: {limit_price:.4f}")
    print(f"  Order type : GTC (good-till-cancelled)")
    confirm = input("\nSend order? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    # 6. Place GTC limit order
    order_args = OrderArgs(
        token_id=token_id,
        price=limit_price,
        size=size,
        side=side,
    )

    print("\nSending order...")
    try:
        signed_order = client.create_order(order_args)
        response = client.post_order(signed_order, OrderType.GTC)
        print("\nResponse:")
        print(json.dumps(response, indent=2))
    except Exception as exc:
        print(f"\n[error] {exc}")


if __name__ == "__main__":
    main()
