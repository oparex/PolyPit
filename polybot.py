import requests
import time
from datetime import datetime, timezone

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


def get_current_5min_timestamp():
    """Get the current UTC timestamp rounded down to the nearest 5 minutes (seconds)."""
    now = int(time.time())
    return now - (now % 300)


def build_slug(ts: int) -> str:
    """Build the BTC up/down 5-minute market slug for a given timestamp."""
    return f"btc-updown-5m-{ts}"


def fetch_market(slug: str) -> dict | None:
    """Fetch market data by slug from the Gamma API."""
    url = f"{GAMMA_API}/markets/slug/{slug}"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        print(f"[error] GET {url} -> {resp.status_code}")
        return None
    return resp.json()


def fetch_orderbook(token_id: str) -> dict | None:
    """Fetch the order book for a given CLOB token ID."""
    url = f"{CLOB_API}/book"
    resp = requests.get(url, params={"token_id": token_id}, timeout=10)
    if resp.status_code != 200:
        print(f"[error] GET {url}?token_id={token_id} -> {resp.status_code}")
        return None
    return resp.json()


COL_W = 30
MAX_ROWS = 10
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
CYAN = "\033[36m"
RESET = "\033[0m"


def fmt_row(price: str, size: str, color: str) -> str:
    bar_len = min(int(float(size) / 200), COL_W - 16)
    bar = "\u2588" * bar_len
    return f"  {color}{price:>6s}{RESET} {DIM}|{RESET} {size:>10s} {color}{bar}{RESET}"


def pad(text: str, width: int) -> str:
    """Pad a string (which may contain ANSI codes) to a visible width."""
    visible = len(text.replace(DIM, "").replace(GREEN, "").replace(RED, "")
                      .replace(BOLD, "").replace(CYAN, "").replace(RESET, ""))
    return text + " " * max(0, width - visible)


def print_books_side_by_side(books: list[dict], names: list[str]):
    # sort first, then take the levels closest to the spread
    asks_left = sorted(books[0].get("asks", []), key=lambda e: float(e["price"]))[:MAX_ROWS]
    asks_right = sorted(books[1].get("asks", []), key=lambda e: float(e["price"]))[:MAX_ROWS]
    bids_left = sorted(books[0].get("bids", []), key=lambda e: float(e["price"]), reverse=True)[:MAX_ROWS]
    bids_right = sorted(books[1].get("bids", []), key=lambda e: float(e["price"]), reverse=True)[:MAX_ROWS]

    # asks displayed high->low so lowest ask is near the spread
    asks_left = list(reversed(asks_left))
    asks_right = list(reversed(asks_right))

    sep = f"  {DIM}\u2502{RESET}  "
    header_l = f"{BOLD}{CYAN}{names[0]:^{COL_W}}{RESET}"
    header_r = f"{BOLD}{CYAN}{names[1]:^{COL_W}}{RESET}"
    print(f"\n  {header_l}{sep}{header_r}")
    print(f"  {'─' * COL_W}{sep}{'─' * COL_W}")

    # asks
    sub_l = f"  {DIM}{'ASKS (sell)':^{COL_W}}{RESET}"
    sub_r = f"  {DIM}{'ASKS (sell)':^{COL_W}}{RESET}"
    print(f"{sub_l}{sep}{sub_r}")

    max_ask = max(len(asks_left), len(asks_right))
    for i in range(max_ask):
        left = pad(fmt_row(asks_left[i]["price"], asks_left[i]["size"], RED), COL_W) if i < len(asks_left) else " " * COL_W
        right = pad(fmt_row(asks_right[i]["price"], asks_right[i]["size"], RED), COL_W) if i < len(asks_right) else " " * COL_W
        print(f"{left}{sep}{right}")

    # spread line
    spread_l = f"  {DIM}{'── spread ──':─^{COL_W}}{RESET}"
    spread_r = f"  {DIM}{'── spread ──':─^{COL_W}}{RESET}"
    print(f"{spread_l}{sep}{spread_r}")

    # bids
    sub_l = f"  {DIM}{'BIDS (buy)':^{COL_W}}{RESET}"
    sub_r = f"  {DIM}{'BIDS (buy)':^{COL_W}}{RESET}"
    print(f"{sub_l}{sep}{sub_r}")

    max_bid = max(len(bids_left), len(bids_right))
    for i in range(max_bid):
        left = pad(fmt_row(bids_left[i]["price"], bids_left[i]["size"], GREEN), COL_W) if i < len(bids_left) else " " * COL_W
        right = pad(fmt_row(bids_right[i]["price"], bids_right[i]["size"], GREEN), COL_W) if i < len(bids_right) else " " * COL_W
        print(f"{left}{sep}{right}")


def main():
    ts = get_current_5min_timestamp()
    slug = build_slug(ts)
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"Slug : {slug}")
    print(f"Time : {dt}")
    print()

    market = fetch_market(slug)
    if market is None:
        print("Could not fetch market. It may not exist yet.")
        return

    title = market.get("title") or market.get("question", "(no title)")
    print(f"Market: {title}")

    tokens = market.get("clobTokenIds")
    outcomes = market.get("outcomes")

    if not tokens:
        print("No CLOB token IDs found in market data.")
        return

    if isinstance(tokens, str):
        tokens = [t.strip() for t in tokens.strip("[]").split(",") if t.strip()]
    if isinstance(outcomes, str):
        outcomes = [o.strip().strip('"') for o in outcomes.strip("[]").split(",") if o.strip()]

    if not outcomes:
        outcomes = [f"Outcome {i}" for i in range(len(tokens))]

    books = []
    for token_id in tokens:
        token_id = token_id.strip('" ')
        book = fetch_orderbook(token_id)
        books.append(book if book else {"bids": [], "asks": []})

    print_books_side_by_side(books, outcomes)
    print()


if __name__ == "__main__":
    main()
