# Chainlink Price Feed Fix

## Problem Identified

The application was using **Bitstamp WebSocket** for live BTC price updates, but Polymarket markets **settle against Chainlink oracle prices**. This created a discrepancy where:

1. **Bitstamp** streams every trade (high frequency, ~100s updates/minute)
2. **Chainlink** oracle updates are infrequent (triggered by price deviation thresholds or time-based heartbeats)
3. The displayed price didn't match the settlement source
4. Strike prices were fetched from Chainlink, but live prices were from Bitstamp

## Root Cause

When testing the Chainlink WebSocket connection from Polymarket RTDS:
- Only **1 update received in 60 seconds** (the initial snapshot)
- No live price change events were pushed by the WebSocket
- The connection stayed open but didn't receive updates

However, when **reconnecting** to the WebSocket:
- Each new connection receives a fresh snapshot with the latest Chainlink price
- This snapshot is updated frequently (every few seconds)

## Solution Implemented

Changed from **Bitstamp streaming** to **Chainlink polling** via Polymarket RTDS:

### Before (Bitstamp):
```python
def run_btc_price(ob: OrderBook):
    """Background thread that streams BTC/USD price from Bitstamp WebSocket."""
    # Connected to wss://ws.bitstamp.net
    # Received trade events continuously
```

### After (Chainlink):
```python
def run_btc_price(ob: OrderBook):
    """Background thread that polls BTC/USD price from Chainlink via Polymarket RTDS.
    
    Polls every 2 seconds by reconnecting to get the latest snapshot.
    This matches the settlement source Polymarket uses.
    """
    # Connects to wss://ws-live-data.polymarket.com
    # Reconnects every 2 seconds to get fresh snapshot
```

## Benefits

1. ✅ **Accurate Settlement Matching** - Uses the same price source as Polymarket settlement
2. ✅ **Consistent Strike & Live Prices** - Both from Chainlink oracle
3. ✅ **Fair BS/MC Pricing** - Fair value calculations use the correct underlying price
4. ✅ **Reliable Updates** - Polling ensures we get updates even if the WebSocket doesn't push them

## Testing

Test script created: `test_chainlink.py`

Sample output showing 2-second polling interval:
```
Time                Price     Change
------------------------------------
14:27:15     $  66,126.35           
14:27:18     $  66,122.77 -3.58 (-0.005%)
14:27:20     $  66,117.86 -4.92 (-0.007%)
14:27:22     $  66,123.57 +5.71 (+0.009%)
```

## Files Modified

1. `polybot_ws.py` - Updated `run_btc_price()` function
2. `arb_recorder.py` - Updated `run_btc_price()` function  
3. `CLAUDE.md` - Updated architecture documentation
4. `README.md` - Updated APIs table
5. `test_chainlink.py` - New test script (created)

## Performance Impact

- **Polling frequency**: 2 seconds (vs continuous streaming)
- **Network overhead**: Minimal - small WebSocket reconnect every 2s
- **Latency**: Acceptable for 5-minute markets
- **Accuracy**: Higher - matches actual settlement source

## Verification

Run the test script to verify Chainlink polling is working:
```bash
./venv/bin/python3 test_chainlink.py
```

Or start the main app and verify BTC price updates every 2 seconds in the header.
