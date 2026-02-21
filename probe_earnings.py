"""
One-shot probe: log in to Robinhood and print the raw get_earnings()
response for a handful of symbols so we can confirm the shape.

Usage:
    ROBINHOOD_USERNAME=... ROBINHOOD_PASSWORD=... python3 probe_earnings.py

Optional env vars:
    ROBINHOOD_TOTP_SECRET   — if MFA is enabled
"""

import json
import os
import sys

import robin_stocks.robinhood as rh

USERNAME = os.environ.get("ROBINHOOD_USERNAME")
PASSWORD = os.environ.get("ROBINHOOD_PASSWORD")
TOTP_SECRET = os.environ.get("ROBINHOOD_TOTP_SECRET", "")

if not USERNAME or not PASSWORD:
    sys.exit("Set ROBINHOOD_USERNAME and ROBINHOOD_PASSWORD env vars")

# --- login ---------------------------------------------------------------
mfa_code = None
if TOTP_SECRET:
    import pyotp
    mfa_code = pyotp.TOTP(TOTP_SECRET).now()

print(f"Logging in as {USERNAME}...")
result = rh.login(username=USERNAME, password=PASSWORD, mfa_code=mfa_code, store_session=False)
if not result:
    sys.exit("Login failed")
print("Logged in.\n")

# --- probe symbols -------------------------------------------------------
# Mix of: a stock with near-term earnings, one with no earnings (ETF),
# and one from the tracked watchlist
SYMBOLS = ["WPM", "CCJ", "SLV", "SPY", "AAPL"]

for sym in SYMBOLS:
    print(f"{'='*60}")
    print(f"get_earnings({sym!r})")
    print(f"{'='*60}")
    try:
        data = rh.stocks.get_earnings(sym)
        print(json.dumps(data, indent=2, default=str))
    except Exception as exc:
        print(f"ERROR: {exc}")
    print()

rh.logout()
print("Done.")
