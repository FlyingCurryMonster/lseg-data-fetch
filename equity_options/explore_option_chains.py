"""
Discover option chains and test intraday data retention for NVDA, AMD, SPY.

Findings:
- Discovery Search with UnderlyingQuoteRIC filter enumerates all options
  - NVDA: UnderlyingQuoteRIC eq 'NVDA.O'
  - AMD:  UnderlyingQuoteRIC eq 'AMD.O'
  - SPY:  UnderlyingQuoteRIC eq 'SPY' AND AssetCategory eq 'EIO'
- Filter to .U suffix (OPRA composite) for standard US equity options
- Skip chain RICs (0#, Z#), aggregate RICs, and flex options (.FO)
- 1-min bars: ~5.5 months retention (back to ~Oct 2025, 43K bars max)
- Trade ticks: 10K per request, ~2 weeks depth
- Quote ticks: similar
- Pagination via Top/Skip, max Top=100
- No visible rate limit headers but use 0.3s sleep between searches,
  0.5s between pricing requests
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os
import sys
import requests
import time
import pandas as pd

load_dotenv()

config = {
    "sessions": {
        "default": "platform.rdp",
        "platform": {
            "rdp": {
                "app-key": os.getenv("DSWS_APPKEY"),
                "username": os.getenv("DSWS_USERNAME"),
                "password": os.getenv("DSWS_PASSWORD"),
                "signon_control": True
            }
        }
    }
}

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lseg-data.config.json")
with open(config_path, "w") as f:
    json.dump(config, f, indent=4)

session = ld.open_session(config_name=config_path)
token = session._access_token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"


def refresh_token():
    global token, headers
    token = session._access_token
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def search(payload):
    resp = requests.post(SEARCH_URL, headers=headers, json=payload)
    if resp.status_code == 401:
        refresh_token()
        resp = requests.post(SEARCH_URL, headers=headers, json=payload)
    if resp.status_code != 200:
        print(f"  SEARCH ERROR {resp.status_code}: {resp.text[:300]}")
        return None
    return resp.json()


def hist_get(view, ric, params):
    url = f"{HIST_URL}/{view}/{ric}"
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 401:
        refresh_token()
        resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        print(f"  HIST ERROR {resp.status_code}: {resp.text[:300]}")
        return None
    return resp.json()


def is_standard_option_ric(ric):
    """Filter to standard OPRA equity options only."""
    if not isinstance(ric, str):
        return False
    if not ric.endswith(".U"):
        return False
    # Skip chain/aggregate RICs
    if ric.startswith("0#") or ric.startswith("Z#"):
        return False
    if "OPTTOT" in ric:
        return False
    return True


# =====================================================================
# PART 1: Enumerate ALL standard OPRA options for NVDA, AMD, SPY
# =====================================================================
print("=" * 70)
print("PART 1: ENUMERATE OPTION CONTRACTS")
print("=" * 70)

# Each underlying needs slightly different filter
underlyings = {
    "NVDA": "UnderlyingQuoteRIC eq 'NVDA.O'",
    "AMD": "UnderlyingQuoteRIC eq 'AMD.O'",
    "SPY": "UnderlyingQuoteRIC eq 'SPY' and AssetCategory eq 'EIO'",
}

all_contracts = []

for ticker, base_filter in underlyings.items():
    print(f"\n--- {ticker} ---")

    # Get total
    result = search({
        "Query": "",
        "View": "EquityDerivativeQuotes",
        "Select": "RIC,ExpiryDate,StrikePrice,CallPutOption",
        "Filter": f"{base_filter} and AssetState eq 'AC'",
        "Top": 1,
    })
    if not result:
        continue

    total = result.get("Total", 0)
    print(f"  Total in search: {total}")

    # Paginate
    skip = 0
    ticker_contracts = []
    while skip < total:
        result = search({
            "Query": "",
            "View": "EquityDerivativeQuotes",
            "Select": "RIC,ExpiryDate,StrikePrice,CallPutOption",
            "Filter": f"{base_filter} and AssetState eq 'AC'",
            "Top": 100,
            "Skip": skip,
        })
        if not result:
            break
        hits = result.get("Hits", [])
        if not hits:
            break
        for h in hits:
            ric = h.get("RIC", "")
            if not is_standard_option_ric(ric):
                continue
            ticker_contracts.append({
                "underlying": ticker,
                "ric": ric,
                "expiry": h.get("ExpiryDate", "")[:10],
                "strike": h.get("StrikePrice"),
                "cp": h.get("CallPutOption"),
            })
        skip += len(hits)
        if skip % 500 == 0 or skip >= total:
            print(f"  Fetched {skip}/{total} (kept {len(ticker_contracts)} standard)...")
        time.sleep(0.3)

    # Summary
    df = pd.DataFrame(ticker_contracts)
    if not df.empty:
        # Filter to active expiries only
        today = "2026-03-18"
        active = df[df["expiry"] >= today]
        expiries = sorted(active["expiry"].unique())
        calls = len(active[active["cp"] == "Call"])
        puts = len(active[active["cp"] == "Put"])
        no_cp = len(active[active["cp"].isna()])
        strikes = sorted(active["strike"].dropna().unique())
        print(f"  Standard OPRA options: {len(df)} total, {len(active)} active")
        print(f"  Active expiry dates: {len(expiries)}")
        print(f"    Nearest: {expiries[:5]}")
        print(f"    Farthest: {expiries[-5:]}")
        print(f"  Calls: {calls}, Puts: {puts}, Unknown CP: {no_cp}")
        if strikes:
            print(f"  Strike range: {strikes[0]} - {strikes[-1]}")

    all_contracts.extend(ticker_contracts)

# Save
contracts_df = pd.DataFrame(all_contracts)
# Filter to active only
contracts_df = contracts_df[contracts_df["expiry"] >= "2026-03-18"].copy()
contracts_df.to_csv("all_option_contracts.csv", index=False)
print(f"\nSaved {len(contracts_df)} active contracts to all_option_contracts.csv")
print(f"  NVDA: {len(contracts_df[contracts_df['underlying']=='NVDA'])}")
print(f"  AMD:  {len(contracts_df[contracts_df['underlying']=='AMD'])}")
print(f"  SPY:  {len(contracts_df[contracts_df['underlying']=='SPY'])}")


# =====================================================================
# PART 2: Intraday data retention tests
# =====================================================================
print("\n" + "=" * 70)
print("PART 2: INTRADAY DATA RETENTION TESTS")
print("=" * 70)

# Pick test options: near-ATM April 2026 calls
test_targets = [
    ("NVDA", 180, "2026-04-17"),
    ("AMD", 200, "2026-04-17"),
    ("SPY", 660, "2026-04-17"),
]

test_rics = []
for ticker, target_strike, target_exp in test_targets:
    sub = contracts_df[
        (contracts_df["underlying"] == ticker) &
        (contracts_df["expiry"] == target_exp) &
        (contracts_df["cp"] == "Call")
    ].copy()
    if sub.empty:
        print(f"  {ticker}: no April calls found, trying broader search...")
        sub = contracts_df[
            (contracts_df["underlying"] == ticker) &
            (contracts_df["expiry"] >= "2026-04-01") &
            (contracts_df["expiry"] <= "2026-04-30") &
            (contracts_df["cp"] == "Call")
        ].copy()
    if not sub.empty:
        sub["dist"] = abs(sub["strike"] - target_strike)
        best = sub.sort_values("dist").iloc[0]
        test_rics.append((ticker, best["ric"], best["strike"], best["expiry"]))
        print(f"  {ticker}: {best['ric']} K={best['strike']} exp={best['expiry']}")
    else:
        print(f"  {ticker}: NO OPTIONS FOUND")

print()

for ticker, ric, strike, expiry in test_rics:
    print(f"\n--- {ric} ({ticker} K={strike} exp={expiry}) ---")

    # 1-min bars
    data = hist_get("intraday-summaries", ric, {"count": "50000"})
    if data:
        for item in data:
            if "data" in item and item["data"]:
                rows = item["data"]
                real_rows = [r for r in rows if any(v is not None for v in r[1:])]
                print(f"  1-min bars: {len(rows)} total, {len(real_rows)} with data")
                if rows:
                    print(f"    Range: {rows[-1][0][:19]} to {rows[0][0][:19]}")
    time.sleep(0.5)

    # Trade ticks
    data = hist_get("events", ric, {"count": "50000", "eventTypes": "trade"})
    if data:
        for item in data:
            if "data" in item and item["data"]:
                ticks = item["data"]
                print(f"  Trade ticks: {len(ticks)}")
                if ticks:
                    print(f"    Range: {ticks[-1][0][:19]} to {ticks[0][0][:19]}")
    time.sleep(0.5)

    # Quote ticks
    data = hist_get("events", ric, {"count": "50000", "eventTypes": "quote"})
    if data:
        for item in data:
            if "data" in item and item["data"]:
                ticks = item["data"]
                print(f"  Quote ticks: {len(ticks)}")
                if ticks:
                    print(f"    Range: {ticks[-1][0][:19]} to {ticks[0][0][:19]}")
    time.sleep(0.5)


# =====================================================================
# PART 3: Test batch fetch + rate limit behavior
# =====================================================================
print("\n" + "=" * 70)
print("PART 3: BATCH FETCH + RATE LIMITS")
print("=" * 70)

# Test: how many sequential requests can we make without errors?
# Pick 10 random NVDA options and fetch 1-min bars for each
nvda_rics = contracts_df[
    (contracts_df["underlying"] == "NVDA") &
    (contracts_df["expiry"] == "2026-04-17") &
    (contracts_df["cp"] == "Call")
]["ric"].tolist()[:10]

print(f"\nRapid-fire test: 10 NVDA Apr-17 calls, 0.5s spacing")
for i, ric in enumerate(nvda_rics):
    t0 = time.time()
    data = hist_get("intraday-summaries", ric, {"count": "100"})
    elapsed = time.time() - t0
    if data:
        for item in data:
            if "data" in item:
                n = len(item["data"])
                print(f"  [{i+1}] {ric}: {n} bars ({elapsed:.1f}s)")
    else:
        print(f"  [{i+1}] {ric}: FAILED ({elapsed:.1f}s)")
    time.sleep(0.5)


ld.close_session()
os.remove(config_path)
print("\nDone.")
