"""
Fetch intraday option pricing data from LSEG for near-ATM April 2026 options
on NVDA, AMD, and SPY.

Target contracts (April 17, 2026 expiry, Calls):
  - NVDA $180 Call  (NVDA ~$180 as of 2026-03-18)
  - AMD  $200 Call  (AMD  ~$200 as of 2026-03-18)
  - SPY  $660 Call  (SPY  ~$661 as of 2026-03-18)

This script:
  1. Uses LSEG Discovery Search to find the correct option RICs
  2. Fetches intraday pricing data (1-minute bars) via ld.get_history()
  3. Saves results to CSV

OptionMetrics references (for cross-validation):
  - NVDA $180 C Apr-17-2026: optionid 171443316
  - AMD  $200 C Apr-17-2026: optionid 171154093
"""

import json
import sys
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.lseg_rest_api import LSEGRestClient

load_dotenv()

# --- Session setup ---
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
rest = LSEGRestClient(session)

# =====================================================================
# Step 1: Find option RICs via Discovery Search
# =====================================================================
# LSEG option RIC format for US equity options:
#   <ROOT><expiry><strike><C/P>.U
# e.g., NVDA260417C00180000.U  (NVDA, 2026-04-17, Call, $180)
#
# We'll search to confirm the RICs exist.

targets = [
    {"underlying": "NVDA", "strike": 180, "cp": "C", "expiry": "2026-04-17"},
    {"underlying": "AMD",  "strike": 200, "cp": "C", "expiry": "2026-04-17"},
    {"underlying": "SPY",  "strike": 660, "cp": "C", "expiry": "2026-04-17"},
]

print("=" * 70)
print("STEP 1: SEARCHING FOR OPTION RICs")
print("=" * 70)

found_rics = []

for t in targets:
    query = f"{t['underlying']} {t['strike']} Call April 2026"
    print(f"\nSearching: {query}")

    try:
        result = rest.search(
            query=query,
            view="EquityDerivativeQuotes",
            select="RIC,DocumentTitle,ExpiryDate,StrikePrice,CallPutOption,ExchangeName",
            filter=f"ExpiryDate ge 2026-04-17 and ExpiryDate le 2026-04-17 "
                   f"and StrikePrice eq {t['strike']} "
                   f"and CallPutOption eq 'CALL' "
                   f"and RIC ne null",
            top=20
        )

        hits = result.get("Hits", [])
        print(f"  Found {len(hits)} results:")
        for hit in hits:
            ric = hit.get("RIC", "?")
            title = hit.get("DocumentTitle", "?")
            expiry = hit.get("ExpiryDate", "?")
            strike = hit.get("StrikePrice", "?")
            exchange = hit.get("ExchangeName", "?")
            print(f"    RIC={ric}  Title={title}  Expiry={expiry}  Strike={strike}  Exchange={exchange}")
            found_rics.append({
                "underlying": t["underlying"],
                "ric": ric,
                "title": title,
                "expiry": expiry,
                "strike": strike,
                "exchange": exchange,
            })

    except Exception as e:
        print(f"  ERROR: {e}")

    time.sleep(1)

# Show summary of found RICs
print("\n" + "=" * 70)
print("FOUND RICs SUMMARY")
print("=" * 70)
if found_rics:
    ric_df = pd.DataFrame(found_rics)
    print(ric_df.to_string(index=False))
    ric_df.to_csv("option_rics.csv", index=False)
    print("\nSaved RIC list to option_rics.csv")
else:
    print("No RICs found! Try adjusting search parameters.")

# =====================================================================
# Step 2: Fetch intraday pricing data
# =====================================================================
# ld.get_history supports interval="minute" for intraday data

# Use the first RIC found for each underlying
rics_to_fetch = []
for t in targets:
    matches = [r for r in found_rics if r["underlying"] == t["underlying"]]
    if matches:
        # Prefer the composite/main exchange RIC (usually the shortest or .U suffix)
        rics_to_fetch.append(matches[0]["ric"])
    else:
        print(f"WARNING: No RIC found for {t['underlying']} — skipping")

if not rics_to_fetch:
    print("No RICs to fetch. Exiting.")
    ld.close_session()
    os.remove(config_path)
    sys.exit(1)

print("\n" + "=" * 70)
print(f"STEP 2: FETCHING INTRADAY DATA FOR {rics_to_fetch}")
print("=" * 70)

all_data = []

for ric in rics_to_fetch:
    print(f"\nFetching intraday data for {ric}...")

    try:
        # Try 1-minute bars for last few trading days
        data = ld.get_history(
            universe=ric,
            interval="minute",
            start=(datetime.now() - timedelta(days=5)).strftime("%Y-%m-%dT00:00:00"),
            end=datetime.now().strftime("%Y-%m-%dT23:59:59"),
        )

        if data is not None and not data.empty:
            data["RIC"] = ric
            data.index.name = "timestamp"
            df = data.reset_index()
            all_data.append(df)
            print(f"  Got {len(df)} rows, columns: {list(df.columns)}")
            print(f"  Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            print(f"  Sample:")
            print(df.head(3).to_string(index=False))
        else:
            print(f"  No data returned for {ric}")

    except Exception as e:
        print(f"  ERROR: {e}")

        # Fallback: try 5-minute bars
        print(f"  Retrying with 5-minute interval...")
        try:
            data = ld.get_history(
                universe=ric,
                interval="five_minutes",
                start=(datetime.now() - timedelta(days=5)).strftime("%Y-%m-%dT00:00:00"),
                end=datetime.now().strftime("%Y-%m-%dT23:59:59"),
            )
            if data is not None and not data.empty:
                data["RIC"] = ric
                data.index.name = "timestamp"
                df = data.reset_index()
                all_data.append(df)
                print(f"  Got {len(df)} rows (5-min bars)")
                print(f"  Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            else:
                print(f"  Still no data for {ric}")
        except Exception as e2:
            print(f"  5-min also failed: {e2}")

    time.sleep(2)

# =====================================================================
# Step 3: Save results
# =====================================================================
if all_data:
    result = pd.concat(all_data, ignore_index=True)
    output_file = "intraday_option_prices.csv"
    result.to_csv(output_file, index=False)

    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Total rows: {len(result)}")
    print(f"Columns: {list(result.columns)}")
    for ric in result["RIC"].unique():
        subset = result[result["RIC"] == ric]
        print(f"  {ric}: {len(subset)} rows, {subset['timestamp'].min()} to {subset['timestamp'].max()}")
    print(f"\nSaved to {output_file}")
else:
    print("\nNo intraday data was retrieved.")

ld.close_session()
os.remove(config_path)
