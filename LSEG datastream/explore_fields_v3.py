"""
Explore fields v3: Comprehensive field discovery for dividend futures and options.

Strategy: Call get_history WITHOUT specifying fields to get ALL available fields.
Test on representative instruments from each product (SDA, SDI, FEXD) and type
(futures vs options).

Also test additional fields found via web search that we haven't tried:
  OPINT_1, IMP_VOLT, TRNOVR_UNS, VWAP, MID_PRICE, NUM_MOVES,
  OFF_CLOSE, MKT_HIGH, MKT_LOW, MKT_OPEN
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import os
import time

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

ld.open_session(config_name=config_path)

end = datetime.now().strftime("%Y-%m-%d")
start_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

# Representative instruments to test
test_instruments = {
    "SDA future (active)":   "SDAZ26",
    "SDA future (expired)":  "SDAZ25^2",
    "SDI future (active)":   "SDIM26",
    "FEXD future (active)":  "FEXDZ26",
    "SDA option (call)":     "1SDA85L27",
    "SDA option (put)":      "1SDA64X27",
    "FEXD option (call)":    "FEXD3500L6",
    "FEXD option (put)":     "FEXD7500X6",
}

# ============================================================
# TEST 1: Get ALL fields by omitting the fields parameter
# ============================================================
print("=" * 80)
print("TEST 1: All available fields (no fields param) — last 30 days")
print("=" * 80)

all_fields_found = {}

for label, ric in test_instruments.items():
    print(f"\n--- {label}: {ric} ---")
    try:
        data = ld.get_history(
            universe=[ric],
            start=start_30d,
            end=end
        )
        if data is not None and not data.empty:
            cols = list(data.columns)
            all_fields_found[ric] = cols
            non_null = {c: data[c].notna().sum() for c in cols}
            print(f"  Shape: {data.shape}")
            print(f"  Columns ({len(cols)}): {cols}")
            print(f"  Non-null counts:")
            for c, cnt in non_null.items():
                print(f"    {c:20s}: {cnt}/{len(data)} rows")
        else:
            print(f"  No data returned")
    except Exception as e:
        print(f"  ERROR: {e}")
    time.sleep(1)

# ============================================================
# TEST 2: Test additional fields from web search
# ============================================================
print("\n" + "=" * 80)
print("TEST 2: Test additional candidate fields individually")
print("=" * 80)

extra_fields = [
    "OPINT_1",      # open interest (alternate name)
    "OPINT",        # open interest (original name)
    "IMP_VOLT",     # implied volatility
    "TRNOVR_UNS",   # turnover unsigned
    "VWAP",         # volume-weighted avg price
    "MID_PRICE",    # mid price
    "NUM_MOVES",    # number of trades
    "OFF_CLOSE",    # official close
    "HST_CLOSE",    # historical close (prev day)
    "CF_CLOSE",     # close
    "MKT_HIGH",     # market high
    "MKT_LOW",      # market low
    "MKT_OPEN",     # market open
    "BID",          # best bid
    "ASK",          # best ask
    "BIDSIZE",      # bid size
    "ASKSIZE",      # ask size
    "PERATIO",      # PE ratio (probably N/A for futures)
    "ORDBK_VOL",    # order book volume
]

# Test on one future and one option
test_rics = ["SDAZ26", "1SDA85L27"]

for ric in test_rics:
    print(f"\n--- Testing extra fields on {ric} ---")
    for field in extra_fields:
        try:
            data = ld.get_history(
                universe=[ric],
                fields=[field],
                start=start_30d,
                end=end
            )
            if data is not None and not data.empty:
                non_null = data[field].notna().sum() if field in str(data.columns) else 0
                print(f"  {field:15s} -> {data.shape[0]} rows, {non_null} non-null")
            else:
                print(f"  {field:15s} -> empty/None")
        except Exception as e:
            err = str(e)[:60]
            print(f"  {field:15s} -> ERROR: {err}")
        time.sleep(0.3)

# ============================================================
# TEST 3: Same tests on FEXD
# ============================================================
print("\n" + "=" * 80)
print("TEST 3: Test extra fields on FEXD future and option")
print("=" * 80)

fexd_rics = ["FEXDZ26", "FEXD3500L6"]

for ric in fexd_rics:
    print(f"\n--- Testing extra fields on {ric} ---")
    for field in extra_fields:
        try:
            data = ld.get_history(
                universe=[ric],
                fields=[field],
                start=start_30d,
                end=end
            )
            if data is not None and not data.empty:
                non_null = data[field].notna().sum() if field in str(data.columns) else 0
                print(f"  {field:15s} -> {data.shape[0]} rows, {non_null} non-null")
            else:
                print(f"  {field:15s} -> empty/None")
        except Exception as e:
            err = str(e)[:60]
            print(f"  {field:15s} -> ERROR: {err}")
        time.sleep(0.3)

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 80)
print("SUMMARY: Default fields returned per instrument type")
print("=" * 80)

if all_fields_found:
    # Find common vs unique fields across instrument types
    all_cols = set()
    for cols in all_fields_found.values():
        all_cols.update(cols)

    print(f"\nAll unique fields seen: {sorted(all_cols)}")
    print(f"\nField availability matrix:")
    print(f"  {'Field':20s}", end="")
    for ric in all_fields_found:
        print(f"  {ric:>12s}", end="")
    print()
    for col in sorted(all_cols):
        print(f"  {col:20s}", end="")
        for ric, cols in all_fields_found.items():
            present = "Y" if col in cols else "-"
            print(f"  {present:>12s}", end="")
        print()

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
