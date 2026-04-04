"""
Explore available fields and metadata for dividend futures options via LSEG Data Library.

Goals:
1. Understand what the column names (TRDPRC_1, HIGH_1, etc.) mean
2. Figure out how to get strike, expiry, and call/put flag from the API
3. Discover what fields are available for futures vs options
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os

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

ld.open_session(config_name=config_path)

# --- Test 1: Snapshot metadata for a call, a put, and a future ---
print("=" * 80)
print("TEST 1: Snapshot metadata fields for option call, put, and underlying future")
print("=" * 80)
sample_rics = ["1SDA85L27", "1SDA64X27", "SDAc1"]
metadata_fields = [
    "DSPLY_NAME", "CF_NAME",           # display names
    "STRIKE_PRC", "EXPIR_DATE",        # strike and expiry
    "PUT_CALL", "PUTCALLIND",          # call/put indicator
    "UNDERLYING_RIC",                  # underlying RIC
    "CF_CURR", "CONTR_MNTH",           # currency, contract month
    "LOTSZUNITS",                      # lot size
    "SETTLE", "TRDPRC_1",             # settlement, last trade
    "BID", "ASK",                      # bid/ask
]

try:
    data1 = ld.get_data(universe=sample_rics, fields=metadata_fields)
    print(data1.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 2: Price/volume fields explanation ---
print("\n" + "=" * 80)
print("TEST 2: All pricing fields for a sample option")
print("=" * 80)
pricing_fields = [
    "TRDPRC_1",    # last trade price
    "HIGH_1",      # daily high
    "LOW_1",       # daily low
    "OPEN_PRC",    # open price
    "CF_CLOSE",    # official close
    "HST_CLOSE",   # historical close (previous day)
    "SETTLE",      # settlement price
    "BID",         # best bid
    "ASK",         # best ask
    "ACVOL_UNS",   # accumulated volume (unsigned)
    "OPINT",       # open interest
    "NUM_MOVES",   # number of trades
]

try:
    data2 = ld.get_data(universe=["1SDA85L27"], fields=pricing_fields)
    print(data2.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 3: Check what fields are available in get_history ---
print("\n" + "=" * 80)
print("TEST 3: Historical data with all candidate fields for an option (last 5 days)")
print("=" * 80)
hist_fields = [
    "TRDPRC_1", "HIGH_1", "LOW_1", "OPEN_PRC",
    "SETTLE", "BID", "ASK",
    "ACVOL_UNS", "OPINT",
]

try:
    from datetime import datetime, timedelta
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    data3 = ld.get_history(
        universe=["1SDA85L27"],
        fields=hist_fields,
        start=start,
        end=end
    )
    print(data3.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 4: How far back can we go? ---
print("\n" + "=" * 80)
print("TEST 4: Historical data for SDAc1 going back 10 years")
print("=" * 80)
try:
    data4 = ld.get_history(
        universe=["SDAc1"],
        fields=["TRDPRC_1", "SETTLE", "ACVOL_UNS", "OPINT"],
        start="2014-01-01",
        end=end
    )
    print(f"Shape: {data4.shape}")
    print(f"Date range: {data4.index.min()} to {data4.index.max()}")
    print(data4.head(5))
    print("...")
    print(data4.tail(5))
except Exception as e:
    print(f"Error: {e}")

# --- Test 5: Euro Stoxx 50 dividend future metadata ---
print("\n" + "=" * 80)
print("TEST 5: Euro Stoxx 50 dividend future (FEXDc1) metadata")
print("=" * 80)
try:
    data5 = ld.get_data(
        universe=["FEXDc1", "FEXDc2", "FEXDc3"],
        fields=metadata_fields
    )
    print(data5.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
