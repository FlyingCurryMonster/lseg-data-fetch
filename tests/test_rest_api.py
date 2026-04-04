"""
Test the LSEGRestClient wrapper — verify all 3 REST endpoints work.

Tests:
  1. symbology_lookup with showHistory for AAPL ISIN
  2. search with AssetState eq 'DC' filter (expired instruments)
  3. historical_pricing for a known futures RIC (SDAZ27)
"""

import json
import sys
import lseg.data as ld
from dotenv import load_dotenv
import os

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

# --- Test 1: Symbology Lookup with showHistory ---
print("=" * 80)
print("TEST 1: symbology_lookup — AAPL ISIN with showHistory")
print("=" * 80)

try:
    df = rest.symbology_lookup_df(
        identifiers=["US0378331005"],
        from_types=["ISIN"],
        to_types=["RIC"],
        show_history=True,
    )
    print(f"Rows returned: {len(df)}")
    print(df.to_string())
except Exception as e:
    print(f"ERROR: {e}")

# --- Test 2: Discovery Search REST — expired instruments ---
print("\n" + "=" * 80)
print("TEST 2: search — expired S&P 500 dividend futures")
print("=" * 80)

try:
    result = rest.search(
        query="S&P 500 Annual Dividend Future",
        filter="AssetState eq 'DC'",
        select="RIC,DocumentTitle,ExpiryDate,AssetState,ExchangeName",
        top=20,
    )
    hits = result.get("Hits", [])
    print(f"Total hits: {result.get('Total', '?')}")
    for hit in hits[:10]:
        print(f"  {hit.get('RIC', '?'):20s}  {hit.get('DocumentTitle', '?')}")
    if not hits:
        print("  (no hits — check if filter syntax is correct)")
        print(f"  Raw response keys: {list(result.keys())}")
        print(f"  Response preview: {json.dumps(result, indent=2)[:500]}")
except Exception as e:
    print(f"ERROR: {e}")

# --- Test 3: Historical Pricing REST ---
print("\n" + "=" * 80)
print("TEST 3: historical_pricing — SDAZ27 last 10 days")
print("=" * 80)

try:
    result = rest.historical_pricing(
        ric="SDAZ27",
        start="2025-01-01",
        end="2025-01-15",
    )
    # Response structure varies; print what we get
    if isinstance(result, list):
        print(f"Got list with {len(result)} elements")
        for item in result[:3]:
            print(f"  {item}")
    elif isinstance(result, dict):
        print(f"Response keys: {list(result.keys())}")
        data = result.get("data", result.get("Data", []))
        if data:
            print(f"Data rows: {len(data)}")
            for row in data[:5]:
                print(f"  {row}")
        else:
            print(f"Response preview: {json.dumps(result, indent=2)[:500]}")
except Exception as e:
    print(f"ERROR: {e}")

# --- Cleanup ---
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
