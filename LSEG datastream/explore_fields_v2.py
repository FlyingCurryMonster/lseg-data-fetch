"""
Explore fields v2:
- Try get_data with individual fields to find which ones work
- Use discovery.search to get option metadata (strike, expiry, cp_flag)
- Test get_history with OPINT separately
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

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
start_5d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

# --- Test 1: Discovery search to get option metadata ---
print("=" * 80)
print("TEST 1: Use discovery.search to get option metadata (strike, expiry, cp)")
print("=" * 80)

# Search for all SDA options on Dec 2027
results = ld.discovery.search(
    query='S&P 500 Annual Dividend Electronic Equity Index Option Dec 2027',
    top=10,
    select='DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetCategory'
)
print(results.to_string())

# --- Test 2: Search for SDA options Dec 2026 with metadata ---
print("\n" + "=" * 80)
print("TEST 2: Dec 2026 options with metadata")
print("=" * 80)

results2 = ld.discovery.search(
    query='S&P 500 Annual Dividend Electronic Equity Index Option Dec 2026',
    top=20,
    select='DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC'
)
print(results2.to_string())

# --- Test 3: Get the full option chain using chain RIC ---
print("\n" + "=" * 80)
print("TEST 3: Try to get option chain via discovery.Chain")
print("=" * 80)

try:
    # SDA is the annual dividend future - try to find option chains
    chain_results = ld.discovery.search(
        query='S&P 500 Annual Dividend option chain',
        top=10,
        select='DocumentTitle,RIC,ExchangeName,AssetCategory'
    )
    print(chain_results.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 4: Try Chain object for option chains ---
print("\n" + "=" * 80)
print("TEST 4: Try ld.discovery.Chain for SDA Dec 2026 options")
print("=" * 80)

try:
    # Try various chain RIC formats for SDA options
    for chain_ric in ["0#1SDA*.L26", "0#SDA+2612", "0#1SDA+", "0#SDA+L26"]:
        print(f"\nTrying chain: {chain_ric}")
        try:
            chain = ld.discovery.Chain(chain_ric)
            constituents = chain.constituents
            print(f"  Found {len(constituents)} constituents")
            print(f"  First 10: {constituents[:10]}")
        except Exception as e:
            print(f"  Error: {e}")
except Exception as e:
    print(f"Error: {e}")

# --- Test 5: What history fields work for options ---
print("\n" + "=" * 80)
print("TEST 5: Test each field individually in get_history for option")
print("=" * 80)

test_fields = [
    "TRDPRC_1", "HIGH_1", "LOW_1", "OPEN_PRC",
    "SETTLE", "BID", "ASK",
    "ACVOL_UNS", "OPINT", "NUM_MOVES",
    "IMP_VOLT",  # implied volatility
]

for field in test_fields:
    try:
        data = ld.get_history(
            universe=["1SDA85L27"],
            fields=[field],
            start=start_5d,
            end=end
        )
        has_data = data[field].notna().any() if field in str(data.columns) else False
        print(f"  {field:15s} -> shape={data.shape}, has_data={not data.empty}")
    except Exception as e:
        print(f"  {field:15s} -> ERROR: {e}")

# --- Test 6: Euro Stoxx dividend option search ---
print("\n" + "=" * 80)
print("TEST 6: Search for Euro Stoxx 50 dividend options")
print("=" * 80)

results6 = ld.discovery.search(
    query='Euro Stoxx 50 dividend option',
    top=15,
    select='DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,ExchangeName,AssetCategory'
)
print(results6.to_string())

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
