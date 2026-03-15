"""
Follow-up enumeration to find Euro Stoxx 50 dividend options.
The first attempt returned 0 — try different search queries.
Also check if SDI (quarterly) options exist at all.
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
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

# ============================================================
# TEST 1: Try various search queries for FEXD options
# ============================================================
print("=" * 80)
print("TEST 1: Try different search queries for Euro Stoxx 50 dividend options")
print("=" * 80)

queries_to_try = [
    "EURO STOXX 50 dividend option Eurex",
    "FEXD option",
    "Euro Stoxx 50 Dividend Future Option",
    "EURO STOXX 50 Index Dividend Future Option",
    "Eurex EURO STOXX 50 dividend option",
    "STOXX 50 dividend index option Eurex",
    "Eurex dividend index option",
]

for q in queries_to_try:
    print(f"\nQuery: '{q}'")
    results = ld.discovery.search(
        query=q,
        top=10,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,AssetCategory"
    )
    if not results.empty:
        print(f"  Found {len(results)} results")
        for _, row in results.head(5).iterrows():
            print(f"    {row['RIC']:20s} | {str(row.get('StrikePrice','')):>8s} | {row.get('DocumentTitle','')[:80]}")
    else:
        print("  No results")
    time.sleep(0.5)

# ============================================================
# TEST 2: Try searching by Eurex option asset category
# ============================================================
print("\n" + "=" * 80)
print("TEST 2: Search Eurex options broadly")
print("=" * 80)

results = ld.discovery.search(
    query="Eurex dividend future option",
    top=30,
    select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,ExchangeName,AssetCategory"
)
if not results.empty:
    print(f"Found {len(results)} results")
    print(results[["RIC", "DocumentTitle"]].to_string())
else:
    print("No results")

# ============================================================
# TEST 3: Search for SDI options broadly
# ============================================================
print("\n" + "=" * 80)
print("TEST 3: Search for quarterly dividend options broadly")
print("=" * 80)

queries_sdi = [
    "S&P 500 Quarterly Dividend option",
    "SDI option CME",
    "quarterly dividend index option",
]

for q in queries_sdi:
    print(f"\nQuery: '{q}'")
    results = ld.discovery.search(
        query=q,
        top=10,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,AssetCategory"
    )
    if not results.empty:
        print(f"  Found {len(results)} results")
        for _, row in results.head(5).iterrows():
            print(f"    {row['RIC']:20s} | {row.get('DocumentTitle','')[:80]}")
    else:
        print("  No results")
    time.sleep(0.5)

# ============================================================
# TEST 4: Check the option chain RICs we found earlier
# ============================================================
print("\n" + "=" * 80)
print("TEST 4: Search using known chain RICs as clues")
print("=" * 80)

# We know 0#1SDAZ26+ is a chain RIC. Search for related terms
results = ld.discovery.search(
    query="EURO STOXX 50 Index Dividend Option chain",
    top=20,
    select="DocumentTitle,RIC,AssetCategory,ExchangeName"
)
if not results.empty:
    print(f"Found {len(results)} results")
    print(results[["RIC", "DocumentTitle"]].to_string())
else:
    print("No results")

# ============================================================
# TEST 5: Look for OEXD (options on FEXD)
# ============================================================
print("\n" + "=" * 80)
print("TEST 5: Search for OEXD or other Eurex dividend option products")
print("=" * 80)

queries = [
    "OEXD Eurex",
    "Eurex STOXX dividend option chain",
    "FEXD option chain Eurex",
]

for q in queries:
    print(f"\nQuery: '{q}'")
    results = ld.discovery.search(
        query=q,
        top=10,
        select="DocumentTitle,RIC,AssetCategory,ExchangeName"
    )
    if not results.empty:
        print(f"  Found {len(results)} results")
        for _, row in results.head(5).iterrows():
            print(f"    {row['RIC']:20s} | {row.get('DocumentTitle','')[:80]}")
    else:
        print("  No results")
    time.sleep(0.5)

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
