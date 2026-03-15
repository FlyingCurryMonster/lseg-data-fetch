"""
Enumerate EXPIRED dividend futures and options.

Expired instruments on LSEG get RICs with ^ suffix.
We need to search for these separately since discovery.search
only returns active contracts by default.

Strategy:
  - Search with AssetState filter for inactive/expired
  - Search for known expired RIC patterns (e.g., SDAZ25^2)
  - Try searching by year for older expiries (2015-2025)
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
# PART 1: Search for expired SDA futures
# ============================================================
print("=" * 80)
print("PART 1: Expired S&P 500 Annual Dividend Futures")
print("=" * 80)

all_expired_futures = []

# Try searching with inactive state
for year in range(2015, 2026):
    print(f"\n  Searching expired SDA futures Dec {year}...")
    results = ld.discovery.search(
        query=f"S&P 500 Annual Dividend Electronic Equity Index Future Dec {year}",
        top=20,
        select="DocumentTitle,RIC,ExpiryDate,ExchangeName,AssetCategory,AssetState"
    )
    if not results.empty:
        # Filter to individual contracts (not chains/continuations)
        contracts = results[~results["RIC"].str.contains("0#|c\\d", regex=True, na=False)]
        if not contracts.empty:
            contracts = contracts.copy()
            contracts["Product"] = "SDA"
            contracts["ExpiryYear"] = year
            all_expired_futures.append(contracts)
            print(f"    Found {len(contracts)} contracts")
            for _, row in contracts.iterrows():
                print(f"      {row['RIC']:20s} | state={row.get('AssetState','')} | {row.get('DocumentTitle','')[:60]}")
        else:
            print(f"    No individual contracts")
    else:
        print(f"    No results")
    time.sleep(0.5)

# ============================================================
# PART 2: Search for expired FEXD futures
# ============================================================
print("\n" + "=" * 80)
print("PART 2: Expired Euro Stoxx 50 Dividend Futures")
print("=" * 80)

for year in range(2015, 2026):
    print(f"\n  Searching expired FEXD futures Dec {year}...")
    results = ld.discovery.search(
        query=f"EURO STOXX 50 Index Dividend Future Dec {year}",
        top=20,
        select="DocumentTitle,RIC,ExpiryDate,ExchangeName,AssetCategory,AssetState"
    )
    if not results.empty:
        contracts = results[~results["RIC"].str.contains("0#|c\\d", regex=True, na=False)]
        # Filter out spreads
        contracts = contracts[~contracts["RIC"].str.contains("-", na=False)]
        if not contracts.empty:
            contracts = contracts.copy()
            contracts["Product"] = "FEXD"
            contracts["ExpiryYear"] = year
            all_expired_futures.append(contracts)
            print(f"    Found {len(contracts)} contracts")
            for _, row in contracts.iterrows():
                print(f"      {row['RIC']:20s} | state={row.get('AssetState','')} | {row.get('DocumentTitle','')[:60]}")
        else:
            print(f"    No individual contracts")
    else:
        print(f"    No results")
    time.sleep(0.5)

# ============================================================
# PART 3: Search for expired SDI futures
# ============================================================
print("\n" + "=" * 80)
print("PART 3: Expired S&P 500 Quarterly Dividend Futures")
print("=" * 80)

for year in range(2015, 2026):
    for month_name in ["Mar", "Jun", "Sep", "Dec"]:
        results = ld.discovery.search(
            query=f"S&P 500 Quarterly Dividend Electronic Equity Index Future {month_name} {year}",
            top=10,
            select="DocumentTitle,RIC,ExpiryDate,ExchangeName,AssetCategory,AssetState"
        )
        if not results.empty:
            contracts = results[~results["RIC"].str.contains("0#|c\\d", regex=True, na=False)]
            if not contracts.empty:
                contracts = contracts.copy()
                contracts["Product"] = "SDI"
                contracts["ExpiryYear"] = year
                all_expired_futures.append(contracts)
                print(f"  SDI {month_name} {year}: {len(contracts)} contracts")
                for _, row in contracts.iterrows():
                    print(f"    {row['RIC']:20s} | state={row.get('AssetState','')} | {row.get('DocumentTitle','')[:60]}")
        time.sleep(0.3)

expired_futures_df = pd.concat(all_expired_futures, ignore_index=True) if all_expired_futures else pd.DataFrame()
expired_futures_df = expired_futures_df.drop_duplicates(subset=["RIC"]) if not expired_futures_df.empty else expired_futures_df
print(f"\nTotal expired futures found: {len(expired_futures_df)}")
expired_futures_df.to_csv("enumerated_expired_futures.csv", index=False)

# ============================================================
# PART 4: Search for expired SDA options
# ============================================================
print("\n" + "=" * 80)
print("PART 4: Expired SDA options (Dec 2015-2025)")
print("=" * 80)

all_expired_options = []

for year in range(2015, 2026):
    print(f"\n  Searching expired SDA options Dec {year}...")
    results = ld.discovery.search(
        query=f"S&P 500 Annual Dividend Electronic Equity Index Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "SDA"
            options_only["ExpiryYear"] = year
            all_expired_options.append(options_only)
            print(f"    Found {len(options_only)} options")
        else:
            print(f"    No individual options")
    else:
        print(f"    No results")
    time.sleep(0.5)

    # Also composite
    results = ld.discovery.search(
        query=f"S&P 500 Annual Dividend Composite Equity Index Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "SDA_composite"
            options_only["ExpiryYear"] = year
            all_expired_options.append(options_only)
            print(f"    + {len(options_only)} composite options")
    time.sleep(0.5)

# ============================================================
# PART 5: Search for expired FEXD options
# ============================================================
print("\n" + "=" * 80)
print("PART 5: Expired FEXD options (Dec 2015-2025)")
print("=" * 80)

for year in range(2015, 2026):
    print(f"\n  Searching expired FEXD options Dec {year}...")
    results = ld.discovery.search(
        query=f"Eurex EURO STOXX 50 Indx Div Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "FEXD"
            options_only["ExpiryYear"] = year
            all_expired_options.append(options_only)
            print(f"    Found {len(options_only)} options")
        else:
            print(f"    No individual options")
    else:
        print(f"    No results")
    time.sleep(0.5)

expired_options_df = pd.concat(all_expired_options, ignore_index=True) if all_expired_options else pd.DataFrame()
expired_options_df = expired_options_df.drop_duplicates(subset=["RIC"]) if not expired_options_df.empty else expired_options_df
print(f"\nTotal expired options found: {len(expired_options_df)}")
if not expired_options_df.empty:
    print(f"  By product: {expired_options_df.groupby('Product').size().to_dict()}")
    print(f"  By year: {expired_options_df.groupby('ExpiryYear').size().to_dict()}")
expired_options_df.to_csv("enumerated_expired_options.csv", index=False)

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Expired futures found:  {len(expired_futures_df)}")
print(f"Expired options found:  {len(expired_options_df)}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\nDone.")
