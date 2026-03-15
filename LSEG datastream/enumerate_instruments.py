"""
Enumerate all dividend futures and their options available via LSEG Data Library.

Products of interest:
  - CME S&P 500 Annual Dividend Futures (SDA) + options
  - CME S&P 500 Quarterly Dividend Futures (SDI) + options
  - Eurex Euro Stoxx 50 Dividend Futures (FEXD) + options

Approach:
  - Use discovery.search to find all contracts (futures + options)
  - Capture metadata: RIC, strike, expiry, call/put, underlying
  - Save results to CSV for review
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import pandas as pd
import os
import time

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

# ============================================================
# PART 1: Enumerate all FUTURES contracts
# ============================================================
print("=" * 80)
print("PART 1: Enumerating dividend futures contracts")
print("=" * 80)

futures_queries = [
    {
        "name": "S&P 500 Annual Dividend Futures",
        "query": "S&P 500 Annual Dividend Electronic Equity Index Future",
        "product": "SDA",
        "exchange": "CME"
    },
    {
        "name": "S&P 500 Quarterly Dividend Futures",
        "query": "S&P 500 Quarterly Dividend Electronic Equity Index Future",
        "product": "SDI",
        "exchange": "CME"
    },
    {
        "name": "Euro Stoxx 50 Dividend Futures",
        "query": "EURO STOXX 50 Index Dividend Future",
        "product": "FEXD",
        "exchange": "Eurex"
    },
]

all_futures = []

for fq in futures_queries:
    print(f"\nSearching: {fq['name']}...")
    results = ld.discovery.search(
        query=fq["query"],
        top=100,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,ExchangeName,AssetCategory,AssetState"
    )
    if not results.empty:
        results["Product"] = fq["product"]
        results["ProductName"] = fq["name"]
        all_futures.append(results)
        print(f"  Found {len(results)} results")
        # Show unique RICs (exclude chains)
        non_chain = results[~results["RIC"].str.startswith("0#")]
        print(f"  Individual contracts: {len(non_chain)}")
        print(f"  RICs: {non_chain['RIC'].tolist()}")
    else:
        print(f"  No results found")
    time.sleep(0.5)  # rate limit

futures_df = pd.concat(all_futures, ignore_index=True) if all_futures else pd.DataFrame()
print(f"\nTotal futures results: {len(futures_df)}")
futures_df.to_csv("enumerated_futures.csv", index=False)
print("Saved to enumerated_futures.csv")

# ============================================================
# PART 2: Enumerate OPTIONS on S&P 500 Annual Dividend Futures
# ============================================================
print("\n" + "=" * 80)
print("PART 2: Enumerating options on S&P 500 Annual Dividend Futures")
print("=" * 80)

# Search per expiry year to get more complete coverage
all_sda_options = []
for year in range(2024, 2036):
    print(f"\n  Searching SDA options Dec {year}...")
    results = ld.discovery.search(
        query=f"S&P 500 Annual Dividend Electronic Equity Index Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
    )
    if not results.empty:
        # Filter out chain RICs
        options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "SDA"
            options_only["ExpiryYear"] = year
            all_sda_options.append(options_only)
            print(f"    Found {len(options_only)} individual options")
        else:
            print(f"    No individual options (only chains)")
    else:
        print(f"    No results")
    time.sleep(0.5)

# Also search for composite options (SDA without 1 prefix)
for year in range(2024, 2036):
    print(f"\n  Searching SDA composite options Dec {year}...")
    results = ld.discovery.search(
        query=f"S&P 500 Annual Dividend Composite Equity Index Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "SDA_composite"
            options_only["ExpiryYear"] = year
            all_sda_options.append(options_only)
            print(f"    Found {len(options_only)} individual options")
        else:
            print(f"    No individual options")
    else:
        print(f"    No results")
    time.sleep(0.5)

sda_options_df = pd.concat(all_sda_options, ignore_index=True) if all_sda_options else pd.DataFrame()
print(f"\nTotal SDA options found: {len(sda_options_df)}")
if not sda_options_df.empty:
    print(f"  Unique expiry dates: {sorted(sda_options_df['ExpiryDate'].dropna().unique())}")
    print(f"  Strike range: {sda_options_df['StrikePrice'].min()} - {sda_options_df['StrikePrice'].max()}")
    print(f"  Calls: {(sda_options_df['DocumentTitle'].str.contains('Call', na=False)).sum()}")
    print(f"  Puts: {(sda_options_df['DocumentTitle'].str.contains('Put', na=False)).sum()}")
sda_options_df.to_csv("enumerated_sda_options.csv", index=False)
print("Saved to enumerated_sda_options.csv")

# ============================================================
# PART 3: Enumerate OPTIONS on S&P 500 Quarterly Dividend Futures
# ============================================================
print("\n" + "=" * 80)
print("PART 3: Enumerating options on S&P 500 Quarterly Dividend Futures")
print("=" * 80)

all_sdi_options = []
for year in range(2024, 2030):
    for month_name in ["Mar", "Jun", "Sep", "Dec"]:
        print(f"\n  Searching SDI options {month_name} {year}...")
        results = ld.discovery.search(
            query=f"S&P 500 Quarterly Dividend Electronic Equity Index Option {month_name} {year}",
            top=200,
            select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
        )
        if not results.empty:
            options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
            if not options_only.empty:
                options_only = options_only.copy()
                options_only["Product"] = "SDI"
                options_only["ExpiryYear"] = year
                all_sdi_options.append(options_only)
                print(f"    Found {len(options_only)} individual options")
            else:
                print(f"    No individual options")
        else:
            print(f"    No results")
        time.sleep(0.3)

sdi_options_df = pd.concat(all_sdi_options, ignore_index=True) if all_sdi_options else pd.DataFrame()
print(f"\nTotal SDI options found: {len(sdi_options_df)}")
sdi_options_df.to_csv("enumerated_sdi_options.csv", index=False)
print("Saved to enumerated_sdi_options.csv")

# ============================================================
# PART 4: Enumerate OPTIONS on Euro Stoxx 50 Dividend Futures
# ============================================================
print("\n" + "=" * 80)
print("PART 4: Enumerating options on Euro Stoxx 50 Dividend Futures")
print("=" * 80)

all_fexd_options = []
for year in range(2024, 2036):
    print(f"\n  Searching FEXD options Dec {year}...")
    results = ld.discovery.search(
        query=f"EURO STOXX 50 Index Dividend Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "FEXD"
            options_only["ExpiryYear"] = year
            all_fexd_options.append(options_only)
            print(f"    Found {len(options_only)} individual options")
        else:
            print(f"    No individual options")
    else:
        print(f"    No results")
    time.sleep(0.5)

# Also try quarterly expiries for FEXD
for year in range(2024, 2030):
    for month_name in ["Mar", "Jun", "Sep"]:
        print(f"\n  Searching FEXD options {month_name} {year}...")
        results = ld.discovery.search(
            query=f"EURO STOXX 50 Index Dividend Option {month_name} {year}",
            top=200,
            select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
        )
        if not results.empty:
            options_only = results[~results["RIC"].str.contains("0#|\\+$", regex=True, na=False)]
            if not options_only.empty:
                options_only = options_only.copy()
                options_only["Product"] = "FEXD"
                options_only["ExpiryYear"] = year
                all_fexd_options.append(options_only)
                print(f"    Found {len(options_only)} individual options")
            else:
                print(f"    No individual options")
        else:
            print(f"    No results")
        time.sleep(0.3)

fexd_options_df = pd.concat(all_fexd_options, ignore_index=True) if all_fexd_options else pd.DataFrame()
print(f"\nTotal FEXD options found: {len(fexd_options_df)}")
if not fexd_options_df.empty:
    print(f"  Unique expiry dates: {sorted(fexd_options_df['ExpiryDate'].dropna().unique())}")
    print(f"  Strike range: {fexd_options_df['StrikePrice'].min()} - {fexd_options_df['StrikePrice'].max()}")
fexd_options_df.to_csv("enumerated_fexd_options.csv", index=False)
print("Saved to enumerated_fexd_options.csv")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Futures contracts found:       {len(futures_df)}")
print(f"SDA options found:             {len(sda_options_df)}")
print(f"SDI options found:             {len(sdi_options_df)}")
print(f"FEXD options found:            {len(fexd_options_df)}")
total = len(sda_options_df) + len(sdi_options_df) + len(fexd_options_df)
print(f"Total options to download:     {total}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\nDone. Review the CSV files to confirm coverage before downloading price data.")
