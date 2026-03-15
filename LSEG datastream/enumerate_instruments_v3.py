"""
Enumerate Euro Stoxx 50 dividend options (FEXD) using correct search queries.

From v2 we learned:
  - RIC format: FEXD<strike*100><L/X><year_single_digit> e.g. FEXD7500X6 = 75 Put Dec 2026
  - Chain RICs: 0#FEXDZ0+, 0#FEXDZ7+, 0#FEXDZ25+, etc.
  - Query "OEXD Eurex" or "Eurex EURO STOXX 50 Indx Div Option" finds them
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
# PART 1: Enumerate FEXD options per expiry year
# ============================================================
print("=" * 80)
print("PART 1: Enumerating Euro Stoxx 50 Dividend Options")
print("=" * 80)

all_fexd_options = []

# Try each year — the RIC uses single digit year codes,
# chain RICs we found: 0#FEXDZ0+ (2030), 0#FEXDZ7+ (2027), 0#FEXDZ25+ (2025)
for year in range(2024, 2036):
    yy = str(year)[-2:]  # e.g. "26"

    # Try the query that worked in v2
    print(f"\n  Searching FEXD options Dec {year}...")
    results = ld.discovery.search(
        query=f"Eurex EURO STOXX 50 Indx Div Option Dec {year}",
        top=200,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "FEXD"
            options_only["ExpiryYear"] = year
            all_fexd_options.append(options_only)
            print(f"    Found {len(options_only)} individual options")
        else:
            print(f"    No individual options (only chains)")
    else:
        # Try alternate query
        results = ld.discovery.search(
            query=f"FEXD option Dec {year} Eurex",
            top=200,
            select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
        )
        if not results.empty:
            options_only = results[~results["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
            if not options_only.empty:
                options_only = options_only.copy()
                options_only["Product"] = "FEXD"
                options_only["ExpiryYear"] = year
                all_fexd_options.append(options_only)
                print(f"    Found {len(options_only)} individual options (alt query)")
            else:
                print(f"    No individual options")
        else:
            print(f"    No results")
    time.sleep(0.5)

# ============================================================
# PART 2: Try a broad search to catch anything we missed
# ============================================================
print("\n" + "=" * 80)
print("PART 2: Broad FEXD option search")
print("=" * 80)

# Search broadly with high top
broad_queries = [
    "OEXD Eurex STOXX 50 dividend option",
    "Eurex ESTX 50 DVP Index Option",
    "FEXD option Eurex call",
    "FEXD option Eurex put",
]

for q in broad_queries:
    print(f"\nQuery: '{q}'")
    results = ld.discovery.search(
        query=q,
        top=50,
        select="DocumentTitle,RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC,ExchangeName,AssetState"
    )
    if not results.empty:
        options_only = results[~results["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
        if not options_only.empty:
            options_only = options_only.copy()
            options_only["Product"] = "FEXD"
            # Deduplicate later
            all_fexd_options.append(options_only)
            print(f"  Found {len(options_only)} individual options")
            for _, row in options_only.head(5).iterrows():
                print(f"    {row['RIC']:20s} | K={row.get('StrikePrice',''):>8} | {row.get('ExpiryDate','')} | {row.get('DocumentTitle','')[:60]}")
        else:
            print(f"  No individual options")
    else:
        print(f"  No results")
    time.sleep(0.5)

# Combine and deduplicate
if all_fexd_options:
    fexd_options_df = pd.concat(all_fexd_options, ignore_index=True)
    fexd_options_df = fexd_options_df.drop_duplicates(subset=["RIC"])
    print(f"\n\nTotal unique FEXD options found: {len(fexd_options_df)}")
    if not fexd_options_df.empty:
        print(f"  Unique expiry dates: {sorted(fexd_options_df['ExpiryDate'].dropna().unique())}")
        print(f"  Strike range: {fexd_options_df['StrikePrice'].min()} - {fexd_options_df['StrikePrice'].max()}")
        calls = fexd_options_df['DocumentTitle'].str.contains('Call', na=False).sum()
        puts = fexd_options_df['DocumentTitle'].str.contains('Put', na=False).sum()
        print(f"  Calls: {calls}, Puts: {puts}")

        # Show RIC pattern
        print(f"\n  Sample RICs:")
        for _, row in fexd_options_df.head(10).iterrows():
            print(f"    {row['RIC']:20s} | K={row.get('StrikePrice',''):>8} | exp={row.get('ExpiryDate','')} | {row.get('PutCallIndicator','')}")

    fexd_options_df.to_csv("enumerated_fexd_options.csv", index=False)
    print("\nSaved to enumerated_fexd_options.csv")
else:
    print("\nNo FEXD options found at all.")

# ============================================================
# PART 3: List all option chain RICs we can find
# ============================================================
print("\n" + "=" * 80)
print("PART 3: All FEXD option chain RICs")
print("=" * 80)

results = ld.discovery.search(
    query="FEXD option chain Eurex",
    top=50,
    select="DocumentTitle,RIC,AssetCategory"
)
if not results.empty:
    chains = results[results["RIC"].str.contains("0#|\\+", regex=True, na=False)]
    print(f"Found {len(chains)} chain RICs:")
    for _, row in chains.iterrows():
        print(f"  {row['RIC']:20s} | {row['DocumentTitle'][:70]}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
