"""
Build clean options master file from enumerated option data.

Usage:
  python build_options_master.py [file1.csv file2.csv ...]

  If no files given, defaults to enumerated_sda_options.csv + enumerated_fexd_options.csv.

Output:
  instrument_master_options.csv — individual options with strike, expiry, cp_flag, underlying

Excludes: chain RICs, ATM markers, duplicates
"""

import argparse
import pandas as pd
import os


def parse_cp(title):
    """Parse call/put from DocumentTitle since PutCallIndicator may be missing."""
    if pd.isna(title):
        return None
    if " Call " in title:
        return "C"
    elif " Put " in title:
        return "P"
    return None


parser = argparse.ArgumentParser(description="Build clean options master from enumerated CSVs")
parser.add_argument("files", nargs="*", help="Input CSV files (default: enumerated_sda_options.csv + enumerated_fexd_options.csv)")
args = parser.parse_args()

if args.files:
    input_files = args.files
else:
    input_files = ["enumerated_sda_options.csv", "enumerated_fexd_options.csv"]

print("=" * 80)
print("Building clean options master")
print("=" * 80)
print(f"Input files: {input_files}")

frames = []
for f in input_files:
    if os.path.exists(f):
        df = pd.read_csv(f)
        print(f"  {f}: {len(df)} rows")
        frames.append(df)
    else:
        print(f"  {f}: NOT FOUND, skipping")

if not frames:
    print("No input data found!")
    exit(1)

all_options = pd.concat(frames, ignore_index=True)
print(f"\nTotal raw rows: {len(all_options)}")

# --- Clean ---
# Remove chain/ATM RICs
all_options = all_options[~all_options["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
# Deduplicate — electronic (1SDA) and composite (SDA) are separate instruments
all_options = all_options.drop_duplicates(subset=["RIC"])

# Parse call/put
all_options["cp_flag"] = all_options["DocumentTitle"].apply(parse_cp)

# Set product column
if "Product" in all_options.columns:
    all_options["product"] = all_options["Product"]

print(f"After cleanup: {len(all_options)}")

# Per-product summary
for product in sorted(all_options["product"].unique()):
    subset = all_options[all_options["product"] == product]
    calls = (subset["cp_flag"] == "C").sum()
    puts = (subset["cp_flag"] == "P").sum()
    print(f"\n  {product}: {len(subset)} options ({calls} calls, {puts} puts)")
    if "ExpiryYear" in subset.columns:
        print(f"    By expiry year: {subset.groupby('ExpiryYear').size().to_dict()}")
    if "RIC" in subset.columns and product == "SDA":
        electronic = subset["RIC"].str.startswith("1SDA").sum()
        composite = len(subset) - electronic
        print(f"    Electronic (1SDA): {electronic}, Composite (SDA): {composite}")

# Standardize columns
options_master = all_options.rename(columns={
    "StrikePrice": "strike",
    "ExpiryDate": "expiry_date",
    "UnderlyingQuoteRIC": "underlying_ric",
    "ExpiryYear": "expiry_year",
})

cols_to_keep = ["RIC", "product", "strike", "expiry_date", "cp_flag", "underlying_ric", "expiry_year", "DocumentTitle"]
options_master = options_master[[c for c in cols_to_keep if c in options_master.columns]]
options_master = options_master.sort_values(["product", "expiry_date", "strike", "cp_flag"]).reset_index(drop=True)

# Verify
print(f"\n=== OPTIONS MASTER ===")
print(f"Total: {len(options_master)}")
print(f"\nStrike ranges:")
for product in options_master["product"].unique():
    subset = options_master[options_master["product"] == product]
    print(f"  {product}: {subset['strike'].min()} - {subset['strike'].max()}")
print(f"\nExpiry coverage:")
for product in options_master["product"].unique():
    subset = options_master[options_master["product"] == product]
    if "expiry_year" in subset.columns:
        print(f"  {product}: {sorted(subset['expiry_year'].unique())}")

print(f"\n=== VERIFICATION ===")
print(f"Any NaN RICs: {options_master['RIC'].isna().sum()}")
print(f"Any NaN strikes: {options_master['strike'].isna().sum()}")
print(f"Any NaN cp_flag: {options_master['cp_flag'].isna().sum()}")
print(f"Any NaN expiry_date: {options_master['expiry_date'].isna().sum()}")

options_master.to_csv("instrument_master_options.csv", index=False)
print(f"\nSaved to instrument_master_options.csv ({len(options_master)} contracts)")
