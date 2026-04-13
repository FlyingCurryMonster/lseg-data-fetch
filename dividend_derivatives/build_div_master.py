"""
Build clean instrument master files from enumerated data.

Usage:
  python build_div_master.py file1.csv [file2.csv ...]

  If no files given, defaults to enumerated_futures.csv + enumerated_expired_futures.csv.

Output:
  1. instrument_master_futures.csv — actual individual futures contracts only
  2. instrument_master_options.csv — actual individual options with metadata (if option CSVs exist)

Excludes: continuation RICs, chain RICs, spreads, duplicates, misc junk
"""

import argparse
import pandas as pd
import os

# ============================================================
# PART 1: Clean futures master
# ============================================================
parser = argparse.ArgumentParser(description="Build clean instrument master from enumerated CSVs")
parser.add_argument("files", nargs="*", help="Input CSV files (default: enumerated_futures.csv + enumerated_expired_futures.csv)")
args = parser.parse_args()

if args.files:
    input_files = args.files
else:
    input_files = ["enumerated_futures.csv", "enumerated_expired_futures.csv"]

print("=" * 80)
print("PART 1: Building clean futures master")
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

all_input = pd.concat(frames, ignore_index=True)

print(f"\nTotal raw rows: {len(all_input)}")

# --- Clean with verification (track what each filter removes) ---
clean = all_input.copy()
removed = {}

def apply_filter(df, mask, label):
    """Apply a boolean mask, track removed rows, return filtered df."""
    dropped = df[mask]
    if not dropped.empty:
        removed[label] = dropped
    return df[~mask]

clean = apply_filter(clean, clean["RIC"].str.startswith("0#", na=False), "Chain RICs (0#...)")
clean = apply_filter(clean, clean["RIC"].str.match(r"^(?:01)?[A-Z0-9]+c\d+", na=False), "Continuation RICs (...c1, ...c2)")
clean = apply_filter(clean, clean["RIC"].str.contains("-", na=False), "Spreads (contain -)")
clean = apply_filter(clean, clean["RIC"].str.match(r"^1SDA", na=False), "Leaked options (1SDA...)")
clean = apply_filter(clean, clean["RIC"].str.contains("/", na=False), "Exchange-prefixed (/)")
fexd_mask = (clean["Product"] == "FEXD") & (~clean["RIC"].str.startswith("FEXD", na=False))
clean = apply_filter(clean, fexd_mask, "FEXD false positives")

n_before_dedup = len(clean)
clean = clean.drop_duplicates(subset=["RIC"])
n_dupes = n_before_dedup - len(clean)

print(f"After cleanup: {len(clean)}")
print(f"\n--- Removed by filter ---")
for label, dropped in removed.items():
    print(f"  {label}: {len(dropped)} rows")
    for ric in dropped["RIC"].head(5).tolist():
        print(f"    e.g. {ric}")
if n_dupes:
    print(f"  Duplicates: {n_dupes}")
total_removed = sum(len(d) for d in removed.values()) + n_dupes
print(f"  TOTAL removed: {total_removed} / {len(all_input)} raw rows")

# --- Classify active vs expired ---
def classify_status(row):
    if "^" in str(row.get("RIC", "")):
        return "expired"
    if row.get("AssetState") == "DC":
        return "expired"
    return "active"

all_futures = clean
all_futures["product"] = all_futures["Product"]  # trust enumeration tagging
all_futures["status"] = all_futures.apply(classify_status, axis=1)

# Join underlying name from Eurex product list
eurex_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "eurex_productlist.csv")
if os.path.exists(eurex_csv):
    eurex = pd.read_csv(eurex_csv, sep=";", encoding="utf-8-sig")
    eurex_map = eurex[["PRODUCT_ID", "PRODUCT_NAME"]].drop_duplicates()
    eurex_map = eurex_map.rename(columns={"PRODUCT_ID": "product", "PRODUCT_NAME": "underlying"})
    all_futures = all_futures.merge(eurex_map, on="product", how="left")

# Keep only relevant columns
output_cols = ["RIC", "product", "underlying", "status", "ExpiryDate", "DocumentTitle", "ExchangeName"]
if "ProductGroup" in all_futures.columns:
    output_cols.insert(2, "ProductGroup")
# Drop columns that don't exist (e.g. underlying if no eurex CSV)
output_cols = [c for c in output_cols if c in all_futures.columns]
futures_master = all_futures[output_cols].copy()
futures_master = futures_master.sort_values(["product", "RIC"]).reset_index(drop=True)

print(f"\n=== FUTURES MASTER ===")
print(f"Total: {len(futures_master)}")
print(f"\nBy product and status:")
print(futures_master.groupby(["product", "status"]).size().to_string())
# Print RIC lists for index products; summary for SSF/SSDF
bulk_products = set()
if "ProductGroup" in futures_master.columns:
    for group in ["SSDF", "SSF"]:
        bulk_products |= set(futures_master[futures_master["ProductGroup"] == group]["product"].unique())
print(f"\nIndex product RICs:")
for product in sorted(futures_master["product"].unique()):
    if product in bulk_products:
        continue
    rics = futures_master[futures_master["product"] == product]["RIC"].tolist()
    print(f"\n  {product}: {rics}")
if "ProductGroup" in futures_master.columns:
    for group in ["SSDF", "SSF"]:
        group_rows = futures_master[futures_master["ProductGroup"] == group]
        if not group_rows.empty:
            print(f"\n{group}: {len(group_rows)} contracts across {group_rows['product'].nunique()} products")

futures_master.to_csv("instrument_master_futures.csv", index=False)
print(f"\nSaved to instrument_master_futures.csv")

print(f"\nDone. Futures master: {len(futures_master)} contracts")
