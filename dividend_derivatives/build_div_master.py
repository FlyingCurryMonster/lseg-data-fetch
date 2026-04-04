"""
Build clean instrument master files from enumerated data.

Output:
  1. instrument_master_futures.csv — actual individual futures contracts only
  2. instrument_master_options.csv — actual individual options with metadata

Excludes: continuation RICs, chain RICs, spreads, duplicates, misc junk
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

# ============================================================
# PART 1: Clean futures master
# ============================================================
print("=" * 80)
print("PART 1: Building clean futures master")
print("=" * 80)

# Load active futures
active = pd.read_csv("enumerated_futures.csv")
# Load expired futures
expired = pd.read_csv("enumerated_expired_futures.csv")

print(f"Raw active futures rows: {len(active)}")
print(f"Raw expired futures rows: {len(expired)}")

# --- Clean active futures ---
# Remove chain RICs (0#...)
active = active[~active["RIC"].str.startswith("0#", na=False)]
# Remove continuation RICs (e.g., SDAc1, FEXDc1)
active = active[~active["RIC"].str.match(r"^[A-Z0-9]+c\d+$", na=False)]
# Remove spreads (contain "-")
active = active[~active["RIC"].str.contains("-", na=False)]
# Remove options that leaked into the futures search (contain L or X followed by digits at end)
# SDA options look like 1SDA85L27, legitimate futures look like SDAZ27
active = active[~active["RIC"].str.match(r"^1SDA", na=False)]
# Remove misc RICs (CME/1SDA, EUREX/D1AI, etc.)
active = active[~active["RIC"].str.contains("/", na=False)]
# Remove JSE-related RICs (FEZID, SX5D, DEUF, SYGEU, EXW1ID) — we only want Eurex FEXD
active_fexd = active[active["Product"] == "FEXD"]
active_fexd = active_fexd[active_fexd["RIC"].str.startswith("FEXD", na=False)]
# Keep SDA and SDI as-is (already clean after above filters)
active_sda = active[active["Product"] == "SDA"]
active_sdi = active[active["Product"] == "SDI"]
# Remove SDI continuation RICs (01SDIc1^2, etc.)
active_sdi = active_sdi[~active_sdi["RIC"].str.match(r"^01SDIc", na=False)]
active_clean = pd.concat([active_sda, active_sdi, active_fexd], ignore_index=True)

print(f"\nActive futures after cleanup: {len(active_clean)}")
print(f"  SDA: {len(active_sda)}")
print(f"  SDI: {len(active_sdi)}")
print(f"  FEXD: {len(active_fexd)}")

# --- Clean expired futures ---
# Only keep the primary exchange RICs (SDA/SDI for CME, FEXD for Eurex)
expired_sda = expired[expired["RIC"].str.contains("SDA", na=False)]
expired_sdi = expired[expired["RIC"].str.contains("SDI", na=False)]
expired_fexd = expired[expired["RIC"].str.startswith("FEXD", na=False)]
expired_clean = pd.concat([expired_sda, expired_sdi, expired_fexd], ignore_index=True)
# Remove continuation RICs that snuck in
expired_clean = expired_clean[~expired_clean["RIC"].str.match(r"^01SDIc\d", na=False)]

print(f"\nExpired futures after cleanup: {len(expired_clean)}")
print(f"  SDA: {len(expired_sda)}")
print(f"  SDI: {len(expired_sdi[~expired_sdi['RIC'].str.match(r'^01SDIc', na=False)])}")
print(f"  FEXD: {len(expired_fexd)}")

# --- Combine and build master ---
# Standardize columns
def classify_product(ric):
    if "SDA" in ric and "SDI" not in ric:
        return "SDA"
    elif "SDI" in ric:
        return "SDI"
    elif "FEXD" in ric:
        return "FEXD"
    return "OTHER"

def classify_status(row):
    if "^" in str(row.get("RIC", "")):
        return "expired"
    if row.get("AssetState") == "DC":
        return "expired"
    return "active"

all_futures = pd.concat([active_clean, expired_clean], ignore_index=True)
all_futures = all_futures.drop_duplicates(subset=["RIC"])
all_futures["product"] = all_futures["RIC"].apply(classify_product)
all_futures["status"] = all_futures.apply(classify_status, axis=1)

# Keep only relevant columns
futures_master = all_futures[["RIC", "product", "status", "ExpiryDate", "DocumentTitle", "ExchangeName"]].copy()
futures_master = futures_master.sort_values(["product", "RIC"]).reset_index(drop=True)

print(f"\n=== FUTURES MASTER ===")
print(f"Total: {len(futures_master)}")
print(f"\nBy product and status:")
print(futures_master.groupby(["product", "status"]).size().to_string())
print(f"\nAll RICs:")
for product in ["SDA", "SDI", "FEXD"]:
    rics = futures_master[futures_master["product"] == product]["RIC"].tolist()
    print(f"\n  {product}: {rics}")

futures_master.to_csv("instrument_master_futures.csv", index=False)
print(f"\nSaved to instrument_master_futures.csv")

# ============================================================
# PART 2: Clean options master
# ============================================================
print("\n" + "=" * 80)
print("PART 2: Building clean options master")
print("=" * 80)

# Load SDA options
sda_opts = pd.read_csv("enumerated_sda_options.csv")
# Load FEXD options
fexd_opts = pd.read_csv("enumerated_fexd_options.csv")

print(f"Raw SDA options rows: {len(sda_opts)}")
print(f"Raw FEXD options rows: {len(fexd_opts)}")

# --- Clean SDA options ---
# Remove chain/ATM RICs
sda_opts = sda_opts[~sda_opts["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
# Deduplicate — electronic (1SDA) and composite (SDA) are separate instruments
sda_opts = sda_opts.drop_duplicates(subset=["RIC"])

# Parse call/put from DocumentTitle since PutCallIndicator may be missing
def parse_cp(title):
    if pd.isna(title):
        return None
    if " Call " in title:
        return "C"
    elif " Put " in title:
        return "P"
    return None

sda_opts["cp_flag"] = sda_opts["DocumentTitle"].apply(parse_cp)

print(f"\nSDA options after cleanup: {len(sda_opts)}")
print(f"  Electronic (1SDA): {sda_opts['RIC'].str.startswith('1SDA').sum()}")
print(f"  Composite (SDA): {(~sda_opts['RIC'].str.startswith('1SDA')).sum()}")
print(f"  Calls: {(sda_opts['cp_flag'] == 'C').sum()}")
print(f"  Puts: {(sda_opts['cp_flag'] == 'P').sum()}")
print(f"  By expiry year: {sda_opts.groupby('ExpiryYear').size().to_dict()}")

# --- Clean FEXD options ---
fexd_opts = fexd_opts[~fexd_opts["RIC"].str.contains("0#|\\+$|ATM|\\*", regex=True, na=False)]
fexd_opts = fexd_opts.drop_duplicates(subset=["RIC"])
fexd_opts["cp_flag"] = fexd_opts["DocumentTitle"].apply(parse_cp)

print(f"\nFEXD options after cleanup: {len(fexd_opts)}")
print(f"  Calls: {(fexd_opts['cp_flag'] == 'C').sum()}")
print(f"  Puts: {(fexd_opts['cp_flag'] == 'P').sum()}")
print(f"  By expiry year: {fexd_opts.groupby('ExpiryYear').size().to_dict()}")

# --- Combine into options master ---
sda_opts["product"] = sda_opts["Product"]
fexd_opts["product"] = "FEXD"

options_master = pd.concat([sda_opts, fexd_opts], ignore_index=True)

# Standardize columns
options_master = options_master.rename(columns={
    "StrikePrice": "strike",
    "ExpiryDate": "expiry_date",
    "UnderlyingQuoteRIC": "underlying_ric",
    "ExpiryYear": "expiry_year",
})

cols_to_keep = ["RIC", "product", "strike", "expiry_date", "cp_flag", "underlying_ric", "expiry_year", "DocumentTitle"]
options_master = options_master[[c for c in cols_to_keep if c in options_master.columns]]
options_master = options_master.sort_values(["product", "expiry_date", "strike", "cp_flag"]).reset_index(drop=True)

print(f"\n=== OPTIONS MASTER ===")
print(f"Total: {len(options_master)}")
print(f"\nBy product:")
print(options_master.groupby("product").size().to_string())
print(f"\nStrike ranges:")
for product in options_master["product"].unique():
    subset = options_master[options_master["product"] == product]
    print(f"  {product}: {subset['strike'].min()} - {subset['strike'].max()}")
print(f"\nExpiry coverage:")
for product in options_master["product"].unique():
    subset = options_master[options_master["product"] == product]
    print(f"  {product}: {sorted(subset['expiry_year'].unique())}")

# Verify no junk
print(f"\n=== VERIFICATION ===")
print(f"Any NaN RICs: {options_master['RIC'].isna().sum()}")
print(f"Any NaN strikes: {options_master['strike'].isna().sum()}")
print(f"Any NaN cp_flag: {options_master['cp_flag'].isna().sum()}")
print(f"Any NaN expiry_date: {options_master['expiry_date'].isna().sum()}")

# Show sample
print(f"\nSample SDA options:")
print(options_master[options_master["product"] == "SDA"].head(10)[["RIC", "strike", "expiry_date", "cp_flag", "underlying_ric"]].to_string())
print(f"\nSample FEXD options:")
print(options_master[options_master["product"] == "FEXD"].head(10)[["RIC", "strike", "expiry_date", "cp_flag", "underlying_ric"]].to_string())

options_master.to_csv("instrument_master_options.csv", index=False)
print(f"\nSaved to instrument_master_options.csv")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)
print(f"Futures master:  {len(futures_master)} contracts")
print(f"Options master:  {len(options_master)} contracts")
print(f"Total instruments: {len(futures_master) + len(options_master)}")
