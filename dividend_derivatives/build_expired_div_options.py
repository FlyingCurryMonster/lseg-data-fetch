"""
Build instrument master for expired dividend options by brute-force
scanning candidate RICs and validating with ld.get_history().

discovery.search does NOT index expired options, so we construct RICs
from the known naming convention and test which ones return data.

Expired option RIC pattern (confirmed via LSEG docs):
  SDA:  1SDA<strike><L|X><yy>^L<yy>   (electronic)
        SDA<strike><L|X><yy>^L<yy>     (composite)
  FEXD: FEXD<strike*10><L|X><y>^L<yy>  (single-digit base year)

^L = December (L is 12th letter A-L). Both calls and puts use ^L for Dec.

Output: instrument_master_expired_options.csv
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


def test_ric(ric, start="2015-01-01", end="2026-01-01"):
    """Test if a RIC returns data. Returns row count or 0."""
    try:
        data = ld.get_history(universe=ric, start=start, end=end)
        if data is not None and not data.empty:
            return len(data)
    except:
        pass
    return 0


def scan_sda_strikes(year_2d, cp_flag, prefix="1SDA"):
    """Scan SDA option strikes for a given expiry year.

    Args:
        year_2d: Two-digit year (e.g., 24, 25)
        cp_flag: 'L' for calls, 'X' for puts
        prefix: '1SDA' (electronic) or 'SDA' (composite)

    Returns list of dicts with RIC, strike, rows.
    """
    hits = []

    # Integer strikes: 30-120
    for strike in range(30, 121):
        ric = f"{prefix}{strike}{cp_flag}{year_2d}^L{year_2d}"
        rows = test_ric(ric)
        if rows:
            hits.append({"RIC": ric, "strike": float(strike), "rows": rows})
        time.sleep(0.2)

    # Fractional strikes (0.25 increments) in 50-100 range
    for whole in range(50, 101):
        for frac_cents, frac_val in [("25", 0.25), ("5", 0.50), ("75", 0.75)]:
            strike_str = f"{whole}{frac_cents}"
            strike_val = whole + frac_val
            ric = f"{prefix}{strike_str}{cp_flag}{year_2d}^L{year_2d}"
            rows = test_ric(ric)
            if rows:
                hits.append({"RIC": ric, "strike": strike_val, "rows": rows})
            time.sleep(0.2)

    return hits


def scan_fexd_strikes(year_1d, year_2d, cp_flag):
    """Scan FEXD option strikes for a given expiry year.

    FEXD encodes strikes as strike * 10 (e.g., 105.0 -> 1050).

    Args:
        year_1d: Single-digit year for base RIC (e.g., 5 for 2025)
        year_2d: Two-digit year for ^ suffix (e.g., 25)
        cp_flag: 'L' for calls, 'X' for puts

    Returns list of dicts with RIC, strike, rows.
    """
    hits = []

    # FEXD strikes typically range from ~50 to ~200 (in index points)
    # Encoded as strike * 10, so 500 to 2000
    # Scan at 10-unit increments (= 1.0 point increments) from 500 to 2000
    for strike_enc in range(500, 2001, 10):
        strike_val = strike_enc / 10.0
        ric = f"FEXD{strike_enc}{cp_flag}{year_1d}^L{year_2d}"
        rows = test_ric(ric)
        if rows:
            hits.append({"RIC": ric, "strike": strike_val, "rows": rows})
        time.sleep(0.2)

    return hits


# ==================================================================
# SDA expired options: Dec 2024 and Dec 2025
# ==================================================================
all_hits = []

for year_2d in [24, 25]:
    for cp_flag, cp_name in [("L", "C"), ("X", "P")]:
        print(f"\n{'='*60}")
        print(f"SDA Dec 20{year_2d} {'Calls' if cp_name == 'C' else 'Puts'} (electronic)")
        print(f"{'='*60}")

        hits = scan_sda_strikes(year_2d, cp_flag, prefix="1SDA")
        print(f"  Found {len(hits)} strikes")
        for h in hits:
            h["product"] = "SDA"
            h["cp_flag"] = cp_name
            h["expiry_year"] = 2000 + year_2d
            h["venue"] = "electronic"
        all_hits.extend(hits)

    # Composite: only scan calls at a few key strikes to verify coverage matches
    print(f"\n  Composite spot check for Dec 20{year_2d}...")
    for strike in [60, 70, 80]:
        ric = f"SDA{strike}L{year_2d}^L{year_2d}"
        rows = test_ric(ric)
        if rows:
            all_hits.append({
                "RIC": ric, "strike": float(strike), "rows": rows,
                "product": "SDA", "cp_flag": "C",
                "expiry_year": 2000 + year_2d, "venue": "composite",
            })
            # Also add put
            ric_put = f"SDA{strike}X{year_2d}^L{year_2d}"
            rows_put = test_ric(ric_put)
            if rows_put:
                all_hits.append({
                    "RIC": ric_put, "strike": float(strike), "rows": rows_put,
                    "product": "SDA", "cp_flag": "P",
                    "expiry_year": 2000 + year_2d, "venue": "composite",
                })
        time.sleep(0.2)

# ==================================================================
# FEXD expired options: Dec 2020 – Dec 2025
# ==================================================================
for year_1d, year_2d in [(5, 25), (4, 24), (3, 23), (2, 22), (1, 21), (0, 20)]:
    for cp_flag, cp_name in [("L", "C"), ("X", "P")]:
        print(f"\n{'='*60}")
        print(f"FEXD Dec 20{year_2d} {'Calls' if cp_name == 'C' else 'Puts'}")
        print(f"{'='*60}")

        hits = scan_fexd_strikes(year_1d, year_2d, cp_flag)
        print(f"  Found {len(hits)} strikes")
        for h in hits:
            h["product"] = "FEXD"
            h["cp_flag"] = cp_name
            h["expiry_year"] = 2000 + year_2d
            h["venue"] = "electronic"
        all_hits.extend(hits)

# ==================================================================
# Build and save master
# ==================================================================
if all_hits:
    master = pd.DataFrame(all_hits)
    master = master.sort_values(["product", "expiry_year", "strike", "cp_flag", "venue"]).reset_index(drop=True)

    print("\n" + "=" * 80)
    print("EXPIRED OPTIONS MASTER")
    print("=" * 80)
    print(f"Total contracts: {len(master)}")
    print(f"\nBy product, year, cp_flag:")
    print(master.groupby(["product", "expiry_year", "cp_flag"]).size().to_string())
    print(f"\nStrike ranges:")
    for product in master["product"].unique():
        for year in sorted(master[master["product"] == product]["expiry_year"].unique()):
            subset = master[(master["product"] == product) & (master["expiry_year"] == year)]
            print(f"  {product} {year}: {subset['strike'].min()} - {subset['strike'].max()} ({len(subset)} contracts)")

    master.to_csv("instrument_master_expired_options.csv", index=False)
    print(f"\nSaved to instrument_master_expired_options.csv")
else:
    print("\nNo expired options found!")

# --- Cleanup ---
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)
