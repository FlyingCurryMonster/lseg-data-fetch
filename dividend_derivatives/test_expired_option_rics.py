"""
Test whether we can retrieve data for expired dividend option RICs by
constructing candidate RICs from the known naming convention.

Active option RIC format:
  1SDA<strike><L|X><yy>     (electronic)
  SDA<strike><L|X><yy>      (composite)
  L = Call, X = Put

Expired RIC convention (from NOTES.md):
  Suffix ^<decade_digit>  where 1=2010s, 2=2020s
  e.g., SDAZ5^1 = Dec 2015 expired future

So expired options should be something like:
  1SDA85L25^2   = 85 Call Dec 2025, expired (2020s decade)
  1SDA60X24^2   = 60 Put Dec 2024, expired (2020s decade)

We'll test a grid of candidate RICs using both:
  1. ld.get_history() (library wrapper)
  2. historical_pricing REST endpoint (direct)
"""

import json
import sys
import lseg.data as ld
from dotenv import load_dotenv
import os
import time

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

# ==================================================================
# Build candidate expired option RICs
# ==================================================================

# Known strike range from active options: roughly 42-100
# Expiry years that have already passed: 2015-2025
# Month code for December = Z (all SDA options expire in Dec)
# Decade suffix: ^1 for 2010s (2015-2019), ^2 for 2020s (2020-2025)

# Start with a small targeted set to see what format works
candidates = []

# Dec 2025 (just expired) — try common strikes
for strike in [70, 75, 80, 85]:
    for cp in ["L", "X"]:  # L=Call, X=Put
        # Electronic
        candidates.append(f"1SDA{strike}{cp}25^2")
        candidates.append(f"1SDA{strike}{cp}25")  # maybe no ^ needed?
        # Composite
        candidates.append(f"SDA{strike}{cp}25^2")
        candidates.append(f"SDA{strike}{cp}25")

# Dec 2024
for strike in [70, 75, 80]:
    for cp in ["L", "X"]:
        candidates.append(f"1SDA{strike}{cp}24^2")
        candidates.append(f"SDA{strike}{cp}24^2")

# Dec 2023
for strike in [60, 65, 70]:
    for cp in ["L", "X"]:
        candidates.append(f"1SDA{strike}{cp}23^2")
        candidates.append(f"SDA{strike}{cp}23^2")

# Older: Dec 2019 (decade ^1)
for strike in [50, 55, 60]:
    for cp in ["L", "X"]:
        candidates.append(f"1SDA{strike}{cp}19^1")
        candidates.append(f"SDA{strike}{cp}19^1")

# Also try the expired futures RIC pattern to understand the ^ convention
# We know SDAZ25^2 works (from search results)
candidates.append("SDAZ25^2")  # known working expired future — control test

print(f"Testing {len(candidates)} candidate RICs")
print("=" * 80)

# ==================================================================
# Test 1: Use ld.get_history() one at a time
# ==================================================================
print("\nTEST 1: ld.get_history() — one RIC at a time")
print("=" * 80)

hits = []
misses = []

for ric in candidates:
    try:
        data = ld.get_history(
            universe=ric,
            start="2020-01-01",
            end="2025-12-31",
        )
        if data is not None and not data.empty:
            print(f"  HIT:  {ric:25s}  {len(data)} rows, cols: {list(data.columns)[:5]}")
            hits.append(ric)
        else:
            print(f"  MISS: {ric}")
            misses.append(ric)
    except Exception as e:
        err_str = str(e)[:80]
        print(f"  ERR:  {ric:25s}  {err_str}")
        misses.append(ric)

    time.sleep(1)  # gentle rate limiting

print(f"\nSummary: {len(hits)} hits, {len(misses)} misses")
if hits:
    print(f"Working RICs: {hits}")

# ==================================================================
# Test 2: Use historical_pricing REST for any misses — try different
# RIC patterns that might work via REST but not the library
# ==================================================================
if not hits:
    print("\n\nTEST 2: historical_pricing REST — trying alternate patterns")
    print("=" * 80)

    # Try some alternate expired option RIC patterns
    alt_candidates = [
        # Maybe the month code is embedded differently for options
        "1SDA80L5^2",      # single-digit year?
        "1SDA80LZ25^2",    # with month code Z?
        "1SDA80L25^L20",   # like the pattern in NOTES: ^<month><yy>
        "1SDA80L5^L20",    # shorter year
        "SDA80L25^L20",    # composite
        # The NOTES mention: LCO5500L0^L20 pattern for expired options
        # Pattern: <base_ric><month_code>^<month_letter><yy>
        "1SDA80L5^L25",    # month_code L (Dec?), year 25
        "1SDA80LZ5^2",     # Z for Dec
    ]

    for ric in alt_candidates:
        try:
            result = rest.historical_pricing(ric=ric, start="2020-01-01", end="2025-12-31")
            if isinstance(result, list) and result:
                data = result[0].get("data", [])
                if data:
                    print(f"  HIT:  {ric:25s}  {len(data)} rows")
                    hits.append(ric)
                else:
                    print(f"  MISS: {ric}  (empty data)")
            else:
                print(f"  MISS: {ric}")
        except Exception as e:
            print(f"  ERR:  {ric:25s}  {str(e)[:80]}")
        time.sleep(1)

# ==================================================================
# Summary
# ==================================================================
print("\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)
print(f"Candidates tested: {len(candidates)}")
print(f"Hits: {len(hits)}")
if hits:
    print(f"Working RICs: {hits}")
else:
    print("No expired option RICs found with any pattern tested.")
    print("Next steps: try more RIC pattern variations or check LSEG docs")

# --- Cleanup ---
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)
