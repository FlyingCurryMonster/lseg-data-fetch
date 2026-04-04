"""
Build a security master table linking LSEG identifiers to CRSP PERMNOs.

Strategy:
  Phase 1  — LSEG-led: paginate discovery.search for all US ordinary shares,
             collect RIC + CUSIP + ISIN + PermID.
  Phase 2  — CRSP-led fallback: for CUSIPs in CRSP but missing from Phase 1,
             batch-convert via convert_symbols (CUSIP -> RIC).
  Phase 3  — Join: lseg_cusip[:8] == crsp_cusip -> add PERMNO column.
  Phase 4  — RIC history: for each matched security, call showHistory REST API
             to get full RIC timeline (effectiveFrom / effectiveTo).

Output files (in this directory):
  secmaster_lseg_raw.csv      — all LSEG rows from discovery.search
  secmaster_joined.csv        — LSEG rows joined to CRSP PERMNO
  secmaster_ric_history.csv   — per-PERMNO RIC timelines from showHistory
"""

import json
import os
import sys
import time
import lseg.data as ld
from dotenv import load_dotenv
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.lseg_rest_api import LSEGRestClient

load_dotenv()

# ---------------------------------------------------------------------------
# Session setup
# ---------------------------------------------------------------------------
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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Phase 1: LSEG-led — paginate discovery.search for US ordinary shares
# ---------------------------------------------------------------------------
print("=" * 80)
print("PHASE 1: discovery.search — US ordinary shares")
print("=" * 80)

SEARCH_FIELDS = "RIC,Cusip,Isin,PermID,TickerSymbol,CommonName,ExchangeCode,ExchangeName,AssetCategory,AssetState,CountryCode"
FILTER = "AssetCategory eq 'Ordinary Shares' and CountryCode eq 'USA'"
PAGE_SIZE = 1000  # test with smaller first, then try 10000

all_rows = []
page = 0
navigate_cursor = None

while True:
    try:
        kwargs = dict(
            query="",
            filter=FILTER,
            top=PAGE_SIZE,
            select=SEARCH_FIELDS,
        )
        if navigate_cursor:
            kwargs["navigate"] = navigate_cursor

        results = ld.discovery.search(**kwargs)

        if results is None or len(results) == 0:
            print(f"  Page {page}: no results, stopping.")
            break

        all_rows.append(results)
        print(f"  Page {page}: {len(results)} rows (cumulative: {sum(len(r) for r in all_rows)})")

        # Check if there's a next cursor — discovery.search returns metadata
        # If the library exposes navigate/cursor info, use it; otherwise stop at first page
        # For now test what we get with a single large page
        if len(results) < PAGE_SIZE:
            print("  Got fewer rows than page size — likely last page.")
            break

        # TODO: If lseg-data exposes cursor/navigate in response metadata, use it here.
        # For now, break after first page to see how many we get.
        print("  NOTE: Pagination cursor not yet implemented — stopping after first page.")
        print("  Increase PAGE_SIZE and re-test, or implement cursor pagination.")
        break

    except Exception as e:
        print(f"  Error on page {page}: {e}")
        break

    page += 1

if all_rows:
    lseg_raw = pd.concat(all_rows, ignore_index=True)
    print(f"\nTotal LSEG rows from discovery.search: {len(lseg_raw)}")
    print(lseg_raw.dtypes)
    print(lseg_raw.head(5).to_string())
    lseg_raw.to_csv(os.path.join(SCRIPT_DIR, "secmaster_lseg_raw.csv"), index=False)
    print("Saved: secmaster_lseg_raw.csv")
else:
    lseg_raw = pd.DataFrame()
    print("No data returned from discovery.search.")

# ---------------------------------------------------------------------------
# Phase 2: CRSP-led fallback — for any CRSP CUSIP not in Phase 1,
#           use convert_symbols(CUSIP -> RIC) in batches
# ---------------------------------------------------------------------------
print("\n" + "=" * 80)
print("PHASE 2: CRSP-led fallback via convert_symbols")
print("=" * 80)

# Load CRSP active securities from ClickHouse (run separately; here we read from CSV if available)
# For testing, use a hardcoded small sample.
# In production: query ClickHouse for all CRSP PERMNOs + CUSIPs.

# Sample: get 8-digit CRSP CUSIPs not covered by Phase 1
# We'll do a spot-check with 20 known tickers
sample_cusips_8digit = [
    "03783310",  # AAPL
    "59491810",  # MSFT
    "02079K30",  # GOOGL
    "02313510",  # AMZN
    "67066G10",  # NVDA
    "30303M10",  # META
    "88160R10",  # TSLA
    "08467070",  # BRK/B - approximate
    "46625H10",  # JPM
    "92826C83",  # V
]

# LSEG wants 9-digit CUSIPs; we're missing the check digit.
# Try submitting 8-digit and see if it resolves:
print("Testing CUSIP lookup with 8-digit (no check digit) CUSIPs:")
try:
    result_p2 = ld.discovery.convert_symbols(
        symbols=sample_cusips_8digit[:5],
        from_symbol_type=ld.discovery.SymbolTypes.CUSIP,
        to_symbol_types=[
            ld.discovery.SymbolTypes.RIC,
            ld.discovery.SymbolTypes.ISIN,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
        ]
    )
    print("8-digit CUSIP result:")
    print(result_p2.to_string())
except Exception as e:
    print(f"  8-digit CUSIP lookup failed: {e}")

# Also try 9-digit (append '0' as a dummy check — wrong but tests if length matters):
print("\nTesting CUSIP lookup with 9-digit CUSIPs (proper check digits for known symbols):")
sample_cusips_9digit = [
    "037833100",  # AAPL
    "594918104",  # MSFT
    "46625H100",  # JPM
    "30303M102",  # META
    "023135106",  # AMZN
]
try:
    result_p2b = ld.discovery.convert_symbols(
        symbols=sample_cusips_9digit,
        from_symbol_type=ld.discovery.SymbolTypes.CUSIP,
        to_symbol_types=[
            ld.discovery.SymbolTypes.RIC,
            ld.discovery.SymbolTypes.ISIN,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
        ]
    )
    print("9-digit CUSIP result:")
    print(result_p2b.to_string())
except Exception as e:
    print(f"  9-digit CUSIP lookup failed: {e}")

# ---------------------------------------------------------------------------
# Phase 3: Join LSEG raw to CRSP
# ---------------------------------------------------------------------------
print("\n" + "=" * 80)
print("PHASE 3: Join LSEG -> CRSP via cusip[:8]")
print("=" * 80)

if not lseg_raw.empty and "Cusip" in lseg_raw.columns:
    # Normalize: strip whitespace, take first 8 chars
    lseg_raw["cusip8"] = lseg_raw["Cusip"].str.strip().str[:8]

    # Load CRSP security_names (latest row per PERMNO) from ClickHouse
    # For now, load from a small in-memory sample to test join logic
    crsp_sample = pd.DataFrame({
        "PERMNO": [14593, 10107, 13407, 76076],
        "CUSIP": ["03783310", "59491810", "30303M10", "46625H10"],
        "Ticker": ["AAPL", "MSFT", "META", "JPM"],
        "IssuerNm": ["APPLE INC", "MICROSOFT CORP", "META PLATFORMS INC", "JPMORGAN CHASE & CO"],
    })

    joined = lseg_raw.merge(crsp_sample, left_on="cusip8", right_on="CUSIP", how="inner")
    print(f"Matched {len(joined)} rows")
    if not joined.empty:
        print(joined[["RIC", "Cusip", "cusip8", "PERMNO", "Ticker", "IssuerNm"]].to_string())
        joined.to_csv(os.path.join(SCRIPT_DIR, "secmaster_joined.csv"), index=False)
        print("Saved: secmaster_joined.csv")
else:
    joined = pd.DataFrame()
    print("Skipping join — lseg_raw empty or no Cusip column.")

# ---------------------------------------------------------------------------
# Phase 4: RIC history via showHistory REST API
# ---------------------------------------------------------------------------
print("\n" + "=" * 80)
print("PHASE 4: showHistory for matched securities")
print("=" * 80)

test_isins = {
    "META": "US30303M1027",
    "AAPL": "US0378331005",
    "MSFT": "US5949181045",
    "JPM":  "US46625H1005",
}

all_history = []
for name, isin in test_isins.items():
    print(f"\n  {name} ({isin}):")
    df = rest.symbology_lookup_df(
        identifiers=[isin],
        from_types=["ISIN"],
        route="FindPrimaryRIC",
        show_history=True,
    )
    for _, r in df.iterrows():
        print(f"    {r['value']:20s}  {r['effective_from']}  ->  {r['effective_to']}")
    all_history.append(df)
    time.sleep(0.2)  # gentle rate limiting

if all_history:
    hist_df = pd.concat(all_history, ignore_index=True)
    hist_df.to_csv(os.path.join(SCRIPT_DIR, "secmaster_ric_history.csv"), index=False)
    print(f"\nSaved: secmaster_ric_history.csv ({len(hist_df)} rows)")

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
