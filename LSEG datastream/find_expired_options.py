"""
Search for expired dividend options using the Discovery Search REST API directly.

The Python wrapper ld.discovery.search() returned 0 expired options when we tried
AssetState eq 'DC' filters. This script hits the REST endpoint directly to test
different filter/query combinations and see if we can surface expired option RICs.

Products we're looking for:
  - SDA: CME S&P 500 Annual Dividend Options (electronic: 1SDA*, composite: SDA*)
  - FEXD: Eurex Euro Stoxx 50 Dividend Options (OEXD*)
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os
import time

from lseg_rest_api import LSEGRestClient

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
# Strategy: Try multiple query/filter combinations via REST search
# to find expired option RICs.
# ==================================================================

queries = [
    # --- SDA expired options ---
    {
        "label": "SDA expired options — direct query",
        "query": "S&P 500 Annual Dividend Option",
        "filter": "AssetState eq 'DC'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator,UnderlyingQuoteRIC",
        "top": 100,
    },
    {
        "label": "SDA expired options — electronic (1SDA)",
        "query": "1SDA",
        "filter": "AssetState eq 'DC'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator",
        "top": 100,
    },
    {
        "label": "SDA expired options — RIC prefix search",
        "query": "SDA",
        "filter": "AssetState eq 'DC' and RICRegexp eq 'SDA.*\\^'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState",
        "top": 100,
    },
    {
        "label": "SDA expired options — DERIVATIVE_QUOTES view",
        "query": "S&P 500 Annual Dividend Option",
        "filter": "AssetState eq 'DC'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator",
        "top": 100,
        "view": "DerivativeQuotes",
    },
    {
        "label": "SDA expired options — by past expiry date",
        "query": "S&P 500 Annual Dividend Option",
        "filter": "ExpiryDate lt 2025-01-01",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator",
        "top": 100,
    },
    # --- FEXD expired options ---
    {
        "label": "FEXD expired options — Eurex dividend option",
        "query": "Eurex EURO STOXX 50 Indx Div Option",
        "filter": "AssetState eq 'DC'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator",
        "top": 100,
    },
    {
        "label": "FEXD expired options — OEXD prefix",
        "query": "OEXD",
        "filter": "AssetState eq 'DC'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator",
        "top": 100,
    },
    # --- Broad derivative search with expired filter ---
    {
        "label": "All expired dividend options — broad query",
        "query": "dividend option",
        "filter": "AssetState eq 'DC'",
        "select": "RIC,DocumentTitle,ExpiryDate,AssetState,ExchangeName",
        "top": 50,
    },
    # --- Also try the Python wrapper for comparison ---
]

for q in queries:
    label = q.pop("label")
    print("=" * 80)
    print(f"QUERY: {label}")
    print(f"  params: {q}")
    print("=" * 80)

    try:
        result = rest.search(**q)

        # The response structure may vary — explore it
        total = result.get("Total", "?")
        hits = result.get("Hits", [])
        print(f"  Total: {total}, Hits returned: {len(hits)}")

        if hits:
            for hit in hits[:15]:
                ric = hit.get("RIC", "?")
                title = hit.get("DocumentTitle", "?")
                expiry = hit.get("ExpiryDate", "?")
                state = hit.get("AssetState", "?")
                print(f"    {ric:25s}  {state:5s}  {expiry}  {title}")
            if len(hits) > 15:
                print(f"    ... and {len(hits) - 15} more")
        else:
            # Print response structure for debugging
            print(f"  Response keys: {list(result.keys())}")
            preview = json.dumps(result, indent=2)[:600]
            print(f"  Preview: {preview}")

    except Exception as e:
        print(f"  ERROR: {e}")

    time.sleep(1)

# ==================================================================
# Also compare with what the Python wrapper returns
# ==================================================================
print("\n" + "=" * 80)
print("COMPARISON: ld.discovery.search() for expired SDA options")
print("=" * 80)

try:
    results = ld.discovery.search(
        query="S&P 500 Annual Dividend Option",
        filter="AssetState eq 'DC'",
        top=100,
        select="RIC,DocumentTitle,ExpiryDate,AssetState,StrikePrice,PutCallIndicator",
    )
    if results is not None and len(results) > 0:
        print(f"  Python wrapper returned {len(results)} rows")
        print(results.head(10).to_string())
    else:
        print("  Python wrapper returned 0 results (as expected)")
except Exception as e:
    print(f"  ERROR: {e}")

# --- Cleanup ---
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
