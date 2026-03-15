"""
Explore symbology v2: Cross-reference LSEG convert_symbols with CRSP security_names.
Verify RIC -> CUSIP mapping works and understand CUSIP format differences.
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os
import pandas as pd

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

# --- Test 1: Bulk symbology conversion for a batch of tickers ---
print("=" * 80)
print("TEST 1: Bulk RIC -> CUSIP/ISIN for 20 large-cap stocks")
print("=" * 80)

rics = [
    "AAPL.O", "MSFT.O", "GOOGL.O", "AMZN.O", "NVDA.O",
    "META.O", "TSLA.O", "BRKb.N", "JPM.N", "V.N",
    "JNJ.N", "WMT.N", "PG.N", "MA.N", "HD.N",
    "BAC.N", "XOM.N", "PFE.N", "ABBV.N", "KO.N"
]

try:
    result = ld.discovery.convert_symbols(
        symbols=rics,
        from_symbol_type=ld.discovery.SymbolTypes.RIC,
        to_symbol_types=[
            ld.discovery.SymbolTypes.CUSIP,
            ld.discovery.SymbolTypes.ISIN,
            ld.discovery.SymbolTypes.SEDOL,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
            ld.discovery.SymbolTypes.OA_PERM_ID,
        ]
    )
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 2: Try going the other direction: CUSIP -> RIC ---
print("\n" + "=" * 80)
print("TEST 2: CUSIP -> RIC (reverse mapping)")
print("=" * 80)

cusips = ["037833100", "594918104", "46625H100"]  # AAPL, MSFT, JPM (9-digit)

try:
    result2 = ld.discovery.convert_symbols(
        symbols=cusips,
        from_symbol_type=ld.discovery.SymbolTypes.CUSIP,
        to_symbol_types=[
            ld.discovery.SymbolTypes.RIC,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
            ld.discovery.SymbolTypes.ISIN,
        ]
    )
    print(result2.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 3: CUSIP format check ---
print("\n" + "=" * 80)
print("TEST 3: CUSIP format comparison (LSEG 9-digit vs CRSP 8-digit)")
print("=" * 80)
print("LSEG returns 9-digit CUSIP (with check digit):")
print("  AAPL: 037833100")
print("  JPM:  46625H100")
print("  MSFT: 594918104")
print()
print("CRSP stores 8-digit CUSIP (no check digit):")
print("  AAPL: 03783310")
print("  JPM:  46625H10")
print("  MSFT: 59491810")
print()
print("To join: truncate LSEG CUSIP to first 8 chars, or pad CRSP CUSIP with check digit")

# --- Test 4: Search with CUSIP/ISIN/PermID select fields ---
print("\n" + "=" * 80)
print("TEST 4: discovery.search returning identifier fields")
print("=" * 80)

try:
    results4 = ld.discovery.search(
        query='Apple Inc ordinary share NASDAQ',
        top=5,
        select='DocumentTitle,RIC,Cusip,Isin,Sedol,PermID,TickerSymbol,ExchangeCode'
    )
    print(results4.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 5: Can we do a screener to pull all S&P 500 members with identifiers? ---
print("\n" + "=" * 80)
print("TEST 5: Screener for index constituents with identifiers")
print("=" * 80)

try:
    # Try using the Screener
    print("Available Screener methods:")
    print([x for x in dir(ld.discovery.Screener) if not x.startswith('_')])
except Exception as e:
    print(f"Error: {e}")

# --- Test 6: Search for all equities on NASDAQ with identifiers ---
print("\n" + "=" * 80)
print("TEST 6: Paginated search - how many results can we get?")
print("=" * 80)

try:
    results6 = ld.discovery.search(
        query='ordinary share United States',
        top=100,
        select='RIC,Cusip,Isin,Sedol,TickerSymbol,CommonName,ExchangeCode'
    )
    print(f"Returned {len(results6)} rows")
    print(results6.head(10).to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
