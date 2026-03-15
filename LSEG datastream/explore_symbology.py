"""
Explore LSEG symbology / security master capabilities.
Goal: Map RIC -> CUSIP, ISIN, SEDOL, PermID, etc.
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os

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

# --- Test 1: convert_symbols for equities (RIC -> CUSIP, ISIN, SEDOL) ---
print("=" * 80)
print("TEST 1: convert_symbols for equities")
print("=" * 80)
try:
    result = ld.discovery.convert_symbols(
        symbols=["AAPL.O", "MSFT.O", "JPM.N", "VOD.L"],
        from_symbol_type=ld.discovery.SymbolTypes.RIC,
        to_symbol_types=[
            ld.discovery.SymbolTypes.CUSIP,
            ld.discovery.SymbolTypes.ISIN,
            ld.discovery.SymbolTypes.SEDOL,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
        ]
    )
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 2: convert_symbols for dividend futures ---
print("\n" + "=" * 80)
print("TEST 2: convert_symbols for S&P 500 dividend futures")
print("=" * 80)
try:
    result2 = ld.discovery.convert_symbols(
        symbols=["SDAc1", "SDAZ26", "SDAZ27", "FEXDc1"],
        from_symbol_type=ld.discovery.SymbolTypes.RIC,
        to_symbol_types=[
            ld.discovery.SymbolTypes.CUSIP,
            ld.discovery.SymbolTypes.ISIN,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
        ]
    )
    print(result2.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 3: convert_symbols for dividend options ---
print("\n" + "=" * 80)
print("TEST 3: convert_symbols for dividend options")
print("=" * 80)
try:
    result3 = ld.discovery.convert_symbols(
        symbols=["1SDA85L27", "1SDA64X27"],
        from_symbol_type=ld.discovery.SymbolTypes.RIC,
        to_symbol_types=[
            ld.discovery.SymbolTypes.CUSIP,
            ld.discovery.SymbolTypes.ISIN,
            ld.discovery.SymbolTypes.TICKER_SYMBOL,
        ]
    )
    print(result3.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 4: What symbol types are available? ---
print("\n" + "=" * 80)
print("TEST 4: All available SymbolTypes")
print("=" * 80)
for attr in dir(ld.discovery.SymbolTypes):
    if not attr.startswith("_"):
        print(f"  {attr}")

# --- Test 5: get_data with identifier fields ---
print("\n" + "=" * 80)
print("TEST 5: get_history / search metadata for identifier fields")
print("=" * 80)
try:
    # Try discovery.search with identifier fields
    results = ld.discovery.search(
        query='AAPL.O',
        top=3,
        select='DocumentTitle,RIC,Cusip,Isin,Sedol,PermID,ExchangeTicker,CommonName'
    )
    print(results.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Test 6: Broader security master - search with filters ---
print("\n" + "=" * 80)
print("TEST 6: Search for S&P 500 constituents with identifiers")
print("=" * 80)
try:
    results6 = ld.discovery.search(
        query='S&P 500 constituent',
        top=10,
        filter="AssetCategory eq 'EQT'",
        select='DocumentTitle,RIC,Cusip,Isin,Sedol,PermID,ExchangeTicker'
    )
    print(results6.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
