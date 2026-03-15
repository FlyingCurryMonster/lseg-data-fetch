"""
Security master exploration:
- Pull symbol/identifier history from LSEG for a given RIC
- Cross-reference against CRSP security_names time series
- Goal: understand what we can build as a security master from LSEG Workspace
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

# CRSP tells us:
#   PERMNO 13407 = Facebook/Meta
#   CUSIP (8-digit) = 30303M10
#   Ticker: FB (2012-2022) -> META (2022+)
#   IssuerNm: FACEBOOK INC (2012-2021) -> META PLATFORMS INC (2021+)
#   SecurityBegDt: 2012-05-18 (IPO)
#   PrimaryExch: Q (NASDAQ)

print("=" * 80)
print("TEST 1: What does LSEG convert_symbols return for META.O?")
print("=" * 80)

result = ld.discovery.convert_symbols(
    symbols=["META.O"],
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
print()
print("CRSP 8-digit CUSIP: 30303M10")
print(f"LSEG 9-digit CUSIP: {result['CUSIP'].iloc[0]}")
print(f"Match on first 8:   {result['CUSIP'].iloc[0][:8] == '30303M10'}")

# --- Test 2: Can we get historical name/ticker changes from LSEG? ---
print("\n" + "=" * 80)
print("TEST 2: discovery.search for all RICs tied to CUSIP 30303M102 (META)")
print("=" * 80)

# Search by CUSIP to find all listings
results2 = ld.discovery.search(
    query='Meta Platforms Facebook',
    top=20,
    select='DocumentTitle,RIC,Cusip,Isin,TickerSymbol,PermID,ExchangeCode,AssetStatus,AssetCategory,CommonName'
)
print(results2.to_string())

# --- Test 3: What fields does search support that could build a secmaster? ---
print("\n" + "=" * 80)
print("TEST 3: Full field set available for equity secmaster")
print("=" * 80)

results3 = ld.discovery.search(
    query='Meta Platforms Inc ordinary share NASDAQ',
    top=5,
    select='DocumentTitle,RIC,Cusip,Isin,Sedol,PermID,TickerSymbol,CommonName,ExchangeCode,ExchangeName,AssetCategory,AssetClass,AssetState,CountryCode,CountryOfRisk,IssuerOAPermID,InstrumentType'
)
print(results3.to_string())

# --- Test 4: Bulk convert - what can we do at scale? ---
print("\n" + "=" * 80)
print("TEST 4: Bulk convert 50 S&P 500 RICs -> CUSIP/ISIN/PermID")
print("=" * 80)

sp500_sample = [
    "AAPL.O", "MSFT.O", "GOOGL.O", "AMZN.O", "NVDA.O",
    "META.O", "TSLA.O", "BRKb.N", "JPM.N", "V.N",
    "JNJ.N", "WMT.N", "PG.N", "MA.N", "HD.N",
    "BAC.N", "XOM.N", "PFE.N", "ABBV.N", "KO.N",
    "AVGO.O", "MRK.N", "CVX.N", "LLY.N", "PEP.O",
    "COST.O", "TMO.N", "MCD.N", "ACN.N", "ABT.N",
    "CSCO.O", "CRM.N", "NEE.N", "DHR.N", "TXN.O",
    "NKE.N", "LIN.N", "PM.N", "ORCL.N", "AMGN.O",
    "RTX.N", "UPS.N", "HON.O", "QCOM.O", "IBM.N",
    "AMD.O", "INTC.O", "GE.N", "CAT.N", "BA.N"
]

bulk = ld.discovery.convert_symbols(
    symbols=sp500_sample,
    from_symbol_type=ld.discovery.SymbolTypes.RIC,
    to_symbol_types=[
        ld.discovery.SymbolTypes.CUSIP,
        ld.discovery.SymbolTypes.ISIN,
        ld.discovery.SymbolTypes.SEDOL,
        ld.discovery.SymbolTypes.TICKER_SYMBOL,
        ld.discovery.SymbolTypes.OA_PERM_ID,
    ]
)
print(f"Returned {len(bulk)} rows")
print(bulk.to_string())
bulk.to_csv("sample_secmaster_sp500.csv")
print("\nSaved to sample_secmaster_sp500.csv")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
