"""
Probe LSEG for dividend distribution data, ex-dates, and corporate action
adjustment factors on common equities.

Tests:
1. TR.Div* fields — dividend history (ex-date, amount, type, currency)
2. TR.CA* fields — corporate actions (splits, adjustment factors)
3. European stocks to see if coverage extends across regions
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os

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

# =====================================================================
# TEST 1: Dividend history for US stocks
# =====================================================================
print("=" * 80)
print("TEST 1: Dividend history — US stocks (AAPL, MSFT, JNJ)")
print("=" * 80)

div_fields = [
    "TR.DivExDate",
    "TR.DivPayDate",
    "TR.DivRecordDate",
    "TR.DivUnadjustedGross",
    "TR.DivAdjustedGross",
    "TR.DivType",
    "TR.DivCurrency",
    "TR.DivAnnouncedDate",
]

try:
    data1 = ld.get_data(
        universe=["AAPL.O", "MSFT.O", "JNJ"],
        fields=div_fields,
        parameters={"SDate": "2020-01-01", "EDate": "2026-04-13"}
    )
    print(f"Shape: {data1.shape}")
    print(data1.to_string())
except Exception as e:
    print(f"Error: {e}")

# =====================================================================
# TEST 2: Corporate actions / adjustment factors for US stocks
# =====================================================================
print("\n" + "=" * 80)
print("TEST 2: Corporate actions / adjustment factors — US stocks")
print("=" * 80)

ca_fields = [
    "TR.CAEffectiveDate",
    "TR.CAAdjustmentFactor",
    "TR.CAAdjustmentType",
    "TR.CAEventType",
    "TR.CAAnnouncementDate",
]

try:
    data2 = ld.get_data(
        universe=["AAPL.O", "TSLA.O", "NVDA.O", "AMZN.O"],
        fields=ca_fields,
        parameters={"SDate": "2015-01-01", "EDate": "2026-04-13"}
    )
    print(f"Shape: {data2.shape}")
    print(data2.to_string())
except Exception as e:
    print(f"Error: {e}")

# =====================================================================
# TEST 3: Dividend history for European stocks
# =====================================================================
print("\n" + "=" * 80)
print("TEST 3: Dividend history — European stocks (VOD.L, SAN.PA, SIE.DE)")
print("=" * 80)

try:
    data3 = ld.get_data(
        universe=["VOD.L", "SAN.PA", "SIE.DE"],
        fields=div_fields,
        parameters={"SDate": "2020-01-01", "EDate": "2026-04-13"}
    )
    print(f"Shape: {data3.shape}")
    print(data3.to_string())
except Exception as e:
    print(f"Error: {e}")

# =====================================================================
# TEST 4: Corporate actions for European stocks
# =====================================================================
print("\n" + "=" * 80)
print("TEST 4: Corporate actions — European stocks")
print("=" * 80)

try:
    data4 = ld.get_data(
        universe=["VOD.L", "SAN.PA", "SIE.DE"],
        fields=ca_fields,
        parameters={"SDate": "2015-01-01", "EDate": "2026-04-13"}
    )
    print(f"Shape: {data4.shape}")
    print(data4.to_string())
except Exception as e:
    print(f"Error: {e}")

# =====================================================================
# TEST 5: Combined dividend + split data for a stock with a known split
#         (AAPL 4:1 in 2020, TSLA 3:1 in 2022, NVDA 10:1 in 2024)
# =====================================================================
print("\n" + "=" * 80)
print("TEST 5: Dividends + splits combined for AAPL (known 4:1 split Aug 2020)")
print("=" * 80)

combined_fields = div_fields + ca_fields

try:
    data5 = ld.get_data(
        universe=["AAPL.O"],
        fields=combined_fields,
        parameters={"SDate": "2019-01-01", "EDate": "2026-04-13"}
    )
    print(f"Shape: {data5.shape}")
    print(data5.to_string())
except Exception as e:
    print(f"Error: {e}")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
