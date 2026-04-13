"""
Test the Discovery Symbology API directly via HTTP to access showHistory and effectiveAt
parameters that the Python convert_symbols wrapper doesn't expose.

From the official API docs (discovery/symbology/v1):
- showHistory=true  → returns effectiveFrom/effectiveTo dates for each identifier mapping
- effectiveAt       → point-in-time query (ISO 8601 UTC: yyyy-MM-ddTHH:mm:ss.msZ)
- FindPrimaryRIC    → predefined route: given ISIN/CUSIP, find the primary RIC
"""

import json
import sys
import lseg.data as ld
from dotenv import load_dotenv
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.lseg_rest_api import LSEGRestClient

load_dotenv()

# --- Session setup (to get access token) ---
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

# --- Test 1: showHistory for META ISIN -> RIC ---
print("=" * 80)
print("TEST 1: showHistory=True — full RIC history for META's ISIN")
print("=" * 80)

result1 = rest.symbology_lookup(
    identifiers=["US30303M1027"],
    from_types=["ISIN"],
    to_types=["RIC"],
    show_history=True,
)
print(json.dumps(result1, indent=2))

# --- Test 2: effectiveAt — what was META's RIC in 2020? ---
print("\n" + "=" * 80)
print("TEST 2: effectiveAt='2020-01-01' — what RIC did this ISIN have in 2020?")
print("=" * 80)

result2 = rest.symbology_lookup(
    identifiers=["US30303M1027"],
    from_types=["ISIN"],
    to_types=["RIC"],
    effective_at="2020-01-01T00:00:00.000Z",
)
print(json.dumps(result2, indent=2))

# --- Test 3: FindPrimaryRIC with showHistory for META's CUSIP ---
print("\n" + "=" * 80)
print("TEST 3: FindPrimaryRIC + showHistory for META CUSIP")
print("=" * 80)

result3 = rest.symbology_lookup(
    identifiers=["30303M102"],
    from_types=["CUSIP"],
    route="FindPrimaryRIC",
    show_history=True,
)
print(json.dumps(result3, indent=2))

# --- Test 4: showHistory for AAPL ISIN -> RIC (longer history) ---
print("\n" + "=" * 80)
print("TEST 4: showHistory for AAPL ISIN — verify stable RIC")
print("=" * 80)

result4 = rest.symbology_lookup(
    identifiers=["US0378331005"],
    from_types=["ISIN"],
    to_types=["RIC"],
    show_history=True,
)
print(json.dumps(result4, indent=2))

# --- Cleanup ---
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
