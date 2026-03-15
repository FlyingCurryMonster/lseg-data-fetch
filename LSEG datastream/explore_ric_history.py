"""
Test the Discovery Symbology API directly via HTTP to access showHistory and effectiveAt
parameters that the Python convert_symbols wrapper doesn't expose.

From the official API docs (discovery/symbology/v1):
- showHistory=true  → returns effectiveFrom/effectiveTo dates for each identifier mapping
- effectiveAt       → point-in-time query (ISO 8601 UTC: yyyy-MM-ddTHH:mm:ss.msZ)
- FindPrimaryRIC    → predefined route: given ISIN/CUSIP, find the primary RIC
"""

import json
import requests
import lseg.data as ld
from dotenv import load_dotenv
import os

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

# Get the access token from the active session
token = session._access_token

SYMBOLOGY_URL = "https://api.refinitiv.com/discovery/symbology/v1/lookup"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# --- Test 1: showHistory for META ISIN -> RIC ---
print("=" * 80)
print("TEST 1: showHistory=True — full RIC history for META's ISIN")
print("=" * 80)

payload1 = {
    "from": [{"identifierTypes": ["ISIN"], "values": ["US30303M1027"]}],
    "to": [{"identifierTypes": ["RIC"]}],
    "type": "auto",
    "showHistory": True
}

resp1 = requests.post(SYMBOLOGY_URL, headers=headers, json=payload1)
print(f"Status: {resp1.status_code}")
print(json.dumps(resp1.json(), indent=2))

# --- Test 2: effectiveAt — what was META's RIC in 2020? ---
print("\n" + "=" * 80)
print("TEST 2: effectiveAt='2020-01-01' — what RIC did this ISIN have in 2020?")
print("=" * 80)

payload2 = {
    "from": [{"identifierTypes": ["ISIN"], "values": ["US30303M1027"]}],
    "to": [{"identifierTypes": ["RIC"]}],
    "type": "auto",
    "effectiveAt": "2020-01-01T00:00:00.000Z"
}

resp2 = requests.post(SYMBOLOGY_URL, headers=headers, json=payload2)
print(f"Status: {resp2.status_code}")
print(json.dumps(resp2.json(), indent=2))

# --- Test 3: FindPrimaryRIC with showHistory for META's CUSIP ---
print("\n" + "=" * 80)
print("TEST 3: FindPrimaryRIC + showHistory for META CUSIP")
print("=" * 80)

payload3 = {
    "from": [{"identifierTypes": ["CUSIP"], "values": ["30303M102"]}],
    "type": "predefined",
    "route": "FindPrimaryRIC",
    "showHistory": True
}

resp3 = requests.post(SYMBOLOGY_URL, headers=headers, json=payload3)
print(f"Status: {resp3.status_code}")
print(json.dumps(resp3.json(), indent=2))

# --- Test 4: showHistory for AAPL ISIN -> RIC (longer history) ---
print("\n" + "=" * 80)
print("TEST 4: showHistory for AAPL ISIN — verify stable RIC")
print("=" * 80)

payload4 = {
    "from": [{"identifierTypes": ["ISIN"], "values": ["US0378331005"]}],
    "to": [{"identifierTypes": ["RIC"]}],
    "type": "auto",
    "showHistory": True
}

resp4 = requests.post(SYMBOLOGY_URL, headers=headers, json=payload4)
print(f"Status: {resp4.status_code}")
print(json.dumps(resp4.json(), indent=2))

# --- Cleanup ---
ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
