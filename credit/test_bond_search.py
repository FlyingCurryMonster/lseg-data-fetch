"""Test pagination: does Skip/Boost/OrderBy enable paging beyond 10K?"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import lseg.data as ld
from shared.lseg_rest_api import LSEGRestClient
import requests

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)

base_filter = "DbType eq 'CORP' and IsActive eq true and RCSCurrencyLeaf eq 'US Dollar'"

# Test 1: Try Skip parameter
print("=== Test 1: Skip=100, Top=5 ===")
try:
    payload = {
        "Query": "",
        "View": "GovCorpInstruments",
        "Filter": base_filter,
        "Select": "RIC,ISIN,IssuerLegalName",
        "Top": 5,
        "Skip": 100,
    }
    resp = requests.post(
        "https://api.refinitiv.com/discovery/search/v1/",
        headers={"Authorization": f"Bearer {session._access_token}", "Content-Type": "application/json"},
        json=payload,
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Total: {data.get('Total')}, Hits: {len(data.get('Hits', []))}")
        for h in data.get("Hits", [])[:3]:
            print(f"    {h.get('RIC')} - {h.get('IssuerLegalName')}")
    else:
        print(f"  Error: {resp.text[:300]}")
except Exception as e:
    print(f"  Exception: {e}")

# Test 2: Try Skip=5000
print("\n=== Test 2: Skip=5000, Top=5 ===")
try:
    payload["Skip"] = 5000
    resp = requests.post(
        "https://api.refinitiv.com/discovery/search/v1/",
        headers={"Authorization": f"Bearer {session._access_token}", "Content-Type": "application/json"},
        json=payload,
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Total: {data.get('Total')}, Hits: {len(data.get('Hits', []))}")
        for h in data.get("Hits", [])[:3]:
            print(f"    {h.get('RIC')} - {h.get('IssuerLegalName')}")
    else:
        print(f"  Error: {resp.text[:300]}")
except Exception as e:
    print(f"  Exception: {e}")

# Test 3: Try Skip=10000 (beyond 10K boundary)
print("\n=== Test 3: Skip=10000, Top=5 ===")
try:
    payload["Skip"] = 10000
    resp = requests.post(
        "https://api.refinitiv.com/discovery/search/v1/",
        headers={"Authorization": f"Bearer {session._access_token}", "Content-Type": "application/json"},
        json=payload,
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Total: {data.get('Total')}, Hits: {len(data.get('Hits', []))}")
    else:
        print(f"  Error: {resp.text[:300]}")
except Exception as e:
    print(f"  Exception: {e}")

# Test 4: Can we do Skip=9995, Top=10000? (skip near end + large top)
print("\n=== Test 4: Skip=9995, Top=10 (right at boundary) ===")
try:
    payload["Skip"] = 9995
    payload["Top"] = 10
    resp = requests.post(
        "https://api.refinitiv.com/discovery/search/v1/",
        headers={"Authorization": f"Bearer {session._access_token}", "Content-Type": "application/json"},
        json=payload,
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Total: {data.get('Total')}, Hits: {len(data.get('Hits', []))}")
    else:
        print(f"  Error: {resp.text[:300]}")
except Exception as e:
    print(f"  Exception: {e}")

# Test 5: Skip + Top > 10000
print("\n=== Test 5: Skip=9000, Top=5000 (sum > 10K) ===")
try:
    payload["Skip"] = 9000
    payload["Top"] = 5000
    resp = requests.post(
        "https://api.refinitiv.com/discovery/search/v1/",
        headers={"Authorization": f"Bearer {session._access_token}", "Content-Type": "application/json"},
        json=payload,
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Total: {data.get('Total')}, Hits: {len(data.get('Hits', []))}")
    else:
        print(f"  Error: {resp.text[:300]}")
except Exception as e:
    print(f"  Exception: {e}")

ld.close_session()
