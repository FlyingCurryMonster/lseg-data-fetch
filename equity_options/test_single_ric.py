#!/usr/bin/env python3
"""Quick spot-check: fetch bars for a single RIC via LSEG API."""
import sys, os, json, requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import lseg.data as ld

HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"

def setup_session():
    config = {
        "sessions": {
            "default": "platform.rdp",
            "platform": {
                "rdp": {
                    "app-key":  os.getenv("DSWS_APPKEY"),
                    "username": os.getenv("DSWS_USERNAME"),
                    "password": os.getenv("DSWS_PASSWORD"),
                    "signon_control": True,
                }
            },
        }
    }
    config_path = os.path.join(os.path.dirname(__file__), "test-lseg-data.config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)
    session = ld.open_session(config_name=config_path)
    return session

def fetch_bars(session, ric):
    url = f"{HIST_URL}/intraday-summaries/{ric}"
    token = session._access_token
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params={"count": "10000"}, timeout=30)
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        bars = 0
        rows = []
        for item in data:
            if isinstance(item, dict) and "data" in item and item["data"]:
                rows = item["data"]
                bars = len(rows)
        print(f"  Bars returned: {bars}")
        if bars > 0:
            print(f"  Latest:  {rows[0][:6]}")
            print(f"  Earliest: {rows[-1][:6]}")
            # Count bars with actual price data (non-None OHLC)
            has_data = sum(1 for r in rows if any(v is not None for v in r[1:5]))
            print(f"  Bars with price data: {has_data}/{bars}")
            # Show a bar with data if any
            for r in rows:
                if any(v is not None for v in r[1:5]):
                    print(f"  Sample bar with data: {r[:6]}")
                    break
    else:
        print(f"  Response: {resp.text[:500]}")

if __name__ == "__main__":
    rics = sys.argv[1:]
    if not rics:
        # Default: test a zero-bar NVDA vs a working NVDA
        rics = [
            "NVDAA162605300.U^A26",   # zero-bar (Jan 16, 2026 exp, $53 call)
            "NVDAA022605000.U^A26",   # had bars (Jan 2, 2026 exp, $50 call)
            "TSLAA162631000.U^A26",   # zero-bar TSLA (Jan 16, 2026, $310 call)
        ]

    print("Connecting to LSEG...", flush=True)
    session = setup_session()
    print("Connected.\n")

    for ric in rics:
        print(f"Fetching: {ric}")
        fetch_bars(session, ric)
        print()

    ld.close_session()
