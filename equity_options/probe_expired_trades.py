"""
Probe whether the LSEG events endpoint returns trade ticks for expired option RICs.
Uses a handful of known-good SPY expired RICs from contracts.csv.

Usage:
  python probe_expired_ticks.py
"""

import csv
import json
import os
import time

import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv()

HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"
CONTRACTS_CSV = "data/SPY/contracts.csv"
N_PROBE = 8


def setup_session():
    config = {
        "sessions": {
            "default": "platform.rdp",
            "platform": {
                "rdp": {
                    "app-key":        os.getenv("DSWS_APPKEY"),
                    "username":       os.getenv("DSWS_USERNAME"),
                    "password":       os.getenv("DSWS_PASSWORD"),
                    "signon_control": True,
                }
            },
        }
    }
    config_path = "probe_lseg.config.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
    session = ld.open_session(config_name=config_path)
    return session, config_path


def get_token(session):
    return session._access_token


def probe_ric(token, ric):
    url = f"{HIST_URL}/events/{ric}"
    headers = {"Authorization": f"Bearer {token}"}
    params  = {"count": "100", "eventTypes": "trade"}
    resp    = requests.get(url, headers=headers, params=params)
    return resp.status_code, resp.text


def main():
    # Pick a spread of recently-expired SPY RICs: Jan, Feb, Mar expirations
    # (all within 3-month tick retention window from today ~Mar 28 2026)
    probe_rics = []
    with open(CONTRACTS_CSV) as f:
        rows = list(csv.DictReader(f))

    # Grab OM-source RICs across a few different expiry months
    for suffix in ("^A26", "^B26", "^N26", "^O26"):   # Jan call, Feb call, Jan put, Feb put
        batch = [r["query_ric"] for r in rows
                 if r["source"] == "om" and r["query_ric"].endswith(suffix)]
        probe_rics.extend(batch[:2])  # 2 per expiry

    probe_rics = probe_rics[:N_PROBE]
    print(f"Probing {len(probe_rics)} expired SPY RICs for trade ticks...\n")

    session, config_path = setup_session()
    token = get_token(session)

    try:
        for ric in probe_rics:
            status, body = probe_ric(token, ric)
            if status == 401:
                token = get_token(session)
                status, body = probe_ric(token, ric)

            if status != 200:
                print(f"  {ric}  HTTP {status}  {body[:120]}")
                continue

            data = json.loads(body)
            field_names = None
            rows_out = []
            for item in data:
                if "headers" in item and field_names is None:
                    field_names = [h["name"] for h in item["headers"]]
                if "data" in item:
                    rows_out = item["data"]

            n = len(rows_out)
            if n == 0:
                print(f"  {ric}  0 ticks")
            else:
                earliest = rows_out[-1][0][:19]
                latest   = rows_out[0][0][:19]
                print(f"  {ric}  {n} ticks  {earliest} – {latest}")
                if field_names:
                    print(f"    columns: {field_names}")

            time.sleep(0.1)

    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)


if __name__ == "__main__":
    main()
