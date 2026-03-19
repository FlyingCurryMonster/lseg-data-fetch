"""
Download trade and quote tick data for all active SPY options from LSEG.

Usage:
  python download_spy_ticks.py              # discover contracts + download trades + quotes
  python download_spy_ticks.py trades       # download trade ticks only (skip discovery if contracts CSV exists)
  python download_spy_ticks.py quotes       # download quote ticks only
  python download_spy_ticks.py discover     # discover contracts only

Resume: The script reads SPY/download_log.jsonl to skip already-completed (ric, type) pairs.
        Kill and restart safely at any time.

Output directory: SPY/
  spy_option_contracts.csv   — all discovered option RICs
  spy_trade_ticks.csv        — trade tick data
  spy_quote_ticks.csv        — quote tick data
  download_log.jsonl         — per-contract completion log (also used for resume)
"""

import csv
import json
import os
import sys
import time
from datetime import datetime

import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# Config
# =====================================================================
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "SPY")
CONTRACTS_CSV = os.path.join(BASE_DIR, "spy_option_contracts.csv")
TRADE_CSV = os.path.join(BASE_DIR, "spy_trade_ticks.csv")
QUOTE_CSV = os.path.join(BASE_DIR, "spy_quote_ticks.csv")
LOG_FILE = os.path.join(BASE_DIR, "download_log.jsonl")

SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"

TICK_BATCH_SIZE = 10000
SEARCH_SLEEP = 0.3
TICK_SLEEP = 0.5
SUMMARY_INTERVAL = 100  # print summary every N contracts

TRADE_FIELDS = [
    "DATE_TIME", "EVENT_TYPE", "RTL", "SOURCE_DATETIME", "SEQNUM",
    "TRDXID_1", "TRDPRC_1", "TRDVOL_1", "BID", "BIDSIZE", "ASK",
    "ASKSIZE", "PRCTCK_1", "OPINT_1", "PCTCHNG", "ACVOL_UNS",
    "OPEN_PRC", "HIGH_1", "LOW_1", "QUALIFIERS", "TAG"
]

QUOTE_FIELDS = [
    "DATE_TIME", "EVENT_TYPE", "RTL", "SOURCE_DATETIME", "BID",
    "BIDSIZE", "ASK", "ASKSIZE", "BUYER_ID", "SELLER_ID", "IMP_VOLT",
    "IMP_VOLTA", "IMP_VOLTB", "DELTA", "THETA", "GAMMA",
    "QUALIFIERS", "TAG"
]


# =====================================================================
# Session management
# =====================================================================
def setup_session():
    config = {
        "sessions": {
            "default": "platform.rdp",
            "platform": {
                "rdp": {
                    "app-key": os.getenv("DSWS_APPKEY"),
                    "username": os.getenv("DSWS_USERNAME"),
                    "password": os.getenv("DSWS_PASSWORD"),
                    "signon_control": True,
                }
            },
        }
    }
    config_path = os.path.join(BASE_DIR, "lseg-data.config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)
    session = ld.open_session(config_name=config_path)
    return session, config_path


class TokenManager:
    def __init__(self, session):
        self.session = session
        self.token = session._access_token

    def headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def refresh(self):
        self.token = self.session._access_token

    def post(self, url, payload):
        resp = requests.post(url, headers=self.headers(), json=payload)
        if resp.status_code == 401:
            self.refresh()
            resp = requests.post(url, headers=self.headers(), json=payload)
        return resp

    def get(self, url, params=None):
        resp = requests.get(url, headers=self.headers(), params=params)
        if resp.status_code == 401:
            self.refresh()
            resp = requests.get(url, headers=self.headers(), params=params)
        return resp


# =====================================================================
# Contract discovery
# =====================================================================
def is_standard_opra_ric(ric):
    if not isinstance(ric, str):
        return False
    if not ric.endswith(".U"):
        return False
    if ric.startswith("0#") or ric.startswith("Z#"):
        return False
    if "OPTTOT" in ric:
        return False
    return True


def discover_contracts(tm):
    """Discover all active SPY OPRA option contracts via Discovery Search."""
    print("=" * 70)
    print("STEP 1: DISCOVERING SPY OPTION CONTRACTS")
    print("=" * 70)

    base_filter = "UnderlyingQuoteRIC eq 'SPY' and AssetCategory eq 'EIO' and AssetState eq 'AC'"

    # Get expiry buckets via navigators
    resp = tm.post(SEARCH_URL, {
        "Query": "",
        "View": "EquityDerivativeQuotes",
        "Select": "RIC",
        "Filter": base_filter,
        "Top": 1,
        "Navigators": "ExpiryDate(buckets:100)",
    })
    resp.raise_for_status()
    data = resp.json()
    total = data.get("Total", 0)
    buckets = data.get("Navigators", {}).get("ExpiryDate", {}).get("Buckets", [])
    print(f"  Total contracts: {total}")
    print(f"  Expiry buckets: {len(buckets)}")

    # For each bucket, paginate to get all contracts
    all_contracts = []

    for bi, bucket in enumerate(buckets):
        bucket_filter = bucket.get("Filter", "")
        bucket_label = bucket.get("Label", "?")
        bucket_count = bucket.get("Count", 0)

        if not bucket_filter:
            continue

        combined_filter = f"{base_filter} and {bucket_filter}"
        skip = 0
        bucket_rics = []

        while skip < bucket_count + 100:  # safety margin
            resp = tm.post(SEARCH_URL, {
                "Query": "",
                "View": "EquityDerivativeQuotes",
                "Select": "RIC,ExpiryDate,StrikePrice,CallPutOption",
                "Filter": combined_filter,
                "Top": 100,
                "Skip": skip,
            })
            if resp.status_code != 200:
                print(f"  ERROR on bucket {bucket_label} skip={skip}: {resp.status_code}")
                break

            hits = resp.json().get("Hits", [])
            if not hits:
                break

            for h in hits:
                ric = h.get("RIC", "")
                if is_standard_opra_ric(ric):
                    bucket_rics.append({
                        "ric": ric,
                        "expiry": h.get("ExpiryDate", "")[:10],
                        "strike": h.get("StrikePrice"),
                        "cp": h.get("CallPutOption"),
                    })

            skip += len(hits)
            time.sleep(SEARCH_SLEEP)

        all_contracts.extend(bucket_rics)
        print(f"  Bucket {bi+1}/{len(buckets)}: {bucket_label} — {len(bucket_rics)} OPRA options (running total: {len(all_contracts)})")

    # Deduplicate by RIC (buckets may overlap)
    seen = set()
    deduped = []
    for c in all_contracts:
        if c["ric"] not in seen:
            seen.add(c["ric"])
            deduped.append(c)
    all_contracts = deduped

    # Save
    with open(CONTRACTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ric", "expiry", "strike", "cp"])
        writer.writeheader()
        writer.writerows(all_contracts)

    print(f"\n  Saved {len(all_contracts)} unique contracts to {CONTRACTS_CSV}")
    return all_contracts


def load_contracts():
    """Load contracts from CSV."""
    contracts = []
    with open(CONTRACTS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            contracts.append(row)
    return contracts


# =====================================================================
# Log / resume
# =====================================================================
def load_completed():
    """Read download_log.jsonl and return set of (ric, type) completed."""
    completed = set()
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    completed.add((entry["ric"], entry["type"]))
                except (json.JSONDecodeError, KeyError):
                    continue
    return completed


def log_completion(ric, tick_type, ticks, earliest, latest, num_requests, elapsed):
    """Append a completion entry to the log."""
    entry = {
        "ric": ric,
        "type": tick_type,
        "ticks": ticks,
        "earliest": earliest,
        "latest": latest,
        "requests": num_requests,
        "elapsed_s": round(elapsed, 2),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


# =====================================================================
# Tick download
# =====================================================================
def download_ticks_for_ric(tm, ric, event_type):
    """Download all ticks (trade or quote) for a single RIC.

    Returns: (rows, field_names, num_requests, earliest_ts, latest_ts)
      rows: list of lists (raw API data rows)
      field_names: list of field name strings from headers
    """
    url = f"{HIST_URL}/events/{ric}"
    all_rows = []
    field_names = None
    num_requests = 0
    end_param = None

    while True:
        params = {"count": str(TICK_BATCH_SIZE), "eventTypes": event_type}
        if end_param:
            params["end"] = end_param

        resp = tm.get(url, params=params)
        num_requests += 1

        if resp.status_code != 200:
            print(f"    API error {resp.status_code} for {ric}: {resp.text[:200]}")
            break

        data = resp.json()
        batch_rows = []
        for item in data:
            if "headers" in item and field_names is None:
                field_names = [h["name"] for h in item["headers"]]
            if "data" in item and item["data"]:
                batch_rows = item["data"]

        if not batch_rows:
            break

        all_rows.extend(batch_rows)

        if len(batch_rows) < TICK_BATCH_SIZE:
            break

        # Next page: use earliest timestamp as end
        end_param = batch_rows[-1][0]
        time.sleep(TICK_SLEEP)

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest = all_rows[0][0][:19] if all_rows else ""

    return all_rows, field_names, num_requests, earliest, latest


def get_csv_size(path):
    """Get file size in MB."""
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0.0


def download_all_ticks(tm, contracts, tick_type, csv_path, field_names_expected):
    """Download trade or quote ticks for all contracts."""
    completed = load_completed()
    remaining = [(i, c) for i, c in enumerate(contracts)
                 if (c["ric"], tick_type) not in completed]

    total_contracts = len(contracts)
    already_done = total_contracts - len(remaining)

    print(f"\n{'=' * 70}")
    print(f"DOWNLOADING {tick_type.upper()} TICKS")
    print(f"{'=' * 70}")
    print(f"  Total contracts: {total_contracts}")
    print(f"  Already completed: {already_done}")
    print(f"  Remaining: {len(remaining)}")

    if not remaining:
        print("  Nothing to do!")
        return

    # Check if CSV needs header
    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0

    total_ticks = 0
    contracts_with_data = 0
    contracts_empty = 0
    global_earliest = None
    global_latest = None
    start_time = time.time()

    csv_file = open(csv_path, "a", newline="")
    writer = csv.writer(csv_file)
    if write_header:
        writer.writerow(["ric"] + field_names_expected)

    try:
        for progress_idx, (orig_idx, contract) in enumerate(remaining):
            ric = contract["ric"]
            t0 = time.time()

            rows, field_names, num_requests, earliest, latest = download_ticks_for_ric(
                tm, ric, tick_type
            )

            elapsed = time.time() - t0
            n_ticks = len(rows)
            total_ticks += n_ticks

            # Write rows to CSV
            for row in rows:
                writer.writerow([ric] + row)
            csv_file.flush()

            # Track stats
            if n_ticks > 0:
                contracts_with_data += 1
                if global_earliest is None or earliest < global_earliest:
                    global_earliest = earliest
                if global_latest is None or latest > global_latest:
                    global_latest = latest
            else:
                contracts_empty += 1

            # Log completion
            log_completion(ric, tick_type, n_ticks, earliest, latest, num_requests, elapsed)

            # Console output
            done_count = already_done + progress_idx + 1
            csv_mb = get_csv_size(csv_path)
            time_range = f"({earliest[:10]} – {latest[:10]})" if n_ticks > 0 else "(no data)"
            print(
                f"[{tick_type}] {done_count}/{total_contracts}  {ric}  "
                f"{n_ticks} ticks  {time_range}  "
                f"{num_requests} req  {elapsed:.1f}s  |  "
                f"total: {total_ticks:,} ticks  CSV: {csv_mb:.1f} MB"
            )

            # Summary every N contracts
            if (progress_idx + 1) % SUMMARY_INTERVAL == 0:
                elapsed_total = time.time() - start_time
                rate = (progress_idx + 1) / elapsed_total if elapsed_total > 0 else 0
                eta_s = (len(remaining) - progress_idx - 1) / rate if rate > 0 else 0
                eta_h = eta_s / 3600
                print(f"\n  --- SUMMARY after {done_count}/{total_contracts} contracts ---")
                print(f"  Total {tick_type} ticks: {total_ticks:,}")
                print(f"  Contracts with data: {contracts_with_data}, empty: {contracts_empty}")
                print(f"  Data range: {global_earliest or 'N/A'} to {global_latest or 'N/A'}")
                print(f"  CSV size: {csv_mb:.1f} MB")
                print(f"  Elapsed: {elapsed_total/3600:.1f}h, ETA: {eta_h:.1f}h")
                print(f"  Rate: {rate:.1f} contracts/s")
                print()

            time.sleep(TICK_SLEEP)

    finally:
        csv_file.close()

    # Final summary
    elapsed_total = time.time() - start_time
    csv_mb = get_csv_size(csv_path)
    print(f"\n{'=' * 70}")
    print(f"{tick_type.upper()} DOWNLOAD COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Contracts processed: {len(remaining)}")
    print(f"  Contracts with data: {contracts_with_data}")
    print(f"  Contracts empty: {contracts_empty}")
    print(f"  Total ticks: {total_ticks:,}")
    print(f"  Data range: {global_earliest or 'N/A'} to {global_latest or 'N/A'}")
    print(f"  CSV size: {csv_mb:.1f} MB")
    print(f"  Elapsed: {elapsed_total/3600:.1f}h")


# =====================================================================
# Main
# =====================================================================
def main():
    os.makedirs(BASE_DIR, exist_ok=True)

    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode not in ("all", "discover", "trades", "quotes"):
        print(f"Unknown mode: {mode}. Use: all, discover, trades, quotes")
        sys.exit(1)

    session, config_path = setup_session()
    tm = TokenManager(session)

    try:
        # Step 1: Discover contracts
        if mode in ("all", "discover"):
            contracts = discover_contracts(tm)
        else:
            if not os.path.exists(CONTRACTS_CSV):
                print(f"No contracts file found at {CONTRACTS_CSV}. Run discovery first.")
                sys.exit(1)
            contracts = load_contracts()
            print(f"Loaded {len(contracts)} contracts from {CONTRACTS_CSV}")

        if mode == "discover":
            return

        # Step 2: Trade ticks
        if mode in ("all", "trades"):
            download_all_ticks(tm, contracts, "trade", TRADE_CSV, TRADE_FIELDS)

        # Step 3: Quote ticks
        if mode in ("all", "quotes"):
            download_all_ticks(tm, contracts, "quote", QUOTE_CSV, QUOTE_FIELDS)

    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)


if __name__ == "__main__":
    main()
