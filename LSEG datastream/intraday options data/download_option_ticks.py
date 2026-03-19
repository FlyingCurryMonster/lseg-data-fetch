"""
Download trade and quote tick data for all active options on a given underlying.

Usage:
  python download_option_ticks.py TICKER [MODE]

  TICKER: SPY, NVDA, AMD, TSLA, etc.
  MODE:   all (default), discover, trades, quotes

Examples:
  python download_option_ticks.py NVDA              # discover + trades + quotes
  python download_option_ticks.py AMD trades        # trade ticks only
  python download_option_ticks.py TSLA discover     # discover contracts only
  python download_option_ticks.py SPY quotes        # quote ticks only

Resume: The script reads {TICKER}/download_log.jsonl to skip completed (ric, type) pairs.
        Kill and restart safely at any time — network errors are retried automatically.

Output directory: {TICKER}/
  option_contracts.csv   — all discovered option RICs
  trade_ticks.csv        — trade tick data
  quote_ticks.csv        — quote tick data
  download_log.jsonl     — per-contract completion log (also used for resume)
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
# Ticker config
# =====================================================================
# Map ticker to the Discovery Search filter needed.
# SPY is special: needs AssetCategory filter because UnderlyingQuoteRIC has no suffix.
# Most US equities use .O (Nasdaq) or .N (NYSE).
UNDERLYING_MAP = {
    "SPY": ("UnderlyingQuoteRIC eq 'SPY' and AssetCategory eq 'EIO'", "SPY.P"),
    "NVDA": ("UnderlyingQuoteRIC eq 'NVDA.O'", "NVDA.O"),
    "AMD": ("UnderlyingQuoteRIC eq 'AMD.O'", "AMD.O"),
    "TSLA": ("UnderlyingQuoteRIC eq 'TSLA.O'", "TSLA.O"),
    "AAPL": ("UnderlyingQuoteRIC eq 'AAPL.O'", "AAPL.O"),
    "AMZN": ("UnderlyingQuoteRIC eq 'AMZN.O'", "AMZN.O"),
    "GOOG": ("UnderlyingQuoteRIC eq 'GOOG.O'", "GOOG.O"),
    "MSFT": ("UnderlyingQuoteRIC eq 'MSFT.O'", "MSFT.O"),
    "META": ("UnderlyingQuoteRIC eq 'META.O'", "META.O"),
    "QQQ": ("UnderlyingQuoteRIC eq 'QQQ.O'", "QQQ.O"),
    "IWM": ("UnderlyingQuoteRIC eq 'IWM' and AssetCategory eq 'EIO'", "IWM.P"),
}

# =====================================================================
# Constants
# =====================================================================
SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"

TICK_BATCH_SIZE = 10000
SEARCH_SLEEP = 0.3
TICK_SLEEP = 0.5
SUMMARY_INTERVAL = 100
MAX_RETRIES = 5

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
def setup_session(base_dir):
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
    config_path = os.path.join(base_dir, "lseg-data.config.json")
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

    def _request_with_retry(self, method, url, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                resp = method(url, headers=self.headers(), **kwargs)
                if resp.status_code == 401:
                    self.refresh()
                    resp = method(url, headers=self.headers(), **kwargs)
                return resp
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = min(60, 2 ** attempt * 5)
                print(f"    Network error (attempt {attempt+1}/{MAX_RETRIES}): {type(e).__name__}")
                print(f"    Retrying in {wait}s...")
                time.sleep(wait)
        raise requests.exceptions.ConnectionError(f"Failed after {MAX_RETRIES} retries for {url}")

    def post(self, url, payload):
        return self._request_with_retry(requests.post, url, json=payload)

    def get(self, url, params=None):
        return self._request_with_retry(requests.get, url, params=params)


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


def discover_contracts(tm, ticker, base_filter, contracts_csv):
    """Discover all active OPRA option contracts via Discovery Search."""
    print("=" * 70)
    print(f"STEP 1: DISCOVERING {ticker} OPTION CONTRACTS")
    print("=" * 70)

    search_filter = f"{base_filter} and AssetState eq 'AC'"

    # Get expiry buckets via navigators
    resp = tm.post(SEARCH_URL, {
        "Query": "",
        "View": "EquityDerivativeQuotes",
        "Select": "RIC",
        "Filter": search_filter,
        "Top": 1,
        "Navigators": "ExpiryDate(buckets:100)",
    })
    resp.raise_for_status()
    data = resp.json()
    total = data.get("Total", 0)
    buckets = data.get("Navigators", {}).get("ExpiryDate", {}).get("Buckets", [])
    print(f"  Total contracts in search: {total}")
    print(f"  Expiry buckets: {len(buckets)}")

    # For each bucket, paginate to get all contracts
    all_contracts = []

    for bi, bucket in enumerate(buckets):
        bucket_filter = bucket.get("Filter", "")
        bucket_label = bucket.get("Label", "?")
        bucket_count = bucket.get("Count", 0)

        if not bucket_filter:
            continue

        combined_filter = f"{search_filter} and {bucket_filter}"
        skip = 0
        bucket_rics = []

        while skip < bucket_count + 100:
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

    # Deduplicate by RIC
    seen = set()
    deduped = []
    for c in all_contracts:
        if c["ric"] not in seen:
            seen.add(c["ric"])
            deduped.append(c)
    all_contracts = deduped

    # Save
    with open(contracts_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ric", "expiry", "strike", "cp"])
        writer.writeheader()
        writer.writerows(all_contracts)

    print(f"\n  Saved {len(all_contracts)} unique contracts to {contracts_csv}")
    return all_contracts


def load_contracts(contracts_csv):
    contracts = []
    with open(contracts_csv, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            contracts.append(row)
    return contracts


# =====================================================================
# Log / resume
# =====================================================================
def load_completed(log_file):
    completed = set()
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
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


def log_completion(log_file, ric, tick_type, ticks, earliest, latest, num_requests, elapsed):
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
    with open(log_file, "a") as f:
        f.write(json.dumps(entry) + "\n")


# =====================================================================
# Tick download
# =====================================================================
def download_ticks_for_ric(tm, ric, event_type):
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

        end_param = batch_rows[-1][0]
        time.sleep(TICK_SLEEP)

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest = all_rows[0][0][:19] if all_rows else ""

    return all_rows, field_names, num_requests, earliest, latest


def get_csv_size(path):
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0.0


def download_all_ticks(tm, contracts, tick_type, csv_path, field_names_expected, log_file, ticker):
    completed = load_completed(log_file)
    remaining = [(i, c) for i, c in enumerate(contracts)
                 if (c["ric"], tick_type) not in completed]

    total_contracts = len(contracts)
    already_done = total_contracts - len(remaining)

    print(f"\n{'=' * 70}")
    print(f"[{ticker}] DOWNLOADING {tick_type.upper()} TICKS")
    print(f"{'=' * 70}")
    print(f"  Total contracts: {total_contracts}")
    print(f"  Already completed: {already_done}")
    print(f"  Remaining: {len(remaining)}")

    if not remaining:
        print("  Nothing to do!")
        return

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

            for row in rows:
                writer.writerow([ric] + row)
            csv_file.flush()

            if n_ticks > 0:
                contracts_with_data += 1
                if global_earliest is None or earliest < global_earliest:
                    global_earliest = earliest
                if global_latest is None or latest > global_latest:
                    global_latest = latest
            else:
                contracts_empty += 1

            log_completion(log_file, ric, tick_type, n_ticks, earliest, latest, num_requests, elapsed)

            done_count = already_done + progress_idx + 1
            csv_mb = get_csv_size(csv_path)
            time_range = f"({earliest[:10]} – {latest[:10]})" if n_ticks > 0 else "(no data)"
            print(
                f"[{tick_type}] {done_count}/{total_contracts}  {ric}  "
                f"{n_ticks} ticks  {time_range}  "
                f"{num_requests} req  {elapsed:.1f}s  |  "
                f"total: {total_ticks:,} ticks  CSV: {csv_mb:.1f} MB"
            )

            if (progress_idx + 1) % SUMMARY_INTERVAL == 0:
                elapsed_total = time.time() - start_time
                rate = (progress_idx + 1) / elapsed_total if elapsed_total > 0 else 0
                eta_s = (len(remaining) - progress_idx - 1) / rate if rate > 0 else 0
                eta_h = eta_s / 3600
                print(f"\n  --- [{ticker}] SUMMARY after {done_count}/{total_contracts} contracts ---")
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

    elapsed_total = time.time() - start_time
    csv_mb = get_csv_size(csv_path)
    print(f"\n{'=' * 70}")
    print(f"[{ticker}] {tick_type.upper()} DOWNLOAD COMPLETE")
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
    if len(sys.argv) < 2:
        print("Usage: python download_option_ticks.py TICKER [MODE]")
        print(f"  Supported tickers: {', '.join(sorted(UNDERLYING_MAP.keys()))}")
        print("  Modes: all (default), discover, trades, quotes")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    mode = sys.argv[2] if len(sys.argv) > 2 else "all"

    if mode not in ("all", "discover", "trades", "quotes"):
        print(f"Unknown mode: {mode}. Use: all, discover, trades, quotes")
        sys.exit(1)

    if ticker not in UNDERLYING_MAP:
        # Try to guess: assume Nasdaq-listed
        print(f"Warning: {ticker} not in known map, assuming UnderlyingQuoteRIC = '{ticker}.O'")
        base_filter = f"UnderlyingQuoteRIC eq '{ticker}.O'"
    else:
        base_filter, _ = UNDERLYING_MAP[ticker]

    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(script_dir, ticker)
    os.makedirs(base_dir, exist_ok=True)

    contracts_csv = os.path.join(base_dir, "option_contracts.csv")
    trade_csv = os.path.join(base_dir, "trade_ticks.csv")
    quote_csv = os.path.join(base_dir, "quote_ticks.csv")
    log_file = os.path.join(base_dir, "download_log.jsonl")

    print(f"Ticker: {ticker}")
    print(f"Filter: {base_filter}")
    print(f"Output: {base_dir}/")

    session, config_path = setup_session(base_dir)
    tm = TokenManager(session)

    try:
        # Step 1: Discover contracts
        if mode in ("all", "discover"):
            contracts = discover_contracts(tm, ticker, base_filter, contracts_csv)
        else:
            if not os.path.exists(contracts_csv):
                print(f"No contracts file found at {contracts_csv}. Run discovery first:")
                print(f"  python download_option_ticks.py {ticker} discover")
                sys.exit(1)
            contracts = load_contracts(contracts_csv)
            print(f"Loaded {len(contracts)} contracts from {contracts_csv}")

        if mode == "discover":
            return

        # Step 2: Trade ticks
        if mode in ("all", "trades"):
            download_all_ticks(tm, contracts, "trade", trade_csv, TRADE_FIELDS, log_file, ticker)

        # Step 3: Quote ticks (disabled — greeks-only value not worth the volume)
        if mode in ("quotes",):
            download_all_ticks(tm, contracts, "quote", quote_csv, QUOTE_FIELDS, log_file, ticker)

    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)


if __name__ == "__main__":
    main()
