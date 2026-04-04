"""
Download trade and quote tick data for all active options on a given underlying.

Usage:
  python download_option_ticks.py TICKER [MODE] [WORKERS]

  TICKER:  SPY, NVDA, AMD, TSLA, etc.
  MODE:    all (default), discover, trades, quotes
  WORKERS: parallel workers (default: 8)

Examples:
  python download_option_ticks.py NVDA              # discover + trades
  python download_option_ticks.py AMD trades        # trade ticks only
  python download_option_ticks.py TSLA discover     # discover contracts only
  python download_option_ticks.py SPY trades 12     # trade ticks, 12 workers

Resume: The script reads {TICKER}/download_log.jsonl to skip completed (ric, type) pairs.
        Kill and restart safely at any time — network errors are retried automatically.

Output directory: {TICKER}/
  option_contracts.csv   — all discovered option RICs
  trade_ticks.csv        — trade tick data
  quote_ticks.csv        — quote tick data
  download_log.jsonl     — per-contract completion log (also used for resume)
  ticks_progress.log     — timestamped throughput log
"""

import csv
import json
import os
import sys
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# Ticker config
# =====================================================================
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
HIST_URL   = "https://api.refinitiv.com/data/historical-pricing/v1/views"

TICK_BATCH_SIZE    = 10000
SUMMARY_INTERVAL   = 500
PROGRESS_INTERVAL  = 60       # seconds between progress log entries
MAX_RETRIES        = 5

INITIAL_RATE  = 23.0   # req/sec starting rate (events endpoint cap is 25)
RATE_BACKOFF  = 0.90   # multiply rate by this on each 429
MIN_RATE      = 0.5    # floor
DEFAULT_WORKERS = 8

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
# Adaptive rate limiter (token bucket)
# =====================================================================
class AdaptiveRateLimiter:
    def __init__(self, initial_rate=INITIAL_RATE):
        self._lock   = threading.Lock()
        self._rate   = initial_rate
        self._tokens = initial_rate
        self._last   = time.monotonic()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(0.005)

    def on_429(self):
        with self._lock:
            self._rate = max(MIN_RATE, self._rate * RATE_BACKOFF)

    def current_rate(self):
        with self._lock:
            return self._rate


# =====================================================================
# Session / token management (thread-safe)
# =====================================================================
def setup_session(base_dir):
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
    config_path = os.path.join(base_dir, "lseg-data.config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)
    session = ld.open_session(config_name=config_path)
    return session, config_path


class TokenManager:
    def __init__(self, session):
        self._session = session
        self._token   = session._access_token
        self._lock    = threading.Lock()

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type":  "application/json",
        }

    def _refresh(self):
        with self._lock:
            self._token = self._session._access_token

    def _request_with_retry(self, method, url, rate_limiter=None, **kwargs):
        for attempt in range(MAX_RETRIES):
            if rate_limiter:
                rate_limiter.acquire()
            try:
                resp = method(url, headers=self._headers(), **kwargs)
                if resp.status_code == 401:
                    self._refresh()
                    resp = method(url, headers=self._headers(), **kwargs)
                if resp.status_code == 429:
                    if rate_limiter:
                        rate_limiter.on_429()
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    time.sleep(retry_after)
                    continue
                return resp
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = min(60, 2 ** attempt * 5)
                time.sleep(wait)
        raise requests.exceptions.ConnectionError(
            f"Failed after {MAX_RETRIES} retries for {url}"
        )

    def post(self, url, payload, rate_limiter=None):
        return self._request_with_retry(
            requests.post, url, rate_limiter=rate_limiter, json=payload
        )

    def get(self, url, params=None, rate_limiter=None):
        return self._request_with_retry(
            requests.get, url, rate_limiter=rate_limiter, params=params
        )


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


def discover_contracts(tm, ticker, base_filter, contracts_csv, rate_limiter):
    print("=" * 70)
    print(f"STEP 1: DISCOVERING {ticker} OPTION CONTRACTS")
    print("=" * 70)

    search_filter = f"{base_filter} and AssetState eq 'AC'"

    resp = tm.post(SEARCH_URL, {
        "Query": "",
        "View": "EquityDerivativeQuotes",
        "Select": "RIC",
        "Filter": search_filter,
        "Top": 1,
        "Navigators": "ExpiryDate(buckets:100)",
    }, rate_limiter=rate_limiter)
    resp.raise_for_status()
    data    = resp.json()
    total   = data.get("Total", 0)
    buckets = data.get("Navigators", {}).get("ExpiryDate", {}).get("Buckets", [])
    print(f"  Total contracts in search: {total}")
    print(f"  Expiry buckets: {len(buckets)}")

    all_contracts = []
    for bi, bucket in enumerate(buckets):
        bucket_filter = bucket.get("Filter", "")
        bucket_label  = bucket.get("Label", "?")
        bucket_count  = bucket.get("Count", 0)
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
            }, rate_limiter=rate_limiter)
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
                        "ric":    ric,
                        "expiry": h.get("ExpiryDate", "")[:10],
                        "strike": h.get("StrikePrice"),
                        "cp":     h.get("CallPutOption"),
                    })
            skip += len(hits)

        all_contracts.extend(bucket_rics)
        print(f"  Bucket {bi+1}/{len(buckets)}: {bucket_label} — "
              f"{len(bucket_rics)} OPRA options (running total: {len(all_contracts)})")

    seen   = set()
    deduped = []
    for c in all_contracts:
        if c["ric"] not in seen:
            seen.add(c["ric"])
            deduped.append(c)

    with open(contracts_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ric", "expiry", "strike", "cp"])
        writer.writeheader()
        writer.writerows(deduped)

    print(f"\n  Saved {len(deduped)} unique contracts to {contracts_csv}")
    return deduped


def load_contracts(contracts_csv):
    with open(contracts_csv, "r") as f:
        return list(csv.DictReader(f))


# =====================================================================
# Log / resume
# =====================================================================
def load_completed(log_file):
    completed = set()
    if not os.path.exists(log_file):
        return completed
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


# =====================================================================
# Tick download (single contract, called from worker threads)
# =====================================================================
def download_ticks_for_ric(tm, ric, event_type, rate_limiter):
    url = f"{HIST_URL}/events/{ric}"
    all_rows   = []
    field_names = None
    num_requests = 0
    end_param   = None

    while True:
        params = {"count": str(TICK_BATCH_SIZE), "eventTypes": event_type}
        if end_param:
            params["end"] = end_param

        resp = tm.get(url, params=params, rate_limiter=rate_limiter)
        num_requests += 1

        if resp.status_code != 200:
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

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest   = all_rows[0][0][:19]  if all_rows else ""
    return all_rows, field_names, num_requests, earliest, latest


# =====================================================================
# Parallel tick downloader
# =====================================================================
def download_all_ticks(tm, contracts, tick_type, csv_path, field_names_expected,
                       log_file, ticker, workers):
    completed  = load_completed(log_file)
    remaining  = [(i, c) for i, c in enumerate(contracts)
                  if (c["ric"], tick_type) not in completed]

    total_contracts = len(contracts)
    already_done    = total_contracts - len(remaining)

    print(f"\n{'=' * 70}")
    print(f"[{ticker}] DOWNLOADING {tick_type.upper()} TICKS  (workers={workers})")
    print(f"{'=' * 70}")
    print(f"  Total contracts:   {total_contracts}")
    print(f"  Already completed: {already_done}")
    print(f"  Remaining:         {len(remaining)}")

    if not remaining:
        print("  Nothing to do!")
        return

    rate_limiter = AdaptiveRateLimiter(INITIAL_RATE)

    # Thread-safe shared state
    csv_lock  = threading.Lock()
    log_lock  = threading.Lock()
    stat_lock = threading.Lock()

    stats = {
        "total_ticks":        0,
        "contracts_with_data": 0,
        "contracts_empty":    0,
        "total_requests":     0,
        "total_429s":         0,
        "global_earliest":    None,
        "global_latest":      None,
        "done_count":         already_done,
    }

    progress_log_path = os.path.join(os.path.dirname(csv_path), "ticks_progress.log")
    start_time        = time.time()

    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    csv_file     = open(csv_path, "a", newline="")
    csv_writer   = csv.writer(csv_file)
    if write_header:
        csv_writer.writerow(["ric"] + field_names_expected)

    def worker_task(orig_idx, contract):
        ric = contract["ric"]
        t0  = time.time()
        rows, field_names, num_reqs, earliest, latest = download_ticks_for_ric(
            tm, ric, tick_type, rate_limiter
        )
        elapsed = time.time() - t0
        return ric, rows, num_reqs, earliest, latest, elapsed

    def progress_logger():
        while not stop_progress.is_set():
            time.sleep(PROGRESS_INTERVAL)
            if stop_progress.is_set():
                break
            elapsed = time.time() - start_time
            with stat_lock:
                done  = stats["done_count"]
                ticks = stats["total_ticks"]
                reqs  = stats["total_requests"]
            rate  = rate_limiter.current_rate()
            csv_mb = os.path.getsize(csv_path) / (1024 * 1024) if os.path.exists(csv_path) else 0
            line = (
                f"\n[{datetime.utcnow():%Y-%m-%d %H:%M:%S}Z] "
                f"{ticker} {tick_type} ticks\n"
                f"  Progress:        {done}/{total_contracts} contracts\n"
                f"  Total ticks:     {ticks:,}\n"
                f"  Total requests:  {reqs:,}\n"
                f"  Elapsed:         {elapsed/3600:.2f}h\n"
                f"  Current rate:    {rate:.1f} req/sec\n"
                f"  CSV size:        {csv_mb:.1f} MB\n"
            )
            print(line, flush=True)
            with open(progress_log_path, "a") as pf:
                pf.write(line)

    stop_progress = threading.Event()
    progress_thread = threading.Thread(target=progress_logger, daemon=True)
    progress_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(worker_task, orig_idx, contract): (orig_idx, contract)
                for orig_idx, contract in remaining
            }

            for future in as_completed(futures):
                orig_idx, contract = futures[future]
                try:
                    ric, rows, num_reqs, earliest, latest, elapsed = future.result()
                except Exception as e:
                    ric = contract["ric"]
                    print(f"  ERROR {ric}: {e}")
                    rows, num_reqs, earliest, latest, elapsed = [], 0, "", "", 0.0

                n_ticks = len(rows)

                with csv_lock:
                    for row in rows:
                        csv_writer.writerow([ric] + row)
                    csv_file.flush()

                log_entry = {
                    "ric":       ric,
                    "type":      tick_type,
                    "ticks":     n_ticks,
                    "earliest":  earliest,
                    "latest":    latest,
                    "requests":  num_reqs,
                    "elapsed_s": round(elapsed, 2),
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                with log_lock:
                    with open(log_file, "a") as lf:
                        lf.write(json.dumps(log_entry) + "\n")

                with stat_lock:
                    stats["done_count"] += 1
                    stats["total_ticks"] += n_ticks
                    stats["total_requests"] += num_reqs
                    if n_ticks > 0:
                        stats["contracts_with_data"] += 1
                        if stats["global_earliest"] is None or earliest < stats["global_earliest"]:
                            stats["global_earliest"] = earliest
                        if stats["global_latest"] is None or latest > stats["global_latest"]:
                            stats["global_latest"] = latest
                    else:
                        stats["contracts_empty"] += 1
                    done_count = stats["done_count"]

                time_range = f"({earliest[:10]} – {latest[:10]})" if n_ticks > 0 else "(no data)"
                csv_mb = os.path.getsize(csv_path) / (1024 * 1024) if os.path.exists(csv_path) else 0
                print(
                    f"[{tick_type}] {done_count}/{total_contracts}  {ric}  "
                    f"{n_ticks:,} ticks  {time_range}  "
                    f"{num_reqs} req  {elapsed:.1f}s  |  "
                    f"total: {stats['total_ticks']:,}  "
                    f"rate: {rate_limiter.current_rate():.1f}/s  "
                    f"CSV: {csv_mb:.0f} MB"
                )

    finally:
        stop_progress.set()
        progress_thread.join(timeout=2)
        csv_file.close()

    elapsed_total = time.time() - start_time
    csv_mb = os.path.getsize(csv_path) / (1024 * 1024) if os.path.exists(csv_path) else 0

    summary = (
        f"\n{'=' * 70}\n"
        f"[{ticker}] {tick_type.upper()} DOWNLOAD COMPLETE\n"
        f"{'=' * 70}\n"
        f"  Contracts processed:  {len(remaining)}\n"
        f"  Contracts with data:  {stats['contracts_with_data']}\n"
        f"  Contracts empty:      {stats['contracts_empty']}\n"
        f"  Total ticks:          {stats['total_ticks']:,}\n"
        f"  Total requests:       {stats['total_requests']:,}\n"
        f"  Data range:           {stats['global_earliest'] or 'N/A'} to {stats['global_latest'] or 'N/A'}\n"
        f"  CSV size:             {csv_mb:.1f} MB\n"
        f"  Elapsed:              {elapsed_total/3600:.2f}h\n"
        f"  Final rate:           {rate_limiter.current_rate():.2f} req/sec\n"
    )
    print(summary)
    with open(progress_log_path, "a") as pf:
        pf.write(summary)


# =====================================================================
# Main
# =====================================================================
def main():
    if len(sys.argv) < 2:
        print("Usage: python download_option_ticks.py TICKER [MODE] [WORKERS]")
        print(f"  Supported tickers: {', '.join(sorted(UNDERLYING_MAP.keys()))}")
        print("  Modes: all (default), discover, trades, quotes")
        sys.exit(1)

    ticker  = sys.argv[1].upper()
    mode    = sys.argv[2] if len(sys.argv) > 2 else "all"
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_WORKERS

    if mode not in ("all", "discover", "trades", "quotes"):
        print(f"Unknown mode: {mode}. Use: all, discover, trades, quotes")
        sys.exit(1)

    if ticker not in UNDERLYING_MAP:
        print(f"Warning: {ticker} not in known map, assuming UnderlyingQuoteRIC = '{ticker}.O'")
        base_filter = f"UnderlyingQuoteRIC eq '{ticker}.O'"
    else:
        base_filter, _ = UNDERLYING_MAP[ticker]

    script_dir    = os.path.dirname(os.path.abspath(__file__))
    base_dir      = os.path.join(script_dir, ticker)
    os.makedirs(base_dir, exist_ok=True)

    contracts_csv = os.path.join(base_dir, "option_contracts.csv")
    trade_csv     = os.path.join(base_dir, "trade_ticks.csv")
    quote_csv     = os.path.join(base_dir, "quote_ticks.csv")
    log_file      = os.path.join(base_dir, "download_log.jsonl")

    print(f"Ticker:  {ticker}")
    print(f"Mode:    {mode}")
    print(f"Workers: {workers}")
    print(f"Filter:  {base_filter}")
    print(f"Output:  {base_dir}/")

    session, config_path = setup_session(base_dir)
    tm = TokenManager(session)

    # Single rate limiter shared across all stages
    rate_limiter = AdaptiveRateLimiter(INITIAL_RATE)

    try:
        if mode in ("all", "discover"):
            contracts = discover_contracts(tm, ticker, base_filter, contracts_csv, rate_limiter)
        else:
            if not os.path.exists(contracts_csv):
                print(f"No contracts file found at {contracts_csv}. Run discovery first.")
                sys.exit(1)
            contracts = load_contracts(contracts_csv)
            print(f"Loaded {len(contracts)} contracts from {contracts_csv}")

        if mode == "discover":
            return

        if mode in ("all", "trades"):
            download_all_ticks(
                tm, contracts, "trade", trade_csv, TRADE_FIELDS,
                log_file, ticker, workers
            )

        # Quote ticks disabled — too dense, greeks-only value not worth volume
        if mode in ("quotes",):
            download_all_ticks(
                tm, contracts, "quote", quote_csv, QUOTE_FIELDS,
                log_file, ticker, workers
            )

    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)


if __name__ == "__main__":
    main()
