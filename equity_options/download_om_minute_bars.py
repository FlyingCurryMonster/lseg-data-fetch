"""
Download 1-minute bars for expired option contracts from a pre-generated
RIC list CSV, using the LSEG Historical Pricing API.

Reads contracts from a CSV file (no ClickHouse required). Supports all three
contract source files:
  - all_om_contracts.csv    (OptionMetrics, base_ric/query_ric columns)
  - all_cboe_contracts.csv  (CBOE Dec 5 snapshot, base_ric/query_ric columns)
  - all_names_gap_rics.csv  (gap period Aug–Dec 2025, ric column)

Usage:
  python download_om_minute_bars.py TICKER [WORKERS] [--csv CONTRACTS_CSV]

  TICKER:  NVDA, AMD, TSLA, SPY, etc.
  WORKERS: parallel workers (default: 8)
  --csv:   path to contracts CSV (default: data/{TICKER}/contracts.csv)

Output: {TICKER}/om_minute_bars.csv        (bar data; columns discovered from API)
        {TICKER}/om_bars_log.jsonl          (resume + progress log)
        {TICKER}/om_bars_progress.log       (timestamped throughput log)

Safe to kill and restart — resumes from om_bars_log.jsonl.
Downloads full available history (up to LSEG's 1-year rolling retention window).
"""

import argparse
import csv
import json
import os
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# Config
# =====================================================================
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"
AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token"
BATCH_SIZE = 10000
MAX_RETRIES = 5
INITIAL_RATE = 25.0   # req/sec — match LSEG's stated limit, avoid 429 burst at startup
MAX_RATE     = 50.0   # upper bound for recovery — leave room to probe above 25
RATE_BACKOFF = 0.80   # multiply by this on each 429 (drop 20%)
MIN_RATE = 1.0


# =====================================================================
# Contract loading from CSV
# =====================================================================
def load_contracts_from_csv(ticker, csv_path):
    """
    Load contracts for a ticker from a pre-generated CSV.

    Supports:
      - Files with base_ric/query_ric columns (all_om_contracts.csv,
        all_cboe_contracts.csv)
      - Files with a ric column (all_names_gap_rics.csv)

    Returns list of (base_ric, query_ric) tuples.
    """
    contracts = []
    ticker_upper = ticker.upper()
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        has_ticker_col = "ticker" in (reader.fieldnames or [])
        for row in reader:
            if has_ticker_col and row.get("ticker", "").upper() != ticker_upper:
                continue
            if "query_ric" in row:
                contracts.append((row["base_ric"], row["query_ric"]))
            else:
                ric = row["ric"]
                contracts.append((ric.split("^")[0], ric))
    return contracts


# =====================================================================
# Adaptive rate limiter
# =====================================================================
class AdaptiveRateLimiter:
    def __init__(self, initial_rate=INITIAL_RATE):
        self._rate = initial_rate
        self._tokens = initial_rate
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._request_times = deque()
        self._total_requests = 0
        self._total_429s = 0
        self._last_429_time = 0.0      # monotonic time of last 429
        self._last_recovery_time = 0.0 # monotonic time of last rate increase

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._total_requests += 1
                    self._request_times.append(now)
                    while self._request_times and now - self._request_times[0] > 60:
                        self._request_times.popleft()
                    # Recover rate by 15% if no 429 in the last 15s
                    if (self._rate < MAX_RATE
                            and now - self._last_429_time >= 15.0
                            and now - self._last_recovery_time >= 15.0):
                        old = self._rate
                        self._rate = min(MAX_RATE, self._rate * 1.15)
                        self._last_recovery_time = now
                        print(f"\n  *** recovery — rate {old:.2f} → {self._rate:.2f} req/sec ***\n",
                              flush=True)
                    return
            time.sleep(0.001)

    def on_429(self):
        with self._lock:
            old = self._rate
            self._rate = max(MIN_RATE, self._rate * RATE_BACKOFF)
            self._total_429s += 1
            self._last_429_time = time.monotonic()
            print(f"\n  *** 429 — rate {old:.2f} → {self._rate:.2f} req/sec "
                  f"(total 429s: {self._total_429s}) ***\n", flush=True)

    def requests_per_minute(self):
        with self._lock:
            now = time.monotonic()
            while self._request_times and now - self._request_times[0] > 60:
                self._request_times.popleft()
            return len(self._request_times)

    @property
    def rate(self):
        return self._rate

    @property
    def total_requests(self):
        return self._total_requests

    @property
    def total_429s(self):
        return self._total_429s


# =====================================================================
# Session management (thread-safe, direct OAuth)
# =====================================================================
class TokenManager:
    """Direct OAuth token management — no lseg.data SDK dependency.

    Authenticates via password grant, refreshes via refresh_token grant,
    falls back to full re-auth, and proactively refreshes before expiry.
    """

    def __init__(self):
        self._app_key = os.getenv("DSWS_APPKEY")
        self._username = os.getenv("DSWS_USERNAME")
        self._password = os.getenv("DSWS_PASSWORD")
        self._token = None
        self._refresh_token_str = None
        self._token_expiry = 0
        self._lock = threading.Lock()
        self._consecutive_401s = 0
        self.authenticate()

    def authenticate(self):
        """Get initial token via password grant."""
        resp = requests.post(AUTH_URL, data={
            "grant_type": "password",
            "username": self._username,
            "password": self._password,
            "client_id": self._app_key,
            "scope": "trapi",
            "takeExclusiveSignOnControl": "true",
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._refresh_token_str = data.get("refresh_token")
        self._token_expiry = time.time() + int(data.get("expires_in", 300)) - 30
        self._consecutive_401s = 0
        print(f"  [auth] Authenticated (token expires in {data.get('expires_in', '?')}s)",
              flush=True)

    def refresh(self):
        """Refresh token via refresh_token grant, fall back to full re-auth."""
        with self._lock:
            if self._refresh_token_str:
                try:
                    resp = requests.post(AUTH_URL, data={
                        "grant_type": "refresh_token",
                        "refresh_token": self._refresh_token_str,
                        "client_id": self._app_key,
                    }, headers={"Content-Type": "application/x-www-form-urlencoded"})
                    if resp.status_code == 200:
                        data = resp.json()
                        self._token = data["access_token"]
                        self._refresh_token_str = data.get("refresh_token",
                                                           self._refresh_token_str)
                        self._token_expiry = (time.time()
                                              + int(data.get("expires_in", 300)) - 30)
                        self._consecutive_401s = 0
                        print(f"  [auth] Token refreshed "
                              f"(expires in {data.get('expires_in', '?')}s)",
                              flush=True)
                        return
                except Exception:
                    pass
            # Fallback: full re-auth
            self.authenticate()

    def headers(self):
        """Return auth headers, proactively refreshing if near expiry."""
        if time.time() > self._token_expiry:
            self.refresh()
        return {"Authorization": f"Bearer {self._token}"}

    def on_401(self):
        """Called on 401 response — refresh and track consecutive failures."""
        with self._lock:
            self._consecutive_401s += 1
        self.refresh()

    @property
    def consecutive_401s(self):
        return self._consecutive_401s


# =====================================================================
# Fetch bars for one RIC (all pages, full history)
# =====================================================================
def fetch_bars(tm, rl, ric):
    """
    Fetch all available 1-min bars for a RIC (up to LSEG's 1-year retention window),
    paginating backwards until no more data.

    Returns:
        (all_rows, num_requests, earliest, latest, field_names, status)
        field_names: list of column names from the API response headers, or None
        status: "ok" (got data), "empty" (API returned 200 with no data),
                "error" (request failed after retries)
    """
    url = f"{HIST_URL}/intraday-summaries/{ric}"
    all_rows = []
    num_requests = 0
    end_param = None
    field_names = None
    status = "ok"

    while True:
        params = {"count": str(BATCH_SIZE)}
        if end_param:
            params["end"] = end_param

        resp = None
        for attempt in range(MAX_RETRIES):
            rl.acquire()
            try:
                resp = requests.get(url, headers=tm.headers(), params=params, timeout=30)
                num_requests += 1
                if resp.status_code == 429:
                    rl.on_429()
                    time.sleep(2)
                    continue
                if resp.status_code == 401:
                    tm.on_401()
                    continue
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                num_requests += 1
                wait = min(60, 2 ** attempt * 5)
                print(f"    Network error ({ric}): {type(e).__name__}, retry in {wait}s", flush=True)
                time.sleep(wait)

        if resp is None or resp.status_code != 200:
            status = "error"
            break

        data = resp.json()
        batch_rows = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if field_names is None and "headers" in item:
                field_names = [
                    h["name"] if isinstance(h, dict) else str(h)
                    for h in item["headers"]
                ]
            if "data" in item and item["data"]:
                batch_rows = item["data"]

        if not batch_rows:
            break

        all_rows.extend(batch_rows)

        if len(batch_rows) < BATCH_SIZE:
            break

        end_param = batch_rows[-1][0]

    if status != "error":
        status = "ok" if all_rows else "empty"

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest   = all_rows[0][0][:19]  if all_rows else ""
    return all_rows, num_requests, earliest, latest, field_names, status


# =====================================================================
# Resume helpers
# =====================================================================
def load_completed(log_file):
    """Load completed contracts from log. Only treats 'ok' and 'empty' as done.

    Legacy log entries (no 'status' field) are treated as done if requests < MAX_RETRIES,
    and as needing retry if requests == MAX_RETRIES (likely failed).
    """
    completed = set()
    if os.path.exists(log_file):
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    ric = entry["ric"]
                    status = entry.get("status")
                    if status is not None:
                        # New format: only skip ok/empty
                        if status in ("ok", "empty"):
                            completed.add(ric)
                    else:
                        # Legacy format: treat max-retry entries as needing retry
                        if entry.get("requests", 0) < MAX_RETRIES:
                            completed.add(ric)
                except (json.JSONDecodeError, KeyError):
                    continue
    return completed


# =====================================================================
# Worker
# =====================================================================
def worker_task(tm, rl, ric, query_ric, bars_csv, csv_lock, header_event,
                log_file, log_lock, counters, counter_lock):
    t0 = time.time()
    rows, num_requests, earliest, latest, field_names, status = fetch_bars(tm, rl, query_ric)
    elapsed = time.time() - t0
    n = len(rows)

    # Only write data for clean completions — not errors
    if rows and status == "ok":
        with csv_lock:
            # Write CSV header on first worker that returns data (once only)
            if field_names and not header_event.is_set():
                with open(bars_csv, "w", newline="") as f:
                    csv.writer(f).writerow(["ric"] + field_names)
                header_event.set()
            with open(bars_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow([ric] + row)

    entry = {
        "ric": ric,
        "query_ric": query_ric,
        "bars": n,
        "status": status,
        "earliest": earliest,
        "latest": latest,
        "requests": num_requests,
        "elapsed_s": round(elapsed, 2),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with log_lock:
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    with counter_lock:
        counters["done"] += 1
        counters["total_bars"] += n
        counters["with_data"] += (1 if n > 0 else 0)
        if status == "error":
            counters["errors"] = counters.get("errors", 0) + 1

    return ric, n, earliest, latest, elapsed, status


# =====================================================================
# Main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Download 1-min option bars from pre-generated RIC list"
    )
    parser.add_argument("ticker", help="Ticker symbol (e.g. NVDA, SPY)")
    parser.add_argument("workers", nargs="?", type=int, default=8)
    parser.add_argument("--csv", dest="contracts_csv", default=None,
                        help="Contracts CSV with base_ric/query_ric columns "
                             "(default: data/{TICKER}/contracts.csv)")
    args = parser.parse_args()

    ticker = args.ticker.upper()
    num_workers = args.workers

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir      = os.path.join(script_dir, "data", ticker)
    os.makedirs(base_dir, exist_ok=True)
    contracts_csv = args.contracts_csv or os.path.join(base_dir, "contracts.csv")

    bars_csv     = os.path.join(base_dir, "om_minute_bars.csv")
    log_file     = os.path.join(base_dir, "om_bars_log.jsonl")
    progress_log = os.path.join(base_dir, "om_bars_progress.log")

    print(f"Ticker:       {ticker}")
    print(f"Contracts:    {os.path.basename(contracts_csv)}")
    print(f"Workers:      {num_workers}")
    print(f"History:      full (1-year LSEG retention window)")
    print(f"Initial rate: {INITIAL_RATE} req/sec  (limit: 25)")
    print(f"Output:       {bars_csv}")
    print()

    print(f"Loading contracts from {os.path.basename(contracts_csv)}...", flush=True)
    contracts = load_contracts_from_csv(ticker, contracts_csv)
    print(f"  Found {len(contracts):,} contracts for {ticker}")

    # Resume
    completed = load_completed(log_file)
    remaining = [(base_ric, query_ric) for base_ric, query_ric in contracts
                 if base_ric not in completed]

    print(f"  Already completed: {len(completed):,}")
    print(f"  Remaining:         {len(remaining):,}")
    print()

    if not remaining:
        print("Nothing to do!")
        return

    tm = TokenManager()
    rl = AdaptiveRateLimiter(INITIAL_RATE)

    csv_lock     = threading.Lock()
    log_lock     = threading.Lock()
    counter_lock = threading.Lock()
    counters = {"done": 0, "total_bars": 0, "with_data": 0, "errors": 0}

    # header_event: set when CSV header has been written (either pre-existing or by first worker)
    header_event = threading.Event()
    if os.path.exists(bars_csv) and os.path.getsize(bars_csv) > 0:
        header_event.set()  # resuming — header already present, don't overwrite

    total      = len(remaining)
    start_time = time.time()
    last_progress_log = time.time()

    # Write header to progress log
    with open(progress_log, "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"Run started: {datetime.utcnow().isoformat()}Z\n")
        f.write(f"Ticker: {ticker}, Contracts: {total}, Workers: {num_workers}\n")
        f.write(f"{'='*60}\n")

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(
                    worker_task, tm, rl, base_ric, query_ric,
                    bars_csv, csv_lock, header_event,
                    log_file, log_lock, counters, counter_lock
                ): base_ric
                for base_ric, query_ric in remaining
            }

            for future in as_completed(futures):
                ric, n, earliest, latest, elapsed, status = future.result()

                with counter_lock:
                    done       = counters["done"]
                    total_bars = counters["total_bars"]
                    errors     = counters.get("errors", 0)

                csv_mb    = os.path.getsize(bars_csv) / 1024 / 1024 if os.path.exists(bars_csv) else 0
                rpm       = rl.requests_per_minute()
                time_range = f"({earliest[:10]} – {latest[:10]})" if n > 0 else "(no data)"
                err_tag   = f"  *** ERROR ***" if status == "error" else ""

                print(
                    f"[bars] {done}/{total}  {ric}  {n:,} bars  {time_range}  "
                    f"{elapsed:.1f}s  |  total: {total_bars:,}  CSV: {csv_mb:.0f}MB  "
                    f"rate: {rl.rate:.1f}/s  rpm: {rpm}"
                    f"  errors: {errors}" + err_tag,
                    flush=True
                )

                # Timestamped progress log every 60s
                now = time.time()
                if now - last_progress_log >= 60:
                    elapsed_total     = now - start_time
                    contracts_per_min = done / elapsed_total * 60 if elapsed_total > 0 else 0
                    eta_h             = (total - done) / (done / elapsed_total) / 3600 if done > 0 else 0
                    bars_per_min      = total_bars / elapsed_total * 60 if elapsed_total > 0 else 0

                    summary = (
                        f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] "
                        f"Progress: {done}/{total} ({100*done/total:.1f}%)  "
                        f"rpm: {rpm}  rate: {rl.rate:.1f}/s  "
                        f"bars/min: {bars_per_min:,.0f}  "
                        f"contracts/min: {contracts_per_min:.1f}  "
                        f"CSV: {csv_mb:.0f}MB  ETA: {eta_h:.1f}h  "
                        f"429s: {rl.total_429s}"
                    )
                    print(f"\n  ── PROGRESS ─────────────────────────────────────")
                    print(f"  {summary}")
                    print(f"  ─────────────────────────────────────────────────\n", flush=True)

                    with open(progress_log, "a") as pf:
                        pf.write(summary + "\n")

                    last_progress_log = now

    finally:
        pass

    elapsed_total = time.time() - start_time
    csv_mb = os.path.getsize(bars_csv) / 1024 / 1024 if os.path.exists(bars_csv) else 0

    errors = counters.get("errors", 0)
    summary = (
        f"\n{'='*70}\n"
        f"COMPLETE: {ticker} OM minute bars\n"
        f"  Contracts:         {total:,}\n"
        f"  With data:         {counters['with_data']:,}\n"
        f"  Errors:            {errors:,}\n"
        f"  Total bars:        {counters['total_bars']:,}\n"
        f"  CSV size:          {csv_mb:.0f} MB\n"
        f"  Elapsed:           {elapsed_total/3600:.2f}h\n"
        f"  Total requests:    {rl.total_requests:,}\n"
        f"  Total 429s:        {rl.total_429s}\n"
        f"  Final rate:        {rl.rate:.2f} req/sec\n"
    )
    print(summary, flush=True)
    with open(progress_log, "a") as pf:
        pf.write(summary)


if __name__ == "__main__":
    main()
