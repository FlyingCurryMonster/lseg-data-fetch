"""
Download 1-minute bars for option contracts that had trade data.

Adaptive rate limiter: starts at INITIAL_RATE req/sec, backs off 10% on each 429.
Thread pool: multiple workers fetch contracts in parallel.
Logs requests/min to track optimal throughput.

Usage:
  python download_minute_bars.py TICKER [WORKERS]

  TICKER:  SPY, NVDA, AMD, TSLA, etc.
  WORKERS: parallel workers (default: 8)

Source: {TICKER}/download_log.jsonl — only contracts with ticks > 0
Output: {TICKER}/minute_bars.csv
        {TICKER}/bars_log.jsonl       (resume log)

Safe to kill and restart — resumes from bars_log.jsonl.
"""

import csv
import json
import os
import sys
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date

import requests
from dotenv import load_dotenv
import lseg.data as ld

load_dotenv()

# =====================================================================
# Config
# =====================================================================
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"
BATCH_SIZE = 10000
MAX_RETRIES = 5
INITIAL_RATE = 23.0   # req/sec starting point (limit is 25 for intraday-summaries)
RATE_BACKOFF = 0.90   # multiply by this on each 429
MIN_RATE = 0.5        # never go below this

MONTH_CODES = "ABCDEFGHIJKL"


def make_expired_ric(ric, expiry_str):
    """Append ^<month_code><YY> suffix for expired contracts."""
    month = int(expiry_str[5:7])
    year = expiry_str[2:4]
    return f"{ric}^{MONTH_CODES[month - 1]}{year}"


def resolve_ric(ric, expiry_str):
    """Return the RIC to query: active format if still live, expired format if past."""
    try:
        expiry = date.fromisoformat(expiry_str)
    except (ValueError, TypeError):
        return ric
    if expiry < date.today():
        return make_expired_ric(ric, expiry_str)
    return ric


BAR_FIELDS = [
    "DATE_TIME", "HIGH_1", "LOW_1", "OPEN_PRC", "TRDPRC_1",
    "NUM_MOVES", "ACVOL_UNS",
    "BID_HIGH_1", "BID_LOW_1", "OPEN_BID", "BID", "BID_NUMMOV",
    "ASK_HIGH_1", "ASK_LOW_1", "OPEN_ASK", "ASK", "ASK_NUMMOV",
]


# =====================================================================
# Adaptive rate limiter (token bucket)
# =====================================================================
class AdaptiveRateLimiter:
    """
    Token bucket with adaptive backoff on 429 errors.
    Thread-safe. Tracks requests per minute for logging.
    """
    def __init__(self, initial_rate=INITIAL_RATE):
        self._rate = initial_rate      # current allowed req/sec
        self._tokens = initial_rate    # start full
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._request_times = deque()  # timestamps of recent requests
        self._total_requests = 0
        self._total_429s = 0

    def acquire(self):
        """Block until a token is available, then consume one."""
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
                    # Trim entries older than 60s
                    while self._request_times and now - self._request_times[0] > 60:
                        self._request_times.popleft()
                    return
            time.sleep(0.001)

    def on_429(self):
        with self._lock:
            old = self._rate
            self._rate = max(MIN_RATE, self._rate * RATE_BACKOFF)
            self._total_429s += 1
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
# Session / token management (thread-safe)
# =====================================================================
class TokenManager:
    def __init__(self, session):
        self.session = session
        self._token = session._access_token
        self._lock = threading.Lock()

    def headers(self):
        with self._lock:
            return {"Authorization": f"Bearer {self._token}"}

    def refresh(self):
        with self._lock:
            self._token = self.session._access_token


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


# =====================================================================
# Fetch all 1-min bars for one RIC (paginated)
# =====================================================================
def fetch_bars(tm, rl, ric):
    url = f"{HIST_URL}/intraday-summaries/{ric}"
    all_rows = []
    field_names = None
    num_requests = 0
    end_param = None

    while True:
        params = {"count": str(BATCH_SIZE)}
        if end_param:
            params["end"] = end_param

        # Retry loop for this page
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
                    tm.refresh()
                    continue

                break  # success or non-retriable error

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                num_requests += 1
                wait = min(60, 2 ** attempt * 5)
                print(f"    Network error ({ric}): {type(e).__name__}, retry in {wait}s", flush=True)
                time.sleep(wait)

        if resp is None or resp.status_code not in (200,):
            break

        data = resp.json()
        batch_rows = []
        for item in data:
            if isinstance(item, dict):
                if "headers" in item and field_names is None:
                    field_names = [h["name"] for h in item["headers"]]
                if "data" in item and item["data"]:
                    batch_rows = item["data"]

        if not batch_rows:
            break

        all_rows.extend(batch_rows)

        if len(batch_rows) < BATCH_SIZE:
            break

        end_param = batch_rows[-1][0]

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest = all_rows[0][0][:19] if all_rows else ""
    return all_rows, num_requests, earliest, latest


# =====================================================================
# Load / resume helpers
# =====================================================================
def load_contracts_with_trades(trade_log, contracts_csv):
    """Return list of (ric, expiry) tuples that had > 0 trade ticks."""
    # Build expiry lookup from contracts CSV
    expiry_map = {}
    if os.path.exists(contracts_csv):
        with open(contracts_csv) as f:
            for row in csv.DictReader(f):
                expiry_map[row["ric"]] = row.get("expiry", "")

    contracts = []
    if not os.path.exists(trade_log):
        return contracts
    with open(trade_log) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if entry.get("type") == "trade" and entry.get("ticks", 0) > 0:
                    ric = entry["ric"]
                    expiry = expiry_map.get(ric, "")
                    contracts.append((ric, expiry))
            except (json.JSONDecodeError, KeyError):
                continue
    return contracts


def load_completed_bars(bars_log):
    completed = set()
    if os.path.exists(bars_log):
        with open(bars_log) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    completed.add(json.loads(line)["ric"])
                except (json.JSONDecodeError, KeyError):
                    continue
    return completed


# =====================================================================
# Worker task (one per contract)
# =====================================================================
def worker_task(tm, rl, ric, expiry, bars_csv, csv_lock, bars_log, log_lock, counters, counter_lock):
    query_ric = resolve_ric(ric, expiry)
    t0 = time.time()
    rows, num_requests, earliest, latest = fetch_bars(tm, rl, query_ric)
    elapsed = time.time() - t0
    n = len(rows)

    if rows:
        with csv_lock:
            with open(bars_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow([ric] + row)  # store original RIC as key

    entry = {
        "ric": ric,
        "query_ric": query_ric,
        "bars": n,
        "earliest": earliest,
        "latest": latest,
        "requests": num_requests,
        "elapsed_s": round(elapsed, 2),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with log_lock:
        with open(bars_log, "a") as f:
            f.write(json.dumps(entry) + "\n")

    with counter_lock:
        counters["done"] += 1
        counters["total_bars"] += n
        counters["with_data"] += (1 if n > 0 else 0)

    return ric, n, earliest, latest, elapsed


# =====================================================================
# Main
# =====================================================================
def main():
    if len(sys.argv) < 2:
        print("Usage: python download_minute_bars.py TICKER [WORKERS]")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 8

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(script_dir, ticker)
    trade_log = os.path.join(base_dir, "download_log.jsonl")
    bars_csv = os.path.join(base_dir, "minute_bars.csv")
    bars_log = os.path.join(base_dir, "bars_log.jsonl")

    print(f"Ticker:       {ticker}")
    print(f"Workers:      {num_workers}")
    print(f"Initial rate: {INITIAL_RATE} req/sec")
    print(f"Output:       {bars_csv}")
    print()

    contracts_csv = os.path.join(base_dir, "option_contracts.csv")
    all_contracts = load_contracts_with_trades(trade_log, contracts_csv)
    completed = load_completed_bars(bars_log)
    remaining = [(ric, expiry) for ric, expiry in all_contracts if ric not in completed]

    print(f"Contracts with trade data: {len(all_contracts):,}")
    print(f"Already completed:         {len(completed):,}")
    print(f"Remaining:                 {len(remaining):,}")
    print()

    if not remaining:
        print("Nothing to do!")
        return

    if not os.path.exists(bars_csv) or os.path.getsize(bars_csv) == 0:
        with open(bars_csv, "w", newline="") as f:
            csv.writer(f).writerow(["ric"] + BAR_FIELDS)

    session, config_path = setup_session(base_dir)
    tm = TokenManager(session)
    rl = AdaptiveRateLimiter(INITIAL_RATE)

    csv_lock = threading.Lock()
    log_lock = threading.Lock()
    counter_lock = threading.Lock()
    counters = {"done": 0, "total_bars": 0, "with_data": 0}

    total = len(remaining)
    start_time = time.time()
    last_rpm_log = time.time()

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(
                    worker_task, tm, rl, ric, expiry,
                    bars_csv, csv_lock, bars_log, log_lock,
                    counters, counter_lock
                ): ric
                for ric, expiry in remaining
            }

            for future in as_completed(futures):
                ric, n, earliest, latest, elapsed = future.result()

                with counter_lock:
                    done = counters["done"]
                    total_bars = counters["total_bars"]

                csv_mb = os.path.getsize(bars_csv) / 1024 / 1024 if os.path.exists(bars_csv) else 0
                rpm = rl.requests_per_minute()
                time_range = f"({earliest[:10]} – {latest[:10]})" if n > 0 else "(no data)"

                print(
                    f"[bars] {done}/{total}  {ric}  {n:,} bars  {time_range}  "
                    f"{elapsed:.1f}s  |  total: {total_bars:,}  CSV: {csv_mb:.0f}MB  "
                    f"rate: {rl.rate:.1f}/s  rpm: {rpm}",
                    flush=True
                )

                # Print RPM summary every 60s
                now = time.time()
                if now - last_rpm_log >= 60:
                    elapsed_total = now - start_time
                    contracts_per_min = done / elapsed_total * 60 if elapsed_total > 0 else 0
                    eta_h = (total - done) / (done / elapsed_total) / 3600 if done > 0 else 0
                    print(f"\n  ── RPM REPORT ──────────────────────────────────")
                    print(f"  Requests/min:      {rpm}")
                    print(f"  Rate limit:        {rl.rate:.2f} req/sec")
                    print(f"  Total requests:    {rl.total_requests:,}")
                    print(f"  Total 429s:        {rl.total_429s}")
                    print(f"  Contracts/min:     {contracts_per_min:.1f}")
                    print(f"  Progress:          {done}/{total} ({100*done/total:.1f}%)")
                    print(f"  ETA:               {eta_h:.1f}h")
                    print(f"  ────────────────────────────────────────────────\n", flush=True)
                    last_rpm_log = now

    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)

    elapsed_total = time.time() - start_time
    csv_mb = os.path.getsize(bars_csv) / 1024 / 1024 if os.path.exists(bars_csv) else 0

    print(f"\n{'=' * 70}")
    print(f"COMPLETE: {ticker} minute bars")
    print(f"  Contracts processed: {total:,}")
    print(f"  Contracts with data: {counters['with_data']:,}")
    print(f"  Total bars:          {counters['total_bars']:,}")
    print(f"  CSV size:            {csv_mb:.0f} MB")
    print(f"  Elapsed:             {elapsed_total/3600:.1f}h")
    print(f"  Total requests sent: {rl.total_requests:,}")
    print(f"  Total 429s:          {rl.total_429s}")
    print(f"  Final rate:          {rl.rate:.2f} req/sec")


if __name__ == "__main__":
    main()
