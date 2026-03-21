"""
Download 1-minute bars for all expired option contracts in the OptionMetrics
option_pricing table, within the LSEG 1-year retention window.

Queries ClickHouse for contracts, constructs expired LSEG RICs, downloads
1-min bars with adaptive rate limiting and parallel workers.

Usage:
  python download_om_minute_bars.py TICKER [WORKERS]

  TICKER:  NVDA, AMD, TSLA, SPY, etc.
  WORKERS: parallel workers (default: 8)

Output: {TICKER}/om_minute_bars.csv
        {TICKER}/om_bars_log.jsonl     (resume + progress log)
        {TICKER}/om_bars_progress.log  (timestamped throughput log)

Safe to kill and restart — resumes from om_bars_log.jsonl.

LSEG RIC construction from OptionMetrics fields:
  - Root: ticker symbol
  - Month code: A-L (Jan-Dec) for calls, M-X (Jan-Dec) for puts
  - Day: DD of expiry
  - Year: YY of expiry
  - Strike: (strike_price / 10) zero-padded to 5 digits
  - Suffix: ^<month_code><YY> for expired contracts
  Example: NVDA $120 Call exp 2025-06-20 → NVDAF202512000.U^F25
"""

import csv
import json
import os
import sys
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta

import requests
from dotenv import load_dotenv
import lseg.data as ld
from clickhouse_driver import Client

load_dotenv()

# =====================================================================
# Config
# =====================================================================
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"
BATCH_SIZE = 10000
MAX_RETRIES = 5
INITIAL_RATE = 23.0   # req/sec (limit is 25 for intraday-summaries)
RATE_BACKOFF = 0.90   # multiply by this on each 429
MIN_RATE = 0.5

# OptionMetrics secid map — add more as needed
OM_SECID = {
    "NVDA": 108321,
    "AMD":  101121,
    "TSLA": 143439,
    "SPY":  109820,
}

# LSEG ticker root overrides (if different from OM ticker)
LSEG_ROOT = {
    "SPY": "SPY",
    "NVDA": "NVDA",
    "AMD": "AMD",
    "TSLA": "TSLA",
}

CALL_CODES = "ABCDEFGHIJKL"  # A=Jan ... L=Dec
PUT_CODES  = "MNOPQRSTUVWX"  # M=Jan ... X=Dec

BAR_FIELDS = [
    "DATE_TIME", "HIGH_1", "LOW_1", "OPEN_PRC", "TRDPRC_1",
    "NUM_MOVES", "ACVOL_UNS",
    "BID_HIGH_1", "BID_LOW_1", "OPEN_BID", "BID", "BID_NUMMOV",
    "ASK_HIGH_1", "ASK_LOW_1", "OPEN_ASK", "ASK", "ASK_NUMMOV",
]


# =====================================================================
# RIC construction
# =====================================================================
def build_lseg_ric(root, exdate_str, cp_flag, strike_price):
    """
    Build expired LSEG OPRA RIC from OptionMetrics fields.

    Args:
        root:         ticker root (e.g. 'NVDA')
        exdate_str:   'YYYY-MM-DD'
        cp_flag:      'C' or 'P'
        strike_price: raw OM value (divide by 10 for LSEG strike cents)

    Returns:
        Expired RIC string, e.g. 'NVDAF202512000.U^F25'
    """
    month = int(exdate_str[5:7])
    day   = int(exdate_str[8:10])
    year  = int(exdate_str[2:4])

    month_code = CALL_CODES[month - 1] if cp_flag == "C" else PUT_CODES[month - 1]
    lseg_strike = int(strike_price) // 10
    expired_code = CALL_CODES[month - 1]  # suffix always uses call-side letter

    active_ric = f"{root}{month_code}{day:02d}{year:02d}{lseg_strike:05d}.U"
    return f"{active_ric}^{expired_code}{year:02d}"


# =====================================================================
# ClickHouse query
# =====================================================================
def fetch_om_contracts(secid, ticker, window_start, window_end):
    """
    Query OptionMetrics for all unique contracts expiring within the window.
    Returns list of (exdate, cp_flag, strike_price) tuples.
    """
    ch = Client("localhost")
    rows = ch.execute(
        """
        SELECT DISTINCT exdate, cp_flag, strike_price
        FROM option_metrics.option_pricing
        WHERE secid = %(secid)s
          AND exdate >= %(start)s
          AND exdate < %(end)s
        ORDER BY exdate, cp_flag, strike_price
        """,
        {"secid": secid, "start": window_start, "end": window_end},
    )
    return rows  # list of (date, str, int)


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
# Session management (thread-safe)
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
                    "app-key":  os.getenv("DSWS_APPKEY"),
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
# Fetch bars for one RIC (all pages)
# =====================================================================
def fetch_bars(tm, rl, ric):
    url = f"{HIST_URL}/intraday-summaries/{ric}"
    all_rows = []
    num_requests = 0
    end_param = None

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
                    tm.refresh()
                    continue
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                num_requests += 1
                wait = min(60, 2 ** attempt * 5)
                print(f"    Network error ({ric}): {type(e).__name__}, retry in {wait}s", flush=True)
                time.sleep(wait)

        if resp is None or resp.status_code != 200:
            break

        data = resp.json()
        batch_rows = []
        for item in data:
            if isinstance(item, dict) and "data" in item and item["data"]:
                batch_rows = item["data"]

        if not batch_rows:
            break

        all_rows.extend(batch_rows)

        if len(batch_rows) < BATCH_SIZE:
            break

        end_param = batch_rows[-1][0]

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest   = all_rows[0][0][:19]  if all_rows else ""
    return all_rows, num_requests, earliest, latest


# =====================================================================
# Resume helpers
# =====================================================================
def load_completed(log_file):
    completed = set()
    if os.path.exists(log_file):
        with open(log_file) as f:
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
# Worker
# =====================================================================
def worker_task(tm, rl, ric, query_ric, bars_csv, csv_lock, log_file, log_lock, counters, counter_lock):
    t0 = time.time()
    rows, num_requests, earliest, latest = fetch_bars(tm, rl, query_ric)
    elapsed = time.time() - t0
    n = len(rows)

    if rows:
        with csv_lock:
            with open(bars_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow([ric] + row)

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
        with open(log_file, "a") as f:
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
        print("Usage: python download_om_minute_bars.py TICKER [WORKERS]")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 8

    if ticker not in OM_SECID:
        print(f"Unknown ticker: {ticker}. Known: {', '.join(sorted(OM_SECID))}")
        sys.exit(1)

    secid = OM_SECID[ticker]
    root  = LSEG_ROOT.get(ticker, ticker)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir   = os.path.join(script_dir, ticker)
    os.makedirs(base_dir, exist_ok=True)

    bars_csv    = os.path.join(base_dir, "om_minute_bars.csv")
    log_file    = os.path.join(base_dir, "om_bars_log.jsonl")
    progress_log = os.path.join(base_dir, "om_bars_progress.log")

    # 1-year retention window
    today        = date.today()
    window_end   = today
    window_start = today - timedelta(days=365)

    print(f"Ticker:       {ticker}  (OM secid {secid})")
    print(f"Window:       {window_start} → {window_end}  (1-year retention)")
    print(f"Workers:      {num_workers}")
    print(f"Initial rate: {INITIAL_RATE} req/sec  (limit: 25)")
    print(f"Output:       {bars_csv}")
    print()

    # Fetch contracts from OptionMetrics
    print("Querying OptionMetrics for expired contracts...", flush=True)
    om_rows = fetch_om_contracts(secid, ticker, str(window_start), str(window_end))
    print(f"  Found {len(om_rows):,} unique contracts in OptionMetrics")

    # Build RICs
    contracts = []
    for exdate, cp_flag, strike_price in om_rows:
        exdate_str = str(exdate)[:10]
        ric       = build_lseg_ric(root, exdate_str, cp_flag, strike_price)
        # strip the ^suffix for the "canonical" key (stored in CSV and log)
        base_ric  = ric.split("^")[0]
        contracts.append((base_ric, ric, exdate_str))

    # Resume
    completed = load_completed(log_file)
    remaining = [(base_ric, query_ric, exdate) for base_ric, query_ric, exdate in contracts
                 if base_ric not in completed]

    print(f"  Already completed: {len(completed):,}")
    print(f"  Remaining:         {len(remaining):,}")
    print()

    if not remaining:
        print("Nothing to do!")
        return

    # Write CSV header if new
    if not os.path.exists(bars_csv) or os.path.getsize(bars_csv) == 0:
        with open(bars_csv, "w", newline="") as f:
            csv.writer(f).writerow(["ric"] + BAR_FIELDS)

    session, config_path = setup_session(base_dir)
    tm = TokenManager(session)
    rl = AdaptiveRateLimiter(INITIAL_RATE)

    csv_lock     = threading.Lock()
    log_lock     = threading.Lock()
    counter_lock = threading.Lock()
    counters = {"done": 0, "total_bars": 0, "with_data": 0}

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
                    bars_csv, csv_lock, log_file, log_lock,
                    counters, counter_lock
                ): base_ric
                for base_ric, query_ric, _ in remaining
            }

            for future in as_completed(futures):
                ric, n, earliest, latest, elapsed = future.result()

                with counter_lock:
                    done       = counters["done"]
                    total_bars = counters["total_bars"]

                csv_mb    = os.path.getsize(bars_csv) / 1024 / 1024 if os.path.exists(bars_csv) else 0
                rpm       = rl.requests_per_minute()
                time_range = f"({earliest[:10]} – {latest[:10]})" if n > 0 else "(no data)"

                print(
                    f"[bars] {done}/{total}  {ric}  {n:,} bars  {time_range}  "
                    f"{elapsed:.1f}s  |  total: {total_bars:,}  CSV: {csv_mb:.0f}MB  "
                    f"rate: {rl.rate:.1f}/s  rpm: {rpm}",
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
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)

    elapsed_total = time.time() - start_time
    csv_mb = os.path.getsize(bars_csv) / 1024 / 1024 if os.path.exists(bars_csv) else 0

    summary = (
        f"\n{'='*70}\n"
        f"COMPLETE: {ticker} OM minute bars\n"
        f"  Contracts:         {total:,}\n"
        f"  With data:         {counters['with_data']:,}\n"
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
