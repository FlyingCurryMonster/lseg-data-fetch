"""
Download trade tick data for option contracts from a pre-generated RIC list CSV.

Reads from data/{TICKER}/contracts.csv (same file as minute bars).
Filters to contracts expiring within the last ~3 months (LSEG trade tick retention window).
Downloads all available trade ticks via the LSEG Historical Pricing events endpoint.

Usage:
  python download_trades.py TICKER [WORKERS]

  TICKER:  NVDA, AMD, TSLA, SPY, etc.
  WORKERS: parallel workers (default: 8)

Output: data/{TICKER}/trade_ticks.csv        (tick data; columns discovered from API)
        data/{TICKER}/trades_log.jsonl        (resume + completion log)
        data/{TICKER}/trades_progress.log     (timestamped throughput log)
        data/{TICKER}/trades_run.log          (run log, mirrors stdout)

Safe to kill and restart — resumes from trades_log.jsonl.
"""

import argparse
import csv
import json
import os
import re
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import requests
from dotenv import load_dotenv
import lseg.data as ld

load_dotenv()

# =====================================================================
# Config
# =====================================================================
HIST_URL     = "https://api.refinitiv.com/data/historical-pricing/v1/views"
BATCH_SIZE   = 10000
MAX_RETRIES  = 5
INITIAL_RATE = 23.0   # req/sec (events endpoint cap is 25)
RATE_BACKOFF = 0.90
MIN_RATE     = 0.5
TICK_WINDOW_DAYS = 92  # ~3 months trade tick retention

CALL_MONTHS = {'A':1,'B':2,'C':3,'D':4,'E':5,'F':6,'G':7,'H':8,'I':9,'J':10,'K':11,'L':12}
PUT_MONTHS  = {'M':1,'N':2,'O':3,'P':4,'Q':5,'R':6,'S':7,'T':8,'U':9,'V':10,'W':11,'X':12}


# =====================================================================
# Expiry parsing
# =====================================================================
def expiry_from_ric(query_ric):
    """
    Parse expiry date from a query_ric string.
    Format: {ROOT}{mc}{DD}{YY}{strike5}.U  (active)
         or {ROOT}{mc}{DD}{YY}{strike5}.U^{mc}{YY}  (expired)
    Returns a date object or None if unparseable.
    """
    base = query_ric.split("^")[0]
    if base.endswith(".U"):
        base = base[:-2]
    # Match: any letters (ROOT), then month_code, then DD (2 digits), YY (2 digits), strike (5 digits)
    m = re.search(r"([A-X])(\d{2})(\d{2})\d+$", base)
    if not m:
        return None
    mc, dd, yy = m.group(1), int(m.group(2)), int(m.group(3))
    month = CALL_MONTHS.get(mc) or PUT_MONTHS.get(mc)
    if not month:
        return None
    try:
        return date(2000 + yy, month, dd)
    except ValueError:
        return None


def is_in_tick_window(query_ric, cutoff):
    """Return True if the contract's expiry is on or after cutoff (or can't be parsed)."""
    expiry = expiry_from_ric(query_ric)
    if expiry is None:
        return True   # include if we can't parse — let the API decide
    return expiry >= cutoff


# =====================================================================
# Contract loading
# =====================================================================
def load_contracts_from_csv(ticker, csv_path, cutoff):
    """
    Load contracts for a ticker, filtered to the tick retention window.
    Returns list of (base_ric, query_ric) tuples.
    """
    contracts = []
    skipped   = 0
    ticker_upper = ticker.upper()
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        has_ticker_col = "ticker" in (reader.fieldnames or [])
        for row in reader:
            if has_ticker_col and row.get("ticker", "").upper() != ticker_upper:
                continue
            if "query_ric" in row:
                base_ric  = row["base_ric"]
                query_ric = row["query_ric"]
            else:
                ric       = row["ric"]
                base_ric  = ric.split("^")[0]
                query_ric = ric
            if is_in_tick_window(query_ric, cutoff):
                contracts.append((base_ric, query_ric))
            else:
                skipped += 1
    return contracts, skipped


# =====================================================================
# Adaptive rate limiter (identical to download_om_minute_bars.py)
# =====================================================================
class AdaptiveRateLimiter:
    def __init__(self, initial_rate=INITIAL_RATE):
        self._rate             = initial_rate
        self._tokens           = initial_rate
        self._last_refill      = time.monotonic()
        self._lock             = threading.Lock()
        self._request_times    = deque()
        self._total_requests   = 0
        self._total_429s       = 0
        self._last_429_time    = 0.0
        self._last_recovery_time = 0.0

    def acquire(self):
        while True:
            with self._lock:
                now     = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens      = min(self._rate, self._tokens + elapsed * self._rate)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._total_requests += 1
                    self._request_times.append(now)
                    while self._request_times and now - self._request_times[0] > 60:
                        self._request_times.popleft()
                    if (self._rate < INITIAL_RATE
                            and now - self._last_429_time    >= 60.0
                            and now - self._last_recovery_time >= 60.0):
                        old = self._rate
                        self._rate = min(INITIAL_RATE, self._rate * 1.05)
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
    def rate(self):            return self._rate
    @property
    def total_requests(self):  return self._total_requests
    @property
    def total_429s(self):      return self._total_429s


# =====================================================================
# Session management (thread-safe)
# =====================================================================
class TokenManager:
    def __init__(self, session):
        self.session = session
        self._token  = session._access_token
        self._lock   = threading.Lock()

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


# =====================================================================
# Fetch ticks for one RIC (all pages)
# =====================================================================
def fetch_ticks(tm, rl, ric):
    """
    Fetch all available trade ticks for a RIC, paginating backwards.
    Returns: (all_rows, num_requests, earliest, latest, field_names)
    """
    url       = f"{HIST_URL}/events/{ric}"
    all_rows  = []
    num_requests = 0
    end_param = None
    field_names = None

    while True:
        params = {"count": str(BATCH_SIZE), "eventTypes": "trade"}
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
                print(f"    Network error ({ric}): {type(e).__name__}, retry in {wait}s",
                      flush=True)
                time.sleep(wait)

        if resp is None or resp.status_code != 200:
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

    earliest = all_rows[-1][0][:19] if all_rows else ""
    latest   = all_rows[0][0][:19]  if all_rows else ""
    return all_rows, num_requests, earliest, latest, field_names


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
def worker_task(tm, rl, ric, query_ric, ticks_csv, csv_lock, header_event,
                log_file, log_lock, counters, counter_lock):
    t0 = time.time()
    rows, num_requests, earliest, latest, field_names = fetch_ticks(tm, rl, query_ric)
    elapsed = time.time() - t0
    n = len(rows)

    if rows:
        with csv_lock:
            if field_names and not header_event.is_set():
                with open(ticks_csv, "w", newline="") as f:
                    csv.writer(f).writerow(["ric"] + field_names)
                header_event.set()
            with open(ticks_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow([ric] + row)

    entry = {
        "ric":       ric,
        "query_ric": query_ric,
        "ticks":     n,
        "earliest":  earliest,
        "latest":    latest,
        "requests":  num_requests,
        "elapsed_s": round(elapsed, 2),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with log_lock:
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    with counter_lock:
        counters["done"]        += 1
        counters["total_ticks"] += n
        counters["with_data"]   += (1 if n > 0 else 0)

    return ric, n, earliest, latest, elapsed


# =====================================================================
# Main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Download trade ticks for options from pre-generated RIC list"
    )
    parser.add_argument("ticker",  help="Ticker symbol (e.g. NVDA, SPY)")
    parser.add_argument("workers", nargs="?", type=int, default=8)
    args = parser.parse_args()

    ticker      = args.ticker.upper()
    num_workers = args.workers
    cutoff      = date.today() - timedelta(days=TICK_WINDOW_DAYS)

    script_dir    = os.path.dirname(os.path.abspath(__file__))
    base_dir      = os.path.join(script_dir, "data", ticker)
    os.makedirs(base_dir, exist_ok=True)

    contracts_csv = os.path.join(base_dir, "contracts.csv")
    ticks_csv     = os.path.join(base_dir, "trade_ticks.csv")
    log_file      = os.path.join(base_dir, "trades_log.jsonl")
    progress_log  = os.path.join(base_dir, "trades_progress.log")
    run_log       = os.path.join(base_dir, "trades_run.log")

    def log(msg):
        print(msg, flush=True)
        with open(run_log, "a") as f:
            f.write(msg + "\n")

    log(f"Ticker:       {ticker}")
    log(f"Workers:      {num_workers}")
    log(f"Tick window:  {cutoff} → today (~{TICK_WINDOW_DAYS} days)")
    log(f"Initial rate: {INITIAL_RATE} req/sec  (limit: 25)")
    log(f"Output:       {ticks_csv}")
    log("")

    log(f"Loading contracts from contracts.csv...")
    contracts, skipped = load_contracts_from_csv(ticker, contracts_csv, cutoff)
    log(f"  In tick window: {len(contracts):,}  |  skipped (too old): {skipped:,}")

    completed = load_completed(log_file)
    remaining = [(base_ric, query_ric) for base_ric, query_ric in contracts
                 if base_ric not in completed]

    log(f"  Already completed: {len(completed):,}")
    log(f"  Remaining:         {len(remaining):,}")
    log("")

    if not remaining:
        log("Nothing to do — COMPLETE")
        return

    session, config_path = setup_session(base_dir)
    tm = TokenManager(session)
    rl = AdaptiveRateLimiter(INITIAL_RATE)

    csv_lock     = threading.Lock()
    log_lock     = threading.Lock()
    counter_lock = threading.Lock()
    counters = {"done": 0, "total_ticks": 0, "with_data": 0}

    header_event = threading.Event()
    if os.path.exists(ticks_csv) and os.path.getsize(ticks_csv) > 0:
        header_event.set()

    total      = len(remaining)
    start_time = time.time()
    last_progress_log = time.time()

    with open(progress_log, "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"Run started: {datetime.utcnow().isoformat()}Z\n")
        f.write(f"Ticker: {ticker}, Contracts: {total}, Workers: {num_workers}\n")
        f.write(f"Tick window cutoff: {cutoff}\n")
        f.write(f"{'='*60}\n")

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(
                    worker_task, tm, rl, base_ric, query_ric,
                    ticks_csv, csv_lock, header_event,
                    log_file, log_lock, counters, counter_lock
                ): base_ric
                for base_ric, query_ric in remaining
            }

            for future in as_completed(futures):
                ric, n, earliest, latest, elapsed = future.result()

                with counter_lock:
                    done        = counters["done"]
                    total_ticks = counters["total_ticks"]

                csv_mb     = os.path.getsize(ticks_csv) / 1024 / 1024 if os.path.exists(ticks_csv) else 0
                rpm        = rl.requests_per_minute()
                time_range = f"({earliest[:10]} – {latest[:10]})" if n > 0 else "(no data)"

                print(
                    f"[ticks] {done}/{total}  {ric}  {n:,} ticks  {time_range}  "
                    f"{elapsed:.1f}s  |  total: {total_ticks:,}  CSV: {csv_mb:.0f}MB  "
                    f"rate: {rl.rate:.1f}/s  rpm: {rpm}",
                    flush=True
                )

                now = time.time()
                if now - last_progress_log >= 60:
                    elapsed_total      = now - start_time
                    contracts_per_min  = done / elapsed_total * 60 if elapsed_total > 0 else 0
                    eta_h              = (total - done) / (done / elapsed_total) / 3600 if done > 0 else 0
                    ticks_per_min      = total_ticks / elapsed_total * 60 if elapsed_total > 0 else 0

                    summary = (
                        f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] "
                        f"Progress: {done}/{total} ({100*done/total:.1f}%)  "
                        f"rpm: {rpm}  rate: {rl.rate:.1f}/s  "
                        f"ticks/min: {ticks_per_min:,.0f}  "
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
    csv_mb = os.path.getsize(ticks_csv) / 1024 / 1024 if os.path.exists(ticks_csv) else 0

    summary = (
        f"\n{'='*70}\n"
        f"COMPLETE: {ticker} trade ticks\n"
        f"  Contracts in window: {total:,}\n"
        f"  With data:           {counters['with_data']:,}\n"
        f"  Total ticks:         {counters['total_ticks']:,}\n"
        f"  CSV size:            {csv_mb:.0f} MB\n"
        f"  Elapsed:             {elapsed_total/3600:.2f}h\n"
        f"  Total requests:      {rl.total_requests:,}\n"
        f"  Total 429s:          {rl.total_429s}\n"
        f"  Final rate:          {rl.rate:.2f} req/sec\n"
    )
    log(summary)
    with open(progress_log, "a") as pf:
        pf.write(summary)


if __name__ == "__main__":
    main()
