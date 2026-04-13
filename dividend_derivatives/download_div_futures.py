"""
Download daily pricing data for all futures in the instrument master.

Usage:
  python download_div_futures.py [--master PATH] [--start DATE] [--workers N]

Reads: instrument_master_futures.csv (or --master)
Output:
  futures_daily_prices.csv   — daily OHLCV (appended incrementally)
  futures_download_log.jsonl — per-RIC resume log
  futures_download.log       — human-readable progress log

Uses direct REST + TokenManager for reliable token refresh during long runs.
Parallel workers for throughput. Resumes from futures_download_log.jsonl —
safe to kill and restart.
"""

import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.token_manager import TokenManager

HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "futures_daily_prices.csv")
LOG_JSONL = os.path.join(SCRIPT_DIR, "futures_download_log.jsonl")
PROGRESS_LOG = os.path.join(SCRIPT_DIR, "futures_download.log")

# Thread-safe locks for file writes
csv_lock = threading.Lock()
jsonl_lock = threading.Lock()
progress_lock = threading.Lock()
counter_lock = threading.Lock()


def log_progress(msg):
    """Print and append to progress log file."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with progress_lock:
        with open(PROGRESS_LOG, "a") as f:
            f.write(line + "\n")


def load_completed_rics():
    """Load set of RICs already downloaded from JSONL log."""
    completed = set()
    if os.path.exists(LOG_JSONL):
        with open(LOG_JSONL) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if entry.get("status") in ("ok", "empty"):
                        completed.add(entry["ric"])
                except (json.JSONDecodeError, KeyError):
                    pass
    return completed


def log_ric_result(ric, status, rows=0, date_min=None, date_max=None):
    """Append a per-RIC result to the JSONL log."""
    entry = {
        "ric": ric,
        "status": status,
        "rows": rows,
        "ts": datetime.now().isoformat(),
    }
    if date_min:
        entry["date_min"] = date_min
    if date_max:
        entry["date_max"] = date_max
    with jsonl_lock:
        with open(LOG_JSONL, "a") as f:
            f.write(json.dumps(entry) + "\n")


def fetch_history(tm, ric, start, end, max_retries=3):
    """Fetch daily history for one RIC via REST. Returns list of dicts or None."""
    url = f"{HIST_URL}/{ric}"
    params = {"interval": "P1D", "start": start, "end": end}

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=tm.headers(), params=params)
            if resp.status_code == 401:
                tm.on_401()
                continue
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                log_progress(f"  [429] Rate limited on {ric}, waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()

            data = resp.json()
            if not data or not isinstance(data, list):
                return None

            item = data[0]
            headers = [h.get("name") for h in item.get("headers", [])]
            rows = item.get("data", [])
            if not headers or not rows:
                return None

            return [dict(zip(headers, row)) for row in rows]

        except requests.exceptions.RequestException as e:
            log_progress(f"  [error] {ric} attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)

    return None


def append_rows_to_csv(rows_dicts, ric, csv_path):
    """Append rows for one RIC to the output CSV (thread-safe)."""
    df = pd.DataFrame(rows_dicts)
    df["RIC"] = ric
    if "DATE" in df.columns:
        df = df.rename(columns={"DATE": "date"})

    with csv_lock:
        write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
        df.to_csv(csv_path, mode="a", header=write_header, index=False)
    return df


def process_ric(tm, ric, start, end, counters):
    """Download one RIC and log the result. Called from thread pool."""
    try:
        rows = fetch_history(tm, ric, start, end)

        if rows:
            df = append_rows_to_csv(rows, ric, OUTPUT_CSV)
            date_min = df["date"].min() if "date" in df.columns else None
            date_max = df["date"].max() if "date" in df.columns else None
            log_ric_result(ric, "ok", rows=len(rows), date_min=str(date_min), date_max=str(date_max))
            with counter_lock:
                counters["rows"] += len(rows)
                counters["done"] += 1
        else:
            log_ric_result(ric, "empty")
            with counter_lock:
                counters["empty"] += 1
                counters["done"] += 1

    except Exception as e:
        log_ric_result(ric, "error")
        with counter_lock:
            counters["errors"] += 1
            counters["done"] += 1
        log_progress(f"  {ric}: ERROR — {e}")


def main():
    parser = argparse.ArgumentParser(description="Download daily prices for dividend futures")
    parser.add_argument("--master", default=os.path.join(SCRIPT_DIR, "instrument_master_futures.csv"),
                        help="Path to instrument master CSV")
    parser.add_argument("--start", default="2005-01-01", help="Start date (default: 2005-01-01)")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers (default: 10)")
    args = parser.parse_args()

    master = pd.read_csv(args.master)
    all_rics = master["RIC"].tolist()

    log_progress("=" * 80)
    log_progress(f"Downloading daily prices for {len(all_rics)} futures")
    log_progress(f"Master: {args.master}")
    log_progress(f"Start: {args.start}, Workers: {args.workers}")
    log_progress("=" * 80)

    if "ProductGroup" in master.columns:
        for group, grp in master.groupby("ProductGroup"):
            log_progress(f"  {group}: {len(grp)} contracts across {grp['product'].nunique()} products")
    else:
        for product, count in master.groupby("product").size().items():
            log_progress(f"  {product}: {count}")

    # Resume
    completed = load_completed_rics()
    remaining = [r for r in all_rics if r not in completed]
    if completed:
        log_progress(f"\nResuming — {len(completed)} RICs already done, {len(remaining)} remaining")
    else:
        log_progress(f"\nStarting fresh — {len(remaining)} RICs to download")

    if not remaining:
        log_progress("Nothing to do!")
        return

    # Authenticate
    tm = TokenManager()

    end = datetime.now().strftime("%Y-%m-%d")
    counters = {"rows": 0, "empty": 0, "errors": 0, "done": 0}
    total = len(remaining)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_ric, tm, ric, args.start, end, counters): ric
                   for ric in remaining}

        for future in as_completed(futures):
            future.result()  # propagate exceptions
            done = counters["done"]
            if done % 200 == 0 or done <= 5:
                elapsed = time.time() - start_time
                rate = done / elapsed if elapsed > 0 else 0
                eta_min = (total - done) / rate / 60 if rate > 0 else 0
                log_progress(f"  Progress: {done}/{total} "
                             f"({counters['rows']} rows, {counters['empty']} empty, "
                             f"{counters['errors']} errors) "
                             f"— {rate:.1f} RICs/sec, ETA {eta_min:.0f}min")

    elapsed = time.time() - start_time
    log_progress("\n" + "=" * 80)
    log_progress("DOWNLOAD COMPLETE")
    log_progress("=" * 80)
    log_progress(f"Total RICs processed: {total}")
    log_progress(f"Total rows downloaded: {counters['rows']}")
    log_progress(f"Empty (no data): {counters['empty']}")
    log_progress(f"Errors: {counters['errors']}")
    log_progress(f"Elapsed: {elapsed / 60:.1f} minutes ({elapsed / 3600:.1f} hours)")
    if os.path.exists(OUTPUT_CSV):
        log_progress(f"Output: {OUTPUT_CSV}")
    log_progress(f"Log: {LOG_JSONL}")


if __name__ == "__main__":
    main()
