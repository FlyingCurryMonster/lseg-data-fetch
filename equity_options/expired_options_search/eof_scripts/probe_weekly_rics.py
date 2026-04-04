"""
Probe all brute-force weekly/daily RIC candidates against LSEG.
Runs through all 26K candidates in rics_brute_force_gap.csv and records
which ones return data. Results saved to probe_weekly_results.csv.

Usage:
  python probe_weekly_rics.py [WORKERS]
"""
import csv
import json
import os
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/rakin/wrds-data/LSEG datastream/.env")

BASE_DIR = "/home/rakin/wrds-data/LSEG datastream/intraday options data"
EXP_DIR  = "/home/rakin/wrds-data/LSEG datastream/expired options search"
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"

INITIAL_RATE = 23.0
RATE_BACKOFF = 0.90
MIN_RATE     = 0.5
MAX_RETRIES  = 3
WORKERS      = int(sys.argv[1]) if len(sys.argv) > 1 else 8


# ── Rate limiter ──────────────────────────────────────────────────────────────
class AdaptiveRateLimiter:
    def __init__(self):
        self._lock   = threading.Lock()
        self._rate   = INITIAL_RATE
        self._tokens = INITIAL_RATE
        self._last   = time.monotonic()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._rate, self._tokens + (now - self._last) * self._rate)
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


# ── Token manager ─────────────────────────────────────────────────────────────
class TokenManager:
    def __init__(self, session):
        self._session = session
        self._token   = session._access_token
        self._lock    = threading.Lock()

    def _headers(self):
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    def _refresh(self):
        with self._lock:
            self._token = self._session._access_token

    def get(self, url, params=None, rate_limiter=None):
        for attempt in range(MAX_RETRIES):
            if rate_limiter:
                rate_limiter.acquire()
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=15)
                if resp.status_code == 401:
                    self._refresh()
                    resp = requests.get(url, headers=self._headers(), params=params, timeout=15)
                if resp.status_code == 429:
                    if rate_limiter:
                        rate_limiter.on_429()
                    time.sleep(int(resp.headers.get("Retry-After", 5)))
                    continue
                return resp
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                time.sleep(min(60, 2 ** attempt * 5))
        return None


def probe_ric(tm, rl, ric):
    """Returns (status_code, has_data, error)."""
    url = f"{HIST_URL}/intraday-summaries/{requests.utils.quote(ric, safe='')}"
    try:
        resp = tm.get(url, params={"count": 1, "interval": "PT1M"}, rate_limiter=rl)
        if resp is None:
            return (None, False, "timeout")
        if resp.status_code == 200:
            body = resp.json()
            # Response can be a dict {"data": [...]} or a list directly
            if isinstance(body, list):
                rows = body
            else:
                rows = body.get("data", [])
            return (200, len(rows) > 0, None)
        return (resp.status_code, False, None)
    except Exception as e:
        return (None, False, str(e))


def main():
    input_file  = f"{EXP_DIR}/rics_brute_force_gap.csv"
    output_file = f"{EXP_DIR}/probe_weekly_results.csv"

    # Load candidates, skip already-probed if output exists
    already_done = set()
    if os.path.exists(output_file):
        with open(output_file) as f:
            for row in csv.DictReader(f):
                already_done.add(row['ric'])

    candidates = []
    with open(input_file) as f:
        for row in csv.DictReader(f):
            if row['ric'] not in already_done:
                candidates.append(row)

    total_in_file = sum(1 for _ in open(input_file)) - 1
    print(f"Total candidates: {total_in_file:,}")
    print(f"Already probed:   {len(already_done):,}")
    print(f"Remaining:        {len(candidates):,}")
    if not candidates:
        print("All done.")
        return

    # Setup LSEG session
    config = {
        "sessions": {
            "default": "platform.rdp",
            "platform": {"rdp": {
                "app-key":  os.getenv("DSWS_APPKEY"),
                "username": os.getenv("DSWS_USERNAME"),
                "password": os.getenv("DSWS_PASSWORD"),
                "signon_control": True,
            }},
        }
    }
    config_path = os.path.join(BASE_DIR, "lseg-data.config.json")
    with open(config_path, "w") as f:
        json.dump(config, f)
    session = ld.open_session(config_name=config_path)
    tm = TokenManager(session)
    rl = AdaptiveRateLimiter()

    # Open output in append mode
    write_header = not os.path.exists(output_file) or len(already_done) == 0
    out_f = open(output_file, 'a', newline='')
    fieldnames = ['ric', 'ticker', 'exdate', 'cp', 'strike', 'status', 'has_data', 'error']
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    lock     = threading.Lock()
    t0       = time.monotonic()
    counters = {'done': 0, 'has_data': 0, 'no_data': 0, 'error': 0}

    def _probe(row):
        status, has_data, err = probe_ric(tm, rl, row['ric'])
        return row, status, has_data, err

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_probe, row): row for row in candidates}
        for fut in as_completed(futures):
            row, status, has_data, err = fut.result()
            with lock:
                writer.writerow({
                    'ric':      row['ric'],
                    'ticker':   row['ticker'],
                    'exdate':   row['exdate'],
                    'cp':       row['cp'],
                    'strike':   row['strike'],
                    'status':   status,
                    'has_data': has_data,
                    'error':    err or '',
                })
                out_f.flush()
                counters['done'] += 1
                if has_data:
                    counters['has_data'] += 1
                elif status == 200:
                    counters['no_data'] += 1
                else:
                    counters['error'] += 1

                if counters['done'] % 500 == 0:
                    elapsed = time.monotonic() - t0
                    rate = counters['done'] / elapsed
                    remaining = len(candidates) - counters['done']
                    eta = remaining / rate / 60 if rate > 0 else 0
                    print(f"  [{counters['done']:,}/{len(candidates):,}]  "
                          f"{rate:.1f} req/s  "
                          f"found={counters['has_data']:,}  "
                          f"ETA {eta:.0f}m  "
                          f"rate_limit={rl.current_rate():.1f}")

    out_f.close()
    elapsed = time.monotonic() - t0

    print(f"\n=== Done in {elapsed/60:.1f} min ===")
    print(f"  Has data:  {counters['has_data']:,}")
    print(f"  200/empty: {counters['no_data']:,}")
    print(f"  Non-200:   {counters['error']:,}")

    # Breakdown of found RICs by ticker/exdate
    print("\n=== Found RICs by ticker and expiry ===")
    found = defaultdict(lambda: defaultdict(int))
    with open(output_file) as f:
        for row in csv.DictReader(f):
            if row['has_data'] == 'True':
                found[row['ticker']][row['exdate']] += 1
    for ticker in sorted(found):
        print(f"\n  {ticker}:")
        for exp in sorted(found[ticker]):
            print(f"    {exp}: {found[ticker][exp]:,} contracts")

    session.close()


if __name__ == '__main__':
    main()
