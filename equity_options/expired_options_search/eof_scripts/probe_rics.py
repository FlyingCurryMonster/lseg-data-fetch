"""
Spot-check constructed RICs against LSEG intraday-summaries endpoint.
Samples 1000 RICs stratified by ticker, source (OM gap vs CBOE), and C/P.
Uses count=1 — minimal request, just confirms the RIC exists and has data.

Usage:
  python probe_rics.py [WORKERS]
"""
import csv
import json
import os
import sys
import time
import threading
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/rakin/wrds-data/LSEG datastream/.env")

BASE_DIR  = "/home/rakin/wrds-data/LSEG datastream/intraday options data"
EXP_DIR   = "/home/rakin/wrds-data/LSEG datastream/expired options search"
HIST_URL  = "https://api.refinitiv.com/data/historical-pricing/v1/views"

INITIAL_RATE = 23.0
RATE_BACKOFF = 0.90
MIN_RATE     = 0.5
MAX_RETRIES  = 3
WORKERS      = int(sys.argv[1]) if len(sys.argv) > 1 else 8
N_SAMPLE     = 1000


# ── Rate limiter ──────────────────────────────────────────────────────────────
class AdaptiveRateLimiter:
    def __init__(self):
        self._lock = threading.Lock()
        self._rate = INITIAL_RATE
        self._tokens = INITIAL_RATE
        self._last = time.monotonic()

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


# ── Token manager ─────────────────────────────────────────────────────────────
class TokenManager:
    def __init__(self, session):
        self._session = session
        self._token = session._access_token
        self._lock = threading.Lock()

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
    """Returns (ric, status_code, has_data, error)."""
    url = f"{HIST_URL}/intraday-summaries/{requests.utils.quote(ric, safe='')}"
    try:
        resp = tm.get(url, params={"count": 1, "interval": "PT1M"}, rate_limiter=rl)
        if resp is None:
            return (ric, None, False, "timeout/connection")
        if resp.status_code == 200:
            data = resp.json()
            rows = data.get("data", [])
            return (ric, 200, len(rows) > 0, None)
        return (ric, resp.status_code, False, None)
    except Exception as e:
        return (ric, None, False, str(e))


def load_rics(filepath, source_label):
    rows = []
    with open(filepath) as f:
        for row in csv.DictReader(f):
            row['source'] = source_label
            rows.append(row)
    return rows


def stratified_sample(all_rics, n):
    """Sample n RICs stratified by ticker + source + cp."""
    groups = defaultdict(list)
    for r in all_rics:
        key = (r['ticker'], r['source'], r['cp'])
        groups[key].append(r)

    # Assign quota proportional to group size
    total = len(all_rics)
    sampled = []
    for key, items in groups.items():
        quota = max(1, round(n * len(items) / total))
        sampled.extend(random.sample(items, min(quota, len(items))))

    # Trim or top up to exactly n
    random.shuffle(sampled)
    sampled = sampled[:n]
    # If under n, fill from remainder
    if len(sampled) < n:
        used = set(id(r) for r in sampled)
        rest = [r for r in all_rics if id(r) not in used]
        random.shuffle(rest)
        sampled += rest[:n - len(sampled)]
    return sampled[:n]


def main():
    # Setup session
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

    # Load both RIC sets
    om_rics   = load_rics(f"{EXP_DIR}/rics_from_om_gap.csv",       "om_gap")
    cboe_rics = load_rics(f"{EXP_DIR}/rics_from_cboe_dec2025.csv", "cboe_dec2025")
    all_rics  = om_rics + cboe_rics
    print(f"Total RICs available: {len(all_rics):,}  (OM gap: {len(om_rics):,}, CBOE: {len(cboe_rics):,})")

    # Stratified sample
    random.seed(42)
    sample = stratified_sample(all_rics, N_SAMPLE)
    print(f"Sampled {len(sample):,} RICs for probing\n")

    # Print sample breakdown
    breakdown = defaultdict(int)
    for r in sample:
        breakdown[(r['ticker'], r['source'], r['cp'])] += 1
    print("Sample breakdown (ticker, source, cp) → count:")
    for k in sorted(breakdown):
        print(f"  {k[0]:6s} {k[1]:14s} {k[2]}  →  {breakdown[k]}")
    print()

    # Probe
    results = []
    lock = threading.Lock()
    done = [0]
    t0 = time.monotonic()

    def _probe(row):
        result = probe_ric(tm, rl, row['ric'])
        with lock:
            done[0] += 1
            if done[0] % 100 == 0:
                elapsed = time.monotonic() - t0
                rate = done[0] / elapsed
                print(f"  [{done[0]}/{N_SAMPLE}]  {rate:.1f} req/s  elapsed {elapsed:.0f}s")
        return result, row

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_probe, row): row for row in sample}
        for fut in as_completed(futures):
            (ric, status, has_data, err), row = fut.result()
            results.append({
                'ric':      ric,
                'ticker':   row['ticker'],
                'exdate':   row['exdate'],
                'cp':       row['cp'],
                'strike':   row['strike'],
                'source':   row['source'],
                'status':   status,
                'has_data': has_data,
                'error':    err or '',
            })

    # Save results
    out_path = f"{EXP_DIR}/probe_results.csv"
    with open(out_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)

    # Summary
    elapsed = time.monotonic() - t0
    print(f"\n=== Results ({elapsed:.0f}s) ===")
    by_status = defaultdict(int)
    for r in results:
        by_status[r['status']] += 1
    for s in sorted(by_status, key=lambda x: (x is None, x)):
        print(f"  HTTP {s}: {by_status[s]:,}")

    hit  = sum(1 for r in results if r['has_data'])
    miss = sum(1 for r in results if r['status'] == 200 and not r['has_data'])
    err  = sum(1 for r in results if r['status'] != 200)
    print(f"\n  Has data:       {hit:,}  ({hit/len(results)*100:.1f}%)")
    print(f"  200 but empty:  {miss:,}")
    print(f"  Non-200:        {err:,}")

    # Breakdown by source
    print("\n=== By source ===")
    for src in ('om_gap', 'cboe_dec2025'):
        sub = [r for r in results if r['source'] == src]
        h = sum(1 for r in sub if r['has_data'])
        print(f"  {src}: {h}/{len(sub)} have data ({h/len(sub)*100:.1f}%)")

    # Breakdown by ticker
    print("\n=== By ticker ===")
    tickers = sorted(set(r['ticker'] for r in results))
    for t in tickers:
        sub = [r for r in results if r['ticker'] == t]
        h = sum(1 for r in sub if r['has_data'])
        print(f"  {t:6s}: {h}/{len(sub)} ({h/len(sub)*100:.1f}%)")

    print(f"\nFull results saved to {out_path}")
    session.close()


if __name__ == '__main__':
    main()
