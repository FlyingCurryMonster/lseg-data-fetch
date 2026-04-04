"""
Probe alternative expiry dates around NVDA Oct 24 and Oct 31 gaps.
Tests Wed/Thu/Mon around each missing Friday.

Usage: python probe_nvda_alt_dates.py [WORKERS]
"""
import csv, json, os, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
EXP_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"
WORKERS  = int(sys.argv[1]) if len(sys.argv) > 1 else 8

class AdaptiveRateLimiter:
    def __init__(self):
        self._lock = threading.Lock()
        self._rate = 23.0
        self._tokens = 23.0
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
            self._rate = max(0.5, self._rate * 0.90)

class TokenManager:
    def __init__(self, session):
        self._session = session
        self._token = session._access_token
        self._lock = threading.Lock()
    def _headers(self):
        return {"Authorization": f"Bearer {self._token}"}
    def _refresh(self):
        with self._lock:
            self._token = self._session._access_token
    def get(self, url, params=None, rate_limiter=None):
        for attempt in range(3):
            if rate_limiter: rate_limiter.acquire()
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=15)
                if resp.status_code == 401:
                    self._refresh()
                    resp = requests.get(url, headers=self._headers(), params=params, timeout=15)
                if resp.status_code == 429:
                    if rate_limiter: rate_limiter.on_429()
                    time.sleep(int(resp.headers.get("Retry-After", 5)))
                    continue
                return resp
            except: time.sleep(min(60, 2**attempt*5))
        return None

def probe(tm, rl, ric):
    url = f"{HIST_URL}/intraday-summaries/{requests.utils.quote(ric, safe='')}"
    resp = tm.get(url, params={"count": 1, "interval": "PT1M"}, rate_limiter=rl)
    if resp is None: return None, False
    if resp.status_code == 200:
        body = resp.json()
        rows = body if isinstance(body, list) else body.get("data", [])
        return 200, len(rows) > 0
    return resp.status_code, False

def main():
    config = {"sessions": {"default": "platform.rdp", "platform": {"rdp": {
        "app-key": os.getenv("DSWS_APPKEY"), "username": os.getenv("DSWS_USERNAME"),
        "password": os.getenv("DSWS_PASSWORD"), "signon_control": True,
    }}}}
    config_path = os.path.join(BASE_DIR, "lseg-data.config.json")
    with open(config_path, "w") as f: json.dump(config, f)
    session = ld.open_session(config_name=config_path)
    tm = TokenManager(session)
    rl = AdaptiveRateLimiter()

    candidates = list(csv.DictReader(open(f"{EXP_DIR}/rics_nvda_alt_dates.csv")))
    print(f"Probing {len(candidates):,} candidates...")

    results = []
    lock = threading.Lock()
    done = [0]

    def _probe(row):
        status, has_data = probe(tm, rl, row['ric'])
        with lock:
            done[0] += 1
        return row, status, has_data

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for fut in as_completed(pool.submit(_probe, r) for r in candidates):
            row, status, has_data = fut.result()
            results.append((row, status, has_data))

    session.close()

    # Summary by date
    from collections import defaultdict
    by_date = defaultdict(lambda: {'found': 0, 'total': 0})
    for row, status, has_data in results:
        by_date[row['exdate']]['total'] += 1
        if has_data:
            by_date[row['exdate']]['found'] += 1

    print("\nNVDA results by date:")
    for d in sorted(by_date):
        r = by_date[d]
        print(f"  {d}: {r['found']}/{r['total']} contracts found")

if __name__ == '__main__':
    main()
