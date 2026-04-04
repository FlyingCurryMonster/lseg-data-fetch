"""
Probe all 111K RICs in master_gap_rics_all.csv against LSEG.
Resumes from probe_master_gap_results.csv if interrupted.

Usage: python probe_master_gap_rics.py [WORKERS]
"""
import csv, json, os, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
EXP_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HIST_URL  = "https://api.refinitiv.com/data/historical-pricing/v1/views"
WORKERS   = int(sys.argv[1]) if len(sys.argv) > 1 else 8
INPUT     = f"{EXP_DIR}/master_gap_rics_all.csv"
OUTPUT    = f"{EXP_DIR}/probe_master_gap_results.csv"


class AdaptiveRateLimiter:
    def __init__(self):
        self._lock = threading.Lock()
        self._rate = 23.0; self._tokens = 23.0; self._last = time.monotonic()
    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._rate, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1.0: self._tokens -= 1.0; return
            time.sleep(0.005)
    def on_429(self):
        with self._lock: self._rate = max(0.5, self._rate * 0.90)
    def current_rate(self):
        with self._lock: return self._rate


class TokenManager:
    def __init__(self, session):
        self._session = session; self._token = session._access_token; self._lock = threading.Lock()
    def _headers(self): return {"Authorization": f"Bearer {self._token}"}
    def _refresh(self):
        with self._lock: self._token = self._session._access_token
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
                    time.sleep(int(resp.headers.get("Retry-After", 5))); continue
                return resp
            except: time.sleep(min(60, 2 ** attempt * 5))
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
    # Resume: skip already-probed RICs
    already_done = set()
    if os.path.exists(OUTPUT):
        with open(OUTPUT) as f:
            for row in csv.DictReader(f):
                already_done.add(row['ric'])

    candidates = []
    with open(INPUT) as f:
        for row in csv.DictReader(f):
            if row['ric'] not in already_done:
                candidates.append(row)

    total_in_file = sum(1 for _ in open(INPUT)) - 1
    print(f"Total RICs:     {total_in_file:,}")
    print(f"Already probed: {len(already_done):,}")
    print(f"Remaining:      {len(candidates):,}")
    print(f"ETA:            ~{len(candidates)/23/60:.0f} min at 23 req/s")
    if not candidates:
        print("All done.")
        return

    # Setup session
    config = {"sessions": {"default": "platform.rdp", "platform": {"rdp": {
        "app-key":  os.getenv("DSWS_APPKEY"),
        "username": os.getenv("DSWS_USERNAME"),
        "password": os.getenv("DSWS_PASSWORD"),
        "signon_control": True,
    }}}}
    config_path = os.path.join(BASE_DIR, "lseg-data.config.json")
    with open(config_path, "w") as f: json.dump(config, f)
    session = ld.open_session(config_name=config_path)
    tm = TokenManager(session)
    rl = AdaptiveRateLimiter()

    write_header = not os.path.exists(OUTPUT) or len(already_done) == 0
    out_f = open(OUTPUT, 'a', newline='')
    fieldnames = ['ric','ticker','exdate','cp','strike','status','has_data']
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if write_header: writer.writeheader()

    lock = threading.Lock()
    t0 = time.monotonic()
    counters = {'done': 0, 'found': 0, 'empty': 0, 'error': 0}

    def _probe(row):
        status, has_data = probe(tm, rl, row['ric'])
        return row, status, has_data

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for fut in as_completed(pool.submit(_probe, r) for r in candidates):
            row, status, has_data = fut.result()
            with lock:
                writer.writerow({'ric': row['ric'], 'ticker': row['ticker'],
                                 'exdate': row['exdate'], 'cp': row['cp'],
                                 'strike': row['strike'], 'status': status,
                                 'has_data': has_data})
                out_f.flush()
                counters['done'] += 1
                if has_data: counters['found'] += 1
                elif status == 200: counters['empty'] += 1
                else: counters['error'] += 1

                if counters['done'] % 1000 == 0:
                    elapsed = time.monotonic() - t0
                    rate = counters['done'] / elapsed
                    eta = (len(candidates) - counters['done']) / max(rate, 0.1) / 60
                    print(f"  [{counters['done']:,}/{len(candidates):,}]  "
                          f"{rate:.1f} req/s  found={counters['found']:,}  "
                          f"empty={counters['empty']:,}  error={counters['error']:,}  "
                          f"ETA {eta:.0f}m  rate_cap={rl.current_rate():.1f}")

    out_f.close()
    elapsed = time.monotonic() - t0
    print(f"\n=== Done in {elapsed/60:.1f} min ===")
    print(f"  Found:  {counters['found']:,}")
    print(f"  Empty:  {counters['empty']:,}")
    print(f"  Error:  {counters['error']:,}")

    # Summary by ticker
    print("\n=== Found RICs by ticker ===")
    by_ticker = defaultdict(lambda: defaultdict(int))
    with open(OUTPUT) as f:
        for row in csv.DictReader(f):
            if row['has_data'] == 'True':
                by_ticker[row['ticker']][row['exdate']] += 1
    for ticker in sorted(by_ticker):
        total = sum(by_ticker[ticker].values())
        print(f"  {ticker:6s}: {len(by_ticker[ticker])} expiries, {total:,} contracts")

    session.close()


if __name__ == '__main__':
    main()
