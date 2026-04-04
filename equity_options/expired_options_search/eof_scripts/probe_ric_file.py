"""
Generic RIC probe script. Probes all RICs in an input CSV against LSEG
intraday-summaries endpoint. Resumes if output file already exists.

Usage: python probe_ric_file.py <input_csv> <output_csv> [WORKERS]

Input CSV must have columns: ric, ticker, exdate, cp, strike
"""
import csv, json, os, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import lseg.data as ld
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views"

if len(sys.argv) < 3:
    print("Usage: python probe_ric_file.py <input_csv> <output_csv> [WORKERS]")
    sys.exit(1)

INPUT   = sys.argv[1]
OUTPUT  = sys.argv[2]
WORKERS = int(sys.argv[3]) if len(sys.argv) > 3 else 8


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
    total_in_file = 0
    with open(INPUT) as f:
        for row in csv.DictReader(f):
            total_in_file += 1
            if row['ric'] not in already_done:
                candidates.append(row)

    print(f"Total RICs:     {total_in_file:,}")
    print(f"Already probed: {len(already_done):,}")
    print(f"Remaining:      {len(candidates):,}")
    print(f"ETA:            ~{len(candidates)/23/3600:.1f} hours at 23 req/s")
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
    fieldnames = ['ric', 'ticker', 'exdate', 'cp', 'strike', 'status', 'has_data']
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if write_header: writer.writeheader()

    lock = threading.Lock()
    t0   = time.monotonic()
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
                if has_data:           counters['found'] += 1
                elif status == 200:    counters['empty'] += 1
                else:                  counters['error'] += 1

                if counters['done'] % 5000 == 0:
                    elapsed = time.monotonic() - t0
                    rate = counters['done'] / elapsed
                    eta  = (len(candidates) - counters['done']) / max(rate, 0.1) / 3600
                    pct  = counters['done'] / len(candidates) * 100
                    print(f"  [{counters['done']:,}/{len(candidates):,}] {pct:.1f}%  "
                          f"{rate:.1f} req/s  found={counters['found']:,}  "
                          f"hit={counters['found']/counters['done']*100:.1f}%  "
                          f"ETA {eta:.1f}h  cap={rl.current_rate():.1f}")

    out_f.close()
    elapsed = time.monotonic() - t0

    print(f"\n=== Done in {elapsed/3600:.1f}h ===")
    print(f"  Found:  {counters['found']:,}  ({counters['found']/counters['done']*100:.1f}%)")
    print(f"  Empty:  {counters['empty']:,}")
    print(f"  Error:  {counters['error']:,}")

    # Summary by ticker
    print("\n=== Hit rate by ticker (top 30 by found) ===")
    by_ticker = defaultdict(lambda: [0, 0])  # [found, total]
    with open(OUTPUT) as f:
        for row in csv.DictReader(f):
            by_ticker[row['ticker']][1] += 1
            if row['has_data'] == 'True':
                by_ticker[row['ticker']][0] += 1
    top = sorted(by_ticker.items(), key=lambda x: -x[1][0])[:30]
    for ticker, (found, total) in top:
        print(f"  {ticker:8s}: {found:,}/{total:,} ({found/total*100:.0f}%)")

    session.close()


if __name__ == '__main__':
    main()
