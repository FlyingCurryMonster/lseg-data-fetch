"""
Build gap RICs (Oct 18 → Dec 4, 2025) for all ~700 weekly/daily names
in the CBOE Dec 5 snapshot.

Strategy:
  - Weekly names: 6 missing Fridays × CBOE Dec 5 strike set
  - Daily names:  all weekdays × CBOE Dec 5 strike set
  - Monthly-only: skip (Nov 21 monthly already in OM)

Outputs:
  all_names_gap_rics.csv  — full candidate list
  all_names_gap_summary.csv — per-ticker stats
"""
import csv, re, os
from collections import defaultdict
from datetime import date, timedelta

import sys
sys.path.insert(0, os.path.dirname(__file__))
from build_rics import to_ric

BASE = "/home/rakin/wrds-data/LSEG datastream/expired options search"

GAP_START     = date(2025, 10, 18)
GAP_END       = date(2025, 12, 4)
FULL_HOLIDAYS = {date(2025, 11, 27)}

# Already fully handled names — skip to avoid duplicates
ALREADY_DONE = {'NVDA','AMD','TSLA','SPY','SPX','SPXW',
                'AAPL','MSFT','AMZN','GOOGL','META','JPM',
                'LLY','AVGO','COST','XOM'}


def all_fridays(start, end):
    d = start
    while d.weekday() != 4: d += timedelta(1)
    result = []
    while d <= end:
        if d not in FULL_HOLIDAYS: result.append(d)
        d += timedelta(7)
    return result


def all_weekdays(start, end):
    result = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in FULL_HOLIDAYS:
            result.append(d)
        d += timedelta(1)
    return result


def classify(near_exps):
    """Classify series type from near-term expirations."""
    if len(near_exps) < 2:
        return 'monthly'
    dates = [date.fromisoformat(e) for e in sorted(near_exps)]
    gaps = [(dates[i+1]-dates[i]).days for i in range(len(dates)-1)]
    if any(g == 1 for g in gaps): return 'daily'
    if any(g <= 7 for g in gaps): return 'weekly'
    return 'monthly'


# ── Load CBOE snapshot ────────────────────────────────────────────────────────
print("Loading CBOE Dec 5 snapshot...")
osi_re = re.compile(r'^(\w+)\s+(\d{6})([CP])(\d{8})$')
ticker_strikes  = defaultdict(set)   # ticker → {(cp, strike)} from nearest weekly
ticker_near_exp = defaultdict(set)   # ticker → near-term expirations for classification

with open(f"{BASE}/cboe_all_series_20251205.csv") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) < 2: continue
        m = osi_re.match(row[1].strip())
        if not m: continue
        ticker, exp6, cp, strike_raw = m.groups()
        exdate = f"20{exp6[:2]}-{exp6[2:4]}-{exp6[4:]}"
        if '2025-12-04' <= exdate <= '2025-12-19':
            ticker_near_exp[ticker].add(exdate)
        # Use Dec 5 or Dec 12 as strike template (nearest to gap end)
        if exdate in ('2025-12-05', '2025-12-08'):  # Dec 5 for weeklies, Dec 8 for dailies
            ticker_strikes[ticker].add((cp, int(strike_raw)/1000.0))

# Fallback: if no Dec 5, use Dec 12
for ticker in list(ticker_near_exp.keys()):
    if not ticker_strikes[ticker]:
        for row_exdate in ('2025-12-12', '2025-12-09', '2025-12-10', '2025-12-11',
                           '2025-12-15', '2025-12-16', '2025-12-17', '2025-12-18', '2025-12-19'):
            # Re-scan — we need a second pass for fallback tickers
            pass

print(f"Tickers with near-term expirations: {len(ticker_near_exp):,}")

# Second pass for tickers missing Dec 5 template
missing_template = {t for t in ticker_near_exp if not ticker_strikes[t]}
if missing_template:
    print(f"Running second pass for {len(missing_template):,} tickers missing Dec 5 template...")
    with open(f"{BASE}/cboe_all_series_20251205.csv") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) < 2: continue
            m = osi_re.match(row[1].strip())
            if not m: continue
            ticker, exp6, cp, strike_raw = m.groups()
            if ticker not in missing_template: continue
            exdate = f"20{exp6[:2]}-{exp6[2:4]}-{exp6[4:]}"
            if exdate <= '2025-12-19':
                ticker_strikes[ticker].add((cp, int(strike_raw)/1000.0))

# ── Classify and generate candidates ─────────────────────────────────────────
gap_fridays  = all_fridays(GAP_START, GAP_END)
gap_weekdays = all_weekdays(GAP_START, GAP_END)

series_counts  = defaultdict(int)
all_candidates = []
summary_rows   = []

for ticker in sorted(ticker_near_exp.keys()):
    if ticker in ALREADY_DONE:
        continue
    if not ticker_strikes[ticker]:
        continue

    series = classify(ticker_near_exp[ticker])
    series_counts[series] += 1

    if series == 'monthly':
        # Nov 21 covered by OM — skip
        summary_rows.append({'ticker': ticker, 'series': series,
                             'gap_expiries': 0, 'candidates': 0})
        continue

    exp_dates = gap_weekdays if series == 'daily' else gap_fridays

    rows = []
    for d in exp_dates:
        for cp, strike in ticker_strikes[ticker]:
            rows.append({'ric':    to_ric(ticker, d, cp, strike),
                         'ticker': ticker,
                         'exdate': str(d),
                         'cp':     cp,
                         'strike': strike})

    all_candidates.extend(rows)
    summary_rows.append({'ticker': ticker, 'series': series,
                         'gap_expiries': len(exp_dates),
                         'candidates': len(rows)})

# ── Save ──────────────────────────────────────────────────────────────────────
out_rics = f"{BASE}/all_names_gap_rics.csv"
with open(out_rics, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike'])
    w.writeheader()
    w.writerows(all_candidates)

out_summary = f"{BASE}/all_names_gap_summary.csv"
with open(out_summary, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['ticker','series','gap_expiries','candidates'])
    w.writeheader()
    w.writerows(summary_rows)

print(f"\nSeries breakdown:")
for s, n in sorted(series_counts.items()): print(f"  {s}: {n:,} tickers")
print(f"\nTotal candidates: {len(all_candidates):,}")
print(f"At 23 req/sec with 8 workers: ~{len(all_candidates)/23/3600:.1f} hours to probe all")
print(f"\nSaved to {out_rics}")
print(f"Summary saved to {out_summary}")
