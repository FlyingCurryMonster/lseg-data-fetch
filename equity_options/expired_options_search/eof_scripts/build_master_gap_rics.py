"""
Build the master RIC list for gap expirations (Aug 29 → Dec 4, 2025).

Strategy:
  - Expiry dates: taken from brute-force probe results (confirmed to have data)
  - Strike set:   taken from CBOE Dec 5 snapshot (nearest weekly for the series)

For each confirmed gap expiry, we use the strike set from the nearest
December weekly expiry of the same series type as the definitive strike list.

Output: master_gap_rics_{TICKER}.csv  per ticker
        master_gap_rics_all.csv       combined
"""
import csv, re, sys, os
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from build_rics import to_ric

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

ALL_TICKERS = ['NVDA','AMD','TSLA','SPY','SPX','SPXW',
               'AAPL','MSFT','AMZN','GOOGL','META','JPM','LLY','AVGO','COST','XOM']

# ── Load CBOE Dec 5 strike sets ──────────────────────────────────────────────
# For each ticker, find the nearest weekly (Friday on or after Dec 5)
# and use its (cp, strike) set as the template.

osi_re = re.compile(r'^(\w+)\s+(\d{6})([CP])(\d{8})$')
cboe_by_ticker_exp = defaultdict(lambda: defaultdict(set))  # ticker → exdate → {(cp,strike)}

with open(f"{BASE}/cboe_all_series_20251205.csv") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) < 2: continue
        m = osi_re.match(row[1].strip())
        if not m: continue
        ticker, exp6, cp, strike_raw = m.groups()
        if ticker not in ALL_TICKERS: continue
        exdate = f"20{exp6[:2]}-{exp6[2:4]}-{exp6[4:]}"
        strike = int(strike_raw) / 1000.0
        cboe_by_ticker_exp[ticker][exdate].add((cp, strike))

def get_cboe_strike_template(ticker, series_type='weekly'):
    """Get strike set from the nearest Dec weekly in the CBOE snapshot."""
    # Find Fridays in Dec 2025 that are in the snapshot
    exps = sorted(cboe_by_ticker_exp[ticker].keys())
    # Prefer Dec 5 (closest to gap end), then Dec 12
    for preferred in ['2025-12-05', '2025-12-12', '2025-12-19']:
        if preferred in cboe_by_ticker_exp[ticker]:
            return cboe_by_ticker_exp[ticker][preferred], preferred
    # Fallback: first available
    if exps:
        return cboe_by_ticker_exp[ticker][exps[0]], exps[0]
    return set(), None


# ── Load confirmed gap expiries from probe results ───────────────────────────
confirmed_exps = defaultdict(set)  # ticker → set of confirmed expiry date strings

probe_files = [
    f"{BASE}/probe_weekly_results.csv",
    f"{BASE}/probe_top10_results.csv",
]
for pf in probe_files:
    if not os.path.exists(pf):
        continue
    with open(pf) as f:
        for row in csv.DictReader(f):
            if row['has_data'] == 'True':
                confirmed_exps[row['ticker']].add(row['exdate'])

# Also include confirmed expirations from OM gap (already verified in OM)
with open(f"{BASE}/om_gap_contracts_aug2025.csv") as f:
    for row in csv.DictReader(f):
        if row['ticker'] in ALL_TICKERS:
            confirmed_exps[row['ticker']].add(row['exdate'])

# ── Build master RIC list ────────────────────────────────────────────────────
all_rows = []

print(f"{'Ticker':8s} {'Template exp':14s} {'Strikes':>8s} {'Gap expiries':>12s} {'Total RICs':>10s}")
print("-" * 60)

for ticker in ALL_TICKERS:
    # Gap expiries = confirmed expiries that fall before Dec 5
    gap_exps = sorted(e for e in confirmed_exps[ticker] if e < '2025-12-05')
    if not gap_exps:
        continue

    cp_strikes, template_exp = get_cboe_strike_template(ticker)
    if not cp_strikes:
        print(f"{ticker:8s}: no CBOE strike template found")
        continue

    rows = []
    for exdate in gap_exps:
        for cp, strike in cp_strikes:
            ric = to_ric(ticker, exdate, cp, strike)
            rows.append({'ric': ric, 'ticker': ticker, 'exdate': exdate,
                         'cp': cp, 'strike': strike})

    all_rows.extend(rows)
    print(f"{ticker:8s} {template_exp:14s} {len(cp_strikes):>8,} {len(gap_exps):>12,} {len(rows):>10,}")

    # Save per-ticker file
    out = f"{BASE}/master_gap_rics_{ticker}.csv"
    with open(out, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike'])
        w.writeheader()
        w.writerows(rows)

# Save combined file
out_all = f"{BASE}/master_gap_rics_all.csv"
with open(out_all, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike'])
    w.writeheader()
    w.writerows(all_rows)

print(f"\nTotal RICs across all tickers: {len(all_rows):,}")
print(f"Saved to {out_all}")
