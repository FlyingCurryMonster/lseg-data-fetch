"""
Build brute-force gap RICs for top 10 S&P 500 names (AAPL, MSFT, AMZN, GOOGL,
META, JPM, LLY, AVGO, COST, XOM). Uses OM strike templates from Oct 3/10/17
and fills missing Fridays Oct 18 → Dec 4, 2025.

Reads OM strike data from tool-results file saved by ClickHouse query.
Outputs rics_top10_gap.csv.
"""
import json, csv, re, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from build_rics import to_ric
from datetime import date, timedelta
from collections import defaultdict

BASE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
OM_QUERY = "/home/rakin/.claude/projects/-home-rakin-wrds-data/82b6c285-05cf-4688-8dca-c5d5dbb8996b/tool-results/mcp-clickhouse-option-metrics-run_select_query-1774224755418.txt"

TICKERS      = ['AAPL','MSFT','AMZN','GOOGL','META','JPM','LLY','AVGO','COST','XOM']
GAP_START    = date(2025, 10, 18)
GAP_END      = date(2025, 12, 4)
FULL_HOLIDAYS = {date(2025, 11, 27)}


def all_fridays(start, end):
    d = start
    while d.weekday() != 4:
        d += timedelta(1)
    result = []
    while d <= end:
        if d not in FULL_HOLIDAYS:
            result.append(d)
        d += timedelta(7)
    return result


# Load OM strike templates (union of Oct 3, 10, 17)
with open(OM_QUERY) as f:
    data = json.load(f)

strikes = defaultdict(set)
for ticker, exdate, cp, strike in data['rows']:
    strikes[ticker].add((cp, float(strike)))

print("Strike template sizes (union of Oct 3/10/17):")
for t in sorted(strikes):
    print(f"  {t}: {len(strikes[t])} (cp,strike) pairs")

# Load already-known expirations from OM gap + CBOE snapshot
known = defaultdict(set)
with open(f"{BASE}/om_gap_contracts_aug2025.csv") as f:
    for row in csv.DictReader(f):
        if row['ticker'] in TICKERS:
            known[row['ticker']].add(row['exdate'])

osi_re = re.compile(r'^(\w+)\s+(\d{6})([CP])(\d{8})$')
with open(f"{BASE}/cboe_all_series_20251205.csv") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) < 2:
            continue
        m = osi_re.match(row[1].strip())
        if not m:
            continue
        ticker, exp6, cp, _ = m.groups()
        if ticker in TICKERS:
            known[ticker].add(f"20{exp6[:2]}-{exp6[2:4]}-{exp6[4:]}")

# Generate candidates
candidates = []
for ticker in TICKERS:
    missing = [d for d in all_fridays(GAP_START, GAP_END) if str(d) not in known[ticker]]
    if not missing:
        print(f"{ticker}: no missing Fridays")
        continue
    if not strikes[ticker]:
        print(f"{ticker}: WARNING — no strike template, skipping")
        continue
    for d in missing:
        for cp, strike in strikes[ticker]:
            candidates.append({
                'ric':    to_ric(ticker, d, cp, strike),
                'ticker': ticker,
                'exdate': str(d),
                'cp':     cp,
                'strike': strike,
                'source': 'brute_force',
            })
    n = len(missing) * len(strikes[ticker])
    dates_str = ', '.join(str(d) for d in missing)
    print(f"{ticker}: {len(missing)} missing Fridays → {n:,} candidates  ({dates_str})")

out = f"{BASE}/rics_top10_gap.csv"
with open(out, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike','source'])
    w.writeheader()
    w.writerows(candidates)

print(f"\nTotal: {len(candidates):,} candidates → {out}")
print(f"At 23 req/sec with 8 workers: ~{len(candidates)/23/60:.0f} min")
