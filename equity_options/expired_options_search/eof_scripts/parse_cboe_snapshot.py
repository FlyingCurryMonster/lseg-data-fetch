"""Parse CBOE all-series snapshot (Dec 5 2025) — count contracts by ticker/expiry."""
import csv, re
from collections import defaultdict

SNAPSHOT = "/home/rakin/wrds-data/LSEG datastream/expired options search/cboe_all_series_20251205.csv"
TICKERS = ['NVDA', 'SPY', 'AMD', 'TSLA']

# OSI format: TICKER(padded to 6) YYMMDD C/P STRIKE(8 digits, thousandths)
osi_re = re.compile(r'^(\w+)\s+(\d{6})([CP])(\d{8})$')

counts = defaultdict(lambda: defaultdict(int))
all_contracts = defaultdict(list)

with open(SNAPSHOT) as f:
    reader = csv.reader(f)
    next(reader)  # header
    for row in reader:
        if len(row) < 2:
            continue
        osi = row[1].strip()
        m = osi_re.match(osi)
        if not m:
            continue
        ticker, expiry, cp, strike_raw = m.groups()
        if ticker in TICKERS:
            strike = int(strike_raw) / 1000.0
            counts[ticker][expiry] += 1
            all_contracts[ticker].append((expiry, cp, strike))

for ticker in TICKERS:
    exps = sorted(counts[ticker])
    total = sum(counts[ticker].values())
    print(f"\n{ticker}: {len(exps)} expiries, {total} total contracts")
    for e in exps:
        print(f"  20{e[:2]}-{e[2:4]}-{e[4:]} : {counts[ticker][e]:,} contracts")

# Show strike range per expiry for NVDA
print("\n\nNVDA strike ranges by expiry:")
from itertools import groupby
nvda = sorted(all_contracts['NVDA'])
from collections import defaultdict as dd
by_exp = dd(list)
for exp, cp, strike in nvda:
    by_exp[exp].append(strike)
for exp in sorted(by_exp):
    strikes = by_exp[exp]
    print(f"  20{exp[:2]}-{exp[2:4]}-{exp[4:]} : ${min(strikes):.0f} – ${max(strikes):.0f}  ({len(strikes)} contracts, {len(set(strikes))} unique strikes)")
