"""
Generate brute-force RIC candidates for weekly/daily expirations missing from
both OM (ends Aug 29) and CBOE Dec 5 snapshot.

Gap: Oct 17 → Dec 4, 2025 (roughly)
  - NVDA/AMD/TSLA: Fridays only
  - SPY:           Mon/Wed/Fri (+ some end-of-month)
  - SPX/SPXW:      Mon-Fri (daily AM/PM settlement)

Strategy: use the union of all strikes seen in the last 3 known weeklies
per ticker as the candidate strike set. Over-inclusive is fine — probe
will confirm which RICs actually exist.
"""
import csv
import json
import os
from datetime import date, timedelta
from collections import defaultdict
from build_rics import to_ric  # reuse RIC builder

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
GAP_START = date(2025, 10, 18)
GAP_END   = date(2025, 12, 4)   # CBOE snapshot covers Dec 5 onward

# US market holidays in the gap window (approximate)
HOLIDAYS = {
    date(2025, 11, 11),  # Veterans Day (bond market; equities sometimes open — skip)
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 11, 28),  # Day after Thanksgiving (half day — but still a trading day for expiries)
}
# Thanksgiving is the main one; Nov 28 is still a valid expiry date for SPY
FULL_HOLIDAYS = {date(2025, 11, 27)}


def all_fridays(start, end):
    """All Fridays in [start, end], excluding full holidays."""
    d = start
    while d.weekday() != 4:
        d += timedelta(1)
    result = []
    while d <= end:
        if d not in FULL_HOLIDAYS:
            result.append(d)
        d += timedelta(7)
    return result


def all_mwf(start, end):
    """All Mon/Wed/Fri trading days in [start, end]."""
    result = []
    d = start
    while d <= end:
        if d.weekday() in (0, 2, 4) and d not in FULL_HOLIDAYS:
            result.append(d)
        d += timedelta(1)
    return result


def all_weekdays(start, end):
    """All Mon-Fri trading days in [start, end]."""
    result = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in FULL_HOLIDAYS:
            result.append(d)
        d += timedelta(1)
    return result


def load_known_expirations(filepath, ticker):
    """Load already-known expiry dates for a ticker from a RIC CSV."""
    exps = set()
    with open(filepath) as f:
        for row in csv.DictReader(f):
            if row['ticker'] == ticker:
                exps.add(row['exdate'])
    return exps


def load_om_strikes(om_strikes_file, ticker, ref_exdates):
    """
    Load strike set from OM data for a ticker, using the last N known weeklies
    as the reference. Returns a set of (cp, strike) tuples.
    """
    strikes = set()
    with open(om_strikes_file) as f:
        for row in csv.DictReader(f):
            if row['ticker'] == ticker and row['exdate'] in ref_exdates:
                strikes.add((row['cp_flag'], float(row['strike'])))
    return strikes


def main():
    om_rics_file   = f"{BASE}/rics_from_om_gap.csv"
    cboe_rics_file = f"{BASE}/rics_from_cboe_dec2025.csv"
    om_data_file   = f"{BASE}/om_gap_contracts_aug2025.csv"

    # Reference expirations for strike templates: last 3 known weeklies per ticker
    ref_exdates = {
        'NVDA': {'2025-10-03', '2025-10-10', '2025-10-17'},
        'AMD':  {'2025-10-03', '2025-10-10', '2025-10-17'},
        'TSLA': {'2025-10-03', '2025-10-10', '2025-10-17'},
        'SPY':  {'2025-10-03', '2025-10-10', '2025-10-17'},
        'SPX':  {'2025-10-01', '2025-10-02', '2025-10-03'},
        'SPXW': {'2025-10-01', '2025-10-02', '2025-10-03'},  # will use SPX strikes from OM
    }

    # Expiry schedule type per ticker
    expiry_fn = {
        'NVDA':  all_fridays,
        'AMD':   all_fridays,
        'TSLA':  all_fridays,
        'SPY':   all_mwf,
        'SPX':   all_weekdays,
        'SPXW':  all_weekdays,
    }

    all_candidates = []
    summary = {}

    for ticker in ('NVDA', 'AMD', 'TSLA', 'SPY', 'SPX', 'SPXW'):
        # Load already-known expirations for this ticker
        known = load_known_expirations(om_rics_file, ticker) | \
                load_known_expirations(cboe_rics_file, ticker)

        # Generate all candidate expiry dates in gap
        candidate_dates = expiry_fn[ticker](GAP_START, GAP_END)

        # Filter to only truly missing dates
        missing_dates = [d for d in candidate_dates if str(d) not in known]

        if not missing_dates:
            print(f"{ticker}: no missing dates in gap")
            summary[ticker] = 0
            continue

        # Get strike template from last known weeklies
        # SPXW is listed as SPX in OM — use SPX strikes as template
        om_ticker = 'SPX' if ticker == 'SPXW' else ticker
        cp_strikes = load_om_strikes(om_data_file, om_ticker, ref_exdates[ticker])

        if not cp_strikes:
            print(f"{ticker}: WARNING — no strike template found, skipping")
            continue

        # Generate candidates
        ticker_candidates = []
        for exp_date in missing_dates:
            for cp, strike in cp_strikes:
                ric = to_ric(ticker, exp_date, cp, strike)
                ticker_candidates.append({
                    'ric':    ric,
                    'ticker': ticker,
                    'exdate': str(exp_date),
                    'cp':     cp,
                    'strike': strike,
                    'source': 'brute_force',
                })

        all_candidates.extend(ticker_candidates)
        summary[ticker] = len(ticker_candidates)
        missing_str = ', '.join(str(d) for d in missing_dates)
        print(f"{ticker}: {len(missing_dates)} missing dates → {len(ticker_candidates):,} candidates")
        print(f"  Dates: {missing_str}")

    # Save
    out_file = f"{BASE}/rics_brute_force_gap.csv"
    with open(out_file, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike','source'])
        w.writeheader()
        w.writerows(all_candidates)

    total = len(all_candidates)
    print(f"\nTotal brute-force candidates: {total:,}")
    print(f"Saved to {out_file}")
    print(f"\nAt 23 req/sec with 8 workers: ~{total/23/60:.0f} min to probe all")


if __name__ == '__main__':
    main()
