"""
Build per-ticker contracts.csv files from all three RIC source files.

Reads:
  - expired options search/all_om_contracts.csv    (OM, base_ric/query_ric)
  - expired options search/all_cboe_contracts.csv  (CBOE Dec 5, base_ric/query_ric)
  - expired options search/all_names_gap_rics.csv  (gap Aug-Dec 2025, ric column)

Writes:
  - data/{TICKER}/contracts.csv  with columns: base_ric, query_ric, source

Deduplicates by base_ric (OM takes priority, then CBOE, then gap).
Creates data/{TICKER}/ directory if needed.

Usage:
  python build_ticker_contracts.py
"""

import csv
import os
from collections import defaultdict

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
SEARCH_DIR   = os.path.join(SCRIPT_DIR, "..", "expired options search")
DATA_DIR     = os.path.join(SCRIPT_DIR, "data")

SOURCES = [
    (os.path.join(SEARCH_DIR, "all_om_contracts.csv"),   "om"),
    (os.path.join(SEARCH_DIR, "all_cboe_contracts.csv"), "cboe"),
    (os.path.join(SEARCH_DIR, "all_names_gap_rics.csv"), "gap"),
]


def load_source(path, source_name):
    """Yield (ticker, base_ric, query_ric) from a source CSV."""
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("ticker", "").upper()
            if not ticker:
                continue
            if "query_ric" in row:
                yield ticker, row["base_ric"], row["query_ric"]
            else:
                ric = row["ric"]
                yield ticker, ric.split("^")[0], ric


def main():
    # ticker -> {base_ric -> (query_ric, source)}  — first writer wins (OM > CBOE > gap)
    ticker_contracts = defaultdict(dict)

    for path, source_name in SOURCES:
        if not os.path.exists(path):
            print(f"  SKIP (not found): {os.path.basename(path)}")
            continue
        count = 0
        new = 0
        for ticker, base_ric, query_ric in load_source(path, source_name):
            count += 1
            if base_ric not in ticker_contracts[ticker]:
                ticker_contracts[ticker][base_ric] = (query_ric, source_name)
                new += 1
        print(f"  {source_name:6s}  {count:>8,} rows   {new:>8,} new unique contracts   {len(ticker_contracts):>6,} tickers so far")

    print(f"\nWriting per-ticker contracts.csv files...")
    tickers_written = 0
    total_contracts = 0

    for ticker, contracts in sorted(ticker_contracts.items()):
        ticker_dir = os.path.join(DATA_DIR, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        out_path = os.path.join(ticker_dir, "contracts.csv")
        with open(out_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["base_ric", "query_ric", "source"])
            for base_ric, (query_ric, source) in sorted(contracts.items()):
                writer.writerow([base_ric, query_ric, source])
        tickers_written += 1
        total_contracts += len(contracts)

    print(f"Done. {tickers_written:,} tickers, {total_contracts:,} total contracts.")
    print(f"Output: {DATA_DIR}/{{TICKER}}/contracts.csv")


if __name__ == "__main__":
    main()
