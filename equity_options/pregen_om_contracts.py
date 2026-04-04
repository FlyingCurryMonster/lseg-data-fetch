"""
Pre-generate all contract RICs from ClickHouse into a single flat CSV so the
data feed machine can run downloads without a ClickHouse connection.

Reads all_om_tickers.csv, queries option_metrics.option_pricing for each
ticker, and writes all_om_contracts.csv with columns:
  ticker, secid, exdate, cp_flag, strike_price, base_ric, query_ric

Usage:
  python pregen_om_contracts.py [--workers N]
"""

import csv
import os
import sys
import threading
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from clickhouse_driver import Client

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
TICKERS_CSV = os.path.join(SCRIPT_DIR, "all_om_tickers.csv")
OUTPUT_CSV  = os.path.join(SCRIPT_DIR, "all_om_contracts.csv")

CALL_CODES = "ABCDEFGHIJKL"
PUT_CODES  = "MNOPQRSTUVWX"


def build_lseg_ric(root, exdate_str, cp_flag, strike_price):
    month = int(exdate_str[5:7])
    day   = int(exdate_str[8:10])
    year  = int(exdate_str[2:4])
    month_code   = CALL_CODES[month - 1] if cp_flag == "C" else PUT_CODES[month - 1]
    lseg_strike  = int(strike_price) // 10
    expired_code = CALL_CODES[month - 1]
    active_ric   = f"{root}{month_code}{day:02d}{year:02d}{lseg_strike:05d}.U"
    return f"{active_ric}^{expired_code}{year:02d}"


def fetch_contracts(ticker, secid, window_start, window_end):
    ch = Client("localhost")
    rows = ch.execute(
        """
        SELECT DISTINCT exdate, cp_flag, strike_price
        FROM option_metrics.option_pricing
        WHERE secid = %(secid)s
          AND exdate >= %(start)s
          AND exdate < %(end)s
        ORDER BY exdate, cp_flag, strike_price
        """,
        {"secid": secid, "start": str(window_start), "end": str(window_end)},
    )
    out = []
    for exdate, cp_flag, strike_price in rows:
        exdate_str = str(exdate)[:10]
        query_ric  = build_lseg_ric(ticker, exdate_str, cp_flag, strike_price)
        base_ric   = query_ric.split("^")[0]
        out.append([ticker, secid, exdate_str, cp_flag, strike_price, base_ric, query_ric])
    return ticker, out


def main():
    num_workers = 8
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--workers" and i + 2 < len(sys.argv):
            num_workers = int(sys.argv[i + 2])

    today        = date.today()
    window_end   = today
    window_start = today - timedelta(days=365)

    print(f"Window:  {window_start} → {window_end}")
    print(f"Workers: {num_workers}")
    print(f"Output:  {OUTPUT_CSV}")
    print()

    tickers = []
    with open(TICKERS_CSV) as f:
        for row in csv.DictReader(f):
            tickers.append((row["ticker"], int(row["secid"])))

    total = len(tickers)
    done  = 0
    lock  = threading.Lock()

    with open(OUTPUT_CSV, "w", newline="") as outf:
        writer = csv.writer(outf)
        writer.writerow(["ticker", "secid", "exdate", "cp_flag", "strike_price", "base_ric", "query_ric"])

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = {
                pool.submit(fetch_contracts, ticker, secid, window_start, window_end): ticker
                for ticker, secid in tickers
            }
            for fut in as_completed(futures):
                ticker, rows = fut.result()
                with lock:
                    writer.writerows(rows)
                    outf.flush()
                    done += 1
                    print(f"[{done}/{total}] {ticker}  {len(rows):,} contracts", flush=True)

    print("\nDone.")


if __name__ == "__main__":
    main()
