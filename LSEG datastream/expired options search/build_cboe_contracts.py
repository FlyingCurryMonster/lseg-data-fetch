"""
Parse the full CBOE all-series snapshot and build LSEG RICs for all contracts.

Input:  cboe_all_series_20251205.csv  (OSI format)
Output: all_cboe_contracts.csv

Columns: ticker, exdate, cp_flag, strike, base_ric, query_ric

OSI format: TICKER(padded to 6) YYMMDD C/P STRIKE(8 digits, thousandths of a dollar)
Example:    NVDA   251219C00120000  → NVDA, 2025-12-19, C, 120.000

LSEG expired RIC format (OPRA equity style):
  {ROOT}{month_code}{DD}{YY}{strike_5digits}.U^{expired_code}{YY}
  Strike in LSEG = OSI strike / 10  (OSI is thousandths, LSEG is hundredths * 10)

Usage:
  python build_cboe_contracts.py
"""

import csv
import os
import re

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT     = os.path.join(SCRIPT_DIR, "cboe_all_series_20251205.csv")
OUTPUT_CSV   = os.path.join(SCRIPT_DIR, "all_cboe_contracts.csv")

CALL_CODES = "ABCDEFGHIJKL"
PUT_CODES  = "MNOPQRSTUVWX"

osi_re = re.compile(r'^(\S+)\s+(\d{6})([CP])(\d{8})$')


def build_lseg_ric(root, exdate_str, cp_flag, osi_strike):
    """
    Build expired LSEG OPRA RIC.
    osi_strike: raw OSI integer (thousandths of a dollar)
    LSEG strike = osi_strike // 10  (drops the last digit, gives hundredths * 10)
    """
    month = int(exdate_str[5:7])
    day   = int(exdate_str[8:10])
    year  = int(exdate_str[2:4])

    month_code   = CALL_CODES[month - 1] if cp_flag == "C" else PUT_CODES[month - 1]
    lseg_strike  = osi_strike // 10
    expired_code = CALL_CODES[month - 1]

    active_ric = f"{root}{month_code}{day:02d}{year:02d}{lseg_strike:05d}.U"
    return f"{active_ric}^{expired_code}{year:02d}"


def main():
    total_rows = 0
    written    = 0
    skipped    = 0

    with open(SNAPSHOT) as inf, open(OUTPUT_CSV, "w", newline="") as outf:
        reader = csv.reader(inf)
        next(reader)  # skip header

        writer = csv.writer(outf)
        writer.writerow(["ticker", "exdate", "cp_flag", "strike", "base_ric", "query_ric"])

        for row in reader:
            total_rows += 1
            if len(row) < 2:
                skipped += 1
                continue

            osi = row[1].strip()
            m = osi_re.match(osi)
            if not m:
                skipped += 1
                continue

            ticker, expiry, cp_flag, strike_raw = m.groups()
            osi_strike = int(strike_raw)
            strike     = osi_strike / 1000.0

            # Parse expiry: YYMMDD → YYYY-MM-DD
            yy, mm, dd = expiry[:2], expiry[2:4], expiry[4:6]
            year_full  = int("20" + yy)
            exdate_str = f"{year_full}-{mm}-{dd}"

            query_ric = build_lseg_ric(ticker, exdate_str, cp_flag, osi_strike)
            base_ric  = query_ric.split("^")[0]

            writer.writerow([ticker, exdate_str, cp_flag, strike, base_ric, query_ric])
            written += 1

            if written % 100000 == 0:
                print(f"  {written:,} contracts written...", flush=True)

    print(f"\nDone. {written:,} contracts written, {skipped:,} skipped.")
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
