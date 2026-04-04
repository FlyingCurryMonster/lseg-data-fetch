"""
Add LSEG RIC columns to all_om_contracts_raw.csv → all_om_contracts.csv
"""
import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV  = os.path.join(SCRIPT_DIR, "all_om_contracts_raw.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "all_om_contracts.csv")

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


def main():
    written = 0
    with open(INPUT_CSV) as inf, open(OUTPUT_CSV, "w", newline="") as outf:
        reader = csv.DictReader(inf)
        writer = csv.writer(outf)
        writer.writerow(["secid", "ticker", "exdate", "cp_flag", "strike_price", "base_ric", "query_ric"])

        for row in reader:
            exdate_str   = str(row["exdate"])[:10]
            query_ric    = build_lseg_ric(row["ticker"], exdate_str, row["cp_flag"], int(row["strike_price"]))
            base_ric     = query_ric.split("^")[0]
            writer.writerow([row["secid"], row["ticker"], exdate_str, row["cp_flag"],
                             row["strike_price"], base_ric, query_ric])
            written += 1
            if written % 500000 == 0:
                print(f"  {written:,} rows...", flush=True)

    print(f"Done. {written:,} contracts → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
