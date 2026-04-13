"""
Cross-check LSEG dividend data against CRSP distributions.

Compares ex-dates, amounts, and pay dates for AAPL, MSFT, JNJ.
Also compares stock split records.
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

# --- Session setup ---
config = {
    "sessions": {
        "default": "platform.rdp",
        "platform": {
            "rdp": {
                "app-key": os.getenv("DSWS_APPKEY"),
                "username": os.getenv("DSWS_USERNAME"),
                "password": os.getenv("DSWS_PASSWORD"),
                "signon_control": True
            }
        }
    }
}
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lseg-data.config.json")
with open(config_path, "w") as f:
    json.dump(config, f, indent=4)

ld.open_session(config_name=config_path)

# =====================================================================
# Pull LSEG dividend data
# =====================================================================
print("Pulling LSEG dividend data...")
lseg_divs = ld.get_data(
    universe=["AAPL.O", "MSFT.O", "JNJ"],
    fields=[
        "TR.DivExDate",
        "TR.DivPayDate",
        "TR.DivUnadjustedGross",
        "TR.DivAdjustedGross",
        "TR.DivType",
    ],
    parameters={"SDate": "2020-01-01", "EDate": "2025-12-31"}
)

# Clean up LSEG data
lseg_divs = lseg_divs.dropna(subset=["Dividend Ex Date"])
lseg_divs["Dividend Ex Date"] = pd.to_datetime(lseg_divs["Dividend Ex Date"]).dt.strftime("%Y-%m-%d")
lseg_divs["Dividend Pay Date"] = pd.to_datetime(lseg_divs["Dividend Pay Date"]).dt.strftime("%Y-%m-%d")

# Map LSEG RICs to tickers
ric_to_ticker = {"AAPL.O": "AAPL", "MSFT.O": "MSFT", "JNJ": "JNJ"}
lseg_divs["Ticker"] = lseg_divs["Instrument"].map(ric_to_ticker)

print(f"LSEG: {len(lseg_divs)} dividend records")

# =====================================================================
# CRSP data (hardcoded from ClickHouse query results)
# =====================================================================
# PERMNO mapping: AAPL=14593, MSFT=10107, JNJ=22111
crsp_data = [
    # MSFT
    (10107, "MSFT", "2020-02-19", 0.51, "2020-03-12"),
    (10107, "MSFT", "2020-05-20", 0.51, "2020-06-11"),
    (10107, "MSFT", "2020-08-19", 0.51, "2020-09-10"),
    (10107, "MSFT", "2020-11-18", 0.56, "2020-12-10"),
    (10107, "MSFT", "2021-02-17", 0.56, "2021-03-11"),
    (10107, "MSFT", "2021-05-19", 0.56, "2021-06-10"),
    (10107, "MSFT", "2021-08-18", 0.56, "2021-09-09"),
    (10107, "MSFT", "2021-11-17", 0.62, "2021-12-09"),
    (10107, "MSFT", "2022-02-16", 0.62, "2022-03-10"),
    (10107, "MSFT", "2022-05-18", 0.62, "2022-06-09"),
    (10107, "MSFT", "2022-08-17", 0.62, "2022-09-08"),
    (10107, "MSFT", "2022-11-16", 0.68, "2022-12-08"),
    (10107, "MSFT", "2023-02-15", 0.68, "2023-03-09"),
    (10107, "MSFT", "2023-05-17", 0.68, "2023-06-08"),
    (10107, "MSFT", "2023-08-16", 0.68, "2023-09-14"),
    (10107, "MSFT", "2023-11-15", 0.75, "2023-12-14"),
    (10107, "MSFT", "2024-02-14", 0.75, "2024-03-14"),
    (10107, "MSFT", "2024-05-15", 0.75, "2024-06-13"),
    (10107, "MSFT", "2024-08-15", 0.75, "2024-09-12"),
    (10107, "MSFT", "2024-11-21", 0.83, "2024-12-12"),
    (10107, "MSFT", "2025-02-20", 0.83, "2025-03-13"),
    (10107, "MSFT", "2025-05-15", 0.83, "2025-06-12"),
    (10107, "MSFT", "2025-08-21", 0.83, "2025-09-11"),
    (10107, "MSFT", "2025-11-20", 0.91, "2025-12-11"),
    # AAPL
    (14593, "AAPL", "2020-02-07", 0.77, "2020-02-13"),
    (14593, "AAPL", "2020-05-08", 0.82, "2020-05-14"),
    (14593, "AAPL", "2020-08-07", 0.82, "2020-08-13"),
    (14593, "AAPL", "2020-11-06", 0.205, "2020-11-12"),
    (14593, "AAPL", "2021-02-05", 0.205, "2021-02-11"),
    (14593, "AAPL", "2021-05-07", 0.22, "2021-05-13"),
    (14593, "AAPL", "2021-08-06", 0.22, "2021-08-12"),
    (14593, "AAPL", "2021-11-05", 0.22, "2021-11-11"),
    (14593, "AAPL", "2022-02-04", 0.22, "2022-02-10"),
    (14593, "AAPL", "2022-05-06", 0.23, "2022-05-12"),
    (14593, "AAPL", "2022-08-05", 0.23, "2022-08-11"),
    (14593, "AAPL", "2022-11-04", 0.23, "2022-11-10"),
    (14593, "AAPL", "2023-02-10", 0.23, "2023-02-16"),
    (14593, "AAPL", "2023-05-12", 0.24, "2023-05-18"),
    (14593, "AAPL", "2023-08-11", 0.24, "2023-08-17"),
    (14593, "AAPL", "2023-11-10", 0.24, "2023-11-16"),
    (14593, "AAPL", "2024-02-09", 0.24, "2024-02-15"),
    (14593, "AAPL", "2024-05-10", 0.25, "2024-05-16"),
    (14593, "AAPL", "2024-08-12", 0.25, "2024-08-15"),
    (14593, "AAPL", "2024-11-08", 0.25, "2024-11-14"),
    (14593, "AAPL", "2025-02-10", 0.25, "2025-02-13"),
    (14593, "AAPL", "2025-05-12", 0.26, "2025-05-15"),
    (14593, "AAPL", "2025-08-11", 0.26, "2025-08-14"),
    (14593, "AAPL", "2025-11-10", 0.26, "2025-11-13"),
    # JNJ
    (22111, "JNJ", "2020-02-24", 0.95, "2020-03-10"),
    (22111, "JNJ", "2020-05-22", 1.01, "2020-06-09"),
    (22111, "JNJ", "2020-08-24", 1.01, "2020-09-08"),
    (22111, "JNJ", "2020-11-23", 1.01, "2020-12-08"),
    (22111, "JNJ", "2021-02-22", 1.01, "2021-03-09"),
    (22111, "JNJ", "2021-05-24", 1.06, "2021-06-08"),
    (22111, "JNJ", "2021-08-23", 1.06, "2021-09-07"),
    (22111, "JNJ", "2021-11-22", 1.06, "2021-12-07"),
    (22111, "JNJ", "2022-02-18", 1.06, "2022-03-08"),
    (22111, "JNJ", "2022-05-23", 1.13, "2022-06-07"),
    (22111, "JNJ", "2022-08-22", 1.13, "2022-09-06"),
    (22111, "JNJ", "2022-11-21", 1.13, "2022-12-06"),
    (22111, "JNJ", "2023-02-17", 1.13, "2023-03-07"),
    (22111, "JNJ", "2023-05-22", 1.19, "2023-06-06"),
    (22111, "JNJ", "2023-08-25", 1.19, "2023-09-07"),
    (22111, "JNJ", "2023-11-20", 1.19, "2023-12-05"),
    (22111, "JNJ", "2024-02-16", 1.19, "2024-03-05"),
    (22111, "JNJ", "2024-05-20", 1.24, "2024-06-04"),
    (22111, "JNJ", "2024-08-27", 1.24, "2024-09-10"),
    (22111, "JNJ", "2024-11-26", 1.24, "2024-12-10"),
    (22111, "JNJ", "2025-02-18", 1.24, "2025-03-04"),
    (22111, "JNJ", "2025-05-27", 1.30, "2025-06-10"),
    (22111, "JNJ", "2025-08-26", 1.30, "2025-09-09"),
    (22111, "JNJ", "2025-11-25", 1.30, "2025-12-09"),
]

crsp_df = pd.DataFrame(crsp_data, columns=["PERMNO", "Ticker", "ExDate", "DivAmt", "PayDate"])

# =====================================================================
# Compare by ticker
# =====================================================================
for ticker in ["AAPL", "MSFT", "JNJ"]:
    print("\n" + "=" * 80)
    print(f"  {ticker}: CRSP vs LSEG")
    print("=" * 80)

    crsp = crsp_df[crsp_df["Ticker"] == ticker].copy()
    lseg = lseg_divs[lseg_divs["Ticker"] == ticker].copy()

    crsp_dates = set(crsp["ExDate"])
    lseg_dates = set(lseg["Dividend Ex Date"])

    matched = crsp_dates & lseg_dates
    crsp_only = crsp_dates - lseg_dates
    lseg_only = lseg_dates - crsp_dates

    print(f"\n  CRSP records: {len(crsp)}")
    print(f"  LSEG records: {len(lseg)}")
    print(f"  Matched ex-dates: {len(matched)}")

    if crsp_only:
        print(f"\n  IN CRSP ONLY (not in LSEG):")
        for d in sorted(crsp_only):
            amt = crsp[crsp["ExDate"] == d]["DivAmt"].iloc[0]
            print(f"    {d}  ${amt:.4f}")

    if lseg_only:
        print(f"\n  IN LSEG ONLY (not in CRSP):")
        for d in sorted(lseg_only):
            amt = lseg[lseg["Dividend Ex Date"] == d]["Gross Dividend Amount"].iloc[0]
            print(f"    {d}  ${amt}")

    # Compare amounts on matched dates
    print(f"\n  Amount comparison on matched dates:")
    mismatches = 0
    for d in sorted(matched):
        crsp_amt = crsp[crsp["ExDate"] == d]["DivAmt"].iloc[0]
        lseg_amt = float(lseg[lseg["Dividend Ex Date"] == d]["Gross Dividend Amount"].iloc[0])
        crsp_pay = crsp[crsp["ExDate"] == d]["PayDate"].iloc[0]
        lseg_pay = lseg[lseg["Dividend Ex Date"] == d]["Dividend Pay Date"].iloc[0]

        status = "OK" if abs(crsp_amt - lseg_amt) < 0.001 else "MISMATCH"
        pay_status = "OK" if crsp_pay == lseg_pay else "DIFF"

        if status != "OK" or pay_status != "OK":
            mismatches += 1
            print(f"    {d}: CRSP ${crsp_amt:.4f} vs LSEG ${lseg_amt:.4f} [{status}]  "
                  f"Pay: CRSP {crsp_pay} vs LSEG {lseg_pay} [{pay_status}]")

    if mismatches == 0:
        print(f"    All {len(matched)} matched records: amounts and pay dates IDENTICAL")
    else:
        print(f"    {mismatches} mismatches out of {len(matched)} matched records")

# =====================================================================
# Stock split comparison
# =====================================================================
print("\n" + "=" * 80)
print("  STOCK SPLIT COMPARISON")
print("=" * 80)

print("\n  CRSP: AAPL 4:1 split")
print("    Ex-date: 2020-08-31, DisFacPr=3, DisFacShr=3")
print("    Meaning: 1 old share -> (1+3) = 4 new shares")

print("\n  LSEG: AAPL 4:1 split")
lseg_ca = ld.get_data(
    universe=["AAPL.O"],
    fields=["TR.CAEffectiveDate", "TR.CAAdjustmentFactor", "TR.CAAdjustmentType"],
    parameters={"SDate": "2020-01-01", "EDate": "2021-01-01"}
)
for _, row in lseg_ca.iterrows():
    factor = row.get("Adjustment Factor")
    if factor is not None and factor != 1.0:
        print(f"    Effective: {row['Capital Change Effective Date']}, Factor: {factor}")
        print(f"    Meaning: new price = old price * {factor} (i.e., {int(1/factor)}:1 split)")

print("\n  Date difference: CRSP ex-date is 2020-08-31 (Mon), LSEG effective is 2020-08-28 (Fri)")
print("  CRSP uses the first trading day post-split; LSEG uses the actual effective date.")

# =====================================================================
# AAPL adjusted dividend check
# =====================================================================
print("\n" + "=" * 80)
print("  AAPL ADJUSTED DIVIDEND VERIFICATION")
print("=" * 80)
print("\n  Pre-split dividends (LSEG adjusted = gross * 0.25):")
for _, row in lseg_divs[
    (lseg_divs["Ticker"] == "AAPL") &
    (lseg_divs["Dividend Ex Date"] < "2020-08-28")
].iterrows():
    gross = float(row["Gross Dividend Amount"])
    adj = float(row["Adjusted Gross Dividend Amount"])
    ratio = adj / gross if gross else 0
    print(f"    {row['Dividend Ex Date']}: Gross ${gross:.4f} -> Adj ${adj:.4f}  (ratio: {ratio:.4f})")

print("\n  Post-split dividends (should be equal, no further adjustment):")
for _, row in lseg_divs[
    (lseg_divs["Ticker"] == "AAPL") &
    (lseg_divs["Dividend Ex Date"] >= "2020-08-28")
].head(4).iterrows():
    gross = float(row["Gross Dividend Amount"])
    adj = float(row["Adjusted Gross Dividend Amount"])
    print(f"    {row['Dividend Ex Date']}: Gross ${gross:.4f} -> Adj ${adj:.4f}  (equal: {abs(gross - adj) < 0.001})")

# --- Cleanup ---
ld.close_session()
os.remove(config_path)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
