"""
Convert (ticker, exdate, cp_flag, strike) → LSEG OPRA RIC format.
Format: {ticker}{OPRA_month_cp_code}{DD}{YY}{strike*100:05d}.U

OPRA month codes:
  Calls: A=Jan B=Feb C=Mar D=Apr E=May F=Jun G=Jul H=Aug I=Sep J=Oct K=Nov L=Dec
  Puts:  M=Jan N=Feb O=Mar P=Apr Q=May R=Jun S=Jul T=Aug U=Sep V=Oct W=Nov X=Dec
"""
import csv
from datetime import date

CALL_CODES = {1:'A',2:'B',3:'C',4:'D',5:'E',6:'F',7:'G',8:'H',9:'I',10:'J',11:'K',12:'L'}
PUT_CODES  = {1:'M',2:'N',3:'O',4:'P',5:'Q',6:'R',7:'S',8:'T',9:'U',10:'V',11:'W',12:'X'}

def to_ric(ticker, exdate, cp, strike):
    """
    ticker : str  e.g. 'NVDA'
    exdate : str  e.g. '2025-12-19' or date object
    cp     : str  'C' or 'P' (or 'Call'/'Put')
    strike : float e.g. 200.0
    """
    if isinstance(exdate, str):
        exdate = date.fromisoformat(exdate)
    cp = cp[0].upper()  # normalize to 'C' or 'P'
    codes = CALL_CODES if cp == 'C' else PUT_CODES
    month_code = codes[exdate.month]
    dd = f"{exdate.day:02d}"
    yy = f"{exdate.year % 100:02d}"
    strike_enc = f"{int(round(strike * 100)):05d}"
    return f"{ticker}{month_code}{dd}{yy}{strike_enc}.U"


def validate():
    """Validate against known good RICs from option_contracts.csv."""
    known = [
        # ric, expiry, strike, cp
        ("NVDAA302609000.U", "2026-01-30", 90,  "Call"),
        ("NVDAM302607500.U", "2026-01-30", 75,  "Put"),
        ("NVDAC162618000.U", "2026-03-16", 180, "Call"),
        ("NVDAO162618000.U", "2026-03-16", 180, "Put"),
        ("NVDAD012624500.U", "2026-04-01", 245, "Call"),
        ("NVDAP012614500.U", "2026-04-01", 145, "Put"),
        ("NVDAM302626000.U", "2026-01-30", 260, "Put"),
        ("NVDAA302634000.U", "2026-01-30", 340, "Call"),
    ]
    print("=== Validation ===")
    all_ok = True
    for expected, expiry, strike, cp in known:
        got = to_ric("NVDA", expiry, cp, strike)
        status = "OK" if got == expected else "FAIL"
        if status == "FAIL":
            all_ok = False
        print(f"  {status}  expected={expected}  got={got}")
    print(f"\nAll OK: {all_ok}\n")
    return all_ok


def build_from_om_gap(om_file, out_file):
    """Build RICs from om_gap_contracts_aug2025.csv."""
    rows = []
    with open(om_file) as f:
        for row in csv.DictReader(f):
            ric = to_ric(row['ticker'], row['exdate'], row['cp_flag'], float(row['strike']))
            rows.append({'ric': ric, 'ticker': row['ticker'], 'exdate': row['exdate'],
                         'cp': row['cp_flag'], 'strike': row['strike']})
    with open(out_file, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike'])
        w.writeheader()
        w.writerows(rows)
    print(f"Written {len(rows):,} RICs to {out_file}")
    return rows


def build_from_cboe(cboe_file, tickers, out_file):
    """Build RICs from cboe_all_series_20251205.csv for given tickers."""
    import re
    osi_re = re.compile(r'^(\w+)\s+(\d{6})([CP])(\d{8})$')
    rows = []
    with open(cboe_file) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) < 2: continue
            m = osi_re.match(row[1].strip())
            if not m: continue
            ticker, exp6, cp, strike_raw = m.groups()
            if ticker not in tickers: continue
            exdate = f"20{exp6[:2]}-{exp6[2:4]}-{exp6[4:]}"
            strike = int(strike_raw) / 1000.0
            ric = to_ric(ticker, exdate, cp, strike)
            rows.append({'ric': ric, 'ticker': ticker, 'exdate': exdate,
                         'cp': cp, 'strike': strike})
    with open(out_file, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['ric','ticker','exdate','cp','strike'])
        w.writeheader()
        w.writerows(rows)
    print(f"Written {len(rows):,} RICs to {out_file}")
    return rows


if __name__ == '__main__':
    import os
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

    validate()

    TICKERS = {'NVDA', 'SPY', 'AMD', 'TSLA', 'SPX', 'SPXW'}

    # Build from OM gap (Sep–Dec 2025 expirations known as of Aug 29)
    om_rics = build_from_om_gap(
        f"{base}/om_gap_contracts_aug2025.csv",
        f"{base}/rics_from_om_gap.csv"
    )

    # Build from CBOE Dec 5 snapshot (Dec 2025 onward)
    cboe_rics = build_from_cboe(
        f"{base}/cboe_all_series_20251205.csv",
        TICKERS,
        f"{base}/rics_from_cboe_dec2025.csv"
    )

    # Summary
    from collections import defaultdict
    print("\n=== OM gap RICs by ticker ===")
    by_t = defaultdict(int)
    for r in om_rics: by_t[r['ticker']] += 1
    for t in sorted(by_t): print(f"  {t}: {by_t[t]:,}")

    print("\n=== CBOE Dec 2025 RICs by ticker ===")
    by_t2 = defaultdict(int)
    for r in cboe_rics: by_t2[r['ticker']] += 1
    for t in sorted(by_t2): print(f"  {t}: {by_t2[t]:,}")
