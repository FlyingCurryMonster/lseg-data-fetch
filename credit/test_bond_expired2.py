"""Test expired bond pricing using CUSIP/ISIN identifiers."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import lseg.data as ld
from lseg_rest_api import LSEGRestClient
import requests

config_path = os.path.join(os.path.dirname(__file__), "intraday options data", "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)
token = session._access_token
hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Get matured bonds with all available identifiers
test_cases = [
    ("Matured ~2025", "MaturityDate ge 2025-01-01 and MaturityDate lt 2025-06-01"),
    ("Matured ~2020", "MaturityDate ge 2020-01-01 and MaturityDate lt 2020-06-01"),
    ("Matured ~2015", "MaturityDate ge 2015-01-01 and MaturityDate lt 2015-06-01"),
    ("Matured ~2010", "MaturityDate ge 2010-01-01 and MaturityDate lt 2010-06-01"),
]

for label, date_filter in test_cases:
    print(f"\n{'='*60}")
    print(f"=== {label} ===")
    result = rest.search(
        view="GovCorpInstruments",
        filter=(
            f"DbType eq 'CORP' and IsActive eq false and RCSCurrencyLeaf eq 'US Dollar' "
            f"and RCSBondGradeLeaf eq 'Investment Grade' "
            f"and RCSIssuerCountryLeaf xeq 'United States' "
            f"and InstrumentTypeDescription eq 'Note' "
            f"and FaceIssuedUSD gt 1000000000 "
            f"and {date_filter}"
        ),
        select="RIC,ISIN,CUSIP,IssuerLegalName,CouponRate,MaturityDate,IssueDate,AssetStatus",
        top=3,
    )

    for h in result.get("Hits", [])[:1]:
        ric = h.get("RIC")
        isin = h.get("ISIN")
        cusip = h.get("CUSIP")
        issuer = h.get("IssuerLegalName", "?")
        cpn = h.get("CouponRate", "?")
        issue = h.get("IssueDate", "?")[:10]
        mat = h.get("MaturityDate", "?")[:10]
        print(f"  {issuer} {cpn}% | issued {issue} mat {mat}")
        print(f"  RIC={ric} | ISIN={isin} | CUSIP={cusip}")

        # Try each identifier format against pricing API
        candidates = []
        if cusip:
            candidates.append(("CUSIP=", f"{cusip}="))
        if isin:
            candidates.append(("ISIN=", f"{isin}="))
            candidates.append(("ISIN (no =)", isin))
        if ric:
            candidates.append(("RIC", ric))

        for id_label, identifier in candidates:
            url = f"https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries/{identifier}"
            params = {"start": "2000-01-01", "end": "2026-04-03", "interval": "P1D"}
            resp = requests.get(url, headers=hdrs, params=params)
            if resp.status_code == 200:
                data = resp.json()
                for item in data:
                    if isinstance(item, dict) and item.get("data"):
                        rows = item["data"]
                        first_date = rows[-1][0] if rows else "?"
                        last_date = rows[0][0] if rows else "?"
                        print(f"  [{id_label}] => {len(rows)} rows | {first_date} to {last_date}")
                        break
                else:
                    print(f"  [{id_label}] => 200 but no data")
            else:
                print(f"  [{id_label}] => HTTP {resp.status_code}")

ld.close_session()
