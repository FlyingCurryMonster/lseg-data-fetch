"""Test how far back bond pricing history goes."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import lseg.data as ld
from lseg_rest_api import LSEGRestClient
import requests

config_path = os.path.join(os.path.dirname(__file__), "intraday options data", "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)
token = session._access_token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Find bonds issued in different eras
test_cases = [
    ("Issued ~2024 (recent)", "IssueDate ge 2024-01-01 and IssueDate lt 2025-01-01"),
    ("Issued ~2020", "IssueDate ge 2020-01-01 and IssueDate lt 2021-01-01"),
    ("Issued ~2015", "IssueDate ge 2015-01-01 and IssueDate lt 2016-01-01"),
    ("Issued ~2010", "IssueDate ge 2010-01-01 and IssueDate lt 2011-01-01"),
    ("Issued ~2005", "IssueDate ge 2005-01-01 and IssueDate lt 2006-01-01"),
]

for label, date_filter in test_cases:
    print(f"\n=== {label} ===")
    result = rest.search(
        view="GovCorpInstruments",
        filter=(
            f"DbType eq 'CORP' and IsActive eq true and RCSCurrencyLeaf eq 'US Dollar' "
            f"and RCSBondGradeLeaf eq 'Investment Grade' "
            f"and RCSIssuerCountryLeaf xeq 'United States' "
            f"and InstrumentTypeDescription eq 'Note' "
            f"and FaceIssuedUSD gt 1000000000 "
            f"and {date_filter}"
        ),
        select="RIC,IssuerLegalName,CouponRate,MaturityDate,IssueDate",
        top=3,
    )

    for h in result.get("Hits", [])[:1]:  # just test first bond
        ric = h.get("RIC")
        issuer = h.get("IssuerLegalName", "?")
        cpn = h.get("CouponRate", "?")
        issue = h.get("IssueDate", "?")[:10]
        mat = h.get("MaturityDate", "?")[:10]
        print(f"  Bond: {ric} | {issuer} {cpn}% | issued {issue} mat {mat}")

        # Pull pricing going back as far as possible
        url = f"https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries/{ric}"
        params = {"start": "2000-01-01", "end": "2026-04-03", "interval": "P1D"}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                if isinstance(item, dict) and item.get("data"):
                    rows = item["data"]
                    first_date = rows[-1][0] if rows else "?"  # last element = earliest
                    last_date = rows[0][0] if rows else "?"    # first element = most recent
                    print(f"  Rows: {len(rows)} | Earliest: {first_date} | Latest: {last_date}")
        else:
            print(f"  HTTP {resp.status_code}: {resp.text[:200]}")

ld.close_session()
