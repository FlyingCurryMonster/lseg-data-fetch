"""Test expired/matured bond discovery and pricing."""

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

# First: check what AssetStatus values exist for inactive bonds
print("=== AssetStatus breakdown (all USD corp, active + inactive) ===")
result = rest.search(
    view="GovCorpInstruments",
    filter="DbType eq 'CORP' and RCSCurrencyLeaf eq 'US Dollar'",
    top=0,
    navigators="AssetStatus(buckets:20)",
)
for nav in result.get("Navigators", {}).get("AssetStatus", {}).get("Buckets", []):
    print(f"  {nav['Label']:15s} {nav['Count']:>10,}")

# Now search for matured bonds
print("\n=== Matured (IsActive eq false) USD IG corp notes ===")
result = rest.search(
    view="GovCorpInstruments",
    filter=(
        "DbType eq 'CORP' and IsActive eq false and RCSCurrencyLeaf eq 'US Dollar' "
        "and RCSBondGradeLeaf eq 'Investment Grade' "
        "and RCSIssuerCountryLeaf xeq 'United States' "
        "and InstrumentTypeDescription eq 'Note' "
        "and FaceIssuedUSD gt 1000000000"
    ),
    select="RIC,IssuerLegalName,CouponRate,MaturityDate,IssueDate,FaceIssuedUSD,AssetStatus",
    top=10,
)
print(f"Total: {result.get('Total', '?')}")
for h in result.get("Hits", []):
    print(f"  {h.get('RIC', ''):25s} {h.get('IssuerLegalName', ''):35s} {h.get('CouponRate')}% issued {h.get('IssueDate', '')[:10]} mat {h.get('MaturityDate', '')[:10]}  status={h.get('AssetStatus')}")

# Test pricing on matured bonds from different eras
print("\n\n=== Testing pricing on matured bonds ===")
test_cases = [
    ("Matured ~2025", "MaturityDate ge 2025-01-01 and MaturityDate lt 2025-06-01"),
    ("Matured ~2023", "MaturityDate ge 2023-01-01 and MaturityDate lt 2023-06-01"),
    ("Matured ~2020", "MaturityDate ge 2020-01-01 and MaturityDate lt 2020-06-01"),
    ("Matured ~2015", "MaturityDate ge 2015-01-01 and MaturityDate lt 2015-06-01"),
    ("Matured ~2010", "MaturityDate ge 2010-01-01 and MaturityDate lt 2010-06-01"),
]

for label, date_filter in test_cases:
    print(f"\n--- {label} ---")
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
        select="RIC,IssuerLegalName,CouponRate,MaturityDate,IssueDate,AssetStatus",
        top=3,
    )
    print(f"  Found: {result.get('Total', '?')} matured bonds")

    for h in result.get("Hits", [])[:1]:
        ric = h.get("RIC")
        issuer = h.get("IssuerLegalName", "?")
        cpn = h.get("CouponRate", "?")
        issue = h.get("IssueDate", "?")[:10]
        mat = h.get("MaturityDate", "?")[:10]
        status = h.get("AssetStatus", "?")
        print(f"  Bond: {ric} | {issuer} {cpn}% | issued {issue} mat {mat} | status={status}")

        url = f"https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries/{ric}"
        params = {"start": "2000-01-01", "end": "2026-04-03", "interval": "P1D"}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                if isinstance(item, dict) and item.get("data"):
                    rows = item["data"]
                    first_date = rows[-1][0] if rows else "?"
                    last_date = rows[0][0] if rows else "?"
                    print(f"  Rows: {len(rows)} | Earliest: {first_date} | Latest: {last_date}")
                elif isinstance(item, dict) and "data" in item and not item["data"]:
                    print(f"  No pricing data returned")
        else:
            print(f"  HTTP {resp.status_code}: {resp.text[:200]}")

ld.close_session()
