"""Test bond pricing: historical prices + analytics for a few bonds."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import lseg.data as ld
from shared.lseg_rest_api import LSEGRestClient
import requests, json

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)

# First find some well-known issuer bonds
print("=== Finding some liquid, well-known bonds ===")
result = rest.search(
    view="GovCorpInstruments",
    filter=(
        "DbType eq 'CORP' and IsActive eq true and RCSCurrencyLeaf eq 'US Dollar' "
        "and RCSBondGradeLeaf eq 'Investment Grade' "
        "and RCSIssuerCountryLeaf xeq 'United States' "
        "and InstrumentTypeDescription eq 'Note' "
        "and FaceIssuedUSD gt 1000000000"
    ),
    select="RIC,ISIN,CUSIP,IssuerLegalName,CouponRate,MaturityDate,IssueDate,FaceIssuedUSD,EOMAmountOutstanding,CdsSeniorityEquivalentDescription",
    top=20,
)
print(f"Total large US IG notes: {result.get('Total', '?')}\n")
for h in result.get("Hits", [])[:20]:
    print(f"  {h.get('RIC', ''):25s} {h.get('IssuerLegalName', ''):35s} {h.get('CouponRate', '')}% mat {h.get('MaturityDate', '')[:10]}  ${h.get('FaceIssuedUSD', 0)/1e9:.1f}B")

# Pick a few RICs to test pricing
test_rics = []
for h in result.get("Hits", [])[:5]:
    if h.get("RIC"):
        test_rics.append(h["RIC"])

print(f"\n\n=== Testing Historical Pricing on {len(test_rics)} bonds ===")
for ric in test_rics:
    print(f"\n--- {ric} ---")
    try:
        data = rest.historical_pricing(ric, start="2020-01-01", end="2026-04-01", interval="P1M")
        headers = data.get("headers", [])
        rows = data.get("data", [])
        if headers:
            field_names = [h.get("name", h.get("title", "?")) for h in headers]
            print(f"  Fields: {field_names}")
        if rows:
            print(f"  Rows: {len(rows)}")
            print(f"  First: {rows[0]}")
            print(f"  Last:  {rows[-1]}")
        else:
            print("  No data returned")
    except Exception as e:
        print(f"  Error: {e}")

# Also test the intraday-summaries endpoint (what we use for options)
print(f"\n\n=== Testing via events/interday endpoints ===")
token = session._access_token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

for ric in test_rics[:2]:
    print(f"\n--- {ric} (interday-summaries) ---")
    url = f"https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries/{ric}"
    params = {"start": "2024-01-01", "end": "2026-04-01", "interval": "P1D"}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        data = resp.json()
        # Navigate the response structure
        for item in data:
            if isinstance(item, dict):
                h = item.get("headers", [])
                d = item.get("data", [])
                if h:
                    names = [x.get("name", "?") for x in h]
                    print(f"  Fields: {names}")
                    print(f"  Rows: {len(d)}")
                    if d:
                        print(f"  First: {d[0]}")
                        print(f"  Last:  {d[-1]}")
    else:
        print(f"  Status {resp.status_code}: {resp.text[:200]}")

# Test ld.get_data() for analytics
print(f"\n\n=== Testing ld.get_data() for analytics ===")
try:
    analytics = ld.get_data(
        test_rics[:5],
        [
            "TR.FiIssuerName", "TR.CUSIP", "TR.FiNetCoupon", "TR.FiMaturityDate",
            "TR.YieldToMaturityAnalytics", "TR.ModifiedDurationAnalytics",
            "TR.GovernmentSpreadAnalytics", "TR.ZSpreadAnalytics",
            "TR.OASAnalytics", "TR.PriceCleanAnalytics",
        ]
    )
    print(analytics.to_string())
except Exception as e:
    print(f"  Error: {e}")

ld.close_session()
