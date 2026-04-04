"""Check total universe size for ALL bonds (corp + govt + agency + muni + everything)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import lseg.data as ld
from shared.lseg_rest_api import LSEGRestClient

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)

# Total universe
print("=== ALL bonds (GovCorpInstruments view, no filter) ===")
result = rest.search(view="GovCorpInstruments", top=0)
print(f"Total: {result.get('Total', '?')}")

# By DbType
print("\n=== DbType breakdown ===")
result = rest.search(view="GovCorpInstruments", top=0, navigators="DbType(buckets:20)")
for nav in result.get("Navigators", {}).get("DbType", {}).get("Buckets", []):
    print(f"  {nav['Label']:15s} {nav['Count']:>12,}")

# Active vs inactive
print("\n=== Active vs Inactive ===")
for active in [True, False]:
    result = rest.search(
        view="GovCorpInstruments",
        filter=f"IsActive eq {str(active).lower()}",
        top=0,
    )
    print(f"  IsActive={active}: {result.get('Total', '?'):>12,}")

# Currency breakdown
print("\n=== Top 20 currencies ===")
result = rest.search(view="GovCorpInstruments", top=0, navigators="RCSCurrencyLeaf(buckets:20)")
for nav in result.get("Navigators", {}).get("RCSCurrencyLeaf", {}).get("Buckets", []):
    print(f"  {nav['Label']:30s} {nav['Count']:>12,}")

# Issue year distribution
print("\n=== Issue year distribution (all bonds) ===")
result = rest.search(
    view="GovCorpInstruments",
    top=0,
    navigators="IssueDate(type:date_histogram,interval:year,order:value_desc)",
)
for nav in result.get("Navigators", {}).get("IssueDate", {}).get("Buckets", [])[:35]:
    print(f"  {nav['Label'][:4]}: {nav['Count']:>12,}")

ld.close_session()
