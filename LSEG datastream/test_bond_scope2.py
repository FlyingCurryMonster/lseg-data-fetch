"""Year distribution - query per year to avoid navigator crash."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import lseg.data as ld
from lseg_rest_api import LSEGRestClient

config_path = os.path.join(os.path.dirname(__file__), "intraday options data", "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)

print("=== Bonds per issue year (all types, all currencies) ===")
total = 0
for year in range(2026, 1979, -1):
    result = rest.search(
        view="GovCorpInstruments",
        filter=f"IssueDate ge {year}-01-01 and IssueDate lt {year+1}-01-01",
        top=0,
    )
    count = result.get("Total", 0)
    total += count
    if count > 0:
        slices_needed = (count // 10000) + 1
        flag = " ***" if count > 10000 else ""
        print(f"  {year}: {count:>10,}  ({slices_needed} queries needed){flag}")

print(f"\n  Total accounted for: {total:>10,}")

# Check if there are bonds with no issue date
result = rest.search(
    view="GovCorpInstruments",
    filter="IssueDate lt 1980-01-01",
    top=0,
)
print(f"  Pre-1980: {result.get('Total', 0):>10,}")

ld.close_session()
