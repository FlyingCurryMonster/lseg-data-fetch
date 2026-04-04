"""Test run: check if 1-day windows fit under 10K for recent dense periods."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import date, timedelta
import lseg.data as ld
from lseg_rest_api import LSEGRestClient
import download_bond_master as bm

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "intraday options data", "lseg-data.config.json")
session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)

# Check daily counts for a busy week in Jan 2025
print("=== Daily counts for Jan 6-17, 2025 ===")
cursor = date(2025, 1, 6)
for i in range(12):
    d = cursor + timedelta(days=i)
    d_next = d + timedelta(days=1)
    count = bm.search_count(rest, bm.date_filter(d, d_next))
    flag = " ***" if count > 10000 else ""
    print(f"  {d} ({d.strftime('%a')}): {count:>8,}{flag}")

# Also check the busiest month (Mar 2025) for a few days
print("\n=== Daily counts for Mar 3-14, 2025 ===")
cursor = date(2025, 3, 3)
for i in range(12):
    d = cursor + timedelta(days=i)
    d_next = d + timedelta(days=1)
    count = bm.search_count(rest, bm.date_filter(d, d_next))
    flag = " ***" if count > 10000 else ""
    print(f"  {d} ({d.strftime('%a')}): {count:>8,}{flag}")

ld.close_session()
