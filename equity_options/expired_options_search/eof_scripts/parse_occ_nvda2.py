"""Parse OCC series-search TSV — debug the parsing and check expiry date range."""
import requests
import io
import re
from datetime import date

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.theocc.com/",
}

r = requests.get(
    "https://marketdata.theocc.com/series-search?symbolType=U&symbol=NVDA",
    headers=headers, timeout=20
)

lines = r.text.splitlines()
# Print first 12 lines to understand structure
print("=== First 12 lines ===")
for i, l in enumerate(lines[:12]):
    print(f"  [{i}] {repr(l)}")

print(f"\n=== Total lines: {len(lines)} ===")

# Print a few data lines
print("\n=== Lines 7-12 (data) ===")
for l in lines[7:12]:
    print(repr(l))

# Count years present
years = {}
for l in lines[7:]:
    parts = l.split("\t")
    if len(parts) >= 4:
        yr = parts[1].strip()
        if yr.isdigit():
            yr = int(yr)
            years[yr] = years.get(yr, 0) + 1

print(f"\n=== Year distribution ===")
for yr in sorted(years):
    print(f"  {yr}: {years[yr]} rows")
