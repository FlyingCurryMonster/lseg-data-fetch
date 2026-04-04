"""Parse OCC series-search — handle extra tab, find all expiry dates."""
import requests
from datetime import date
from collections import Counter

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.theocc.com/",
}

r = requests.get(
    "https://marketdata.theocc.com/series-search?symbolType=U&symbol=NVDA",
    headers=headers, timeout=20
)

lines = r.text.splitlines()
today = date.today()

records = []
for line in lines[7:]:
    parts = [p.strip() for p in line.split("\t")]
    # Remove empty fields caused by double-tab after ticker
    parts = [p for p in parts if p != ""]
    # Expected: ProductSymbol, year, Month, Day, Integer, Dec, C/P, Call, Put, PositionLimit
    if len(parts) < 7:
        continue
    try:
        yr  = int(parts[1])
        mo  = int(parts[2])
        day = int(parts[3])
        expiry = date(yr, mo, day)
        records.append(expiry)
    except (ValueError, IndexError):
        continue

expiries = sorted(set(records))
print(f"Total data rows parsed: {len(records)}")
print(f"Unique expiry dates: {len(expiries)}")
print(f"\nEarliest expiry: {expiries[0] if expiries else 'N/A'}")
print(f"Latest expiry:   {expiries[-1] if expiries else 'N/A'}")

# Expired vs active
expired = [e for e in expiries if e < today]
active  = [e for e in expiries if e >= today]
print(f"\nExpired: {len(expired)} dates, Active: {len(active)} dates")

# Monthly breakdown
from collections import defaultdict
monthly = defaultdict(int)
for e in records:
    monthly[(e.year, e.month)] += 1

print("\nAll expiry months (year, month): row count")
for ym in sorted(monthly):
    yr, mo = ym
    exp_flag = " ← EXPIRED" if date(yr, mo, 28) < today else ""
    print(f"  {yr}-{mo:02d}: {monthly[ym]:,} rows{exp_flag}")
