"""Parse full OCC series-search response for NVDA and check expiry date coverage."""
import requests
import io
import pandas as pd
from datetime import date

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.theocc.com/",
}

r = requests.get(
    "https://marketdata.theocc.com/series-search?symbolType=U&symbol=NVDA",
    headers=headers, timeout=20
)
print(f"Status: {r.status_code}  bytes: {len(r.content)}")

raw = r.text
# Skip header lines until we hit the column header row
lines = raw.splitlines()
data_start = None
for i, line in enumerate(lines):
    if "ProductSymbol" in line:
        data_start = i
        break

print(f"Header row at line {data_start}")
print(f"Total lines: {len(lines)}")

# Parse as TSV from the header row
tsv = "\n".join(lines[data_start:])
df = pd.read_csv(io.StringIO(tsv), sep="\t")
df.columns = [c.strip() for c in df.columns]
print(f"\nColumns: {list(df.columns)}")
print(f"Total rows: {len(df)}")

# Build expiry date
df = df.dropna(subset=["year", "Month", "Day"])
df["year"]  = df["year"].astype(int)
df["Month"] = df["Month"].astype(int)
df["Day"]   = df["Day"].astype(int)
df["expiry"] = pd.to_datetime(df[["year", "Month", "Day"]].rename(
    columns={"year": "year", "Month": "month", "Day": "day"}
))

today = date.today()
df["is_expired"] = df["expiry"].dt.date < today

print(f"\nExpiry date range: {df['expiry'].min().date()} → {df['expiry'].max().date()}")
print(f"Expired contracts:  {df['is_expired'].sum():,}")
print(f"Active contracts:   {(~df['is_expired']).sum():,}")

# Key question: how far back do expired ones go?
expired = df[df["is_expired"]].copy()
print(f"\nExpired expiry breakdown:")
print(expired.groupby(expired["expiry"].dt.to_period("M")).size().to_string())

# Check if we have our target window: Aug 2025 → Mar 2026
target = expired[(expired["expiry"].dt.date >= date(2025, 8, 1)) &
                 (expired["expiry"].dt.date <= date(2026, 3, 21))]
print(f"\nTarget window (Aug 2025 → Mar 2026): {len(target):,} expired contracts")
print(target.groupby(target["expiry"].dt.to_period("M")).size().to_string())

# Save full data
out = "/home/rakin/wrds-data/LSEG datastream/expired options search/nvda_occ_series.csv"
df.to_csv(out, index=False)
print(f"\nSaved to {out}")
