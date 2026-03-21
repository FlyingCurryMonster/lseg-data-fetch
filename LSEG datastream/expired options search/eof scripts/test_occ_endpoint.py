"""Test OCC series-search endpoint — check availability and whether expired series are included."""
import requests
import io
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.theocc.com/",
}

for base in ["https://marketdata.theocc.com", "https://www.theocc.com/webapps"]:
    url = f"{base}/series-search?symbolType=U&symbol=NVDA"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"\n{url}")
        print(f"  Status: {r.status_code}  len={len(r.content)}")
        if r.status_code == 200:
            print(r.text[:1000])
    except Exception as e:
        print(f"  ERROR: {e}")
