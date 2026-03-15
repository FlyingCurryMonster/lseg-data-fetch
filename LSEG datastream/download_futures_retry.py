"""
Retry failed FEXD futures from the first download.
Append results to futures_daily_prices.csv
"""

import json
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import os
import time

load_dotenv()

config = {
    "sessions": {
        "default": "platform.rdp",
        "platform": {
            "rdp": {
                "app-key": os.getenv("DSWS_APPKEY"),
                "username": os.getenv("DSWS_USERNAME"),
                "password": os.getenv("DSWS_PASSWORD"),
                "signon_control": True
            }
        }
    }
}
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lseg-data.config.json")
with open(config_path, "w") as f:
    json.dump(config, f, indent=4)

ld.open_session(config_name=config_path)

# Failed RICs from first run
failed_rics = [
    'FEXDH26', 'FEXDH27', 'FEXDM26', 'FEXDM27', 'FEXDU26',
    'FEXDZ0^2', 'FEXDZ25^2', 'FEXDZ26', 'FEXDZ27', 'FEXDZ28',
    'FEXDZ29', 'FEXDZ2^2', 'FEXDZ33', 'FEXDZ34', 'FEXDZ35'
]

end = datetime.now().strftime("%Y-%m-%d")
start = "2005-01-01"
fields = ["TRDPRC_1", "HIGH_1", "LOW_1", "OPEN_PRC", "SETTLE", "ACVOL_UNS"]

master = pd.read_csv("instrument_master_futures.csv")

all_data = []
still_failed = []

# Try in smaller batches with longer delays to avoid rate limits
BATCH_SIZE = 3
for i in range(0, len(failed_rics), BATCH_SIZE):
    batch = failed_rics[i:i + BATCH_SIZE]
    print(f"\nRetrying: {batch}")

    try:
        data = ld.get_history(
            universe=batch,
            fields=fields,
            start=start,
            end=end
        )

        if data is not None and not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                frames = []
                for ric in batch:
                    if ric in data.columns.get_level_values(0):
                        ric_data = data[ric].copy()
                        ric_data["RIC"] = ric
                        ric_data.index.name = "date"
                        frames.append(ric_data.reset_index())
                if frames:
                    batch_df = pd.concat(frames, ignore_index=True)
                    all_data.append(batch_df)
                    print(f"  Got {len(batch_df)} rows across {len(frames)} RICs")
            else:
                data["RIC"] = batch[0]
                data.index.name = "date"
                all_data.append(data.reset_index())
                print(f"  Got {len(data)} rows for {batch[0]}")
        else:
            print(f"  No data returned")
            still_failed.extend(batch)

    except Exception as e:
        print(f"  ERROR: {e}")
        still_failed.extend(batch)

    time.sleep(5)  # longer delay to avoid rate limiting

if all_data:
    new_data = pd.concat(all_data, ignore_index=True)
    new_data = new_data.merge(master[["RIC", "product", "status"]], on="RIC", how="left")

    # Load existing data and append
    existing = pd.read_csv("futures_daily_prices.csv")
    combined = pd.concat([existing, new_data], ignore_index=True)
    combined = combined.drop_duplicates(subset=["RIC", "date"])
    combined = combined.sort_values(["product", "RIC", "date"]).reset_index(drop=True)

    print(f"\n=== RESULT ===")
    print(f"New rows: {len(new_data)}")
    print(f"Total rows after merge: {len(combined)}")
    print(f"Unique RICs: {combined['RIC'].nunique()}")
    print(f"\nBy product:")
    combined["date"] = pd.to_datetime(combined["date"])
    for product in ["SDA", "SDI", "FEXD"]:
        subset = combined[combined["product"] == product]
        if not subset.empty:
            print(f"  {product}: {subset['RIC'].nunique()} RICs, {len(subset)} rows, "
                  f"dates: {subset['date'].min()} to {subset['date'].max()}")

    print(f"\nStill failed ({len(still_failed)}): {still_failed}")

    combined.to_csv("futures_daily_prices.csv", index=False)
    print(f"\nSaved to futures_daily_prices.csv")
else:
    print(f"\nAll retries failed: {still_failed}")

ld.close_session()
os.remove(config_path)
