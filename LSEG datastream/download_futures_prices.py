"""
Download daily pricing data for all dividend futures in the instrument master.

Reads: instrument_master_futures.csv
Output: futures_daily_prices.csv

Fields: TRDPRC_1 (last), HIGH_1, LOW_1, OPEN_PRC, SETTLE, ACVOL_UNS
History: as far back as API allows, up to today
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

# Load instrument master
master = pd.read_csv("instrument_master_futures.csv")
all_rics = master["RIC"].tolist()
print(f"Total futures to download: {len(all_rics)}")
print(f"  SDA: {(master['product'] == 'SDA').sum()}")
print(f"  SDI: {(master['product'] == 'SDI').sum()}")
print(f"  FEXD: {(master['product'] == 'FEXD').sum()}")

end = datetime.now().strftime("%Y-%m-%d")
start = "2005-01-01"  # go as far back as possible

fields = ["TRDPRC_1", "HIGH_1", "LOW_1", "OPEN_PRC", "SETTLE", "ACVOL_UNS"]

# Download in batches — API may have limits on number of RICs per request
BATCH_SIZE = 10
all_data = []
failed_rics = []

for i in range(0, len(all_rics), BATCH_SIZE):
    batch = all_rics[i:i + BATCH_SIZE]
    batch_num = i // BATCH_SIZE + 1
    total_batches = (len(all_rics) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nBatch {batch_num}/{total_batches}: {batch}")

    try:
        data = ld.get_history(
            universe=batch,
            fields=fields,
            start=start,
            end=end
        )

        if data is not None and not data.empty:
            # get_history returns MultiIndex columns (RIC, field) when multiple RICs
            # Reshape to long format: date, ric, field1, field2, ...
            if isinstance(data.columns, pd.MultiIndex):
                # Stack RICs into rows
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
                # Single RIC returns flat columns
                data["RIC"] = batch[0]
                data.index.name = "date"
                all_data.append(data.reset_index())
                print(f"  Got {len(data)} rows for {batch[0]}")
        else:
            print(f"  No data returned")
            failed_rics.extend(batch)

    except Exception as e:
        print(f"  ERROR: {e}")
        failed_rics.extend(batch)

    time.sleep(0.5)

# Combine all data
if all_data:
    result = pd.concat(all_data, ignore_index=True)

    # Merge product info from master
    result = result.merge(master[["RIC", "product", "status"]], on="RIC", how="left")

    # Sort by product, RIC, date
    result = result.sort_values(["product", "RIC", "date"]).reset_index(drop=True)

    # Summary stats
    print("\n" + "=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"Total rows: {len(result)}")
    print(f"Unique RICs with data: {result['RIC'].nunique()}")
    print(f"\nBy product:")
    for product in ["SDA", "SDI", "FEXD"]:
        subset = result[result["product"] == product]
        if not subset.empty:
            print(f"  {product}: {subset['RIC'].nunique()} RICs, {len(subset)} rows, "
                  f"dates: {subset['date'].min()} to {subset['date'].max()}")

    print(f"\nFailed RICs ({len(failed_rics)}): {failed_rics}")

    result.to_csv("futures_daily_prices.csv", index=False)
    print(f"\nSaved to futures_daily_prices.csv")
else:
    print("\nNo data downloaded!")

# Cleanup
ld.close_session()
os.remove(config_path)
