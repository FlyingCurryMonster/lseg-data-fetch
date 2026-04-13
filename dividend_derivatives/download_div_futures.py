"""
Download daily pricing data for all dividend futures in the instrument master.

Reads: instrument_master_futures.csv
Output: futures_daily_prices.csv, failed_rics_futures.csv (if any failures)

Fields: all available (no fields param — API returns everything it has)
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
if "ProductGroup" in master.columns:
    index = master[master["ProductGroup"] == "INDEX"]
    for product, count in index.groupby("product").size().items():
        print(f"  {product}: {count}")
    for group in ["SSDF", "SSF"]:
        group_rows = master[master["ProductGroup"] == group]
        if not group_rows.empty:
            print(f"  {group}: {len(group_rows)} contracts across {group_rows['product'].nunique()} products")
else:
    for product, count in master.groupby("product").size().items():
        print(f"  {product}: {count}")

end = datetime.now().strftime("%Y-%m-%d")
start = "2005-01-01"

all_data = []
failed_rics = []

BATCH_SIZE = 3
for i in range(0, len(all_rics), BATCH_SIZE):
    batch = all_rics[i:i + BATCH_SIZE]
    batch_num = i // BATCH_SIZE + 1
    total_batches = (len(all_rics) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nBatch {batch_num}/{total_batches}: {batch}")

    try:
        data = ld.get_history(
            universe=batch,
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
                    else:
                        print(f"  WARNING: {ric} not in response columns")
                        failed_rics.append(ric)
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
            failed_rics.extend(batch)
    
    except Exception as e:
        print(f"  ERROR: {e}")
        failed_rics.extend(batch)

    time.sleep(5)

# Combine all data
if all_data:
    result = pd.concat(all_data, ignore_index=True)
    result = result.merge(master[["RIC", "product", "status"]], on="RIC", how="left")
    result = result.sort_values(["product", "RIC", "date"]).reset_index(drop=True)

    print("\n" + "=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"Total rows: {len(result)}")
    print(f"Total columns: {len(result.columns)}")
    print(f"Columns: {list(result.columns)}")
    print(f"Unique RICs with data: {result['RIC'].nunique()}")
    print(f"\nBy product:")
    result["date"] = pd.to_datetime(result["date"])
    result_with_group = result.merge(master[["RIC", "ProductGroup"]].drop_duplicates(), on="RIC", how="left") if "ProductGroup" in master.columns else result
    bulk_products = set()
    if "ProductGroup" in result_with_group.columns:
        for group in ["SSDF", "SSF"]:
            bulk_products |= set(result_with_group[result_with_group["ProductGroup"] == group]["product"].unique())
    for product in sorted(result["product"].unique()):
        if product in bulk_products:
            continue
        subset = result[result["product"] == product]
        if not subset.empty:
            print(f"  {product}: {subset['RIC'].nunique()} RICs, {len(subset)} rows, "
                  f"dates: {subset['date'].min()} to {subset['date'].max()}")
    if "ProductGroup" in result_with_group.columns:
        for group in ["SSDF", "SSF"]:
            g = result_with_group[result_with_group["ProductGroup"] == group]
            if not g.empty:
                print(f"  {group}: {g['product'].nunique()} products, {g['RIC'].nunique()} RICs, {len(g)} rows")

    result.to_csv("futures_daily_prices.csv", index=False)
    print(f"\nSaved to futures_daily_prices.csv")
else:
    print("\nNo data downloaded!")

# Handle failed RICs
if failed_rics:
    failed_df = pd.DataFrame({"RIC": failed_rics})
    failed_df = failed_df.merge(master[["RIC", "product", "status"]], on="RIC", how="left")
    failed_df.to_csv("failed_rics_futures.csv", index=False)
    print(f"\nFailed RICs ({len(failed_rics)}): {failed_rics}")
    print("Saved to failed_rics_futures.csv")
else:
    print("\nNo failed RICs!")

ld.close_session()
os.remove(config_path)
