"""
Download daily pricing data for dividend options.

Usage:
  python download_options_prices.py           # downloads active options
  python download_options_prices.py expired   # downloads expired options

Active mode:
  Reads: instrument_master_options.csv
  Output: options_daily_prices.csv, failed_rics_options.csv

Expired mode:
  Reads: instrument_master_expired_options.csv
  Output: expired_options_daily_prices.csv, failed_rics_expired_options.csv
  Skips RICs already present in expired_options_daily_prices.csv

Retry logic:
  - If the failed_rics file exists and is non-empty, retry only those RICs
    and append results to the existing prices CSV.
  - Otherwise, download all RICs from the instrument master (fresh run).
  - On completion: if new failures, overwrite failed_rics file.
    If none, write empty failed_rics file so next run does a full download.

Fields: all available (no fields param — API returns everything it has)
History: as far back as API allows, up to today
"""

import json
import sys
import lseg.data as ld
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import os
import time

load_dotenv()

# Determine mode from command line
mode = sys.argv[1] if len(sys.argv) > 1 else "active"
if mode == "expired":
    master_file = "instrument_master_expired_options.csv"
    prices_file = "expired_options_daily_prices.csv"
    failed_path = "failed_rics_expired_options.csv"
    merge_cols = ["RIC", "product", "strike", "cp_flag"]
    print("MODE: expired options")
elif mode == "active":
    master_file = "instrument_master_options.csv"
    prices_file = "options_daily_prices.csv"
    failed_path = "failed_rics_options.csv"
    merge_cols = ["RIC", "product", "strike", "expiry_date", "cp_flag"]
    print("MODE: active options")
else:
    print(f"Unknown mode: {mode}. Use 'active' or 'expired'.")
    sys.exit(1)

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
master = pd.read_csv(master_file)

# Check for prior failures to retry
retry_mode = False
if os.path.exists(failed_path):
    prior_failed = pd.read_csv(failed_path)
    if len(prior_failed) > 0:
        all_rics = prior_failed["RIC"].tolist()
        retry_mode = True
        print(f"RETRY MODE: {len(all_rics)} failed RICs from previous run")
    else:
        all_rics = master["RIC"].tolist()
        print(f"No prior failures — full download")
else:
    all_rics = master["RIC"].tolist()
    print(f"No failed_rics file — full download")

# Skip RICs already downloaded (only on fresh runs, not retries)
if not retry_mode and os.path.exists(prices_file):
    existing_rics = set(pd.read_csv(prices_file, usecols=["RIC"])["RIC"].unique())
    before = len(all_rics)
    all_rics = [r for r in all_rics if r not in existing_rics]
    skipped = before - len(all_rics)
    if skipped > 0:
        print(f"Skipping {skipped} RICs already in {prices_file}")

print(f"Total options to download: {len(all_rics)}")
for p in master[master["RIC"].isin(all_rics)]["product"].unique():
    count = master[(master["RIC"].isin(all_rics)) & (master["product"] == p)].shape[0]
    print(f"  {p}: {count}")

end = datetime.now().strftime("%Y-%m-%d")
start = "2005-01-01"

all_data = []
failed_rics = []

BATCH_SIZE = 3
total_batches = (len(all_rics) + BATCH_SIZE - 1) // BATCH_SIZE

for i in range(0, len(all_rics), BATCH_SIZE):
    batch = all_rics[i:i + BATCH_SIZE]
    batch_num = i // BATCH_SIZE + 1
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
    new_data = pd.concat(all_data, ignore_index=True)
    # Only merge columns that exist in the master
    available_merge_cols = [c for c in merge_cols if c in master.columns]
    new_data = new_data.merge(
        master[available_merge_cols],
        on="RIC", how="left"
    )

    # Append to existing data if prices file exists
    if os.path.exists(prices_file):
        existing = pd.read_csv(prices_file)
        result = pd.concat([existing, new_data], ignore_index=True)
        print(f"\nAppended {len(new_data)} new rows to {len(existing)} existing rows")
    else:
        result = new_data

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
    for product in result["product"].unique():
        subset = result[result["product"] == product]
        if not subset.empty:
            print(f"  {product}: {subset['RIC'].nunique()} RICs, {len(subset)} rows, "
                  f"dates: {subset['date'].min()} to {subset['date'].max()}")

    result.to_csv(prices_file, index=False)
    print(f"\nSaved to {prices_file}")
else:
    print("\nNo data downloaded!")

# Handle failed RICs — always write the file (empty if no failures)
if failed_rics:
    failed_df = pd.DataFrame({"RIC": failed_rics})
    available_merge_cols = [c for c in merge_cols if c in master.columns]
    failed_df = failed_df.merge(
        master[available_merge_cols],
        on="RIC", how="left"
    )
    failed_df.to_csv(failed_path, index=False)
    print(f"\nFailed RICs ({len(failed_rics)}): {failed_rics}")
    print(f"Saved to {failed_path}")
else:
    # Write empty CSV so next run knows there are no failures
    pd.DataFrame(columns=["RIC"]).to_csv(failed_path, index=False)
    print("\nNo failed RICs!")

ld.close_session()
if os.path.exists(config_path):
    os.remove(config_path)
