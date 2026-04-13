"""
Build a US equity security master snapshot plus a RIC-history sidecar.

Input contract:
  A CSV with one row per current CRSP security and at least:
    - permno
    - cusip or cusip8

Optional input columns such as ticker, comnam, ncusip, and permco are preserved
in the unmatched output when present, but are not required.

Outputs (written to this directory by default):
  - us_lseg_equity_universe_raw.csv
  - us_security_master_snapshot.csv
  - us_security_master_ric_history.csv
  - us_security_master_unmatched.csv

Workflow:
  1. Pull the current US ordinary-share universe from LSEG.
  2. Join CRSP rows to LSEG on cusip8 = lseg_cusip9[:8].
  3. Build a current snapshot keyed by PERMNO.
  4. Fetch primary-RIC history by ISIN and write it as a separate sidecar.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone

import lseg.data as ld
import pandas as pd
from dotenv import load_dotenv

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.lseg_rest_api import LSEGRestClient

load_dotenv()


SEARCH_FIELDS = (
    "RIC,Cusip,Isin,PermID,TickerSymbol,CommonName,"
    "ExchangeCode,ExchangeName,AssetCategory,AssetState,CountryCode"
)
SEARCH_FILTER = "AssetCategory eq 'Ordinary Shares' and CountryCode eq 'USA'"
DEFAULT_TOP = 10000
DEFAULT_BATCH_SLEEP = 0.2

RAW_UNIVERSE_CSV = "us_lseg_equity_universe_raw.csv"
SNAPSHOT_CSV = "us_security_master_snapshot.csv"
HISTORY_CSV = "us_security_master_ric_history.csv"
UNMATCHED_CSV = "us_security_master_unmatched.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a US security-master snapshot from CRSP rows + LSEG"
    )
    parser.add_argument(
        "--crsp-input",
        required=True,
        help="CSV file with at least permno and cusip/cusip8 columns",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.dirname(os.path.abspath(__file__)),
        help="Directory for output CSVs (default: security_master/)",
    )
    parser.add_argument(
        "--search-top",
        type=int,
        default=DEFAULT_TOP,
        help="Max LSEG rows to request for the US equity universe",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=None,
        help="Limit number of matched ISINs for history lookup (useful for smoke tests)",
    )
    parser.add_argument(
        "--batch-sleep",
        type=float,
        default=DEFAULT_BATCH_SLEEP,
        help="Sleep in seconds between RIC-history requests",
    )
    return parser.parse_args()


def open_session(config_dir: str):
    config = {
        "sessions": {
            "default": "platform.rdp",
            "platform": {
                "rdp": {
                    "app-key": os.getenv("DSWS_APPKEY"),
                    "username": os.getenv("DSWS_USERNAME"),
                    "password": os.getenv("DSWS_PASSWORD"),
                    "signon_control": True,
                }
            },
        }
    }
    config_path = os.path.join(config_dir, "lseg-data.config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    session = ld.open_session(config_name=config_path)
    return session, config_path


def normalize_cusip8(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", str(value)).upper()
    if not cleaned:
        return None
    return cleaned[:8]


def require_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def load_crsp_snapshot(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    if "permno" not in df.columns:
        raise ValueError("CRSP input must contain a 'permno' column")

    if "cusip8" in df.columns:
        cusip_col = "cusip8"
    elif "cusip" in df.columns:
        cusip_col = "cusip"
    elif "ncusip" in df.columns:
        cusip_col = "ncusip"
    else:
        raise ValueError("CRSP input must contain one of: cusip8, cusip, ncusip")

    df = df.copy()
    df["permno"] = df["permno"].astype(str).str.strip()
    df["crsp_cusip8"] = df[cusip_col].apply(normalize_cusip8)
    df = df[df["permno"] != ""].copy()

    if df["permno"].duplicated().any():
        dupes = df.loc[df["permno"].duplicated(), "permno"].unique().tolist()
        raise ValueError(
            f"CRSP input contains duplicate permno rows; expected a static snapshot. "
            f"Examples: {dupes[:10]}"
        )

    return df


def fetch_lseg_us_equity_universe(rest: LSEGRestClient, top: int) -> pd.DataFrame:
    result = rest.search(
        query="",
        filter=SEARCH_FILTER,
        select=SEARCH_FIELDS,
        top=top,
        view="EquityQuotes",
    )
    total = result.get("Total", 0)
    hits = result.get("Hits", [])

    if total > top:
        raise RuntimeError(
            f"LSEG search returned Total={total}, exceeding top={top}. "
            "Increase --search-top or implement segmented fetch before proceeding."
        )

    if not hits:
        raise RuntimeError("LSEG search returned no US ordinary-share rows")

    df = pd.DataFrame(hits)
    df["lseg_cusip9"] = df.get("Cusip", pd.Series(dtype=str)).astype(str).str.strip()
    df["lseg_cusip8"] = df["lseg_cusip9"].apply(normalize_cusip8)
    return df


def first_non_null(series: pd.Series):
    values = series.dropna()
    values = values[values.astype(str).str.strip() != ""]
    if values.empty:
        return None
    return values.iloc[0]


def collapse_lseg_candidates(lseg_df: pd.DataFrame) -> pd.DataFrame:
    work = lseg_df[lseg_df["lseg_cusip8"].notna()].copy()
    grouped = (
        work.groupby("lseg_cusip8", dropna=False)
        .agg(
            {
                "lseg_cusip9": first_non_null,
                "RIC": first_non_null,
                "Isin": first_non_null,
                "PermID": first_non_null,
                "TickerSymbol": first_non_null,
                "CommonName": first_non_null,
                "ExchangeCode": first_non_null,
                "ExchangeName": first_non_null,
                "CountryCode": first_non_null,
                "AssetState": first_non_null,
            }
        )
        .reset_index()
    )
    return grouped


def build_snapshot(crsp_df: pd.DataFrame, lseg_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    lseg_compact = collapse_lseg_candidates(lseg_df)
    matched = crsp_df.merge(
        lseg_compact,
        left_on="crsp_cusip8",
        right_on="lseg_cusip8",
        how="left",
        suffixes=("", "_lseg"),
    )

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    matched["snapshot_date"] = snapshot_date
    matched["primary_ric"] = matched["RIC"]
    matched["lseg_cusip9"] = matched["lseg_cusip9"].replace({"nan": None})

    snapshot = pd.DataFrame(
        {
            "permno": matched["permno"],
            "crsp_cusip8": matched["crsp_cusip8"],
            "lseg_cusip9": matched["lseg_cusip9"],
            "isin": matched.get("Isin"),
            "permid": matched.get("PermID"),
            "primary_ric": matched["primary_ric"],
            "ticker": matched.get("TickerSymbol"),
            "common_name": matched.get("CommonName"),
            "exchange_code": matched.get("ExchangeCode"),
            "exchange_name": matched.get("ExchangeName"),
            "country_code": matched.get("CountryCode"),
            "asset_state": matched.get("AssetState"),
            "snapshot_date": matched["snapshot_date"],
        }
    )

    unmatched = matched[matched["primary_ric"].isna()].copy()
    unmatched["issue"] = "no_lseg_match_on_cusip8"
    return snapshot, unmatched


def fetch_primary_ric_history(
    rest: LSEGRestClient,
    snapshot_df: pd.DataFrame,
    limit: int | None = None,
    batch_sleep: float = DEFAULT_BATCH_SLEEP,
) -> pd.DataFrame:
    work = snapshot_df.dropna(subset=["isin"]).copy()
    work = work[work["isin"].astype(str).str.strip() != ""]
    work = work[["permno", "isin", "snapshot_date"]].drop_duplicates()

    if limit is not None:
        work = work.head(limit)

    rows = []
    total = len(work)
    for idx, record in enumerate(work.itertuples(index=False), start=1):
        print(f"[history] {idx}/{total}  permno={record.permno}  isin={record.isin}", flush=True)
        df = rest.symbology_lookup_df(
            identifiers=[record.isin],
            from_types=["ISIN"],
            route="FindPrimaryRIC",
            show_history=True,
        )
        if df.empty:
            continue
        df = df.rename(columns={"value": "ric"})
        df["permno"] = record.permno
        df["isin"] = record.isin
        df["history_source"] = "FindPrimaryRIC_showHistory"
        df["snapshot_date"] = record.snapshot_date
        rows.append(df[["permno", "isin", "ric", "effective_from", "effective_to", "history_source", "snapshot_date"]])
        time.sleep(batch_sleep)

    if not rows:
        return pd.DataFrame(
            columns=[
                "permno",
                "isin",
                "ric",
                "effective_from",
                "effective_to",
                "history_source",
                "snapshot_date",
            ]
        )
    return pd.concat(rows, ignore_index=True)


def main() -> None:
    args = parse_args()
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 80)
    print("US SECURITY MASTER BUILD")
    print("=" * 80)
    print(f"CRSP input:   {args.crsp_input}")
    print(f"Output dir:   {output_dir}")
    print(f"Search top:   {args.search_top}")
    if args.history_limit is not None:
        print(f"History cap:  {args.history_limit}")
    print()

    crsp_df = load_crsp_snapshot(args.crsp_input)
    print(f"Loaded CRSP rows: {len(crsp_df):,}")
    print(f"Rows with cusip8: {(crsp_df['crsp_cusip8'].notna()).sum():,}")

    session, config_path = open_session(output_dir)
    rest = LSEGRestClient(session)

    try:
        lseg_df = fetch_lseg_us_equity_universe(rest, args.search_top)
        print(f"LSEG US ordinary-share rows: {len(lseg_df):,}")
        lseg_df.to_csv(os.path.join(output_dir, RAW_UNIVERSE_CSV), index=False)

        snapshot_df, unmatched_df = build_snapshot(crsp_df, lseg_df)
        matched_rows = snapshot_df["primary_ric"].notna().sum()
        print(f"Matched rows:   {matched_rows:,}")
        print(f"Unmatched rows: {len(unmatched_df):,}")

        snapshot_df.to_csv(os.path.join(output_dir, SNAPSHOT_CSV), index=False)
        unmatched_df.to_csv(os.path.join(output_dir, UNMATCHED_CSV), index=False)

        history_df = fetch_primary_ric_history(
            rest,
            snapshot_df[snapshot_df["primary_ric"].notna()].copy(),
            limit=args.history_limit,
            batch_sleep=args.batch_sleep,
        )
        history_df.to_csv(os.path.join(output_dir, HISTORY_CSV), index=False)

    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)

    print()
    print(f"Wrote {RAW_UNIVERSE_CSV}")
    print(f"Wrote {SNAPSHOT_CSV}")
    print(f"Wrote {UNMATCHED_CSV}")
    print(f"Wrote {HISTORY_CSV}")
    print("Done.")


if __name__ == "__main__":
    main()
