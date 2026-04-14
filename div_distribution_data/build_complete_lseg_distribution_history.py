"""
Build complete-universe LSEG dividend-distribution history and a CRSP comparison layer.

Inputs:
  - div_distribution_data/complete_stock_universe.csv
  - div_distribution_data/us_crsp_distribution_events.csv

Outputs:
  - div_distribution_data/complete_lseg_distribution_events.csv
  - div_distribution_data/complete_lseg_distribution_coverage_summary.csv
  - div_distribution_data/complete_lseg_distribution_access_exceptions.csv
  - div_distribution_data/crsp_vs_lseg_distribution_compare.csv
  - div_distribution_data/crsp_vs_lseg_distribution_coverage_summary.csv
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date

import lseg.data as ld
import pandas as pd
from dotenv import load_dotenv


DIVIDEND_FIELDS = [
    "TR.DivExDate",
    "TR.DivPayDate",
    "TR.DivRecordDate",
    "TR.DivUnadjustedGross",
    "TR.DivAdjustedGross",
    "TR.DivType",
    "TR.DivCurrency",
    "TR.DivAnnouncedDate",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build LSEG dividend history for the complete stock universe")
    parser.add_argument(
        "--stock-universe",
        default=os.path.join("div_distribution_data", "complete_stock_universe.csv"),
        help="Complete stock universe manifest",
    )
    parser.add_argument(
        "--crsp-events",
        default=os.path.join("div_distribution_data", "us_crsp_distribution_events.csv"),
        help="CRSP canonical event file for comparison",
    )
    parser.add_argument(
        "--output-dir",
        default="div_distribution_data",
        help="Directory for output files",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Preferred LSEG batch size",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional LSEG start date override (YYYY-MM-DD). Defaults to CRSP min ex_date.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional LSEG end date override (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


def open_session(config_dir: str):
    load_dotenv()
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


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col]) or pd.api.types.is_object_dtype(out[col]):
            out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def join_unique(series: pd.Series) -> str:
    vals = sorted({str(v).strip() for v in series.dropna() if str(v).strip()})
    return "|".join(vals)


def read_stock_universe(path: str) -> pd.DataFrame:
    df = normalize_strings(pd.read_csv(path, dtype=str))
    if df["share_isin"].duplicated().any():
        raise ValueError("Complete stock universe must be unique on share_isin")
    return df


def read_crsp_cash_events(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df = normalize_strings(df)
    df = df[df["has_cash_distribution"].str.lower() == "true"].copy()
    df["dividend_amount_num"] = pd.to_numeric(df["dividend_amount"], errors="coerce")
    return df


def fetch_batch(keys: list[str]) -> pd.DataFrame:
    data = ld.get_data(
        universe=keys,
        fields=DIVIDEND_FIELDS,
        parameters={"SDate": fetch_batch.start_date, "EDate": fetch_batch.end_date},
    )
    if data is None or data.empty:
        return pd.DataFrame()
    return data.copy()


def fetch_lseg_dividend_history(universe: pd.DataFrame, batch_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    keys_df = universe[["share_isin", "lseg_query_key", "lseg_query_key_type"]].copy()
    keys_df = keys_df[keys_df["lseg_query_key"] != ""].copy()
    keys = keys_df["lseg_query_key"].tolist()

    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    total = len(keys)
    total_batches = (total + batch_size - 1) // batch_size if total else 0

    for start in range(0, total, batch_size):
        batch = keys[start:start + batch_size]
        batch_no = start // batch_size + 1
        print(f"[complete lseg dividend history] batch {batch_no}/{total_batches} ({len(batch)} keys)", flush=True)
        try:
            data = fetch_batch(batch)
            if not data.empty:
                frames.append(data)
            continue
        except Exception as batch_error:
            print(f"  batch fallback to one-by-one after error: {batch_error}", flush=True)

        for key in batch:
            try:
                data = fetch_batch([key])
                if not data.empty:
                    frames.append(data)
            except Exception as key_error:
                meta = keys_df[keys_df["lseg_query_key"] == key].iloc[0]
                failures.append(
                    {
                        "share_isin": meta["share_isin"],
                        "lseg_query_key": meta["lseg_query_key"],
                        "lseg_query_key_type": meta["lseg_query_key_type"],
                        "error": str(key_error),
                    }
                )

    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    failures_df = pd.DataFrame(failures)
    return raw, failures_df


def clean_lseg_dividend_history(raw: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    rename_map = {
        "Instrument": "resolved_lseg_instrument",
        "Dividend Ex Date": "ex_date",
        "Dividend Pay Date": "pay_date",
        "Dividend Record Date": "record_date",
        "Gross Dividend Amount": "gross_dividend_amount",
        "Adjusted Gross Dividend Amount": "adjusted_gross_dividend_amount",
        "Dividend Type": "lseg_dividend_type",
        "Dividend Currency": "dividend_currency",
        "Dividend Announced Date": "announce_date",
    }
    df = df.rename(columns=rename_map)
    for col in [
        "resolved_lseg_instrument",
        "ex_date",
        "pay_date",
        "record_date",
        "gross_dividend_amount",
        "adjusted_gross_dividend_amount",
        "lseg_dividend_type",
        "dividend_currency",
        "announce_date",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    for col in ["ex_date", "pay_date", "record_date", "announce_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
        df[col] = df[col].fillna("")

    df["gross_dividend_amount"] = pd.to_numeric(df["gross_dividend_amount"], errors="coerce")
    df["adjusted_gross_dividend_amount"] = pd.to_numeric(df["adjusted_gross_dividend_amount"], errors="coerce")

    df = normalize_strings(df)
    df = df[df["ex_date"] != ""].copy()
    ex_dt = pd.to_datetime(df["ex_date"], errors="coerce")
    pay_dt = pd.to_datetime(df["pay_date"], errors="coerce")
    df["lseg_zero_amount_flag"] = (
        df["gross_dividend_amount"].fillna(0).eq(0)
        & df["adjusted_gross_dividend_amount"].fillna(0).eq(0)
    )
    df["lseg_pay_date_before_ex_date_flag"] = pay_dt.notna() & ex_dt.notna() & (pay_dt < ex_dt)
    df["lseg_suspicious_event_flag"] = df["lseg_zero_amount_flag"] | df["lseg_pay_date_before_ex_date_flag"]

    merge_cols = [
        "share_isin",
        "permno",
        "ticker",
        "underlier_name",
        "current_primary_ric",
        "current_permid",
        "lseg_query_key",
        "lseg_query_key_type",
        "query_key_fallback_used",
        "country_code",
        "products",
        "product_groups",
        "reuters_ul_codes",
        "identifier_status",
        "in_crsp_secmaster",
        "in_eurex_underliers",
        "in_eurex_us_underliers",
        "in_eurex_non_us_underliers",
        "universe_category",
    ]
    universe_by_key = universe[merge_cols].copy()

    out = df.merge(universe_by_key, left_on="resolved_lseg_instrument", right_on="lseg_query_key", how="left")
    return out


def aggregate_crsp_for_compare(crsp_cash: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    compare_universe = universe[universe["permno"] != ""].copy()
    join_cols = [
        "permno",
        "share_isin",
        "ticker",
        "underlier_name",
        "current_primary_ric",
        "products",
        "product_groups",
        "universe_category",
    ]
    df = crsp_cash.merge(compare_universe[join_cols], on=["permno", "share_isin", "ticker", "underlier_name", "products", "product_groups", "universe_category"], how="left")
    grouped = (
        df.groupby(
            ["permno", "share_isin", "ticker", "underlier_name", "current_primary_ric", "products", "product_groups", "universe_category", "ex_date"],
            dropna=False,
        )
        .agg(
            crsp_cash_event_count=("ex_date", "size"),
            crsp_dividend_amount=("dividend_amount_num", "sum"),
            crsp_pay_dates=("pay_date", join_unique),
            crsp_dis_types=("dis_type", join_unique),
            crsp_dis_detail_types=("dis_detail_type", join_unique),
        )
        .reset_index()
        .rename(columns={"current_primary_ric": "primary_ric"})
    )
    return grouped


def aggregate_lseg_for_compare(lseg_events: pd.DataFrame) -> pd.DataFrame:
    df = lseg_events[lseg_events["permno"] != ""].copy()
    grouped = (
        df.groupby(
            ["permno", "share_isin", "ticker", "underlier_name", "current_primary_ric", "products", "product_groups", "universe_category", "ex_date"],
            dropna=False,
        )
        .agg(
            lseg_event_count=("ex_date", "size"),
            lseg_gross_dividend_amount=("gross_dividend_amount", "sum"),
            lseg_adjusted_dividend_amount=("adjusted_gross_dividend_amount", "sum"),
            lseg_pay_dates=("pay_date", join_unique),
            lseg_dividend_types=("lseg_dividend_type", join_unique),
            lseg_currencies=("dividend_currency", join_unique),
            lseg_zero_amount_rows=("lseg_zero_amount_flag", "sum"),
            lseg_pay_date_before_ex_date_rows=("lseg_pay_date_before_ex_date_flag", "sum"),
            lseg_suspicious_event_rows=("lseg_suspicious_event_flag", "sum"),
        )
        .reset_index()
        .rename(columns={"current_primary_ric": "primary_ric"})
    )
    return grouped


def build_compare(crsp_agg: pd.DataFrame, lseg_agg: pd.DataFrame) -> pd.DataFrame:
    join_cols = ["permno", "share_isin", "ticker", "underlier_name", "primary_ric", "products", "product_groups", "universe_category", "ex_date"]
    compare = crsp_agg.merge(lseg_agg, on=join_cols, how="outer")
    compare = normalize_strings(compare)
    compare["crsp_cash_event_count"] = pd.to_numeric(compare["crsp_cash_event_count"], errors="coerce").fillna(0).astype(int)
    compare["lseg_event_count"] = pd.to_numeric(compare["lseg_event_count"], errors="coerce").fillna(0).astype(int)
    compare["crsp_dividend_amount"] = pd.to_numeric(compare["crsp_dividend_amount"], errors="coerce")
    compare["lseg_gross_dividend_amount"] = pd.to_numeric(compare["lseg_gross_dividend_amount"], errors="coerce")
    compare["lseg_adjusted_dividend_amount"] = pd.to_numeric(compare["lseg_adjusted_dividend_amount"], errors="coerce")
    compare["lseg_zero_amount_rows"] = pd.to_numeric(compare["lseg_zero_amount_rows"], errors="coerce").fillna(0).astype(int)
    compare["lseg_pay_date_before_ex_date_rows"] = pd.to_numeric(compare["lseg_pay_date_before_ex_date_rows"], errors="coerce").fillna(0).astype(int)
    compare["lseg_suspicious_event_rows"] = pd.to_numeric(compare["lseg_suspicious_event_rows"], errors="coerce").fillna(0).astype(int)

    compare["in_crsp"] = compare["crsp_cash_event_count"] > 0
    compare["in_lseg"] = compare["lseg_event_count"] > 0
    compare["ex_date_status"] = "unclassified"
    compare.loc[compare["in_crsp"] & compare["in_lseg"], "ex_date_status"] = "both"
    compare.loc[compare["in_crsp"] & (~compare["in_lseg"]), "ex_date_status"] = "crsp_only"
    compare.loc[(~compare["in_crsp"]) & compare["in_lseg"], "ex_date_status"] = "lseg_only"

    compare["gross_amount_diff"] = compare["lseg_gross_dividend_amount"] - compare["crsp_dividend_amount"]
    compare["gross_amount_match"] = (
        compare["in_crsp"].astype(bool)
        & compare["in_lseg"].astype(bool)
        & (compare["gross_amount_diff"].abs().fillna(999999) <= 0.0001)
    )
    compare["pay_date_match"] = (
        compare["in_crsp"].astype(bool)
        & compare["in_lseg"].astype(bool)
        & (compare["crsp_pay_dates"] == compare["lseg_pay_dates"])
    )
    compare["matched_on_ex_date_amount_and_pay_date"] = (
        compare["in_crsp"].astype(bool)
        & compare["in_lseg"].astype(bool)
        & compare["gross_amount_match"].astype(bool)
        & compare["pay_date_match"].astype(bool)
    )
    compare["lseg_has_suspicious_event"] = compare["lseg_suspicious_event_rows"] > 0
    return compare.sort_values(["permno", "ex_date", "primary_ric"], kind="stable").reset_index(drop=True)


def build_lseg_summary(universe: pd.DataFrame, lseg_events: pd.DataFrame, failures: pd.DataFrame) -> pd.DataFrame:
    event_share_isin = set(lseg_events["share_isin"].dropna().astype(str).str.strip())
    rows = [
        {"metric": "complete_universe_rows", "value": len(universe)},
        {"metric": "complete_universe_unique_share_isin", "value": universe["share_isin"].nunique()},
        {"metric": "complete_universe_rows_with_permno", "value": int((universe["permno"] != "").sum())},
        {"metric": "complete_universe_rows_without_permno", "value": int((universe["permno"] == "").sum())},
        {"metric": "lseg_event_rows", "value": len(lseg_events)},
        {"metric": "lseg_event_names_with_rows", "value": len(event_share_isin)},
        {"metric": "lseg_names_without_rows", "value": int(len(universe) - len(event_share_isin))},
        {"metric": "lseg_access_exception_rows", "value": len(failures)},
        {"metric": "lseg_zero_amount_rows", "value": int(lseg_events["lseg_zero_amount_flag"].fillna(False).sum())},
        {"metric": "lseg_pay_date_before_ex_date_rows", "value": int(lseg_events["lseg_pay_date_before_ex_date_flag"].fillna(False).sum())},
        {"metric": "lseg_suspicious_event_rows", "value": int(lseg_events["lseg_suspicious_event_flag"].fillna(False).sum())},
    ]
    return pd.DataFrame(rows)


def build_compare_summary(compare: pd.DataFrame) -> pd.DataFrame:
    rows = [
        {"metric": "compare_rows", "value": len(compare)},
        {"metric": "compare_both", "value": int((compare["ex_date_status"] == "both").sum())},
        {"metric": "compare_crsp_only", "value": int((compare["ex_date_status"] == "crsp_only").sum())},
        {"metric": "compare_lseg_only", "value": int((compare["ex_date_status"] == "lseg_only").sum())},
        {"metric": "compare_matched_on_ex_date_amount_and_pay_date", "value": int(compare["matched_on_ex_date_amount_and_pay_date"].fillna(False).sum())},
        {"metric": "compare_rows_with_suspicious_lseg_event", "value": int(compare["lseg_has_suspicious_event"].fillna(False).sum())},
    ]
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    universe = read_stock_universe(args.stock_universe)
    crsp_cash = read_crsp_cash_events(args.crsp_events)
    fetch_batch.start_date = args.start_date or crsp_cash["ex_date"].min()
    fetch_batch.end_date = args.end_date or date.today().isoformat()

    print("=" * 80)
    print("COMPLETE LSEG DIVIDEND HISTORY BUILD")
    print("=" * 80)
    print(f"Stock universe:   {args.stock_universe}")
    print(f"CRSP events:      {args.crsp_events}")
    print(f"Output dir:       {os.path.abspath(args.output_dir)}")
    print(f"Start date:       {fetch_batch.start_date}")
    print(f"End date:         {fetch_batch.end_date}")
    print(f"Universe rows:    {len(universe):,}")
    print()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    session = None
    config_path = None
    try:
        session, config_path = open_session(script_dir)
        raw, failures = fetch_lseg_dividend_history(universe=universe, batch_size=args.batch_size)
    finally:
        if session is not None:
            ld.close_session()
        if config_path and os.path.exists(config_path):
            os.remove(config_path)

    if raw is None:
        raw = pd.DataFrame()
    if failures is None:
        failures = pd.DataFrame(columns=["share_isin", "lseg_query_key", "lseg_query_key_type", "error"])

    lseg_events = clean_lseg_dividend_history(raw=raw, universe=universe) if not raw.empty else pd.DataFrame()
    if lseg_events.empty:
        raise RuntimeError("LSEG returned no dividend-history events for the complete stock universe")

    crsp_agg = aggregate_crsp_for_compare(crsp_cash=crsp_cash, universe=universe)
    lseg_agg = aggregate_lseg_for_compare(lseg_events=lseg_events)
    compare = build_compare(crsp_agg=crsp_agg, lseg_agg=lseg_agg)
    lseg_summary = build_lseg_summary(universe=universe, lseg_events=lseg_events, failures=failures)
    compare_summary = build_compare_summary(compare=compare)

    events_path = os.path.join(args.output_dir, "complete_lseg_distribution_events.csv")
    lseg_summary_path = os.path.join(args.output_dir, "complete_lseg_distribution_coverage_summary.csv")
    failures_path = os.path.join(args.output_dir, "complete_lseg_distribution_access_exceptions.csv")
    compare_path = os.path.join(args.output_dir, "crsp_vs_lseg_distribution_compare.csv")
    compare_summary_path = os.path.join(args.output_dir, "crsp_vs_lseg_distribution_coverage_summary.csv")

    lseg_events.to_csv(events_path, index=False)
    lseg_summary.to_csv(lseg_summary_path, index=False)
    failures.to_csv(failures_path, index=False)
    compare.to_csv(compare_path, index=False)
    compare_summary.to_csv(compare_summary_path, index=False)

    print(f"LSEG event rows:  {len(lseg_events):,}")
    print(f"Access failures:  {len(failures):,}")
    print(f"Compare rows:     {len(compare):,}")
    print()
    print(f"Wrote {events_path}")
    print(f"Wrote {lseg_summary_path}")
    print(f"Wrote {failures_path}")
    print(f"Wrote {compare_path}")
    print(f"Wrote {compare_summary_path}")


if __name__ == "__main__":
    main()
