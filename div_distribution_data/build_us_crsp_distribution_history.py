"""
Build canonical CRSP dividend-distribution and split-adjustment history for all
CRSP-mapped names in the complete stock universe.

Inputs:
  - div_distribution_data/complete_stock_universe.csv

Outputs:
  - div_distribution_data/us_crsp_name_universe.csv
  - div_distribution_data/us_crsp_distribution_events.csv
  - div_distribution_data/us_crsp_split_events.csv
  - div_distribution_data/us_crsp_adjustment_summary.csv
"""

from __future__ import annotations

import argparse
import io
import os

import pandas as pd
import requests


CLICKHOUSE_URL = "http://localhost:8123/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build CRSP dividend history for CRSP-mapped names")
    parser.add_argument(
        "--stock-universe",
        default=os.path.join("div_distribution_data", "complete_stock_universe.csv"),
        help="Complete stock universe manifest",
    )
    parser.add_argument(
        "--output-dir",
        default="div_distribution_data",
        help="Directory for output CSVs",
    )
    parser.add_argument(
        "--clickhouse-url",
        default=CLICKHOUSE_URL,
        help="ClickHouse HTTP endpoint",
    )
    return parser.parse_args()


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def query_clickhouse_df(query: str, url: str) -> pd.DataFrame:
    payload = f"{query}\nFORMAT CSVWithNames"
    resp = requests.post(url, data=payload.encode("utf-8"), timeout=300)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), dtype=str)


def join_unique(series: pd.Series) -> str:
    vals = sorted({str(v).strip() for v in series.dropna() if str(v).strip()})
    return "|".join(vals)


def build_us_crsp_universe(path: str) -> pd.DataFrame:
    df = normalize_strings(pd.read_csv(path, dtype=str))
    df = df[df["permno"] != ""].copy()
    df = df.rename(columns={"current_permid": "permid", "current_primary_ric": "primary_ric"})
    return df[
        [
            "share_isin",
            "permno",
            "ticker",
            "underlier_name",
            "primary_ric",
            "permid",
            "country_code",
            "products",
            "product_groups",
            "identifier_status",
            "in_crsp_secmaster",
            "in_eurex_underliers",
            "in_eurex_us_underliers",
            "in_eurex_non_us_underliers",
            "universe_category",
        ]
    ].copy()


def build_distribution_query(permnos: list[str]) -> str:
    permno_list = ", ".join(permnos)
    return f"""
SELECT
    toString(PERMNO) AS permno,
    toString(DisExDt) AS ex_date,
    toString(DisDeclareDt) AS declare_date,
    toString(DisRecordDt) AS record_date,
    toString(DisPayDt) AS pay_date,
    DisSeqNbr AS dis_seq_nbr,
    DisOrdinaryFlg AS ordinary_flag,
    DisType AS dis_type,
    DisDetailType AS dis_detail_type,
    DisFreqType AS dis_freq_type,
    DisPaymentType AS dis_payment_type,
    DisTaxType AS dis_tax_type,
    DisOrigCurType AS original_currency_type,
    DisDivAmt AS dividend_amount,
    DisFacPr AS dis_fac_pr,
    DisFacShr AS dis_fac_shr,
    DisAmountSourceType AS amount_source_type,
    PrimaryExch AS primary_exch,
    SICCD AS siccd,
    NASDIssuno AS nasd_issuno
FROM crsp.distributions
WHERE PERMNO IN ({permno_list})
ORDER BY PERMNO, DisExDt, DisSeqNbr
"""


def derive_adjustment_columns(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    for col in ["dividend_amount", "dis_fac_pr", "dis_fac_shr"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    positive_pr = work["dis_fac_pr"].notna() & (work["dis_fac_pr"] > -1) & (work["dis_fac_pr"] != 0)
    positive_shr = work["dis_fac_shr"].notna() & (work["dis_fac_shr"] > -1) & (work["dis_fac_shr"] != 0)

    work["event_price_multiplier"] = 1.0
    work.loc[positive_pr, "event_price_multiplier"] = 1.0 / (1.0 + work.loc[positive_pr, "dis_fac_pr"])

    work["event_share_multiplier"] = 1.0
    work.loc[positive_shr, "event_share_multiplier"] = 1.0 + work.loc[positive_shr, "dis_fac_shr"]

    work["has_adjustment_event"] = positive_pr | positive_shr
    work["has_cash_distribution"] = work["dividend_amount"].notna() & (work["dividend_amount"] != 0)

    work = work.sort_values(["permno", "ex_date", "dis_seq_nbr"], kind="stable").reset_index(drop=True)
    work["cum_price_factor_to_present"] = 1.0
    work["cum_share_factor_to_present"] = 1.0

    for permno, idx in work.groupby("permno").groups.items():
        sub = work.loc[list(idx)].copy()
        rev_price = sub["event_price_multiplier"].iloc[::-1].cumprod().iloc[::-1]
        rev_share = sub["event_share_multiplier"].iloc[::-1].cumprod().iloc[::-1]
        work.loc[sub.index, "cum_price_factor_to_present"] = rev_price.values
        work.loc[sub.index, "cum_share_factor_to_present"] = rev_share.values

    work["dividend_amount_adj_to_present"] = work["dividend_amount"] * work["cum_price_factor_to_present"]
    return work


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(
            ["permno", "share_isin", "ticker", "primary_ric", "underlier_name", "products", "product_groups", "universe_category"],
            dropna=False,
        )
        .agg(
            first_ex_date=("ex_date", "min"),
            last_ex_date=("ex_date", "max"),
            event_rows=("ex_date", "size"),
            cash_distribution_rows=("has_cash_distribution", "sum"),
            adjustment_event_rows=("has_adjustment_event", "sum"),
            latest_cum_price_factor=("cum_price_factor_to_present", "last"),
            latest_cum_share_factor=("cum_share_factor_to_present", "last"),
        )
        .reset_index()
    )
    return grouped


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    universe = build_us_crsp_universe(args.stock_universe)
    permnos = universe["permno"].dropna().astype(str).tolist()

    print("=" * 80)
    print("US CRSP DISTRIBUTION HISTORY BUILD")
    print("=" * 80)
    print(f"Stock universe:   {args.stock_universe}")
    print(f"Output dir:       {os.path.abspath(args.output_dir)}")
    print(f"ClickHouse URL:   {args.clickhouse_url}")
    print(f"Universe rows:    {len(universe):,}")
    print(f"Unique PERMNO:    {len(permnos):,}")
    print()

    dist = query_clickhouse_df(build_distribution_query(permnos), args.clickhouse_url)
    dist = normalize_strings(dist)
    dist = dist.merge(universe, on="permno", how="left")
    dist = derive_adjustment_columns(dist)

    events_path = os.path.join(args.output_dir, "us_crsp_distribution_events.csv")
    splits_path = os.path.join(args.output_dir, "us_crsp_split_events.csv")
    summary_path = os.path.join(args.output_dir, "us_crsp_adjustment_summary.csv")
    universe_path = os.path.join(args.output_dir, "us_crsp_name_universe.csv")

    split_events = dist[dist["has_adjustment_event"]].copy()
    summary = build_summary(dist)

    universe.to_csv(universe_path, index=False)
    dist.to_csv(events_path, index=False)
    split_events.to_csv(splits_path, index=False)
    summary.to_csv(summary_path, index=False)

    print(f"Distribution rows pulled: {len(dist):,}")
    print(f"Split / adjustment rows:  {len(split_events):,}")
    print()
    print(f"Wrote {universe_path}")
    print(f"Wrote {events_path}")
    print(f"Wrote {splits_path}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
