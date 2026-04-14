"""
Build the complete stock universe for dividend-distribution work.

Universe definition:
  - all CRSP names that currently have LSEG identifiers in security_master
  - plus all Eurex SSF / SSDF underlier stock identities not already in that CRSP set

Outputs:
  - complete_stock_universe.csv
  - complete_stock_universe_summary.csv
  - complete_stock_universe_excluded_non_single_stock.csv
  - complete_stock_universe_crsp_unmatched_exceptions.csv
"""

from __future__ import annotations

import argparse
import os

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the complete stock universe manifest")
    parser.add_argument(
        "--us-secmaster",
        default=os.path.join("security_master", "us_security_master_snapshot.csv"),
        help="US security-master snapshot",
    )
    parser.add_argument(
        "--us-secmaster-unmatched",
        default=os.path.join("security_master", "us_security_master_unmatched.csv"),
        help="US security-master unmatched exceptions",
    )
    parser.add_argument(
        "--eurex-us",
        default=os.path.join(
            "security_master",
            "eurex_ssf_ssdf",
            "eurex_ssf_ssdf_underliers_us.csv",
        ),
        help="US Eurex underlier mapping",
    )
    parser.add_argument(
        "--eurex-non-us",
        default=os.path.join(
            "security_master",
            "eurex_ssf_ssdf",
            "eurex_ssf_ssdf_underliers_non_us.csv",
        ),
        help="Non-US Eurex underlier mapping",
    )
    parser.add_argument(
        "--output-dir",
        default="div_distribution_data",
        help="Directory for outputs",
    )
    return parser.parse_args()


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def join_unique(series: pd.Series) -> str:
    vals = sorted({str(v).strip() for v in series.dropna() if str(v).strip()})
    return "|".join(vals)


def first_nonempty(series: pd.Series) -> str:
    for value in series:
        text = str(value).strip()
        if text:
            return text
    return ""


def classify_non_single_stock(df: pd.DataFrame) -> pd.Series:
    name = df.get("underlier_name", pd.Series("", index=df.index)).fillna("").astype(str)
    prod = df.get("product_name", pd.Series("", index=df.index)).fillna("").astype(str)
    text = name.str.lower() + " " + prod.str.lower()
    return text.str.contains("basket", regex=False)


def build_crsp_rows(path: str) -> pd.DataFrame:
    df = normalize_strings(pd.read_csv(path, dtype=str))
    df = df[df["isin"] != ""].copy()
    df = df.rename(
        columns={
            "isin": "share_isin",
            "permid": "current_permid",
            "primary_ric": "current_primary_ric",
            "ticker": "ticker",
            "common_name": "underlier_name",
        }
    )
    df["lseg_query_key"] = df["current_primary_ric"]
    df["lseg_query_key_type"] = "primary_ric"
    df["query_key_fallback_used"] = False
    df["in_crsp_secmaster"] = True
    df["in_eurex_underliers"] = False
    df["in_eurex_us_underliers"] = False
    df["in_eurex_non_us_underliers"] = False
    df["products"] = ""
    df["product_groups"] = ""
    df["reuters_ul_codes"] = ""
    df["country_code"] = "US"
    df["identifier_status"] = "us_security_master_match"
    df["source_universe"] = "crsp_secmaster"
    df["is_non_single_stock"] = False
    return df[
        [
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
            "source_universe",
            "is_non_single_stock",
        ]
    ].copy()


def build_eurex_rows(path: str, us_flag: bool) -> pd.DataFrame:
    df = normalize_strings(pd.read_csv(path, dtype=str))
    df["is_non_single_stock"] = classify_non_single_stock(df)
    df["candidate_primary_ric"] = df["underlier_lseg_ric"]
    df.loc[df["candidate_primary_ric"] == "", "candidate_primary_ric"] = df["us_underlier_primary_ric"]
    df["candidate_permid"] = df["underlier_lseg_permid"]
    df.loc[df["candidate_permid"] == "", "candidate_permid"] = df["us_underlier_permid"]
    df["candidate_ticker"] = df["underlier_lseg_ticker"]
    df.loc[df["candidate_ticker"] == "", "candidate_ticker"] = df["us_underlier_ticker"]
    df["candidate_permno"] = df["us_underlier_permno"]

    grouped = (
        df.groupby("share_isin", dropna=False)
        .agg(
            {
                "candidate_permno": first_nonempty,
                "candidate_ticker": first_nonempty,
                "underlier_name": join_unique,
                "candidate_primary_ric": first_nonempty,
                "candidate_permid": first_nonempty,
                "country_code": first_nonempty,
                "product": join_unique,
                "product_group": join_unique,
                "reuters_ul_code": join_unique,
                "identifier_status": join_unique,
                "is_non_single_stock": "max",
            }
        )
        .reset_index()
    )
    grouped = grouped.rename(
        columns={
            "candidate_permno": "permno",
            "candidate_ticker": "ticker",
            "candidate_primary_ric": "current_primary_ric",
            "candidate_permid": "current_permid",
            "product": "products",
            "product_group": "product_groups",
            "reuters_ul_code": "reuters_ul_codes",
        }
    )
    grouped["lseg_query_key"] = grouped["current_primary_ric"]
    grouped["lseg_query_key_type"] = "primary_ric"
    grouped["query_key_fallback_used"] = False
    no_primary = grouped["lseg_query_key"] == ""
    grouped.loc[no_primary & (grouped["reuters_ul_codes"] != ""), "lseg_query_key"] = grouped.loc[
        no_primary & (grouped["reuters_ul_codes"] != ""), "reuters_ul_codes"
    ].str.split("|").str[0]
    grouped.loc[no_primary & (grouped["reuters_ul_codes"] != ""), "lseg_query_key_type"] = "reuters_ul_code"
    no_key = grouped["lseg_query_key"] == ""
    grouped.loc[no_key, "lseg_query_key"] = grouped.loc[no_key, "share_isin"]
    grouped.loc[no_key, "lseg_query_key_type"] = "share_isin"
    grouped["query_key_fallback_used"] = grouped["lseg_query_key_type"] != "primary_ric"
    grouped["in_crsp_secmaster"] = False
    grouped["in_eurex_underliers"] = True
    grouped["in_eurex_us_underliers"] = us_flag
    grouped["in_eurex_non_us_underliers"] = not us_flag
    grouped["source_universe"] = "eurex_us_underliers" if us_flag else "eurex_non_us_underliers"
    return grouped[
        [
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
            "source_universe",
            "is_non_single_stock",
        ]
    ].copy()


def combine_sources(crsp_df: pd.DataFrame, eurex_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([crsp_df, eurex_df], ignore_index=True)
    combined = normalize_strings(combined)

    grouped = (
        combined.groupby("share_isin", dropna=False)
        .agg(
            {
                "permno": first_nonempty,
                "ticker": first_nonempty,
                "underlier_name": join_unique,
                "current_primary_ric": first_nonempty,
                "current_permid": first_nonempty,
                "lseg_query_key": first_nonempty,
                "lseg_query_key_type": first_nonempty,
                "query_key_fallback_used": "max",
                "country_code": first_nonempty,
                "products": join_unique,
                "product_groups": join_unique,
                "reuters_ul_codes": join_unique,
                "identifier_status": join_unique,
                "in_crsp_secmaster": "max",
                "in_eurex_underliers": "max",
                "in_eurex_us_underliers": "max",
                "in_eurex_non_us_underliers": "max",
                "source_universe": join_unique,
                "is_non_single_stock": "max",
            }
        )
        .reset_index()
    )
    for col in [
        "query_key_fallback_used",
        "in_crsp_secmaster",
        "in_eurex_underliers",
        "in_eurex_us_underliers",
        "in_eurex_non_us_underliers",
        "is_non_single_stock",
    ]:
        grouped[col] = grouped[col].map(lambda x: str(x).strip().lower() == "true")
    grouped["universe_category"] = "other"
    grouped.loc[grouped["in_crsp_secmaster"] & (~grouped["in_eurex_underliers"]), "universe_category"] = "crsp_only"
    grouped.loc[grouped["in_crsp_secmaster"] & grouped["in_eurex_underliers"], "universe_category"] = "crsp_and_eurex"
    grouped.loc[(~grouped["in_crsp_secmaster"]) & grouped["in_eurex_underliers"], "universe_category"] = "eurex_only"
    return grouped


def build_summary(stock_universe: pd.DataFrame, excluded: pd.DataFrame, crsp_unmatched: pd.DataFrame) -> pd.DataFrame:
    rows = [
        {"metric": "complete_stock_universe_rows", "value": len(stock_universe)},
        {"metric": "rows_in_crsp_secmaster", "value": int(stock_universe["in_crsp_secmaster"].sum())},
        {"metric": "rows_in_eurex_underliers", "value": int(stock_universe["in_eurex_underliers"].sum())},
        {"metric": "rows_in_eurex_us_underliers", "value": int(stock_universe["in_eurex_us_underliers"].sum())},
        {"metric": "rows_in_eurex_non_us_underliers", "value": int(stock_universe["in_eurex_non_us_underliers"].sum())},
        {"metric": "rows_crsp_only", "value": int((stock_universe["universe_category"] == "crsp_only").sum())},
        {"metric": "rows_crsp_and_eurex", "value": int((stock_universe["universe_category"] == "crsp_and_eurex").sum())},
        {"metric": "rows_eurex_only", "value": int((stock_universe["universe_category"] == "eurex_only").sum())},
        {"metric": "rows_using_primary_ric_query_key", "value": int((stock_universe["lseg_query_key_type"] == "primary_ric").sum())},
        {"metric": "rows_using_reuters_query_key", "value": int((stock_universe["lseg_query_key_type"] == "reuters_ul_code").sum())},
        {"metric": "rows_using_isin_query_key", "value": int((stock_universe["lseg_query_key_type"] == "share_isin").sum())},
        {"metric": "excluded_non_single_stock_rows", "value": len(excluded)},
        {"metric": "crsp_unmatched_identifier_rows", "value": len(crsp_unmatched)},
    ]
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    crsp_rows = build_crsp_rows(args.us_secmaster)
    eurex_us = build_eurex_rows(args.eurex_us, us_flag=True)
    eurex_non_us = build_eurex_rows(args.eurex_non_us, us_flag=False)
    eurex_all = pd.concat([eurex_us, eurex_non_us], ignore_index=True)
    stock_universe_all = combine_sources(crsp_rows, eurex_all)

    excluded = stock_universe_all[stock_universe_all["is_non_single_stock"]].copy()
    stock_universe = stock_universe_all[~stock_universe_all["is_non_single_stock"]].copy()
    crsp_unmatched = normalize_strings(pd.read_csv(args.us_secmaster_unmatched, dtype=str))
    summary = build_summary(stock_universe=stock_universe, excluded=excluded, crsp_unmatched=crsp_unmatched)

    stock_universe_path = os.path.join(args.output_dir, "complete_stock_universe.csv")
    summary_path = os.path.join(args.output_dir, "complete_stock_universe_summary.csv")
    excluded_path = os.path.join(args.output_dir, "complete_stock_universe_excluded_non_single_stock.csv")
    unmatched_path = os.path.join(args.output_dir, "complete_stock_universe_crsp_unmatched_exceptions.csv")

    stock_universe.to_csv(stock_universe_path, index=False)
    summary.to_csv(summary_path, index=False)
    excluded.to_csv(excluded_path, index=False)
    crsp_unmatched.to_csv(unmatched_path, index=False)

    print("=" * 80)
    print("COMPLETE STOCK UNIVERSE BUILD")
    print("=" * 80)
    print(f"CRSP identifier-complete rows: {len(crsp_rows):,}")
    print(f"Eurex stock identities:        {len(eurex_all):,}")
    print(f"Excluded non-single-stock:     {len(excluded):,}")
    print(f"Complete stock universe rows:  {len(stock_universe):,}")
    print()
    print(f"Wrote {stock_universe_path}")
    print(f"Wrote {summary_path}")
    print(f"Wrote {excluded_path}")
    print(f"Wrote {unmatched_path}")


if __name__ == "__main__":
    main()
