"""
Validate the US security master against CRSP-backed and Eurex-US mapping outputs.

This script answers two practical questions:

1. How healthy is the current CRSP -> LSEG US security-master bridge overall?
2. For the US-labeled Eurex SSF/SSDF product universe, which identifiers agree with
   the canonical US security master and which product / stock identities remain
   unaccounted for on the CRSP side?

Outputs:
  - us_secmaster_validation_summary.csv
  - us_secmaster_identifier_agreement_by_product.csv
  - us_secmaster_unaccounted_products.csv
  - us_secmaster_unaccounted_identities.csv
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the US security master")
    parser.add_argument(
        "--snapshot",
        default=os.path.join("security_master", "us_security_master_snapshot.csv"),
        help="Canonical US security master snapshot CSV",
    )
    parser.add_argument(
        "--unmatched",
        default=os.path.join("security_master", "us_security_master_unmatched.csv"),
        help="Unmatched CRSP security-master rows CSV",
    )
    parser.add_argument(
        "--eurex-us",
        default=os.path.join(
            "security_master",
            "eurex_ssf_ssdf",
            "eurex_ssf_ssdf_underliers_us.csv",
        ),
        help="US Eurex underlier mapping CSV",
    )
    parser.add_argument(
        "--output-dir",
        default="security_master",
        help="Directory for validation outputs",
    )
    return parser.parse_args()


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def bool_sum(series: pd.Series) -> int:
    return int(series.fillna(False).astype(bool).sum())


def count_unique_nonempty(series: pd.Series) -> int:
    return int(series[series != ""].nunique())


def write_metric_rows(path: str, rows: Iterable[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def build_product_agreement(eurex_us: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    snapshot_cmp = snapshot.rename(
        columns={
            "permno": "snapshot_permno",
            "isin": "snapshot_isin",
            "permid": "snapshot_permid",
            "primary_ric": "snapshot_primary_ric",
            "ticker": "snapshot_ticker",
            "common_name": "snapshot_common_name",
        }
    )

    merged = eurex_us.merge(
        snapshot_cmp[
            [
                "snapshot_permno",
                "snapshot_isin",
                "snapshot_permid",
                "snapshot_primary_ric",
                "snapshot_ticker",
                "snapshot_common_name",
            ]
        ],
        left_on="us_underlier_permno",
        right_on="snapshot_permno",
        how="left",
    )

    has_permno = merged["us_underlier_permno"] != ""
    has_snapshot_row = merged["snapshot_permno"] != ""

    merged["has_any_lseg_identifier"] = (
        (merged["share_isin"] != "")
        | (merged["underlier_lseg_ric"] != "")
        | (merged["underlier_lseg_permid"] != "")
        | (merged["underlier_lseg_cusip9"] != "")
    )
    merged["has_permno"] = has_permno
    merged["has_snapshot_row"] = has_snapshot_row
    merged["isin_agrees_with_snapshot"] = has_permno & has_snapshot_row & (
        merged["share_isin"] == merged["snapshot_isin"]
    )
    merged["permid_agrees_with_snapshot"] = has_permno & has_snapshot_row & (
        merged["us_underlier_permid"] == merged["snapshot_permid"]
    )
    merged["primary_ric_agrees_with_snapshot"] = has_permno & has_snapshot_row & (
        merged["us_underlier_primary_ric"] == merged["snapshot_primary_ric"]
    )
    merged["ticker_agrees_with_snapshot"] = has_permno & has_snapshot_row & (
        merged["us_underlier_ticker"] == merged["snapshot_ticker"]
    )
    merged["fully_agrees_with_snapshot"] = (
        merged["isin_agrees_with_snapshot"]
        & merged["permid_agrees_with_snapshot"]
        & merged["primary_ric_agrees_with_snapshot"]
        & merged["ticker_agrees_with_snapshot"]
    )
    merged["unaccounted_on_crsp_side"] = merged["has_any_lseg_identifier"] & (~merged["has_permno"])

    return merged


def build_identity_unaccounted(product_agreement: pd.DataFrame) -> pd.DataFrame:
    identity = (
        product_agreement.groupby("share_isin", dropna=False)
        .agg(
            underlier_name=("underlier_name", lambda s: "|".join(sorted({x for x in s if x}))),
            products=("product", lambda s: "|".join(sorted({x for x in s if x}))),
            product_groups=("product_group", lambda s: "|".join(sorted({x for x in s if x}))),
            underlier_lseg_ric=("underlier_lseg_ric", lambda s: "|".join(sorted({x for x in s if x}))),
            underlier_lseg_permid=("underlier_lseg_permid", lambda s: "|".join(sorted({x for x in s if x}))),
            underlier_lseg_cusip9=("underlier_lseg_cusip9", lambda s: "|".join(sorted({x for x in s if x}))),
            us_underlier_permno=("us_underlier_permno", lambda s: "|".join(sorted({x for x in s if x}))),
            identifier_status=("identifier_status", lambda s: "|".join(sorted({x for x in s if x}))),
            identifier_source=("identifier_source", lambda s: "|".join(sorted({x for x in s if x}))),
        )
        .reset_index()
    )
    identity = identity[identity["share_isin"] != ""].copy()
    identity["has_any_lseg_identifier"] = (
        (identity["share_isin"] != "")
        | (identity["underlier_lseg_ric"] != "")
        | (identity["underlier_lseg_permid"] != "")
        | (identity["underlier_lseg_cusip9"] != "")
    )
    identity["has_permno"] = identity["us_underlier_permno"] != ""
    return identity[identity["has_any_lseg_identifier"] & (~identity["has_permno"])].copy()


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    snapshot = normalize_strings(pd.read_csv(args.snapshot, dtype=str))
    unmatched = normalize_strings(pd.read_csv(args.unmatched, dtype=str))
    eurex_us = normalize_strings(pd.read_csv(args.eurex_us, dtype=str))

    product_agreement = build_product_agreement(eurex_us=eurex_us, snapshot=snapshot)
    unaccounted_products = product_agreement[product_agreement["unaccounted_on_crsp_side"]].copy()
    unaccounted_identities = build_identity_unaccounted(product_agreement)

    summary_rows: list[dict[str, object]] = []

    snapshot_total = len(snapshot) + len(unmatched)
    summary_rows.extend(
        [
            {"section": "overall_us_snapshot", "metric": "crsp_rows_total", "value": snapshot_total},
            {"section": "overall_us_snapshot", "metric": "matched_rows", "value": len(snapshot)},
            {"section": "overall_us_snapshot", "metric": "unmatched_rows", "value": len(unmatched)},
            {
                "section": "overall_us_snapshot",
                "metric": "match_rate",
                "value": round(len(snapshot) / snapshot_total, 6) if snapshot_total else None,
            },
            {
                "section": "overall_us_snapshot",
                "metric": "duplicate_permnos_in_snapshot",
                "value": int(snapshot["permno"].duplicated().sum()),
            },
            {
                "section": "overall_us_snapshot",
                "metric": "duplicate_permnos_in_unmatched",
                "value": int(unmatched["permno"].duplicated().sum()),
            },
        ]
    )

    summary_rows.extend(
        [
            {"section": "eurex_us_products", "metric": "total_products", "value": len(product_agreement)},
            {
                "section": "eurex_us_products",
                "metric": "products_with_any_lseg_identifier",
                "value": bool_sum(product_agreement["has_any_lseg_identifier"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "products_with_permno",
                "value": bool_sum(product_agreement["has_permno"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "products_with_snapshot_row",
                "value": bool_sum(product_agreement["has_snapshot_row"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "isin_agreements",
                "value": bool_sum(product_agreement["isin_agrees_with_snapshot"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "permid_agreements",
                "value": bool_sum(product_agreement["permid_agrees_with_snapshot"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "primary_ric_agreements",
                "value": bool_sum(product_agreement["primary_ric_agrees_with_snapshot"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "ticker_agreements",
                "value": bool_sum(product_agreement["ticker_agrees_with_snapshot"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "fully_agreeing_products",
                "value": bool_sum(product_agreement["fully_agrees_with_snapshot"]),
            },
            {
                "section": "eurex_us_products",
                "metric": "products_unaccounted_on_crsp_side",
                "value": len(unaccounted_products),
            },
        ]
    )

    summary_rows.extend(
        [
            {
                "section": "eurex_us_identities",
                "metric": "unique_share_isin",
                "value": count_unique_nonempty(product_agreement["share_isin"]),
            },
            {
                "section": "eurex_us_identities",
                "metric": "unique_identities_with_permno",
                "value": count_unique_nonempty(
                    product_agreement.loc[product_agreement["has_permno"], "share_isin"]
                ),
            },
            {
                "section": "eurex_us_identities",
                "metric": "unique_identities_unaccounted_on_crsp_side",
                "value": len(unaccounted_identities),
            },
        ]
    )

    summary_path = os.path.join(args.output_dir, "us_secmaster_validation_summary.csv")
    agreement_path = os.path.join(
        args.output_dir, "us_secmaster_identifier_agreement_by_product.csv"
    )
    unaccounted_products_path = os.path.join(
        args.output_dir, "us_secmaster_unaccounted_products.csv"
    )
    unaccounted_identities_path = os.path.join(
        args.output_dir, "us_secmaster_unaccounted_identities.csv"
    )

    write_metric_rows(summary_path, summary_rows)
    product_agreement.to_csv(agreement_path, index=False)
    unaccounted_products.to_csv(unaccounted_products_path, index=False)
    unaccounted_identities.to_csv(unaccounted_identities_path, index=False)

    print("=" * 80)
    print("US SECURITY MASTER VALIDATION")
    print("=" * 80)
    print(f"Snapshot rows matched:   {len(snapshot):,}")
    print(f"Snapshot rows unmatched: {len(unmatched):,}")
    print(f"Snapshot match rate:     {len(snapshot) / snapshot_total:.2%}" if snapshot_total else "Snapshot match rate: n/a")
    print()
    print(f"US Eurex products:               {len(product_agreement):,}")
    print(f"Products with any LSEG id:       {bool_sum(product_agreement['has_any_lseg_identifier']):,}")
    print(f"Products with CRSP permno:       {bool_sum(product_agreement['has_permno']):,}")
    print(f"Products fully agreeing:         {bool_sum(product_agreement['fully_agrees_with_snapshot']):,}")
    print(f"Products unaccounted on CRSP:    {len(unaccounted_products):,}")
    print()
    print(f"US Eurex unique stock identities: {count_unique_nonempty(product_agreement['share_isin']):,}")
    print(
        "Unique identities with CRSP permno: "
        f"{count_unique_nonempty(product_agreement.loc[product_agreement['has_permno'], 'share_isin']):,}"
    )
    print(f"Unique identities unaccounted:    {len(unaccounted_identities):,}")
    print()
    if len(unaccounted_identities):
        print("Unaccounted stock identities:")
        for _, row in unaccounted_identities.sort_values(["underlier_name", "share_isin"]).iterrows():
            print(
                f"  - {row['underlier_name']} | share_isin={row['share_isin']} | "
                f"products={row['products']} | lseg_ric={row['underlier_lseg_ric']}"
            )
        print()
    print(f"Wrote {summary_path}")
    print(f"Wrote {agreement_path}")
    print(f"Wrote {unaccounted_products_path}")
    print(f"Wrote {unaccounted_identities_path}")


if __name__ == "__main__":
    main()
