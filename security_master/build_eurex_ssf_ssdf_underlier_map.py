"""
Build Eurex SSF / SSDF underlier mapping tables.

Purpose:
  1. Give each Eurex SSF / SSDF product a stable underlier identifier.
  2. Join US-underlier products back to the US security master when possible.
  3. Enrich the contract-level futures master so queries like
     "all Apple SSF and SSDF contracts" are easy.

Outputs (written under security_master/eurex_ssf_ssdf/ by default):
  - eurex_ssf_ssdf_product_underliers.csv
  - eurex_ssf_ssdf_underliers_us.csv
  - eurex_ssf_ssdf_underliers_non_us.csv
  - eurex_ssf_ssdf_contracts_enriched.csv
  - futures_daily_prices_enriched.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
from datetime import datetime, timezone

import lseg.data as ld
import pandas as pd
from dotenv import load_dotenv


DEFAULT_BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build underlier mapping tables for Eurex SSF / SSDF products"
    )
    parser.add_argument(
        "--eurex-productlist",
        default=os.path.join("dividend_derivatives", "eurex_productlist.csv"),
        help="Path to Eurex product list CSV",
    )
    parser.add_argument(
        "--futures-master",
        default=os.path.join("security_master", "eurex_ssf_ssdf", "instrument_master_futures.csv"),
        help="Path to SSF / SSDF futures master CSV",
    )
    parser.add_argument(
        "--us-security-master",
        default=os.path.join("security_master", "us_security_master_snapshot.csv"),
        help="Path to canonical US security master snapshot CSV",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join("security_master", "eurex_ssf_ssdf"),
        help="Directory for output CSVs",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Batch size for ISIN -> LSEG conversion",
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


def clean_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def derive_product_group(product_name: str | None) -> str:
    text = product_name or ""
    return "SSDF" if "dividend futures" in text.lower() else "SSF"


def derive_underlier_name(product_name: str | None, product_group: str) -> str | None:
    if not product_name:
        return None
    name = product_name.strip()
    if product_group == "SSDF":
        return re.sub(r"\s+Dividend Futures$", "", name, flags=re.IGNORECASE)
    return name


def parse_document_title(value: object) -> tuple[str | None, str | None]:
    text = clean_text(value)
    if not text:
        return None, None
    parts = [part.strip() for part in text.split(",")]
    common_name = parts[0] if parts else None
    exchange_name = parts[-1] if len(parts) >= 2 else None
    return common_name, exchange_name


def load_eurex_product_map(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
    df = df[df["PRODUCT_TYPE"] == "FSTK"].copy()
    df["product_group"] = df["PRODUCT_NAME"].apply(derive_product_group)
    df["underlier_name"] = [
        derive_underlier_name(name, group)
        for name, group in zip(df["PRODUCT_NAME"], df["product_group"], strict=False)
    ]

    out = pd.DataFrame(
        {
            "product": df["PRODUCT_ID"].apply(clean_text),
            "product_name": df["PRODUCT_NAME"].apply(clean_text),
            "product_group": df["product_group"],
            "country_code": df["COUNTRY_CODE"].apply(clean_text),
            "currency": df["CURRENCY"].apply(clean_text),
            "underlier_name": df["underlier_name"].apply(clean_text),
            "product_isin": df["PRODUCT_ISIN"].apply(clean_text),
            "underlying_isin": df["UNDERLYING_ISIN"].apply(clean_text),
            "share_isin": df["SHARE_ISIN"].apply(clean_text),
            "reuters_chain_ric": df["REUTERS_CODE"].apply(clean_text),
            "reuters_ul_code": df["REUTERS_UL_CODE"].apply(clean_text),
            "bloomberg_ul_code": df["BLOOMBERG_UL_CODE"].apply(clean_text),
            "cash_market_id": df["CASH_MARKET_ID"].apply(clean_text),
            "settlement_type": df["SETTLEMENT_TYPE"].apply(clean_text),
        }
    )
    out["productlist_share_isin"] = out["share_isin"]
    out["underlier_key"] = out["share_isin"]
    out = out.drop_duplicates(subset=["product"]).reset_index(drop=True)
    return out


def resolve_isins_via_lseg(share_isins: list[str], batch_size: int) -> pd.DataFrame:
    rows = []
    total = len(share_isins)
    total_batches = math.ceil(total / batch_size) if total else 0

    for start in range(0, total, batch_size):
        batch = share_isins[start:start + batch_size]
        batch_no = start // batch_size + 1
        print(f"[resolve] batch {batch_no}/{total_batches} ({len(batch)} ISINs)", flush=True)

        result = ld.discovery.convert_symbols(
            symbols=batch,
            from_symbol_type=ld.discovery.SymbolTypes.ISIN,
            to_symbol_types=[
                ld.discovery.SymbolTypes.RIC,
                ld.discovery.SymbolTypes.TICKER_SYMBOL,
                ld.discovery.SymbolTypes.OA_PERM_ID,
                ld.discovery.SymbolTypes.CUSIP,
            ],
        )
        if result is None or result.empty:
            continue

        df = result.reset_index().rename(columns={"index": "share_isin"}).copy()
        parsed = df.get("DocumentTitle", pd.Series(dtype=str)).apply(parse_document_title)
        df["lseg_common_name"] = parsed.apply(lambda item: item[0] if isinstance(item, tuple) else None)
        df["lseg_exchange_name"] = parsed.apply(lambda item: item[1] if isinstance(item, tuple) else None)

        rows.append(
            pd.DataFrame(
                {
                    "share_isin": df["share_isin"].apply(clean_text),
                    "underlier_lseg_ric": df.get("RIC"),
                    "underlier_lseg_ticker": df.get("TickerSymbol"),
                    "underlier_lseg_permid": df.get("IssuerOAPermID"),
                    "underlier_lseg_cusip9": df.get("CUSIP"),
                    "underlier_lseg_common_name": df["lseg_common_name"],
                    "underlier_lseg_exchange_name": df["lseg_exchange_name"],
                }
            )
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "share_isin",
                "underlier_lseg_ric",
                "underlier_lseg_ticker",
                "underlier_lseg_permid",
                "underlier_lseg_cusip9",
                "underlier_lseg_common_name",
                "underlier_lseg_exchange_name",
            ]
        )

    return pd.concat(rows, ignore_index=True).drop_duplicates(subset=["share_isin"])


def resolve_reuters_underliers_via_lseg(reuters_codes: list[str], batch_size: int) -> pd.DataFrame:
    rows = []
    total = len(reuters_codes)
    total_batches = math.ceil(total / batch_size) if total else 0

    for start in range(0, total, batch_size):
        batch = reuters_codes[start:start + batch_size]
        batch_no = start // batch_size + 1
        print(f"[fallback] batch {batch_no}/{total_batches} ({len(batch)} Reuters underliers)", flush=True)

        result = ld.discovery.convert_symbols(
            symbols=batch,
            from_symbol_type=ld.discovery.SymbolTypes.RIC,
            to_symbol_types=[
                ld.discovery.SymbolTypes.ISIN,
                ld.discovery.SymbolTypes.CUSIP,
                ld.discovery.SymbolTypes.TICKER_SYMBOL,
                ld.discovery.SymbolTypes.OA_PERM_ID,
            ],
        )
        if result is None or result.empty:
            continue

        df = result.reset_index().rename(columns={"index": "reuters_ul_code"}).copy()
        parsed = df.get("DocumentTitle", pd.Series(dtype=str)).apply(parse_document_title)
        df["lseg_common_name"] = parsed.apply(lambda item: item[0] if isinstance(item, tuple) else None)
        df["lseg_exchange_name"] = parsed.apply(lambda item: item[1] if isinstance(item, tuple) else None)

        rows.append(
            pd.DataFrame(
                {
                    "reuters_ul_code": df["reuters_ul_code"].apply(clean_text),
                    "fallback_share_isin": df.get("IssueISIN").apply(clean_text),
                    "fallback_underlier_lseg_ticker": df.get("TickerSymbol").apply(clean_text),
                    "fallback_underlier_lseg_permid": df.get("IssuerOAPermID").apply(clean_text),
                    "fallback_underlier_lseg_cusip9": df.get("CUSIP", pd.Series(index=df.index, dtype=str)).apply(clean_text),
                    "fallback_underlier_lseg_common_name": df["lseg_common_name"],
                    "fallback_underlier_lseg_exchange_name": df["lseg_exchange_name"],
                    "fallback_underlier_lseg_ric": df["reuters_ul_code"].apply(clean_text),
                }
            )
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "reuters_ul_code",
                "fallback_share_isin",
                "fallback_underlier_lseg_ric",
                "fallback_underlier_lseg_ticker",
                "fallback_underlier_lseg_permid",
                "fallback_underlier_lseg_cusip9",
                "fallback_underlier_lseg_common_name",
                "fallback_underlier_lseg_exchange_name",
            ]
        )

    return pd.concat(rows, ignore_index=True).drop_duplicates(subset=["reuters_ul_code"])


def build_status(row: pd.Series) -> str:
    if pd.notna(row.get("us_underlier_permno")) and str(row.get("us_underlier_permno")).strip():
        return "us_security_master_match"
    if pd.notna(row.get("underlier_lseg_ric")) and str(row.get("underlier_lseg_ric")).strip():
        return "lseg_isin_match_only"
    if pd.notna(row.get("share_isin")) and str(row.get("share_isin")).strip():
        return "productlist_isin_only"
    return "missing_underlier_identifier"


def build_identifier_source(row: pd.Series) -> str:
    if clean_text(row.get("productlist_share_isin")):
        return "share_isin_productlist"
    if clean_text(row.get("fallback_share_isin")):
        return "reuters_ul_code_fallback"
    return "unresolved"


def prepare_us_snapshot_lookup(us_snapshot: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "permno",
        "isin",
        "primary_ric",
        "permid",
        "ticker",
        "common_name",
        "exchange_name",
    ]
    lookup = us_snapshot[cols].copy()
    lookup["isin"] = lookup["isin"].apply(clean_text)
    lookup = lookup[lookup["isin"].notna()].drop_duplicates(subset=["isin"]).rename(
        columns={
            "permno": "us_underlier_permno",
            "isin": "share_isin",
            "primary_ric": "us_underlier_primary_ric",
            "permid": "us_underlier_permid",
            "ticker": "us_underlier_ticker",
            "common_name": "us_underlier_common_name",
            "exchange_name": "us_underlier_exchange_name",
        }
    )
    return lookup


def collapse_contract_lookup(df: pd.DataFrame) -> pd.DataFrame:
    ordered = df.copy()
    ordered["source_rank"] = ordered["identifier_source"].map(
        {
            "share_isin_productlist": 1,
            "reuters_ul_code_fallback": 2,
            "unresolved": 3,
        }
    ).fillna(9)
    ordered["status_rank"] = ordered["identifier_status"].map(
        {
            "us_security_master_match": 1,
            "lseg_isin_match_only": 2,
            "productlist_isin_only": 3,
            "missing_underlier_identifier": 4,
        }
    ).fillna(9)
    ordered = ordered.sort_values(["RIC", "source_rank", "status_rank", "product"])
    return ordered.drop_duplicates(subset=["RIC"], keep="first").drop(
        columns=["source_rank", "status_rank"]
    )


def normalize_csv_row(row: list[str], header_len: int) -> list[str]:
    if len(row) == header_len:
        return row
    if len(row) < header_len:
        return row + [""] * (header_len - len(row))

    # Some raw daily-price rows contain extra empty fields immediately before the
    # final RIC column. Keep the first N-1 data columns and the last field as RIC.
    return row[: header_len - 1] + [row[-1]]


def write_enriched_prices_csv(prices_path: str, lookup_df: pd.DataFrame, out_path: str) -> None:
    lookup_cols = [
        "product",
        "ProductGroup",
        "underlying",
        "underlier_name",
        "share_isin",
        "productlist_share_isin",
        "underlier_lseg_ric",
        "underlier_lseg_ticker",
        "underlier_lseg_permid",
        "underlier_lseg_cusip9",
        "us_underlier_permno",
        "us_underlier_primary_ric",
        "us_underlier_ticker",
        "identifier_status",
        "identifier_source",
    ]
    lookup = (
        lookup_df[["RIC", *lookup_cols]]
        .fillna("")
        .drop_duplicates(subset=["RIC"])
        .set_index("RIC")
        .to_dict(orient="index")
    )

    with open(prices_path, newline="") as src, open(out_path, "w", newline="") as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)

        header = next(reader)
        writer.writerow(header + lookup_cols)

        for row in reader:
            fixed = normalize_csv_row(row, len(header))
            ric = fixed[-1]
            extra = lookup.get(ric, {})
            writer.writerow(fixed + [extra.get(col, "") for col in lookup_cols])


def main() -> None:
    args = parse_args()
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 80)
    print("EUREX SSF / SSDF UNDERLIER MAP BUILD")
    print("=" * 80)
    print(f"Product list:       {args.eurex_productlist}")
    print(f"Futures master:     {args.futures_master}")
    print(f"US security master: {args.us_security_master}")
    print(f"Output dir:         {output_dir}")
    print()

    product_map = load_eurex_product_map(args.eurex_productlist)
    futures_master = pd.read_csv(args.futures_master, dtype=str)
    us_snapshot = pd.read_csv(args.us_security_master, dtype=str)
    us_lookup = prepare_us_snapshot_lookup(us_snapshot)

    share_isins = sorted(
        {
            value
            for value in product_map["share_isin"].apply(clean_text).tolist()
            if value
        }
    )
    fallback_reuters = sorted(
        {
            value
            for value in product_map.loc[
                product_map["share_isin"].isna(), "reuters_ul_code"
            ].apply(clean_text).tolist()
            if value
        }
    )

    load_dotenv()
    session, config_path = open_session(output_dir)
    try:
        lseg_underliers = resolve_isins_via_lseg(share_isins, args.batch_size)
        fallback_underliers = resolve_reuters_underliers_via_lseg(fallback_reuters, args.batch_size)
    finally:
        ld.close_session()
        if os.path.exists(config_path):
            os.remove(config_path)

    product_map = product_map.merge(lseg_underliers, on="share_isin", how="left")
    product_map = product_map.merge(fallback_underliers, on="reuters_ul_code", how="left")

    needs_fallback = product_map["share_isin"].isna()
    product_map.loc[needs_fallback, "share_isin"] = product_map.loc[needs_fallback, "fallback_share_isin"]
    product_map.loc[needs_fallback, "underlier_lseg_ric"] = product_map.loc[needs_fallback, "fallback_underlier_lseg_ric"]
    product_map.loc[needs_fallback, "underlier_lseg_ticker"] = product_map.loc[needs_fallback, "fallback_underlier_lseg_ticker"]
    product_map.loc[needs_fallback, "underlier_lseg_permid"] = product_map.loc[needs_fallback, "fallback_underlier_lseg_permid"]
    product_map.loc[needs_fallback, "underlier_lseg_cusip9"] = product_map.loc[needs_fallback, "fallback_underlier_lseg_cusip9"]
    product_map.loc[needs_fallback, "underlier_lseg_common_name"] = product_map.loc[needs_fallback, "fallback_underlier_lseg_common_name"]
    product_map.loc[needs_fallback, "underlier_lseg_exchange_name"] = product_map.loc[needs_fallback, "fallback_underlier_lseg_exchange_name"]
    product_map["underlier_key"] = product_map["share_isin"].fillna(product_map["reuters_ul_code"])

    product_map = product_map.merge(us_lookup, on="share_isin", how="left")

    product_map["snapshot_date"] = datetime.now(timezone.utc).date().isoformat()
    product_map["identifier_source"] = product_map.apply(build_identifier_source, axis=1)
    product_map["identifier_status"] = product_map.apply(build_status, axis=1)
    product_map = product_map.drop_duplicates(subset=["product"]).reset_index(drop=True)

    us_products = product_map[product_map["country_code"] == "US"].copy()
    non_us_products = product_map[product_map["country_code"] != "US"].copy()

    enriched_contracts = futures_master.merge(product_map, on="product", how="left")
    enriched_contracts = collapse_contract_lookup(enriched_contracts)

    all_path = os.path.join(output_dir, "eurex_ssf_ssdf_product_underliers.csv")
    us_path = os.path.join(output_dir, "eurex_ssf_ssdf_underliers_us.csv")
    non_us_path = os.path.join(output_dir, "eurex_ssf_ssdf_underliers_non_us.csv")
    contracts_path = os.path.join(output_dir, "eurex_ssf_ssdf_contracts_enriched.csv")
    prices_path = os.path.join(output_dir, "futures_daily_prices_enriched.csv")

    product_map.to_csv(all_path, index=False)
    us_products.to_csv(us_path, index=False)
    non_us_products.to_csv(non_us_path, index=False)
    enriched_contracts.to_csv(contracts_path, index=False)

    raw_prices_path = os.path.join(output_dir, "futures_daily_prices.csv")
    if os.path.exists(raw_prices_path):
        write_enriched_prices_csv(raw_prices_path, enriched_contracts, prices_path)

    print(f"Product underliers: {len(product_map):,}")
    print(f"US products:        {len(us_products):,}")
    print(f"Non-US products:    {len(non_us_products):,}")
    print(
        f"US snapshot matches: {(us_products['us_underlier_permno'].notna()).sum():,} / {len(us_products):,}"
    )
    print(
        f"All LSEG ISIN matches: {(product_map['underlier_lseg_ric'].notna()).sum():,} / {len(product_map):,}"
    )
    print()
    print(f"Wrote {all_path}")
    print(f"Wrote {us_path}")
    print(f"Wrote {non_us_path}")
    print(f"Wrote {contracts_path}")
    if os.path.exists(raw_prices_path):
        print(f"Wrote {prices_path}")


if __name__ == "__main__":
    main()
