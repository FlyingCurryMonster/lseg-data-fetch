"""
Enumerate dividend futures available via LSEG Discovery Search API.

Usage:
  python enumerate_div_contracts.py SSDF SSF       # single stock products only
  python enumerate_div_contracts.py INDEX           # index div futures only
  python enumerate_div_contracts.py INDEX SSDF SSF  # everything

Groups:
  INDEX — CME SDA/SDI + Eurex FEXD (hardcoded search queries)
  SSDF  — Eurex Single Stock Dividend Futures (~341 from eurex_productlist.csv)
  SSF   — Eurex Single Stock Futures (~1311 from eurex_productlist.csv)

Uses direct OAuth (TokenManager) instead of lseg.data SDK to avoid token expiry
during long-running enumeration. Saves incrementally to CSV so progress survives
crashes. Resumes by skipping products already in the CSV.

Output: enumerated_futures.csv (appended incrementally)
"""

import argparse
import os
import sys
import time

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.token_manager import TokenManager

SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"

SEARCH_SELECT = "DocumentTitle,RIC,StrikePrice,ExpiryDate,ExchangeName,AssetCategory,AssetState"
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "enumerated_futures.csv")


# =====================================================================
# REST search helper
# =====================================================================
def rest_search(tm, query, top=100, select=SEARCH_SELECT, max_retries=3):
    """Call LSEG Discovery Search via REST with token refresh on 401.

    Returns a DataFrame matching ld.discovery.search() output format,
    or an empty DataFrame on failure.
    """
    payload = {
        "Query": query,
        "View": "SearchAll",
        "Top": top,
        "Select": select,
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(SEARCH_URL, headers=tm.headers(), json=payload)
            if resp.status_code == 401:
                tm.on_401()
                continue
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  [429] Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("Hits", [])
            if hits:
                return pd.DataFrame(hits)
            return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            print(f"  [error] Search attempt {attempt + 1}/{max_retries}: {e}", flush=True)
            if attempt < max_retries - 1:
                time.sleep(2)

    return pd.DataFrame()


# =====================================================================
# Resume: load already-enumerated products from existing CSV
# =====================================================================
def load_completed_products(csv_path):
    """Return set of Product IDs already in the output CSV."""
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path)
            if "Product" in df.columns:
                return set(df["Product"].unique())
        except Exception:
            pass
    return set()


def append_to_csv(df, csv_path):
    """Append a DataFrame to the output CSV, writing header if file doesn't exist."""
    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    df.to_csv(csv_path, mode="a", header=write_header, index=False)


# =====================================================================
# Main
# =====================================================================
INDEX_QUERIES = [
    {
        "product": "SDA",
        "name": "S&P 500 Annual Dividend Futures",
        "query": "S&P 500 Annual Dividend Electronic Equity Index Future",
    },
    {
        "product": "SDI",
        "name": "S&P 500 Quarterly Dividend Futures",
        "query": "S&P 500 Quarterly Dividend Electronic Equity Index Future",
    },
    {
        "product": "FEXD",
        "name": "Euro Stoxx 50 Dividend Futures",
        "query": "EURO STOXX 50 Index Dividend Future",
    },
]


def main():
    parser = argparse.ArgumentParser(description="Enumerate dividend futures via LSEG Discovery Search")
    parser.add_argument("groups", nargs="+", choices=["INDEX", "SSDF", "SSF"],
                        help="Product groups to enumerate")
    args = parser.parse_args()
    groups = [g.upper() for g in args.groups]

    print("=" * 80)
    print(f"Enumerating dividend futures: {', '.join(groups)}")
    print("=" * 80)
    print(f"Output: {OUTPUT_CSV}")

    # Load Eurex product list (needed for SSDF/SSF)
    eurex_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "eurex_productlist.csv")
    eurex_products = pd.read_csv(eurex_csv, sep=";", encoding="utf-8-sig")

    ssdfs = eurex_products[
        (eurex_products["PRODUCT_TYPE"] == "FSTK") &
        (eurex_products["PRODUCT_NAME"].str.contains("Dividend", case=False, na=False))
    ]
    ssfs = eurex_products[
        (eurex_products["PRODUCT_TYPE"] == "FSTK") &
        (~eurex_products["PRODUCT_NAME"].str.contains("Dividend", case=False, na=False))
    ]
    print(f"\nEurex product list: {len(ssdfs)} SSDFs, {len(ssfs)} SSFs")

    # Resume: check what's already done
    completed = load_completed_products(OUTPUT_CSV)
    if completed:
        print(f"Resuming — {len(completed)} products already in CSV")

    # Authenticate
    tm = TokenManager()

    total_new_rows = 0

    # ============================================================
    # INDEX: SDA / SDI / FEXD
    # ============================================================
    if "INDEX" in groups:
        print("\n" + "=" * 80)
        print("Enumerating Index Dividend Futures (SDA/SDI/FEXD)")
        print("=" * 80)

        for fq in INDEX_QUERIES:
            if fq["product"] in completed:
                print(f"\n  {fq['name']} — already in CSV, skipping")
                continue
            print(f"\n  Searching: {fq['name']}...", flush=True)
            results = rest_search(tm, fq["query"])
            if not results.empty:
                results["Product"] = fq["product"]
                results["ProductName"] = fq["name"]
                results["ProductGroup"] = "INDEX"
                non_chain = results[~results["RIC"].str.startswith("0#", na=False)]
                print(f"  Found {len(non_chain)} individual contracts")
                append_to_csv(results, OUTPUT_CSV)
                total_new_rows += len(results)
            else:
                print(f"  No results")
            time.sleep(0.5)

    # ============================================================
    # SSDF: Eurex Single Stock Dividend Futures
    # ============================================================
    if "SSDF" in groups:
        print("\n" + "=" * 80)
        print("Enumerating Eurex Single Stock Dividend Futures")
        print("=" * 80)

        ssdf_todo = [(i, row) for i, (_, row) in enumerate(ssdfs.iterrows(), 1)
                     if row["PRODUCT_ID"] not in completed]
        print(f"  {len(ssdfs)} total, {len(ssdf_todo)} remaining")

        zero_ssdf = 0
        for idx, row in ssdf_todo:
            product_id = row["PRODUCT_ID"]
            product_name = row["PRODUCT_NAME"]
            print(f"\n  SSDF {idx}/{len(ssdfs)}: {product_name} ({product_id})...", end="", flush=True)

            results = rest_search(tm, f"{product_name} Eurex")
            if not results.empty:
                results["Product"] = product_id
                results["ProductName"] = product_name
                results["ProductGroup"] = "SSDF"
                non_chain = results[~results["RIC"].str.startswith("0#", na=False)]
                print(f" {len(non_chain)} contracts", flush=True)
                append_to_csv(results, OUTPUT_CSV)
                total_new_rows += len(results)
            else:
                zero_ssdf += 1
                print(f" no results", flush=True)

            time.sleep(0.5)

        print(f"\nSSDFs done. Zero-result products: {zero_ssdf}/{len(ssdfs)}")

    # ============================================================
    # SSF: Eurex Single Stock Futures
    # ============================================================
    if "SSF" in groups:
        print("\n" + "=" * 80)
        print("Enumerating Eurex Single Stock Futures")
        print("=" * 80)

        ssf_todo = [(i, row) for i, (_, row) in enumerate(ssfs.iterrows(), 1)
                    if row["PRODUCT_ID"] not in completed]
        print(f"  {len(ssfs)} total, {len(ssf_todo)} remaining")

        zero_ssf = 0
        for idx, row in ssf_todo:
            product_id = row["PRODUCT_ID"]
            product_name = row["PRODUCT_NAME"]
            print(f"\n  SSF {idx}/{len(ssfs)}: {product_name} ({product_id})...", end="", flush=True)

            results = rest_search(tm, f"{product_name} Eurex future")
            if not results.empty:
                results["Product"] = product_id
                results["ProductName"] = product_name
                results["ProductGroup"] = "SSF"
                non_chain = results[~results["RIC"].str.startswith("0#", na=False)]
                print(f" {len(non_chain)} contracts", flush=True)
                append_to_csv(results, OUTPUT_CSV)
                total_new_rows += len(results)
            else:
                zero_ssf += 1
                print(f" no results", flush=True)

            time.sleep(0.5)

        print(f"\nSSFs done. Zero-result products: {zero_ssf}/{len(ssfs)}")

    # ============================================================
    # SUMMARY
    # ============================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if os.path.exists(OUTPUT_CSV):
        full_df = pd.read_csv(OUTPUT_CSV)
        print(f"Total rows in {os.path.basename(OUTPUT_CSV)}: {len(full_df)}")
        if "ProductGroup" in full_df.columns:
            for group, count in full_df.groupby("ProductGroup").size().items():
                n_products = full_df[full_df["ProductGroup"] == group]["Product"].nunique()
                print(f"  {group}: {count} rows across {n_products} products")
        print(f"New rows added this run: {total_new_rows}")
    else:
        print("No data enumerated!")

    print("\nDone. Review the CSV to confirm coverage before building master / downloading.")


if __name__ == "__main__":
    main()
