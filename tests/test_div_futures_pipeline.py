from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name: str, relative_path: str):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


download_div_futures = load_module(
    "download_div_futures_module",
    "dividend_derivatives/download_div_futures.py",
)
underlier_map = load_module(
    "build_eurex_ssf_ssdf_underlier_map_module",
    "security_master/build_eurex_ssf_ssdf_underlier_map.py",
)


def test_build_output_header_preserves_preferred_order_and_extra_fields():
    header = download_div_futures.build_output_header(
        [
            "date",
            "OFFBK_VOL",
            "TRDPRC_1",
            "CONVX_BIAS",
            "TOTCNTRVOL",
            "TRDPRC_1",
        ]
    )

    assert header == [
        "date",
        "TRDPRC_1",
        "TOTCNTRVOL",
        "OFFBK_VOL",
        "CONVX_BIAS",
        "RIC",
    ]


def test_finalize_output_csv_writes_union_schema_and_validates(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    manifest_path = tmp_path / "schema.json"
    output_path = tmp_path / "futures_daily_prices.csv"

    download_div_futures.write_stage_csv(
        str(staging_dir),
        "RIC1",
        ["DATE", "TRDPRC_1", "TOTCNTRVOL"],
        [["2026-04-10", 12.5, 40]],
    )
    download_div_futures.write_stage_csv(
        str(staging_dir),
        "RIC2",
        ["DATE", "SETTLE", "CONVX_BIAS"],
        [["2026-04-10", 18.0, 0.42]],
    )

    stats = download_div_futures.finalize_output_csv(
        str(staging_dir),
        {"RIC1", "RIC2"},
        str(output_path),
        str(manifest_path),
    )

    assert stats["rows"] == 2
    assert stats["distinct_rics"] == 2

    with open(output_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert reader.fieldnames == [
        "date",
        "TRDPRC_1",
        "TOTCNTRVOL",
        "SETTLE",
        "CONVX_BIAS",
        "RIC",
    ]
    assert rows[0]["RIC"] == "RIC1"
    assert rows[0]["TOTCNTRVOL"] == "40"
    assert rows[0]["CONVX_BIAS"] == ""
    assert rows[1]["RIC"] == "RIC2"
    assert rows[1]["SETTLE"] == "18.0"
    assert rows[1]["CONVX_BIAS"] == "0.42"


def test_write_enriched_prices_csv_preserves_raw_columns_and_rejects_malformed_rows(tmp_path: Path):
    raw_prices = tmp_path / "raw_prices.csv"
    with open(raw_prices, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "TRDPRC_1", "CONVX_BIAS", "RIC"])
        writer.writerow(["2026-04-10", "12.5", "0.42", "RIC1"])

    lookup_df = pd.DataFrame(
        [
            {
                "RIC": "RIC1",
                "product": "AAPH",
                "ProductGroup": "SSF",
                "underlying": "Apple",
                "underlier_name": "Apple",
                "share_isin": "US0378331005",
                "productlist_share_isin": "US0378331005",
                "underlier_lseg_ric": "AAPL.O",
                "underlier_lseg_ticker": "AAPL",
                "underlier_lseg_permid": "4295905573",
                "underlier_lseg_cusip9": "037833100",
                "us_underlier_permno": "14593",
                "us_underlier_primary_ric": "AAPL.O",
                "us_underlier_ticker": "AAPL",
                "identifier_status": "us_security_master_match",
                "identifier_source": "share_isin_productlist",
            }
        ]
    )

    enriched = tmp_path / "enriched.csv"
    underlier_map.write_enriched_prices_csv(str(raw_prices), lookup_df, str(enriched))

    with open(enriched, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert reader.fieldnames[:4] == ["date", "TRDPRC_1", "CONVX_BIAS", "RIC"]
    assert rows[0]["RIC"] == "RIC1"
    assert rows[0]["CONVX_BIAS"] == "0.42"
    assert rows[0]["product"] == "AAPH"
    assert rows[0]["share_isin"] == "US0378331005"

    malformed = tmp_path / "malformed.csv"
    with open(malformed, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "RIC"])
        writer.writerow(["2026-04-10", "RIC1", "EXTRA"])

    with pytest.raises(ValueError, match="extra unnamed columns"):
        underlier_map.write_enriched_prices_csv(str(malformed), lookup_df, str(tmp_path / "bad.csv"))
