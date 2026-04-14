"""
Download daily pricing data for all futures in the instrument master.

Usage:
  python download_div_futures.py [--master PATH] [--start DATE] [--workers N]

Reads: instrument_master_futures.csv (or --master)
Outputs:
  futures_daily_prices.csv          — finalized raw history with a stable union schema
  futures_download_log.jsonl        — per-RIC resume log
  futures_download.log              — human-readable progress log
  futures_daily_prices_staging/     — per-RIC staged CSV payloads
  futures_daily_prices_schema.json  — union schema manifest for staged data

Uses direct REST + TokenManager for reliable token refresh during long runs.
Parallel workers for throughput. Resumes from futures_download_log.jsonl only for
RICs that already have a staged payload on disk.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.token_manager import TokenManager

HIST_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "futures_daily_prices.csv")
LOG_JSONL = os.path.join(SCRIPT_DIR, "futures_download_log.jsonl")
PROGRESS_LOG = os.path.join(SCRIPT_DIR, "futures_download.log")
STAGING_DIR = os.path.join(SCRIPT_DIR, "futures_daily_prices_staging")
SCHEMA_MANIFEST = os.path.join(SCRIPT_DIR, "futures_daily_prices_schema.json")

PREFERRED_FIELDS = [
    "TRDPRC_1",
    "OPEN_PRC",
    "HIGH_1",
    "LOW_1",
    "ACVOL_UNS",
    "BID",
    "ASK",
    "OPINT_1",
    "TOTCNTRVOL",
    "TOTCNTROI",
    "SETTLE",
    "IMP_YIELD",
    "EDSP",
    "NUM_MOVES",
    "VWAP",
    "ORDBK_VOL",
    "OFFBK_VOL",
    "MID_PRICE",
    "EXPIR_DATE",
    "CRT_MNTH",
    "SETL_PCHNG",
    "SETL_NCHNG",
]
REQUIRED_RAW_COLUMNS = ("date", "RIC")

jsonl_lock = threading.Lock()
progress_lock = threading.Lock()
counter_lock = threading.Lock()
schema_lock = threading.Lock()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download daily prices for dividend futures")
    parser.add_argument(
        "--master",
        default=os.path.join(SCRIPT_DIR, "instrument_master_futures.csv"),
        help="Path to instrument master CSV",
    )
    parser.add_argument("--start", default="2005-01-01", help="Start date (default: 2005-01-01)")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers (default: 10)")
    parser.add_argument("--output-csv", default=OUTPUT_CSV, help="Path for finalized raw prices CSV")
    parser.add_argument("--log-jsonl", default=LOG_JSONL, help="Path for per-RIC resume log")
    parser.add_argument("--progress-log", default=PROGRESS_LOG, help="Path for human-readable progress log")
    parser.add_argument(
        "--staging-dir",
        default=STAGING_DIR,
        help="Directory for per-RIC staged CSV payloads",
    )
    parser.add_argument(
        "--schema-manifest",
        default=SCHEMA_MANIFEST,
        help="Path for the union schema manifest JSON",
    )
    return parser.parse_args()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def normalize_field_name(field: object) -> str | None:
    if field is None:
        return None
    text = str(field).strip()
    if not text:
        return None
    if text == "DATE":
        return "date"
    return text


def normalize_headers(headers: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for header in headers:
        name = normalize_field_name(header)
        if not name or name == "RIC" or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return normalized


def stage_path_for_ric(staging_dir: str, ric: str) -> str:
    digest = hashlib.sha1(ric.encode("utf-8")).hexdigest()
    return os.path.join(staging_dir, f"{digest}.csv")


def atomic_write_json(path: str, payload: object) -> None:
    ensure_parent_dir(path)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp_path, path)


def log_progress(progress_log: str, msg: str) -> None:
    """Print and append to progress log file."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with progress_lock:
        ensure_parent_dir(progress_log)
        with open(progress_log, "a") as f:
            f.write(line + "\n")


def load_download_state(log_jsonl: str, staging_dir: str) -> tuple[set[str], set[str], list[str]]:
    """Return resumable completed RICs, ok RICs with stage files, and stale ok entries."""
    statuses: dict[str, str] = {}
    if os.path.exists(log_jsonl):
        with open(log_jsonl) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ric = entry.get("ric")
                status = entry.get("status")
                if ric and status:
                    statuses[ric] = status

    completed = set()
    ok_with_stage = set()
    stale_ok_entries = []

    for ric, status in statuses.items():
        if status == "empty":
            completed.add(ric)
            continue
        if status == "ok":
            stage_path = stage_path_for_ric(staging_dir, ric)
            if os.path.exists(stage_path):
                completed.add(ric)
                ok_with_stage.add(ric)
            else:
                stale_ok_entries.append(ric)

    return completed, ok_with_stage, stale_ok_entries


def log_ric_result(
    log_jsonl: str,
    ric: str,
    status: str,
    rows: int = 0,
    date_min: str | None = None,
    date_max: str | None = None,
    field_count: int | None = None,
) -> None:
    """Append a per-RIC result to the JSONL log."""
    entry = {
        "ric": ric,
        "status": status,
        "rows": rows,
        "ts": datetime.now().isoformat(),
    }
    if date_min:
        entry["date_min"] = date_min
    if date_max:
        entry["date_max"] = date_max
    if field_count is not None:
        entry["field_count"] = field_count
    with jsonl_lock:
        ensure_parent_dir(log_jsonl)
        with open(log_jsonl, "a") as f:
            f.write(json.dumps(entry) + "\n")


def fetch_history(
    tm: TokenManager,
    ric: str,
    start: str,
    end: str,
    max_retries: int = 3,
    progress_log: str | None = None,
) -> tuple[list[str], list[list[object]]] | None:
    """Fetch daily history for one RIC via REST."""
    url = f"{HIST_URL}/{ric}"
    params = {"interval": "P1D", "start": start, "end": end}

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=tm.headers(), params=params)
            if resp.status_code == 401:
                tm.on_401()
                continue
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                if progress_log:
                    log_progress(progress_log, f"  [429] Rate limited on {ric}, waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()

            data = resp.json()
            if not data or not isinstance(data, list):
                return None

            item = data[0]
            headers = [h.get("name") for h in item.get("headers", [])]
            rows = item.get("data", [])
            if not headers or not rows:
                return None

            return headers, rows

        except requests.exceptions.RequestException as e:
            if progress_log:
                log_progress(progress_log, f"  [error] {ric} attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)

    return None


def write_stage_csv(staging_dir: str, ric: str, headers: list[str], rows: list[list[object]]) -> list[str]:
    normalized_headers = normalize_headers(headers)
    if "date" not in normalized_headers:
        raise ValueError(f"{ric}: DATE field missing from API response")

    os.makedirs(staging_dir, exist_ok=True)
    path = stage_path_for_ric(staging_dir, ric)
    tmp_path = f"{path}.tmp"

    with open(tmp_path, "w", newline="") as f:
        writer = csv.writer(f)
        stage_header = normalized_headers + ["RIC"]
        writer.writerow(stage_header)

        for row in rows:
            if len(row) != len(headers):
                raise ValueError(
                    f"{ric}: row length {len(row)} does not match header length {len(headers)}"
                )

            row_map: dict[str, object] = {}
            for raw_header, value in zip(headers, row, strict=True):
                key = normalize_field_name(raw_header)
                if key and key != "RIC":
                    row_map[key] = value
            writer.writerow([row_map.get(col, "") for col in normalized_headers] + [ric])

    os.replace(tmp_path, path)
    return stage_header


def load_schema_manifest(manifest_path: str) -> list[str]:
    if not os.path.exists(manifest_path):
        return []
    with open(manifest_path) as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise ValueError(f"Schema manifest is not a list: {manifest_path}")
    return [field for field in payload if isinstance(field, str) and field]


def update_schema_manifest(manifest_path: str, fields: list[str]) -> list[str]:
    with schema_lock:
        existing = load_schema_manifest(manifest_path)
        combined = list(existing)
        seen = set(existing)
        for field in fields:
            if field and field not in seen:
                combined.append(field)
                seen.add(field)
        atomic_write_json(manifest_path, combined)
    return combined


def collect_union_schema(staging_dir: str, ok_rics: set[str]) -> list[str]:
    combined = []
    seen = set()
    for ric in sorted(ok_rics):
        stage_path = stage_path_for_ric(staging_dir, ric)
        with open(stage_path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
        for field in header:
            if field and field != "RIC" and field not in seen:
                combined.append(field)
                seen.add(field)
    return combined


def build_output_header(discovered_fields: list[str]) -> list[str]:
    seen = set()
    normalized = []
    for field in discovered_fields:
        if field and field != "RIC" and field not in seen:
            normalized.append(field)
            seen.add(field)

    header = ["date"]
    for field in PREFERRED_FIELDS:
        if field in seen:
            header.append(field)

    extras = sorted(field for field in normalized if field != "date" and field not in PREFERRED_FIELDS)
    header.extend(extras)
    header.append("RIC")
    return header


def validate_price_csv(path: str, expected_rics: set[str]) -> dict[str, int]:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        if len(header) != len(set(header)):
            raise ValueError(f"Duplicate columns in {path}: {header}")

        missing = [col for col in REQUIRED_RAW_COLUMNS if col not in header]
        if missing:
            raise ValueError(f"Missing required columns in {path}: {missing}")

        ric_idx = header.index("RIC")
        date_idx = header.index("date")
        header_len = len(header)

        distinct_rics = set()
        row_count = 0
        for line_no, row in enumerate(reader, start=2):
            if len(row) != header_len:
                raise ValueError(
                    f"{path}: line {line_no} has {len(row)} fields, expected {header_len}"
                )
            if not row[date_idx]:
                raise ValueError(f"{path}: blank date at line {line_no}")
            if not row[ric_idx]:
                raise ValueError(f"{path}: blank RIC at line {line_no}")
            distinct_rics.add(row[ric_idx])
            row_count += 1

    if distinct_rics != expected_rics:
        missing_rics = sorted(expected_rics - distinct_rics)[:10]
        extra_rics = sorted(distinct_rics - expected_rics)[:10]
        raise ValueError(
            f"{path}: distinct RIC mismatch; expected {len(expected_rics)}, "
            f"got {len(distinct_rics)}. Missing sample={missing_rics}, extra sample={extra_rics}"
        )

    return {"rows": row_count, "distinct_rics": len(distinct_rics), "columns": len(header)}


def finalize_output_csv(
    staging_dir: str,
    ok_rics: set[str],
    output_csv: str,
    schema_manifest: str,
) -> dict[str, int]:
    discovered_fields = collect_union_schema(staging_dir, ok_rics)
    update_schema_manifest(schema_manifest, discovered_fields)
    header = build_output_header(discovered_fields)

    ensure_parent_dir(output_csv)
    tmp_path = f"{output_csv}.tmp"
    with open(tmp_path, "w", newline="") as dst:
        writer = csv.DictWriter(dst, fieldnames=header)
        writer.writeheader()

        for ric in sorted(ok_rics):
            stage_path = stage_path_for_ric(staging_dir, ric)
            with open(stage_path, newline="") as src:
                reader = csv.DictReader(src)
                stage_header = reader.fieldnames or []
                if "date" not in stage_header or "RIC" not in stage_header:
                    raise ValueError(f"Stage file missing required columns: {stage_path}")

                for line_no, row in enumerate(reader, start=2):
                    if row.get("RIC") != ric:
                        raise ValueError(
                            f"{stage_path}: expected RIC {ric}, found {row.get('RIC')} at line {line_no}"
                        )
                    if not row.get("date"):
                        raise ValueError(f"{stage_path}: blank date at line {line_no}")
                    writer.writerow({field: row.get(field, "") for field in header})

    os.replace(tmp_path, output_csv)
    return validate_price_csv(output_csv, ok_rics)


def extract_date_range(headers: list[str], rows: list[list[object]]) -> tuple[str | None, str | None]:
    try:
        date_idx = headers.index("DATE")
    except ValueError:
        try:
            date_idx = headers.index("date")
        except ValueError:
            return None, None

    dates = [str(row[date_idx]) for row in rows if len(row) > date_idx and row[date_idx]]
    if not dates:
        return None, None
    return min(dates), max(dates)


def process_ric(
    tm: TokenManager,
    ric: str,
    start: str,
    end: str,
    counters: dict[str, int],
    log_jsonl: str,
    progress_log: str,
    staging_dir: str,
    schema_manifest: str,
) -> None:
    """Download one RIC and stage the result. Called from thread pool."""
    try:
        payload = fetch_history(tm, ric, start, end, progress_log=progress_log)

        if payload:
            headers, rows = payload
            stage_header = write_stage_csv(staging_dir, ric, headers, rows)
            update_schema_manifest(schema_manifest, [field for field in stage_header if field != "RIC"])
            date_min, date_max = extract_date_range(headers, rows)
            log_ric_result(
                log_jsonl,
                ric,
                "ok",
                rows=len(rows),
                date_min=date_min,
                date_max=date_max,
                field_count=len(stage_header),
            )
            with counter_lock:
                counters["rows"] += len(rows)
                counters["done"] += 1
        else:
            log_ric_result(log_jsonl, ric, "empty")
            with counter_lock:
                counters["empty"] += 1
                counters["done"] += 1

    except Exception as e:
        log_ric_result(log_jsonl, ric, "error")
        with counter_lock:
            counters["errors"] += 1
            counters["done"] += 1
        log_progress(progress_log, f"  {ric}: ERROR — {e}")


def main() -> None:
    args = parse_args()

    output_csv = os.path.abspath(args.output_csv)
    log_jsonl = os.path.abspath(args.log_jsonl)
    progress_log = os.path.abspath(args.progress_log)
    staging_dir = os.path.abspath(args.staging_dir)
    schema_manifest = os.path.abspath(args.schema_manifest)

    ensure_parent_dir(output_csv)
    ensure_parent_dir(log_jsonl)
    ensure_parent_dir(progress_log)
    ensure_parent_dir(schema_manifest)
    os.makedirs(staging_dir, exist_ok=True)

    master = pd.read_csv(args.master, dtype=str).fillna("")
    master["RIC"] = master["RIC"].astype(str).str.strip()
    blank_master_rics = int((master["RIC"] == "").sum())
    if blank_master_rics:
        master = master[master["RIC"] != ""].copy()

    all_rics = master["RIC"].tolist()

    log_progress(progress_log, "=" * 80)
    log_progress(progress_log, f"Downloading daily prices for {len(all_rics)} futures")
    log_progress(progress_log, f"Master: {args.master}")
    log_progress(progress_log, f"Start: {args.start}, Workers: {args.workers}")
    log_progress(progress_log, f"Output CSV: {output_csv}")
    log_progress(progress_log, f"Staging dir: {staging_dir}")
    if blank_master_rics:
        log_progress(progress_log, f"Skipped {blank_master_rics} blank-RIC master rows")
    log_progress(progress_log, "=" * 80)

    if "ProductGroup" in master.columns:
        for group, grp in master.groupby("ProductGroup"):
            log_progress(progress_log, f"  {group}: {len(grp)} contracts across {grp['product'].nunique()} products")
    else:
        for product, count in master.groupby("product").size().items():
            log_progress(progress_log, f"  {product}: {count}")

    completed, ok_with_stage, stale_ok_entries = load_download_state(log_jsonl, staging_dir)
    remaining = [r for r in all_rics if r not in completed]
    if stale_ok_entries:
        log_progress(
            progress_log,
            f"Found {len(stale_ok_entries)} stale ok log entries without staged payloads; they will be re-downloaded",
        )
    if completed:
        log_progress(progress_log, f"\nResuming — {len(completed)} RICs already staged, {len(remaining)} remaining")
    else:
        log_progress(progress_log, f"\nStarting fresh — {len(remaining)} RICs to download")

    if not remaining and ok_with_stage:
        stats = finalize_output_csv(staging_dir, ok_with_stage, output_csv, schema_manifest)
        log_progress(
            progress_log,
            f"Nothing to download. Validated existing output: {stats['rows']} rows, "
            f"{stats['distinct_rics']} RICs, {stats['columns']} columns",
        )
        return
    if not remaining:
        log_progress(progress_log, "Nothing to do!")
        return

    tm = TokenManager()

    end = datetime.now().strftime("%Y-%m-%d")
    counters = {"rows": 0, "empty": 0, "errors": 0, "done": 0}
    total = len(remaining)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                process_ric,
                tm,
                ric,
                args.start,
                end,
                counters,
                log_jsonl,
                progress_log,
                staging_dir,
                schema_manifest,
            ): ric
            for ric in remaining
        }

        for future in as_completed(futures):
            future.result()
            done = counters["done"]
            if done % 200 == 0 or done <= 5:
                elapsed = time.time() - start_time
                rate = done / elapsed if elapsed > 0 else 0
                eta_min = (total - done) / rate / 60 if rate > 0 else 0
                log_progress(
                    progress_log,
                    f"  Progress: {done}/{total} "
                    f"({counters['rows']} rows, {counters['empty']} empty, {counters['errors']} errors) "
                    f"— {rate:.1f} RICs/sec, ETA {eta_min:.0f}min",
                )

    completed, ok_with_stage, stale_ok_entries = load_download_state(log_jsonl, staging_dir)
    if stale_ok_entries:
        raise RuntimeError(
            f"Staged payloads missing after download for {len(stale_ok_entries)} ok RICs; "
            f"sample: {stale_ok_entries[:10]}"
        )

    stats = finalize_output_csv(staging_dir, ok_with_stage, output_csv, schema_manifest)

    elapsed = time.time() - start_time
    log_progress(progress_log, "\n" + "=" * 80)
    log_progress(progress_log, "DOWNLOAD COMPLETE")
    log_progress(progress_log, "=" * 80)
    log_progress(progress_log, f"Total RICs processed: {total}")
    log_progress(progress_log, f"Total rows downloaded: {counters['rows']}")
    log_progress(progress_log, f"Empty (no data): {counters['empty']}")
    log_progress(progress_log, f"Errors: {counters['errors']}")
    log_progress(progress_log, f"Elapsed: {elapsed / 60:.1f} minutes ({elapsed / 3600:.1f} hours)")
    log_progress(
        progress_log,
        f"Validated output: {stats['rows']} rows, {stats['distinct_rics']} RICs, {stats['columns']} columns",
    )
    log_progress(progress_log, f"Output: {output_csv}")
    log_progress(progress_log, f"Log: {log_jsonl}")


if __name__ == "__main__":
    main()
