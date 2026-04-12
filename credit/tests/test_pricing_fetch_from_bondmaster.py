"""Test pricing fetch on bonds from the security master.

Samples 200 bonds:
  - 100 using RIC  (50 active + 50 expired)
  - 100 using CUSIP= format (50 active + 50 expired)

For each bond, attempts to pull full daily price history via the
Historical Pricing REST API. Reports row counts + date ranges, and
aggregates a success rate per group so we can see whether the identifiers
in the master actually resolve to priceable instruments.
"""

import sys, os, json, random, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pandas as pd
import lseg.data as ld
from dotenv import load_dotenv
from shared.lseg_rest_api import LSEGRestClient

CREDIT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
LOGDIR = os.path.join(CREDIT_DIR, "logs")
os.makedirs(LOGDIR, exist_ok=True)
MASTER_CSV = os.path.join(CREDIT_DIR, "secmaster", "bond_security_master.csv")
LOG_PATH = os.path.join(LOGDIR, "test_pricing_fetch.log")

def log(msg=""):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def setup_session():
    """Build a platform.rdp session using env-var credentials."""
    load_dotenv(os.path.join(CREDIT_DIR, "..", ".env"))
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
    config_path = os.path.join(LOGDIR, "_lseg_session.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    return ld.open_session(config_name=config_path)


# -- Load master and sample bonds by identifier group --

USECOLS = ["RIC", "CUSIP", "ISIN", "IssuerLegalName", "CouponRate", "MaturityDate", "IssueDate", "IsActive"]


def load_samples(n_per_group=50, seed=42):
    """Bucket the master into 4 groups and sample n_per_group from each.

    Groups:
      ric_active    - has a RIC, IsActive=True
      ric_expired   - has a RIC, IsActive=False
      cusip_active  - has a CUSIP but NO RIC, IsActive=True
      cusip_expired - has a CUSIP but NO RIC, IsActive=False

    The CUSIP groups exclude bonds that also have a RIC -- otherwise we'd
    just be re-testing the RIC path for US bonds that have both. This way
    we isolate the CUSIP= fallback.
    """
    t0 = time.time()
    log(f"Reading master CSV (columns: {USECOLS})...")
    df = pd.read_csv(MASTER_CSV, usecols=USECOLS, dtype=str, low_memory=False)
    log(f"Loaded {len(df):,} rows in {time.time() - t0:.1f}s")

    has_ric = df["RIC"].notna() & (df["RIC"].str.strip() != "")
    has_cusip = df["CUSIP"].notna() & (df["CUSIP"].str.strip() != "")
    is_active = df["IsActive"] == "True"

    buckets = {
        "ric_active":    df[has_ric & is_active],
        "ric_expired":   df[has_ric & ~is_active],
        "cusip_active":  df[~has_ric & has_cusip & is_active],
        "cusip_expired": df[~has_ric & has_cusip & ~is_active],
    }

    log(f"Master buckets: "
        f"ric_active={len(buckets['ric_active']):,}  "
        f"ric_expired={len(buckets['ric_expired']):,}  "
        f"cusip_active={len(buckets['cusip_active']):,}  "
        f"cusip_expired={len(buckets['cusip_expired']):,}")

    picks = []
    for group_name, bucket in buckets.items():
        id_type = "RIC" if group_name.startswith("ric") else "CUSIP"
        take = min(n_per_group, len(bucket))
        sampled = bucket.sample(n=take, random_state=seed)
        for _, row in sampled.iterrows():
            picks.append((group_name, id_type, row.to_dict()))
    return picks


def describe_bond(id_type, row):
    identifier = row["RIC"] if id_type == "RIC" else row["CUSIP"]
    issuer = str(row.get("IssuerLegalName") or "?")[:30]
    cpn = row.get("CouponRate", "?")
    mat = str(row.get("MaturityDate") or "?")[:10]
    return f"{id_type}={identifier:25s} | {issuer:30s} | {cpn}% mat {mat}"


def fetch_pricing(rest, id_type, row):
    """Fetch full daily history. Returns (identifier_used, headers, rows).

    The Historical Pricing API returns a LIST containing one dict per
    instrument; we unwrap the first (and typically only) element.
    """
    if id_type == "RIC":
        identifier = row["RIC"]
    else:
        # CUSIP= format: trailing = signals CUSIP lookup
        identifier = f"{row['CUSIP']}="
    data = rest.historical_pricing(identifier, start="2000-01-01", end="2026-04-05", interval="P1D")
    item = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})
    headers = item.get("headers", [])
    rows = item.get("data", [])
    return identifier, headers, rows


def main():
    log("=" * 80)
    log("Sampling 200 bonds from master: 50 each of {ric,cusip} x {active,expired}")
    log("=" * 80)
    samples = load_samples(n_per_group=50)
    log(f"Selected {len(samples)} bonds total.")

    # Connect
    session = setup_session()
    rest = LSEGRestClient(session)

    # Per-bond results: (group, id_type, identifier_used, row_dict, n_rows, earliest, latest, error)
    results = []

    for i, (group_name, id_type, row) in enumerate(samples, 1):
        try:
            identifier, headers, rows = fetch_pricing(rest, id_type, row)
            n_rows = len(rows)
            earliest = rows[-1][0] if rows else None
            latest = rows[0][0] if rows else None
            status = f"{n_rows:>5} rows" if n_rows else "   NO DATA"
            log(f"[{i:3d}/{len(samples)}] {group_name:14s} {status}  {describe_bond(id_type, row)}  [{identifier}]")
            results.append((group_name, id_type, identifier, row, n_rows, earliest, latest, None))
        except Exception as e:
            log(f"[{i:3d}/{len(samples)}] {group_name:14s}   ERROR     {describe_bond(id_type, row)}  ({e})")
            results.append((group_name, id_type, None, row, -1, None, None, str(e)))

    # -- Per-group summary --
    log("")
    log("=" * 80)
    log("GROUP SUMMARY")
    log("=" * 80)
    log(f"{'Group':16s} {'Tested':>8s} {'With data':>11s} {'No data':>9s} {'Errors':>8s} "
        f"{'Avg rows':>10s} {'Earliest':>12s} {'Latest':>12s}")
    log("-" * 100)
    for group_name in ["ric_active", "ric_expired", "cusip_active", "cusip_expired"]:
        g = [r for r in results if r[0] == group_name]
        if not g:
            continue
        tested = len(g)
        with_data = sum(1 for r in g if r[4] > 0)
        no_data = sum(1 for r in g if r[4] == 0)
        errors = sum(1 for r in g if r[4] == -1)
        row_counts = [r[4] for r in g if r[4] > 0]
        avg_rows = int(sum(row_counts) / len(row_counts)) if row_counts else 0
        earliest_dates = [r[5] for r in g if r[5]]
        latest_dates = [r[6] for r in g if r[6]]
        min_earliest = min(earliest_dates)[:10] if earliest_dates else "-"
        max_latest = max(latest_dates)[:10] if latest_dates else "-"
        log(f"{group_name:16s} {tested:>8d} {with_data:>11d} {no_data:>9d} {errors:>8d} "
            f"{avg_rows:>10d} {min_earliest:>12s} {max_latest:>12s}")

    # -- Overall --
    log("")
    total = len(results)
    total_with_data = sum(1 for r in results if r[4] > 0)
    log(f"Overall: {total_with_data}/{total} bonds returned pricing data "
        f"({100*total_with_data/total:.1f}%)")

    ld.close_session()


if __name__ == "__main__":
    main()
