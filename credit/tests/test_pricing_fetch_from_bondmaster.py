"""Test pricing fetch on 10 random bonds from the security master.

Picks 5 active + 5 inactive bonds (that have RICs), then tries to pull
full daily price history for each via the historical-pricing REST API.
Reports: fields returned, row count, earliest/latest date, and a few
sample rows so we can eyeball what we're getting.
"""

import sys, os, csv, random, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import lseg.data as ld
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


# -- Load master and sample 10 bonds --

def load_samples(n_active=5, n_inactive=5, seed=42):
    active, inactive = [], []
    with open(MASTER_CSV) as f:
        for row in csv.DictReader(f):
            ric = row.get("RIC", "").strip()
            if not ric:
                continue
            if row.get("IsActive") == "True":
                active.append(row)
            else:
                inactive.append(row)

    random.seed(seed)
    picks = []
    picks += random.sample(active, min(n_active, len(active)))
    picks += random.sample(inactive, min(n_inactive, len(inactive)))
    return picks


def describe_bond(row):
    ric = row["RIC"]
    issuer = (row.get("IssuerLegalName") or "?")[:35]
    cpn = row.get("CouponRate", "?")
    mat = (row.get("MaturityDate") or "?")[:10]
    issued = (row.get("IssueDate") or "?")[:10]
    active = row.get("IsActive", "?")
    return f"{ric:30s} | {issuer:35s} | {cpn}% | issued {issued} mat {mat} | active={active}"


def fetch_pricing(rest, ric):
    """Fetch full daily history for a single RIC. Returns (headers, rows)."""
    data = rest.historical_pricing(ric, start="2000-01-01", end="2026-04-05", interval="P1D")
    headers = data.get("headers", [])
    rows = data.get("data", [])
    return headers, rows


def main():
    log("Loading bond master and sampling 10 bonds (5 active, 5 inactive)...")
    samples = load_samples()
    log(f"Selected {len(samples)} bonds.")

    # Connect
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lseg-data.config.json")
    session = ld.open_session(config_name=config_path)
    rest = LSEGRestClient(session)

    results_summary = []

    for i, row in enumerate(samples, 1):
        ric = row["RIC"]
        log(f"{'='*80}")
        log(f"[{i}/{len(samples)}] {describe_bond(row)}")
        log(f"{'='*80}")

        try:
            headers, rows = fetch_pricing(rest, ric)

            if not rows:
                log("  => NO DATA returned")
                results_summary.append((ric, row.get("IsActive"), 0, None, None))
                continue

            field_names = [h.get("name", h.get("title", "?")) for h in headers]
            # rows are newest-first; last element is earliest
            earliest = rows[-1][0] if rows else "?"
            latest = rows[0][0] if rows else "?"

            log(f"  Fields ({len(field_names)}): {field_names}")
            log(f"  Rows:     {len(rows)}")
            log(f"  Earliest: {earliest}")
            log(f"  Latest:   {latest}")
            log(f"  First 3 rows (newest):")
            for r in rows[:3]:
                log(f"    {r}")
            log(f"  Last 3 rows (oldest):")
            for r in rows[-3:]:
                log(f"    {r}")

            results_summary.append((ric, row.get("IsActive"), len(rows), earliest, latest))

        except Exception as e:
            log(f"  => ERROR: {e}")
            results_summary.append((ric, row.get("IsActive"), -1, None, str(e)))

    # -- Summary table --
    log(f"{'='*80}")
    log("SUMMARY")
    log(f"{'='*80}")
    log(f"{'RIC':30s} {'Active':8s} {'Rows':>8s} {'Earliest':>25s} {'Latest':>25s}")
    log("-" * 100)
    for ric, active, count, earliest, latest in results_summary:
        count_str = str(count) if count >= 0 else "ERROR"
        log(f"{ric:30s} {str(active):8s} {count_str:>8s} {str(earliest or ''):>25s} {str(latest or ''):>25s}")

    ld.close_session()


if __name__ == "__main__":
    main()
