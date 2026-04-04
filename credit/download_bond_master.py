"""Download the full LSEG bond security master with adaptive window sizing.

Usage:
    python download_bond_master.py

Iterates through issue date windows (starting at 6 months), shrinking
adaptively if a window exceeds 10K results:
    6mo → 5mo → 4mo → 3mo → 2mo → 1mo → 3wk → 2wk → 1wk → 4d → 1d

If 1-day windows still exceed 10K, splits by DbType (CORP/GOVT/AGNC/OMUN/OTHR).
If a single DbType in a 1-day window still exceeds 10K, splits by currency.

Resumable: completed windows are logged to bond_master_log.jsonl.
Resumes from latest IssueDate in existing CSV.
Output: bond_security_master.csv (appended incrementally).
"""

import sys, os, json, csv, time, requests
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# --- Config ---
OUTDIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(OUTDIR, "bond_security_master.csv")
LOG_PATH = os.path.join(OUTDIR, "bond_master_log.jsonl")
PROGRESS_PATH = os.path.join(OUTDIR, "bond_master_progress.log")

MAX_RESULTS = 10000
SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token"

SELECT_FIELDS = (
    "RIC,ISIN,CUSIP,SEDOL,"
    "IssuerLegalName,IssuerCommonName,IssuerOAPermid,"
    "CouponRate,MaturityDate,IssueDate,"
    "FaceIssuedUSD,EOMAmountOutstanding,"
    "RCSBondGradeLeaf,CdsSeniorityEquivalentDescription,"
    "RCSCurrencyLeaf,RCSCountryLeaf,RCSIssuerCountryLeaf,"
    "InstrumentTypeDescription,DbTypeDescription,DbType,"
    "AssetStatus,IsActive,"
    "RCSTRBC2012Leaf,RCSTRBC2012Name,"
    "IsConvertible,IsPerpetualSecurity,IsGreenBond"
)

CSV_COLUMNS = [
    "RIC", "ISIN", "CUSIP", "SEDOL",
    "IssuerLegalName", "IssuerCommonName", "IssuerOAPermid",
    "CouponRate", "MaturityDate", "IssueDate",
    "FaceIssuedUSD", "EOMAmountOutstanding",
    "RCSBondGradeLeaf", "CdsSeniorityEquivalentDescription",
    "RCSCurrencyLeaf", "RCSCountryLeaf", "RCSIssuerCountryLeaf",
    "InstrumentTypeDescription", "DbTypeDescription", "DbType",
    "AssetStatus", "IsActive",
    "RCSTRBC2012Leaf", "RCSTRBC2012Name",
    "IsConvertible", "IsPerpetualSecurity", "IsGreenBond",
]

WINDOW_STEPS = [
    relativedelta(months=6),
    relativedelta(months=5),
    relativedelta(months=4),
    relativedelta(months=3),
    relativedelta(months=2),
    relativedelta(months=1),
    timedelta(weeks=3),
    timedelta(weeks=2),
    timedelta(weeks=1),
    timedelta(days=4),
    timedelta(days=1),
]

DB_TYPES = ["CORP", "AGNC", "GOVT", "OTHR", "OMUN"]

TOP_CURRENCIES = [
    "US Dollar", "Euro", "Colombian Peso", "Hong Kong Dollar",
    "Japanese Yen", "Chinese Yuan", "South Korean Won", "Canadian Dollar",
    "British Pound", "Singapore Dollar", "Australian Dollar", "Indian Rupee",
    "Brazilian Real", "Deutsche Mark", "Swiss Franc", "Chilean Peso",
    "Mexican Peso", "Norwegian Krone", "Swedish Krona", "South African Rand",
    "New Taiwan Dollar", "Malaysian Ringgit", "Indonesian Rupiah",
    "Thai Baht", "Philippine Peso", "New Zealand Dollar", "Turkish Lira",
    "Polish Zloty", "Czech Koruna", "Hungarian Forint",
]

START_DATE = date(1970, 1, 1)
END_DATE = date(2026, 7, 1)


# =====================================================================
# Auth — direct REST, no SDK
# =====================================================================

class TokenManager:
    """Manages LSEG auth tokens via direct REST calls."""

    def __init__(self):
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
        self._app_key = os.getenv("DSWS_APPKEY")
        self._username = os.getenv("DSWS_USERNAME")
        self._password = os.getenv("DSWS_PASSWORD")
        self._token = None
        self._refresh_token_str = None
        self._token_expiry = 0
        self.authenticate()

    def authenticate(self):
        """Get initial token via password grant."""
        resp = requests.post(AUTH_URL, data={
            "grant_type": "password",
            "username": self._username,
            "password": self._password,
            "client_id": self._app_key,
            "scope": "trapi",
            "takeExclusiveSignOnControl": "true",
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._refresh_token_str = data.get("refresh_token")
        self._token_expiry = time.time() + int(data.get("expires_in", 300)) - 30
        log_progress(f"  Authenticated (token expires in {data.get('expires_in', '?')}s)")

    def refresh(self):
        """Refresh token, falling back to full re-auth."""
        if self._refresh_token_str:
            try:
                resp = requests.post(AUTH_URL, data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token_str,
                    "client_id": self._app_key,
                }, headers={"Content-Type": "application/x-www-form-urlencoded"})
                if resp.status_code == 200:
                    data = resp.json()
                    self._token = data["access_token"]
                    self._refresh_token_str = data.get("refresh_token", self._refresh_token_str)
                    self._token_expiry = time.time() + int(data.get("expires_in", 300)) - 30
                    log_progress(f"  Token refreshed (expires in {data.get('expires_in', '?')}s)")
                    return
            except Exception:
                pass
        # Fallback: full re-auth
        self.authenticate()

    def ensure_valid(self):
        """Proactively refresh if token is about to expire."""
        if time.time() > self._token_expiry:
            self.refresh()

    def headers(self):
        self.ensure_valid()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }


# =====================================================================
# Helpers
# =====================================================================

def log_progress(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(PROGRESS_PATH, "a") as f:
        f.write(line + "\n")


def load_completed_windows():
    completed = set()
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                completed.add(entry["window_key"])
    return completed


def log_completed_window(window_key, count):
    entry = {"window_key": window_key, "count": count, "ts": time.strftime("%Y-%m-%dT%H:%M:%S")}
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def ensure_csv_header():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()


def append_to_csv(hits):
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        for hit in hits:
            writer.writerow(hit)


def api_search(tm, payload):
    """Execute search with retry on 401."""
    for attempt in range(3):
        resp = requests.post(SEARCH_URL, headers=tm.headers(), json=payload)
        if resp.status_code == 401 and attempt < 2:
            log_progress(f"  401 on search, refreshing token (attempt {attempt + 1})...")
            tm.refresh()
            continue
        resp.raise_for_status()
        return resp.json()


def search_count(tm, filt):
    result = api_search(tm, {"Query": "", "View": "GovCorpInstruments", "Filter": filt, "Top": 0})
    return result.get("Total", 0)


def search_download(tm, filt):
    result = api_search(tm, {
        "Query": "", "View": "GovCorpInstruments",
        "Filter": filt, "Select": SELECT_FIELDS, "Top": MAX_RESULTS,
    })
    return result.get("Hits", []), result.get("Total", 0)


def date_filter(start, end):
    return f"IssueDate ge {start.isoformat()} and IssueDate lt {end.isoformat()}"


def step_to_days(step):
    if isinstance(step, timedelta):
        return step.days
    return step.months * 30 + getattr(step, 'days', 0)


def pick_step_for_count(count, current_days):
    if count <= MAX_RESULTS:
        return None
    target_days = max(1, int(current_days * 8000 / count))
    for i, step in enumerate(WINDOW_STEPS):
        step_days = step_to_days(step)
        if step_days <= target_days * 1.5 and step_days <= current_days * 0.8:
            return i
    return len(WINDOW_STEPS) - 1


def find_resume_date():
    if not os.path.exists(CSV_PATH):
        return START_DATE
    latest = None
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row.get("IssueDate", "")[:10]
            if d and len(d) == 10:
                if latest is None or d > latest:
                    latest = d
    if latest:
        return date.fromisoformat(latest)
    return START_DATE


# =====================================================================
# Download logic
# =====================================================================

def download_with_split(tm, start, end, completed, split_by="currency"):
    """Split a date window by a dimension to get under 10K per slice.

    Args:
        split_by: "currency" or "dbtype"
    """
    if split_by == "currency":
        buckets = [(ccy, f"RCSCurrencyLeaf eq '{ccy}'") for ccy in TOP_CURRENCIES]
        excl = " and ".join(f"RCSCurrencyLeaf ne '{c}'" for c in TOP_CURRENCIES)
        other_filt = excl
    else:
        buckets = [(db, f"DbType eq '{db}'") for db in DB_TYPES]
        other_filt = " and ".join(f"DbType ne '{db}'" for db in DB_TYPES)

    total = 0
    for label, extra_filt in buckets:
        window_key = f"{start.isoformat()}_{end.isoformat()}_{split_by}={label}"
        if window_key in completed:
            continue
        filt = f"{date_filter(start, end)} and {extra_filt}"
        count = search_count(tm, filt)
        if count == 0:
            log_completed_window(window_key, 0)
            completed.add(window_key)
            continue
        if count <= MAX_RESULTS:
            hits, _ = search_download(tm, filt)
            append_to_csv(hits)
            log_completed_window(window_key, len(hits))
            completed.add(window_key)
            log_progress(f"    {start} to {end} [{label}]: {len(hits):,} bonds")
            total += len(hits)
        else:
            log_progress(f"    WARNING: {start} [{label}] has {count:,} > 10K, downloading first 10K")
            hits, _ = search_download(tm, filt)
            append_to_csv(hits)
            log_completed_window(window_key, len(hits))
            completed.add(window_key)
            total += len(hits)

    # Catch anything not in our bucket list
    window_key = f"{start.isoformat()}_{end.isoformat()}_{split_by}=OTHER"
    if window_key not in completed:
        filt = f"{date_filter(start, end)} and {other_filt}"
        count = search_count(tm, filt)
        if count == 0:
            log_completed_window(window_key, 0)
            completed.add(window_key)
        elif count <= MAX_RESULTS:
            hits, _ = search_download(tm, filt)
            append_to_csv(hits)
            log_completed_window(window_key, len(hits))
            completed.add(window_key)
            log_progress(f"    {start} to {end} [OTHER]: {len(hits):,} bonds")
            total += len(hits)
        else:
            log_progress(f"    WARNING: {start} [OTHER] has {count:,} > 10K, downloading first 10K")
            hits, _ = search_download(tm, filt)
            append_to_csv(hits)
            log_completed_window(window_key, len(hits))
            completed.add(window_key)
            total += len(hits)
    return total


def process_chunk(tm, start, end, step_idx, completed):
    window_key = f"{start.isoformat()}_{end.isoformat()}"
    if window_key in completed:
        return 0, step_idx

    filt = date_filter(start, end)
    count = search_count(tm, filt)

    if count == 0:
        log_completed_window(window_key, 0)
        completed.add(window_key)
        return 0, step_idx

    if count <= MAX_RESULTS:
        hits, total = search_download(tm, filt)
        append_to_csv(hits)
        log_completed_window(window_key, len(hits))
        completed.add(window_key)
        log_progress(f"  {start} to {end}: {len(hits):,} bonds")
        return len(hits), step_idx

    current_days = (end - start).days

    # If already a 1-day window, can't shrink further — split by currency
    if current_days <= 1:
        log_progress(f"  {start} to {end}: {count:,} bonds > 10K on single day, splitting by currency")
        downloaded = download_with_split(tm, start, end, completed, split_by="currency")
        return downloaded, len(WINDOW_STEPS) - 1

    new_idx = pick_step_for_count(count, current_days)

    sub_step = WINDOW_STEPS[new_idx]
    step_label = f"{step_to_days(sub_step)}d"
    log_progress(f"  {start} to {end}: {count:,} bonds > 10K, using {step_label} sub-windows")

    total_downloaded = 0
    cursor = start
    child_idx = new_idx
    while cursor < end:
        sub_end = min(cursor + sub_step, end)
        downloaded, child_idx = process_chunk(tm, cursor, sub_end, new_idx, completed)
        total_downloaded += downloaded
        cursor = sub_end

    return total_downloaded, new_idx


def main():
    tm = TokenManager()

    completed = load_completed_windows()
    ensure_csv_header()

    resume_date = find_resume_date()
    if resume_date > START_DATE:
        log_progress(f"Resuming from {resume_date} (latest IssueDate in existing CSV)")

    log_progress(f"Starting bond master download. {len(completed)} windows already completed.")

    total_downloaded = 0
    cursor = resume_date
    step_idx = 0

    try:
        while cursor < END_DATE:
            step = WINDOW_STEPS[step_idx]
            window_end = min(cursor + step, END_DATE)
            downloaded, step_idx = process_chunk(tm, cursor, window_end, step_idx, completed)
            total_downloaded += downloaded
            cursor = window_end

            if step_idx > 0:
                step_idx -= 1
    except KeyboardInterrupt:
        log_progress(f"Interrupted. Total downloaded this session: {total_downloaded:,}")
    except Exception as e:
        log_progress(f"Error: {e}")
        raise
    finally:
        log_progress(f"Session complete. Total downloaded: {total_downloaded:,}")


if __name__ == "__main__":
    main()
