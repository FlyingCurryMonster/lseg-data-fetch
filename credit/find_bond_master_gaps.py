"""Find gaps in bond security master by comparing API counts vs local CSV counts.

Strategy:
  1. Year by year: compare API count vs deduped CSV count
  2. For years with gaps: drill into months
  3. Report gap months for targeted re-download
"""

import sys, os, json, time, requests
import pandas as pd
from datetime import date
from dotenv import load_dotenv

# --- Config ---
SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token"
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bond_security_master_deduped.csv")
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bond_master_gap_check.log")

START_YEAR = 1970
END_YEAR = 2026


# =====================================================================
# Auth (same as download script)
# =====================================================================

class TokenManager:
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

    def refresh(self):
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
                    return
            except Exception:
                pass
        self.authenticate()

    def ensure_valid(self):
        if time.time() > self._token_expiry:
            self.refresh()

    def headers(self):
        self.ensure_valid()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def api_search(tm, payload):
    for attempt in range(3):
        resp = requests.post(SEARCH_URL, headers=tm.headers(), json=payload)
        if resp.status_code == 401 and attempt < 2:
            tm.refresh()
            continue
        resp.raise_for_status()
        return resp.json()


def search_count(tm, filt):
    result = api_search(tm, {"Query": "", "View": "GovCorpInstruments", "Filter": filt, "Top": 0})
    return result.get("Total", 0)


def date_filter(start, end):
    return f"IssueDate ge {start.isoformat()} and IssueDate lt {end.isoformat()}"


# =====================================================================
# Main
# =====================================================================

def main():
    log("Loading deduped CSV...")
    df = pd.read_csv(CSV_PATH, usecols=["IssueDate"], low_memory=False)
    df["IssueDate"] = pd.to_datetime(df["IssueDate"], errors="coerce")
    df["year"] = df["IssueDate"].dt.year
    df["month"] = df["IssueDate"].dt.month
    local_year_counts = df.groupby("year").size().to_dict()
    log(f"Loaded {len(df):,} rows")

    log("Authenticating...")
    tm = TokenManager()
    log("Authenticated")

    # Also check pre-1970 bonds
    pre_1970_api = search_count(tm, f"IssueDate lt 1970-01-01")
    pre_1970_local = int(df[df["IssueDate"].dt.year < 1970].shape[0]) if not df.empty else 0
    null_local = int(df["IssueDate"].isna().sum())

    log(f"{'YEAR':<8} {'API':>10} {'LOCAL':>10} {'GAP':>10} {'STATUS'}")
    log("-" * 55)

    if pre_1970_api > 0 or pre_1970_local > 0:
        gap = pre_1970_api - pre_1970_local
        status = "MATCH" if gap == 0 else f"MISSING {gap}"
        log(f"{'<1970':<8} {pre_1970_api:>10,} {pre_1970_local:>10,} {gap:>10,} {status}")

    gap_years = []
    total_api = pre_1970_api
    total_local = pre_1970_local

    for year in range(START_YEAR, END_YEAR + 1):
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1) if year < END_YEAR else date(year, 7, 1)
        api_count = search_count(tm, date_filter(start, end))
        local_count = int(local_year_counts.get(year, 0))
        gap = api_count - local_count
        total_api += api_count
        total_local += local_count

        status = "MATCH" if gap == 0 else ("EXTRA" if gap < 0 else f"MISSING {gap}")
        log(f"{year:<8} {api_count:>10,} {local_count:>10,} {gap:>10,} {status}")

        if gap > 0:
            gap_years.append((year, api_count, local_count, gap))

    log("-" * 55)
    log(f"{'TOTAL':<8} {total_api:>10,} {total_local:>10,} {total_api - total_local:>10,}")
    if null_local > 0:
        log(f"(Plus {null_local:,} rows with null IssueDate in local CSV)")

    # Drill into gap years by month
    if gap_years:
        log(f"{'='*60}")
        log(f"DRILLING INTO {len(gap_years)} GAP YEARS BY MONTH")
        log(f"{'='*60}")

        gap_months = []
        for year, api_yr, local_yr, gap_yr in gap_years:
            log(f"--- {year} (gap: {gap_yr:,}) ---")

            max_month = 6 if year == END_YEAR else 12
            for month in range(1, max_month + 1):
                start = date(year, month, 1)
                if month < 12:
                    end = date(year, month + 1, 1)
                else:
                    end = date(year + 1, 1, 1)

                api_count = search_count(tm, date_filter(start, end))
                local_count = int(df[(df["year"] == year) & (df["month"] == month)].shape[0])
                gap = api_count - local_count

                status = "MATCH" if gap == 0 else ("EXTRA" if gap < 0 else f"MISSING {gap}")
                log(f"  {year}-{month:02d}  api={api_count:>8,}  local={local_count:>8,}  gap={gap:>8,}  {status}")

                if gap > 0:
                    gap_months.append((year, month, api_count, local_count, gap))

        # Summary
        log(f"{'='*60}")
        log(f"GAP SUMMARY: {len(gap_months)} months with missing data")
        log(f"{'='*60}")
        total_gap = 0
        for year, month, api_c, local_c, gap in gap_months:
            log(f"  {year}-{month:02d}: {gap:,} missing (API={api_c:,}, local={local_c:,})")
            total_gap += gap
        log(f"  TOTAL MISSING: {total_gap:,}")

        # Save gap report
        report = {"gap_months": [
            {"year": y, "month": m, "api_count": a, "local_count": l, "gap": g}
            for y, m, a, l, g in gap_months
        ]}
        report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gap_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        log(f"Saved gap report to {report_path}")
    else:
        log("No gaps found — security master is complete!")


if __name__ == "__main__":
    main()
