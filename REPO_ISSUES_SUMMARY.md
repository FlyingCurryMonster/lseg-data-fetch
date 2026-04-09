# Repo Issues Summary

Date: 2026-04-09

This document summarizes the issues found during a repo review that included:
- reading the Claude project session logs under `/home/datafeed/.claude/projects/-home-datafeed-market-data-library-lseg-data-fetch`
- reading all Markdown files in this repo
- inspecting the main scripts and helpers in `equity_options/`, `credit/`, `dividend_derivatives/`, `shared/`, and `tests/`

The review was primarily static. Python syntax compilation passed, but live LSEG API calls were not executed during this review.

## Severity Guide

- High: likely data loss, silent incompleteness, or invalid resume behavior
- Medium: important operational or portability issue, but not immediate silent corruption
- Low: documentation drift, usability issue, or tooling weakness

## Confirmed Issues

### 1. Equity options minute bars and trades can log request failures as zero-data contracts

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
- Problem:
  - The fetch loops retry on 429, 401, connection errors, and timeouts.
  - If retries are exhausted or a non-200 response remains, the code breaks out and returns whatever rows were collected so far.
  - The worker then logs the contract as completed with `bars=0` or `ticks=0` if no rows were returned, without preserving that the real outcome was an error.
- Why it matters:
  - Zero bars can mean "true empty contract" or "request/auth/network failure."
  - This contaminates resume state and makes later investigation of zero-bar contracts ambiguous.
- Recommended fix:
  - Return explicit per-contract status values such as `ok`, `empty`, `auth_error`, `http_error`, `network_error`, `partial_error`.
  - Only treat `ok` and `empty` as terminal clean outcomes.

### 2. Equity options fetchers can write partial contract data and still mark the contract done

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
- Problem:
  - If pagination succeeds for some pages and later fails, the code still returns the partial rows already accumulated.
  - The worker writes those rows to CSV and logs the contract as complete.
- Why it matters:
  - A contract can be partially downloaded with no reliable indication that history is incomplete.
- Recommended fix:
  - Buffer rows in memory and only write them if the contract completed cleanly.
  - If any page fails terminally, log the contract as errored and do not append partial output.

### 3. Equity options token refresh path is not robust enough for long runs

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
  - `shared/lseg_rest_api.py`
- Problem:
  - On 401, the minute-bar and trades downloaders call a local `refresh()` method that only rereads `session._access_token`.
  - That does not guarantee the SDK has actually refreshed the token.
  - `shared/lseg_rest_api.py` is stronger because it attempts `update_access_token()`, but the intraday downloaders do not.
- Why it matters:
  - Long-running jobs can drift into auth failure and quietly produce zero-data or incomplete results.
- Recommended fix:
  - Call the SDK refresh method if available, or reopen the session on 401 before retrying.
  - Treat repeated 401s as explicit contract/job errors, not empty data.

### 4. Equity options resume logic is keyed too loosely and blocks reruns after `query_ric` corrections

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
  - `equity_options/INDEX_RIC_INVESTIGATION.md`
- Problem:
  - Completed state is keyed only by `ric` / `base_ric` in the log-based resume helper.
  - If `contracts.csv` later contains a corrected `query_ric`, the contract still gets skipped because the base RIC already exists in the log.
  - The investigation notes suggest deleting only `om_run.log` to rerun bad index names, but `om_bars_log.jsonl` would still suppress the contracts.
- Why it matters:
  - Wrong-RIC contracts cannot be repaired cleanly with the current resume behavior.
- Recommended fix:
  - Make resume state compare both `base_ric` and the currently expected `query_ric`.
  - Rerun when the current `query_ric` differs from the most recent successful log entry.

### 5. Equity options need a deliberate zero-bar recheck workflow

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/CLAUDE.md`
  - `equity_options/INDEX_RIC_INVESTIGATION.md`
- Problem:
  - The project now has many historical `bars=0` entries, but the current log format does not distinguish true empty contracts from fetch failures.
  - Some zero-bar clusters are likely real, but others may reflect bad RICs or transient failures.
- Why it matters:
  - The existing dataset likely contains a mix of real empties and false empties.
- Recommended fix:
  - Add a repair/recheck pass that extracts prior zero-bar contracts and reruns them under the patched downloader.
  - Track rechecks in a separate log or revisioned status field.

### 6. Bond master downloader can silently truncate dense slices

- Severity: High
- Area: `credit`
- Files:
  - `credit/download_bond_master.py`
- Problem:
  - The docstring says that 1-day windows should split by `DbType` and then by currency if still too large.
  - The implementation instead jumps directly to currency when a 1-day window is still over 10K.
  - If a currency slice is still over 10K, the script logs a warning, downloads only the first 10K hits, and marks that window complete.
- Why it matters:
  - The bond security master can be incomplete without failing loudly.
- Recommended fix:
  - Implement the documented split order.
  - Treat any still-overflowing slice as unresolved rather than "done."

## Operational / Portability Issues

### 7. Gap-RIC helper scripts still contain old-machine hardcoded absolute paths

- Severity: Medium
- Area: `equity_options/expired_options_search/eof_scripts`
- Files:
  - `equity_options/expired_options_search/eof_scripts/parse_cboe_snapshot.py`
  - `equity_options/expired_options_search/eof_scripts/build_top10_gap_rics.py`
- Problem:
  - These scripts still point to `/home/rakin/...` and an old Claude tool-results path.
- Why it matters:
  - The intraday downloader may be portable now, but parts of the contract-generation workflow are not reproducible on this machine.
- Recommended fix:
  - Replace absolute paths with repo-relative locations or explicit CLI arguments.

### 8. Intraday status shell script is brittle

- Severity: Medium
- Area: `equity_options`
- Files:
  - `equity_options/intraday_download_status.sh`
- Problem:
  - The script hardcodes an absolute `cd`.
  - It assumes a specific `ps aux` field layout and infers the active ticker from `awk '{print $13}'`.
  - It only recognizes `download_trades.py`, not other trade-related processes.
- Why it matters:
  - Operational monitoring is fragile and can mislead you about what is actually running.
- Recommended fix:
  - Resolve paths relative to the script directory.
  - Parse process command lines more defensibly.
  - Explicitly say when no matching trades downloader is running.

### 9. Many scripts in the repo are still exploratory rather than productionized

- Severity: Medium
- Area: whole repo
- Files:
  - especially `dividend_derivatives/`, `credit/tests/`, `archive/`, and `equity_options/expired_options_search/eof_scripts/`
- Problem:
  - The repo contains a mix of active pipelines, exploratory scripts, old probes, and archival code.
  - There is no consistent boundary between "current production tool" and "historical experiment."
- Why it matters:
  - It is easy to use the wrong script or rely on stale logic.
- Recommended fix:
  - Mark active scripts clearly and move superseded exploratory files into more explicit archive locations.

## Testing / Validation Issues

### 10. Most `test_*.py` files are live integration scripts, not isolated tests

- Severity: Medium
- Area: `tests/`, `credit/tests/`
- Files:
  - `tests/test_lseg.py`
  - `tests/test_rest_api.py`
  - most `credit/tests/test_*.py`
- Problem:
  - Many scripts named like tests run live API requests, depend on credentials, and execute work at import time.
  - This is not a stable automated regression suite.
- Why it matters:
  - Running `pytest` is not a reliable signal of repo health.
  - Some files also create temporary credential-bearing config files during execution.
- Recommended fix:
  - Separate exploratory scripts from real tests.
  - Add small pure unit tests for parsing, resume-state logic, status handling, and contract-loading behavior.

### 11. There is limited automated validation around resume-state correctness

- Severity: Medium
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
  - `equity_options/build_ticker_contracts.py`
- Problem:
  - The most operationally important logic in the repo is resume behavior, but there are no focused tests around:
    - log parsing
    - log precedence
    - `query_ric` changes
    - partial contract retries
    - terminal vs non-terminal status classification
- Why it matters:
  - Regressions in resume behavior are easy to introduce and hard to detect after the fact.
- Recommended fix:
  - Add unit tests for log interpretation and rerun eligibility.

## Documentation Drift / Repo Memory Issues

### 12. `CLAUDE.md` is the most current source of truth, but several other docs are stale

- Severity: Low
- Area: docs
- Files:
  - `CLAUDE.md`
  - `equity_options/HANDOFF_OM_MINUTE_BARS.md`
  - `equity_options/TODO.md`
  - `equity_options/notes.md`
- Problem:
  - `CLAUDE.md` reflects the current full-history, three-source intraday pipeline.
  - Other docs still describe earlier phases such as:
    - 2-week bar limits
    - OptionMetrics-only workflows
    - old working directories under `wrds-data`
    - older notions of what is still unresolved
- Why it matters:
  - Operational decisions can be made from outdated assumptions.
- Recommended fix:
  - Either update these docs to match the current pipeline or explicitly mark them historical.

### 13. Some notes still reference the old `LSEG datastream` repo layout

- Severity: Low
- Area: docs
- Files:
  - `dividend_derivatives/NOTES.md`
  - `equity_options/notes.md`
  - `equity_options/HANDOFF_OM_MINUTE_BARS.md`
- Problem:
  - Several docs still refer to the old repo name and paths from before the reorganization.
- Why it matters:
  - This increases confusion when resuming work on the datafeed machine.
- Recommended fix:
  - Normalize documentation to the current repo path and directory structure.

### 14. Claude session history shows several repo-migration fixes were applied unevenly

- Severity: Low
- Area: repo organization
- Source:
  - Claude session logs in `/home/datafeed/.claude/projects/-home-datafeed-market-data-library-lseg-data-fetch`
- Problem:
  - The session history shows deliberate work to fix imports and hardcoded paths after moving from `wrds-data` to `market data library`.
  - Some of those fixes landed in active scripts, but not all historical helpers and docs were updated.
- Why it matters:
  - The repo looks more fully migrated than it actually is.
- Recommended fix:
  - Do one final migration sweep specifically for path assumptions and stale references.

## Suggested Fix Order

1. Patch equity-options fetch status handling and partial-write behavior.
2. Patch equity-options token refresh and resume logic.
3. Add a zero-bar recheck workflow for prior logs.
4. Patch the bond master overflow logic.
5. Remove remaining old absolute paths in gap-RIC helpers.
6. Clean up stale docs and clarify which scripts are active vs historical.

## Notes

- The most important practical issue in the repo is not documentation drift. It is that the current intraday downloader can confuse request failures with true zero-data contracts.
- If work resumes on the equity-options jobs, the downloader should be patched before trusting new `bars=0` or `ticks=0` outcomes.
