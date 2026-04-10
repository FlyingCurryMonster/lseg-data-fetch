# Repo Issues Summary

Initial review date: 2026-04-09
Updated: 2026-04-10

This document summarizes the issues found during a repo review that included:
- reading the Claude project session logs under `/home/datafeed/.claude/projects/-home-datafeed-market-data-library-lseg-data-fetch`
- reading all Markdown files in this repo
- inspecting the main scripts and helpers in `equity_options/`, `credit/`, `dividend_derivatives/`, `shared/`, and `tests/`

The review was primarily static. Python syntax compilation passed, but live LSEG API calls were not executed during this review.

## Severity Guide

- High: likely data loss, silent incompleteness, or invalid resume behavior
- Medium: important operational or portability issue, but not immediate silent corruption
- Low: documentation drift, usability issue, or tooling weakness

## Resolved Since Initial Review

### 1. Equity options minute bars and trades now log explicit contract status

- Status: Resolved as of 2026-04-10
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
- What changed:
  - Both intraday downloaders now return and log an explicit `status` value.
  - Resume logic now only treats `ok` and `empty` entries as cleanly completed.
  - Legacy log entries are handled with a compatibility rule instead of being blindly trusted.
- Verification notes:
  - `fetch_bars()` / `fetch_ticks()` now return `status`.
  - Worker log entries now include `"status": ...`.
  - `load_completed()` now checks `status` before deciding a contract is done.

### 2. Equity options no longer write partial contract data on errored fetches

- Status: Resolved as of 2026-04-10
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
- What changed:
  - Workers now only append rows to CSV when the contract finished with `status == "ok"`.
  - Errored contracts are logged but their partial rows are not written out as if complete.
- Verification notes:
  - Both workers gate CSV writes on `if rows and status == "ok":`.

### 3. Equity options token handling now uses direct OAuth instead of the old SDK token copy path

- Status: Resolved as of 2026-04-10
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
- What changed:
  - The intraday downloaders now use direct OAuth password-grant + refresh-token handling.
  - 401 responses call `tm.on_401()`, which refreshes or fully re-authenticates.
  - The old `session._access_token` copy pattern is no longer used in these scripts.
- Verification notes:
  - `AUTH_URL` is now used directly in both files.
  - `TokenManager` has `authenticate()`, `refresh()`, and `on_401()`.

## Open Issues

### 4. Equity options resume logic is keyed too loosely and blocks reruns after `query_ric` corrections

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/download_trades.py`
- Problem:
  - Completed state is keyed only by `ric` / `base_ric` in the log-based resume helper.
  - If `contracts.csv` later contains a corrected `query_ric`, the contract still gets skipped because the base RIC already exists in the log.
  - The investigation notes suggest deleting only `om_run.log` to rerun bad index names, but `om_bars_log.jsonl` would still suppress the contracts.
- Why it matters:
  - Wrong-RIC contracts cannot be repaired cleanly with the current resume behavior.
- Recommended fix:
  - Make resume state compare both `base_ric` and the currently expected `query_ric`.
  - Rerun when the current `query_ric` differs from the most recent successful log entry.

### 5. Equity options still need a deliberate zero-bar recheck workflow

- Severity: High
- Area: `equity_options`
- Files:
  - `equity_options/download_om_minute_bars.py`
  - `equity_options/CLAUDE.md`
  - `equity_options/INDEX_RIC_INVESTIGATION.md`
- Problem:
  - The project now has many historical `bars=0` entries from before the status-logging fix.
  - Current logs are better, but legacy zero-bar outcomes are still ambiguous.
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

1. Patch equity-options resume logic so corrected `query_ric` values rerun automatically.
2. Add a zero-bar recheck workflow for prior legacy logs.
3. Patch the bond master overflow logic.
4. Remove remaining old absolute paths in gap-RIC helpers.
5. Clean up stale docs and clarify which scripts are active vs historical.
6. Add focused unit tests around resume-state interpretation and contract status handling.

## Notes

- The most important remaining equity-options issue is no longer token handling or status logging. It is resume correctness when `query_ric` changes, plus a clean recheck path for legacy zero-bar contracts.
- New intraday logs are materially safer than the old ones, but historical zero-bar outcomes still need review before being trusted.
