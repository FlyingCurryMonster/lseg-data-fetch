# Eurex SSF / SSDF Archive

This directory is a temporary holding area for the current SSF/SSDF output
artifacts copied from `dividend_derivatives/` on 2026-04-13.

Purpose:

- preserve the current generic output files before a later rerun overwrites them
- keep the active pipeline in `dividend_derivatives/` unchanged
- give the security-master work a stable place to reference this snapshot later

Source directory:

- `dividend_derivatives/`

Archived files:

- `enumerated_futures.csv`
- `instrument_master_futures.csv`
- `futures_daily_prices.csv`
- `futures_download_log.jsonl`
- `futures_download.log`
- `enumerate_run.log`
- `enumerate_run2.log`
- `download_run.log`

This is not the active pipeline location. Regeneration still happens from the
scripts in `dividend_derivatives/`.
