# Security Master

Security-master and symbology work lives here.

## Purpose

Build durable lookup tables that bridge internal security identifiers to
LSEG-friendly identifiers without treating RICs as the long-term anchor.

## Identifier Strategy

### US equities

Use:

- `permno` as the CRSP-side anchor
- `crsp_cusip8` as the crosswalk key
- `isin` as the canonical LSEG-friendly identifier
- `permid` as the stable LSEG-side entity identifier when available
- `primary_ric` as a market-facing alias, not the durable master key

Join rule:

`lseg_cusip9[:8] == crsp_cusip8`

### European underliers

Do not anchor on CUSIP.

Use:

- `isin` as the primary durable identifier
- `permid` as the LSEG-side stable identifier
- `primary_ric` as a market-facing alias

European underliers will be built as a separate table after the US snapshot is
validated against the Eurex SSF and SSDF underlier universe.

## Current Deliverables

### `build_secmaster.py`

Builds a current US equity snapshot plus a separate RIC-history sidecar.

Input:

- a static CRSP CSV with at least:
  - `permno`
  - one of `cusip8`, `cusip`, or `ncusip`

Outputs:

- `us_lseg_equity_universe_raw.csv`
- `us_security_master_snapshot.csv`
- `us_security_master_ric_history.csv`
- `us_security_master_unmatched.csv`

### Snapshot schema

- `permno`
- `crsp_cusip8`
- `lseg_cusip9`
- `isin`
- `permid`
- `primary_ric`
- `ticker`
- `common_name`
- `exchange_code`
- `exchange_name`
- `country_code`
- `asset_state`
- `snapshot_date`

### History sidecar schema

- `permno`
- `isin`
- `ric`
- `effective_from`
- `effective_to`
- `history_source`
- `snapshot_date`

## Scope Boundary

This directory does not own the Eurex underlier universe.

That work remains in `dividend_derivatives/`, especially:

- `enumerate_div_contracts.py`
- `EUREX_SINGLE_STOCK.md`
- `eurex_productlist.csv`

This directory consumes those universes later for validation, but does not
generate them.
