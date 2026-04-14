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

Current build path:

- start from a static CRSP current snapshot
- convert `crsp_cusip8 -> crsp_cusip9`
- map `CUSIP9 -> RIC / ISIN / PermID` through LSEG symbology in batches
- join back on `lseg_cusip9[:8] == crsp_cusip8`

Input:

- a static CRSP CSV with at least:
  - `permno`
  - one of `cusip8`, `cusip`, or `ncusip`

### `validate_us_secmaster.py`

Validates the current US security master against:

- `us_security_master_snapshot.csv`
- `us_security_master_unmatched.csv`
- `eurex_ssf_ssdf/eurex_ssf_ssdf_underliers_us.csv`

It reports:

- overall CRSP snapshot match rate
- product-level identifier agreement for the US Eurex subset
- product and stock-identity rows that still have LSEG identifiers but no CRSP `permno`

Outputs:

- `us_secmaster_validation_summary.csv`
- `us_secmaster_identifier_agreement_by_product.csv`
- `us_secmaster_unaccounted_products.csv`
- `us_secmaster_unaccounted_identities.csv`

## CSV Files

### Canonical master

The main output table is:

- `us_security_master_snapshot.csv`

This is the canonical current US security master. If another directory needs a
single lookup table for `permno -> isin / permid / primary_ric`, this is the
file it should use.

### 1. `crsp_current_us_common.csv`

Purpose:

- staging input file exported from `crsp.security_names`
- one current row per active US common-share `PERMNO`
- source snapshot used to build the LSEG-linked master

Schema:

- `permno`
- `cusip8`
- `ticker`
- `comnam`
- `shareclass`
- `usincflg`
- `issuertype`
- `securitytype`
- `securitysubtype`
- `sharetype`
- `securityactiveflg`
- `primaryexch`
- `tradingsymbol`
- `tradingstatusflg`

### 2. `us_lseg_equity_universe_raw.csv`

Purpose:

- raw LSEG symbology output after batch CUSIP conversion
- one row per successful LSEG mapping returned from `CUSIP9 -> identifiers`
- diagnostic file used to inspect raw LSEG coverage before the CRSP join

Schema:

- `lseg_cusip9`
- `lseg_cusip8`
- `RIC`
- `Isin`
- `PermID`
- `TickerSymbol`
- `CommonName`
- `ExchangeCode`
- `ExchangeName`
- `CountryCode`
- `AssetState`

### 3. `us_security_master_snapshot.csv`

Purpose:

- canonical current US security master
- one row per CRSP `PERMNO`
- best current bridge from CRSP identifiers to LSEG-friendly identifiers

Schema:

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

### 4. `us_security_master_ric_history.csv`

Purpose:

- history sidecar for the snapshot table
- point-in-time primary-RIC history keyed by `permno` and `isin`
- used when current `primary_ric` is not enough because the name changed over time

Schema:

- `permno`
- `isin`
- `ric`
- `effective_from`
- `effective_to`
- `history_source`
- `snapshot_date`

### 5. `us_security_master_unmatched.csv`

Purpose:

- diagnostic exceptions file
- rows from the CRSP input that did not match through the current LSEG bridge
- used to analyze stale CUSIPs, coverage gaps, ticker/name drift, and fallback needs

Schema:

- `permno`
- `cusip8`
- `ticker`
- `comnam`
- `shareclass`
- `usincflg`
- `issuertype`
- `securitytype`
- `securitysubtype`
- `sharetype`
- `securityactiveflg`
- `primaryexch`
- `tradingsymbol`
- `tradingstatusflg`
- `crsp_cusip8`
- `crsp_cusip9`
- `lseg_cusip8`
- `lseg_cusip9`
- `RIC`
- `Isin`
- `PermID`
- `TickerSymbol`
- `CommonName`
- `ExchangeCode`
- `ExchangeName`
- `CountryCode`
- `AssetState`
- `snapshot_date`
- `primary_ric`
- `issue`

## Scope Boundary

This directory does not own the Eurex underlier universe.

That work remains in `dividend_derivatives/`, especially:

- `enumerate_div_contracts.py`
- `EUREX_SINGLE_STOCK.md`
- `eurex_productlist.csv`

This directory consumes those universes later for validation, but does not
generate them.

## Temporary Archive

`security_master/eurex_ssf_ssdf/` is a holding area for the current generic
SSF/SSDF output artifacts copied out of `dividend_derivatives/` before they get
overwritten by a later pipeline run.

These archived files are for safekeeping only. The active generation pipeline
and its canonical scripts still live in `dividend_derivatives/`.

## Eurex SSF / SSDF Underliers

`build_eurex_ssf_ssdf_underlier_map.py` builds underlier identifier tables from:

- `dividend_derivatives/eurex_productlist.csv`
- `security_master/eurex_ssf_ssdf/instrument_master_futures.csv`
- `security_master/us_security_master_snapshot.csv`

Identifier strategy:

- universal underlier key: `share_isin`
- US cross-system key when available: `us_underlier_permno`
- LSEG identifiers for all names when available:
  - `underlier_lseg_ric`
  - `underlier_lseg_permid`

Outputs:

- `security_master/eurex_ssf_ssdf/eurex_ssf_ssdf_product_underliers.csv`
  - canonical product-level underlier map
  - one row per Eurex product ID
- `security_master/eurex_ssf_ssdf/eurex_ssf_ssdf_underliers_us.csv`
  - US subset of the product-level underlier map
- `security_master/eurex_ssf_ssdf/eurex_ssf_ssdf_underliers_non_us.csv`
  - non-US subset of the product-level underlier map
- `security_master/eurex_ssf_ssdf/eurex_ssf_ssdf_contracts_enriched.csv`
  - contract-level futures master enriched with underlier identifiers
- `security_master/eurex_ssf_ssdf/futures_daily_prices_enriched.csv`
  - daily futures prices enriched by `RIC` with underlier identifiers

Current counts from the built product-level map:

- all products: `2330`
  - `341` SSDF product IDs
  - `1989` SSF product IDs
- US products: `243`
  - `57` SSDF product IDs
  - `186` SSF product IDs
- non-US products: `2087`
  - `284` SSDF product IDs
  - `1803` SSF product IDs

These are product counts, not contract counts and not unique-underlier counts.

The clean distinction is:

- product-level: one row per Eurex product ID
- contract-level: one row per individual futures contract RIC
- underlier-level: one row per unique `share_isin`

Current unique underlier counts by `share_isin` in the product-level map:

- all products: `696`
- US products: `113`
- non-US products: `583`

So multiple Eurex product IDs can point to the same underlier. For example,
Apple SSF and Apple SSDF products both map to `share_isin = US0378331005`.

Identifier source hierarchy:

- `share_isin_productlist`
  - Eurex product list already provided `SHARE_ISIN`
- `reuters_ul_code_fallback`
  - Eurex `SHARE_ISIN` was null, so `REUTERS_UL_CODE` was resolved through LSEG
- `unresolved`
  - no stable underlier identifier recovered yet

US matching status:

- `238 / 243` US product IDs match the US security master snapshot by
  `share_isin -> isin`
- the remaining 5 US product IDs still have underlier identifiers, but do not
  belong in the current US common-share CRSP snapshot

Those 5 US-labeled Eurex products are:

- `BABF` — Alibaba
- `CEEG` — Coca-Cola Europacific Partners
- `ITAF` — Anheuser-Busch InBev ADR
- `R2CL` — Royal Caribbean Cruises Dividend Futures
- `RCLF` — Royal Caribbean Cruises

These are not missing-underlier cases. They still have:

- `share_isin`
- `reuters_ul_code`
- `underlier_lseg_ric`

They are only unmatched to the US CRSP common-share security master.

Null-`share_isin` fallback cases currently resolved through `reuters_ul_code`:

- `FLTF` → `IE00BWT6H894`
- `IAGF` → `ES0177542018`

For queries like "all Apple SSF and SSDF contracts", use:

- `eurex_ssf_ssdf_contracts_enriched.csv`
- filter on `share_isin = 'US0378331005'`
- or, for US names with a CRSP bridge, `us_underlier_permno = 14593`

For daily-price analysis, use:

- `futures_daily_prices_enriched.csv`
- same underlier filters:
  - `share_isin`
  - `us_underlier_permno` for US names
