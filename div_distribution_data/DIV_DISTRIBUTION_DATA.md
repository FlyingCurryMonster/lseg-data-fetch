# Dividend Distribution Data

## Purpose

This directory now owns the canonical dividend-distribution workflow for the
complete stock universe.

The complete stock universe is:

- all CRSP names that currently have LSEG identifiers in the US security master
- plus all Eurex SSF / SSDF underlier stock identities not already in that CRSP set

This directory no longer uses the old US-Eurex-only builders as canonical
entrypoints.

## Canonical Builders

### 1. Complete Stock Universe

Builder:

- `div_distribution_data/build_complete_stock_universe.py`

Purpose:

- create one stock-universe manifest for dividend work
- anchor on `share_isin`
- preserve `permno` where CRSP coverage exists
- preserve current LSEG access identifiers and source flags
- exclude non-single-stock Eurex basket underliers from the stock universe

Current build result:

- `3,716` CRSP identifier-complete stock rows
- `697` Eurex stock identities before exclusions
- `8` excluded non-single-stock basket rows
- `4,296` rows in the final complete stock universe

Outputs:

- `div_distribution_data/complete_stock_universe.csv`
- `div_distribution_data/complete_stock_universe_summary.csv`
- `div_distribution_data/complete_stock_universe_excluded_non_single_stock.csv`
- `div_distribution_data/complete_stock_universe_crsp_unmatched_exceptions.csv`

Important fields in `complete_stock_universe.csv`:

- `share_isin`
- `permno`
- `ticker`
- `underlier_name`
- `current_primary_ric`
- `current_permid`
- `lseg_query_key`
- `lseg_query_key_type`
- `query_key_fallback_used`
- `country_code`
- `products`
- `product_groups`
- `reuters_ul_codes`
- `identifier_status`
- `in_crsp_secmaster`
- `in_eurex_underliers`
- `in_eurex_us_underliers`
- `in_eurex_non_us_underliers`
- `source_universe`
- `universe_category`

Query-key behavior:

- first choice: `current_primary_ric`
- fallback: `reuters_ul_code` where available
- final fallback: `share_isin`

Current CRSP gap:

- `340` CRSP rows still have no LSEG identifier bridge in the current security
  master
- those remain explicit exceptions in:
  `div_distribution_data/complete_stock_universe_crsp_unmatched_exceptions.csv`

### 2. Canonical CRSP Distribution History

Builder:

- `div_distribution_data/build_us_crsp_distribution_history.py`

Purpose:

- build the canonical CRSP dividend and split history for every stock in the
  complete stock universe that has `permno`
- this is the canonical US-side event history
- this is where split / adjustment factor logic lives
- this is the first event-history build step

Source:

- `crsp.distributions`

Current source coverage observed in the local CRSP database:

- roughly `1.1` million rows
- date range `1926-01-04` to `2026-02-27`

Outputs:

- `div_distribution_data/us_crsp_name_universe.csv`
- `div_distribution_data/us_crsp_distribution_events.csv`
- `div_distribution_data/us_crsp_split_events.csv`
- `div_distribution_data/us_crsp_adjustment_summary.csv`

Important fields in `us_crsp_distribution_events.csv`:

- `permno`
- `share_isin`
- `ticker`
- `primary_ric`
- `underlier_name`
- `products`
- `product_groups`
- `universe_category`
- `ex_date`
- `declare_date`
- `record_date`
- `pay_date`
- `dividend_amount`
- `dis_type`
- `dis_detail_type`
- `dis_fac_pr`
- `dis_fac_shr`
- `event_price_multiplier`
- `event_share_multiplier`
- `has_adjustment_event`
- `has_cash_distribution`
- `cum_price_factor_to_present`
- `cum_share_factor_to_present`
- `dividend_amount_adj_to_present`

Adjustment logic:

- `event_price_multiplier = 1 / (1 + DisFacPr)` when `DisFacPr > 0`
- `event_share_multiplier = 1 + DisFacShr` when `DisFacShr > 0`
- cumulative factors are built in reverse so historical events can be restated on
  the current-share basis

CRSP remains the canonical source for US dividend history where `permno` exists.

### 3. Complete-Universe LSEG Distribution History

Builder:

- `div_distribution_data/build_complete_lseg_distribution_history.py`

Purpose:

- build one canonical LSEG dividend-history file for the complete stock universe
- cover both CRSP-overlap names and Eurex-only non-US names
- this is the second event-history build step
- after the full LSEG event file exists, build the CRSP-vs-LSEG comparison layer
  for all overlap names with `permno`

Source:

- LSEG `TR.Div*` fields through `lseg.data`

Outputs:

- `div_distribution_data/complete_lseg_distribution_events.csv`
- `div_distribution_data/complete_lseg_distribution_coverage_summary.csv`
- `div_distribution_data/complete_lseg_distribution_access_exceptions.csv`
- `div_distribution_data/crsp_vs_lseg_distribution_compare.csv`
- `div_distribution_data/crsp_vs_lseg_distribution_coverage_summary.csv`

Important fields in `complete_lseg_distribution_events.csv`:

- `resolved_lseg_instrument`
- `share_isin`
- `permno`
- `ticker`
- `underlier_name`
- `current_primary_ric`
- `current_permid`
- `lseg_query_key`
- `lseg_query_key_type`
- `query_key_fallback_used`
- `country_code`
- `products`
- `product_groups`
- `universe_category`
- `ex_date`
- `pay_date`
- `record_date`
- `announce_date`
- `gross_dividend_amount`
- `adjusted_gross_dividend_amount`
- `lseg_dividend_type`
- `dividend_currency`
- `lseg_zero_amount_flag`
- `lseg_pay_date_before_ex_date_flag`
- `lseg_suspicious_event_flag`

The comparison file `crsp_vs_lseg_distribution_compare.csv` is keyed on:

- `permno`
- `share_isin`
- `ex_date`

Comparison sequencing:

- step 1: build `us_crsp_distribution_events.csv`
- step 2: build `complete_lseg_distribution_events.csv` for the full complete
  stock universe
- step 3: build `crsp_vs_lseg_distribution_compare.csv` only on the overlap set
  where `permno` exists

Important comparison fields:

- `ex_date_status`
  - `both`
  - `crsp_only`
  - `lseg_only`
- `crsp_dividend_amount`
- `lseg_gross_dividend_amount`
- `gross_amount_diff`
- `gross_amount_match`
- `pay_date_match`
- `matched_on_ex_date_amount_and_pay_date`
- `lseg_zero_amount_rows`
- `lseg_pay_date_before_ex_date_rows`
- `lseg_suspicious_event_rows`
- `lseg_has_suspicious_event`

## Current Interpretation

- CRSP is the canonical source for US names with `permno`
- LSEG is the canonical source for non-US names
- LSEG is also the cross-reference source for CRSP-covered names

The older narrow US-Eurex-only builders were removed because they were redundant
once the complete-universe workflow was introduced.

## Non-Stock Eurex Exceptions

The complete stock universe intentionally excludes known basket-style Eurex
underliers that are not single-stock identities.

Those exclusions are written to:

- `div_distribution_data/complete_stock_universe_excluded_non_single_stock.csv`

Examples include:

- Holcim-Amrize Basket
- Novartis-Sandoz Basket
- Sanofi-EUROAPI Basket
- thyssenkrupp-TKMS Basket
- Unilever-TMICC Basket

## Remaining Gap

The current security master still has:

- `340` unmatched CRSP rows with no LSEG identifier bridge

Those rows are not part of the current complete stock universe and should be
treated as a second-pass identifier recovery problem, not silently ignored.
