# Intraday Options Data via LSEG API

## Target Contracts (April 17, 2026 expiry, Calls)

| Underlying | Strike | RIC | OptionMetrics ID | Underlying Price (2026-03-18) |
|------------|--------|-----|------------------|-------------------------------|
| NVDA | $180 | NVDAD172618000.U | 171443316 | $180.40 |
| AMD | $200 | AMDD172620000.U | 171154093 | $199.46 |
| SPY | $660 | SPYD172666000.U | — | $661.43 |

## Available Intraday Data from LSEG Historical Pricing API

### 1. Tick-level trades (`events?eventTypes=trade`)
Individual trade executions with millisecond timestamps.

Fields:
- `DATE_TIME`, `SOURCE_DATETIME` — millisecond precision
- `TRDPRC_1` — trade price
- `TRDVOL_1` — trade size
- `TRDXID_1` — exchange ID
- `SEQNUM` — sequence number
- `ACVOL_UNS` — cumulative volume
- `OPEN_PRC`, `HIGH_1`, `LOW_1` — running OHLC
- `QUALIFIERS`, `TAG`

### 2. Tick-level quotes (`events?eventTypes=quote`)
Individual quote updates with millisecond timestamps.

Fields:
- `DATE_TIME`, `SOURCE_DATETIME` — millisecond precision
- `BID`, `BIDSIZE`, `ASK`, `ASKSIZE`
- `BUYER_ID`, `SELLER_ID` — exchange codes
- `IMP_VOLT` — implied volatility
- `IMP_VOLTA` — ask implied volatility
- `IMP_VOLTB` — bid implied volatility
- `DELTA`, `THETA`, `GAMMA` — greeks
- `QUALIFIERS`, `TAG`

### 3. Intraday summary bars (`intraday-summaries`)
1-minute OHLC bars with trade and bid/ask aggregates.

Fields:
- `DATE_TIME` — minute boundary
- `HIGH_1`, `LOW_1`, `OPEN_PRC`, `TRDPRC_1` — trade OHLC
- `NUM_MOVES`, `ACVOL_UNS` — trade count and volume
- `BID_HIGH_1`, `BID_LOW_1`, `OPEN_BID`, `BID`, `BID_NUMMOV` — bid bar
- `ASK_HIGH_1`, `ASK_LOW_1`, `OPEN_ASK`, `ASK`, `ASK_NUMMOV` — ask bar

## API Endpoints

Base URL: `https://api.refinitiv.com/data/historical-pricing/v1/views/`

| View | URL Path | Description |
|------|----------|-------------|
| Tick events | `events/{RIC}?eventTypes=trade` | Individual trades |
| Tick quotes | `events/{RIC}?eventTypes=quote` | Individual quote updates |
| Mixed events | `events/{RIC}` | Both trades and quotes interleaved |
| Intraday bars | `intraday-summaries/{RIC}` | 1-minute OHLC bars |
| Daily bars | `interday-summaries/{RIC}` | Daily OHLC bars |

## Data Retention (Active Contracts)

Measured on active SPY options as of March 2026:

| View | Retention | Notes |
|------|-----------|-------|
| Trade ticks | ~3 months | Back to Dec 17, 2025 |
| Quote ticks | ~2.5 weeks | Back to Mar 2, 2026 |
| 1-min bars | ~5.5 months | Not yet measured precisely |
| Daily bars | Years | Full contract lifetime |

For longer tick history, LSEG Tick History (RTH) is a separate product.

## API Rate Limits

Official limits per LSEG developer community (as of 2025):

| Endpoint | Burst Limit | Rate Limit |
|----------|-------------|------------|
| `GET events` (trade/quote ticks) | 25 req/sec | 25 req/sec |
| `GET intraday-summaries` (1-min bars) | 25 req/sec | 25 req/sec |
| `GET interday-summaries` (daily bars) | 50 req/sec | 50 req/sec |
| `GET single-event` | 75 req/sec | 75 req/sec |
| `POST events` | 4 req/sec | 4 req/sec |

Exceeding limits returns HTTP 429 "Too many requests, please try again later."

**Current script behavior**: `TICK_SLEEP = 0.5s` → ~2 req/sec, only 8% of the 25 req/sec limit.

**Optimization potential**:
- Reduce sleep to ~0.05s for ~20 req/sec sequential (10x speedup)
- Add thread pool (8-10 workers) with a token bucket to parallelize across RICs while staying under 25 req/sec
- Adaptive backoff on 429s rather than fixed conservative sleep

Sources: [LSEG Dev Community — rate limit thread](https://community.developers.lseg.com/discussion/116360), [events API rate limit](https://community.developers.lseg.com/discussion/132043)

## Expired Option RICs

Expired options are **not discoverable** via Discovery Search (`AssetState eq 'DC'` returns 0 results for options). Chain functions also don't return expired options.

**Workaround**: Construct the expired RIC manually by appending a `^<month_code><YY>` suffix.

### Expired RIC format
`<active_ric>^<expiry_month_code><expiry_YY>`

Month codes: A=Jan, B=Feb, C=Mar, D=Apr, E=May, F=Jun, G=Jul, H=Aug, I=Sep, J=Oct, K=Nov, L=Dec

Example: `AMDL192513000.U^L25` = AMD $130 Call, expired Dec 19 2025

### Data retention for expired options

Tested on multiple expired contracts (as of March 2026):

| Contract | Daily bars | 1-min bars | Trade ticks |
|----------|-----------|------------|-------------|
| AMD $130C Dec 2025 (3 mo ago) | 686 bars, Mar 2023–Dec 2025 | 75,092 bars, Mar–Dec 2025 (~9 mo) | 103 ticks (last 3 days only) |
| SPY $600P Sep 2025 (6 mo ago) | 370 bars, Apr 2024–Sep 2025 | 52,021 bars, Mar–Sep 2025 (~6 mo) | 0 |
| SPY $590P Dec 2024 (15 mo ago) | 756 bars, Dec 2021–Dec 2024 | 0 | 0 |

**Key insights**:
- **Daily bars**: Retained for full contract lifetime (years). 23 columns including OHLCV, bid/ask, open interest, **greeks (delta, gamma, vega, rho, theta)**, **implied vol (bid/ask/mid)**, theo value, pct change
- **1-min bars**: 1-year rolling retention window from current date (not from expiry). Contracts expired >1 year ago have no minute bars. 15-17 columns: trade OHLCV + bid/ask OHLC + num_moves. Requires pagination (10K cap per request, 90-day max per request)
- **Trade ticks**: Essentially gone shortly after expiry (~3 days retained). 21-22 columns including NBBO snapshot

**Implication**: The 1-year rolling window means minute bars for contracts that expired in Mar 2025 or later are still available now (Mar 2026). This is a rolling window — older data falls off daily. Daily bars with greeks are retained indefinitely and are very rich.

### Daily bar columns (expired options, interday-summaries)
`DATE, TRDPRC_1, OPEN_PRC, HIGH_1, LOW_1, ACVOL_UNS, BID, ASK, OPINT_1, MID_PRICE, IMP_VOLT, IMP_VOLTA, IMP_VOLTB, ASK_INDCTV, BID_INDCTV, DELTA, GAMMA, VEGA, RHO, THEO_VALUE, THETA, PCTCHNG, NETCHNG_1`

### Constructing the expired RIC suffix

The suffix format is `^<month_code><YY>` where month codes match option call codes:
A=Jan, B=Feb, C=Mar, D=Apr, E=May, F=Jun, G=Jul, H=Aug, I=Sep, J=Oct, K=Nov, L=Dec

This applies to both calls and puts — the suffix uses the expiry month only, not the C/P code.

To construct an expired RIC programmatically from an active RIC and expiry date:
```python
MONTH_CODES = 'ABCDEFGHIJKL'
def make_expired_ric(ric, expiry_date):
    # expiry_date: 'YYYY-MM-DD'
    month = int(expiry_date[5:7])
    year = expiry_date[2:4]
    suffix = f"^{MONTH_CODES[month-1]}{year}"
    return ric + suffix
# e.g. 'NVDAC162618500.U' + expiry '2026-03-16' → 'NVDAC162618500.U^C26'
```

### Key bug: download_minute_bars.py uses active RIC format for expired contracts

When fetching 1-min bars, expired contracts return 0 bars if queried with the active RIC (e.g., `NVDAC162618500.U`). The API returns 200 but empty data. The expired suffix must be appended (e.g., `NVDAC162618500.U^C26`).

**Fix needed in download_minute_bars.py**: Compare each contract's expiry date to today. If expired, append the `^<month_code><YY>` suffix before querying intraday-summaries. The expiry date is available in `option_contracts.csv`.

This was discovered during the first test run of download_minute_bars.py on NVDA (March 20, 2026) — Mar 16 2026 contracts showed 0 bars with the active RIC.

### Open questions
- Jan/Feb/Mar 2026 expired options: `^A26`, `^B26`, `^C26` suffix not yet tested with the corrected understanding above (previously tested without suffix). Need to retest.
- Can we use OptionMetrics contract lists as a source of truth to construct expired RICs for contracts we didn't discover ourselves?

## Option RIC Format (US equities, OPRA)

`<ROOT><YYMMDD><C/P><STRIKE*1000 zero-padded 8 digits>.U`

Example: `NVDAD172618000.U` = NVDA, 2026-04-17 (D1726), Call (C after date is implicit in the encoding), $180 (18000), OPRA composite (.U)
