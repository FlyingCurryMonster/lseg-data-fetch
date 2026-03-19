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

## Data Retention

The RDP Historical Pricing API retains tick/intraday data for a limited window (typically days to weeks). For longer tick history, LSEG Tick History (RTH) is a separate product.

## Option RIC Format (US equities, OPRA)

`<ROOT><YYMMDD><C/P><STRIKE*1000 zero-padded 8 digits>.U`

Example: `NVDAD172618000.U` = NVDA, 2026-04-17 (D1726), Call (C after date is implicit in the encoding), $180 (18000), OPRA composite (.U)
