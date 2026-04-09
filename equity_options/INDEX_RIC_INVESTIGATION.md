# Index Option RIC Format — Investigation

## Status: IN PROGRESS (updated 2026-04-09)

The correct RIC format for CBOE index options has been identified. Currently, the
intraday and interday endpoints return all-None price fields for expired index options,
but the RICs themselves are valid (HTTP 200). Need to investigate whether this data
is accessible via a different endpoint, API parameter, or LSEG data tier.

---

## Problem

When bulk-downloading 1-min bars for all OptionMetrics names using the standard
OPRA equity RIC format (`{ROOT}{month_code}{DD}{YY}{strike}.U^{month_code}{YY}`), index
products behaved inconsistently:

| Name | Contracts Queried | Bars Returned | Verdict |
|------|-------------------|---------------|---------|
| NDX (Nasdaq-100 index) | 107,648 | 0 | RIC format was wrong + no retained data |
| SPX (S&P 500 index) | 73,282 | 709,211 | RIC format was wrong (~1% hit rate) |
| XSP (Mini S&P 500) | 71,472 | 475M (partial, 48%) | RIC format correct but skipped — 34% zero-bar rate, too slow |
| RUT (Russell 2000 index) | 40,596 | 0 | RIC format was wrong + no retained data |
| RUTW (Russell 2000 weeklies) | 20,492 | 0 | RIC format was wrong + no retained data |
| MRUT (Micro Russell 2000) | 34,740 | 54.7M | RIC format correct — worked fine (OPRA equity format) |
| SPXW (SPX weeklies) | 17,032 | 0 | RIC format was wrong + no retained data |
| XEO (S&P 100 European) | 14,596 | 0 | RIC format was wrong + no retained data |
| OEX (S&P 100 American) | 11,110 | 0 | RIC format was wrong + no retained data |
| XND (Mini Nasdaq-100) | 13,125 | 0 | RIC format was wrong + no retained data |
| MXEA (MSCI EAFE index) | 13,548 | 0 | RIC format was wrong + no retained data |
| CBTXW (unknown, likely CBOE index) | 15,938 | 0 | Unknown product, no data |
| QQQ | 39,690 | Working | OPRA equity format correct (ETF, not index) |

## Root Cause: Two Issues

### Issue 1: Lowercase month codes

CBOE index options use **lowercase** month codes in LSEG RICs, while OPRA equity
options use **uppercase**.

| | Equity (OPRA) | Index (CBOE) |
|---|---|---|
| Call months | A B C D E F G H I J K L | a b c d e f g h i j k l |
| Put months | M N O P Q R S T U V W X | m n o p q r s t u v w x |
| Example (Jan call) | `NVDAA162605000.U` | `SPXa162655000.U` |

This was confirmed via the LSEG Discovery Search API, which returned active SPX
options with lowercase month codes: `SPXf182660000.U` ($6000 call Jun 2026),
`SPXp172660000.U` ($6000 put Apr 2026), etc.

The expired suffix uses the **uppercase** month code regardless:
`SPXa162655000.U^A26` (not `^a26`).

### Issue 2: Strike encoding for high strikes (>= $10,000)

Our pipeline used OptionMetrics `strike_price // 10` to encode the 5-character
strike field. This works for equity options (strikes < $1,000) but produces too
many digits for index options.

The correct LSEG OPRA encoding for the 5-character strike field:

| Strike Range | Encoding | Example |
|---|---|---|
| < $10 | `00` + int + 2 decimal digits | $5.50 → `00550` |
| $10 – $99.99 | `0` + int + 2 decimal digits | $55.00 → `05500` |
| $100 – $999.99 | int + 2 decimal digits | $120.00 → `12000` |
| $1,000 – $9,999 | int + `0` | $5,500 → `55000` |
| $10,000 – $19,999 | `A` + last 4 digits | $15,000 → `A5000` |
| $20,000 – $29,999 | `B` + last 4 digits | $21,000 → `B1000` |
| $30,000 – $39,999 | `C` + last 4 digits | $30,500 → `C0500` |
| $40,000 – $49,999 | `D` + last 4 digits | $42,000 → `D2000` |

Source: LSEG RULES7, confirmed via
[LSEG GitHub samples](https://github.com/LSEG-API-Samples/Examples.RDP.Python.FindingOptionRICs).

### Issue 3: No retained data for expired index options

Even with the correct RIC format, **LSEG does not retain historical pricing data
for expired index options**. API calls return HTTP 200 with data rows, but all
OHLCV fields are None.

Verified 2026-04-09:
- `SPXa162655000.U^A26` (SPX $5500 call, Jan 16 2026, expired) → 200 OK, all None
- `NDXa1626B0000.U^A26` (NDX $20000 call, Jan 16 2026, expired) → 200 OK, all None
- `RUTa162622000.U^A26` (RUT $2200 call, Jan 16 2026, expired) → 200 OK, all None
- `SPXf182660000.U` (SPX $6000 call, Jun 18 2026, **active**) → real OHLCV with volume

This applies to both intraday-summaries and interday-summaries endpoints.

## Why MRUT Works

MRUT (Micro Russell 2000) uses **uppercase** month codes and standard OPRA equity
format, despite being an index product. This is because MRUT options are structured
as OPRA-listed products with smaller contract sizes, not as CBOE-exclusive index
options. QQQ works for the same reason — it's an ETF, not an index.

## Next Steps

- The correct RIC format for CBOE index options is now known (lowercase months,
  high-strike encoding, `.U` OPRA suffix)
- Expired index options currently return all-None OHLCV on both intraday and interday endpoints
- Need to investigate:
  - [ ] Is there a different LSEG endpoint or API parameter that returns expired index option data?
  - [ ] Does the LSEG Tick History (RTH) product have this data?
  - [ ] Is it a data tier / entitlement issue — do we need a different subscription level?
  - [ ] Can we get this data via the LSEG DataScope Select (DSS) REST API instead?
  - [ ] Contact LSEG support to confirm whether expired index option intraday data is available at all
- MRUT continues to work correctly with standard OPRA equity format
- XSP remains stamped COMPLETE for performance reasons (correct format but too slow)
