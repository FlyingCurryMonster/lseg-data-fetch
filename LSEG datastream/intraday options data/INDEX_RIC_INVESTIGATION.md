# Index Option RIC Format — Investigation

## Problem

When bulk-downloading 1-min bars for all OptionMetrics names using the standard
OPRA equity RIC format (`{ROOT}{month_code}{DD}{YY}{strike}.U^{suffix}`), index
products behaved inconsistently:

| Name | Contracts Queried | Bars Returned | Verdict |
|------|-------------------|---------------|---------|
| NDX (Nasdaq-100 index) | 107,648 | 0 | RIC format wrong — skipped |
| SPX (S&P 500 index) | 73,282 | 709,211 | RIC format mostly wrong (~1% hit rate) — skipped |
| XSP (Mini S&P 500) | 50,560 | 338M | RIC format correct — worked fine |
| RUT (Russell 2000 index) | 40,596 | 0 | RIC format wrong — skipped |
| RUTW (Russell 2000 weeklies) | 20,492 | 0 | RIC format wrong — skipped |
| MRUT (Micro Russell 2000) | 34,740 | 54.7M | RIC format correct — worked fine |
| SPXW (SPX weeklies) | 17,032 | 0 | RIC format wrong — skipped |
| XEO (S&P 100 European) | 14,596 | 0 (expected) | RIC format wrong — skipped |
| OEX (S&P 100 American) | 11,110 | 0 (expected) | RIC format wrong — skipped |
| XND (Mini Nasdaq-100) | 13,125 | 0 (expected) | RIC format wrong — skipped |
| MXEA (MSCI EAFE index) | 13,548 | 0 (expected) | RIC format wrong — skipped |
| CBTXW (unknown, likely CBOE index) | 15,938 | 0 | RIC format wrong — skipped |
| QQQ | 39,690 | Working | OPRA equity format correct (ETF, not index) |

## Hypothesis

NDX, SPX, RUT, and RUTW options trade on CBOE under a different RIC convention
than OPRA equity options. XSP and MRUT use OPRA-style RICs despite being index
products — possibly because they are "micro/mini" sized products structured
differently. QQQ is an ETF, not an index, so OPRA format applies correctly.

The MRUT vs RUTW contrast is a useful clue: both are Russell-based, but MRUT
works and RUTW does not. May relate to exchange listing or contract size.

## What We Know

- **OPRA equity format** (works for ETFs and single stocks):
  `{ROOT}{month_code}{DD}{YY}{strike_5digit}.U^{month_code}{YY}`
  Example: `SPYD172666000.U^D26`

- **CBOE index format** (used for SPX, NDX, RUT — unknown exact format):
  Likely uses a different exchange suffix and/or strike scaling.
  The few SPX contracts that returned data (~1%) may be the clue — check
  what RICs they correspond to vs. what we constructed.

## Investigation Steps

1. **Look up a known liquid SPX contract in LSEG Discovery Search**
   - Use `UnderlyingQuoteRIC eq 'SPX' and AssetState eq 'AC'` in the
     EquityDerivativeQuotes view
   - Compare the returned RIC to what our construction formula produces
   - This will reveal the correct format

2. **Check the SPX contracts that DID return data**
   - Query `SPX/om_bars_log.jsonl` for entries where `bars > 0`
   - Look at the `query_ric` field — those RICs happened to work
   - Compare against the majority that returned 0 to identify the pattern

3. **Check LSEG documentation for CBOE index option RICs**
   - CBOE options may use `.CB` or similar exchange suffix instead of `.U`
   - Strike scaling may differ (SPX strikes are much larger, e.g. 5000+)

4. **Once correct format is known:**
   - Update `build_lseg_ric()` in `download_om_minute_bars.py` with index-specific logic
   - Re-run NDX, SPX, RUT with corrected format (they're already marked COMPLETE
     with bad data — need to delete their `om_run.log` to force a re-run)

## Quick Start: Check SPX Contracts That Worked

```python
import json

hits = []
with open("SPX/om_bars_log.jsonl") as f:
    for line in f:
        e = json.loads(line)
        if e["bars"] > 0:
            hits.append(e)

print(f"SPX contracts with data: {len(hits)}")
for e in hits[:10]:
    print(f"  {e['query_ric']}  {e['bars']} bars  {e['earliest']} – {e['latest']}")
```

## Quick Start: LSEG Discovery Search for Active SPX Contract

```python
import requests

resp = requests.post(
    "https://api.refinitiv.com/discovery/search/v1/",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "Query": "",
        "View": "EquityDerivativeQuotes",
        "Select": "RIC,ExpiryDate,StrikePrice,CallPutOption",
        "Filter": "UnderlyingQuoteRIC eq 'SPX' and AssetState eq 'AC'",
        "Top": 5,
    }
)
print(resp.json()["Hits"])
```
