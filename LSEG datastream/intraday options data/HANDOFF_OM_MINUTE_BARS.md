# Handoff: Download 1-Minute Bars for All OptionMetrics Contracts

## Objective

Download LSEG 1-minute intraday bars for all expired option contracts in the
OptionMetrics database, limited to the **first 2 weeks of available bar data**
per contract. Covers **6,118 distinct names** ordered by liquidity (most
contracts first).

> **Known issue:** NDX, SPX, and RUT use CBOE RIC formats that differ from the
> OPRA equity format this script constructs. They will download 0 or near-0 bars.
> See `INDEX_RIC_INVESTIGATION.md` before re-running those names.

---

## Working Directory

```
/home/rakin/wrds-data/LSEG datastream/intraday options data/
```

---

## Files

| File | Purpose |
|------|---------|
| `download_om_minute_bars.py` | Downloads bars for a single ticker |
| `run_all_om_bars.sh` | Iterates through all 6,118 names, skips completed |
| `all_om_tickers.csv` | All OM tickers ordered by contracts desc (most liquid first) |
| `om_all_run.log` | Master run log — which tickers completed, when, bar counts |

---

## How to Run

### Full run (all 6,118 names, resumable)

```bash
cd "/home/rakin/wrds-data/LSEG datastream/intraday options data"
nohup ./run_all_om_bars.sh 8 > /dev/null 2>&1 &
echo "PID: $!"
```

Kill it at any time. Restart with the same command — completed tickers are
skipped automatically (detected via "COMPLETE" in `{TICKER}/om_run.log`).

### Single ticker

```bash
python download_om_minute_bars.py AAPL 8
```

If the ticker isn't in the hardcoded `OM_SECID` map, the secid is looked up
automatically from ClickHouse.

---

## What the Script Does (per ticker)

1. Looks up secid — from hardcoded map, or dynamically from ClickHouse
2. Queries `option_metrics.option_pricing` for all unique contracts with
   `exdate` in the 1-year LSEG retention window
3. Constructs expired LSEG RICs using OPRA format:
   `{ROOT}{month_code}{DD}{YY}{strike_5digit}.U^{month_code}{YY}`
4. Downloads 1-min bars from `intraday-summaries`, stopping after **2 weeks**
   of data per contract (measured back from the most recent bar)
5. Writes to `{TICKER}/om_minute_bars.csv`
6. Logs each contract to `{TICKER}/om_bars_log.jsonl` — safe to kill/restart

### Output columns (17 fields)

```
ric, DATE_TIME, HIGH_1, LOW_1, OPEN_PRC, TRDPRC_1, NUM_MOVES, ACVOL_UNS,
BID_HIGH_1, BID_LOW_1, OPEN_BID, BID, BID_NUMMOV,
ASK_HIGH_1, ASK_LOW_1, OPEN_ASK, ASK, ASK_NUMMOV
```

---

## Current Status

| Ticker | Contracts | Bars | CSV | Notes |
|--------|-----------|------|-----|-------|
| NVDA | 9,444 | 177.5M | 17 GB | Full history (not 2-week limited) |
| AMD | 5,214 | 85.5M | 8.1 GB | Full history (not 2-week limited) |
| TSLA | 9,714 | 165.8M | 16.6 GB | Full history (not 2-week limited) |
| SPY | 43,222 | 444.9M | 44 GB | Full history (not 2-week limited) |
| AAPL | 4,252 | running | — | 2-week limit, in progress |
| MSFT, AMZN, GOOGL, META, JPM, LLY, AVGO, COST, XOM | — | — | — | Queued |
| Remaining ~6,100 names | — | — | — | Will run via run_all_om_bars.sh |

Note: NVDA/AMD/TSLA/SPY were downloaded with full 1-year history before the
2-week limit was implemented. All subsequent names use the 2-week limit.

---

## Disk Space — Critical

Each liquid name produces multi-GB CSVs. Load into ClickHouse and drop CSVs
regularly. Current disk: ~77% full before these downloads.

**ClickHouse load (schema TBD — confirm before running):**

```bash
clickhouse-client --query "
  INSERT INTO intraday_options.om_minute_bars FORMAT CSVWithNames
" < "AAPL/om_minute_bars.csv"

# Verify row count, then drop
rm "AAPL/om_minute_bars.csv"
```

---

## Rate Limits

- Target: 23 req/sec total across all workers (LSEG cap is 25 for intraday-summaries)
- Adaptive rate limiter backs off 10% per 429, floor 0.5 req/sec
- If persistent 429s: reduce workers → `./run_all_om_bars.sh 4`

---

## Monitoring

```bash
# Master progress
tail -f om_all_run.log

# Current ticker progress
ls */om_run.log | xargs grep "COMPLETE" 2>/dev/null | wc -l   # done count
tail -f AAPL/om_bars_progress.log                              # live throughput
```
