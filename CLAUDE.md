# LSEG Data Fetch — Claude Context

This repo contains all LSEG Datastream download pipelines: intraday equity options
(bars + trades), dividend derivatives, and credit/bond data.

---

## Infrastructure

### Research Machine (primary)
- Ubuntu, ClickHouse 26.1.2 running locally
- Used for data analysis; not for long-running downloads
- Limited disk space

### Data Feed Machine (always-on)
- Ubuntu 22.04, dedicated to streaming/downloading data, never sleeps
- 8TB USB expansion drive mounted locally
- ClickHouse **not** installed — scripts must not depend on it
- Both machines share the expansion drive

### Repo
- `~/market data library/lseg data fetch/` on both machines
- CSV data, logs, and credentials are `.gitignore`d
- `.env` at repo root — LSEG credentials

---

## ClickHouse Tables (Research Machine)

Reference for spot-checking downloaded data against WRDS sources.

### CRSP Database
| Table | Rows | Date Range | Notes |
|-------|------|------------|-------|
| `crsp.daily_stock_monthly` | 33.4M | 1991-2019 | 94-col new CIZ format |
| `crsp.daily_stock_price_old` | 47.3M | 2000-2024 | 63-col old format, RET/RETX as String |
| `crsp.daily_stock_annual_update` | 11.3M | 2020-2024 | Same 94-col schema |
| `crsp.sp500_constituents` | 3.16M | 2000-2024 | Daily prices for S&P 500 members |
| `crsp.security_names` | 186K | — | PERMNO name/identifier history |
| `crsp.daily_index_history` | 181K | 2020-2024 | Index-level daily returns |
| `crsp.distributions` | 171K | — | Dividend/distribution events |
| `crsp.compustat_link` | 39K | — | GVKEY-PERMNO mapping, LINKENDDT='E' = active |
| `crsp.daily_market_returns` | 1.5K | 2020-2025 | VW/EW market returns + S&P |
| `crsp.quarterly_rebalance` | 700 | 2020-2024 | Index rebalance stats |

### OptionMetrics Database
| Table | Rows | Date Range |
|-------|------|------------|
| `option_metrics.forward_price` | 107M | 2000-2025-08-29 |
| `option_metrics.security_prices` | 52M | 2000-2025-08-29 |
| `option_metrics.option_pricing` | 4.3B | 1996-2025-08-29 |
| `option_metrics.index_dividend_yield` | 2.1M | 2000-2023 |
| `option_metrics.zero_coupon_yield_curve` | 255K | 2000-2023 |

### Compustat Database
| Table | Notes |
|-------|-------|
| `compustat.secd` | Daily security data |

### Key Data Quirks
- **CRSP return sentinels**: RET/RETX use B,C; DLRET/DLRETX use A,S → stored as String
- **CRSP SICCD sentinel**: Old format uses 'Z' for missing SIC → stored as String
- **Compustat link LINKENDDT='E'**: Means link still active → stored as String
- **ClickHouse ORDER BY**: Cannot use Nullable columns; must make them non-nullable
- **OptionMetrics `return`**: Reserved word, needs backtick quoting
- **Old vs New CRSP schemas**: Completely different column names (PRC vs DlyPrc, etc.)
- CRSP uses negative sentinel values (e.g., -5 in DisPERMCO) → always use signed Int types
- `SecurityBegDt` has dates back to 1925 → must use Date32 (not Date)

---

## LSEG API

- **Intraday endpoint**: `https://api.refinitiv.com/data/historical-pricing/v1/views/intraday-summaries/{RIC}`
- **Events endpoint**: `https://api.refinitiv.com/data/historical-pricing/v1/views/events/{RIC}?eventTypes=trade&count=10000`
- **Intraday retention**: 1-year rolling window — bars for contracts expired >1 year ago are gone
- **Trade tick retention**: ~3 months; **quote tick retention**: ~2.5 weeks
- **Rate limit**: 25 req/sec for intraday-summaries; script runs at 23 req/sec with adaptive backoff on 429s
- **Pagination**: 10K bars per request; expired contracts typically fit in 1 request
- **Auth**: Bearer token via `lseg.data` SDK, credentials in `.env`

### LSEG Expired RIC Format (OPRA equity)
Active RIC: `{ROOT}{month_code}{DD}{YY}{strike_5digits}.U`
Expired RIC: `{active_ric}^{month_code}{YY}`

Month codes (calls): A=Jan B=Feb C=Mar D=Apr E=May F=Jun G=Jul H=Aug I=Sep J=Oct K=Nov L=Dec
Month codes (puts):  M=Jan N=Feb O=Mar P=Apr Q=May R=Jun S=Jul T=Aug U=Sep V=Oct W=Nov X=Dec
Suffix always uses the call-side month code regardless of C/P.

Strike encoding: `strike_price // 10` (OM strike is in tenths of a cent; LSEG drops last digit)

Example: NVDA $120 Call exp 2025-06-20 → `NVDAF202512000.U^F25`

### LSEG Expired RIC Format (CBOE index options)

CBOE index options (SPX, NDX, RUT, etc.) use **lowercase** month codes and a different
high-strike encoding. Format otherwise identical to OPRA equity (`.U` suffix).

Active RIC: `{ROOT}{lowercase_month_code}{DD}{YY}{strike_5chars}.U`
Expired RIC: `{active_ric}^{UPPERCASE_month_code}{YY}`

Month codes (calls): a=Jan b=Feb c=Mar d=Apr e=May f=Jun g=Jul h=Aug i=Sep j=Oct k=Nov l=Dec
Month codes (puts):  m=Jan n=Feb o=Mar p=Apr q=May r=Jun s=Jul t=Aug u=Sep v=Oct w=Nov x=Dec

High-strike encoding (5-char field):
- $1,000–$9,999: `int + '0'` (e.g., $5500 → `55000`)
- $10,000–$19,999: `'A' + last 4 digits` (e.g., $15000 → `A5000`)
- $20,000–$29,999: `'B' + last 4 digits` (e.g., $21000 → `B1000`)

Example: SPX $5500 Call exp 2026-01-16 → `SPXa162655000.U^A26`

**However, LSEG does not retain historical pricing for expired index options.** The RICs
resolve (HTTP 200) but all OHLCV fields are None. Confirmed 2026-04-09 on both intraday
and interday endpoints. Active index options return real data.

### Known RIC Format Issues
- **NDX, SPX, RUT, RUTW, SPXW, XEO, OEX, XND, MXEA**: RIC format now known (lowercase months, see above) but **LSEG does not retain expired data**. Stamped COMPLETE for **both** minute bars and trades. See `equity_options/INDEX_RIC_INVESTIGATION.md`.
- **XSP**: RIC format is correct (OPRA) but skipped — 34% zero-bar rate across 71K contracts, too slow. Stamped COMPLETE for both.
- **CBTXW**: Unknown CBOE index product, 0 bars/ticks. Stamped COMPLETE for both.
- **MRUT**: Works correctly with OPRA uppercase format despite being an index product (OPRA-listed, not CBOE-exclusive).

---

## Equity Options Pipeline (`equity_options/`)

### Goal
Download 1-minute OHLC bars and trade ticks for all expired US equity option contracts
via the LSEG Historical Pricing API, covering the full retention window.

### Contract Sources (Three Periods)

| Period | Source | File | Contracts |
|--------|--------|------|-----------|
| Mar 25 2025 – Mar 25 2026 | OptionMetrics `option_pricing` table | `expired_options_search/all_om_contracts.csv` | 4.12M |
| Aug 30 – Dec 4 2025 | CBOE Dec 5 Wayback snapshot + brute-force probe | `expired_options_search/all_names_gap_rics.csv` | 482K |
| Dec 5 2025 – present | CBOE Dec 5 Wayback snapshot | `expired_options_search/all_cboe_contracts.csv` | 1.73M (1.09M in window) |

All three files have `base_ric` and `query_ric` columns and require no ClickHouse.

Note: `all_om_contracts.csv` covers Mar 25 2025 – Mar 25 2026 (scoped to the 1-year LSEG retention window). It includes contracts expiring after Aug 29 2025 that were already listed in OM at snapshot time — these overlap with the gap and CBOE periods.

### Gap Period (Aug 30 – Dec 4 2025)
OptionMetrics ends 2025-08-29. To cover the gap before the CBOE Dec 5 snapshot:
- Used CBOE Dec 5 strike ladders + calendar-generated expiry dates
- Probed 482,160 RICs across 672 names — **96.9% hit rate** (467,336 confirmed)
- 14,824 errors remain to be re-probed (session token expired near end of run)
- Monthly-only names (~4,653 tickers) skipped — their Nov 2025 monthly already in OM

### Download Pipeline

> **Legacy naming note**: Scripts and output files use the `om_` prefix (`download_om_minute_bars.py`, `run_all_om_bars.sh`, `om_minute_bars.csv`, `om_bars_log.jsonl`, `om_run.log`) from when this pipeline was OptionMetrics-only. It now covers all three contract sources (OM + CBOE + gap) — the `om_` prefix is purely historical.

**Main bar script**: `download_om_minute_bars.py TICKER [WORKERS] [--csv PATH]`
- Reads contracts from `data/{TICKER}/contracts.csv` (pre-built per-ticker file)
- Downloads full history — no time cap; paginates until LSEG returns no more data
- Column names discovered dynamically from API response headers
- Outputs: `data/{TICKER}/om_minute_bars.csv`, `data/{TICKER}/om_bars_log.jsonl` (resume), `data/{TICKER}/om_bars_progress.log`
- Resume: reads `om_bars_log.jsonl` to skip completed contracts — safe to kill/restart

**Main trades script**: `download_trades.py TICKER [WORKERS]`
- Reads `data/{TICKER}/contracts.csv`, filters to last ~92 days (trade tick retention window)
- Downloads via events endpoint, resumes via `trades_log.jsonl`
- Outputs: `data/{TICKER}/trade_ticks.csv`, `data/{TICKER}/trades_log.jsonl`, `data/{TICKER}/trades_progress.log`, `data/{TICKER}/trades_run.log`
- Trade tick columns: `DATE_TIME, EVENT_TYPE, RTL, SOURCE_DATETIME, SEQNUM, TRDXID_1, TRDPRC_1, TRDVOL_1, BID, BIDSIZE, ASK, ASKSIZE, PRCTCK_1, OPINT_1, PCTCHNG, ACVOL_UNS, OPEN_PRC, HIGH_1, LOW_1, QUALIFIERS, TAG`
- Quote ticks disabled — too dense, greeks-only value not worth volume

**Per-ticker contract files**: `data/{TICKER}/contracts.csv`
- Built by `build_ticker_contracts.py` — merges all three source files, deduplicates by `base_ric`
- Columns: `base_ric, query_ric, source` (source = om/cboe/gap)
- 6,570 tickers total, 5.9M contracts across all sources

**Orchestrators**: `run_all_om_bars.sh` / `run_all_trades.sh`
- Iterate through `all_tickers.csv` (6,570 tickers ordered by contract count desc)
- Skip tickers with `COMPLETE` in their respective run log
- Launched via `nohup`, survive terminal/Claude restarts

**Progress check**:
```bash
COMPLETED=$(grep -l "COMPLETE" data/*/om_run.log 2>/dev/null | wc -l)
ACTIVE=$(ps aux | grep "download_om_minute_bars" | grep -v grep | awk '{print $13}')
tail -2 "data/$ACTIVE/om_run.log"
```

### Download Status (as of 2026-04-10)
- **Scale**: 5.9M contracts across 6,570 tickers (OM + CBOE + gap)
- **Storage**: `data/` symlinked to expansion drive — `data/ → /media/datafeed/Expansion/LSEG-data/intraday options data/data/`. Currently ~472 GB used.
- **2-week CSVs**: 61 research-machine tickers preserved as `om_minute_bars_2week.csv` on expansion.
- **Bars status**: RUNNING — 34 tickers completed, currently on LLY (3.5K/12.5K contracts, ~190 contracts/min, 0 429s). Orchestrator is grinding through a **second-pass cleanup queue** of ~19 high-volume tickers (TSLA, NOW, ASML, APP, LLY, COIN, IVV, GEV, NVDA, SPOT, CRWD, COST, SLV, INTU, BLK, ISRG, ETHU, TLT, MDB) where ~213K legacy `requests==MAX_RETRIES` entries from a prior token-refresh failure are being retried before resuming fresh tickers. Elevated cleanup throughput (~270 contracts/min) is *not* representative of fresh-download speed (~50 contracts/min on QQQ-class names).
- **Trades status**: STOPPED — 5,313 tickers completed, no orchestrator currently running.
- **Rate-limit config** (in `download_om_minute_bars.py`): `INITIAL_RATE=25`, `MAX_RATE=50`, `RATE_BACKOFF=0.80`, recovery 15%/15s. Orchestrator default workers = 32. Tuned after a 48-worker burst hit 14 429s and floored throughput; current config sustains ~50 req/s with 0 429s.
- **Remaining work**: ~5.06M contracts total (~213K in cleanup queue + ~4.85M in 6,521 untouched tickers).
- **Zero-bar/tick rates** (systemic across all names):
  - Bars: **71% zero-bar rate** — 1-year retention, most zeros are genuinely untraded contracts (deep OTM, illiquid strikes). Liquid names: SPY 11%, QQQ 23%, IWM 70%, GLD 96%.
  - Trades: **66% zero-tick rate** — 3-month retention means contracts expired >3 months ago always return 0.
  - Tracked in `om_bars_log.jsonl` / `trades_log.jsonl` (`"bars":0` / `"ticks":0`)
- **100% zero-bar tickers (RIC issues suspected)**: BKNG (all strikes), BRK (likely BRK.A/BRK.B RIC mismatch)
- **Skipped (data access unresolved)**: NDX, SPX, RUT, RUTW, SPXW, XEO, OEX, XND, MXEA, CBTXW — RIC format now known (lowercase months) but current endpoints return all-None for expired index options. Investigating alternative APIs/tiers. XSP skipped for performance. All stamped COMPLETE pending resolution.
- **Storage estimate**: ~106 bytes/bar; realistic total much lower than 3 TB given 71% zero-bar rate

### Older/Superseded Scripts
- `download_option_ticks.py`, `download_spy_ticks.py` — live contract discovery, superseded by `download_trades.py`
- `probe_expired_trades.py` — confirmed expired RICs work on events endpoint
- `pregen_om_contracts.py` — used to generate `all_om_contracts.csv` from ClickHouse

---

## Dividend Derivatives (`dividend_derivatives/`)

Scripts for enumerating and downloading daily prices for dividend futures (SDA/SDI/FEXD)
and options on S&P 500 index dividends.

Key scripts:
- `enumerate_div_contracts*.py` — discover SDA/SDI/FEXD contracts via LSEG search
- `build_div_master.py` — build clean futures/options master files
- `download_div_futures.py` — daily OHLCV for dividend futures
- `download_div_options.py` — daily prices for dividend options
- `build_secmaster.py` — links LSEG RICs to CRSP PERMNOs

### Eurex Single Stock Products
Eurex lists **93 US single stock futures** and **57 US single stock dividend futures**
on names like AAPL, MSFT, AMZN, NVDA, META, JPM, KO, PG, etc. Full product lists,
Eurex IDs, and Reuters chain RICs are documented in `EUREX_SINGLE_STOCK.md`.
**Status**: Product list confirmed via Eurex CSV (2026-04-12). LSEG API access and
RIC format not yet tested — need to probe `get_history()` on sample contracts.

---

## Credit (`credit/`)

Bond/credit market data via LSEG REST API.

- `download_bond_master.py` — downloads bond security master (ISINs, ratings, terms)
- `test_bond_*.py` — exploration/test scripts for bond pricing, history depth, search

---

## Shared Utilities (`shared/`)

- `lseg_rest_api.py` — REST client used by 14 scripts across all sub-projects
- `__init__.py` — provides `REPO_ROOT`, `ENV_PATH` helpers

Import pattern from any subdirectory:
```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.lseg_rest_api import LSEGRestClient
```

---

## Key File Locations

```
lseg data fetch/
├── .env                              # LSEG credentials (gitignored)
├── BOND_DATA_RESEARCH.md
├── shared/
│   ├── __init__.py
│   └── lseg_rest_api.py             # REST client (14 importers)
├── dividend_derivatives/
│   ├── enumerate_div_contracts*.py   # SDA/SDI/FEXD discovery
│   ├── build_div_master.py           # master file builder
│   ├── download_div_futures.py       # daily OHLCV
│   ├── download_div_options.py       # daily prices
│   ├── build_secmaster.py            # RIC → PERMNO mapping
│   ├── NOTES.md
│   └── EUREX_SINGLE_STOCK.md        # 93 US SSFs + 57 US SSDFs — product lists & RICs
├── equity_options/
│   ├── download_om_minute_bars.py    # main bar download script
│   ├── download_trades.py            # trade tick downloader
│   ├── run_all_om_bars.sh            # bar orchestrator (6,570 tickers)
│   ├── run_all_trades.sh             # trades orchestrator
│   ├── build_ticker_contracts.py     # merges 3 source CSVs → per-ticker contracts.csv
│   ├── all_tickers.csv               # 6,570 tickers by contract count (gitignored)
│   ├── INDEX_RIC_INVESTIGATION.md
│   ├── data -> /media/datafeed/Expansion/LSEG-data/intraday options data/data
│   │   └── {TICKER}/
│   │       ├── contracts.csv         # all RICs (OM + CBOE + gap)
│   │       ├── om_minute_bars.csv    # bar data
│   │       ├── om_bars_log.jsonl     # bar resume checkpoint
│   │       ├── trade_ticks.csv       # trade tick data
│   │       └── trades_log.jsonl      # tick resume checkpoint
│   └── expired_options_search/
│       ├── all_om_contracts.csv      # 4.12M OM contracts
│       ├── all_cboe_contracts.csv    # 1.73M CBOE contracts
│       ├── all_names_gap_rics.csv    # 482K gap RIC candidates
│       ├── build_cboe_contracts.py
│       └── eof_scripts/              # gap RIC construction/probe scripts
├── credit/
│   ├── download_bond_master.py
│   └── test_bond_*.py
├── tests/
│   ├── test_lseg.py
│   └── test_rest_api.py
└── archive/                          # deprecated DSWS scripts
```

---

## TODO

- [ ] Re-probe 14,824 errored rows in `all_names_gap_probe_results.csv`
- [ ] Download bars for gap period contracts (`all_names_gap_rics.csv`)
- [ ] Download bars for CBOE Dec 2025–Mar 2026 contracts (`all_cboe_contracts.csv`)
- [ ] Investigate expired index option data access for NDX, SPX, RUT, RUTW — RIC format now known (lowercase months + high-strike encoding) but expired data returns all-None on current endpoints. Need to explore other LSEG APIs/tiers. See `equity_options/INDEX_RIC_INVESTIGATION.md`.
- [ ] Download daily bars (greeks + IV) for all contracts — separate pipeline, retained indefinitely
