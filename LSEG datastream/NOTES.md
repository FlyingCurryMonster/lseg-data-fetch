# LSEG Data Library - Dividend Futures & Options

## Setup

### Python Library
- Package: `lseg-data` (v2.0.1) — **not** the deprecated `DatastreamDSWS`
- Import: `import lseg.data as ld`

### Authentication
- Requires three credentials: **App Key**, **Username**, **Password**
- App Key generated in LSEG Workspace via App Key Generator (select **EDP API** type)
- Credentials stored in `.env` file (chmod 600, gitignored)
- Session type: `platform.rdp` (cloud access — works without Workspace desktop app)
- Must set `signon_control: True` in config to avoid session quota errors

### Session Config
The library requires a JSON config file. We generate it at runtime from `.env` to avoid storing credentials on disk:
```python
config = {
    "sessions": {
        "default": "platform.rdp",
        "platform": {
            "rdp": {
                "app-key": os.getenv("DSWS_APPKEY"),
                "username": os.getenv("DSWS_USERNAME"),
                "password": os.getenv("DSWS_PASSWORD"),
                "signon_control": True
            }
        }
    }
}
```
Config file is deleted after each script run.

---

## API Methods

### `ld.get_history()`
- Returns a DataFrame with daily OHLC, settlement, volume data
- `universe`: list of RICs
- `fields`: list of field names
- `start` / `end`: absolute dates only (e.g., `"2024-01-01"`), **no relative dates** like `-5D`
- History goes back to at least **2015** for S&P 500 dividend futures

### `ld.get_data()`
- Snapshot (current) data — returns empty DataFrames for some instrument types via `platform.rdp` session
- Less reliable than `get_history` for our use case

### `ld.discovery.search()`
- Search for instruments by keyword, filter by asset class
- Returns metadata: `DocumentTitle`, `RIC`, `StrikePrice`, `ExpiryDate`, `PutCallIndicator`, `UnderlyingQuoteRIC`, `ExchangeName`, `AssetCategory`
- **This is our primary tool for enumerating options and getting their metadata**
- `top` parameter controls max results (default 10)

### `ld.discovery.Chain()`
- Gets chain constituents from a chain RIC
- **Hangs/errors on `platform.rdp` session** — requires streaming access we may not have
- Avoid using; use `discovery.search` instead

---

## Available History Fields

| Field | Description | Available for Options? |
|-------|-------------|----------------------|
| `TRDPRC_1` | Last trade price | Yes (sparse — illiquid) |
| `HIGH_1` | Daily high | Yes |
| `LOW_1` | Daily low | Yes |
| `OPEN_PRC` | Open price | Yes |
| `SETTLE` | Settlement/mark price | Yes (most reliable for options) |
| `BID` | Best bid | Mostly NA for these options |
| `ASK` | Best ask | Mostly NA for these options |
| `ACVOL_UNS` | Accumulated volume (unsigned) | Yes |
| `OPINT` | Open interest | Not returned in testing |

---

## Dividend Index Futures Available

### CME (S&P 500)
| Product | Chain RIC | Continuations | Currency |
|---------|-----------|---------------|----------|
| S&P 500 Annual Dividend | `0#SDA:` | `SDAc1` - `SDAc6` | USD |
| S&P 500 Quarterly Dividend | `0#SDI:` | `SDIc1` - `SDIc3` | USD |

- Individual contract RICs: `SDAZ26` (Dec 2026), `SDAZ27` (Dec 2027), etc.
- Front month (SDAc1) trades ~77-82, volume ~1-3K contracts/day
- History available from **Nov 2015**

### Eurex (Euro Stoxx)
| Product | Chain RIC | Continuations | Currency |
|---------|-----------|---------------|----------|
| Euro Stoxx 50 Dividend | `0#FEXD:` | `FEXDc1` - `FEXDc15` | EUR |
| Euro Stoxx Banks Dividend | `0#FEBD:` | — | EUR |
| Euro Stoxx Telecom Dividend | `0#FETD:` | — | EUR |
| Euro Stoxx Select Div 30 | `0#FEDV:` | — | EUR |

- Individual contracts: `FEXDZ26`, `FEXDZ27`, ..., `FEXDZ35` (out to Dec 2035)
- Also quarterly contracts: `FEXDH26` (Mar), `FEXDM26` (Jun), `FEXDU26` (Sep)

### Eurex Single-Stock Dividend Futures
Large selection including BP, Unilever, Lloyds, Santander, Renault, Danone, Intel, and many more.

---

## Options on Dividend Futures

### CME S&P 500 Annual Dividend Options
- **Option chain RICs**: `0#1SDAZ26+` (Dec 2026), `0#1SDAZ27+` (Dec 2027), ..., `0#1SDAZ30+` (Dec 2030)
- Also composite (pit) chains: `0#SDAZ26+`, `0#SDAZ27+`, etc.

#### RIC Naming Convention
```
1SDA<strike><cp_flag><yy>

Examples:
  1SDA85L27   = 85 Call Dec 2027     (L = Call)
  1SDA64X27   = 64 Put Dec 2027      (X = Put)
  1SDA8725X27 = 87.25 Put Dec 2027   (fractional strikes encoded without decimal)
  1SDA8475L26 = 84.75 Call Dec 2026
```
- `1SDA` prefix = electronic
- `SDA` prefix (no 1) = composite
- `L` suffix = Call
- `X` suffix = Put

#### Option Metadata via `discovery.search`
Querying with `select='RIC,StrikePrice,ExpiryDate,PutCallIndicator,UnderlyingQuoteRIC'` returns:
```
RIC           StrikePrice  ExpiryDate   UnderlyingQuoteRIC
1SDA85L27     85.0         2027-12-17   [SDAZ27]
1SDA64X27     64.0         2027-12-17   [SDAZ27]
```

#### Liquidity
- These options are **thinly traded** — lots of NA in trade prices
- `SETTLE` (settlement/mark price) is the most consistently available field
- Dec 2027 85 calls most active (~10-3,500 contracts/day)
- Strikes range roughly 42-100

### Expired Options
- Expired option RICs get renamed with `^` suffix: e.g., `LCO5500L0^L20`
- Pattern: `<base_ric><month_code>^<month_letter><yy>`
- Active options can be found via `discovery.search`; expired ones need the `^` pattern
- Reference: Eikon symbology rules in `RULES5`

---

## Known Issues / Gotchas
1. **No relative dates** in `get_history` — use absolute `YYYY-MM-DD` format
2. **`get_data` returns empty** for some instruments via `platform.rdp` — use `get_history` instead
3. **`discovery.Chain` hangs** on cloud session — use `discovery.search` to enumerate
4. **`discovery.search` has a `top` limit** — must paginate or increase `top` for full option chains
5. **Streaming not available** on `platform.rdp` — Chain objects and real-time pricing may fail
6. **FutureWarning** from pandas `.fillna()` downcasting — cosmetic, can ignore

---

## Scripts
| Script | Purpose | Location |
|--------|---------|----------|
| `test_lseg.py` | Basic connectivity test (AAPL prices) | `LSEG datastream/` |
| `test_dividend_futures.py` | Sample data download for SDA/SDI futures + options | `LSEG datastream/` |
| `explore_fields.py` | Field exploration v1 (metadata, history depth) | `LSEG datastream/` |
| `explore_fields_v2.py` | Field exploration v2 (discovery.search metadata, Chain test) | `LSEG datastream/` |
| `archive/test_dsws.py` | Old DSWS API test (deprecated) | `LSEG datastream/archive/` |

---

## TODO
- [ ] Determine full list of dividend futures we want (CME + Eurex)
- [ ] Enumerate all options for each via `discovery.search` (active + expired)
- [ ] Download historical pricing for all futures and options (daily, max history)
- [ ] Build metadata table: RIC, strike, expiry, cp_flag, underlying_ric
- [ ] Investigate Euro Stoxx 50 dividend options availability
- [ ] Test `OPINT` field retrieval (didn't return in initial tests)
- [ ] Check if `IMP_VOLT` (implied vol) is available
