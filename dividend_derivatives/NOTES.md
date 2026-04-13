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

Expired option RICs get renamed with a `^` suffix. **`discovery.search` does not index expired options** — neither the Python wrapper nor the direct REST endpoint returns them with `AssetState eq 'DC'`. The only way to retrieve expired option data is to **construct the RIC from the naming convention** and query `get_history()` directly.

#### Expired Option RIC Pattern

The `^` suffix uses the **expiry month letter + two-digit year**. December = `L` (12th letter A-L). Both calls and puts use the same `^L<yy>` suffix for December expiries.

**SDA (CME S&P 500 Annual Dividend Options):**
```
Active:   1SDA<strike><L|X><yy>
Expired:  1SDA<strike><L|X><yy>^L<yy>

Examples:
  1SDA85L25^L25   = 85 Call Dec 2025 (expired)
  1SDA80X25^L25   = 80 Put Dec 2025 (expired)
  1SDA70L24^L24   = 70 Call Dec 2024 (expired)
  SDA85L25^L25    = composite equivalent (also works)
```

**FEXD (Eurex Euro Stoxx 50 Dividend Options):**
```
Active:   FEXD<strike*10><L|X><y>       (single-digit year)
Expired:  FEXD<strike*10><L|X><y>^L<yy> (single-digit base year, two-digit ^ year)

Examples:
  FEXD1050L5^L25  = 105.0 Call Dec 2025 (expired)
  FEXD1050X4^L24  = 105.0 Put Dec 2024 (expired)
  FEXD9500L3^L23  = 95.0 Call Dec 2023 (expired)
```

Note: FEXD encodes strikes as `strike * 10` (e.g., 105.0 → `1050`), while SDA uses the raw integer (e.g., 85 → `85`). Fractional SDA strikes drop the decimal (e.g., 78.25 → `7825`).

#### Data Availability for Expired Options

Tested via brute-force RIC construction + `get_history()` (see `test_expired_option_rics.py`):

| Product | Expired Expiry Years with Data | Earliest Price Data | Notes |
|---------|-------------------------------|--------------------|----|
| SDA (electronic `1SDA`) | Dec 2024, Dec 2025 | Jan 2024 | 2023 and earlier return no data at any strike |
| SDA (composite `SDA`) | Dec 2024, Dec 2025 | Jan 2024 | Same coverage as electronic |
| FEXD | Dec 2020 – Dec 2025 | ~2015 (varies by contract) | `FEXD1050L0^L20` has data from Dec 2015 |

SDA options appear to have been listed on CME Globex starting around January 2024. FEXD options go back further — Dec 2019 and earlier (`^L19`) fail, possibly due to a Eurex RIC scheme change.

Strikes confirmed for Dec 2025 SDA expired: 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100 (every 5 points; finer granularity likely exists at fractional strikes near ATM).

**Why SDA options only go back to Jan 2024:** CME launched options on S&P 500 Annual Dividend Index Futures on **January 29, 2024** ([press release](https://www.cmegroup.com/media-room/press-releases/2024/1/09/cme_group_to_launchoptionsonsp500annualdividendindexfuturesonjan.html)). The product did not exist before that date.

**Why FEXD options only go back to Dec 2020:** Eurex OEXD options have existed since May 25, 2010 ([Eurex listing](https://www.eurex.com/ex-en/markets/did/exd/EURO-STOXX-50-Index-Dividend-Options-69870)), so the product predates our data. The pre-2020 gap is likely due to LSEG data coverage limits or expired RIC retention policy.

#### References
- [LSEG: Finding Expired Options and Backtesting](https://developers.lseg.com/en/article-catalog/article/finding-expired-options-and-backtesting-a-short-iron-condor-stra) — official Python code confirming A-L month codes for `^` suffix
- [LSEG: Reconstructing RICs for Expired Futures](https://developers.lseg.com/en/article-catalog/article/reconstructing-rics-for-expired-futures-contracts)
- [LSEG Community: RIC nomenclature for expired options on futures](https://community.developers.lseg.com/questions/82796/ric-nomenclature-for-expired-options-on-futures.html)
- LSEG Workspace: `RULES5` (options on futures nomenclature), `RULES9` (expired RIC construction)

#### Quarterly FEXD Options (TODO — investigate)
Eurex launched **Mid-Curve Options on EURO STOXX 50 Index Dividend Futures (FEXD)** with quarterly expiries in **February 2024** ([Eurex announcement](https://www.eurex.com/ex-en/find/news-center/news/Eurex-launches-Mid-Curve-Options-on-EURO-STOXX-50-Index-Dividend-Futures-3845890)). These are a separate product from the annual FEXD options we've been working with. Our current enumeration and expired options scan only covers December (annual) expiries. Need to investigate: what RIC scheme these use, whether `discovery.search` returns them, and what strike/expiry coverage is available.

---

## Security Master (Symbology)

### What `convert_symbols` returns (per RIC)
| Field | Column name | Notes |
|-------|-------------|-------|
| CUSIP | `CUSIP` | 9-digit (with check digit) |
| ISIN | `IssueISIN` | 12-char |
| SEDOL | `SEDOL` | NYSE/London only; NA for NASDAQ stocks |
| Ticker | `TickerSymbol` | Current ticker |
| Issuer PermID | `IssuerOAPermID` | Stable issuer-level entity ID |

### What `discovery.search` adds
`PermID`, `ExchangeCode`, `ExchangeName`, `AssetCategory`, `AssetState`, `CommonName`, `CountryCode`

### Joining LSEG to CRSP
- LSEG returns **9-digit CUSIP** (with check digit): e.g., `30303M102`
- CRSP stores **8-digit CUSIP** (no check digit): e.g., `30303M10`
- **Join key: `lseg_cusip[:8] == crsp_cusip`** — trim the last character
- Confirmed working for AAPL, MSFT, JPM, META

### Are RICs stable over time? No.
RICs follow the exchange ticker and **change when the ticker changes**:
- `FB.O` → `META.O` on June 9, 2022 (Facebook renamed to Meta Platforms)
- Eurex did a mass derivatives RIC rename in April 2025
- Index RICs changed when Refinitiv rebranded to FTSE/LSEG in 2023
- Expired futures/options get renamed with a `^` suffix

**Consequence**: RIC alone is not a reliable long-term join key. Use one of:
1. **CUSIP[:8]** → links to CRSP PERMNO (best for US equities)
2. **ISIN** → venue-agnostic, stable across renames
3. **PermID** → LSEG's own stable entity identifier, persists through renames

### Can we get historical RICs? Yes — via direct REST API
The Python `convert_symbols` wrapper does NOT expose these, but the underlying `discovery/symbology/v1` REST API has two key parameters:

- **`showHistory: true`** — returns full `effectiveFrom`/`effectiveTo` timeline for every RIC ever linked to an identifier. E.g., querying ISIN `US30303M1027` returns `FB.O` (active until 2022-06-09), `META.O` (active from 2022-06-09), plus dozens of other exchange-specific RICs globally.
- **`effectiveAt`** — point-in-time query (ISO 8601 UTC). E.g., `effectiveAt: "2020-01-01T00:00:00Z"` returns `FB.O` for Meta's ISIN.

Call the endpoint directly with a Bearer token extracted from the active session (`session._access_token`):
```python
token = session._access_token
resp = requests.post(
    "https://api.refinitiv.com/discovery/symbology/v1/lookup",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "from": [{"identifierTypes": ["ISIN"], "values": ["US30303M1027"]}],
        "to":   [{"identifierTypes": ["RIC"]}],
        "type": "auto",
        "showHistory": True
    }
)
```
See `../security_master/explore_ric_history.py` for a working example.

### LSEG security master limitations
- Only returns **current state** — no historical name/ticker changes
- For name/ticker history, use **CRSP `security_names`** (has full time series per PERMNO)
- Derivatives (futures, options) have no CUSIP/ISIN — identified by RIC only

### Example: META / Facebook
```
CRSP PERMNO:  13407
CUSIP (8):    30303M10  →  LSEG CUSIP (9): 30303M102  ✓ match
ISIN:         US30303M1027
PermID:       4297297477

CRSP name history:
  2012-05-18 to 2021-10-31  FACEBOOK INC    ticker: FB
  2021-11-01 to 2022-06-08  META PLATFORMS  ticker: FB
  2022-06-09 to present     META PLATFORMS  ticker: META

LSEG RIC history:
  FB.O   (before June 9, 2022)
  META.O (June 9, 2022 onwards)
```

---

## Known Issues / Gotchas
1. **No relative dates** in `get_history` — use absolute `YYYY-MM-DD` format
2. **`get_data` returns empty** for some instruments via `platform.rdp` — use `get_history` instead
3. **`discovery.Chain` hangs** on cloud session — use `discovery.search` to enumerate
4. **`discovery.search` has a `top` limit** — must paginate or increase `top` for full option chains
5. **Streaming not available** on `platform.rdp` — Chain objects and real-time pricing may fail
6. **FutureWarning** from pandas `.fillna()` downcasting — cosmetic, can ignore

---

## Direct REST API Wrapper (`lseg_rest_api.py`)

For REST endpoints not exposed by the `lseg-data` Python library, we use `lseg_rest_api.py` which provides `LSEGRestClient`. It takes an active `ld` session and manages the bearer token for direct HTTP calls.

```python
from lseg_rest_api import LSEGRestClient

session = ld.open_session(config_name=config_path)
rest = LSEGRestClient(session)
```

### Wrapped Endpoints

| Method | Endpoint | Docs |
|--------|----------|------|
| `symbology_lookup()` | `POST api.refinitiv.com/discovery/symbology/v1/lookup` | [Symbology API](https://developers.lseg.com/en/api-catalog/refinitiv-data-platform/symbology-API) |
| `symbology_lookup_df()` | Same, returns DataFrame | [User guide PDF](https://developers.lseg.com/content/dam/devportal/api-families/refinitiv-data-platform/refinitiv-data-platform-apis/documentation/symbology_user_guide.pdf) |
| `search()` | `POST api.refinitiv.com/discovery/search/v1/` | [API Playground](https://apidocs.refinitiv.com/Apps/ApiDocs) (requires login) |
| `historical_pricing()` | `GET api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries/{ric}` | [RDP APIs](https://developers.lseg.com/en/api-catalog/refinitiv-data-platform/refinitiv-data-platform-apis) |

### Search REST API: `View` is required

The direct search endpoint requires a `View` field in the POST body. Default: `SearchAll`. Valid values include: `SearchAll`, `DerivativeQuotes`, `DerivativeInstruments`, `EquityQuotes`, `EquityInstruments`, `EquityDerivativeQuotes`, `FundQuotes`, `IndexQuotes`, `CommodityQuotes`, `Quotes`, `Instruments`, and others.

---

## Scripts
| Script | Purpose | Location |
|--------|---------|----------|
| `lseg_rest_api.py` | REST API wrapper (`LSEGRestClient`) — symbology, search, historical pricing | `LSEG datastream/` |
| `test_rest_api.py` | Test all 3 REST endpoints via the wrapper | `LSEG datastream/` |
| `test_expired_option_rics.py` | Brute-force expired option RIC discovery (found `^L<yy>` pattern) | `LSEG datastream/` |
| `find_expired_options.py` | Search REST API exploration for expired options (confirmed: not indexed) | `LSEG datastream/` |
| `test_lseg.py` | Basic connectivity test (AAPL prices) | `LSEG datastream/` |
| `test_dividend_futures.py` | Sample data download for SDA/SDI futures + options | `LSEG datastream/` |
| `explore_fields.py` | Field exploration v1 (metadata, history depth) | `LSEG datastream/` |
| `explore_fields_v2.py` | Field exploration v2 (discovery.search metadata, Chain test) | `LSEG datastream/` |
| `explore_symbology.py` | Symbology v1: convert_symbols, CUSIP/ISIN/SEDOL/PermID | `security_master/` |
| `explore_symbology_v2.py` | Symbology v2: bulk conversion, CUSIP→RIC, CRSP cross-reference | `security_master/` |
| `explore_secmaster.py` | Security master: META/CRSP cross-reference, 50-stock bulk test | `security_master/` |
| `explore_ric_history.py` | Historical RICs via REST symbology API (showHistory, effectiveAt) | `security_master/` |
| `build_instrument_master.py` | Build clean instrument masters from enumeration CSVs | `LSEG datastream/` |
| `build_secmaster.py` | Security master: US snapshot + separate RIC-history sidecar | `security_master/` |
| `download_futures_retry.py` | Download daily pricing for all dividend futures | `LSEG datastream/` |
| `download_options_prices.py` | Download daily pricing for all active dividend options | `LSEG datastream/` |
| `enumerate_instruments.py` | Enumerate active futures + SDA options (v1) | `LSEG datastream/` |
| `enumerate_instruments_v2.py` | FEXD option search queries (v2) | `LSEG datastream/` |
| `enumerate_instruments_v3.py` | FEXD options with corrected queries (v3) | `LSEG datastream/` |
| `enumerate_expired.py` | Enumerate expired futures (found 67 via AssetState=DC) | `LSEG datastream/` |
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
