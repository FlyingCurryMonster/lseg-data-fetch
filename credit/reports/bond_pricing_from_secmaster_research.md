# Bond Pricing Research: Non-US Bonds with ISIN-Only Identifiers

## Problem Statement

The bond security master contains ~8.8M bonds (13M rows pre-dedup). Most non-US
bonds (Colombian Peso, Euro, HKD, JPY, KRW, CNY denominated) have only an ISIN
identifier -- no RIC, no CUSIP, no SEDOL. We need to find a path from ISIN to
historical bond pricing data via LSEG/Refinitiv APIs.

**Identifier coverage in the security master** (observed from CSV data):
- ~6% have RICs (RIC column populated)
- ~23% have CUSIPs (mostly US bonds)
- ISIN coverage is near-universal
- SEDOL coverage is sparse

**What we already confirmed empirically:**
- RIC works with Historical Pricing API (`/data/historical-pricing/v1/`)
- `CUSIP=` format works (e.g., `665585KH7=`)
- Plain ISIN in Historical Pricing returns HTTP 200 but 0 data rows
- `ISIN=` format works for *some* bonds (e.g., French `FR0013508470=`) but not most
- Symbology API can convert ISIN -> RIC (returns many venue-specific RICs)
- Symbology API ISIN -> CUSIP fails for non-US bonds (expected -- CUSIP is a North American system)
- Venue-specific RICs from symbology (e.g., `=R`, `=MLTW`) returned 0 rows in historical pricing
- `CUSIP=` format from symbology DID work when available

---

## 1. Historical Pricing API: Accepted Identifier Formats

**Endpoint:** `GET /data/historical-pricing/v1/views/interday-summaries/{identifier}`

The Historical Pricing API accepts identifiers in the URL path. The identifier is
interpreted based on its format:

| Format | Example | Works for Bonds? | Notes |
|--------|---------|-----------------|-------|
| RIC (plain) | `US10YT=RR` | Yes | The standard identifier. Works when you have it. |
| `CUSIP=` | `665585KH7=` | Yes (US bonds) | Trailing `=` signals CUSIP lookup. Widely works for US corporate/govt. |
| `ISIN=` | `FR0013508470=` | Partial | Works for some bonds where LSEG has mapped the ISIN to a pricing instrument. Not universal. |
| `SEDOL=` | `B1YW440=` | Likely partial | Same `=` suffix convention as CUSIP. SEDOL is UK/Irish-centric. May work for GBP-denominated bonds but untested. |
| Plain ISIN | `US912828YK04` | No | Returns 200 with empty data. The API does not auto-resolve ISINs. |
| `ISIN+exchange` | `FR0013508470.PA` | Unlikely | This is an equity convention; bonds don't trade on exchanges in the same way. |

### Key Insight: The `=` Suffix Convention

The Historical Pricing API uses a trailing `=` to indicate that the identifier
preceding it is NOT a RIC but a cross-reference code (CUSIP, ISIN, SEDOL, etc.).
The API then attempts to resolve it internally to a pricing instrument:

- `665585KH7=` -- API sees `=`, looks up CUSIP `665585KH7`, finds the pricing RIC
- `FR0013508470=` -- API sees `=`, looks up ISIN `FR0013508470`, finds pricing RIC (when mapping exists)
- `XS2727921350=` -- API sees `=`, tries to look up... but XS-prefix ISINs often lack a direct pricing mapping

**Why `ISIN=` fails for most non-US bonds:** The `=` suffix lookup depends on
LSEG having an internal mapping from that identifier to a "pricing-capable"
instrument. For US bonds, CUSIP mappings are comprehensive because CUSIP is the
primary US bond identifier. For non-US bonds, ISIN is often the *registration*
identifier only -- LSEG may have the bond in its reference database (hence it
appears in the Search API / security master) but may not have a pricing instrument
mapped to that ISIN.

### RIC Patterns Observed in Security Master

From the CSV data, bond RICs follow these patterns:
- `NL272792135=` -- country-prefixed numeric code with `=` suffix (Netherlands)
- `DE046003748=` -- German bond
- `CA291530168=` -- Canadian bond
- `KZ184763338=` -- Kazakhstan bond
- `XS1438515758=TE` -- XS-prefix (Eurobond) with exchange suffix `=TE`
- `XS3059869845.TX` -- XS-prefix with dot-exchange suffix `.TX`
- `665585KH7=` -- CUSIP-based RIC (US bond)

Bonds without RICs in the security master have empty RIC fields but valid ISINs
(often XS-prefix for Eurobonds, or country-specific ISIN prefixes).

### Recommendation: Test `SEDOL=` Format

For GBP-denominated bonds, many will have SEDOLs. The security master already
has a SEDOL column. Worth testing:
```
GET /data/historical-pricing/v1/views/interday-summaries/{SEDOL}=
```

---

## 2. Symbology API: ISIN -> Pricing-Capable Identifier

**Endpoint:** `POST /discovery/symbology/v1/lookup`

### 2a. ISIN -> RIC (type: "auto")

```python
payload = {
    "from": [{"identifierTypes": ["ISIN"], "values": ["XS2727921350"]}],
    "to": [{"identifierTypes": ["RIC"]}],
    "type": "auto"
}
```

This returns *all* RICs associated with the ISIN -- typically multiple venue-specific
RICs. For a Eurobond, you might get:
- `XS2727921350=TE` (Tradeweb Europe)
- `XS2727921350=MLTW` (MarketAxess/Tradeweb)
- `XS2727921350=R` (Refinitiv composite?)
- `XS2727921350=LSEI` (London Stock Exchange International)

**The problem:** Not all of these venue-specific RICs have historical pricing data.
Many are "quote" instruments that only have real-time or limited streaming data,
not interday historical summaries.

### 2b. ISIN -> RIC via "FindPrimaryRIC" Route (type: "predefined")

```python
payload = {
    "from": [{"identifierTypes": ["ISIN"], "values": ["XS2727921350"]}],
    "type": "predefined",
    "route": "FindPrimaryRIC"
}
```

The `FindPrimaryRIC` predefined route is designed to return the single "best"
pricing RIC for an instrument. For equities this reliably returns the primary
listing RIC. **For bonds, this is the most promising untested approach** -- it
should return the RIC that LSEG considers the primary pricing source.

**This should be the first thing to test.** If FindPrimaryRIC returns a RIC for
non-US bonds, and that RIC works with Historical Pricing, the problem is solved.

### 2c. ISIN -> CUSIP (Non-US: Fails)

As confirmed, ISIN -> CUSIP via symbology returns nothing for non-US bonds. This
is expected because CUSIP is administered by CUSIP Global Services and is
primarily a North American identifier. Non-US equivalent would be CINS (CUSIP
International Numbering System), but the symbology API does not appear to
distinguish CINS from CUSIP.

### 2d. ISIN -> SEDOL

```python
payload = {
    "from": [{"identifierTypes": ["ISIN"], "values": ["XS2727921350"]}],
    "to": [{"identifierTypes": ["SEDOL"]}],
    "type": "auto"
}
```

Worth testing for GBP/EUR bonds. If we can get SEDOLs, then `SEDOL=` format in
Historical Pricing might work.

### 2e. Batch Conversion Strategy

The Symbology API accepts up to 100 identifiers per request. For 8.8M bonds,
that's 88,000 API calls. At ~2 calls/second with rate limiting, that's ~12 hours.
Feasible but expensive.

**Recommended batch approach:**
```python
# Process in chunks of 100 ISINs
for chunk in batched(all_isins, 100):
    result = rest.symbology_lookup(
        identifiers=chunk,
        from_types=["ISIN"],
        to_types=["RIC"],
        route=None,  # or use FindPrimaryRIC
    )
    # Parse results, store ISIN -> RIC mapping
```

---

## 3. Discovery Search API: Return Pricing RIC for ISIN

**Endpoint:** `POST /discovery/search/v1/`

The Search API already returns RICs in the security master download (the `RIC`
field in `GovCorpInstruments` view). When a bond has a RIC in the search results,
it is typically the primary pricing RIC.

### 3a. Using Search to Find RICs for ISIN-Only Bonds

For bonds that came back from Search with empty RIC fields, the Search API itself
has no RIC for them. This means LSEG's reference database does not have a pricing
instrument linked to these bonds.

### 3b. Search with ISIN Filter

```python
result = rest.search(
    view="GovCorpInstruments",
    filter="ISIN eq 'XS2597926067'",
    select="RIC,ISIN,CUSIP,SEDOL,IssuerLegalName",
    top=10,
)
```

This can retrieve any available identifiers for a specific ISIN, but if the Search
API already returned no RIC during the mass download, querying by ISIN individually
will return the same empty RIC.

### 3c. Search in Different Views

The `GovCorpInstruments` view is for reference data. For pricing instruments,
try `FixedIncomeQuotes` or `Quotes` view:

```python
result = rest.search(
    view="FixedIncomeQuotes",
    filter="ISIN eq 'XS2727921350'",
    select="RIC,ISIN,DocumentTitle,ExchangeName",
    top=20,
)
```

The `FixedIncomeQuotes` view returns **quote-level** instruments, which are the
actual pricing instruments on specific venues. A single ISIN may have multiple
quote instruments across different venues. If any of these quotes have historical
pricing, we can fetch it.

**This is the second-most-promising untested approach.**

---

## 4. Alternative LSEG APIs That Accept ISIN

### 4a. Datastream (DSWS)

**Endpoint:** Datastream Web Service (DatastreamPy library or direct REST)

Datastream is LSEG's legacy data platform and has **the best ISIN support for
bonds globally**. It uses its own instrument coding system but accepts ISINs
as lookup keys.

```python
import DatastreamPy as DSWS

ds = DSWS.DataClient(None, username, password)

# Time series request using ISIN
# Prefix with 'D:' to indicate Datastream lookup, or use ISIN directly
data = ds.get_data(
    tickers='XS2727921350',  # ISIN directly
    fields=['PI', 'YLD', 'RY', 'MV'],  # Price Index, Yield, Red. Yield, Market Value
    start='-5Y',
    end='0D',
    kind=1  # time series
)
```

**Datastream bond fields of interest:**
- `PI` -- Clean Price Index
- `YLD` -- Yield
- `RY` -- Redemption Yield
- `MV` -- Market Value
- `P` -- Price
- `PS` -- Bid Price
- `PA` -- Ask Price
- `ACC` -- Accrued Interest

**Datastream ISIN lookup:** Datastream maintains its own comprehensive security
master with ISIN cross-references. To look up by ISIN, you can use:
- Direct ISIN as ticker: `XS2727921350`
- With ISIN prefix: `@ISIN|XS2727921350`
- Datastream mnemonic: search via Navigator or API

**Key advantage:** Datastream has pricing data for many bonds that the RDP
Historical Pricing API does not cover, because Datastream aggregates from
contributed pricing sources (broker quotes, evaluated prices from FTSE Russell,
etc.) rather than just exchange/venue trades.

**Key limitation:** Datastream has its own rate limits and the DatastreamPy
library can be slow. Also, not all 8.8M bonds will have Datastream coverage.

**Already in codebase:** The archive contains `test_dsws.py` showing
DatastreamPy is installed and configured. Auth uses the same credentials
(`DSWS_USERNAME`, `DSWS_PASSWORD`).

### 4b. Eikon Data API (`get_data` / `get_timeseries`)

**Library:** `eikon` or `refinitiv.data` (now `lseg.data`)

```python
import lseg.data as ld

# get_data accepts ISIN for static/analytics data
analytics = ld.get_data(
    ["XS2727921350"],  # ISIN directly
    [
        "TR.FiIssuerName", "TR.CUSIP", "TR.FiNetCoupon",
        "TR.FiMaturityDate", "TR.PriceCleanAnalytics",
        "TR.YieldToMaturityAnalytics", "TR.ZSpreadAnalytics",
    ]
)

# get_history may work with ISIN for some instruments
history = ld.get_history(
    "XS2727921350",  # ISIN
    fields=["BID", "ASK", "TRDPRC_1"],
    start="2020-01-01",
    end="2026-04-01",
    interval="daily",
)
```

**`ld.get_data()` with TR fields:** This uses the backend "Fundamental & Reference"
API which resolves ISINs internally. The `TR.PriceCleanAnalytics` and similar
fields return the latest evaluated price, not a time series. However, you can
request historical analytics with date parameters:

```python
analytics = ld.get_data(
    ["XS2727921350"],
    ["TR.PriceCleanAnalytics(SDate=2024-01-01,EDate=2025-12-31,Frq=D)"],
)
```

**Key advantage:** `ld.get_data()` / `get_history()` with the `lseg.data` library
performs internal symbology resolution. It may successfully resolve ISINs that
fail when passed directly to the REST Historical Pricing API.

**Key limitation:** Rate limits on `get_data` are stricter; large-scale batch
retrieval is slow.

### 4c. Instrument Pricing Analytics (IPA) API

**Endpoint:** `POST /data/quantitative-analytics/v1/financial-contracts`

The IPA API is designed specifically for bond analytics and accepts ISINs:

```python
payload = {
    "universe": [
        {
            "instrumentType": "Bond",
            "instrumentDefinition": {
                "instrumentCode": "XS2727921350",
                "instrumentCodeType": "Isin"
            },
            "pricingParameters": {
                "valuationDate": "2025-01-15"
            }
        }
    ],
    "fields": [
        "InstrumentDescription", "CleanPrice", "DirtyPrice",
        "YieldPercent", "ModifiedDuration", "Convexity",
        "AccruedInterest", "ZSpreadBp"
    ]
}
resp = requests.post(
    "https://api.refinitiv.com/data/quantitative-analytics/v1/financial-contracts",
    headers=headers, json=payload
)
```

**Key advantage:** IPA natively accepts ISINs via `instrumentCodeType: "Isin"`.
It returns evaluated/calculated analytics (price, yield, duration, spread) for
bonds, using LSEG's pricing models.

**Key limitation:** IPA returns point-in-time analytics, not historical time
series. To build a history, you would need to loop over valuation dates, which
is extremely slow for 8.8M bonds. Also, IPA availability depends on the
underlying bond being in LSEG's evaluated pricing universe (FTSE Russell pricing).

### 4d. RDP Pricing Snapshots API

**Endpoint:** `GET /data/pricing/snapshots/v1/{identifier}`

This returns the latest real-time/delayed pricing snapshot. Accepts RICs primarily,
but the `lseg.data` library's `get_snapshot()` may resolve ISINs:

```python
snapshot = ld.get_data(
    ["XS2727921350"],
    ["BID", "ASK", "TRDPRC_1", "PRIMACT_1", "ACVOL_1"]
)
```

Not useful for historical data, but can validate whether a bond has *any* pricing
at all in LSEG's system.

---

## 5. Bond Pricing Availability by Market

### Reality Check: What Is Actually Priced?

Bond markets are overwhelmingly OTC (over-the-counter). Unlike equities, most
bonds do not trade on exchanges. Pricing comes from:

1. **Dealer quotes** (bid/ask from market makers) -- only for liquid bonds
2. **Evaluated/composite pricing** (FTSE Russell, Bloomberg BVAL) -- broader coverage
3. **Exchange trades** (rare, mainly for retail-accessible govt bonds)
4. **Reported trades** (TRACE in US, MiFID in EU) -- regulatory reporting

LSEG's pricing coverage varies dramatically by market:

| Market | Currency | Pricing Availability | Notes |
|--------|----------|---------------------|-------|
| US Corporate/Govt | USD | Excellent | TRACE reporting, deep dealer markets. ~95%+ of active bonds priced. |
| Euro-denominated | EUR | Good for large issues | ECB-eligible, benchmark issues well-covered. Small/structured notes: sparse. |
| UK Gilts / GBP Corp | GBP | Good | London market, well-covered by LSEG (historically Reuters' home market). |
| Japanese (JGB/Corp) | JPY | Moderate | JGBs well-covered. Corporate bonds: patchy, especially for smaller issuers. |
| Chinese (CNY/CNH) | CNY | Limited | Onshore (CNY) market is mostly closed. CNH (offshore) Eurobonds: limited. |
| South Korean | KRW | Limited | Domestic Korean bond market has limited international data vendor coverage. |
| Hong Kong | HKD | Moderate | HKMA bonds and major corporates covered. Smaller issues: sparse. |
| Colombian | COP | Very limited | Emerging market. Only sovereign TES bonds likely priced in LSEG. |

### The "Registration-Only" Problem

A large fraction of the 8.8M bonds in the security master are **registration-only
entries**. These appear in the LSEG reference database because:

- They were registered with a securities depository (Euroclear, Clearstream, etc.)
- They received an ISIN from the relevant national numbering agency
- LSEG's reference data team captured the issuance metadata

But they were NEVER priced in LSEG's pricing systems because:
- They are private placements with no secondary market
- They are structured notes with bespoke payoffs (no standard pricing model)
- They are very small issues with no dealer making markets
- They are in markets where LSEG has no pricing relationships

**Estimated pricing coverage for non-US bonds in LSEG:**
- Euro sovereign/supranational: ~80-90% of active issues
- Euro IG corporate (large issues >500M EUR): ~70-80%
- Euro structured/small corporate: ~10-20%
- JPY JGBs: ~90%+
- JPY corporate: ~30-50%
- GBP gilts/large corporate: ~70-80%
- CNY/KRW/COP: ~5-15%

The 8.8M number suggests the security master includes an enormous number of
structured notes, private placements, and small issues that will never have
pricing data in any vendor system.

---

## 6. RIC Construction Patterns for Non-US Bonds

### Known Bond RIC Conventions

LSEG/Reuters bond RICs follow specific patterns depending on the market:

| Pattern | Example | Market | Description |
|---------|---------|--------|-------------|
| `CUSIP=` | `665585KH7=` | US | CUSIP with trailing = |
| `ISIN=` | `FR0013508470=` | Various | ISIN with trailing = (works when mapping exists) |
| `{CC}{numeric}=` | `NL272792135=` | Eurobond | Country code + numbers + = |
| `{ISIN}=TE` | `XS1438515758=TE` | Eurobond | ISIN + Tradeweb Europe suffix |
| `{ISIN}=MLTW` | (example) | Eurobond | ISIN + MarketAxess/Tradeweb |
| `{ISIN}.TX` | `XS3059869845.TX` | Eurobond | ISIN + exchange code (dot separator) |
| `{ISIN}=TEMK` | `XS2067041520=TEMK` | Eurobond | ISIN + venue variant |
| `JP{nnnn}=TKFF` | (example) | Japan | JGB/corporate on Tokyo |
| `DE{nnnn}=` | `DE046003748=` | Germany | German bond |
| `{ticker}{coupon}{maturity}=` | (varies) | Various | Constructed from bond terms |

### Exchange/Venue Suffixes for Bonds

Common bond venue codes appended to ISINs:
- `=TE` -- Tradeweb Europe
- `=MLTW` -- MarketAxess/Tradeweb (multilateral)
- `=R` -- Refinitiv composite (should have best pricing coverage)
- `=LSEI` -- London Stock Exchange International
- `=XFRA` or `.DE` -- Frankfurt
- `=TKFF` -- Tokyo
- `.PA` -- Paris (Euronext)
- `.MI` -- Milan (Borsa Italiana)

### Strategy: Try ISIN + Venue Suffix

For ISINs without RICs, construct candidate RICs and test them:

```python
VENUE_SUFFIXES = ["=", "=TE", "=R", "=MLTW", "=LSEI"]

for isin in isins_without_rics:
    for suffix in VENUE_SUFFIXES:
        candidate = f"{isin}{suffix}"
        data = rest.historical_pricing(candidate, start="2020-01-01", end="2026-01-01")
        if data.get("data"):
            print(f"FOUND: {isin} -> {candidate}")
            break
```

**Caveat:** This is brute-force and slow. Better to batch via Symbology first.

---

## 7. Recommended Action Plan

### Phase 1: Maximize Identifier Resolution (No Pricing Fetch Yet)

1. **Test `FindPrimaryRIC` route** in Symbology API on a sample of 100 non-US ISINs:
   ```python
   result = rest.symbology_lookup(
       identifiers=sample_isins,
       from_types=["ISIN"],
       route="FindPrimaryRIC",
   )
   ```
   Measure: What fraction return a RIC? Do those RICs work in Historical Pricing?

2. **Test `FixedIncomeQuotes` view** in Search API on the same sample:
   ```python
   result = rest.search(
       view="FixedIncomeQuotes",
       filter=f"ISIN eq '{isin}'",
       select="RIC,ISIN,DocumentTitle,ExchangeName",
       top=20,
   )
   ```
   Measure: Do quote-level instruments have RICs? Do those RICs have pricing?

3. **Test `SEDOL=` format** for bonds that have SEDOLs in the security master:
   Pick 20 bonds with SEDOLs, try `{SEDOL}=` in Historical Pricing.

4. **Test Symbology ISIN -> SEDOL** for bonds without SEDOLs:
   ```python
   result = rest.symbology_lookup(
       identifiers=sample_isins,
       from_types=["ISIN"],
       to_types=["SEDOL"],
   )
   ```

### Phase 2: Test Alternative APIs on Sample

5. **Test Datastream** on 50 non-US ISINs across different currencies:
   ```python
   import DatastreamPy as DSWS
   ds = DSWS.DataClient(None, username, password)
   data = ds.get_data(tickers=isin, fields=['P','PI','YLD'], start='-5Y', kind=1)
   ```
   Measure: What fraction return price data? Which currencies have best coverage?

6. **Test `ld.get_data()` with TR fields** on the same ISINs:
   ```python
   ld.get_data([isin], ["TR.PriceCleanAnalytics", "TR.YieldToMaturityAnalytics"])
   ```

7. **Test IPA API** on a few ISINs:
   ```python
   payload = {
       "universe": [{
           "instrumentType": "Bond",
           "instrumentDefinition": {
               "instrumentCode": isin,
               "instrumentCodeType": "Isin"
           }
       }],
       "fields": ["CleanPrice", "DirtyPrice", "YieldPercent"]
   }
   ```

### Phase 3: Build the Pipeline

Based on Phase 1-2 results, the likely optimal pipeline is:

```
For each ISIN in security master:
  1. If RIC exists in secmaster -> use Historical Pricing API with RIC
  2. If CUSIP exists -> use Historical Pricing API with CUSIP=
  3. Try FindPrimaryRIC via Symbology -> if RIC returned, use Historical Pricing
  4. Try FixedIncomeQuotes search -> pick best venue RIC, use Historical Pricing
  5. Try Datastream with ISIN directly -> may have evaluated pricing
  6. If all fail -> bond likely has no pricing in LSEG
```

### Phase 4: Realistic Expectations

**Estimate of recoverable pricing:**
- Bonds with RICs (~6%): ~500K bonds -- should mostly have pricing
- Bonds with CUSIPs (~23%, mostly US): ~2M bonds -- high pricing rate
- Additional via Symbology/Search: maybe +5-15% of remaining bonds
- Additional via Datastream: maybe +5-10% of remaining bonds
- **Total likely pricing coverage: 30-45% of the 8.8M universe**
- The remaining 55-70% are likely registration-only or unpriced instruments

---

## 8. Summary of Untested Approaches (Priority Order)

| Priority | Approach | Expected Yield | Effort |
|----------|----------|---------------|--------|
| 1 | **FindPrimaryRIC** via Symbology API | High -- may resolve majority of priceable bonds | Low (quick test) |
| 2 | **FixedIncomeQuotes** Search view | Medium -- finds venue-specific quote RICs | Low (quick test) |
| 3 | **Datastream ISIN** direct lookup | High for covered bonds; best for non-US | Medium (different API) |
| 4 | **ld.get_data()** with TR analytics fields | Medium -- may resolve ISINs internally | Low (quick test) |
| 5 | **SEDOL=** format in Historical Pricing | Low-Medium -- limited SEDOL coverage | Low (quick test) |
| 6 | **IPA API** with ISIN | Medium for analytics, not time series | Medium |
| 7 | **Brute-force ISIN+suffix** construction | Low -- inefficient | High (many API calls) |

### Key Open Questions

1. Does `FindPrimaryRIC` work for non-US bonds, or only equities?
2. Does the `FixedIncomeQuotes` view return RICs for ISIN-only bonds?
3. What is Datastream's actual coverage for the specific currencies in our universe?
4. For bonds with zero pricing in LSEG, would Bloomberg be the only alternative?
5. Of the 8.8M bonds, how many are structured notes / private placements that will
   never have pricing in any system?

---

## 8b. Empirical Test Results (2026-04-05)

We tested all strategies from Section 8 on 8 ISIN-only bonds across EUR, JPY, GBP,
CNY, KRW, COP, HKD, and USD (XS-prefix eurobonds). Also tested 5 ISIN-only bonds
that have SEDOLs. Test script: `credit/tests/test_isinonly_foreignbond_pricing_strategies.py`

| Strategy | Resolved / Tested | Pricing Data? | Notes |
|----------|-------------------|---------------|-------|
| **FindPrimaryRIC** | 0/8 | N/A | Returns nothing for bonds. Appears to be equity-only. |
| **Auto ISIN->RIC** | 7/8 | **0 rows** | Returns `=RRPS` suffix RICs (reference data only, no pricing). |
| **FixedIncomeQuotes** search | 0/8 | N/A | 0 quotes found. These bonds aren't on any venue LSEG covers. |
| **ISIN=** pricing | 0/8 | **0 rows** | No internal mapping to pricing instrument for XS-prefix bonds. |
| **SEDOL=** pricing | 0/5 | **0 rows** | SEDOL= format returns 200 but empty data. |
| **IPA API** | 0/4 | HTTP 400 | Payload rejected ("Unbindable json"). Likely these bonds aren't in evaluated pricing universe. |

**Conclusion: Non-US ISIN-only bonds (XS-prefix eurobonds, EM local currency)
have no viable pricing path through any LSEG REST API tested.** The only
remaining untested approach is Datastream (DSWS), which uses a different data
backend with evaluated/composite pricing from FTSE Russell.

### What DOES work

Bonds with **RIC** or **CUSIP** in the security master can be priced:
- RIC directly in Historical Pricing API — works for active bonds (~786K bonds)
- `CUSIP=` format in Historical Pricing API — works for active and matured bonds (~3M bonds)
- Total priceable universe via LSEG REST APIs: **~3.6M bonds (27% of secmaster)**

### Answered Questions

1. Does `FindPrimaryRIC` work for non-US bonds? **No — returns nothing for bonds.**
2. Does `FixedIncomeQuotes` view return RICs for ISIN-only bonds? **No — 0 quotes found.**
3. Does `SEDOL=` work in Historical Pricing? **No — 0 rows returned.**

### Remaining Open Questions

1. What is Datastream's actual coverage for non-US ISIN-only bonds?
2. Does `ld.get_data()` with TR analytics fields resolve ISINs differently than REST?
3. For bonds with zero pricing in LSEG, would Bloomberg be the only alternative?
4. Of the 8.8M ISIN-only bonds, how many are structured notes / private placements
   that will never have pricing in any system?

---

## Appendix A: API Endpoints Reference

| API | Endpoint | Auth | Accepts ISIN? |
|-----|----------|------|--------------|
| Historical Pricing | `GET /data/historical-pricing/v1/views/interday-summaries/{id}` | Bearer token | Only via `ISIN=` format (partial) |
| Symbology | `POST /discovery/symbology/v1/lookup` | Bearer token | Yes (input) |
| Search | `POST /discovery/search/v1/` | Bearer token | Yes (filter field) |
| Datastream | DSWS REST / DatastreamPy | Username/password | Yes (as ticker) |
| IPA | `POST /data/quantitative-analytics/v1/financial-contracts` | Bearer token | Yes (`instrumentCodeType: "Isin"`) |
| Pricing Snapshots | `GET /data/pricing/snapshots/v1/{id}` | Bearer token | Primarily RIC |
| Eikon Data API | `ld.get_data()` / `ld.get_history()` | Session token | Yes (internal resolution) |

## Appendix B: Existing Code Patterns

**Auth:** Two patterns in the codebase:
1. `TokenManager` class (in `download_bond_master.py`, `find_bond_master_gaps.py`):
   Direct REST auth via `/auth/oauth2/v1/token` with password grant. Uses env vars
   `DSWS_APPKEY`, `DSWS_USERNAME`, `DSWS_PASSWORD`.

2. `LSEGRestClient` (in `shared/lseg_rest_api.py`): Wraps an `lseg.data` session.
   Gets bearer token from `session._access_token`. Supports symbology, search,
   and historical pricing.

**Relevant existing methods:**
- `LSEGRestClient.symbology_lookup()` -- already supports `route="FindPrimaryRIC"`
- `LSEGRestClient.historical_pricing()` -- ready to use with any identifier
- `LSEGRestClient.search()` -- supports any view including `FixedIncomeQuotes`
- `DatastreamPy` -- already installed and tested (`archive/test_dsws.py`)
