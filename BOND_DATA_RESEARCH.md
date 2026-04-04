# LSEG Corporate Bond Data — What's Available

LSEG's fixed income platform covers ~5.1M live instruments globally with evaluated pricing for ~2.7M daily. History goes back to 1990 for some instruments. Data sourced from 800+ contributors including Tradeweb, MarketAxess, TRACE, ICAP, etc.

**Key access note**: The `lseg.data` Python SDK supports a "Platform Session" (`platform.ldp`) that works headless — no Workspace desktop needed. But not all content is available this way, and some (like IPA bond analytics) require a separate license.

---

## 1. Bond Reference Data / Security Master — STRONG

The IPA Bond API returns **287 columns** per bond:

- **Identifiers**: RIC, ISIN, CUSIP, SEDOL, PermID, issuer OrgID, equity ticker
- **Terms**: issue date, maturity, coupon rate, interest type, payment frequency, notional, currency, par value, denomination, issue price, amount issued
- **Structure**: callable/puttable/perpetual/sinkable/amortized/PIK flags
- **Floating rate**: index name, fixing RIC, tenor, spread
- **Classification**: bond type, debt type, industry subsector, country of issuer/incorporation

**Access**: `ld.get_data()` with TR fields, IPA bond contracts API, DataScope Select for bulk extraction, Search API for discovery with filters.

**Gap**: No direct GVKEY/PERMNO mapping — you'd chain ISIN/CUSIP through WRDS crosswalk tables.

**Alternative**: For academic research linking to CRSP/Compustat, **WRDS Mergent FISD** (actually an LSEG product!) is better — 140K+ issues with 550+ data items and direct WRDS linking tables.

---

## 2. Bond Pricing and Return Data — MODERATE-STRONG (caveats on history)

### Daily Evaluated Pricing (REPS)

~2.7M instruments, 175+ analytics per instrument:

- Bid/ask/mid price (clean & dirty), bid/ask yield, OAS, Z-spread, G-spread, swap spread, asset swap spread
- Modified duration, convexity, DV01, accrued interest

### IPA Bond Analytics (on-demand calculation) — Excellent

- **Spreads**: ZSpreadBp, OAS, AssetSwapSpread, GovernmentSpread, SwapSpread, CdsSpread, OisZcSpread, and more
- **Yields**: YTM, YTW, YTB, yield-to-next-call/put, current yield, strip yield
- **Risk**: modified duration, convexity, DV01, full cash flow generation
- **Callable/puttable** analytics using Hull-White model

### Bond Indices

LSEG has its own indices (Datastream calculated, FTSE Russell Fixed Income) back to Dec 1988. **NOT ICE BofA** — those require ICE or Bloomberg.

### Critical Limitation on History Depth

Despite LSEG claiming "15+ years," community reports show API-accessible historical bond pricing often only goes back to **~2021** for individual bonds. Datastream (requires Workspace desktop) may go deeper.

**Alternative**: For individual bond returns, **WRDS BondRet** (from TRACE) is the academic standard — monthly price, return, coupon, yield for all US corporates from July 2002+.

---

## 3. Trading / Liquidity Data (TRACE-style) — WEAK

LSEG ingests TRACE as a pricing input and has Tick History back to 1996, but **does not redistribute raw TRACE transaction-level data** through their APIs. You can get some accumulated volume data but not trade-by-trade (price, volume, buy/sell indicator, dealer reporting).

**For TRACE, use WRDS**: Enhanced TRACE + BondRet, with linking tables to CRSP/Compustat/Mergent FISD. Or FINRA directly (36-month academic delay).

---

## 4. Treasury and Risk-Free Curve Data — STRONG

- **Zero Coupon Curve API** (IPA): Multi-curve framework, supports deposits/FRAs/futures/swaps, SOFR/SONIA/ESTR, custom curve construction
- **Historical government bond yields** via `ld.get_history()` (e.g., `US10YT=RR`)
- Good calculation engine for real-time/custom curves

**But for bulk historical research**: Fed H.15 data and Gurkaynak-Sack-Wright zero-coupon curves are free and are the academic standard. OptionMetrics zero-coupon curves (2000-2023) are already loaded in ClickHouse.

---

## 5. Credit Ratings History — MODERATE (practical limitations)

- S&P, Moody's, Fitch, Dominion ratings available
- Issuer-level and issue-level, with outlook data
- Fields: `TR.FiIssuerFitchLongRating`, `TR.FiIssuerMoodysLongRating`, `TR.FiIssuerSPLongRating`, `TR.IR.RatingDate`

**Limitations**:

- **S&P licensing**: S&P changed redistribution policy — you may need a direct S&P license to access through LSEG
- API rate limits make bulk extraction painful (server timeouts on large requests)
- Getting full time-series of rating *changes* (not just current) requires careful parameter work

**Alternative**: WRDS Mergent FISD includes rating history. Moody's DRD and S&P RatingsDirect via WRDS are more practical for bulk academic use.

---

## 6. Default Events and Recoveries — WEAK

LSEG **does not have** a dedicated corporate default/recovery database. No equivalent to Moody's DRD or S&P CreditPro. StarMine predicts forward-looking PD but doesn't provide historical default events.

**Alternatives**:

- **Moody's Default & Recovery Database (DRD)** via WRDS — gold standard
- **S&P CreditPro** — comprehensive default and transition data
- **UCLA-LoPucki Bankruptcy Research Database** — free for large-firm bankruptcies
- WRDS BondRet can identify distressed bonds through price drops

---

## 7. Distance-to-Default / EDF-style Measures — STRONG (via StarMine)

Five StarMine credit risk models:

| Model | Methodology | History |
|-------|------------|---------|
| **Structural Credit Risk (SCR)** | Merton-model with Value-Momentum drift | 1998+ |
| **SmartRatios Credit Risk** | Financial ratios + analyst estimates | 1998+ |
| **Text Mining Credit Risk (TMCR)** | NLP on news, 10-K/Q, transcripts | 1998+ |
| **Combined Credit Risk (CCR)** | Logistic regression combining above 3 | 1998+ |
| **Sovereign Risk** | Country-level | — |

Output: 12-month PD, mapped letter ratings, percentile scores (1-100). Updated daily, global coverage.

**Requires separate StarMine license** — not included in base access. Delivery via FTP feeds, DataScope Select, or API.

**Alternative**: Moody's KMV/EDF is the original and most widely cited in academic literature, but StarMine SCR is a credible substitute.

---

## 8. CDS Data — MODERATE

- End-of-day CDS pricing across **4,000+ curves** globally
- RIC format: `{TICKER}{TENOR}{REGION}{TYPE}={SOURCE}` (e.g., `MSFT5YUSAX=R`)
- Fields: par mid spread, bid/ask spread
- Multiple tenors (1Y, 3Y, 5Y, 7Y, 10Y)
- Historical time series via `ld.get_history()`

**Limitations**: Finding correct CDS RICs is non-trivial. History depth via API may be limited. LSEG acknowledges weak CDS support in developer community.

**Alternative**: **IHS Markit (now S&P Global)** is the gold standard for CDS — widest coverage, longest history. Available through WRDS at some institutions.

---

## Identifier Mapping — STRONG

Symbology API maps between: RIC, ISIN, CUSIP, SEDOL, LEI, PermID, Ticker, LipperID.

**Not natively supported**: FIGI, GVKEY, PERMNO. You'd chain: LSEG ISIN/CUSIP → WRDS crosswalk → GVKEY/PERMNO.

---

## Bottom Line: What to Get Where

### From LSEG (already have access)

- Bond reference data / security master (IPA + DataScope)
- IPA bond analytics engine (on-demand spread/risk calcs — excellent)
- StarMine credit risk models (if licensed)
- CDS data on liquid names
- Zero coupon curve construction
- Identifier mapping (Symbology API)

### From WRDS (needed to fill gaps)

- **Mergent FISD** — bond reference data with CRSP/Compustat linking (this is actually LSEG Mergent)
- **Enhanced TRACE + BondRet** — transaction data + monthly bond returns (Jul 2002+)
- **Moody's DRD** — default events and recovery rates
- **Markit CDS** — if institution has access
- **CRSP-Compustat linking tables** — for the bond→equity→fundamentals bridge

### From Free/Public Sources

- Fed H.15 / GSW zero-coupon curves
- UCLA-LoPucki bankruptcy database
- FINRA academic TRACE (36-month delay)

---

## The Identifier Bridge

```
Bond CUSIP/ISIN
  → (LSEG Symbology) → PermID / Ticker
  → (WRDS Mergent FISD linking) → GVKEY
  → (CRSP-Compustat link) → PERMNO
```

This is the critical "hidden dataset" — the clean crosswalk from bond to issuer to equity to fundamentals. Mergent FISD on WRDS is probably the best single piece to solve this, since it has both bond identifiers and issuer-level IDs that map to Compustat.

---

## Tier Summary

| Category | LSEG Rating | Best Source |
|----------|-------------|-------------|
| Bond reference / security master | Strong | LSEG + WRDS Mergent FISD |
| Bond pricing & analytics | Strong (analytics), Moderate (history) | LSEG IPA + WRDS BondRet |
| Trading / TRACE | Weak | WRDS Enhanced TRACE |
| Treasury / risk-free curves | Strong | Fed H.15 / GSW (free) |
| Credit ratings history | Moderate | WRDS Mergent FISD / Moody's DRD |
| Default events & recoveries | Weak | Moody's DRD via WRDS |
| Distance-to-default / EDF | Strong (StarMine) | LSEG StarMine or Moody's KMV |
| CDS data | Moderate | IHS Markit via WRDS or Bloomberg |
| Identifier mapping | Strong | LSEG Symbology + WRDS crosswalks |
