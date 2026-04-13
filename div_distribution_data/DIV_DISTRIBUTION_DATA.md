# Dividend Distribution, Ex-Date & Adjustment Factor Data Sources

## What We Already Have

### 1. CRSP `distributions` — Best US Source (Already Loaded)

- **170,974 events**, 2020-01-02 to 2025-12-31
- **Ex-dates** (`DisExDt`), declaration dates (`DisDeclareDt`), record dates (`DisRecordDt`), pay dates (`DisPayDt`)
- **Dividend amounts** (`DisDivAmt`)
- **Adjustment factors**: `DisFacPr` (price), `DisFacShr` (share) — populated for splits, spinoffs, mergers
- **Event types**:
  - 159K cash dividends (CD/CDIV)
  - 2.6K stock splits (FRS/STKSPL)
  - 2K special dividends (SD/SDIV)
  - 2K capital gains (CG/CAPG)
  - 1.3K return of capital (ROC)
  - 787 mergers (SP/SECMRG)
- **US stocks only**, linked by PERMNO
- **ClickHouse**: `crsp.distributions`

### 2. Compustat `fundq_std` — Quarterly Aggregates Only

- `dvpspq` / `dvpsxq` — dividends per share (quarterly)
- `ajexq` / `ajpq` — cumulative adjustment factors
- Accounting-level, not event-level — no ex-dates, just "this quarter this company paid X per share"
- Not useful if you need precise ex-dates or per-distribution detail
- **ClickHouse**: `compustat.fundq_std`

---

## What's Missing / Options to Fill Gaps

### For More US History (Pre-2020)

CRSP distributions only go back to 2020 in our ClickHouse instance. The full CRSP
`dsedist` table on WRDS goes back decades. That's a one-time ClickHouse load from WRDS.

### For European Stocks — LSEG Is the Best Bet

CRSP and Compustat are US-only. LSEG/Refinitiv has corporate actions data for global
equities. Two approaches:

#### A. LSEG Data Library `get_data()` with TR Fields

```python
import lseg.data as ld
ld.open_session()

# Per-security dividend history
df = ld.get_data(
    universe=["VOD.L", "SAN.PA", "AAPL.O"],
    fields=[
        "TR.DivExDate",
        "TR.DivPayDate",
        "TR.DivRecordDate",
        "TR.DivUnadjustedGross",
        "TR.DivAdjustedGross",
        "TR.DivType",
        "TR.DivCurrency",
        "TR.CAEffectiveDate",      # corporate action date
        "TR.CAAdjustmentFactor",   # split/adjustment factor
        "TR.CAAdjustmentType"
    ],
    parameters={"SDate": "2015-01-01", "EDate": "2026-04-13"}
)
```

#### B. LSEG Corporate Actions REST Endpoint

The REST API has a `/corporate-actions` endpoint that returns dividends, splits, rights
issues, etc. for any RIC globally.

### Adjustment Factors Specifically

- **CRSP** has `DisFacPr` and `DisFacShr` on splits/spinoffs (2,589 STKSPL + 94 STKDIV rows)
- **LSEG** `TR.CAAdjustmentFactor` gives cumulative adjustment factors for splits, reverse splits, rights issues — works for both US and European
- **Compustat** `ajexq` is cumulative but quarterly granularity

---

## Recommendation Summary

| Need | Best Source |
|------|------------|
| US ex-dates + amounts (2020+) | CRSP `distributions` — already loaded |
| US ex-dates + amounts (pre-2020) | WRDS CRSP `dsedist` — needs one-time pull |
| US adjustment factors (splits) | CRSP `distributions` where `DisFacPr != 0` |
| European dividends + ex-dates | LSEG `TR.DivExDate` / `TR.DivUnadjustedGross` |
| European adjustment factors | LSEG `TR.CAAdjustmentFactor` |
