# Intraday Options Pricing — TODO

## Known Issues to Revisit

- **RUT returned 0 bars on all ~40K contracts** — RUT is CBOE-listed, not OPRA. The `.U` suffix RIC format we construct is wrong for it. Need to find correct CBOE RIC format.
- **NDX, SPX, XSP, MRUT** — all ran with the 2-week limit. NDX returned 0 bars (format mismatch confirmed). SPX returned only 709K bars across 72K contracts (very low hit rate — mostly wrong RICs). XSP returned 338M bars and appears to have worked. Need to investigate correct RIC format for NDX and SPX specifically.
- **Index options general issue**: OptionMetrics covers many index products whose LSEG RICs don't follow the OPRA equity format. Requires separate investigation into correct RIC construction per exchange.

---

## Overall Strategy

Work name by name, testing as we go. After each name completes, validate that we got all possible time series before moving on (see item 6).

**Phase 1 — OptionMetrics-sourced contracts (known RICs, cleaner)**
- Source of truth: `option_metrics.option_pricing` — gives us exact strikes/expiries
- 1-min bars feasible only for contracts expiring after ~March 20, 2025 (1-year rolling window from today)
- Proceed name by name through OM coverage

**Phase 2 — Post-OM-database contracts (Aug 29, 2025 → present)**
- OM database ends Aug 29, 2025; contracts expiring after that date are not in OM
- Must discover or construct RICs without OM as a source of truth
- Contract discovery at scale is the key unsolved problem (see item 4)

**Phase 3 — Trade ticks**
- Once we have a complete set of RICs from phases 1+2, pull trade ticks as far back as possible
- Trade tick retention: ~3 months for active contracts, ~3 days after expiry
- Must be done promptly — contracts expiring soon will lose their tick history quickly

---

## 1. ~~Optimize API query speed~~ (done)
- Resolved: adaptive rate limiter at 23 req/sec, 8 parallel workers, token bucket with 10% backoff on 429
- `download_minute_bars.py` and `download_om_minute_bars.py` already use this

## 2. Get 1-min bars for OptionMetrics contracts (name by name)

**Current status:**
- NVDA: `download_om_minute_bars.py NVDA` running now (~9,444 contracts, ~1 year window)

**Next up:** AMD, TSLA, SPY — same script, same window

**Then:** Extend to all ~5,700 OM securities
- Need mapping from OM secid → LSEG ticker root for RIC construction
- OM `option_pricing` has secid; need a secid→ticker table
- Only securities with options expiring after March 20 2025 are feasible

**Known issue:** Jan/Feb/Mar 2026 expiries — `^A26`/`^B26`/`^C26` suffix returned no data.
May not be flagged as expired yet in LSEG system. Retry these later.

## 3. Get 1-min bars for post-OM contracts (Aug 29, 2025 → present)

These contracts are not in OptionMetrics — must construct or discover RICs independently.

**The problem:** Expired option discovery via LSEG Discovery Search (`AssetState eq 'DC'`) returns 0 results.
Brute-force RIC construction (guess every strike/expiry combo) is too slow at scale and wastes quota on misses.

**Revisit contract discovery approach before brute-forcing:**
- LSEG Discovery Search with `AssetState eq 'AC'` works for active contracts — run it now before they expire
- Consider whether OPRA publishes a contract master file independently of LSEG
- Consider whether there is a chain/navigation endpoint that returns all strikes for a given expiry
- For names with weekly 0DTEs (SPY, QQQ, etc.), weekly expiries from Aug–present are the priority
- Short-term workaround: download active-contract lists now for all names while still accessible

## 4. Validate completeness after each name (routine check)

After completing any name:
1. Compare contracts downloaded vs known universe (from OM + any other source)
2. Flag contracts that may have expired mid-run and thus used the wrong RIC format
   - If a contract expired while we were querying, active-format query returns 0 bars
   - Must detect these (bars=0 for a contract that should have data) and re-query with expired suffix
3. Check date range coverage — are we getting the full 1-year window?
4. Log gaps for follow-up

**Edge case:** Contracts that expire while a run is in progress switch from active to expired RIC format mid-query.
Resolution: after each name finishes, re-query any zero-bar contracts with the expired suffix.

## 5. Pull trade ticks for all discovered contracts

Once we have a complete RIC list from phases 1 and 2:
- Download trade ticks for each contract using `download_option_ticks.py`
- Trade tick retention: ~3 months for active, ~3 days post-expiry — time-sensitive
- Prioritize contracts closest to expiry (most at risk of losing history)
- Script already handles expired RIC format and resume logic

## 6. Get intraday underlying pricing

- Download 1-min bars and trade ticks for the underlying equities/ETFs themselves
- Much simpler: one RIC per security, longer retention
- Needed for delta hedging, realized vol, moneyness calculations

## 7. Load into ClickHouse, drop CSVs

Once operating at scale and churning through many names:
- Load `om_minute_bars.csv`, `minute_bars.csv`, `trade_ticks.csv` into ClickHouse
- Use appropriate compression codecs (ZSTD) and sort keys for efficient querying
- Drop raw CSVs after verified load to reclaim disk space
- Consider partitioning by expiry month or underlying ticker
- Better compression vs CSV: expect 5–10x size reduction
