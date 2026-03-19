# Intraday Options Pricing — TODO

## 1. Optimize API query speed
- Profile current request rate and determine max throughput without triggering rate limits
- Test parallel requests, reduce sleep intervals, batch where possible
- Currently running ~1 req/s with 0.5s sleep; may be able to go faster

## 2. Get 1-min bars for all expired contracts in OptionMetrics option_pricing table
- Construct expired RICs from OptionMetrics contract identifiers using `<active_ric>.U^<month_code><YY>` format
- Only contracts expired within the last ~1 year have minute bars (rolling retention window)
- Prioritize oldest expiries first since they're closest to falling off the retention window
- Need to resolve: Jan/Feb/Mar 2026 expired suffix convention (`^A26` etc. didn't work — may not be flagged expired yet)

## 3. Generate list of expired contracts NOT in OptionMetrics option_pricing table
- Compare LSEG discoverable/constructable contracts against OptionMetrics coverage
- Identify gaps — contracts that traded on OPRA but aren't in OptionMetrics
- Particularly relevant for weekly 0DTEs which OptionMetrics may not fully cover

## 4. Get 1-min bars AND trade ticks for all active contracts across all names
- Currently downloading trade ticks for SPY (done), NVDA (done), AMD (in progress), TSLA (pending)
- Extend to all ~5,700 securities in OptionMetrics
- Need mapping from OptionMetrics secid → LSEG underlying RIC
- Trade ticks: ~3 month retention on active contracts
- 1-min bars: ~1 year retention, includes bid/ask OHLC

## 5. Get intraday underlying pricing for all names in option_pricing table
- Download 1-min bars and/or trade ticks for the underlying equities/ETFs
- These have longer retention and are simpler (one RIC per security vs thousands of option RICs)
- Needed for delta hedging, moneyness calculations, realized vol
