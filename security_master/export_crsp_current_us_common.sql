WITH latest AS (
    SELECT
        PERMNO,
        argMax(CUSIP, SecInfoStartDt) AS CUSIP,
        argMax(Ticker, SecInfoStartDt) AS Ticker,
        argMax(IssuerNm, SecInfoStartDt) AS IssuerNm,
        argMax(ShareClass, SecInfoStartDt) AS ShareClass,
        argMax(USIncFlg, SecInfoStartDt) AS USIncFlg,
        argMax(IssuerType, SecInfoStartDt) AS IssuerType,
        argMax(SecurityType, SecInfoStartDt) AS SecurityType,
        argMax(SecuritySubType, SecInfoStartDt) AS SecuritySubType,
        argMax(ShareType, SecInfoStartDt) AS ShareType,
        argMax(SecurityActiveFlg, SecInfoStartDt) AS SecurityActiveFlg,
        argMax(PrimaryExch, SecInfoStartDt) AS PrimaryExch,
        argMax(TradingSymbol, SecInfoStartDt) AS tradingsymbol,
        argMax(TradingStatusFlg, SecInfoStartDt) AS TradingStatusFlg
    FROM crsp.security_names
    GROUP BY PERMNO
)
SELECT
    PERMNO AS permno,
    CUSIP AS cusip8,
    Ticker AS ticker,
    IssuerNm AS comnam,
    ShareClass AS shareclass,
    USIncFlg AS usincflg,
    IssuerType AS issuertype,
    SecurityType AS securitytype,
    SecuritySubType AS securitysubtype,
    ShareType AS sharetype,
    SecurityActiveFlg AS securityactiveflg,
    PrimaryExch AS primaryexch,
    tradingsymbol,
    TradingStatusFlg AS tradingstatusflg
FROM latest
WHERE USIncFlg = 'Y'
  AND SecurityType = 'EQTY'
  AND SecuritySubType = 'COM'
  AND SecurityActiveFlg = 'Y'
  AND TradingStatusFlg = 'A'
ORDER BY permno
FORMAT CSVWithNames
