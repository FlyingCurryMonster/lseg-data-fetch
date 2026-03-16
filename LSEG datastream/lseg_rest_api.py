"""
Direct REST API client for LSEG/Refinitiv endpoints not exposed by the
lseg-data Python library.

Usage:
    import lseg.data as ld
    from lseg_rest_api import LSEGRestClient

    session = ld.open_session(config_name=config_path)
    rest = LSEGRestClient(session)

    # symbology with showHistory
    result = rest.symbology_lookup_df(
        identifiers=["US30303M1027"], from_types=["ISIN"],
        to_types=["RIC"], show_history=True
    )

    # search with OData filters (e.g., expired options)
    result = rest.search(
        query="S&P 500 Annual Dividend Option",
        filter="AssetState eq 'DC'",
        top=500
    )

    ld.close_session()
"""

import requests
import pandas as pd


class LSEGRestClient:
    """Client for LSEG REST endpoints that require a bearer token from an
    active lseg.data session.

    Does NOT own the session lifecycle — the caller opens/closes the ld session.
    """

    # -- Endpoint URLs --
    SYMBOLOGY_URL = "https://api.refinitiv.com/discovery/symbology/v1/lookup"
    SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
    HISTORICAL_PRICING_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries"

    def __init__(self, session):
        """
        Args:
            session: An already-open lseg.data session
                     (returned by ld.open_session()).
        """
        self._session = session
        self._token = session._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _refresh_token(self):
        """Re-read the token from the session (the library may have
        silently refreshed it)."""
        self._token = self._session._access_token

    def _post(self, url: str, payload: dict) -> requests.Response:
        """POST with automatic 401 retry after token refresh."""
        resp = requests.post(url, headers=self._headers(), json=payload)
        if resp.status_code == 401:
            self._refresh_token()
            resp = requests.post(url, headers=self._headers(), json=payload)
        return resp

    def _get(self, url: str, params: dict = None) -> requests.Response:
        """GET with automatic 401 retry after token refresh."""
        resp = requests.get(url, headers=self._headers(), params=params)
        if resp.status_code == 401:
            self._refresh_token()
            resp = requests.get(url, headers=self._headers(), params=params)
        return resp

    # ==================================================================
    # 1. Symbology Lookup
    # ==================================================================

    def symbology_lookup(
        self,
        identifiers: list[str],
        from_types: list[str],
        to_types: list[str] | None = None,
        route: str | None = None,
        show_history: bool = False,
        effective_at: str | None = None,
    ) -> dict:
        """Look up identifier mappings via the Discovery Symbology API.

        Supports two modes:
          - type="auto": provide to_types to map between identifier systems
          - type="predefined": provide route (e.g., "FindPrimaryRIC") for
            predefined conversion logic

        Args:
            identifiers: List of identifier values (e.g., ISINs, CUSIPs).
            from_types:  Identifier type(s) of the input (e.g., ["ISIN"]).
            to_types:    Target identifier type(s) (e.g., ["RIC"]).
                         Required for type="auto", ignored for type="predefined".
            route:       Predefined route name (e.g., "FindPrimaryRIC").
                         If provided, uses type="predefined" instead of "auto".
            show_history: If True, returns effectiveFrom/effectiveTo dates for
                         each identifier mapping.
            effective_at: ISO 8601 UTC timestamp for point-in-time lookup
                         (e.g., "2020-01-01T00:00:00.000Z").

        Returns:
            Raw JSON response dict from the API.

        API endpoint:
            POST https://api.refinitiv.com/discovery/symbology/v1/lookup

        Docs:
            https://developers.lseg.com/en/api-catalog/refinitiv-data-platform/symbology-API

        User guide (PDF):
            https://developers.lseg.com/content/dam/devportal/api-families/
            refinitiv-data-platform/refinitiv-data-platform-apis/documentation/
            symbology_user_guide.pdf
        """
        payload = {
            "from": [{"identifierTypes": from_types, "values": identifiers}],
        }

        if route:
            payload["type"] = "predefined"
            payload["route"] = route
        else:
            payload["type"] = "auto"
            if to_types:
                payload["to"] = [{"identifierTypes": to_types}]

        if show_history:
            payload["showHistory"] = True
        if effective_at:
            payload["effectiveAt"] = effective_at

        resp = self._post(self.SYMBOLOGY_URL, payload)
        resp.raise_for_status()
        return resp.json()

    def symbology_lookup_df(self, **kwargs) -> pd.DataFrame:
        """Same as symbology_lookup() but parses the nested JSON response
        into a flat DataFrame.

        Returns DataFrame with columns:
            identifier, value, effective_from, effective_to

        See symbology_lookup() for parameter docs.

        Note: With show_history=True, each data item is a flat dict:
            {input: [{value, identifierType}],
             output: [{value, identifierType}],
             effectiveFrom: ..., effectiveTo: ...}
        Without show_history, output may contain a list of matches.
        This method handles both formats.
        """
        data = self.symbology_lookup(**kwargs)
        rows = []
        for item in data.get("data", []):
            input_list = item.get("input", [])
            identifier = input_list[0].get("value", "") if input_list else ""

            output_list = item.get("output", [])
            # With showHistory: effectiveFrom/To are at the item level
            effective_from = item.get("effectiveFrom")
            effective_to = item.get("effectiveTo")

            for out in output_list:
                if isinstance(out, dict) and "identifierType" in out:
                    # showHistory format: output is [{value, identifierType}]
                    rows.append({
                        "identifier": identifier,
                        "value": out.get("value"),
                        "effective_from": effective_from,
                        "effective_to": effective_to,
                    })
                elif isinstance(out, dict) and "value" in out:
                    # Non-history format: output has nested value list
                    for match in out.get("value", []):
                        if isinstance(match, dict):
                            rows.append({
                                "identifier": identifier,
                                "value": match.get("value"),
                                "effective_from": match.get("effectiveFrom"),
                                "effective_to": match.get("effectiveTo"),
                            })
                        else:
                            rows.append({
                                "identifier": identifier,
                                "value": match,
                                "effective_from": None,
                                "effective_to": None,
                            })

        return pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=["identifier", "value", "effective_from", "effective_to"]
        )

    # ==================================================================
    # 2. Discovery Search (direct REST)
    # ==================================================================

    def search(
        self,
        query: str = "",
        filter: str | None = None,
        select: str | None = None,
        top: int = 10,
        navigators: str | None = None,
        view: str | None = None,
    ) -> dict:
        """Search for instruments via the Discovery Search REST API.

        This is the direct REST equivalent of ld.discovery.search(), useful
        when you need OData filter parameters that the Python wrapper may not
        fully expose (e.g., filtering expired derivatives by AssetState).

        Args:
            query:      Free-text search query.
            filter:     OData filter expression
                        (e.g., "AssetState eq 'DC' and ExpiryDate lt 2024-01-01").
            select:     Comma-separated field names to return
                        (e.g., "RIC,DocumentTitle,ExpiryDate,AssetState").
            top:        Max number of results (default 10).
            navigators: Aggregation/grouping expression.
            view:       Search view (e.g., "DERIVATIVE_QUOTES", "EQUITY_QUOTES").

        Returns:
            Raw JSON response dict from the API.

        API endpoint:
            POST https://api.refinitiv.com/discovery/search/v1/

        Docs (requires login):
            https://apidocs.refinitiv.com/Apps/ApiDocs

        Community reference:
            https://community.developers.refinitiv.com/discussion/114548
        """
        payload = {
            "Query": query,
            "Top": top,
        }
        if filter:
            payload["Filter"] = filter
        if select:
            payload["Select"] = select
        if navigators:
            payload["Navigators"] = navigators
        if view:
            payload["View"] = view

        resp = self._post(self.SEARCH_URL, payload)
        resp.raise_for_status()
        return resp.json()

    # ==================================================================
    # 3. Historical Pricing (direct REST)
    # ==================================================================

    def historical_pricing(
        self,
        ric: str,
        start: str | None = None,
        end: str | None = None,
        interval: str = "P1D",
        fields: list[str] | None = None,
    ) -> dict:
        """Get historical interday pricing data for a single RIC via the
        Historical Pricing REST API.

        This is the direct REST equivalent of ld.get_history(), useful as a
        fallback when the Python wrapper behaves unexpectedly.

        Args:
            ric:      RIC code (e.g., "SDAZ27").
            start:    Start date (ISO 8601, e.g., "2020-01-01").
            end:      End date (ISO 8601, e.g., "2024-12-31").
            interval: Data interval. "P1D" for daily (default).
                      Other values: "P1W" (weekly), "P1M" (monthly).
            fields:   List of field names (e.g., ["TRDPRC_1", "SETTLE"]).
                      If None, returns all available fields.

        Returns:
            Raw JSON response dict from the API.

        API endpoint:
            GET https://api.refinitiv.com/data/historical-pricing/v1/
                views/interday-summaries/{ric}

        Docs:
            https://developers.lseg.com/en/api-catalog/refinitiv-data-platform/
            refinitiv-data-platform-apis
        """
        url = f"{self.HISTORICAL_PRICING_URL}/{ric}"
        params = {"interval": interval}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if fields:
            params["fields"] = ",".join(fields)

        resp = self._get(url, params=params)
        resp.raise_for_status()
        return resp.json()
