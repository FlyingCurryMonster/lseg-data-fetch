"""Test pricing strategies for non-US bonds with ISIN only (no RIC, no CUSIP).

Tests approaches from bond_pricing_from_secmaster_research.md:
  1. FindPrimaryRIC via Symbology API
  2. Auto ISIN -> RIC via Symbology
  3. FixedIncomeQuotes search view
  4. ISIN= format in Historical Pricing
  5. SEDOL= format in Historical Pricing
  6. ld.get_data() with TR analytics fields
  7. IPA API with ISIN
"""

import sys, os, json, time, requests
from dotenv import load_dotenv

# Auth — direct REST (same as download script)
AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token"
SEARCH_URL = "https://api.refinitiv.com/discovery/search/v1/"
SYMBOLOGY_URL = "https://api.refinitiv.com/discovery/symbology/v1/lookup"
HIST_PRICING_URL = "https://api.refinitiv.com/data/historical-pricing/v1/views/interday-summaries"
IPA_URL = "https://api.refinitiv.com/data/quantitative-analytics/v1/financial-contracts"

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"))

# Sample ISIN-only bonds across currencies (no RIC, no CUSIP in secmaster)
SAMPLE_ISINS = [
    ("XS2597926067", "EUR", "Edmond De Rothschild France"),
    ("XS2382271604", "JPY", "Vault Investments LLC"),
    ("XS2332523443", "GBP", "Westbury House 2021-1 Ltd"),
    ("XS2646058904", "CNY", "Chuxiong State-owned Capital"),
    ("XS1267239546", "KRW", "Goldman Sachs International"),
    ("XS0289852237", "COP", "Citigroup Inc"),
    ("XS0116796847", "HKD", "KBC Ifima SA"),
    ("XS3297117221", "USD", "CNCB Inv Financial Products"),
]

# ISIN-only bonds that DO have SEDOLs
SEDOL_SAMPLES = [
    ("XS3057945548", "BTXQ858", "SEK", "BNP Paribas Issuance BV"),
    ("GB0007676280", "0767628", "GBP", "J Sainsbury PLC"),
    ("XS0010069911", "4163307", "USD", "Cswi International Finance NV"),
    ("GB0041841379", "4184137", "USD", "JPMorgan Chase Bank NA"),
    ("GB0043686186", "4368618", "USD", "Bank of America NA"),
]


def authenticate():
    resp = requests.post(AUTH_URL, data={
        "grant_type": "password",
        "username": os.getenv("DSWS_USERNAME"),
        "password": os.getenv("DSWS_PASSWORD"),
        "client_id": os.getenv("DSWS_APPKEY"),
        "scope": "trapi",
        "takeExclusiveSignOnControl": "true",
    }, headers={"Content-Type": "application/x-www-form-urlencoded"})
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def test_find_primary_ric(token):
    """Strategy 1: FindPrimaryRIC route in Symbology API."""
    print("\n" + "=" * 70)
    print("STRATEGY 1: FindPrimaryRIC via Symbology API")
    print("=" * 70)

    isins = [s[0] for s in SAMPLE_ISINS]
    resp = requests.post(SYMBOLOGY_URL, headers=headers(token), json={
        "from": [{"identifierTypes": ["ISIN"], "values": isins}],
        "type": "predefined",
        "route": "FindPrimaryRIC",
    })

    if resp.status_code != 200:
        print(f"  ERROR: HTTP {resp.status_code} — {resp.text[:300]}")
        return {}

    data = resp.json()
    ric_map = {}
    for entry in data.get("data", []):
        isin = entry.get("input", [{}])[0].get("value", "?")
        outputs = entry.get("output", [])
        currency = next((s[1] for s in SAMPLE_ISINS if s[0] == isin), "?")
        if outputs:
            ric = outputs[0].get("value", "no value")
            ric_map[isin] = ric
            print(f"  {isin} ({currency}) -> {ric}")
        else:
            print(f"  {isin} ({currency}) -> NO RIC")

    print(f"\n  Resolved: {len(ric_map)}/{len(isins)}")
    return ric_map


def test_auto_isin_to_ric(token):
    """Strategy 2: Auto ISIN -> RIC via Symbology."""
    print("\n" + "=" * 70)
    print("STRATEGY 2: Auto ISIN -> RIC via Symbology")
    print("=" * 70)

    isins = [s[0] for s in SAMPLE_ISINS]
    resp = requests.post(SYMBOLOGY_URL, headers=headers(token), json={
        "from": [{"identifierTypes": ["ISIN"], "values": isins}],
        "to": [{"identifierTypes": ["RIC"]}],
        "type": "auto",
    })

    if resp.status_code != 200:
        print(f"  ERROR: HTTP {resp.status_code} — {resp.text[:300]}")
        return {}

    data = resp.json()
    ric_map = {}
    for entry in data.get("data", []):
        isin = entry.get("input", [{}])[0].get("value", "?")
        outputs = entry.get("output", [])
        currency = next((s[1] for s in SAMPLE_ISINS if s[0] == isin), "?")
        rics = [o.get("value", "") for o in outputs]
        if rics:
            ric_map[isin] = rics
            print(f"  {isin} ({currency}) -> {len(rics)} RICs: {rics[:5]}{'...' if len(rics) > 5 else ''}")
        else:
            print(f"  {isin} ({currency}) -> NO RICs")

    print(f"\n  Resolved: {len(ric_map)}/{len(isins)}")
    return ric_map


def test_fixed_income_quotes_search(token):
    """Strategy 3: FixedIncomeQuotes search view."""
    print("\n" + "=" * 70)
    print("STRATEGY 3: FixedIncomeQuotes Search View")
    print("=" * 70)

    results = {}
    for isin, currency, issuer in SAMPLE_ISINS:
        resp = requests.post(SEARCH_URL, headers=headers(token), json={
            "Query": "",
            "View": "FixedIncomeQuotes",
            "Filter": f"ISIN eq '{isin}'",
            "Select": "RIC,ISIN,DocumentTitle,ExchangeName",
            "Top": 20,
        })

        if resp.status_code != 200:
            print(f"  {isin} ({currency}) -> ERROR HTTP {resp.status_code}")
            continue

        data = resp.json()
        total = data.get("Total", 0)
        hits = data.get("Hits", [])
        rics = [h.get("RIC", "") for h in hits if h.get("RIC")]
        exchanges = [h.get("ExchangeName", "") for h in hits if h.get("ExchangeName")]

        if rics:
            results[isin] = rics
            print(f"  {isin} ({currency}) -> {total} quotes, RICs={rics[:5]}, Exchanges={exchanges[:5]}")
        else:
            print(f"  {isin} ({currency}) -> {total} quotes, NO RICs")

    print(f"\n  Resolved: {len(results)}/{len(SAMPLE_ISINS)}")
    return results


def test_isin_equals_pricing(token):
    """Strategy 4: ISIN= format in Historical Pricing."""
    print("\n" + "=" * 70)
    print("STRATEGY 4: ISIN= Format in Historical Pricing")
    print("=" * 70)

    results = {}
    for isin, currency, issuer in SAMPLE_ISINS:
        identifier = f"{isin}="
        resp = requests.get(
            f"{HIST_PRICING_URL}/{identifier}",
            headers=headers(token),
            params={"interval": "P1D", "count": 5},
        )

        if resp.status_code != 200:
            print(f"  {identifier} ({currency}) -> HTTP {resp.status_code}")
            continue

        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            rows = data[0].get("data", [])
            if rows:
                results[isin] = len(rows)
                hdrs = [h.get("name", "") for h in data[0].get("headers", [])]
                print(f"  {identifier} ({currency}) -> {len(rows)} rows, fields={hdrs[:6]}")
            else:
                print(f"  {identifier} ({currency}) -> 0 rows")
        else:
            print(f"  {identifier} ({currency}) -> empty response")

    print(f"\n  Got pricing: {len(results)}/{len(SAMPLE_ISINS)}")
    return results


def test_sedol_equals_pricing(token):
    """Strategy 5: SEDOL= format in Historical Pricing."""
    print("\n" + "=" * 70)
    print("STRATEGY 5: SEDOL= Format in Historical Pricing")
    print("=" * 70)

    results = {}
    for isin, sedol, currency, issuer in SEDOL_SAMPLES:
        identifier = f"{sedol}="
        resp = requests.get(
            f"{HIST_PRICING_URL}/{identifier}",
            headers=headers(token),
            params={"interval": "P1D", "count": 5},
        )

        if resp.status_code != 200:
            print(f"  {identifier} (ISIN={isin}, {currency}) -> HTTP {resp.status_code}")
            continue

        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            rows = data[0].get("data", [])
            if rows:
                results[isin] = len(rows)
                hdrs = [h.get("name", "") for h in data[0].get("headers", [])]
                print(f"  {identifier} (ISIN={isin}, {currency}) -> {len(rows)} rows, fields={hdrs[:6]}")
            else:
                print(f"  {identifier} (ISIN={isin}, {currency}) -> 0 rows")
        else:
            print(f"  {identifier} (ISIN={isin}, {currency}) -> empty response")

    print(f"\n  Got pricing: {len(results)}/{len(SEDOL_SAMPLES)}")
    return results


def test_ipa_api(token):
    """Strategy 7: IPA API with ISIN."""
    print("\n" + "=" * 70)
    print("STRATEGY 7: IPA (Instrument Pricing Analytics) API")
    print("=" * 70)

    results = {}
    for isin, currency, issuer in SAMPLE_ISINS[:4]:  # test a subset
        payload = {
            "universe": [{
                "instrumentType": "Bond",
                "instrumentDefinition": {
                    "instrumentCode": isin,
                    "instrumentCodeType": "Isin",
                },
            }],
            "fields": [
                "InstrumentDescription", "CleanPrice", "DirtyPrice",
                "YieldPercent", "ModifiedDuration", "Convexity",
                "AccruedInterest",
            ],
        }

        resp = requests.post(IPA_URL, headers=headers(token), json=payload)

        if resp.status_code != 200:
            print(f"  {isin} ({currency}) -> HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        data = resp.json()
        # IPA returns {"data": [{"headers": [...], "rows": [[...]]}]}
        ipa_data = data.get("data", [])
        if ipa_data:
            ipa_headers = [h.get("name", "") for h in ipa_data[0].get("headers", [])]
            ipa_rows = ipa_data[0].get("rows", [])
            if ipa_rows:
                results[isin] = ipa_rows[0]
                print(f"  {isin} ({currency}) -> {ipa_headers}")
                print(f"    values: {ipa_rows[0]}")
            else:
                print(f"  {isin} ({currency}) -> headers but no rows")
        else:
            print(f"  {isin} ({currency}) -> empty response: {json.dumps(data)[:300]}")

    print(f"\n  Got analytics: {len(results)}/{min(4, len(SAMPLE_ISINS))}")
    return results


def test_pricing_with_discovered_rics(token, ric_map):
    """Test Historical Pricing with RICs discovered from strategies 1-3."""
    print("\n" + "=" * 70)
    print("VALIDATION: Test Historical Pricing with Discovered RICs")
    print("=" * 70)

    if not ric_map:
        print("  No RICs discovered to test.")
        return

    results = {}
    for isin, rics in ric_map.items():
        if isinstance(rics, str):
            rics = [rics]
        currency = next((s[1] for s in SAMPLE_ISINS if s[0] == isin), "?")

        for ric in rics[:3]:  # test up to 3 RICs per ISIN
            resp = requests.get(
                f"{HIST_PRICING_URL}/{ric}",
                headers=headers(token),
                params={"interval": "P1D", "count": 5},
            )

            if resp.status_code != 200:
                print(f"  {ric} (ISIN={isin}, {currency}) -> HTTP {resp.status_code}")
                continue

            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                rows = data[0].get("data", [])
                if rows:
                    results[ric] = len(rows)
                    print(f"  {ric} (ISIN={isin}, {currency}) -> {len(rows)} rows ✓")
                    break  # found a working RIC for this ISIN
                else:
                    print(f"  {ric} (ISIN={isin}, {currency}) -> 0 rows")

    print(f"\n  RICs with pricing: {len(results)}")
    return results


def main():
    print("Authenticating...")
    token = authenticate()
    print("Authenticated.\n")

    # Run all strategies
    primary_rics = test_find_primary_ric(token)
    auto_rics = test_auto_isin_to_ric(token)
    fi_quotes_rics = test_fixed_income_quotes_search(token)
    isin_eq_results = test_isin_equals_pricing(token)
    sedol_eq_results = test_sedol_equals_pricing(token)
    ipa_results = test_ipa_api(token)

    # Merge all discovered RICs and test pricing
    all_rics = {}
    for isin, ric in primary_rics.items():
        all_rics[isin] = [ric]
    for isin, rics in auto_rics.items():
        if isin not in all_rics:
            all_rics[isin] = rics[:5]
    for isin, rics in fi_quotes_rics.items():
        if isin not in all_rics:
            all_rics[isin] = rics[:5]

    test_pricing_with_discovered_rics(token, all_rics)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  FindPrimaryRIC:        {len(primary_rics)}/{len(SAMPLE_ISINS)} resolved")
    print(f"  Auto ISIN->RIC:        {len(auto_rics)}/{len(SAMPLE_ISINS)} resolved")
    print(f"  FixedIncomeQuotes:     {len(fi_quotes_rics)}/{len(SAMPLE_ISINS)} found RICs")
    print(f"  ISIN= pricing:         {len(isin_eq_results)}/{len(SAMPLE_ISINS)} got data")
    print(f"  SEDOL= pricing:        {len(sedol_eq_results)}/{len(SEDOL_SAMPLES)} got data")
    print(f"  IPA analytics:         {len(ipa_results)}/{min(4, len(SAMPLE_ISINS))} got data")


if __name__ == "__main__":
    main()
