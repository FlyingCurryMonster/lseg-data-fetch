# Eurex Single Stock Futures & Single Stock Dividend Futures

Research on Eurex-listed single stock products accessible via LSEG Data API.
Confirmed 2026-04-12 via Eurex product list CSV.

Source: [Eurex Product Search](https://www.eurex.com/ex-en/markets/productSearch)
— CSV download: `productlist.csv` (saved locally as `eurex_productlist.csv` in repo root)

---

## Overview

| Product | Type Code | US Names | Total (all countries) |
|---------|-----------|----------|-----------------------|
| Single Stock Futures | `FSTK` | 186 product IDs (~93 unique names) | ~858 |
| Single Stock Dividend Futures | `FSTK` (same type) | 57 | ~345 |

Most US SSF names have **two product IDs** (e.g., `AAPH` + `TAAP` for Apple) — standard
vs total return or different settlement variants. Dividend futures use a single product ID
per name with an `X2YY` naming pattern (e.g., `M2ST` = Microsoft).

---

## US Single Stock Dividend Futures (57 names)

| Eurex ID | Name | Reuters Chain RIC |
|----------|------|-------------------|
| M2MM | 3M Dividend Futures | `0#M2MM:` |
| T2FF | AT&T Dividend Futures | |
| A2BV | AbbVie Dividend Futures | |
| M2OF | Altria Group Dividend Futures | `0#M2OF:` |
| A2MZ | Amazon.com Dividend Futures | `0#A2MZ:` |
| A3AP | Apple Dividend Futures | `0#A2AP:` |
| A2PF | Automatic Data Processing Dividend Futures | |
| B2AC | Bank of America Dividend Futures | `0#B2AC:` |
| B2BY | Best Buy Dividend Futures | |
| B2X | Blackstone Dividend Futures | |
| B2CO | Boeing Dividend Futures | |
| P2CL | Booking Holdings Dividend Futures | |
| B2MY | Bristol-Myers Squibb Dividend Futures | `0#B2MY:` |
| A2VG | Broadcom Dividend Futures | |
| C2VX | Chevron Dividend Futures | `0#C2VX:` |
| C2SC | Cisco Systems Dividend Futures | `0#C2SC:` |
| C2IT | Citigroup Dividend Futures | `0#C2IT:` |
| C2LP | Colgate-Palmolive Dividend Futures | |
| D2OW | Dow Inc. Dividend Futures | |
| X2MF | ExxonMobil Dividend Futures | |
| F2DX | FedEx Dividend Futures | |
| G2EC | GE Aerospace Dividend Futures | `0#G2EC:` |
| G2SF | General Mills Dividend Futures | |
| G2IL | Gilead Sciences Dividend Futures | |
| H2AL | Halliburton Dividend Futures | |
| H2PE | Hewlett Packard Dividend Futures | |
| I2BM | IBM Dividend Futures | `0#I2BM:` |
| I2NT | Intel Dividend Futures | `0#I2NT:` |
| J2PM | JPMorgan Chase Dividend Futures | `0#J2PM:` |
| J2NJ | Johnson & Johnson Dividend Futures | `0#J2NJ:` |
| L2VS | Las Vegas Sands Dividend Futures | |
| M2CD | McDonald's Dividend Futures | `0#M2CD:` |
| M2CC | Merck & Co. Dividend Futures | |
| M2ET | MetLife Dividend Futures | |
| F2BU | Meta Platforms Dividend Futures | |
| M2ST | Microsoft Dividend Futures | `0#M2ST:` |
| N2KE | NIKE Dividend Futures | |
| N2VA | NVIDIA Dividend Futures | |
| N2EM | Newmont Dividend Futures | |
| F2PL | NextEra Energy Dividend Futures | |
| P2YX | Paychex Dividend Futures | |
| P2EP | PepsiCo Dividend Futures | `0#P2EP:` |
| P2FE | Pfizer Dividend Futures | `0#P2FE:` |
| P2M | Philip Morris International Dividend Futures | `0#P2MM:` |
| P2GF | Procter & Gamble Dividend Futures | |
| P2AS | Public Storage Dividend Futures | |
| R2CL | Royal Caribbean Cruises Dividend Futures | |
| F2OO | Salesforce Dividend Futures | |
| T2RO | T. Rowe Price Dividend Futures | |
| K2OF | The Coca-Cola Company Dividend Futures | `0#K2OF:` |
| H2DF | The Home Depot Dividend Futures | |
| U2NP | Union Pacific Dividend Futures | |
| U2PS | United Parcel Service Dividend Futures | |
| U2NH | UnitedHealth Group Dividend Futures | |
| V2ZF | Verizon Dividend Futures | |
| W2MT | Walmart Dividend Futures | `0#W2MT:` |
| D2IS | Walt Disney Dividend Futures | |

---

## US Single Stock Futures (93 unique names)

| Eurex ID(s) | Name | Reuters Chain RIC |
|-------------|------|-------------------|
| MMMF, TMMM | 3M | `0#MMMF:` |
| TFFF, TTFF | AT&T | `0#TFFF:` |
| ABVF, TABV | AbbVie | `0#ABVF:` |
| ABTF, TABT | Abbott Laboratories | `0#ABTF:` |
| ALKF | Alaska Air Group | |
| BABF | Alibaba | `0#BABF:` |
| GOAF | Alphabet Class A | `0#GOAF:` |
| GOCF, TGOC | Alphabet Class C | `0#GOCF:` |
| MOFF, TMOF | Altria Group | `0#MOFF:` |
| AMZG, TAMZ | Amazon.com | `0#TAMZ:` |
| ALFF | American Airlines | |
| AXPF, TAXP | American Express | `0#AXPF:` |
| AMGF, TAMG | Amgen | `0#AMGF:` |
| ITAF | Anheuser-Busch InBev ADR | `0#ITAF:` |
| AAPH, TAAP | Apple | `0#AAPF2:` |
| ADPF, TADP | Automatic Data Processing | `0#ADPF:` |
| BACF, TBAC | Bank of America | `0#BACF:` |
| BAXG | Baxter International | `0#BAXF2:` |
| BDXF | Becton, Dickinson & Co. | `0#BDXF:` |
| BRKF | Berkshire Hathaway | |
| BBYF | Best Buy | |
| BXFF, TBXF | Blackstone | |
| BCOF, TBCO | Boeing | `0#BCOF:` |
| PCLF, TPCL | Booking Holdings | `0#PCLN:` |
| BMYF, TBMY | Bristol-Myers Squibb | `0#BMYF:` |
| AVGF, TAVG | Broadcom | |
| CH1F | C.H. Robinson Worldwide | `0#CH1F:` |
| CVSF, TCVS | CVS Health Corp. | `0#CVSF:` |
| CLHF | Cardinal Health | `0#CLHFF:` |
| CATF, TCAT | Caterpillar | `0#CATF:` |
| CVXF, TCVX | Chevron | `0#CVXF:` |
| CSCF, TCSC | Cisco Systems | `0#CSCF:` |
| CITG, TCIT | Citigroup | `0#CITF2:` |
| CEEG | Coca-Cola Europacific Partners | `0#CEEF2:` |
| CLFF, TCLF | Colgate-Palmolive | `0#CLFF:` |
| CMCF, TCMC | Comcast | `0#CMCF:` |
| CAOF | ConAgra Brands | `0#CAOF:` |
| COSF, TCOS | Costco Wholesale | `0#COSF:` |
| DRIF | Darden Restaurants | `0#DRIF:` |
| DEFF, TDEF | Deere & Co. | `0#DEFF:` |
| DALF | Delta Air Lines | |
| DFFF | Dominion Energy | `0#DFFF:` |
| DOWF | Dow Inc. | |
| DUKF, TDUK | Duke Energy | `0#DUKF:` |
| WLPF, TWLP | Elevance Health | `0#WLPF:` |
| LLYF, TLLY | Eli Lilly & Co. | `0#LLYF:` |
| EXDF | Expedia | `0#EXPE:` |
| XOMF, TXOM | ExxonMobil | `0#XOMF:` |
| FDXF, TFDX | FedEx | `0#FDXF:` |
| FCXG, TFCX | Freeport-McMoRan Copper & Gold | `0#FCXF2:` |
| GECF | GE Aerospace | `0#GECF:` |
| SYMF | Gen Digital | `0#SYMF:` |
| TGEC | General Electric | `0#TGEC:` |
| GISF | General Mills | `0#GISF:` |
| GILF, TGIL | Gilead Sciences | `0#GILF:` |
| GOSF, TGOS | Goldman Sachs Group | `0#GOSF:` |
| HPQF | HP | `0#HPQF:` |
| HALF | Halliburton | `0#HALF:` |
| HPEF | Hewlett Packard | |
| HONF, THON | Honeywell International | `0#HONF:` |
| IBMF, TIBM | IBM | `0#IBMF:` |
| INTF, TINT | Intel | `0#INTF:` |
| JBHF | J. B. Hunt Transport Services | |
| JPMF, TJPM | JPMorgan Chase | `0#JPMF:` |
| JBLF | JetBlue Airways | |
| JNJF, TJNJ | Johnson & Johnson | `0#JNJF:` |
| KMBF | Kimberly-Clark | `0#KMBF:` |
| LVSF | Las Vegas Sands | `0#LVSF:` |
| LOWF, TLOW | Lowe's | `0#LOWF:` |
| LYFF | Lyft | |
| MTCF, TMTC | Mastercard | |
| MCDF, TMCD | McDonald's | `0#MCDF:` |
| MCCF, TMCC | Merck & Co. | `0#MCCF:` |
| METF | MetLife | |
| FBUF, TFBU | Meta Platforms | `0#FBUF:` |
| MSTF, TMST | Microsoft | `0#MSTF:` |
| NKEF, TNKE | NIKE | `0#NKEF:` |
| NO8F | NOV Inc. | `0#NO8F:` |
| NVDF, TNVD | NVIDIA | |
| NTFF, TNTF | Netflix | `0#NTFF:` |
| NEMF | Newmont | `0#NEMF:` |
| FPLF, TFPL | NextEra Energy | `0#FPLF:` |
| NSCF, TNSC | Norfolk Southern | |
| ORCF, TORC | Oracle | `0#ORCF:` |
| PAYF | Paychex | |
| PEPF, TPEP | PepsiCo | `0#PEPF:` |
| PFEF, TPFE | Pfizer | `0#PFEF:` |
| PMFF, TPMF | Philip Morris International | `0#PMFF:` |
| PGFF, TPGF | Procter & Gamble | `0#PGFF:` |
| PSFF | Public Storage | |
| UTXF, TUTX | RTX Corporation | `0#UTXF:` |
| RCLF | Royal Caribbean Cruises | `0#RCLF:` |
| FOOG, TFOO | Salesforce | `0#FOOF2:` |
| SOFF, TSOC | Southern Company | `0#SOFF:` |
| LUVF | Southwest Airlines | |
| SYYF | Sysco | `0#SYYF:` |
| TROF | T. Rowe Price | |
| TGTF, TTGT | Target | `0#TGTF:` |
| TSLG, TTSL | Tesla | `0#TTSL:` |
| TXNF, TTXN | Texas Instruments | `0#TXNF:` |
| CSPF | The Campbell's Co. | `0#CSPF:` |
| KOFF, TKOF | The Coca-Cola Company | `0#KOFF:` |
| HDFF, THDF | The Home Depot | `0#HDFF:` |
| TRVF, TTRV | The Travelers Companies | `0#TRVF:` |
| UBRF, TUBR | Uber Technologies | |
| UNPF, TUNP | Union Pacific | `0#UNPF:` |
| UALF | United Airlines | |
| UPSF, TUPS | United Parcel Service | `0#UPSF:` |
| UNHF, TUNH | UnitedHealth Group | `0#UNHF:` |
| VISG, TVIS | VISA | `0#VISF2:` |
| VZFF, TVZF | Verizon | `0#VZFF:` |
| WMTF, TWMT | Walmart | `0#WMTF:` |
| DISF, TDIS | Walt Disney | `0#DISF:` |
| EBAF | eBay | `0#EBAF:` |

---

## RIC Format (TODO — confirm via LSEG API)

### Expected contract RIC pattern
Individual contract RICs likely follow standard Eurex futures month codes:
```
{EUREX_ID}{month_code}{yy}

Month codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec

Examples (speculative, needs confirmation):
  M2STZ26  = Microsoft Dividend Future Dec 2026
  M2STZ27  = Microsoft Dividend Future Dec 2027
  MSTFZ26  = Microsoft SSF Dec 2026
```

### Chain RIC pattern
Reuters chain RICs from the Eurex CSV use `0#` prefix:
```
0#M2ST:   = all Microsoft Dividend Future contracts
0#MSTF:   = all Microsoft SSF contracts
```

### Eurex April 2025 RIC rename
Eurex did a mass derivatives RIC rename in April 2025 (noted in NOTES.md symbology
section). Current RICs may differ from pre-2025 references. Confirm current RICs via
`ld.discovery.search()` before building download pipelines.

---

## Data Availability (TODO — needs testing)

Not yet tested against LSEG API. Key questions:

1. **Do the Reuters chain RICs resolve via `ld.discovery.search()`?**
   - Chain objects hang on `platform.rdp` (known issue from NOTES.md)
   - But `discovery.search()` with the Eurex ID or product name should work

2. **Does `ld.get_history()` return data for individual contract RICs?**
   - FEXD (index dividend futures) confirmed working back to ~2015
   - Need to test a US single stock dividend future (e.g., `M2STZ26`)

3. **What fields are available?**
   - Expect: TRDPRC_1, SETTLE, HIGH_1, LOW_1, OPEN_PRC, ACVOL_UNS
   - SETTLE likely most reliable (same as FEXD experience)
   - OPINT unknown

4. **History depth for SSFs vs SSDFs?**

5. **Expired contract data retention?**
   - Critical for back-testing — need to test expired RICs with `^` suffix

---

## TODO

- [ ] Probe sample RICs via `ld.discovery.search()` to confirm current RIC format
- [ ] Test `get_history()` on a few US SSDF and SSF contracts
- [ ] Confirm expired contract RIC format and data retention
- [ ] Enumerate all individual contract RICs per name (active + expired)
- [ ] Build download pipeline (can reuse `download_div_futures.py` pattern)
- [ ] Determine field availability (SETTLE, OPINT, IMP_VOLT)
