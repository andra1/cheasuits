# Lead Source Strategy: Lis Pendens / Notice of Foreclosure
## Target Market: Belleville, IL (St. Clair County)

---

## Overview

Illinois is a **judicial foreclosure state**. There is no out-of-court Notice of Default process like you'd find in California or Texas. Instead, a lender must file a formal lawsuit in the **Circuit Court** (Chancery Division) to foreclose. As part of that process, the lender (or their attorney) records a **Notice of Foreclosure (Lis Pendens)** with the **County Recorder of Deeds** to put the public on notice that a foreclosure action is pending against the property.

This creates two parallel filing trails to monitor:
1. **Circuit Court** — the actual foreclosure lawsuit (chancery case)
2. **Recorder of Deeds** — the lis pendens document recorded against the property title

Both are public record and both are accessible for St. Clair County.

**Actionable window:** From lis pendens recording to auction is typically **210–360+ days** in Illinois due to mandatory reinstatement and redemption periods. This is longer than most states — more time to reach owners.

---

## Illinois Foreclosure Timeline (Statutory)

```
Lender files complaint in Circuit Court (Chancery Division)
        ↓
Lis Pendens recorded with County Recorder of Deeds  ←── Monitor this
        ↓
Homeowner served / summons issued (30 days to respond)
        ↓
90-day Reinstatement Period (owner can cure default by paying arrears)
        ↓
Foreclosure Judgment entered by court
        ↓
90-day Redemption Period (owner can redeem by paying full balance)
        ↓
Sheriff's Sale (public auction)
        ↓
Confirmation of Sale (court order)
```

**Best intervention window:** Weeks 1–8 after lis pendens recording. Owner is in distress but still has time to act — highest motivation, most flexibility on price.

---

## Data Available in a Lis Pendens Filing

Per Illinois statute 735 ILCS 5/15-1503, a Notice of Foreclosure must include:

| Field | Description |
|---|---|
| Plaintiff name | Lender or loan servicer filing suit |
| Case number | Circuit Court case reference |
| Court name | St. Clair County Circuit Court |
| Defendant name(s) | Borrower(s) — the owner(s) you want to reach |
| Property address | Street address of subject property |
| Legal description | Lot/block or metes and bounds |
| APN / Parcel number | Assessor parcel number — key for enrichment |
| Mortgage reference | Book/page of original mortgage recorded |
| Recording date | When lis pendens was filed — your pipeline timestamp |

The Circuit Court complaint (accessible separately) also contains:
- Loan origination date and original loan amount
- Outstanding balance at time of filing
- Number of missed payments / amount in default
- Attorney contact for lender

---

## Data Sources for St. Clair County

### Source 1: AVA Search — Recorder of Deeds (Free)
**URL:** https://ilstclair.fidlar.com/ILStClair/AvaWeb/

The Recorder of Deeds partners with **Fidlar Technologies** and provides a free index-only search called AVA Search. No login required.

**What it provides:**
- Party name (grantor/grantee)
- Recorded date range
- Parcel number
- Document number
- Document type (filter for: `LIS PENDENS`, `NOTICE OF FORECLOSURE`)

**What it does NOT provide:** Document images (requires paid Tapestry or Laredo account)

**Automation approach:**
- Daily HTTP polling of AVA Search filtered by document type + date range (last 1–2 days)
- Parse results for new lis pendens entries
- No API — requires scraping the web interface
- Technology: `playwright` or `httpx` + `BeautifulSoup`
- Parcel number format required: `00-00-0-000-000`

**Cost:** Free for index data. $8.75/search via Tapestry EON for full document images if needed.

---

### Source 2: Circuit Court Web Inquiry (Free)
**URL:** https://webinquiry.stclaircountycourts.org

St. Clair County's Circuit Court has an online case search portal (MiCOURT system). Foreclosure cases appear in the **Chancery Division**.

**What it provides:**
- Case number and filing date
- Plaintiff (lender) and defendant (borrower) names
- Case status (active, judgment entered, dismissed)
- Hearing dates and docket entries
- Attorney of record for both sides

**Automation approach:**
- Search by case type (Chancery / Foreclosure) and date range
- Cross-reference case numbers found in AVA Search lis pendens records
- Scrape docket entries to track case status changes (judgment entered = redemption clock starts)
- Technology: `playwright` for JS-rendered court portals

**Cost:** Free

---

### Source 3: St. Clair County Assessor — Property Tax Inquiry (Free)
**URL:** https://stclairil.devnetwedge.com/

Cross-reference parcel numbers from lis pendens filings against the assessor/tax system.

**What it provides:**
- Owner mailing address (may differ from property address — key for absentee owner flag)
- Assessed value (EAV) and fair market value estimate
- Tax payment status — whether taxes are also delinquent (stacking signal)
- Property class, lot size, structure details
- Tax year history

**Automation approach:**
- Look up each new parcel number from lis pendens filings
- Flag properties where taxes are also delinquent (multi-distress signal)
- Extract owner mailing address for skip tracing

**Cost:** Free

---

### Source 4: Fidlar Tapestry EON (Paid, Optional)
**URL:** https://tapestry.fidlar.com/TapestryEON/

Paid access to full document images from the Recorder of Deeds. Needed only if you want to read the actual lis pendens document (e.g., to pull the mortgage reference or confirm legal description).

**Cost:** $8.75/search, pay-as-you-go

**When to use:** Only pull full images on high-scoring leads (e.g., after filtering by estimated equity, property value range, etc.)

---

### Source 5: ATTOM Data API (Paid, Scale Option)
**URL:** https://api.developer.attomdata.com/docs

ATTOM aggregates recorder data nationally, including Illinois foreclosure/lis pendens data across all counties. Covers St. Clair County.

**What it provides:**
- Lis pendens / pre-foreclosure records via `/property/foreclosuredetail` endpoint
- Normalized across all counties in a single API call
- Enriched with AVM (estimated value), equity estimate, deed history, tax status

**When to use:** Once you expand beyond St. Clair County. For a single-county pilot, direct scraping is cheaper and produces faster/fresher data.

**Cost:** Subscription-based (contact for quote — typically $300–600+/month for investor-tier access)

---

## Proposed Automation Architecture (Belleville Pilot)

```
┌─────────────────────────────────────┐
│         Scheduler (daily, 6 AM)     │
└────────────────┬────────────────────┘
                 │
    ┌────────────▼────────────┐
    │   AVA Search Scraper    │  ← New lis pendens by date
    │   (Fidlar / Recorder)   │
    └────────────┬────────────┘
                 │  parcel + defendant name + recording date
    ┌────────────▼────────────┐
    │  Assessor Enrichment    │  ← Owner address, AV, tax status
    │  (DevNetWedge lookup)   │
    └────────────┬────────────┘
                 │
    ┌────────────▼────────────┐
    │  Circuit Court Lookup   │  ← Case status, lender, attorney
    │  (MiCOURT scraper)      │
    └────────────┬────────────┘
                 │
    ┌────────────▼────────────┐
    │   Scoring / Filtering   │  ← Value range, equity, tax delinquency
    └────────────┬────────────┘
                 │
    ┌────────────▼────────────┐
    │   Skip Trace (batch)    │  ← Phone + email for owner
    │   BatchSkipTracing API  │
    └────────────┬────────────┘
                 │
    ┌────────────▼────────────┐
    │   Output / Alert        │  ← CSV, CRM push, or Slack alert
    └─────────────────────────┘
```

---

## Filtering Criteria (Belleville Pilot)

Target properties that meet all of the following:

- **Estimated value:** $100,000 – $500,000 (use assessor EAV × equalization factor ~2.5–3x for market estimate)
- **Property type:** Residential (single family, small multifamily)
- **Recording date:** Within last 60 days (freshest leads, before competitors find them)
- **Tax status:** Flag if delinquent (higher motivation stack)
- **Owner mailing address ≠ property address** → absentee owner flag

Exclude:
- Properties with existing sheriff's sale date scheduled (too late in timeline)
- Commercial / industrial parcels
- Vacant land

---

## Data Fields to Capture per Lead

```python
{
    "source": "lis_pendens_stclair",
    "recording_date": "2026-03-20",
    "document_number": "2026-XXXXX",
    "parcel_id": "00-00-0-000-000",
    "property_address": "123 Main St, Belleville IL 62220",
    "legal_description": "...",
    "defendant_name": "John Doe",          # borrower
    "plaintiff_name": "Wells Fargo Bank",  # lender
    "court_case_number": "26-CH-000XXX",
    "case_status": "active",
    "owner_mailing_address": "456 Oak Ave, St Louis MO 63101",
    "absentee_owner": True,
    "assessed_value": 85000,
    "estimated_market_value": 220000,      # AV × equalization factor
    "tax_delinquent": False,
    "tax_amount_owed": 0,
    "skip_trace_phone": None,              # populated after skip trace
    "skip_trace_email": None,
    "distress_score": None,                # populated by scoring engine
    "notes": ""
}
```

---

## Open Questions / To Resolve

- **AVA Search scraping feasibility** — needs hands-on testing to confirm the HTML structure is stable enough for automated polling. Fidlar portals can vary by county.
- **Equalization factor for St. Clair County** — Illinois uses a county equalization factor (multiplier) to get from assessed value to fair market value. Need to pull current St. Clair County multiplier from IDOR (Illinois Department of Revenue).
- **MiCOURT scraping** — circuit court portal is JavaScript-rendered (MiCOURT system). Need to test if Playwright handles it cleanly or if login is required.
- **Skip trace volume** — even at a small county scale, batching skip traces weekly is cheaper than per-record. Evaluate BatchSkipTracing vs. PropStream's built-in skip trace for cost.
- **Alert format** — for a solo investor, a daily email digest or Slack message is sufficient before building any dashboard.
- **Laredo Anywhere subscription** — if full document images are needed regularly, the $75/month plan (250 min) is more economical than Tapestry's per-search fee at any volume above ~9 searches/month.

---

## Immediate Next Steps

1. **Manually test AVA Search** — search for document type `LIS PENDENS` over the last 30 days to understand result volume (ballpark: how many filings per month in St. Clair County?)
2. **Manually test MiCOURT** — pull a sample foreclosure case to confirm what fields are accessible without login
3. **Pull St. Clair County equalization factor** from IDOR website
4. **Prototype AVA Search scraper** — daily poll, parse new lis pendens, write to CSV/DB
5. **Add assessor enrichment** on each new parcel — flag delinquent taxes and absentee owners
6. **Evaluate skip trace pricing** — run 20–30 test records through BatchSkipTracing

---

*Sources*
- [Illinois Foreclosure Statute — 735 ILCS 5/15-1503](https://www.ilga.gov/legislation/ilcs/ilcs4.asp?ActID=2017&ChapterID=56&SeqStart=107100000&SeqEnd=115800000)
- [St. Clair County Recorder of Deeds — Remote Access](https://www.co.st-clair.il.us/departments/recorder-of-deeds/remote-access)
- [St. Clair County AVA Search (Free Index)](https://ilstclair.fidlar.com/ILStClair/AvaWeb/)
- [St. Clair County Circuit Court Web Inquiry](https://webinquiry.stclaircountycourts.org)
- [St. Clair County Property Tax Inquiry](https://stclairil.devnetwedge.com/)
- [ATTOM Foreclosure Data API](https://www.attomdata.com/data/foreclosure-data/)
- [Illinois Foreclosure Timeline — Chicago Fed](https://www.chicagofed.org/~/media/publications/profitwise-news-and-views/2011/foreclosure-process-timeline-pdf.pdf)
