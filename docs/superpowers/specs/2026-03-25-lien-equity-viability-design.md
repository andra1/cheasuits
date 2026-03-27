# Lien Enrichment, Equity Spread & Deal Viability Score Design

**Date**: 2026-03-25
**Scope**: St. Clair County, IL — PRD Feature 2B implementation
**Status**: Approved for implementation

---

## Problem

The pipeline can estimate market value (via `valuation.py`) and mortgage debt (via `mortgage.py`), but has no comprehensive lien picture. Equity spread — the core input to deal viability — requires knowing **all** encumbrances on a property, not just mortgages. Beyond that, no scoring engine exists to synthesize equity, comps confidence, neighborhood health, and other factors into a single deal viability score.

## Scope

**In scope:**
- Lien enrichment module: scrape federal tax liens, state tax liens, and judgment liens from Fidlar
- Aggregate total lien burden: mortgages + recorded liens + delinquent property taxes
- Equity spread calculation: `estimated_market_value - total_lien_burden`
- Deal viability score (0-100) per PRD section 2B
- DB schema additions for lien data and viability scores

**Out of scope:**
- Distress signal score (2A) — separate feature
- Composite ranking and exit strategy (2C) — depends on 2A + 2B
- Mechanic's liens, HOA liens, utility liens (lower priority, can add later)
- Dashboard UI changes

---

## Part 1: Lien Enrichment Module

### Data Sources

All from the Fidlar AVA Search API (same system as `mortgage.py`), searched by parcel ID:

| Lien Type | Fidlar Doc Code | Release Doc Code | Amount Field |
|-----------|----------------|------------------|--------------|
| Federal Tax Lien | `86` | `141` (RELEASE OF FEDERAL TAX LIEN) | ConsiderationAmount |
| Federal Tax Lien (Estate) | `85` | `141` | ConsiderationAmount |
| Illinois Tax Lien | `92` | `155` (RELEASE OF ILLINOIS LIEN) | ConsiderationAmount |
| State Lien | `248` | `155` | ConsiderationAmount |
| Judgment | `94` | `153` (RELEASE OF JUDGEMENT) | ConsiderationAmount |
| Memorandum of Judgment | `106` | `153` | ConsiderationAmount |

### Matching Logic

Follows the same pattern as `mortgage.py`:

1. Fetch all lien documents for the parcel
2. Fetch all corresponding release documents
3. Match releases to liens by:
   - First: `AssociatedDocuments` reference on the release
   - Second: Party name match + release date after lien date
4. Unmatched liens are presumed **active**
5. Extract amount from `ConsiderationAmount` or `Notes` field

**Key difference from mortgages:** Liens are NOT satisfied by property sales (unlike mortgages which are paid at closing). Federal tax liens and judgments survive transfers. So the "sale date release" heuristic from `mortgage.py` does NOT apply here.

### Module: `src/enrichment/liens.py`

```
LienRecord dataclass:
    document_number: str
    lien_type: str          # "federal_tax", "state_tax", "judgment"
    recorded_date: str
    creditor: str           # Party2 (the lien holder)
    debtor: str             # Party1 (the property owner)
    amount: float | None
    parcel_id: str
    is_released: bool

Functions:
    fetch_lien_history(parcel_id) -> list[LienRecord]
    get_active_liens(parcel_id) -> list[LienRecord]
    get_total_lien_amount(parcel_id) -> float | None
    enrich_liens_from_db(db_path) -> int
```

Reuses `mortgage.py` helpers: `_get_token()`, `_search_documents()`, `_parse_date()`, `_parse_amount()`. These should be extracted to a shared `src/enrichment/_fidlar.py` utility module to avoid duplication.

### Rate Limiting

Each parcel requires up to 8 API calls (4 lien types + 4 release types). With `REQUEST_DELAY = 0.3s`, that's ~2.4s per parcel. For ~200 properties, that's ~8 minutes. Acceptable for a batch enrichment run.

---

## Part 2: Total Lien Burden & Equity Spread

### Aggregation

Total lien burden for a property combines three sources:

```
total_lien_burden = total_mortgage_debt    (from mortgage.py, already in DB)
                  + total_recorded_liens   (from liens.py, new)
                  + delinquent_tax_amount  (from delinquent_taxes table join)
```

**Delinquent tax amount**: The `delinquent_taxes` table has `total_tax` (annual tax amount) and `publication_year`. For properties that appear in the delinquent tax list, we use `total_tax` as a proxy for the minimum amount owed. This underestimates (they may owe multiple years), but it's the best free data available.

The join between `properties` and `delinquent_taxes` uses `REPLACE(p.parcel_id, '-', '') = dt.parcel_id` (existing pattern from `get_delinquent_overlap()`).

### Equity Spread Calculation

```python
equity_spread = estimated_market_value - total_lien_burden
equity_ratio  = equity_spread / estimated_market_value  # 0.0 to 1.0 (can be negative)
```

Both values stored on the property row for scoring.

---

## Part 3: Deal Viability Score (0-100)

Per PRD section 2B, weighted composite of six factors:

### Factor 1: Equity Spread (30 points)

| Equity Ratio | Score |
|-------------|-------|
| >= 60% | 30 |
| 40-60% | 24 |
| 20-40% | 18 |
| 0-20% | 10 |
| Negative (underwater) | 0 |
| No valuation data | 0 |

### Factor 2: Comp Confidence (20 points)

Uses existing `comps_confidence` field from `comps.py`:

| Confidence | Score |
|-----------|-------|
| "high" (3+ comps) | 20 |
| "medium" (1-2 comps) | 12 |
| "low" | 5 |
| No comps data | 0 |

### Factor 3: Neighborhood Vacancy Rate (15 points)

Uses `vacancy_rate_residential` from `usps_vacancy` joined via `census_tract`:

| Vacancy Rate | Score |
|-------------|-------|
| < 5% | 15 |
| 5-8% | 12 |
| 8-12% | 8 |
| 12-15% | 4 |
| > 15% | 0 |
| No data | 7 (neutral) |

### Factor 4: Assessed Value Range / Buy Box (15 points)

Configurable buy box (default: $30K-$150K assessed value):

| Assessed Value | Score |
|---------------|-------|
| Within buy box | 15 |
| Within 20% of box edges | 8 |
| Outside range | 0 |
| No data | 0 |

### Factor 5: Time Pressure (10 points)

Days since the earliest distress signal (lis pendens filing date, tax delinquency year):

| Days Since Signal | Score |
|------------------|-------|
| > 365 | 10 (long stale = max pressure) |
| 180-365 | 8 |
| 90-180 | 6 |
| 30-90 | 4 |
| < 30 | 2 (too fresh, may resolve) |
| No date | 0 |

### Factor 6: Owner Reachability (10 points)

| Condition | Score |
|----------|-------|
| Has mailing address + is local (IL) | 10 |
| Has mailing address + out-of-state | 6 |
| Has mailing address (state unknown) | 5 |
| No mailing address | 0 |

### Output

Stored per property in a `deal_viability` set of columns:

| Column | Type | Description |
|--------|------|-------------|
| `total_lien_burden` | REAL | Sum of all liens, mortgages, and delinquent taxes |
| `equity_spread` | REAL | market_value - total_lien_burden |
| `equity_ratio` | REAL | equity_spread / market_value |
| `viability_score` | INTEGER | 0-100 composite |
| `viability_details` | TEXT | JSON breakdown of each factor's score |
| `viability_scored_at` | TEXT | ISO timestamp |

---

## Part 4: Shared Fidlar Utility Extraction

Extract common Fidlar API helpers from `mortgage.py` into `src/enrichment/_fidlar.py`:

- `_get_token() -> str`
- `_search_documents(parcel_id, doc_type, doc_type_name) -> list[dict]`
- `_parse_date(raw) -> str`
- `_parse_amount(doc) -> float | None`
- `_normalize_party_name(name) -> str` (renamed from `_normalize_lender`)
- `REQUEST_DELAY`, `MAX_RETRIES`, API constants

`mortgage.py` imports from `_fidlar.py` instead of defining these locally. `liens.py` does the same.

---

## DB Schema Changes

### New columns on `properties` table (via migration):

```sql
ALTER TABLE properties ADD COLUMN federal_tax_lien_amount REAL;
ALTER TABLE properties ADD COLUMN state_tax_lien_amount REAL;
ALTER TABLE properties ADD COLUMN judgment_lien_amount REAL;
ALTER TABLE properties ADD COLUMN total_recorded_liens REAL;
ALTER TABLE properties ADD COLUMN lien_count INTEGER;
ALTER TABLE properties ADD COLUMN lien_enriched_at TEXT;
ALTER TABLE properties ADD COLUMN lien_error TEXT;
ALTER TABLE properties ADD COLUMN total_lien_burden REAL;
ALTER TABLE properties ADD COLUMN equity_spread REAL;
ALTER TABLE properties ADD COLUMN equity_ratio REAL;
ALTER TABLE properties ADD COLUMN viability_score INTEGER;
ALTER TABLE properties ADD COLUMN viability_details TEXT;
ALTER TABLE properties ADD COLUMN viability_scored_at TEXT;
```

---

## Pipeline Order

```
assessor.py → census_tract.py → valuation.py → comps.py → mortgage.py → liens.py → viability scoring
```

Viability scoring runs after ALL enrichment is complete, since it reads from multiple enrichment columns.

---

## Implementation Plan

1. Extract shared Fidlar utilities to `_fidlar.py`
2. Refactor `mortgage.py` to import from `_fidlar.py`
3. Build `liens.py` following `mortgage.py` pattern
4. Add DB migrations for lien + viability columns
5. Build `src/scoring/viability.py` with equity calculation + 6-factor scoring
6. Add DB helpers for scoring reads/writes
7. Test: unit tests for scoring math, integration test for lien fetching
