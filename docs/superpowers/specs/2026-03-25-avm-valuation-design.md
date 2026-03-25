# AVM Valuation Module Design

## Problem

The pipeline identifies distressed properties via lis pendens filings and delinquent tax records, but has no way to estimate market value. County assessed values exist but reflect tax assessment (33.33% of FMV in Illinois), lag 1-2 years, and don't account for market conditions. Investors need a ballpark market value (within 20-30%) to screen deals and gauge equity/discount.

## Scope

- **In scope:** As-is market value estimation for properties identified by `parcel_id`
- **Out of scope:** ARV (After Repair Value), rental yield analysis, condition assessment

## Approach: Assessed Value Multiplier + Zillow/Redfin Hybrid

Two independent valuation signals blended into a single estimate:

1. **Assessed Value Multiplier** — leverages existing `assessed_value` from DevNetWedge enrichment
2. **Zillow/Redfin Scrape** — fetches public Zestimate and Redfin Estimate for a second opinion

### Component 1: Assessed Value Multiplier

Illinois law requires properties be assessed at 33.33% of fair market value. The Illinois Department of Revenue publishes annual equalization factors per county to correct assessment levels.

**Calculation:**

```
estimated_market_value = assessed_value * equalization_factor * 3
```

- `assessed_value`: Already stored from DevNetWedge (Board of Review Equalized value)
- `equalization_factor`: St. Clair County's annual factor from IL Dept. of Revenue (typically near 1.0). Stored as a config constant, updated annually.
- `3`: Inverse of the 33.33% statutory assessment ratio

**Coverage:** 100% of enriched properties (any record with `assessed_value` populated).

**Accuracy:** ~20-30% for individual properties. Systematically lags market by 1-2 years due to assessment cycle.

### Component 2: Zillow/Redfin Scrape

For each property with a `property_address`, fetch publicly displayed value estimates.

**Scraping strategy:**

- **Redfin (primary):** Cleaner data, less aggressive bot detection. Fetch property page, extract estimate from page content.
- **Zillow (secondary):** Zestimate embedded in page JSON-LD or meta tags. More aggressive bot detection.
- Use `httpx` with browser-like headers
- Rate limiting: 1-2 requests per second
- Rotating user-agent strings

**Fallback chain:**

1. Try Redfin first
2. Fall back to Zillow if Redfin has no data
3. Fall back to assessed multiplier if neither has data

**Coverage:** Varies. Good in metro-adjacent areas (Belleville, O'Fallon, Shiloh). Gaps in rural St. Clair County. Expected 60-80% coverage for lis pendens properties.

### Component 3: Blending Logic

**Priority waterfall:**

1. Both Zillow and Redfin available → average them
2. Only one external estimate → use it directly
3. No external estimates → fall back to assessed multiplier

**Sanity check:** If external estimate diverges more than 50% from the assessed multiplier value, set `valuation_confidence = "low"`. This catches stale/incorrect external data and wildly off assessments.

**Confidence levels:**

- `high`: External estimate(s) available AND within 50% of assessed multiplier
- `medium`: Only assessed multiplier available (no external corroboration)
- `low`: External estimate diverges >50% from assessed multiplier

## Data Model Changes

New columns added to both `properties` and `delinquent_taxes` tables:

| Field | Type | Description |
|---|---|---|
| `assessed_multiplier_value` | REAL | assessed_value * eq_factor * 3 |
| `zillow_estimate` | REAL | Zestimate scraped from Zillow |
| `redfin_estimate` | REAL | Estimate scraped from Redfin |
| `estimated_market_value` | REAL | Final blended value |
| `valuation_source` | TEXT | "assessed_multiplier", "zillow", "redfin", "blended" |
| `valuation_confidence` | TEXT | "high", "medium", "low" |
| `valued_at` | TEXT (ISO timestamp) | When valuation was computed |

Schema migration: `ALTER TABLE` statements in `database.py`, following the existing pattern for adding enrichment columns.

## Module Structure

New file: `src/enrichment/valuation.py`

```
src/enrichment/valuation.py
    compute_assessed_multiplier(assessed_value, eq_factor) -> float
    fetch_redfin_estimate(address) -> float | None
    fetch_zillow_estimate(address) -> float | None
    blend_estimates(assessed_mult, zillow, redfin) -> (value, source, confidence)
    enrich_valuations_from_db()        # enriches properties table
    enrich_delinquent_valuations_from_db()  # enriches delinquent_taxes table
```

Follows the same pattern as `src/enrichment/assessor.py`:
- Queries DB for records where `valued_at IS NULL` and `assessed_value IS NOT NULL`
- Enriches each record with valuation data
- Writes results back via `UPDATE` statements
- Rate limits external requests (1-2 req/sec)
- Tracks errors to avoid re-querying failed properties

## Configuration

```python
# St. Clair County, IL — update annually from IL Dept. of Revenue
STCLAIR_EQUALIZATION_FACTOR = 1.0  # check https://tax.illinois.gov
ASSESSMENT_RATIO = 1 / 3  # Illinois statutory 33.33%
SCRAPE_DELAY = 1.0  # seconds between external requests
MAX_RETRIES = 2
DIVERGENCE_THRESHOLD = 0.5  # 50% divergence triggers low confidence
```

## Integration Points

- **Depends on:** `assessor.py` enrichment (needs `assessed_value` and `property_address` populated)
- **Consumed by:** `prepare_data.py` (adds valuation fields to `data.json` for dashboard)
- **Dashboard:** `Table.js` and `ScoreBadge.js` can display `estimated_market_value` and `valuation_confidence`
- **Scoring:** `estimated_market_value` can feed into the scoring algorithm (e.g., equity spread = market value - total liens)

## Pipeline Order

```
ava_search.py (ingest) → assessor.py (enrich) → valuation.py (value) → prepare_data.py (export)
```

## Error Handling

- If `assessed_value` is NULL, skip the record (cannot compute multiplier)
- If external scraping fails (HTTP error, blocked, no data), log warning and fall back
- Store `enrichment_error` style field if all valuation methods fail for a record
- Never overwrite a good valuation with a failed one

## Testing

- Unit tests for `compute_assessed_multiplier()` with known values
- Unit tests for `blend_estimates()` covering all waterfall branches and confidence levels
- Integration tests for scraping functions (mocked HTTP responses)
- End-to-end test: run against a known property and verify output columns populated

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Zillow/Redfin block scraping | No external estimates | Assessed multiplier as reliable fallback; rotate user agents |
| Assessed values lag market | Stale valuations | Clearly label `valuation_confidence`; external estimates correct for recency |
| Equalization factor changes | Systematic bias | Check IL DOR annually; store factor as config, not hardcoded |
| Coverage gaps in rural areas | Missing valuations for some properties | Assessed multiplier provides 100% coverage baseline |
| Zillow ToS prohibits scraping | Legal gray area | Low volume (<100 req/day); for personal investment use only |
