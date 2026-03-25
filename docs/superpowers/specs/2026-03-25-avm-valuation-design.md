# AVM Valuation Module Design

## Problem

The pipeline identifies distressed properties via lis pendens filings and delinquent tax records, but has no way to estimate market value. County assessed values exist but reflect tax assessment (33.33% of FMV in Illinois), lag 1-2 years, and don't account for market conditions. Investors need a ballpark market value (within 20-30%) to screen deals and gauge equity/discount.

## Scope

- **In scope:** As-is market value estimation for properties identified by `parcel_id`
- **Out of scope:** ARV (After Repair Value), rental yield analysis, condition assessment, dashboard UI changes, re-valuation/staleness policies (V1 limitation — properties are valued once; a `--force` flag or age-based refresh can be added later)

## Approach: Assessed Value Multiplier + Zillow/Redfin Hybrid

Two independent valuation signals blended into a single estimate:

1. **Assessed Value Multiplier** — leverages existing `assessed_value` from DevNetWedge enrichment
2. **Zillow/Redfin Scrape** — fetches public Zestimate and Redfin Estimate for a second opinion

### Component 1: Assessed Value Multiplier

Illinois law requires properties be assessed at 33.33% of fair market value. The assessment pipeline works in stages:

1. **Township Assessor** sets an initial assessed value
2. **County Board of Review** equalizes within the county (this is the `assessed_value` stored from DevNetWedge — "Board of Review Equalized")
3. **Illinois Department of Revenue** publishes an annual **state equalization multiplier** per county to bring aggregate assessment levels to the statutory 33.33%

The state multiplier corrects for systematic under/over-assessment at the county level. It is published at https://tax.illinois.gov (search "equalization factor" or "assessment/sales ratio studies").

**Calculation:**

```
estimated_market_value = assessed_value * STATE_MULTIPLIER / ASSESSMENT_RATIO
```

Where:
- `assessed_value`: Board of Review Equalized value from DevNetWedge (already county-equalized)
- `STATE_MULTIPLIER`: Illinois DOR state equalization multiplier for St. Clair County (e.g., 1.0049 for tax year 2024). This is NOT a county-level factor — the county equalization is already embedded in `assessed_value`.
- `ASSESSMENT_RATIO`: 1/3 (0.3333) — the Illinois statutory ratio

Example: assessed_value=$44,000, state_multiplier=1.0049 → $44,000 × 1.0049 / 0.3333 = ~$132,650

**Coverage:** 100% of enriched properties (any record with `assessed_value` populated).

**Accuracy:** ~20-30% for individual properties. Systematically lags market by 1-2 years due to assessment cycle.

### Component 2: Zillow/Redfin Scrape

For each property with a `property_address`, fetch publicly displayed value estimates.

**Address-to-URL resolution:**

The `property_address` from DevNetWedge is multi-line (e.g., `"904 CALISTA RIDGE DR\nSHILOH, IL 62221"`). Before querying external sites:

1. Flatten to single line: replace `\n` with `, `
2. Normalize to title case for URL formatting

**Redfin (primary):**
1. Hit the autocomplete endpoint: `GET https://www.redfin.com/stingray/do/location-autocomplete?v=2&al=1&location={url_encoded_address}`
2. Parse the response to extract the property URL path (JSON-like response with `url` field)
3. Fetch the property page at `https://www.redfin.com{url_path}`
4. Extract the Redfin Estimate from the page HTML (look for `data-rf-test-id="avmLdpPrice"` or similar attribute, or parse the `__NEXT_DATA__` JSON blob)

**Zillow (secondary):**
1. Hit the search: `GET https://www.zillow.com/homes/{formatted_address}_rb/`
2. If the page resolves to a property detail page, extract the Zestimate from the JSON-LD `<script type="application/ld-json">` block or from the `__NEXT_DATA__` script tag (look for `zestimate` field)
3. If the search returns a list page (ambiguous match), skip — do not guess

Both scrapers use `urllib.request` (consistent with existing codebase) with:
- Browser-like User-Agent headers (rotate from a small pool)
- Rate limiting: 1-2 requests per second (`SCRAPE_DELAY` config)
- Max 2 retries on transient failures

**Fallback chain:**

1. Try Redfin first
2. Fall back to Zillow if Redfin has no data
3. Fall back to assessed multiplier if neither has data

**Coverage:** Varies. Good in metro-adjacent areas (Belleville, O'Fallon, Shiloh). Gaps in rural St. Clair County. Expected 60-80% coverage for lis pendens properties.

### Component 3: Blending Logic

**Complete decision matrix:**

| Zillow | Redfin | Divergence from assessed mult | `estimated_market_value` | `valuation_source` | `valuation_confidence` |
|--------|--------|-------------------------------|--------------------------|---------------------|------------------------|
| Yes | Yes | Both within 50% | avg(zillow, redfin) | "blended" | "high" |
| Yes | Yes | One or both exceed 50% | avg(zillow, redfin) | "blended" | "low" |
| Yes | No | Within 50% | zillow | "zillow" | "high" |
| Yes | No | Exceeds 50% | zillow | "zillow" | "low" |
| No | Yes | Within 50% | redfin | "redfin" | "high" |
| No | Yes | Exceeds 50% | redfin | "redfin" | "low" |
| No | No | N/A | assessed_mult | "assessed_multiplier" | "medium" |

**Key decisions:**
- External estimates are always preferred over the assessed multiplier (they incorporate market conditions)
- When external estimates diverge significantly from assessed, we still use the external value but flag confidence as "low" so the user knows to investigate
- "medium" confidence means only the assessed multiplier was available — usable for screening but should be verified before making offers
- Divergence is calculated as: `abs(external - assessed_mult) / assessed_mult > DIVERGENCE_THRESHOLD`

## Data Model Changes

New columns added to the `properties` table only:

| Field | Type | Description |
|---|---|---|
| `assessed_multiplier_value` | REAL | assessed_value * state_multiplier / assessment_ratio |
| `zillow_estimate` | REAL | Zestimate scraped from Zillow |
| `redfin_estimate` | REAL | Estimate scraped from Redfin |
| `estimated_market_value` | REAL | Final blended value |
| `valuation_source` | TEXT | "assessed_multiplier", "zillow", "redfin", "blended" |
| `valuation_confidence` | TEXT | "high", "medium", "low" |
| `valued_at` | TEXT (ISO timestamp) | When valuation was computed |
| `valuation_error` | TEXT | Error message if all valuation methods fail; prevents re-querying |

**Why `properties` only:** The `properties` table is the canonical enrichment target for the lis pendens pipeline and the sole source for `prepare_data.py` export. Delinquent tax records that overlap can access valuations via the existing `get_delinquent_overlap()` join. If standalone delinquent-tax valuations are needed later, the columns can be added then.

Schema migration: `ALTER TABLE` statements in `database.py`, following the existing pattern for adding enrichment columns.

## Module Structure

New file: `src/enrichment/valuation.py`

```
src/enrichment/valuation.py
    compute_assessed_multiplier(assessed_value, state_multiplier) -> float
    fetch_redfin_estimate(address) -> float | None
    fetch_zillow_estimate(address) -> float | None
    blend_estimates(assessed_mult, zillow, redfin) -> (value, source, confidence)
    enrich_valuations_from_db()   # enriches properties table
```

Follows the same pattern as `src/enrichment/assessor.py`:
- Queries DB for records where `valued_at IS NULL AND valuation_error IS NULL AND assessed_value IS NOT NULL`
- Enriches each record with valuation data
- Writes results back via `UPDATE` statements
- Rate limits external requests (1-2 req/sec)
- On complete failure (no method produced a value), writes error to `valuation_error` to prevent infinite retries

## Configuration

```python
# St. Clair County, IL
# State equalization multiplier — published annually by IL Dept. of Revenue
# https://tax.illinois.gov (search "equalization factor")
# The Board of Review Equalized value from DevNetWedge already includes
# county-level equalization. This multiplier is the STATE-level correction.
STCLAIR_STATE_MULTIPLIER = 1.0049  # Tax year 2024 — verify annually
ASSESSMENT_RATIO = 1 / 3  # Illinois statutory 33.33%

SCRAPE_DELAY = 1.0  # seconds between external requests
MAX_RETRIES = 2
DIVERGENCE_THRESHOLD = 0.5  # 50% divergence triggers low confidence
```

## Integration Points

- **Depends on:** `assessor.py` enrichment (needs `assessed_value` and `property_address` populated)
- **Consumed by:** `prepare_data.py` — must add these fields to `read_db()` SELECT and `build_output()` feature dict:
  - `estimated_market_value`
  - `valuation_source`
  - `valuation_confidence`
  - `valued_at`
- **Scoring:** `estimated_market_value` can feed into the scoring algorithm (e.g., equity spread = market value - total liens)
- **Dashboard:** Out of scope for this spec. Valuation data will be available in `data.json` for future UI work.

## Pipeline Order

```
ava_search.py (ingest) → assessor.py (enrich) → valuation.py (value) → prepare_data.py (export)
```

## Error Handling

- If `assessed_value` is NULL, skip the record (cannot compute multiplier)
- If `valuation_error` is already set, skip the record (already failed, avoid infinite retries)
- If external scraping fails (HTTP error, blocked, no data), log warning and fall back to next source
- If ALL methods fail for a record, write descriptive error to `valuation_error`
- Never overwrite a good valuation with a failed one

## Testing

- Unit tests for `compute_assessed_multiplier()` with known values
- Unit tests for `blend_estimates()` covering all 7 rows of the decision matrix
- Integration tests for scraping functions (mocked HTTP responses)
- End-to-end test: run against a known property and verify output columns populated

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Zillow/Redfin block scraping | No external estimates | Assessed multiplier as reliable fallback; rotate user agents |
| Assessed values lag market | Stale valuations | Clearly label `valuation_confidence`; external estimates correct for recency |
| State multiplier changes | Systematic bias | Check IL DOR annually; store as named config, not magic number |
| Coverage gaps in rural areas | Missing valuations for some properties | Assessed multiplier provides 100% coverage baseline |
| Zillow ToS prohibits scraping | Legal gray area | Low volume (<100 req/day); for personal investment use only |
| Scraping endpoints change | Broken fetchers | Modular design — each fetcher is independent and can be updated without affecting others |
| V1 has no re-valuation | Values go stale over time | Known limitation; `--force` refresh or age-based policy planned for V2 |
