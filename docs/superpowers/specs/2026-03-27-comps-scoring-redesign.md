# Comps Scoring Redesign — Design Spec

**Date:** 2026-03-27
**Status:** Approved

## Summary

Redesign the comparable sales scoring to prioritize square footage similarity over distance. Add a hard filter rejecting comps with >30% sqft difference. Replace lot-size-based price adjustment with sqft-based adjustment. Add a Zillow recently sold scraper to expand the comps pool beyond Redfin (which is returning 403s).

## Goals

1. Sqft similarity is the dominant scoring factor (50% weight)
2. Hard filter: reject any comp with >30% sqft difference from the subject
3. Remove lot size from scoring and price adjustment entirely
4. Use sqft-based price adjustment instead (`comp_price * subject_sqft / comp_sqft`)
5. Add Zillow recently sold scraper as a new comp source
6. Distance demoted to 30%, recency to 20%

## Scoring Redesign (`comps.py`)

### Hard Gate

Before scoring, reject comps that fail the sqft filter:

```
if subject has sqft AND comp has sqft:
    sqft_diff = abs(subject_sqft - comp_sqft) / subject_sqft
    if sqft_diff > 0.30:
        reject comp (skip it entirely)
```

If either side is missing sqft data, the comp passes the gate — don't reject based on missing data.

### New Weights

```
WEIGHT_SQFT     = 0.50   # dominant — how close in square footage
WEIGHT_DISTANCE = 0.30   # closer is better, but secondary
WEIGHT_RECENCY  = 0.20   # recent sales preferred, lowest priority
```

### Sqft Similarity Score (0-1)

```
if subject_sqft and comp_sqft and both > 0:
    sqft_score = 1 - (abs(subject_sqft - comp_sqft) / subject_sqft)
else:
    sqft_score = 0.3   # penalty for missing data, not neutral
```

Because of the hard gate, `sqft_score` will always be >= 0.7 when both values are present.

### Distance Score (0-1)

Unchanged: `1 - dist / max_dist`. Zero at max distance, 1 at zero distance.

### Recency Score (0-1)

Unchanged: `1 - days_ago / max_days`. Zero at max lookback, 1 at today.

### Removed

- `WEIGHT_LOT_SIZE` — deleted
- Lot size similarity scoring — deleted
- Lot-size-based price adjustment in `estimate_from_comps` — replaced with sqft-based

### Sqft-Based Price Adjustment

In `estimate_from_comps`, replace the lot size adjustment with:

```
if subject_sqft and comp_sqft and both > 0:
    sqft_ratio = subject_sqft / comp_sqft
    sqft_ratio = clamp(sqft_ratio, 0.7, 1.3)
    adjusted_price = comp_price * sqft_ratio
else:
    adjusted_price = comp_price  # no adjustment if data missing
```

This adjusts the comp's sale price to account for the size difference within the allowed 30% band.

### `property_comps` Table Changes

The `lot_size_ratio` column is repurposed:
- Rename conceptually to reflect sqft ratio (but keep the column name for backwards compat)
- Write `sqft_ratio` into `lot_size_ratio` column
- Write sqft-adjusted price into `adjusted_price` column

## Zillow Recently Sold Scraper (`comps_zillow.py`)

### New File: `src/enrichment/comps_zillow.py`

Follows the same pattern as `comps_redfin.py`.

### Approach

1. Hit Zillow's recently sold search page for St. Clair County, IL
2. Parse the `__NEXT_DATA__` JSON blob from the HTML
3. Extract sold listings with all relevant fields
4. Uses `curl_cffi` with browser impersonation (same as Zestimate scraper)

### URL Pattern

`https://www.zillow.com/st-clair-county-il/sold/` with pagination via query parameters. Each page contains ~40 results.

### Pagination

Navigate through available pages up to a cap of 10 pages (~400 results). Zillow's `__NEXT_DATA__` contains pagination metadata to detect when results are exhausted.

### Output Record Format

Same schema as `comps_redfin.py`, matching the `comparable_sales` table:

```python
{
    "address": str,           # full address
    "lat": float,
    "lng": float,
    "sale_price": float,      # sold price
    "sale_date": str,         # YYYY-MM-DD
    "property_type": str,
    "sqft": float,
    "beds": int,
    "baths": float,
    "lot_size": float,        # acres
    "year_built": int,
    "source": "zillow",
    "source_id": str,         # zpid
    "scraped_at": str,        # ISO timestamp
}
```

### Storage

Uses existing `upsert_comparable_sales()` — keyed on `(address, sale_date, source)`. Zillow records won't conflict with Redfin records due to different `source` value.

### CLI

```
python -m src.enrichment.comps_zillow [--db data/cheasuits.db] [--days 180] [-v]
```

Entry point: `fetch_and_store(db_path, sold_within_days=180)` — same interface as Redfin scraper.

### Rate Limiting

1 second delay between page fetches. Max 3 retries per page.

## Files Changed

| Action | File | Change |
|--------|------|--------|
| Modify | `src/enrichment/comps.py` | New weights, hard sqft filter, sqft-based scoring and price adjustment, remove lot size |
| Create | `src/enrichment/comps_zillow.py` | Zillow recently sold scraper |
| Modify | `tests/test_valuation_refactor.py` | Update comps scoring tests |

## What Stays the Same

- `comps_redfin.py` — unchanged (still a valid comp source)
- `comps_recorder.py` — unchanged
- `comparable_sales` table schema — unchanged
- `property_comps` table schema — unchanged (reuse `lot_size_ratio` for sqft ratio)
- `enrich_comps_from_db` orchestrator — same flow, just calls updated scoring
- Frontend `CompsCard.js` — unchanged (reads same data structure)
- `find_comps` function signature — unchanged
- `estimate_from_comps` function signature — unchanged
