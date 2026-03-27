# Valuation Module Refactor — Design Spec

**Date:** 2026-03-27
**Status:** Approved
**Supersedes:** 2026-03-25-avm-valuation-design.md

## Summary

Refactor the valuation system to eliminate blended estimates and the assessed multiplier method. Separate valuations into their own table with full audit trail. Separate comps into a dedicated join table with per-comp detail. Prioritize Zillow/Redfin as primary valuation sources with comps as fallback. Add a new Comparable Sales card to the property detail page with expandable comp rows and source links.

## Goals

1. Remove the assessed multiplier valuation method entirely
2. Remove blended estimate logic — each source stands alone
3. Store all valuations in a dedicated `valuations` table (one row per source per property)
4. Store Zillow/Redfin listing URLs when available
5. Store individual comp matches in a `property_comps` join table
6. Simple priority rule for `estimated_market_value`: Zillow/Redfin first, comps median fallback
7. New Comparable Sales card on the property detail page with expandable detail rows and source links
8. Switch Zillow/Redfin scrapers from `urllib.request` to `curl_cffi` to avoid 403 bot detection

## Database Schema Changes

### New Table: `valuations`

```sql
CREATE TABLE IF NOT EXISTS valuations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_number TEXT NOT NULL REFERENCES properties(document_number),
    source TEXT NOT NULL,          -- "zillow" | "redfin" | "comps"
    estimate REAL NOT NULL,        -- dollar amount
    source_url TEXT,               -- Zillow/Redfin property page URL (null for comps)
    confidence TEXT,               -- "high" | "medium" | "low"
    comp_count INTEGER,            -- only populated for source="comps"
    valued_at TEXT NOT NULL,       -- ISO timestamp
    UNIQUE(document_number, source)
);
CREATE INDEX idx_valuations_doc ON valuations(document_number);
```

One row per (property, source). Upserted on re-runs — always reflects the latest estimate from each source.

### New Table: `property_comps`

```sql
CREATE TABLE IF NOT EXISTS property_comps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_number TEXT NOT NULL REFERENCES properties(document_number),
    comp_sale_id INTEGER NOT NULL REFERENCES comparable_sales(id),
    distance_miles REAL,           -- haversine distance from subject
    similarity_score REAL,         -- 0-1 composite (distance + recency + lot size)
    lot_size_ratio REAL,           -- subject_lot / comp_lot, clamped 0.5-2.0
    adjusted_price REAL,           -- sale_price * lot_size_ratio
    matched_at TEXT,               -- ISO timestamp
    UNIQUE(document_number, comp_sale_id)
);
CREATE INDEX idx_property_comps_doc ON property_comps(document_number);
```

Links a subject property to specific `comparable_sales` rows with scoring metadata.

### Modified Table: `comparable_sales`

Add an autoincrement primary key so `property_comps` can FK to individual rows:

```sql
-- Current: composite unique on (address, sale_date, sale_price)
-- New: add id INTEGER PRIMARY KEY AUTOINCREMENT
-- Keep existing unique constraint as dedup index
```

### Columns Removed from `properties`

The following columns move to the `valuations` and `property_comps` tables:

- `assessed_multiplier_value`
- `zillow_estimate`
- `redfin_estimate`
- `comps_estimate`
- `comps_count`
- `comps_confidence`
- `comps_updated_at`
- `valuation_source`
- `valuation_confidence`
- `valuation_error`

### Columns Kept on `properties`

- `estimated_market_value` — denormalized, set by priority rule
- `valued_at` — timestamp of winning estimate
- `assessed_value` — raw county assessment, still useful for reference

## Valuation Pipeline (`valuation.py`)

### Deleted

- `compute_assessed_multiplier()` function and all associated constants (`STCLAIR_STATE_MULTIPLIER`, `ASSESSMENT_RATIO`)
- `blend_estimates()` function and `DIVERGENCE_THRESHOLD`
- The `--revalue` flag — replaced by upsert behavior on re-runs

### Refactored Flow

For each property with an address:

1. **Redfin:** autocomplete → resolve property URL → fetch page → extract estimate. Uses `curl_cffi` with browser impersonation. Returns `(estimate, property_page_url)` or `(None, None)`.
2. **Zillow:** construct URL from address → fetch page → extract Zestimate. Uses `curl_cffi` with browser impersonation. Returns `(estimate, property_page_url)` or `(None, None)`.
3. **Write to `valuations`:** one row per successful source with `source`, `estimate`, `source_url`, `confidence="high"`, `valued_at`.

### Priority Rule (denormalization)

A separate function runs after both valuation and comps pipelines complete:

```
For each property:
  1. Query valuations table for all rows with this document_number
  2. If Zillow AND Redfin exist → average of the two estimates
  3. If only one of Zillow/Redfin exists → use that estimate
  4. If comps row exists (and no Zillow/Redfin) → use comps estimate
  5. Otherwise → estimated_market_value stays NULL
  6. Write winning value + valued_at to properties table
```

This function is idempotent and can be called after any pipeline step.

## Comps Pipeline (`comps.py`)

### Data Collection (unchanged)

`comps_redfin.py` and `comps_recorder.py` continue to populate `comparable_sales` as before.

### Comp Matching (refactored output)

Same scoring logic — distance (40%), recency (30%), lot size similarity (30%). Changes:

1. For each subject property, find and score comps as before
2. **Delete existing `property_comps` rows** for this property (within a transaction)
3. **Insert new `property_comps` rows** — one per matched comp with: `distance_miles`, `similarity_score`, `lot_size_ratio`, `adjusted_price`, `matched_at`
4. **Insert/upsert `valuations` row** with `source="comps"`, `estimate` = similarity-score-weighted average of adjusted prices (same weighting as current `estimate_from_comps`), `comp_count` = number of comps, `confidence` from count (3+ = "high", 2 = "medium", 1 = "low")

No longer writes directly to `properties` columns.

## Frontend Changes

### Financial Overview Card (revised)

- Hero metric: `estimated_market_value` with source name and external link (e.g., "Source: Redfin · View on Redfin ↗")
- County assessed value shown alongside for reference
- "All Valuations" table below: lists every `valuations` row for this property
  - Columns: Source, Estimate, Confidence (badge), Date, Link (external)
  - Active/winning source highlighted

### New Card: Comparable Sales

- **Summary bar:** Comps Estimate, Confidence badge, Comp Count
- **Comp list:** ranked rows, each showing: rank, address, sale price, distance, sale date
- **Expandable detail:** click a row to reveal:
  - Grid: sale price, adjusted price, distance, similarity score, lot size, lot ratio, beds/baths, sqft, year built
  - Links: "View on Redfin ↗" (if source is redfin), "County Recorder ↗" (if source is recorder)
- Comps sorted by similarity score descending (best match first)

### Equity & Liens Card

Unchanged — reads `estimated_market_value` from `properties`.

### Viability Score Card

Unchanged structure. The `comp_confidence` factor now reads from the `valuations` table row where `source="comps"` instead of the old `properties.comps_confidence` column.

## Data Export (`prepare_data.py`)

The JSON export adds two nested arrays per property:

```json
{
  "document_number": "2224286",
  "estimated_market_value": 373878,
  "valued_at": "2026-03-27T10:00:00",
  "assessed_value": 131939,
  "valuations": [
    {
      "source": "redfin",
      "estimate": 378500,
      "source_url": "https://www.redfin.com/IL/Shiloh/904-Calista-Ridge-Dr-62221/home/...",
      "confidence": "high",
      "valued_at": "2026-03-27T10:00:00"
    },
    {
      "source": "zillow",
      "estimate": 369256,
      "source_url": "https://www.zillow.com/homes/904-Calista-Ridge-Dr,-Shiloh,-IL-62221_rb/",
      "confidence": "high",
      "valued_at": "2026-03-27T10:00:00"
    },
    {
      "source": "comps",
      "estimate": 350000,
      "confidence": "medium",
      "comp_count": 4,
      "valued_at": "2026-03-27T10:30:00"
    }
  ],
  "comps": [
    {
      "address": "123 Oak St, Belleville, IL",
      "sale_price": 185000,
      "sale_date": "2026-01-15",
      "distance_miles": 0.3,
      "similarity_score": 0.91,
      "lot_size_ratio": 1.05,
      "adjusted_price": 194250,
      "sqft": 1600,
      "beds": 3,
      "baths": 2,
      "lot_size": 0.22,
      "year_built": 1992,
      "source": "redfin",
      "source_id": "MLS12345"
    }
  ]
}
```

Removed fields from top level: `valuation_source`, `valuation_confidence`, `comps_estimate`, `comps_count`, `comps_confidence`.

## Scoring Impact (`viability.py`)

Minimal change:
- `score_comp_confidence()` reads confidence from `valuations` table where `source="comps"` and `document_number` matches, instead of `properties.comps_confidence`
- All other scoring functions unchanged
- Equity calculation still reads `properties.estimated_market_value`

## Dependencies

- `curl_cffi` — new dependency, replaces `urllib.request` for Zillow/Redfin scraping to bypass TLS fingerprint-based bot detection
- All other dependencies unchanged
