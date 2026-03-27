# Property Details Enrichment — Design Spec

**Date:** 2026-03-26
**Status:** Implemented

## Problem

The `properties` table lacks physical characteristics (sqft, beds, baths, property type, year built) needed for accurate valuation. The planned Zillow-only valuation methodology requires these fields to find similar properties as a fallback when a direct Zillow lookup fails.

## Approach

Zillow-only enrichment. The DevNetWedge assessor site was investigated and confirmed to NOT have dwelling/building characteristics — that data only exists in scanned Property Record Card PDFs which would require OCR.

**Single source: Zillow property details** — new standalone module that fetches Zillow property pages and extracts: beds, baths, sqft, property_type, year_built from the `__NEXT_DATA__` JSON blob (specifically `gdpClientCache -> property`).

## New Database Columns

Added to `properties` table:

| Column | Type | Source |
|--------|------|--------|
| `sqft` | REAL | Zillow |
| `beds` | INTEGER | Zillow |
| `baths` | REAL | Zillow |
| `property_type` | TEXT | Zillow (SFR, multi, condo, townhouse, land, manufactured) |
| `year_built` | INTEGER | Zillow |
| `stories` | INTEGER | Reserved (no current source) |
| `property_details_source` | TEXT | "zillow" (or "assessor+zillow" if assessor data exists) |
| `property_details_at` | TEXT | ISO timestamp |
| `property_details_error` | TEXT | Error for skip-on-rerun |

## Property Details Module

New file: `src/enrichment/property_details.py`

- Queries properties with `property_details_at IS NULL AND property_details_error IS NULL`
- Fetches Zillow property page using address-based URL slug
- Parses `__NEXT_DATA__` JSON: `props.pageProps.componentProps.gdpClientCache` (may need double-parse as it can be a JSON string)
- Extracts: `property.bedrooms`, `property.bathrooms`, `property.livingArea`, `property.homeType`, `property.yearBuilt`
- Regex fallback for pages where JSON parsing fails
- homeType values mapped to canonical types (SINGLE_FAMILY -> SFR, MULTI_FAMILY -> multi, etc.)
- Gap-fill logic: if assessor already set sqft/year_built, those are preserved
- CLI: `python -m src.enrichment.property_details --db data/cheasuits.db -v`
- Rate limiting (1.5s delay), retry logic, error recording

## Database Helpers

New functions in `src/db/database.py`:

- `get_undetailed_properties()` — properties needing details enrichment
- `update_property_details()` — write property detail fields
- `set_property_details_error()` — record failures for skip-on-rerun

## Pipeline Order

1. Run assessor scraper (existing — provides tax/owner data, no dwelling details)
2. Run property details module (Zillow — beds, baths, sqft, type, year built)
3. Run valuation module (future: Zillow-only methodology uses these fields for fallback)
