"""Property Details Enrichment — Zillow-sourced physical characteristics.

Fetches beds, baths, sqft, property type, and year built from Zillow property
pages and writes them to the properties table. Only fills gaps — if the assessor
already set sqft or year_built, Zillow doesn't overwrite.

Usage:
    python -m src.enrichment.property_details [--db data/cheasuits.db] [-v]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

SCRAPE_DELAY = 1.5  # seconds between requests (be polite to Zillow)
MAX_RETRIES = 2

# Zillow homeType values mapped to our canonical types
HOMETYPE_MAP = {
    "SINGLE_FAMILY": "SFR",
    "SingleFamily": "SFR",
    "MULTI_FAMILY": "multi",
    "MultiFamily": "multi",
    "CONDO": "condo",
    "Condominium": "condo",
    "TOWNHOUSE": "townhouse",
    "Townhouse": "townhouse",
    "MANUFACTURED": "manufactured",
    "MOBILE": "manufactured",
    "LOT": "land",
    "VACANT_LAND": "land",
    "VacantLand": "land",
    "APARTMENT": "multi",
    "Apartment": "multi",
    "HOME_TYPE_UNKNOWN": "unknown",
}


def _normalize_address(raw: str) -> str:
    """Flatten multi-line DB address to single line for URL queries."""
    return raw.replace("\n", ", ").strip()


def _address_to_zillow_slug(address: str) -> str:
    """Convert address to Zillow URL slug.

    '209 Edwards St, Cahokia, IL 62206' -> '209-Edwards-St,-Cahokia,-IL-62206'
    """
    slug = re.sub(r'[^\w,\s-]', '', address)
    slug = re.sub(r'[\s]+', '-', slug.strip())
    return slug


def _parse_property_from_gdp_cache(gdp_cache: dict) -> Optional[dict]:
    """Extract property details from gdpClientCache values.

    gdpClientCache contains query results keyed by query name.
    Each value has a .property object with the fields we need.
    """
    for cache_val in gdp_cache.values():
        if not isinstance(cache_val, dict):
            # gdpClientCache values may be JSON strings themselves
            if isinstance(cache_val, str):
                try:
                    cache_val = json.loads(cache_val)
                except (json.JSONDecodeError, ValueError):
                    continue

        prop = cache_val.get("property") if isinstance(cache_val, dict) else None
        if not prop or not isinstance(prop, dict):
            continue

        result = {}

        # Beds
        beds = prop.get("bedrooms")
        if isinstance(beds, (int, float)) and beds > 0:
            result["beds"] = int(beds)

        # Baths
        baths = prop.get("bathrooms")
        if isinstance(baths, (int, float)) and baths > 0:
            result["baths"] = float(baths)

        # Square footage
        sqft = prop.get("livingArea")
        if isinstance(sqft, (int, float)) and sqft > 0:
            result["sqft"] = float(sqft)

        # Property type
        home_type = prop.get("homeType")
        if home_type:
            result["property_type"] = HOMETYPE_MAP.get(home_type, home_type)

        # Year built
        year = prop.get("yearBuilt")
        if isinstance(year, (int, float)) and year > 1600:
            result["year_built"] = int(year)

        if result:
            return result

    return None


def _parse_property_from_html_regex(html: str) -> Optional[dict]:
    """Fallback: extract property details via regex patterns in page HTML."""
    result = {}

    beds_match = re.search(r'"bedrooms"\s*:\s*(\d+)', html)
    if beds_match:
        result["beds"] = int(beds_match.group(1))

    baths_match = re.search(r'"bathrooms"\s*:\s*([\d.]+)', html)
    if baths_match:
        result["baths"] = float(baths_match.group(1))

    sqft_match = re.search(r'"livingArea"\s*:\s*(\d+)', html)
    if sqft_match:
        result["sqft"] = float(sqft_match.group(1))

    type_match = re.search(r'"homeType"\s*:\s*"([^"]+)"', html)
    if type_match:
        raw_type = type_match.group(1)
        result["property_type"] = HOMETYPE_MAP.get(raw_type, raw_type)

    year_match = re.search(r'"yearBuilt"\s*:\s*(\d{4})', html)
    if year_match:
        result["year_built"] = int(year_match.group(1))

    return result if result else None


def fetch_zillow_property_details(address: str) -> Optional[dict]:
    """Fetch property details from Zillow for a given address.

    Uses curl_cffi with browser impersonation to bypass bot detection.
    Returns dict with available fields: beds, baths, sqft, property_type, year_built.
    Returns None if the page can't be fetched or no data found.
    """
    normalized = _normalize_address(address)
    slug = _address_to_zillow_slug(normalized)
    url = f"https://www.zillow.com/homes/{slug}_rb/"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = curl_requests.get(
                url,
                impersonate="chrome",
                timeout=15,
            )
            if resp.status_code != 200:
                raise Exception(f"HTTP {resp.status_code}")
            html = resp.text

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.debug(f"Attempt {attempt} failed for {normalized}: {e}")
                time.sleep(1)
                continue
            logger.warning(f"Zillow fetch failed for {normalized}: {e}")
            return None

        # Try __NEXT_DATA__ JSON blob first
        try:
            match = re.search(
                r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                html, re.DOTALL,
            )
            if match:
                page_data = json.loads(match.group(1))
                gdp_cache_raw = (
                    page_data.get("props", {})
                    .get("pageProps", {})
                    .get("componentProps", {})
                    .get("gdpClientCache", {})
                )

                # gdpClientCache may be a JSON string that needs double-parsing
                if isinstance(gdp_cache_raw, str):
                    try:
                        gdp_cache = json.loads(gdp_cache_raw)
                    except (json.JSONDecodeError, ValueError):
                        gdp_cache = {}
                else:
                    gdp_cache = gdp_cache_raw

                if gdp_cache:
                    result = _parse_property_from_gdp_cache(gdp_cache)
                    if result:
                        logger.info(
                            f"Zillow details for {normalized}: "
                            f"{result.get('beds', '?')}bd/{result.get('baths', '?')}ba "
                            f"{result.get('sqft', '?')}sqft {result.get('property_type', '?')}"
                        )
                        return result

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"Zillow JSON parse failed for {normalized}: {e}")

        # Regex fallback
        result = _parse_property_from_html_regex(html)
        if result:
            logger.info(
                f"Zillow details (regex) for {normalized}: "
                f"{result.get('beds', '?')}bd/{result.get('baths', '?')}ba "
                f"{result.get('sqft', '?')}sqft"
            )
            return result

        logger.debug(f"Zillow: no property details found for {normalized}")
        return None

    return None


# ---------------------------------------------------------------------------
# Enrichment Orchestrator
# ---------------------------------------------------------------------------

def enrich_property_details(db_path: Path) -> None:
    """Fetch property details from Zillow for all un-detailed properties."""
    from src.db.database import (
        get_db, get_undetailed_properties, update_property_details,
        set_property_details_error,
    )

    conn = get_db(db_path)
    rows = get_undetailed_properties(conn)

    if not rows:
        print("No properties needing details enrichment.")
        conn.close()
        return

    print(f"Enriching property details for {len(rows)} properties from Zillow...")

    enriched = 0
    failed = 0

    for i, row in enumerate(rows):
        doc_num = row["document_number"]
        address = row.get("property_address", "")

        if not address:
            set_property_details_error(conn, doc_num, "no address")
            failed += 1
            continue

        if i > 0:
            time.sleep(SCRAPE_DELAY)

        details = fetch_zillow_property_details(address)

        if not details:
            set_property_details_error(conn, doc_num, "zillow: no data found")
            failed += 1
            logger.warning(f"[{i+1}/{len(rows)}] {doc_num} -> no details found")
            continue

        # Gap-fill: preserve existing assessor values for sqft and year_built
        fields = {}
        for field in ("beds", "baths", "property_type"):
            if field in details:
                fields[field] = details[field]

        for field in ("sqft", "year_built"):
            existing = row.get(field)
            if existing is not None and existing > 0:
                # Assessor already provided this, keep it
                pass
            elif field in details:
                fields[field] = details[field]

        # Determine source
        has_assessor_data = any(
            row.get(f) is not None and row.get(f) > 0
            for f in ("sqft", "year_built")
        )
        has_zillow_data = bool(fields)

        if has_assessor_data and has_zillow_data:
            fields["property_details_source"] = "assessor+zillow"
        elif has_assessor_data:
            fields["property_details_source"] = "assessor"
        elif has_zillow_data:
            fields["property_details_source"] = "zillow"
        else:
            set_property_details_error(conn, doc_num, "no detail fields available")
            failed += 1
            continue

        update_property_details(conn, doc_num, fields)
        enriched += 1
        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} -> "
            f"{fields.get('beds', '?')}bd/{fields.get('baths', '?')}ba "
            f"{fields.get('sqft', '?')}sqft ({fields.get('property_details_source')})"
        )

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(rows)} ({enriched} enriched, {failed} failed)")

    conn.close()
    print(f"\nEnriched {enriched}/{len(rows)} properties ({failed} failed)")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich properties with physical details (beds, baths, sqft) from Zillow"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    enrich_property_details(Path(args.db))


if __name__ == "__main__":
    main()
