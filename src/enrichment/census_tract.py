"""
Census Tract Enrichment — maps geocoded properties to census tract FIPS codes.

Uses the Census Bureau's geocoder API to look up the census tract for each
property's lat/lng coordinates, enabling joins against the usps_vacancy table.

API: https://geocoding.geo.census.gov/geocoder/geographies/coordinates
No API key required. Free. Rate-limited.

Usage:
    # Enrich properties table
    python -m src.enrichment.census_tract --db data/cheasuits.db --table properties

    # Enrich delinquent_taxes table
    python -m src.enrichment.census_tract --db data/cheasuits.db --table delinquent
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
REQUEST_DELAY = 0.3  # seconds between API calls
MAX_RETRIES = 3
DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"


def parse_geocoder_response(data: dict) -> Optional[str]:
    """Extract the 11-digit GEOID from a Census Bureau geocoder response.

    Returns the GEOID string or None if the response doesn't contain tract info.
    """
    try:
        tracts = data["result"]["geographies"].get("Census Tracts", [])
        if tracts:
            return tracts[0].get("GEOID")
    except (KeyError, TypeError, IndexError):
        pass
    return None


def fetch_census_tract(lat: float, lng: float) -> Optional[str]:
    """Look up the census tract GEOID for a coordinate pair.

    Args:
        lat: Latitude.
        lng: Longitude.

    Returns:
        11-digit FIPS GEOID string, or None on failure.
    """
    params = urllib.parse.urlencode({
        "x": lng,
        "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    })
    url = f"{GEOCODER_URL}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "CheasuitsBot/1.0"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            geoid = parse_geocoder_response(data)
            if geoid:
                logger.debug(f"({lat}, {lng}) -> tract {geoid}")
            else:
                logger.warning(f"No tract found for ({lat}, {lng})")
            return geoid

        except urllib.error.HTTPError as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"HTTP {e.code} on attempt {attempt}/{MAX_RETRIES}. Retrying...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"HTTP {e.code} for ({lat}, {lng}) after {MAX_RETRIES} attempts")
                return None
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Failed for ({lat}, {lng}) after {MAX_RETRIES} attempts: {e}")
                return None

    return None


def enrich_properties(db_path: Path) -> None:
    """Fetch census tract GEOID for all geocoded but un-tracted properties."""
    from src.db.database import get_db, get_untracted_properties, update_property_tract

    conn = get_db(db_path)
    rows = get_untracted_properties(conn)

    if not rows:
        print("No properties need tract enrichment.")
        conn.close()
        return

    print(f"Enriching {len(rows)} properties with census tract...")

    enriched = 0
    failed = 0
    cache: dict[tuple[float, float], Optional[str]] = {}

    for i, row in enumerate(rows):
        lat, lng = row["lat"], row["lng"]
        key = (round(lat, 6), round(lng, 6))

        if key in cache:
            geoid = cache[key]
            if geoid:
                update_property_tract(conn, row["document_number"], geoid)
                enriched += 1
            else:
                failed += 1
            continue

        if i > 0:
            time.sleep(REQUEST_DELAY)

        geoid = fetch_census_tract(lat, lng)
        cache[key] = geoid

        if geoid:
            update_property_tract(conn, row["document_number"], geoid)
            enriched += 1
            logger.info(f"[{i+1}/{len(rows)}] ({lat}, {lng}) -> {geoid}")
        else:
            failed += 1
            logger.warning(f"[{i+1}/{len(rows)}] ({lat}, {lng}) -> FAILED")

    conn.close()
    print(f"\nEnriched {enriched}/{len(rows)} properties with census tract ({failed} failed)")


def enrich_delinquent(db_path: Path) -> None:
    """Fetch census tract GEOID for all geocoded but un-tracted delinquent tax rows."""
    from src.db.database import get_db, get_untracted_delinquent, update_delinquent_tract

    conn = get_db(db_path)
    rows = get_untracted_delinquent(conn)

    if not rows:
        print("No delinquent tax records need tract enrichment.")
        conn.close()
        return

    print(f"Enriching {len(rows)} delinquent tax records with census tract...")

    enriched = 0
    failed = 0
    cache: dict[tuple[float, float], Optional[str]] = {}

    for i, row in enumerate(rows):
        lat, lng = row["lat"], row["lng"]
        key = (round(lat, 6), round(lng, 6))

        if key in cache:
            geoid = cache[key]
            if geoid:
                update_delinquent_tract(conn, row["id"], geoid)
                enriched += 1
            else:
                failed += 1
            continue

        if i > 0:
            time.sleep(REQUEST_DELAY)

        geoid = fetch_census_tract(lat, lng)
        cache[key] = geoid

        if geoid:
            update_delinquent_tract(conn, row["id"], geoid)
            enriched += 1
            if (i + 1) % 100 == 0:
                logger.info(f"[{i+1}/{len(rows)}] Progress: {enriched} enriched, {failed} failed")
        else:
            failed += 1

    conn.close()
    print(f"\nEnriched {enriched}/{len(rows)} delinquent records with census tract ({failed} failed)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich geocoded properties with census tract FIPS codes"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--table", choices=["properties", "delinquent"], default="properties",
        help="Which table to enrich (default: properties)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.table == "delinquent":
        enrich_delinquent(Path(args.db))
    else:
        enrich_properties(Path(args.db))


if __name__ == "__main__":
    main()
