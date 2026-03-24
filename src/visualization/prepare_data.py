"""
Lis Pendens Map Data Preprocessor

Reads a CSV of St. Clair County lis pendens filings (produced by ava_search),
geocodes parcel IDs via the county's public ArcGIS REST API, and outputs a
data.json file for the Leaflet map dashboard.

Usage:
    python -m src.visualization.prepare_data                      # default CSV
    python -m src.visualization.prepare_data --input custom.csv   # custom CSV
    python -m src.visualization.prepare_data -v                   # verbose logging
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import logging
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CSV = PROJECT_ROOT / "lis_pendens_30day.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "dashboard" / "public" / "data.json"

ARCGIS_URL = (
    "https://arcgispublicmap.co.st-clair.il.us/server/rest/services/"
    "SCC_parcel_map_data/MapServer/29/query"
)

REQUEST_DELAY = 0.2  # seconds between geocoding requests
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# CSV Parsing
# ---------------------------------------------------------------------------

def parse_legals(legals_str: str) -> tuple[list[str], list[str]]:
    """Parse the legals field into parcel IDs and subdivision names.

    The legals field contains semicolon-separated Python dict literals.
    LegalType='P' entries have parcel numbers, LegalType='S' have subdivision info.

    Returns:
        (parcel_ids, subdivisions)
    """
    parcel_ids = []
    subdivisions = []

    if not legals_str:
        return parcel_ids, subdivisions

    # Split on '; {' to separate dict entries, keeping the '{' on each chunk
    chunks = re.split(r";\s*(?=\{)", legals_str.strip())

    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            entry = ast.literal_eval(chunk)
        except (ValueError, SyntaxError):
            logger.debug(f"Could not parse legal entry: {chunk[:80]}")
            continue

        legal_type = entry.get("LegalType", "")
        description = entry.get("Description", "")

        if legal_type == "P" and description:
            parcel_ids.append(description.strip())
        elif legal_type == "S" and description:
            subdivisions.append(description.strip())

    return parcel_ids, subdivisions


def read_csv(csv_path: Path) -> list[dict]:
    """Read the lis pendens CSV and extract structured records."""
    records = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parcel_ids, subdivisions = parse_legals(row.get("legals", ""))

            records.append({
                "document_number": row.get("document_number", "").strip(),
                "case_number": row.get("case_number", "").strip(),
                "case_type": row.get("case_type", "").strip(),
                "recorded_date": row.get("recorded_date", "").strip(),
                "party2": row.get("party2", "").strip(),
                "parcel_ids": parcel_ids,
                "subdivision": subdivisions[0] if subdivisions else "",
            })

    logger.info(f"Read {len(records)} records from {csv_path}")
    return records


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def strip_parcel_hyphens(parcel_id: str) -> str:
    """Remove hyphens from parcel ID for the ArcGIS query.

    '01-35-0-402-022' -> '01350402022'
    """
    return parcel_id.replace("-", "")


def compute_centroid(rings: list[list[list[float]]]) -> tuple[float, float]:
    """Compute centroid from ArcGIS polygon rings.

    Returns (lat, lng) by averaging all ring coordinates.
    ArcGIS returns coordinates as [lng, lat] pairs.
    """
    total_x = 0.0
    total_y = 0.0
    count = 0

    for ring in rings:
        for point in ring:
            total_x += point[0]  # longitude
            total_y += point[1]  # latitude
            count += 1

    if count == 0:
        return (0.0, 0.0)

    return (round(total_y / count, 6), round(total_x / count, 6))


def geocode_parcel(parcel_id: str) -> Optional[tuple[float, float]]:
    """Geocode a single parcel ID via the St. Clair County ArcGIS API.

    Returns (lat, lng) or None if geocoding fails.
    """
    stripped = strip_parcel_hyphens(parcel_id)

    params = urllib.parse.urlencode({
        "where": f"parcel_number='{stripped}'",
        "outFields": "parcel_number",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
    })

    url = f"{ARCGIS_URL}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            features = data.get("features", [])
            if not features:
                logger.debug(f"No geometry found for parcel {parcel_id} ({stripped})")
                return None

            geometry = features[0].get("geometry", {})
            rings = geometry.get("rings", [])
            if not rings:
                logger.debug(f"Empty rings for parcel {parcel_id}")
                return None

            lat, lng = compute_centroid(rings)
            logger.debug(f"Geocoded {parcel_id} -> ({lat}, {lng})")
            return (lat, lng)

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"Geocode attempt {attempt}/{MAX_RETRIES} failed for "
                    f"{parcel_id}: {e}. Retrying..."
                )
                time.sleep(1)
            else:
                logger.error(f"Geocoding failed for {parcel_id} after {MAX_RETRIES} attempts: {e}")
                return None

    return None


def geocode_records(records: list[dict]) -> list[dict]:
    """Geocode all records, adding lat/lng fields.

    Uses the first parcel ID from each record. Applies rate limiting
    between requests.
    """
    geocoded_count = 0
    failed_count = 0
    seen_parcels: dict[str, Optional[tuple[float, float]]] = {}

    for i, record in enumerate(records):
        parcel_ids = record.get("parcel_ids", [])
        if not parcel_ids:
            record["lat"] = None
            record["lng"] = None
            record["parcel_id"] = ""
            failed_count += 1
            logger.warning(
                f"No parcel ID for {record['case_number']} "
                f"(doc {record['document_number']})"
            )
            continue

        parcel_id = parcel_ids[0]
        record["parcel_id"] = parcel_id

        # Check cache
        if parcel_id in seen_parcels:
            coords = seen_parcels[parcel_id]
            if coords:
                record["lat"], record["lng"] = coords
                geocoded_count += 1
            else:
                record["lat"] = None
                record["lng"] = None
                failed_count += 1
            continue

        # Rate limit
        if i > 0:
            time.sleep(REQUEST_DELAY)

        coords = geocode_parcel(parcel_id)
        seen_parcels[parcel_id] = coords

        if coords:
            record["lat"], record["lng"] = coords
            geocoded_count += 1
            logger.info(
                f"[{geocoded_count + failed_count}/{len(records)}] "
                f"{parcel_id} -> ({coords[0]}, {coords[1]})"
            )
        else:
            record["lat"] = None
            record["lng"] = None
            failed_count += 1
            logger.warning(
                f"[{geocoded_count + failed_count}/{len(records)}] "
                f"{parcel_id} -> FAILED (case {record['case_number']})"
            )

    print(f"\nGeocoded {geocoded_count}/{len(records)} parcels "
          f"({failed_count} failed)")

    return records


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def build_output(records: list[dict]) -> dict:
    """Build the data.json structure from geocoded records."""
    # Date range
    dates = [r["recorded_date"] for r in records if r.get("recorded_date")]
    earliest = min(dates) if dates else ""
    latest = max(dates) if dates else ""

    # Case type summary
    type_counts: dict[str, int] = {}
    for r in records:
        ct = r.get("case_type", "") or "other"
        type_counts[ct] = type_counts.get(ct, 0) + 1

    # Separate known types from "other"
    known_types = {"FC", "CV", "CH"}
    summary = {"total": len(records)}
    other_count = 0
    for ct, count in type_counts.items():
        if ct in known_types:
            summary[ct] = count
        else:
            other_count += count
    if other_count:
        summary["other"] = other_count

    geocoded_count = sum(1 for r in records if r.get("lat") is not None)

    features = []
    for r in records:
        features.append({
            "document_number": r["document_number"],
            "case_number": r["case_number"],
            "case_type": r["case_type"],
            "recorded_date": r["recorded_date"],
            "party2": r["party2"],
            "parcel_id": r.get("parcel_id", ""),
            "subdivision": r.get("subdivision", ""),
            "lat": r.get("lat"),
            "lng": r.get("lng"),
        })

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_records": len(records),
        "geocoded_count": geocoded_count,
        "date_range": {"earliest": earliest, "latest": latest},
        "summary": summary,
        "features": features,
    }


def write_output(data: dict, output_path: Path) -> None:
    """Write data.json to disk, creating parent directories if needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Wrote {output_path} ({len(data['features'])} features)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Prepare lis pendens data for the map dashboard"
    )
    parser.add_argument(
        "--input", "-i", type=str, default=None,
        help=f"Input CSV path (default: {DEFAULT_CSV.name})"
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})"
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

    csv_path = Path(args.input) if args.input else DEFAULT_CSV
    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}")
        sys.exit(1)

    print(f"Reading {csv_path}...")
    records = read_csv(csv_path)

    if not records:
        print("No records found in CSV.")
        sys.exit(0)

    print(f"Geocoding {len(records)} parcels via ArcGIS...")
    records = geocode_records(records)

    data = build_output(records)
    write_output(data, output_path)


if __name__ == "__main__":
    main()
