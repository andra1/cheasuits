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
import csv
import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.utils.parsing import parse_legals, strip_parcel_hyphens

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


def read_db(db_path: Path) -> list[dict]:
    """Read property records from the SQLite database."""
    from src.db.database import get_db, get_all, get_ungeocoded, update_geocoding, get_valuations, get_property_comps

    conn = get_db(db_path)
    rows = get_all(conn)

    logger.info(f"Read {len(rows)} records from {db_path}")

    # Geocode rows that need it
    ungeocoded = get_ungeocoded(conn)
    if ungeocoded:
        print(f"Geocoding {len(ungeocoded)} new parcels via ArcGIS...")
        seen: dict[str, tuple[float, float] | None] = {}
        geocoded_count = 0

        for i, row in enumerate(ungeocoded):
            parcel_id = row["parcel_id"]

            if parcel_id in seen:
                coords = seen[parcel_id]
            else:
                if i > 0:
                    time.sleep(REQUEST_DELAY)
                coords = geocode_parcel(parcel_id)
                seen[parcel_id] = coords

            if coords:
                update_geocoding(conn, row["document_number"], coords[0], coords[1])
                geocoded_count += 1
                logger.info(f"[{i+1}/{len(ungeocoded)}] {parcel_id} -> ({coords[0]}, {coords[1]})")
            else:
                logger.warning(f"[{i+1}/{len(ungeocoded)}] {parcel_id} -> FAILED")

        print(f"Geocoded {geocoded_count}/{len(ungeocoded)} new parcels")

    # Re-read all rows (now with updated geocoding)
    rows = get_all(conn)
    conn.close()

    # Convert to the format build_output expects
    records = []
    for row in rows:
        records.append({
            "document_number": row["document_number"] or "",
            "case_number": row["case_number"] or "",
            "case_type": row["case_type"] or "",
            "recorded_date": row["recorded_date"] or "",
            "party2": row["party2"] or "",
            "parcel_id": row["parcel_id"] or "",
            "subdivision": row["subdivision"] or "",
            "lat": row["lat"],
            "lng": row["lng"],
            # Assessor enrichment fields
            "owner_name": row["owner_name"] or "",
            "property_address": row["property_address"] or "",
            "mailing_address": row["mailing_address"] or "",
            "absentee_owner": bool(row["absentee_owner"]) if row["absentee_owner"] is not None else False,
            "assessed_value": row["assessed_value"],
            "net_taxable_value": row["net_taxable_value"],
            "tax_rate": row["tax_rate"],
            "total_tax": row["total_tax"],
            "tax_status": row["tax_status"] or "",
            "property_class": row["property_class"] or "",
            "acres": row["acres"],
            # Valuation fields
            "estimated_market_value": row["estimated_market_value"],
            "valued_at": row["valued_at"] or "",
            # Mortgage fields
            "mortgage_amount": row.get("mortgage_amount"),
            "mortgage_date": row.get("mortgage_date") or "",
            "mortgage_lender": row.get("mortgage_lender") or "",
            "total_mortgage_debt": row.get("total_mortgage_debt"),
            "mortgage_count": row.get("mortgage_count"),
            "mortgage_source": row.get("mortgage_source") or "",
            # Lien fields
            "federal_tax_lien_amount": row.get("federal_tax_lien_amount"),
            "state_tax_lien_amount": row.get("state_tax_lien_amount"),
            "judgment_lien_amount": row.get("judgment_lien_amount"),
            "total_recorded_liens": row.get("total_recorded_liens"),
            "lien_count": row.get("lien_count"),
            # Viability fields
            "total_lien_burden": row.get("total_lien_burden"),
            "equity_spread": row.get("equity_spread"),
            "equity_ratio": row.get("equity_ratio"),
            "viability_score": row.get("viability_score"),
            "viability_details": row.get("viability_details") or "",
        })

    # Attach nested valuations and comps per property
    conn2 = get_db(db_path)
    for rec in records:
        doc_num = rec["document_number"]
        vals = get_valuations(conn2, doc_num)
        rec["valuations"] = [
            {
                "source": v["source"],
                "estimate": v["estimate"],
                "source_url": v.get("source_url") or "",
                "confidence": v.get("confidence") or "",
                "comp_count": v.get("comp_count"),
                "valued_at": v.get("valued_at") or "",
            }
            for v in vals
        ]
        comps = get_property_comps(conn2, doc_num)
        rec["comps"] = [
            {
                "address": c["address"],
                "sale_price": c["sale_price"],
                "sale_date": c["sale_date"],
                "distance_miles": c.get("distance_miles"),
                "similarity_score": c.get("similarity_score"),
                "lot_size_ratio": c.get("lot_size_ratio"),
                "adjusted_price": c.get("adjusted_price"),
                "sqft": c.get("sqft"),
                "beds": c.get("beds"),
                "baths": c.get("baths"),
                "lot_size": c.get("lot_size"),
                "year_built": c.get("year_built"),
                "source": c.get("source") or "",
                "source_id": c.get("source_id") or "",
            }
            for c in comps
        ]
    conn2.close()

    return records


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

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
        feature = {
            "document_number": r["document_number"],
            "case_number": r["case_number"],
            "case_type": r["case_type"],
            "recorded_date": r["recorded_date"],
            "party2": r["party2"],
            "parcel_id": r.get("parcel_id", ""),
            "subdivision": r.get("subdivision", ""),
            "lat": r.get("lat"),
            "lng": r.get("lng"),
        }
        # Include assessor fields if present
        for field in ("owner_name", "property_address", "mailing_address",
                      "absentee_owner", "assessed_value", "net_taxable_value",
                      "tax_rate", "total_tax",
                      "tax_status", "property_class", "acres",
                      "estimated_market_value", "valued_at",
                      "mortgage_amount", "mortgage_date", "mortgage_lender",
                      "total_mortgage_debt", "mortgage_count",
                      "mortgage_source",
                      "federal_tax_lien_amount", "state_tax_lien_amount",
                      "judgment_lien_amount", "total_recorded_liens",
                      "lien_count",
                      "total_lien_burden", "equity_spread", "equity_ratio",
                      "viability_score", "viability_details"):
            if field in r:
                feature[field] = r[field]
        if "valuations" in r:
            feature["valuations"] = r["valuations"]
        if "comps" in r:
            feature["comps"] = r["comps"]
        # Compute estimated equity (legacy: mortgage-only)
        emv = r.get("estimated_market_value")
        debt = r.get("total_mortgage_debt")
        if emv is not None and debt is not None:
            feature["estimated_equity"] = round(emv - debt, 2)
        features.append(feature)

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
        "--db", type=str, default=None,
        help="SQLite database path. When provided, reads from DB instead of CSV."
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

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    if args.db:
        db_path = Path(args.db)
        if not db_path.exists():
            print(f"ERROR: Database not found: {db_path}")
            sys.exit(1)
        print(f"Reading from database {db_path}...")
        records = read_db(db_path)
    else:
        csv_path = Path(args.input) if args.input else DEFAULT_CSV
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

    if not records:
        print("No records found.")
        sys.exit(0)

    data = build_output(records)
    write_output(data, output_path)


if __name__ == "__main__":
    main()
