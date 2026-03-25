"""County Recorder Sales History Scraper — DevNetWedge + ArcGIS.

Discovers nearby parcels via ArcGIS spatial query, then scrapes sales
history from each parcel's DevNetWedge page. Stores arm's-length
transactions in the comparable_sales table.

Usage:
    python -m src.enrichment.comps_recorder [--db data/cheasuits.db] [--radius 1.5] [--months 6] [-v]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from src.utils.parsing import strip_parcel_hyphens

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

DEVNET_BASE_URL = "https://stclairil.devnetwedge.com/parcel/view"
ARCGIS_URL = (
    "https://arcgispublicmap.co.st-clair.il.us/server/rest/services/"
    "SCC_parcel_map_data/MapServer/29/query"
)

REQUEST_DELAY = 0.3
MAX_RETRIES = 3

# Sale types to include (arm's-length transactions)
INCLUDE_SALE_TYPES = {
    "warranty deed", "special warranty deed", "trustees deed",
    "trustee's deed", "wd", "swd", "td",
}

# Sale types to exclude (non-arm's-length)
EXCLUDE_SALE_TYPES = {
    "quit claim deed", "qcd", "qc", "quit claim",
}

# Minimum sale price to be considered arm's-length
MIN_SALE_PRICE = 100


# ---------------------------------------------------------------------------
# ArcGIS Parcel Discovery
# ---------------------------------------------------------------------------

def discover_nearby_parcels(
    lat: float,
    lng: float,
    radius_miles: float = 1.5,
) -> list[dict]:
    """Query ArcGIS for parcels within a bounding box around the given point.

    Returns list of dicts with parcel_number, lat, lng (centroid).
    """
    import math

    # Compute bounding box envelope
    lat_delta = radius_miles / 69.0
    lng_delta = radius_miles / (69.0 * math.cos(math.radians(lat)))

    xmin = lng - lng_delta
    ymin = lat - lat_delta
    xmax = lng + lng_delta
    ymax = lat + lat_delta

    params = urllib.parse.urlencode({
        "where": "1=1",
        "geometry": json.dumps({
            "xmin": xmin, "ymin": ymin,
            "xmax": xmax, "ymax": ymax,
            "spatialReference": {"wkid": 4326},
        }),
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "parcel_number",
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": "500",
        "f": "json",
    })

    url = f"{ARCGIS_URL}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; CheasuitsBot/1.0)",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            features = data.get("features", [])
            parcels = []

            for f in features:
                attrs = f.get("attributes", {})
                geom = f.get("geometry", {})
                parcel_num = attrs.get("parcel_number", "")

                if not parcel_num:
                    continue

                # Compute centroid from rings
                rings = geom.get("rings", [])
                if rings:
                    from src.visualization.prepare_data import compute_centroid
                    plat, plng = compute_centroid(rings)
                else:
                    plat, plng = None, None

                parcels.append({
                    "parcel_number": parcel_num,
                    "lat": plat,
                    "lng": plng,
                })

            logger.info(f"ArcGIS found {len(parcels)} parcels near ({lat}, {lng})")
            return parcels

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"ArcGIS attempt {attempt}/{MAX_RETRIES} failed: {e}")
                time.sleep(1)
            else:
                logger.error(f"ArcGIS query failed after {MAX_RETRIES} attempts: {e}")
                return []

    return []


# ---------------------------------------------------------------------------
# DevNetWedge Sales History Parser
# ---------------------------------------------------------------------------

def _parse_currency(text: str) -> float | None:
    """Parse '$1,234.56' or '1,234' into a float."""
    cleaned = re.sub(r"[^\d.]", "", text)
    if cleaned:
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _is_arm_length(sale_type: str, price: float | None) -> bool:
    """Determine if a sale is arm's-length based on type and price."""
    st = sale_type.lower().strip()

    # Explicit exclude
    if any(exc in st for exc in EXCLUDE_SALE_TYPES):
        return False

    # Price filter
    if price is None or price < MIN_SALE_PRICE:
        return False

    # If sale type matches include list or is unrecognized, include it
    # (many recorder offices use non-standard names)
    return True


def parse_sales_history(html: str, parcel_id: str) -> list[dict]:
    """Extract sales history rows from a DevNetWedge parcel page.

    Sales History table columns:
    Year | Document# | Sale Type | Sale Date | Sold By | Sold To | Gross Price | Personal Property | Net Price
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the Sales History section
    sales_table = None
    for table in soup.find_all("table"):
        header_text = table.get_text().lower()
        if "sale type" in header_text and "sale date" in header_text:
            sales_table = table
            break

    if not sales_table:
        logger.debug(f"No sales history table found for parcel {parcel_id}")
        return []

    records = []
    rows = sales_table.find_all("tr")

    for tr in rows:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 7:
            continue

        # Columns: Year, Document#, Sale Type, Sale Date, Sold By, Sold To, Gross Price, ...
        sale_type = cells[2] if len(cells) > 2 else ""
        sale_date_raw = cells[3] if len(cells) > 3 else ""
        gross_price = _parse_currency(cells[6]) if len(cells) > 6 else None

        # Net price is preferred if available
        net_price = _parse_currency(cells[8]) if len(cells) > 8 else None
        price = net_price if net_price and net_price > 0 else gross_price

        if not _is_arm_length(sale_type, price):
            continue

        # Parse sale date
        sale_date = ""
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                parsed = datetime.strptime(sale_date_raw, fmt)
                sale_date = parsed.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

        if not sale_date:
            continue

        doc_number = cells[1] if len(cells) > 1 else ""

        records.append({
            "sale_date": sale_date,
            "sale_price": price,
            "sale_type": sale_type,
            "source_id": doc_number,
            "parcel_id": parcel_id,
        })

    return records


def fetch_parcel_sales(
    parcel_id: str,
    year: int | None = None,
) -> list[dict]:
    """Fetch a DevNetWedge parcel page and extract sales history.

    Returns list of sale records.
    """
    stripped = strip_parcel_hyphens(parcel_id)
    yr = year or (datetime.now().year - 2)
    url = f"{DEVNET_BASE_URL}/{stripped}/{yr}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; CheasuitsBot/1.0)",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8")

            if not html or len(html) < 200:
                return []

            return parse_sales_history(html, parcel_id)

        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug(f"404 for parcel {parcel_id}")
                return []
            if attempt < MAX_RETRIES:
                logger.warning(f"HTTP {e.code} for {parcel_id}, attempt {attempt}/{MAX_RETRIES}")
                time.sleep(1)
            else:
                logger.error(f"Failed to fetch {parcel_id} after {MAX_RETRIES} attempts")
                return []
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(1)
            else:
                logger.error(f"Failed to fetch {parcel_id}: {e}")
                return []

    return []


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def fetch_area_comps(
    db_path: Path,
    radius_miles: float = 1.5,
    months_back: int = 6,
) -> int:
    """Discover nearby parcels for each property, scrape sales, and store.

    Returns total number of comparable sales stored.
    """
    from src.db.database import get_db, upsert_comparable_sales

    conn = get_db(db_path)

    # Get distinct property locations
    cursor = conn.execute(
        "SELECT DISTINCT lat, lng FROM properties "
        "WHERE lat IS NOT NULL AND lng IS NOT NULL"
    )
    locations = [dict(row) for row in cursor.fetchall()]

    if not locations:
        print("No geocoded properties found.")
        conn.close()
        return 0

    print(f"Discovering parcels near {len(locations)} property locations "
          f"(radius={radius_miles}mi)...")

    # Collect all nearby parcels (deduplicated)
    all_parcels: dict[str, dict] = {}
    for i, loc in enumerate(locations):
        if i > 0:
            time.sleep(REQUEST_DELAY)

        parcels = discover_nearby_parcels(loc["lat"], loc["lng"], radius_miles)
        for p in parcels:
            pnum = p["parcel_number"]
            if pnum not in all_parcels:
                all_parcels[pnum] = p

        logger.info(f"[{i+1}/{len(locations)}] Found {len(parcels)} parcels "
                     f"near ({loc['lat']}, {loc['lng']})")

    print(f"Found {len(all_parcels)} unique parcels to check for sales history")

    # Fetch sales history for each parcel
    cutoff = date.today().replace(
        month=max(1, date.today().month - months_back % 12),
        year=date.today().year - months_back // 12,
    )
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    total_stored = 0
    parcels_with_sales = 0

    parcel_list = list(all_parcels.values())
    for i, parcel in enumerate(parcel_list):
        if i > 0:
            time.sleep(REQUEST_DELAY)

        pnum = parcel["parcel_number"]
        sales = fetch_parcel_sales(pnum)

        if not sales:
            continue

        # Filter by date and build records for DB
        records = []
        for sale in sales:
            if sale["sale_date"] < cutoff_str:
                continue

            # Build address from parcel info (we'll use the parcel_id as proxy)
            # The address comes from the DevNetWedge page, but we already have
            # parcel coordinates from ArcGIS
            records.append({
                "address": f"Parcel {pnum}",
                "lat": parcel.get("lat"),
                "lng": parcel.get("lng"),
                "sale_price": sale["sale_price"],
                "sale_date": sale["sale_date"],
                "property_type": "",
                "sqft": None,
                "beds": None,
                "baths": None,
                "lot_size": None,
                "year_built": None,
                "source": "recorder",
                "source_id": sale.get("source_id", ""),
                "scraped_at": datetime.now().isoformat(timespec="seconds"),
            })

        if records:
            count = upsert_comparable_sales(conn, records)
            total_stored += count
            parcels_with_sales += 1
            logger.info(f"[{i+1}/{len(parcel_list)}] {pnum} -> {count} sales stored")

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(parcel_list)} parcels checked, "
                  f"{total_stored} sales stored")

    conn.close()
    print(f"\nStored {total_stored} recorder sales from {parcels_with_sales} parcels")
    return total_stored


def main():
    parser = argparse.ArgumentParser(
        description="Scrape county recorder sales history for comparable sales"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--radius", type=float, default=1.5,
        help="Search radius in miles (default: 1.5)",
    )
    parser.add_argument(
        "--months", type=int, default=6,
        help="Look-back period in months (default: 6)",
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

    fetch_area_comps(Path(args.db), args.radius, args.months)


if __name__ == "__main__":
    main()
