"""County Recorder Sales History Scraper — DevNetWedge + ArcGIS.

Discovers nearby parcels via ArcGIS spatial query, then scrapes sales
history from each parcel's DevNetWedge page. Stores arm's-length
transactions in the comparable_sales table.

Optimizations over naive per-property approach:
- Grid-based clustering reduces ArcGIS calls from N-properties to ~20 cells
- SQLite progress table allows resuming interrupted runs
- Batch commits every 50 parcels

Usage:
    python -m src.enrichment.comps_recorder [--db data/cheasuits.db] [--radius 1.5] [--months 6] [-v]
    python -m src.enrichment.comps_recorder --reset  # clear progress and re-scrape
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, date
from pathlib import Path

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

# Sale types to exclude (non-arm's-length)
EXCLUDE_SALE_TYPES = {
    "quit claim deed", "qcd", "qc", "quit claim",
}

# Minimum sale price to be considered arm's-length
MIN_SALE_PRICE = 100


# ---------------------------------------------------------------------------
# Progress tracking (SQLite table)
# ---------------------------------------------------------------------------

_PROGRESS_SCHEMA = """
CREATE TABLE IF NOT EXISTS recorder_progress (
    parcel_number TEXT PRIMARY KEY,
    lat REAL,
    lng REAL,
    scraped_at TEXT,
    sales_found INTEGER DEFAULT 0
);
"""


def _ensure_progress_table(conn):
    conn.executescript(_PROGRESS_SCHEMA)
    conn.commit()


def _get_scraped_parcels(conn) -> set[str]:
    """Return set of parcel numbers already scraped."""
    _ensure_progress_table(conn)
    cursor = conn.execute("SELECT parcel_number FROM recorder_progress")
    return {row[0] for row in cursor.fetchall()}


def _mark_scraped(conn, parcel_number: str, sales_found: int, lat=None, lng=None):
    conn.execute(
        "INSERT OR REPLACE INTO recorder_progress "
        "(parcel_number, lat, lng, scraped_at, sales_found) VALUES (?, ?, ?, ?, ?)",
        (parcel_number, lat, lng, datetime.now().isoformat(timespec="seconds"),
         sales_found),
    )


# ---------------------------------------------------------------------------
# Grid-based location clustering
# ---------------------------------------------------------------------------

def _cluster_locations(locations: list[dict], cell_size_miles: float) -> list[tuple[float, float]]:
    """Snap property locations to grid cells and return unique cell centroids.

    Each cell covers cell_size_miles × cell_size_miles. Properties in the
    same cell share one ArcGIS query instead of getting individual queries.
    """
    lat_step = cell_size_miles / 69.0
    # Use median latitude for lng step (good enough for one county)
    med_lat = sorted(loc["lat"] for loc in locations)[len(locations) // 2]
    lng_step = cell_size_miles / (69.0 * math.cos(math.radians(med_lat)))

    cells: dict[tuple[int, int], list[tuple[float, float]]] = {}
    for loc in locations:
        ci = int(loc["lat"] / lat_step)
        cj = int(loc["lng"] / lng_step)
        cells.setdefault((ci, cj), []).append((loc["lat"], loc["lng"]))

    # Return centroid of each occupied cell
    centroids = []
    for pts in cells.values():
        avg_lat = sum(p[0] for p in pts) / len(pts)
        avg_lng = sum(p[1] for p in pts) / len(pts)
        centroids.append((avg_lat, avg_lng))

    return centroids


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
        "resultRecordCount": "1000",
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

            logger.info(f"ArcGIS found {len(parcels)} parcels near ({lat:.4f}, {lng:.4f})")
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

    if any(exc in st for exc in EXCLUDE_SALE_TYPES):
        return False

    if price is None or price < MIN_SALE_PRICE:
        return False

    return True


def parse_sales_history(html: str, parcel_id: str) -> list[dict]:
    """Extract sales history rows from a DevNetWedge parcel page.

    Sales History table columns:
    Year | Document# | Sale Type | Sale Date | Sold By | Sold To | Gross Price | Personal Property | Net Price
    """
    soup = BeautifulSoup(html, "html.parser")

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

        sale_type = cells[2] if len(cells) > 2 else ""
        sale_date_raw = cells[3] if len(cells) > 3 else ""
        gross_price = _parse_currency(cells[6]) if len(cells) > 6 else None

        net_price = _parse_currency(cells[8]) if len(cells) > 8 else None
        price = net_price if net_price and net_price > 0 else gross_price

        if not _is_arm_length(sale_type, price):
            continue

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
    """Fetch a DevNetWedge parcel page and extract sales history."""
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

def _rank_parcels_by_proximity(
    all_parcels: dict[str, dict],
    locations: list[dict],
    max_per_property: int = 30,
) -> list[dict]:
    """Return the closest parcels to each property, deduplicated.

    Instead of scraping all ~27K discovered parcels, pick the nearest
    max_per_property parcels to each subject property. After dedup this
    keeps the scrape count manageable (~2-4K) while covering every
    property's neighborhood.
    """
    from src.enrichment.comps import haversine_distance

    selected: dict[str, dict] = {}

    for loc in locations:
        scored = []
        for pnum, p in all_parcels.items():
            if p.get("lat") is None or p.get("lng") is None:
                continue
            dist = haversine_distance(loc["lat"], loc["lng"], p["lat"], p["lng"])
            scored.append((dist, pnum, p))

        scored.sort(key=lambda x: x[0])

        for _, pnum, p in scored[:max_per_property]:
            if pnum not in selected:
                selected[pnum] = p

    return list(selected.values())


def fetch_area_comps(
    db_path: Path,
    radius_miles: float = 1.5,
    months_back: int = 6,
    reset: bool = False,
) -> int:
    """Discover nearby parcels for each property, scrape sales, and store.

    Uses grid-based clustering to minimize ArcGIS calls, proximity ranking
    to limit scrape volume, and a progress table for resumption.

    Returns total number of comparable sales stored.
    """
    from src.db.database import get_db, upsert_comparable_sales

    conn = get_db(db_path)
    _ensure_progress_table(conn)

    if reset:
        conn.execute("DELETE FROM recorder_progress")
        conn.commit()
        print("Cleared recorder progress — starting fresh.")

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

    # --- Phase 1: Clustered ArcGIS discovery ---
    cell_size = radius_miles * 2
    centroids = _cluster_locations(locations, cell_size)
    print(f"Phase 1: Querying ArcGIS for {len(centroids)} grid cells "
          f"(clustered from {len(locations)} property locations)...")

    all_parcels: dict[str, dict] = {}
    for i, (clat, clng) in enumerate(centroids):
        if i > 0:
            time.sleep(REQUEST_DELAY)

        parcels = discover_nearby_parcels(clat, clng, radius_miles)
        for p in parcels:
            pnum = p["parcel_number"]
            if pnum not in all_parcels:
                all_parcels[pnum] = p

        logger.info(f"[{i+1}/{len(centroids)}] Cell ({clat:.4f}, {clng:.4f}) "
                     f"-> {len(parcels)} parcels (total unique: {len(all_parcels)})")

    print(f"Phase 1 complete: {len(all_parcels)} unique parcels discovered")

    # --- Phase 1b: Rank by proximity to subject properties ---
    ranked = _rank_parcels_by_proximity(all_parcels, locations, max_per_property=30)
    print(f"Ranked to {len(ranked)} parcels (nearest 30 per property, deduplicated)")

    # Filter out already-scraped parcels
    already_scraped = _get_scraped_parcels(conn)
    remaining = [p for p in ranked if p["parcel_number"] not in already_scraped]

    print(f"{len(already_scraped)} already scraped, {len(remaining)} to scrape")

    if not remaining:
        print("All parcels already scraped. Use --reset to re-scrape.")
        conn.close()
        return 0

    # --- Phase 2: Scrape sales history with checkpointing ---
    cutoff = date.today().replace(
        month=max(1, date.today().month - months_back % 12),
        year=date.today().year - months_back // 12,
    )
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    total_stored = 0
    parcels_with_sales = 0
    errors = 0

    est_minutes = len(remaining) * REQUEST_DELAY / 60
    print(f"Phase 2: Scraping sales history for {len(remaining)} parcels "
          f"(~{est_minutes:.0f} min)...")

    for i, parcel in enumerate(remaining):
        if i > 0:
            time.sleep(REQUEST_DELAY)

        pnum = parcel["parcel_number"]
        sales = fetch_parcel_sales(pnum)

        if sales is None:
            errors += 1
            _mark_scraped(conn, pnum, 0, parcel.get("lat"), parcel.get("lng"))
            continue

        # Filter by date and build records for DB
        records = []
        for sale in sales:
            if sale["sale_date"] < cutoff_str:
                continue

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
            logger.info(f"[{i+1}/{len(remaining)}] {pnum} -> {count} sales stored")

        # Mark as scraped (even if no sales — so we don't re-scrape)
        _mark_scraped(conn, pnum, len(records), parcel.get("lat"), parcel.get("lng"))

        # Batch commit and progress every 50 parcels
        if (i + 1) % 50 == 0:
            conn.commit()
            print(f"  Progress: {i+1}/{len(remaining)} parcels scraped, "
                  f"{total_stored} sales stored, {errors} errors")

    conn.commit()
    conn.close()
    print(f"\nDone! Stored {total_stored} recorder sales from "
          f"{parcels_with_sales}/{len(remaining)} parcels ({errors} errors)")
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
        "--reset", action="store_true",
        help="Clear progress table and re-scrape all parcels",
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

    fetch_area_comps(Path(args.db), args.radius, args.months, args.reset)


if __name__ == "__main__":
    main()
