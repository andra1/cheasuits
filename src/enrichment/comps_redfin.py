"""Redfin Recently Sold Scraper — Bulk CSV Download.

Fetches recently sold properties in St. Clair County from Redfin's
gis-csv endpoint and stores them in the comparable_sales table.

Usage:
    python -m src.enrichment.comps_redfin [--db data/cheasuits.db] [--days 180] [-v]
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

# Redfin gis-csv endpoint parameters for St. Clair County, IL
REDFIN_GIS_CSV_URL = "https://www.redfin.com/stingray/api/gis-csv"
REDFIN_PARAMS = {
    "al": "1",
    "market": "stlouis",
    "region_id": "793",       # St. Clair County
    "region_type": "5",       # county
    "status": "9",            # sold
    "num_homes": "350",
    "uipt": "1,2,3,4,5,6",   # all property types
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# CSV Parsing
# ---------------------------------------------------------------------------

def _parse_lot_size(raw: str) -> float | None:
    """Convert Redfin lot size string to acres.

    Handles formats like:
    - "5,000 Sq. Ft." -> ~0.115 acres
    - "0.25 Acres" -> 0.25
    - "10,890 sq ft" -> ~0.25 acres
    """
    if not raw or raw.strip() == "":
        return None

    raw = raw.strip()

    # Check for acres
    acres_match = re.search(r'([\d,.]+)\s*acre', raw, re.IGNORECASE)
    if acres_match:
        try:
            return round(float(acres_match.group(1).replace(",", "")), 4)
        except ValueError:
            pass

    # Check for square feet and convert to acres (1 acre = 43560 sq ft)
    sqft_match = re.search(r'([\d,.]+)\s*sq', raw, re.IGNORECASE)
    if sqft_match:
        try:
            sqft = float(sqft_match.group(1).replace(",", ""))
            return round(sqft / 43560.0, 4)
        except ValueError:
            pass

    return None


def _parse_int(raw: str | None) -> int | None:
    """Safely parse an integer from CSV field."""
    if not raw or raw.strip() == "":
        return None
    try:
        return int(float(raw.strip()))
    except (ValueError, TypeError):
        return None


def _parse_float(raw: str | None) -> float | None:
    """Safely parse a float from CSV field."""
    if not raw or raw.strip() == "":
        return None
    try:
        return float(raw.strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _parse_redfin_csv(csv_text: str) -> list[dict]:
    """Parse Redfin CSV text into comparable_sales records.

    Redfin CSV columns include: SALE TYPE, SOLD DATE, PROPERTY TYPE, ADDRESS,
    CITY, STATE OR PROVINCE, ZIP OR POSTAL CODE, PRICE, BEDS, BATHS,
    SQUARE FEET, LOT SIZE, YEAR BUILT, LATITUDE, LONGITUDE, URL, MLS#, etc.
    """
    records = []

    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        address_parts = [
            row.get("ADDRESS", "").strip(),
            row.get("CITY", "").strip(),
            row.get("STATE OR PROVINCE", "").strip(),
            row.get("ZIP OR POSTAL CODE", "").strip(),
        ]
        address = ", ".join(p for p in address_parts if p)

        price = _parse_float(row.get("PRICE"))
        sold_date = row.get("SOLD DATE", "").strip()

        if not address or not price or price <= 0 or not sold_date:
            continue

        # Normalize date to YYYY-MM-DD
        try:
            parsed_date = datetime.strptime(sold_date, "%B %d, %Y")
            sale_date = parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            try:
                parsed_date = datetime.strptime(sold_date, "%b %d, %Y")
                sale_date = parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                # Try YYYY-MM-DD directly
                sale_date = sold_date

        lat = _parse_float(row.get("LATITUDE"))
        lng = _parse_float(row.get("LONGITUDE"))

        records.append({
            "address": address,
            "lat": lat,
            "lng": lng,
            "sale_price": price,
            "sale_date": sale_date,
            "property_type": row.get("PROPERTY TYPE", "").strip(),
            "sqft": _parse_float(row.get("SQUARE FEET")),
            "beds": _parse_int(row.get("BEDS")),
            "baths": _parse_float(row.get("BATHS")),
            "lot_size": _parse_lot_size(row.get("LOT SIZE", "")),
            "year_built": _parse_int(row.get("YEAR BUILT")),
            "source": "redfin",
            "source_id": row.get("MLS#", "").strip(),
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        })

    return records


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------

def fetch_redfin_sold(sold_within_days: int = 180) -> list[dict]:
    """Fetch recently sold properties from Redfin's gis-csv endpoint.

    Returns list of records ready for comparable_sales table.
    """
    params = dict(REDFIN_PARAMS)
    params["sold_within_days"] = str(sold_within_days)

    url = f"{REDFIN_GIS_CSV_URL}?{urllib.parse.urlencode(params)}"
    logger.info(f"Fetching Redfin sold data: {url}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/csv,application/csv,*/*",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                csv_text = resp.read().decode("utf-8")

            if not csv_text or len(csv_text) < 50:
                logger.warning("Redfin returned empty or tiny response")
                return []

            records = _parse_redfin_csv(csv_text)
            logger.info(f"Parsed {len(records)} sold records from Redfin CSV")
            return records

        except urllib.error.HTTPError as e:
            logger.warning(f"Redfin HTTP {e.code} on attempt {attempt}/{MAX_RETRIES}")
            if attempt == MAX_RETRIES:
                logger.error(f"Redfin fetch failed after {MAX_RETRIES} attempts: {e}")
                return []
        except Exception as e:
            logger.warning(f"Redfin fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                logger.error(f"Redfin fetch failed after {MAX_RETRIES} attempts: {e}")
                return []

    return []


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def fetch_and_store(db_path: Path, sold_within_days: int = 180) -> int:
    """Fetch Redfin sold data and store in comparable_sales table.

    Returns number of records stored.
    """
    from src.db.database import get_db, upsert_comparable_sales

    records = fetch_redfin_sold(sold_within_days)
    if not records:
        print("No records fetched from Redfin.")
        return 0

    conn = get_db(db_path)
    count = upsert_comparable_sales(conn, records)
    conn.close()

    print(f"Stored {count} Redfin sold records in comparable_sales table")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Fetch recently sold properties from Redfin for St. Clair County"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--days", type=int, default=180,
        help="Look-back period in days (default: 180)",
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

    fetch_and_store(Path(args.db), args.days)


if __name__ == "__main__":
    main()
