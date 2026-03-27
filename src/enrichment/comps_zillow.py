"""Zillow Recently Sold Scraper.

Fetches recently sold properties in St. Clair County from Zillow's
search pages and stores them in the comparable_sales table.

Usage:
    python -m src.enrichment.comps_zillow [--db data/cheasuits.db] [--days 180] [-v]
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

ZILLOW_SOLD_URL = "https://www.zillow.com/st-clair-county-il/sold/"

IMPERSONATE_BROWSERS = ["chrome131", "chrome124"]
MAX_PAGES = 10
MAX_RETRIES = 3
PAGE_DELAY = 1.5


def _get_session() -> cffi_requests.Session:
    browser = random.choice(IMPERSONATE_BROWSERS)
    return cffi_requests.Session(impersonate=browser)


def _epoch_ms_to_date(epoch_ms: int | float | None) -> str:
    if not epoch_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, TypeError):
        return ""


def _parse_result(result: dict) -> dict | None:
    """Parse a single Zillow search result into a comparable_sales record."""
    price = result.get("unformattedPrice")
    if not price or price <= 0:
        return None

    address = result.get("address", "")
    if not address:
        return None

    lat_lng = result.get("latLong", {})
    lat = lat_lng.get("latitude")
    lng = lat_lng.get("longitude")

    home_info = result.get("hdpData", {}).get("homeInfo", {})

    date_sold_ms = home_info.get("dateSold")
    sale_date = _epoch_ms_to_date(date_sold_ms)
    if not sale_date:
        sale_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    sqft = result.get("area") or home_info.get("livingArea")
    if sqft is not None:
        sqft = float(sqft)

    beds = result.get("beds") or home_info.get("bedrooms")
    if beds is not None:
        beds = int(beds)

    baths = result.get("baths") or home_info.get("bathrooms")
    if baths is not None:
        baths = float(baths)

    lot_size_sqft = home_info.get("lotSize")
    lot_size = None
    if lot_size_sqft and lot_size_sqft > 0:
        lot_size = round(lot_size_sqft / 43560.0, 4)

    year_built = home_info.get("yearBuilt")
    if year_built is not None:
        year_built = int(year_built)

    home_type = home_info.get("homeType", "")
    zpid = str(result.get("zpid", ""))

    return {
        "address": address,
        "lat": lat,
        "lng": lng,
        "sale_price": float(price),
        "sale_date": sale_date,
        "property_type": home_type,
        "sqft": sqft,
        "beds": beds,
        "baths": baths,
        "lot_size": lot_size,
        "year_built": year_built,
        "source": "zillow",
        "source_id": zpid,
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
    }


def _extract_results_from_html(html: str) -> list[dict]:
    match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    if not match:
        logger.warning("No __NEXT_DATA__ found in Zillow page")
        return []

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse Zillow __NEXT_DATA__: {e}")
        return []

    return (
        data.get("props", {})
        .get("pageProps", {})
        .get("searchPageState", {})
        .get("cat1", {})
        .get("searchResults", {})
        .get("listResults", [])
    )


def fetch_zillow_sold(sold_within_days: int = 180) -> list[dict]:
    session = _get_session()
    all_records = []
    seen_zpids = set()

    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = ZILLOW_SOLD_URL
        else:
            url = f"{ZILLOW_SOLD_URL}{page}_p/"

        if page > 1:
            time.sleep(PAGE_DELAY)

        html = ""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, headers={
                    "Accept": "text/html,application/xhtml+xml",
                }, timeout=20)
                resp.raise_for_status()
                html = resp.text
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    logger.warning(f"Zillow page {page} attempt {attempt}/{MAX_RETRIES}: {e}")
                    time.sleep(2)
                else:
                    logger.error(f"Zillow page {page} failed after {MAX_RETRIES} attempts: {e}")

        if not html:
            break

        results = _extract_results_from_html(html)
        if not results:
            logger.info(f"No results on page {page}, stopping pagination")
            break

        page_count = 0
        for r in results:
            zpid = str(r.get("zpid", ""))
            if zpid in seen_zpids:
                continue
            seen_zpids.add(zpid)

            record = _parse_result(r)
            if record:
                all_records.append(record)
                page_count += 1

        logger.info(f"Page {page}: {page_count} new records (total: {len(all_records)})")

        if len(results) < 20:
            break

    logger.info(f"Fetched {len(all_records)} sold records from Zillow")
    return all_records


def fetch_and_store(db_path: Path, sold_within_days: int = 180) -> int:
    from src.db.database import get_db, upsert_comparable_sales

    records = fetch_zillow_sold(sold_within_days)
    if not records:
        print("No records fetched from Zillow.")
        return 0

    conn = get_db(db_path)
    count = upsert_comparable_sales(conn, records)
    conn.close()

    print(f"Stored {count} Zillow sold records in comparable_sales table")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Fetch recently sold properties from Zillow for St. Clair County"
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
