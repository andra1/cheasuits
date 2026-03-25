"""DevNetWedge Assessor Scraper — St. Clair County property enrichment.

Fetches property data (owner, tax status, assessed value) from the county
assessor's public web portal and updates the pipeline database.

URL pattern: https://stclairil.devnetwedge.com/parcel/view/{parcel_no_hyphens}/{year}

Usage:
    python -m src.enrichment.assessor [--db data/cheasuits.db] [--year 2024] [-v]
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from src.utils.parsing import strip_parcel_hyphens

logger = logging.getLogger(__name__)

BASE_URL = "https://stclairil.devnetwedge.com/parcel/view"
DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"
DEFAULT_YEAR = datetime.now().year - 2  # county data lags ~1 year; most recent complete year
REQUEST_DELAY = 0.3
MAX_RETRIES = 3


@dataclass
class AssessorRecord:
    parcel_id: str
    owner_name: str = ""
    property_address: str = ""
    mailing_address: str = ""
    absentee_owner: bool = False
    assessed_value: float | None = None
    net_taxable_value: float | None = None
    tax_rate: float | None = None
    total_tax: float | None = None
    tax_status: str = ""
    property_class: str = ""
    acres: float | None = None

    def to_db_dict(self) -> dict:
        """Convert to dict suitable for update_enrichment()."""
        d = asdict(self)
        d.pop("parcel_id")
        d["absentee_owner"] = 1 if self.absentee_owner else 0
        return {k: v for k, v in d.items() if v is not None and v != ""}


def _get_field_text(soup: BeautifulSoup, label: str) -> str:
    """Find a div.inner-label by text and return the sibling div.inner-value text."""
    label_div = soup.find("div", class_="inner-label",
                          string=re.compile(re.escape(label), re.IGNORECASE))
    if label_div:
        val_div = label_div.find_next_sibling("div", class_="inner-value")
        if val_div:
            return val_div.get_text(strip=True)
    return ""


def _parse_currency(text: str) -> float | None:
    """Parse '$1,234.56' or '1,234' into a float."""
    cleaned = re.sub(r"[^\d.]", "", text)
    if cleaned:
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def parse_assessor_html(html: str, parcel_id: str) -> AssessorRecord:
    """Parse a DevNetWedge property page into an AssessorRecord."""
    soup = BeautifulSoup(html, "html.parser")

    # "Owner Name & Address" field contains owner name + address on separate lines
    owner_block = _get_field_text(soup, "Owner Name")
    owner_name = owner_block.split("\n")[0].strip() if owner_block else ""

    property_address = _get_field_text(soup, "Site Address")
    mailing_address = _get_field_text(soup, "Mailing Address")
    property_class = _get_field_text(soup, "Property Class")

    acres_text = _get_field_text(soup, "Acres")
    acres = float(acres_text) if acres_text else None

    net_taxable_text = _get_field_text(soup, "Net Taxable Value")
    net_taxable_value = _parse_currency(net_taxable_text)

    # Parse assessed value from "Board of Review Equalized" row in valuation table
    assessed_value = None
    for table in soup.find_all("table"):
        if "Equalized" not in table.get_text():
            continue
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells and "board of review equalized" in cells[0].lower():
                # "Total" column is index 6 (after Homesite, Dwelling, Farm Land, Farm Building, Mineral)
                for cell_text in reversed(cells[1:]):
                    val = _parse_currency(cell_text)
                    if val and val > 0:
                        assessed_value = val
                        break
                break
        if assessed_value:
            break

    tax_rate_text = _get_field_text(soup, "Tax Rate")
    tax_rate = float(re.sub(r"[^\d.]", "", tax_rate_text)) if tax_rate_text and tax_rate_text != "Unavailable" else None

    total_tax_text = _get_field_text(soup, "Total Tax")
    total_tax = _parse_currency(total_tax_text) if total_tax_text != "Unavailable" else None

    # Detect tax sale status
    page_text = soup.get_text()
    if "TAXES SOLD" in page_text.upper() or "TAXSALE" in page_text.upper():
        tax_status = "sold"
    elif "DELINQUENT" in page_text.upper():
        tax_status = "delinquent"
    else:
        tax_status = "paid"

    # Absentee owner: compare site address vs mailing address (case-insensitive)
    absentee = False
    if property_address and mailing_address:
        # Strip owner name prefix from mailing address if present
        mail_addr = mailing_address
        if owner_name and mail_addr.upper().startswith(owner_name.upper()):
            mail_addr = mail_addr[len(owner_name):].strip()

        # Normalize both addresses for comparison (handle minor typos)
        def normalize_addr(addr: str) -> str:
            """Remove punctuation and extra spaces for comparison."""
            return re.sub(r'[^\w\s]', '', addr.upper()).replace('  ', ' ').strip()

        # Use fuzzy matching to handle minor typos (90% similarity threshold)
        def similarity_ratio(s1: str, s2: str) -> float:
            """Calculate simple character-level similarity (0.0 to 1.0)."""
            if not s1 or not s2:
                return 0.0
            # Count matching characters in same positions
            matches = sum(c1 == c2 for c1, c2 in zip(s1, s2))
            max_len = max(len(s1), len(s2))
            return matches / max_len if max_len > 0 else 0.0

        norm_mail = normalize_addr(mail_addr)
        norm_prop = normalize_addr(property_address)

        # Consider addresses the same if >90% similar (allows for minor typos)
        similarity = similarity_ratio(norm_mail, norm_prop)
        absentee = similarity < 0.90

    return AssessorRecord(
        parcel_id=parcel_id,
        owner_name=owner_name,
        property_address=property_address,
        mailing_address=mailing_address,
        absentee_owner=absentee,
        assessed_value=assessed_value,
        net_taxable_value=net_taxable_value,
        tax_rate=tax_rate,
        total_tax=total_tax,
        tax_status=tax_status,
        property_class=property_class,
        acres=acres,
    )


def fetch_parcel(parcel_id: str, year: int) -> Optional[AssessorRecord]:
    """Fetch and parse a single parcel from DevNetWedge.

    Returns AssessorRecord on success, None on failure.
    Raises ValueError with error message for permanent failures (404).
    """
    import urllib.error

    stripped = strip_parcel_hyphens(parcel_id)
    url = f"{BASE_URL}/{stripped}/{year}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CheasuitsBot/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8")

            if not html or len(html) < 200:
                raise ValueError(f"Empty page for parcel {parcel_id}")

            record = parse_assessor_html(html, parcel_id)
            logger.debug(f"Fetched {parcel_id}: owner={record.owner_name}, "
                         f"value={record.net_taxable_value}, tax={record.tax_status}")
            return record

        except ValueError:
            raise  # permanent failure, don't retry
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise ValueError(f"404: parcel {parcel_id} not found") from e
            if attempt < MAX_RETRIES:
                logger.warning(f"HTTP {e.code} on attempt {attempt}/{MAX_RETRIES} "
                               f"for {parcel_id}. Retrying...")
                time.sleep(1)
            else:
                logger.error(f"HTTP {e.code} for {parcel_id} after "
                             f"{MAX_RETRIES} attempts")
                return None
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for "
                               f"{parcel_id}: {e}. Retrying...")
                time.sleep(1)
            else:
                logger.error(f"Failed to fetch {parcel_id} after {MAX_RETRIES} "
                             f"attempts: {e}")
                return None

    return None


def enrich_from_db(db_path: Path, year: int) -> None:
    """Fetch assessor data for all unenriched records in the database."""
    from src.db.database import (
        get_db, get_unenriched, update_enrichment, set_enrichment_error,
    )

    conn = get_db(db_path)
    rows = get_unenriched(conn)

    if not rows:
        print("No unenriched records found.")
        conn.close()
        return

    print(f"Enriching {len(rows)} records from DevNetWedge (year={year})...")

    enriched = 0
    failed = 0
    tax_sold = 0
    cache: dict[str, Optional[AssessorRecord]] = {}

    for i, row in enumerate(rows):
        parcel_id = row["parcel_id"]

        if parcel_id in cache:
            record = cache[parcel_id]
            if record:
                update_enrichment(conn, row["document_number"], record.to_db_dict())
                enriched += 1
            else:
                set_enrichment_error(conn, row["document_number"], "cached failure")
                failed += 1
            continue

        if i > 0:
            time.sleep(REQUEST_DELAY)

        try:
            record = fetch_parcel(parcel_id, year)
        except ValueError as e:
            logger.warning(f"[{i+1}/{len(rows)}] {parcel_id} -> {e}")
            set_enrichment_error(conn, row["document_number"], str(e))
            cache[parcel_id] = None
            failed += 1
            continue

        cache[parcel_id] = record

        if record:
            update_enrichment(conn, row["document_number"], record.to_db_dict())
            enriched += 1
            if record.tax_status == "sold":
                tax_sold += 1
            logger.info(f"[{i+1}/{len(rows)}] {parcel_id} -> "
                        f"{record.owner_name} (tax: {record.tax_status})")
        else:
            set_enrichment_error(conn, row["document_number"],
                                 "fetch failed after retries")
            failed += 1
            logger.warning(f"[{i+1}/{len(rows)}] {parcel_id} -> FAILED")

    conn.close()

    print(f"\nEnriched {enriched}/{len(rows)} records ({failed} failed)")
    if tax_sold:
        print(f"  Notable: {tax_sold} properties with taxes sold at auction")


def enrich_delinquent_from_db(db_path: Path, year: int) -> None:
    """Fetch assessor data for all unenriched delinquent tax records."""
    from src.db.database import (
        get_db, get_unenriched_delinquent, update_delinquent_enrichment,
        set_delinquent_enrichment_error,
    )

    conn = get_db(db_path)
    rows = get_unenriched_delinquent(conn)

    if not rows:
        print("No unenriched delinquent tax records found.")
        conn.close()
        return

    print(f"Enriching {len(rows)} delinquent tax records from DevNetWedge (year={year})...")

    enriched = 0
    failed = 0
    tax_sold = 0
    cache: dict[str, Optional[AssessorRecord]] = {}

    for i, row in enumerate(rows):
        parcel_id = row["parcel_id"]

        if parcel_id in cache:
            record = cache[parcel_id]
            if record:
                update_delinquent_enrichment(conn, row["id"], record.to_db_dict())
                enriched += 1
            else:
                set_delinquent_enrichment_error(conn, row["id"], "cached failure")
                failed += 1
            continue

        if i > 0:
            time.sleep(REQUEST_DELAY)

        try:
            record = fetch_parcel(parcel_id, year)
        except ValueError as e:
            logger.warning(f"[{i+1}/{len(rows)}] {parcel_id} -> {e}")
            set_delinquent_enrichment_error(conn, row["id"], str(e))
            cache[parcel_id] = None
            failed += 1
            continue

        cache[parcel_id] = record

        if record:
            update_delinquent_enrichment(conn, row["id"], record.to_db_dict())
            enriched += 1
            if record.tax_status == "sold":
                tax_sold += 1
            if (i + 1) % 100 == 0:
                logger.info(f"[{i+1}/{len(rows)}] Progress: {enriched} enriched, {failed} failed")
        else:
            set_delinquent_enrichment_error(conn, row["id"],
                                            "fetch failed after retries")
            failed += 1

    conn.close()

    print(f"\nEnriched {enriched}/{len(rows)} records ({failed} failed)")
    if tax_sold:
        print(f"  Notable: {tax_sold} properties with taxes sold at auction")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich lis pendens records with assessor data from DevNetWedge"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--year", type=int, default=DEFAULT_YEAR,
        help=f"Tax year to query (default: {DEFAULT_YEAR})"
    )
    parser.add_argument(
        "--table", choices=["properties", "delinquent"], default="properties",
        help="Which table to enrich: 'properties' (lis pendens) or 'delinquent' (tax list). Default: properties"
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
        enrich_delinquent_from_db(Path(args.db), args.year)
    else:
        enrich_from_db(Path(args.db), args.year)


if __name__ == "__main__":
    main()
