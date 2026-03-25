"""
Delinquent Tax List Parser — St. Clair County

Parses the annual Delinquent Real Estate Publication List PDF from the
St. Clair County Treasurer into structured records and loads them into
the pipeline database.

The PDF contains three columns (Parcel, Street, City) with no gridlines.
Column boundaries are determined by the x-position of extracted words:
    - Parcel: x0 < 110  (11-digit parcel number)
    - Street: 110 <= x0 < 300  (street address)
    - City:   x0 >= 300  (municipality name)

Usage:
    # Parse PDF and load to database
    python -m src.ingestion.delinquent_tax --pdf data/raw/delinquent_2025.pdf --db data/cheasuits.db --year 2025

    # Parse PDF and print summary (no DB write)
    python -m src.ingestion.delinquent_tax --pdf data/raw/delinquent_2025.pdf --dry-run

    # As a module
    from src.ingestion.delinquent_tax import parse_delinquent_pdf
    records = parse_delinquent_pdf("path/to/file.pdf", publication_year=2025)
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Column boundary thresholds (x-coordinates from PDF word positions)
# Parcel column ends around x=103, Street starts around x=108, City starts around x=353
STREET_X_MIN = 108
CITY_X_MIN = 300


@dataclass
class DelinquentTaxRecord:
    """Single row from the delinquent tax publication list."""

    parcel_id: str
    street: str = ""
    city: str = ""
    publication_year: int = 0
    source_file: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def formatted_parcel(self) -> str:
        """Format 11-digit parcel as XX-XX-X-XXX-XXX for display."""
        p = self.parcel_id
        if len(p) == 11 and p.isdigit():
            return f"{p[:2]}-{p[2:4]}-{p[4]}-{p[5:8]}-{p[8:11]}"
        return p


def parse_delinquent_pdf(
    pdf_path: str | Path,
    publication_year: int = 0,
) -> list[DelinquentTaxRecord]:
    """Parse the Treasurer's delinquent real estate PDF.

    Uses pdfplumber word-level extraction to correctly separate the three
    columns (Parcel, Street, City) based on x-coordinates.

    Args:
        pdf_path: Path to the PDF file.
        publication_year: Tax year for these delinquencies (e.g., 2025).

    Returns:
        List of DelinquentTaxRecord objects.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber required: pip install pdfplumber")
        raise

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    records: list[DelinquentTaxRecord] = []
    skipped = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            words = page.extract_words()
            if not words:
                continue

            # Group words by their y-position (same line = same top value, within tolerance)
            lines: dict[float, list[dict]] = {}
            for w in words:
                # Round top to nearest pixel to group words on the same line
                y_key = round(w["top"], 0)
                # Find existing line within tolerance
                matched = False
                for existing_y in lines:
                    if abs(existing_y - y_key) < 3:
                        lines[existing_y].append(w)
                        matched = True
                        break
                if not matched:
                    lines[y_key] = [w]

            # Process each line
            for y_key in sorted(lines.keys()):
                line_words = sorted(lines[y_key], key=lambda w: w["x0"])

                # Extract parcel, street, and city by x-position
                parcel_parts = []
                street_parts = []
                city_parts = []

                for w in line_words:
                    x0 = w["x0"]
                    text = w["text"]
                    if x0 < STREET_X_MIN:
                        parcel_parts.append(text)
                    elif x0 < CITY_X_MIN:
                        street_parts.append(text)
                    else:
                        city_parts.append(text)

                parcel = " ".join(parcel_parts).strip()
                street = " ".join(street_parts).strip()
                city = " ".join(city_parts).strip()

                # Skip header rows and non-parcel lines
                if not parcel or parcel.lower() == "parcel":
                    continue
                if not re.match(r"^\d{11}$", parcel):
                    skipped += 1
                    if skipped <= 10:
                        logger.debug(f"Skipped non-parcel line on page {page_num + 1}: {parcel}")
                    continue

                records.append(DelinquentTaxRecord(
                    parcel_id=parcel,
                    street=street,
                    city=city,
                    publication_year=publication_year,
                    source_file=pdf_path.name,
                ))

    logger.info(f"Parsed {len(records)} records from {len(pdf.pages)} pages ({skipped} skipped)")
    return records


def records_to_db(
    records: list[DelinquentTaxRecord],
    db_path: str | Path,
) -> int:
    """Write DelinquentTaxRecord objects to the delinquent_taxes table."""
    from src.db.database import get_db, upsert_delinquent_taxes

    conn = get_db(db_path)
    db_records = [r.to_dict() for r in records]
    count = upsert_delinquent_taxes(conn, db_records)
    conn.close()
    logger.info(f"Upserted {count} delinquent tax records to {db_path}")
    return count


def export_to_csv(
    records: list[DelinquentTaxRecord],
    output_path: str | Path,
) -> Path:
    """Write records to a CSV file."""
    output_path = Path(output_path)

    if not records:
        logger.warning("No records to export")
        return output_path

    fieldnames = list(asdict(records[0]).keys())

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_dict())

    logger.info(f"Exported {len(records)} records to {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Parse St. Clair County delinquent tax PDF and load to database"
    )
    parser.add_argument(
        "--pdf", type=str, required=True,
        help="Path to the delinquent real estate publication PDF"
    )
    parser.add_argument(
        "--year", type=int, required=True,
        help="Publication/tax year (e.g., 2025)"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="SQLite database path. When provided, writes records to DB."
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Output CSV path (optional)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and print summary without writing to DB"
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

    records = parse_delinquent_pdf(args.pdf, publication_year=args.year)

    if not records:
        print("No records found in PDF.")
        sys.exit(0)

    # Summary
    cities = {}
    for r in records:
        cities[r.city] = cities.get(r.city, 0) + 1

    print(f"\n{'='*60}")
    print(f"  Delinquent Tax Records: {len(records)} total")
    print(f"  Publication Year: {args.year}")
    print(f"  Cities: {len(cities)}")
    print(f"{'='*60}")
    print(f"\n  Top 15 cities by count:")
    for city, count in sorted(cities.items(), key=lambda x: -x[1])[:15]:
        print(f"    {city:<30} {count:>5}")

    if args.dry_run:
        print("\n  [DRY RUN — no data written]")
        sys.exit(0)

    # CSV export
    if args.output:
        export_to_csv(records, args.output)
        print(f"\n  Saved CSV: {args.output}")

    # DB export
    if args.db:
        count = records_to_db(records, args.db)
        print(f"  Wrote {count} records to DB: {args.db}")


if __name__ == "__main__":
    main()
