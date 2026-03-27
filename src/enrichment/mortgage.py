"""Mortgage Amount Lookup — Fidlar County Recorder API.

Fetches mortgage recordings and releases for a parcel from the St. Clair
County Recorder (Fidlar AVA Search API) to estimate the current outstanding
mortgage balance.

Strategy:
    1. Pull all MORTGAGE (code 112) documents for the parcel
    2. Pull all RELEASE OF MORTGAGE (code 157) documents
    3. Match releases to mortgages by lender name + chronology
    4. The most recent unmatched mortgage is presumed active
    5. Extract the original amount from ConsiderationAmount or Notes field
    6. Optionally amortize to estimate current balance

Usage:
    python -m src.enrichment.mortgage --parcel 01-35-0-205-009
    python -m src.enrichment.mortgage --db data/cheasuits.db -v
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from src.enrichment._fidlar import (
    get_token as _get_token,
    search_documents as _search_documents,
    parse_date as _parse_date,
    parse_amount as _parse_amount,
    normalize_party_name as _normalize_lender,
    REQUEST_DELAY,
)

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

DOC_TYPE_MORTGAGE = "112"
DOC_TYPE_RELEASE = "157"


# ---------------------------------------------------------------------------
# Data Model
# ---------------------------------------------------------------------------

@dataclass
class MortgageRecord:
    """A single mortgage recording from the county recorder."""
    document_number: str
    recorded_date: str          # YYYY-MM-DD
    borrower: str               # Party1
    lender: str                 # Party2
    amount: float | None        # Original mortgage amount
    parcel_id: str
    is_released: bool = False   # True if a matching release was found


# ---------------------------------------------------------------------------
# Core Logic
# ---------------------------------------------------------------------------

def _get_last_sale_date(parcel_id: str) -> str | None:
    """Get the most recent arm's-length sale date from DevNetWedge.

    Returns YYYY-MM-DD string or None if no sales found.
    """
    from src.enrichment.comps_recorder import fetch_parcel_sales

    sales = fetch_parcel_sales(parcel_id)
    if not sales:
        return None

    # Sales come back unsorted — find the most recent
    dates = [s["sale_date"] for s in sales if s.get("sale_date")]
    return max(dates) if dates else None


def fetch_mortgage_history(parcel_id: str) -> list[MortgageRecord]:
    """Fetch all mortgage recordings for a parcel and mark released ones.

    A mortgage is considered released if:
    1. A formal RELEASE OF MORTGAGE document matches it, OR
    2. A property sale occurred after the mortgage was recorded (the
       sale satisfies the mortgage at closing — standard practice)

    Returns list of MortgageRecord sorted by date descending (newest first).
    """
    # Fetch mortgages and releases
    mortgage_docs = _search_documents(parcel_id, DOC_TYPE_MORTGAGE, "MORTGAGE")
    time.sleep(REQUEST_DELAY)
    release_docs = _search_documents(parcel_id, DOC_TYPE_RELEASE, "RELEASE OF MORTGAGE")

    # Parse mortgages
    mortgages = []
    for doc in mortgage_docs:
        mortgages.append(MortgageRecord(
            document_number=doc.get("DocumentName", ""),
            recorded_date=_parse_date(doc.get("RecordedDateTime", "")),
            borrower=(doc.get("Party1") or "").strip(),
            lender=(doc.get("Party2") or "").strip(),
            amount=_parse_amount(doc),
            parcel_id=parcel_id,
        ))

    # Parse releases — Party1 on release = lender, Party2 = borrower
    releases = []
    for doc in release_docs:
        releases.append({
            "document_number": doc.get("DocumentName", ""),
            "recorded_date": _parse_date(doc.get("RecordedDateTime", "")),
            "lender": (doc.get("Party1") or "").strip(),
            "borrower": (doc.get("Party2") or "").strip(),
            "associated_docs": [d.get("Name", "") for d in doc.get("AssociatedDocuments", [])],
        })

    # Match formal releases to mortgages
    _match_releases(mortgages, releases)

    # Mark pre-sale mortgages as released (sale satisfies mortgage at closing)
    time.sleep(REQUEST_DELAY)
    last_sale = _get_last_sale_date(parcel_id)
    if last_sale:
        for mortgage in mortgages:
            if not mortgage.is_released and mortgage.recorded_date < last_sale:
                mortgage.is_released = True
                logger.debug(
                    f"Mortgage {mortgage.document_number} marked released "
                    f"(predates sale on {last_sale})"
                )

    # Sort newest first
    mortgages.sort(key=lambda m: m.recorded_date, reverse=True)
    return mortgages


def _match_releases(
    mortgages: list[MortgageRecord],
    releases: list[dict],
) -> None:
    """Mark mortgages as released by matching against release documents.

    Matching strategy (in priority order):
    1. AssociatedDocuments on the release references the mortgage doc number
    2. Lender name match + release date is after mortgage date
    """
    # Build a set of mortgage doc numbers referenced by releases
    released_by_assoc: set[str] = set()
    for rel in releases:
        for assoc_doc in rel["associated_docs"]:
            released_by_assoc.add(assoc_doc)

    # First pass: match by associated document reference
    for mortgage in mortgages:
        if mortgage.document_number in released_by_assoc:
            mortgage.is_released = True

    # Second pass: match by lender name for unmatched mortgages
    unmatched_releases = [
        r for r in releases
        if not any(
            m.document_number in r["associated_docs"]
            for m in mortgages
            if m.is_released
        )
    ]

    for mortgage in mortgages:
        if mortgage.is_released:
            continue

        mort_lender = _normalize_lender(mortgage.lender)
        for rel in unmatched_releases:
            rel_lender = _normalize_lender(rel["lender"])
            # Lender on release matches lender on mortgage, and release is after mortgage
            if (rel_lender and mort_lender and
                (rel_lender in mort_lender or mort_lender in rel_lender) and
                rel["recorded_date"] >= mortgage.recorded_date):
                mortgage.is_released = True
                unmatched_releases.remove(rel)
                break


def get_active_mortgages(parcel_id: str) -> list[MortgageRecord]:
    """Return all active (unreleased) mortgages with known amounts for a parcel.

    Sorted by date descending (newest first).
    """
    history = fetch_mortgage_history(parcel_id)
    return [m for m in history if not m.is_released and m.amount and m.amount > 0]


def get_total_mortgage_debt(parcel_id: str) -> float | None:
    """Return total outstanding mortgage debt for a parcel.

    Sums all active mortgage amounts. Returns None if no active mortgages found.
    """
    active = get_active_mortgages(parcel_id)
    if not active:
        return None
    return sum(m.amount for m in active)


# ---------------------------------------------------------------------------
# Batch Enrichment
# ---------------------------------------------------------------------------

def enrich_mortgages_from_db(db_path: Path) -> int:
    """Batch enrich all properties with mortgage data from Fidlar.

    Queries the Fidlar API for each property's parcel, determines active
    mortgages, and stores the results in the properties table.

    Returns number of properties enriched.
    """
    from src.db.database import (
        get_db, get_unmortgaged_properties, update_mortgage, set_mortgage_error,
    )

    conn = get_db(db_path)
    rows = get_unmortgaged_properties(conn)

    if not rows:
        print("No properties need mortgage enrichment.")
        conn.close()
        return 0

    print(f"Enriching {len(rows)} properties with mortgage data...")

    enriched = 0
    errors = 0

    for i, row in enumerate(rows):
        parcel_id = row["parcel_id"]

        if i > 0:
            time.sleep(REQUEST_DELAY)

        try:
            active = get_active_mortgages(parcel_id)

            if active:
                primary = active[0]  # most recent
                total_debt = sum(m.amount for m in active)

                update_mortgage(conn, row["document_number"], {
                    "mortgage_amount": primary.amount,
                    "mortgage_date": primary.recorded_date,
                    "mortgage_lender": primary.lender,
                    "total_mortgage_debt": total_debt,
                    "mortgage_count": len(active),
                    "mortgage_source": "fidlar_recorder",
                })
                logger.info(
                    f"[{i+1}/{len(rows)}] {parcel_id}: "
                    f"${total_debt:,.0f} ({len(active)} active)"
                )
            else:
                # No active mortgage — record as enriched with zero debt
                update_mortgage(conn, row["document_number"], {
                    "mortgage_amount": 0,
                    "mortgage_date": None,
                    "mortgage_lender": None,
                    "total_mortgage_debt": 0,
                    "mortgage_count": 0,
                    "mortgage_source": "fidlar_recorder",
                })
                logger.info(f"[{i+1}/{len(rows)}] {parcel_id}: no active mortgage")

            enriched += 1

        except Exception as e:
            logger.warning(f"[{i+1}/{len(rows)}] {parcel_id}: error — {e}")
            set_mortgage_error(conn, row["document_number"], str(e))
            errors += 1

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i+1}/{len(rows)} ({enriched} enriched, {errors} errors)")

    conn.close()
    print(f"\nDone! Enriched {enriched}/{len(rows)} properties ({errors} errors)")
    return enriched


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Look up mortgage recordings for a parcel"
    )
    parser.add_argument(
        "--parcel", type=str,
        help="Single parcel ID to look up (e.g., 01-35-0-205-009)",
    )
    parser.add_argument(
        "--db", type=Path, default=DEFAULT_DB,
        help="Database path for batch mode",
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

    if args.parcel:
        print(f"Looking up mortgage history for {args.parcel}...\n")
        history = fetch_mortgage_history(args.parcel)

        if not history:
            print("No mortgage recordings found.")
            return

        for m in history:
            status = "RELEASED" if m.is_released else "ACTIVE"
            amount_str = f"${m.amount:,.2f}" if m.amount else "unknown"
            print(
                f"  [{status}] {m.recorded_date}  {amount_str}  "
                f"{m.borrower} → {m.lender}  (doc {m.document_number})"
            )

        active = [m for m in history if not m.is_released and m.amount]
        if active:
            total = sum(m.amount for m in active)
            print(f"\nTotal active mortgage debt: ${total:,.2f} "
                  f"({len(active)} mortgage{'s' if len(active) > 1 else ''})")
        else:
            print("\nNo active mortgage with known amount found.")
    else:
        enrich_mortgages_from_db(args.db)


if __name__ == "__main__":
    main()
