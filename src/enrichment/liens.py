"""Lien Enrichment — Fidlar County Recorder API.

Fetches federal tax liens, state tax liens, and judgment liens for a parcel
from the St. Clair County Recorder (Fidlar AVA Search API) to determine
active lien burden.

Unlike mortgages, liens are NOT satisfied by property sales. Federal tax
liens and judgments survive transfers, so the sale-date release heuristic
from mortgage.py does NOT apply here.

Usage:
    python -m src.enrichment.liens --parcel 01-35-0-205-009
    python -m src.enrichment.liens --db data/cheasuits.db -v
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from src.enrichment._fidlar import (
    search_documents,
    parse_date,
    parse_amount,
    normalize_party_name,
    REQUEST_DELAY,
)

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"


# ---------------------------------------------------------------------------
# Lien Type Configuration
# ---------------------------------------------------------------------------

LIEN_TYPES = {
    "federal_tax": {
        "search_codes": [
            ("86", "FEDERAL TAX LIEN"),
            ("85", "FEDERAL TAX LIEN ESTATE"),
        ],
        "release_codes": [
            ("141", "RELEASE OF FEDERAL TAX LIEN"),
        ],
    },
    "state_tax": {
        "search_codes": [
            ("92", "ILLINOIS TAX LIEN"),
            ("248", "STATE LIEN"),
        ],
        "release_codes": [
            ("155", "RELEASE OF ILLINOIS LIEN"),
        ],
    },
    "judgment": {
        "search_codes": [
            ("94", "JUDGMENT"),
            ("106", "MEMORANDUM OF JUDGMENT"),
        ],
        "release_codes": [
            ("153", "RELEASE OF JUDGEMENT"),
        ],
    },
}


# ---------------------------------------------------------------------------
# Data Model
# ---------------------------------------------------------------------------

@dataclass
class LienRecord:
    """A single lien recording from the county recorder."""
    document_number: str
    lien_type: str              # "federal_tax", "state_tax", "judgment"
    recorded_date: str          # YYYY-MM-DD
    creditor: str               # Party2 (lien holder)
    debtor: str                 # Party1 (property owner)
    amount: float | None
    parcel_id: str
    is_released: bool = False


# ---------------------------------------------------------------------------
# Release Matching
# ---------------------------------------------------------------------------

def _match_releases(
    liens: list[LienRecord],
    releases: list[dict],
) -> None:
    """Mark liens as released by matching against release documents.

    Matching strategy (in priority order):
    1. AssociatedDocuments on the release references the lien doc number
    2. Creditor name match + release date is after lien date

    Unlike mortgages, liens are NOT released by property sales.
    """
    # Build a set of lien doc numbers referenced by releases
    released_by_assoc: set[str] = set()
    for rel in releases:
        for assoc_doc in rel["associated_docs"]:
            released_by_assoc.add(assoc_doc)

    # First pass: match by associated document reference
    for lien in liens:
        if lien.document_number in released_by_assoc:
            lien.is_released = True

    # Second pass: match by creditor name for unmatched liens
    unmatched_releases = [
        r for r in releases
        if not any(
            l.document_number in r["associated_docs"]
            for l in liens
            if l.is_released
        )
    ]

    for lien in liens:
        if lien.is_released:
            continue

        lien_creditor = normalize_party_name(lien.creditor)
        for rel in unmatched_releases:
            rel_creditor = normalize_party_name(rel["creditor"])
            # Creditor on release matches creditor on lien, and release is after lien
            if (rel_creditor and lien_creditor and
                (rel_creditor in lien_creditor or lien_creditor in rel_creditor) and
                rel["recorded_date"] >= lien.recorded_date):
                lien.is_released = True
                unmatched_releases.remove(rel)
                break


# ---------------------------------------------------------------------------
# Core Logic
# ---------------------------------------------------------------------------

def fetch_lien_history(parcel_id: str) -> list[LienRecord]:
    """Fetch all lien recordings for a parcel and mark released ones.

    Queries all lien types (federal tax, state tax, judgment) and their
    corresponding release documents, then matches releases to liens.

    Returns list of LienRecord sorted by date descending (newest first).
    """
    all_liens: list[LienRecord] = []
    all_releases: list[dict] = []

    for lien_type, config in LIEN_TYPES.items():
        # Fetch lien documents
        for code, name in config["search_codes"]:
            docs = search_documents(parcel_id, code, name)
            time.sleep(REQUEST_DELAY)

            for doc in docs:
                all_liens.append(LienRecord(
                    document_number=doc.get("DocumentName", ""),
                    lien_type=lien_type,
                    recorded_date=parse_date(doc.get("RecordedDateTime", "")),
                    creditor=(doc.get("Party2") or "").strip(),
                    debtor=(doc.get("Party1") or "").strip(),
                    amount=parse_amount(doc),
                    parcel_id=parcel_id,
                ))

        # Fetch release documents
        for code, name in config["release_codes"]:
            docs = search_documents(parcel_id, code, name)
            time.sleep(REQUEST_DELAY)

            for doc in docs:
                all_releases.append({
                    "document_number": doc.get("DocumentName", ""),
                    "recorded_date": parse_date(doc.get("RecordedDateTime", "")),
                    "creditor": (doc.get("Party1") or "").strip(),
                    "debtor": (doc.get("Party2") or "").strip(),
                    "associated_docs": [
                        d.get("Name", "") for d in doc.get("AssociatedDocuments", [])
                    ],
                })

    # Match releases to liens
    _match_releases(all_liens, all_releases)

    # Sort newest first
    all_liens.sort(key=lambda l: l.recorded_date, reverse=True)
    return all_liens


def get_active_liens(parcel_id: str) -> list[LienRecord]:
    """Return all active (unreleased) liens with known amounts for a parcel.

    Sorted by date descending (newest first).
    """
    history = fetch_lien_history(parcel_id)
    return [l for l in history if not l.is_released and l.amount and l.amount > 0]


def get_total_lien_amount(parcel_id: str) -> float | None:
    """Return total active lien amount for a parcel.

    Sums all active lien amounts. Returns None if no active liens found.
    """
    active = get_active_liens(parcel_id)
    if not active:
        return None
    return sum(l.amount for l in active)


# ---------------------------------------------------------------------------
# Batch Enrichment
# ---------------------------------------------------------------------------

def enrich_liens_from_db(db_path: Path) -> int:
    """Batch enrich all properties with lien data from Fidlar.

    Queries the Fidlar API for each property's parcel, determines active
    liens, and stores the results in the properties table.

    Returns number of properties enriched.
    """
    from src.db.database import (
        get_db, get_unlienned_properties, update_liens, set_lien_error,
    )

    conn = get_db(db_path)
    rows = get_unlienned_properties(conn)

    if not rows:
        print("No properties need lien enrichment.")
        conn.close()
        return 0

    print(f"Enriching {len(rows)} properties with lien data...")

    enriched = 0
    errors = 0

    for i, row in enumerate(rows):
        parcel_id = row["parcel_id"]

        if i > 0:
            time.sleep(REQUEST_DELAY)

        try:
            history = fetch_lien_history(parcel_id)
            active = [l for l in history if not l.is_released and l.amount and l.amount > 0]

            # Aggregate by lien type
            federal_total = sum(
                l.amount for l in active if l.lien_type == "federal_tax"
            )
            state_total = sum(
                l.amount for l in active if l.lien_type == "state_tax"
            )
            judgment_total = sum(
                l.amount for l in active if l.lien_type == "judgment"
            )
            total_liens = federal_total + state_total + judgment_total

            update_liens(conn, row["document_number"], {
                "federal_tax_lien_amount": federal_total if federal_total else None,
                "state_tax_lien_amount": state_total if state_total else None,
                "judgment_lien_amount": judgment_total if judgment_total else None,
                "total_recorded_liens": total_liens if total_liens else 0,
                "lien_count": len(active),
            })

            if active:
                logger.info(
                    f"[{i+1}/{len(rows)}] {parcel_id}: "
                    f"${total_liens:,.0f} ({len(active)} active liens)"
                )
            else:
                logger.info(f"[{i+1}/{len(rows)}] {parcel_id}: no active liens")

            enriched += 1

        except Exception as e:
            logger.warning(f"[{i+1}/{len(rows)}] {parcel_id}: error — {e}")
            set_lien_error(conn, row["document_number"], str(e))
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
        description="Look up lien recordings for a parcel"
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
        print(f"Looking up lien history for {args.parcel}...\n")
        history = fetch_lien_history(args.parcel)

        if not history:
            print("No lien recordings found.")
            return

        for l in history:
            status = "RELEASED" if l.is_released else "ACTIVE"
            amount_str = f"${l.amount:,.2f}" if l.amount else "unknown"
            print(
                f"  [{status}] {l.recorded_date}  {l.lien_type:<12}  {amount_str}  "
                f"{l.debtor} → {l.creditor}  (doc {l.document_number})"
            )

        active = [l for l in history if not l.is_released and l.amount]
        if active:
            total = sum(l.amount for l in active)
            print(f"\nTotal active lien amount: ${total:,.2f} "
                  f"({len(active)} lien{'s' if len(active) > 1 else ''})")
        else:
            print("\nNo active liens with known amounts found.")
    else:
        enrich_liens_from_db(args.db)


if __name__ == "__main__":
    main()
