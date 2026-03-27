"""Comparable Sales Matching & Estimation Engine.

Queries the comparable_sales table for nearby recent sales, scores and ranks
them against a subject property, and produces a comps-based value estimate.

Usage:
    python -m src.enrichment.comps [--db data/cheasuits.db] [--radius 1.5] [--months 6] [-v]
"""

from __future__ import annotations

import argparse
import logging
import math
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

# Scoring weights for comp selection
WEIGHT_SQFT = 0.5
WEIGHT_DISTANCE = 0.3
WEIGHT_RECENCY = 0.2

# Hard filter: reject comps with sqft differing by more than this fraction
SQFT_FILTER_THRESHOLD = 0.30

# Maximum number of comps to use for estimation
MAX_COMPS = 10


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Compute distance in miles between two lat/lng points using Haversine formula."""
    R = 3958.8  # Earth radius in miles

    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def bounding_box(lat: float, lng: float, radius_miles: float) -> tuple:
    """Compute lat/lng bounding box for SQL pre-filter.

    Returns (min_lat, max_lat, min_lng, max_lng).
    """
    lat_delta = radius_miles / 69.0
    lng_delta = radius_miles / (69.0 * math.cos(math.radians(lat)))

    return (
        lat - lat_delta,
        lat + lat_delta,
        lng - lng_delta,
        lng + lng_delta,
    )


# ---------------------------------------------------------------------------
# Comp finding and scoring
# ---------------------------------------------------------------------------

def _passes_sqft_filter(subject: dict, comp: dict) -> bool:
    """Hard filter: reject comps whose sqft differs from subject by > 30%.

    If either side is missing or zero sqft, the filter passes (no data to reject on).
    """
    subj_sqft = subject.get("sqft")
    comp_sqft = comp.get("sqft")
    if not subj_sqft or not comp_sqft:
        return True
    ratio = abs(comp_sqft - subj_sqft) / subj_sqft
    return ratio <= SQFT_FILTER_THRESHOLD


def _score_comp(subject: dict, comp: dict) -> float:
    """Score a comparable sale against the subject property (0-1, higher=better).

    Factors: sqft similarity (50%), proximity (30%), recency (20%).
    """
    # Sqft similarity score
    subj_sqft = subject.get("sqft")
    comp_sqft = comp.get("sqft")
    if subj_sqft and comp_sqft and subj_sqft > 0 and comp_sqft > 0:
        ratio = min(subj_sqft, comp_sqft) / max(subj_sqft, comp_sqft)
        sqft_score = ratio ** 3
    elif subj_sqft and not comp_sqft:
        # Missing comp sqft: apply penalty
        sqft_score = 0.3
    else:
        sqft_score = 0.5  # neutral if subject has no sqft

    # Distance score: 0 at max_dist, 1 at 0 distance
    dist = comp.get("_distance", 0)
    max_dist = 3.0  # miles
    dist_score = max(0, 1.0 - dist / max_dist)

    # Recency score: based on days since sale (0 at 365 days, 1 at 0 days)
    try:
        sale_date = datetime.strptime(comp["sale_date"], "%Y-%m-%d").date()
        days_ago = (date.today() - sale_date).days
    except (ValueError, KeyError):
        days_ago = 365
    recency_score = max(0, 1.0 - days_ago / 365)

    return (
        WEIGHT_SQFT * sqft_score
        + WEIGHT_DISTANCE * dist_score
        + WEIGHT_RECENCY * recency_score
    )


def find_comps(
    conn,
    subject: dict,
    radius_miles: float = 1.5,
    months_back: int = 6,
) -> list[dict]:
    """Find comparable sales for a subject property.

    Returns comps sorted by score (best first), with _distance and _score added.
    """
    from src.db.database import get_comps_near

    lat = subject.get("lat")
    lng = subject.get("lng")
    if lat is None or lng is None:
        return []

    candidates = get_comps_near(conn, lat, lng, radius_miles, months_back)

    # Post-filter with exact Haversine distance and sqft hard filter
    comps = []
    for c in candidates:
        if c.get("lat") is None or c.get("lng") is None:
            continue
        dist = haversine_distance(lat, lng, c["lat"], c["lng"])
        if dist <= radius_miles:
            c["_distance"] = round(dist, 3)
            if not _passes_sqft_filter(subject, c):
                continue
            c["_score"] = round(_score_comp(subject, c), 3)
            comps.append(c)

    # Sort by score descending
    comps.sort(key=lambda x: x["_score"], reverse=True)
    return comps[:MAX_COMPS]


# ---------------------------------------------------------------------------
# Estimation
# ---------------------------------------------------------------------------

def estimate_from_comps(
    subject: dict,
    comps: list[dict],
) -> tuple[float | None, int, str]:
    """Estimate value from comparable sales using score-weighted average.

    Returns (estimated_value, comp_count, confidence).
    Confidence: "high" (3+), "medium" (2), "low" (1), None (0).

    Applies sqft-based price adjustment: scales each comp's sale price by
    subject_sqft / comp_sqft (clamped to 0.7–1.3) before averaging.
    """
    if not comps:
        return (None, 0, "")

    subject_sqft = subject.get("sqft")
    total_weight = 0.0
    weighted_sum = 0.0

    for c in comps:
        price = c["sale_price"]

        # Sqft-based price adjustment
        comp_sqft = c.get("sqft")
        if subject_sqft and comp_sqft and subject_sqft > 0 and comp_sqft > 0:
            sqft_ratio = subject_sqft / comp_sqft
            sqft_ratio = max(0.7, min(1.3, sqft_ratio))
            price = price * sqft_ratio

        # Weight by score (which already factors in sqft similarity, distance, recency)
        weight = c.get("_score", 0.5)
        weighted_sum += price * weight
        total_weight += weight

    if total_weight == 0:
        return (None, 0, "")

    estimate = round(weighted_sum / total_weight, 2)
    count = len(comps)

    if count >= 3:
        confidence = "high"
    elif count == 2:
        confidence = "medium"
    else:
        confidence = "low"

    return (estimate, count, confidence)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def enrich_comps_from_db(
    db_path: Path,
    radius_miles: float = 1.5,
    months_back: int = 6,
) -> None:
    """Loop over properties with lat/lng, find comps, write to property_comps
    and valuations tables."""
    from src.db.database import (
        get_db, insert_property_comps, upsert_valuation,
        apply_market_value_priority,
    )

    conn = get_db(db_path)

    cursor = conn.execute(
        "SELECT * FROM properties WHERE lat IS NOT NULL AND lng IS NOT NULL"
    )
    rows = [dict(row) for row in cursor.fetchall()]

    if not rows:
        print("No geocoded properties found.")
        conn.close()
        return

    comp_count = conn.execute("SELECT COUNT(*) FROM comparable_sales").fetchone()[0]
    if comp_count == 0:
        print("No comparable sales in database. Run comps_redfin or comps_recorder first.")
        conn.close()
        return

    print(f"Computing comps estimates for {len(rows)} properties "
          f"({comp_count} comps in DB, radius={radius_miles}mi, months={months_back})...")

    estimated = 0
    no_comps = 0

    for i, row in enumerate(rows):
        doc_num = row["document_number"]

        comps = find_comps(conn, row, radius_miles, months_back)

        if not comps:
            no_comps += 1
            logger.debug(f"[{i+1}/{len(rows)}] {doc_num} -> no comps found")
            continue

        estimate, count, confidence = estimate_from_comps(row, comps)

        if estimate is None:
            no_comps += 1
            continue

        # Write individual comp matches to property_comps
        comp_rows = []
        subject_sqft = row.get("sqft")
        for c in comps:
            comp_id = c.get("id")
            if comp_id is None:
                continue

            comp_sqft = c.get("sqft")
            if subject_sqft and comp_sqft and subject_sqft > 0 and comp_sqft > 0:
                sqft_ratio = max(0.7, min(1.3, subject_sqft / comp_sqft))
            else:
                sqft_ratio = 1.0

            comp_rows.append({
                "comp_sale_id": comp_id,
                "distance_miles": c.get("_distance"),
                "similarity_score": c.get("_score"),
                "lot_size_ratio": round(sqft_ratio, 4),
                "adjusted_price": round(c["sale_price"] * sqft_ratio, 2),
            })

        if comp_rows:
            insert_property_comps(conn, doc_num, comp_rows)

        # Write summary to valuations table
        upsert_valuation(conn, doc_num, {
            "source": "comps",
            "estimate": estimate,
            "confidence": confidence,
            "comp_count": count,
        })

        # Apply priority rule
        apply_market_value_priority(conn, doc_num)

        estimated += 1
        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} -> ${estimate:,.0f} "
            f"({count} comps, {confidence})"
        )

    conn.close()
    print(f"\nEstimated {estimated}/{len(rows)} properties "
          f"({no_comps} had no comps within {radius_miles}mi)")


def main():
    parser = argparse.ArgumentParser(
        description="Compute comps-based valuations for properties"
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

    enrich_comps_from_db(Path(args.db), args.radius, args.months)


if __name__ == "__main__":
    main()
