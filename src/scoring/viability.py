"""Deal Viability Scoring Engine.

Calculates a 0-100 composite viability score for each property based on
equity spread, comp confidence, neighborhood vacancy, assessed value range,
time pressure, and owner reachability.

Usage:
    python -m src.scoring.viability --db data/cheasuits.db -v
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"


# ---------------------------------------------------------------------------
# Equity Calculation
# ---------------------------------------------------------------------------

def calculate_equity(
    market_value: float | None,
    mortgage_debt: float | None,
    recorded_liens: float | None,
    delinquent_tax: float | None,
) -> tuple[float | None, float | None, float | None]:
    """Calculate total lien burden, equity spread, and equity ratio.

    Returns:
        (total_lien_burden, equity_spread, equity_ratio) or (None, None, None)
        if market_value is missing.
    """
    if market_value is None or market_value <= 0:
        return (None, None, None)

    total_burden = (mortgage_debt or 0) + (recorded_liens or 0) + (delinquent_tax or 0)
    spread = market_value - total_burden
    ratio = spread / market_value

    return (round(total_burden, 2), round(spread, 2), round(ratio, 4))


# ---------------------------------------------------------------------------
# Individual Scoring Functions
# ---------------------------------------------------------------------------

def score_equity_spread(equity_ratio: float | None) -> int:
    """Score equity spread (0-30 points).

    >=60% -> 30, 40-60% -> 24, 20-40% -> 18, 0-20% -> 10,
    negative -> 0, None -> 0
    """
    if equity_ratio is None:
        return 0
    if equity_ratio < 0:
        return 0
    if equity_ratio >= 0.6:
        return 30
    if equity_ratio >= 0.4:
        return 24
    if equity_ratio >= 0.2:
        return 18
    return 10


def score_comp_confidence(confidence: str | None) -> int:
    """Score comp confidence (0-20 points).

    high -> 20, medium -> 12, low -> 5, None -> 0
    """
    if confidence is None:
        return 0
    confidence = confidence.lower()
    if confidence == "high":
        return 20
    if confidence == "medium":
        return 12
    if confidence == "low":
        return 5
    return 0


def score_neighborhood_vacancy(vacancy_rate: float | None) -> int:
    """Score neighborhood vacancy rate (0-15 points).

    <5% -> 15, 5-8% -> 12, 8-12% -> 8, 12-15% -> 4,
    >15% -> 0, None -> 7 (neutral)
    """
    if vacancy_rate is None:
        return 7
    if vacancy_rate < 5.0:
        return 15
    if vacancy_rate < 8.0:
        return 12
    if vacancy_rate < 12.0:
        return 8
    if vacancy_rate < 15.0:
        return 4
    return 0


def score_buy_box(
    assessed_value: float | None,
    min_val: float = 30000,
    max_val: float = 150000,
) -> int:
    """Score assessed value within buy box (0-15 points).

    In box -> 15, within 20% of edges -> 8, outside -> 0, None -> 0
    """
    if assessed_value is None:
        return 0

    if min_val <= assessed_value <= max_val:
        return 15

    # Check within 20% of edges
    low_edge = min_val * 0.8
    high_edge = max_val * 1.2
    if low_edge <= assessed_value < min_val or max_val < assessed_value <= high_edge:
        return 8

    return 0


def score_time_pressure(recorded_date: str | None) -> int:
    """Score time pressure based on days since distress signal (0-10 points).

    >365d -> 10, 180-365 -> 8, 90-180 -> 6, 30-90 -> 4,
    <30 -> 2, None -> 0
    """
    if not recorded_date:
        return 0

    try:
        rec_date = datetime.strptime(recorded_date[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return 0

    days = (date.today() - rec_date).days
    if days > 365:
        return 10
    if days >= 180:
        return 8
    if days >= 90:
        return 6
    if days >= 30:
        return 4
    return 2


def score_owner_reachability(mailing_address: str | None) -> int:
    """Score owner reachability (0-10 points).

    Has address + IL -> 10, has address + other state -> 6,
    has address -> 5, None -> 0
    """
    if not mailing_address:
        return 0

    addr_upper = mailing_address.upper()
    # Check for Illinois indicators
    if ", IL " in addr_upper or ", IL\n" in addr_upper or addr_upper.endswith(", IL"):
        return 10
    # Check for any other state (look for state abbreviation pattern)
    # If we have a mailing address with a recognizable state that's not IL
    import re
    state_match = re.search(r',\s*([A-Z]{2})\s+\d{5}', addr_upper)
    if state_match:
        state = state_match.group(1)
        if state != "IL":
            return 6
    # Has address but state unknown
    return 5


# ---------------------------------------------------------------------------
# Composite Scorer
# ---------------------------------------------------------------------------

def calculate_viability_score(property_row: dict, vacancy_rate: float | None) -> dict:
    """Calculate full viability score for a property.

    Args:
        property_row: Dict with all columns from the properties table.
        vacancy_rate: Residential vacancy rate for the property's census tract.

    Returns:
        Dict with total_lien_burden, equity_spread, equity_ratio,
        viability_score (0-100), and viability_details (JSON string).
    """
    market_value = property_row.get("estimated_market_value")
    mortgage_debt = property_row.get("total_mortgage_debt")
    recorded_liens = property_row.get("total_recorded_liens")

    # Equity calculation (delinquent_tax comes from caller via property_row)
    delinquent_tax = property_row.get("_delinquent_tax")
    total_burden, spread, ratio = calculate_equity(
        market_value, mortgage_debt, recorded_liens, delinquent_tax,
    )

    # Score each factor
    equity_pts = score_equity_spread(ratio)
    comps_pts = score_comp_confidence(property_row.get("_comp_confidence"))
    vacancy_pts = score_neighborhood_vacancy(vacancy_rate)
    buybox_pts = score_buy_box(property_row.get("assessed_value"))
    time_pts = score_time_pressure(property_row.get("recorded_date"))
    reach_pts = score_owner_reachability(property_row.get("mailing_address"))

    total_score = equity_pts + comps_pts + vacancy_pts + buybox_pts + time_pts + reach_pts

    details = {
        "equity_spread": equity_pts,
        "comp_confidence": comps_pts,
        "neighborhood_vacancy": vacancy_pts,
        "buy_box": buybox_pts,
        "time_pressure": time_pts,
        "owner_reachability": reach_pts,
    }

    return {
        "total_lien_burden": total_burden,
        "equity_spread": spread,
        "equity_ratio": ratio,
        "viability_score": total_score,
        "viability_details": json.dumps(details),
    }


# ---------------------------------------------------------------------------
# Batch Scorer
# ---------------------------------------------------------------------------

def score_all_properties(db_path: Path, rescore: bool = False) -> None:
    """Score all properties that have been valued and mortgage-enriched.

    Queries properties with estimated_market_value, joins delinquent tax
    and vacancy data, calculates viability scores, and writes results.

    Args:
        rescore: If True, re-score properties that already have viability scores.
    """
    from src.db.database import get_db, update_viability

    conn = get_db(db_path)

    # Get properties that have been valued but not yet scored
    query = "SELECT * FROM properties WHERE estimated_market_value IS NOT NULL"
    if not rescore:
        query += " AND viability_scored_at IS NULL"
    cursor = conn.execute(query)
    rows = [dict(row) for row in cursor.fetchall()]

    if not rows:
        print("No valued properties to score.")
        conn.close()
        return

    print(f"Scoring {len(rows)} properties...")

    scored = 0
    errors = 0

    for i, row in enumerate(rows):
        try:
            parcel_id = row["parcel_id"]

            # Get delinquent tax amount via join (strip hyphens)
            parcel_stripped = parcel_id.replace("-", "")
            dt_cursor = conn.execute(
                "SELECT total_tax FROM delinquent_taxes "
                "WHERE parcel_id = ? ORDER BY publication_year DESC LIMIT 1",
                (parcel_stripped,),
            )
            dt_row = dt_cursor.fetchone()
            delinquent_tax = dict(dt_row)["total_tax"] if dt_row else None

            # Get vacancy rate via census_tract -> usps_vacancy join
            vacancy_rate = None
            census_tract = row.get("census_tract")
            if census_tract:
                vac_cursor = conn.execute(
                    "SELECT vacancy_rate_residential FROM usps_vacancy "
                    "WHERE geoid = ? ORDER BY year DESC, quarter DESC LIMIT 1",
                    (census_tract,),
                )
                vac_row = vac_cursor.fetchone()
                if vac_row:
                    vacancy_rate = dict(vac_row)["vacancy_rate_residential"]

            # Get comp confidence from valuations table
            comp_val_cursor = conn.execute(
                "SELECT confidence FROM valuations "
                "WHERE document_number = ? AND source = 'comps'",
                (row["document_number"],),
            )
            comp_val_row = comp_val_cursor.fetchone()
            row["_comp_confidence"] = dict(comp_val_row)["confidence"] if comp_val_row else None

            # Inject delinquent tax into the row dict for the scorer
            row["_delinquent_tax"] = delinquent_tax

            # Calculate viability score
            result = calculate_viability_score(row, vacancy_rate)

            # Write to DB
            update_viability(conn, row["document_number"], result)

            equity_str = (
                f"${result['equity_spread']:,.0f}"
                if result["equity_spread"] is not None
                else "N/A"
            )
            logger.info(
                f"[{i+1}/{len(rows)}] {parcel_id}: "
                f"score={result['viability_score']}, equity={equity_str}"
            )

            scored += 1

        except Exception as e:
            logger.warning(f"[{i+1}/{len(rows)}] {row.get('parcel_id', '?')}: error — {e}")
            errors += 1

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(rows)} ({scored} scored, {errors} errors)")

    conn.close()
    print(f"\nDone! Scored {scored}/{len(rows)} properties ({errors} errors)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Calculate deal viability scores for properties"
    )
    parser.add_argument(
        "--db", type=Path, default=DEFAULT_DB,
        help="Database path",
    )
    parser.add_argument(
        "--rescore", action="store_true",
        help="Re-score properties that already have viability scores",
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

    score_all_properties(args.db, rescore=args.rescore)


if __name__ == "__main__":
    main()
