"""
HUD/USPS Vacancy Data Ingestion — Census Tract Level

Downloads quarterly vacancy data from the HUD Neighborhood Change Web Map
(NCWM) API for Illinois census tracts and loads it into the pipeline database.

API: https://www.huduser.gov/hudapi/public/uspsncwm
Auth: Bearer token from HUD USER account (set HUD_API_TOKEN env var)

Usage:
    # Fetch latest quarter for Illinois
    python -m src.ingestion.usps_vacancy --db data/cheasuits.db --state 17

    # Fetch specific quarters
    python -m src.ingestion.usps_vacancy --db data/cheasuits.db --state 17 --year 2025 --quarters 1 2 3 4

    # Dry run (no DB write)
    python -m src.ingestion.usps_vacancy --state 17 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.error
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

API_URL = "https://www.huduser.gov/hudapi/public/uspsncwm"
REQUEST_DELAY = 0.5  # seconds between API calls
MAX_RETRIES = 3


@dataclass
class VacancyRecord:
    """Single census tract vacancy record from HUD NCWM API."""

    geoid: str             # 11-digit FIPS (state + county + tract)
    state_fips: str        # 2-digit state FIPS
    county_fips: str       # 3-digit county FIPS
    tract_code: str        # 6-digit tract code
    year: int
    quarter: int
    total_residential: int = 0
    vacant_residential: int = 0
    vacancy_rate_residential: float = 0.0
    no_stat_residential: int = 0
    total_business: int = 0
    vacant_business: int = 0
    vacancy_rate_business: float = 0.0
    no_stat_business: int = 0
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    def to_dict(self) -> dict:
        return asdict(self)


def parse_api_response(results: list[dict], year: int, quarter: int) -> list[VacancyRecord]:
    """Parse the 'results' array from the NCWM API into VacancyRecord objects.

    The API returns fields like TOTAL_RESIDENTIAL_ADDRESSES,
    STV_RESIDENTIAL_ADDRESSES (short-term vacant),
    LTV_RESIDENTIAL_ADDRESSES (long-term vacant), etc.
    We sum STV + LTV to get total vacant.
    """
    records = []
    for r in results:
        geoid = str(r.get("TRACT_ID", ""))
        state_fips = str(r.get("STATE_GEOID", ""))
        county_fips = str(r.get("COUNTY_GEOID", ""))

        # Extract tract code from GEOID (last 6 digits after state+county)
        tract_code = geoid[len(state_fips) + len(county_fips):] if len(geoid) > 5 else ""

        total_res = int(r.get("TOTAL_RESIDENTIAL_ADDRESSES", 0) or 0)
        stv_res = int(r.get("STV_RESIDENTIAL_ADDRESSES", 0) or 0)
        ltv_res = int(r.get("LTV_RESIDENTIAL_ADDRESSES", 0) or 0)
        no_stat_res = int(r.get("NO_STAT_RESIDENTIAL_ADDRESSES", 0) or 0)
        vacant_res = stv_res + ltv_res

        total_bus = int(r.get("TOTAL_BUSINESS_ADDRESSES", 0) or 0)
        stv_bus = int(r.get("STV_BUSINESS_ADDRESSES", 0) or 0)
        ltv_bus = int(r.get("LTV_BUSINESS_ADDRESSES", 0) or 0)
        no_stat_bus = int(r.get("NO_STAT_BUSINESS_ADDRESSES", 0) or 0)
        vacant_bus = stv_bus + ltv_bus

        vac_rate_res = (vacant_res / total_res * 100) if total_res > 0 else 0.0
        vac_rate_bus = (vacant_bus / total_bus * 100) if total_bus > 0 else 0.0

        records.append(VacancyRecord(
            geoid=geoid,
            state_fips=state_fips,
            county_fips=county_fips,
            tract_code=tract_code,
            year=year,
            quarter=quarter,
            total_residential=total_res,
            vacant_residential=vacant_res,
            vacancy_rate_residential=round(vac_rate_res, 2),
            no_stat_residential=no_stat_res,
            total_business=total_bus,
            vacant_business=vacant_bus,
            vacancy_rate_business=round(vac_rate_bus, 2),
            no_stat_business=no_stat_bus,
        ))

    return records


def fetch_state_vacancy(
    state_fips: str,
    year: int,
    quarter: int,
    api_token: str,
) -> list[VacancyRecord]:
    """Fetch all tract-level vacancy data for a state from the HUD NCWM API.

    Args:
        state_fips: 2-digit state FIPS code (e.g. '17' for Illinois).
        year: Data year.
        quarter: Quarter (1-4).
        api_token: HUD API bearer token.

    Returns:
        List of VacancyRecord objects.
    """
    # Map quarter to month: Q1=March, Q2=June, Q3=September, Q4=December
    quarter_month = {1: "03", 2: "06", 3: "09", 4: "12"}
    month = quarter_month.get(quarter, "03")

    params = json.dumps({
        "stateid": state_fips,
        "year_month": f"{year}{month}",
    }).encode("utf-8")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                API_URL,
                data=params,
                headers={
                    "Authorization": f"Bearer {api_token}",
                    "Content-Type": "application/json",
                    "User-Agent": "CheasuitsBot/1.0",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))

            results = body.get("data", {}).get("results", [])
            if not results:
                # Try alternate response structure
                results = body.get("results", [])

            logger.info(f"Fetched {len(results)} tracts for state {state_fips} "
                        f"{year}Q{quarter}")
            return parse_api_response(results, year, quarter)

        except urllib.error.HTTPError as e:
            if e.code == 401:
                logger.error("Invalid API token. Set HUD_API_TOKEN env var.")
                raise ValueError("Invalid HUD API token") from e
            if attempt < MAX_RETRIES:
                logger.warning(f"HTTP {e.code} on attempt {attempt}/{MAX_RETRIES}. Retrying...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"HTTP {e.code} after {MAX_RETRIES} attempts")
                raise
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Failed after {MAX_RETRIES} attempts: {e}")
                raise


def records_to_db(records: list[VacancyRecord], db_path: str | Path) -> int:
    """Write VacancyRecord objects to the usps_vacancy table."""
    from src.db.database import get_db, upsert_vacancy_records

    conn = get_db(db_path)
    db_records = [r.to_dict() for r in records]
    count = upsert_vacancy_records(conn, db_records)
    conn.close()
    logger.info(f"Upserted {count} vacancy records to {db_path}")
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch HUD/USPS vacancy data by census tract and load to database"
    )
    parser.add_argument(
        "--state", type=str, default="17",
        help="State FIPS code (default: 17 = Illinois)"
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Data year (default: current year - 1)"
    )
    parser.add_argument(
        "--quarters", type=int, nargs="+", default=None,
        help="Quarters to fetch (e.g. 1 2 3 4). Default: all four."
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="SQLite database path. When provided, writes records to DB."
    )
    parser.add_argument(
        "--token", type=str, default=None,
        help="HUD API token (default: reads HUD_API_TOKEN env var)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and print summary without writing to DB"
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

    api_token = args.token or os.environ.get("HUD_API_TOKEN", "")
    if not api_token:
        print("Error: Set HUD_API_TOKEN env var or pass --token")
        sys.exit(1)

    year = args.year or (datetime.now().year - 1)
    quarters = args.quarters or [1, 2, 3, 4]

    all_records: list[VacancyRecord] = []

    for q in quarters:
        logger.info(f"Fetching {year}Q{q} for state {args.state}...")
        try:
            records = fetch_state_vacancy(args.state, year, q, api_token)
            all_records.extend(records)
            print(f"  {year}Q{q}: {len(records)} tracts")
        except Exception as e:
            print(f"  {year}Q{q}: FAILED -- {e}")
            continue

        if q != quarters[-1]:
            time.sleep(REQUEST_DELAY)

    if not all_records:
        print("No records fetched.")
        sys.exit(0)

    # Summary
    counties: dict[str, int] = {}
    for r in all_records:
        counties[r.county_fips] = counties.get(r.county_fips, 0) + 1

    avg_vac = sum(r.vacancy_rate_residential for r in all_records) / len(all_records)

    print(f"\n{'='*60}")
    print(f"  USPS Vacancy Records: {len(all_records)} total")
    print(f"  State: {args.state}, Year: {year}, Quarters: {quarters}")
    print(f"  Counties: {len(counties)}")
    print(f"  Avg residential vacancy rate: {avg_vac:.1f}%")
    print(f"{'='*60}")

    if args.dry_run:
        print("\n  [DRY RUN -- no data written]")
        sys.exit(0)

    if args.db:
        count = records_to_db(all_records, args.db)
        print(f"\n  Wrote {count} records to DB: {args.db}")
    else:
        print("\n  No --db specified; records not saved.")


if __name__ == "__main__":
    main()
