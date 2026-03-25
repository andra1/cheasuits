# USPS Vacancy Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest HUD/USPS vacancy data per census tract for Illinois via the NCWM API, store it in a dedicated `usps_vacancy` table, and enrich existing properties with census tract FIPS codes via the Census Bureau geocoder so they can be joined against vacancy rates.

**Architecture:** Two new modules following existing project patterns — `src/ingestion/usps_vacancy.py` downloads tract-level vacancy data from the HUD NCWM API and stores it in a new `usps_vacancy` SQLite table; `src/enrichment/census_tract.py` maps existing geocoded properties to census tract FIPS codes via the Census Bureau geocoder API so they can be joined to vacancy data.

**Tech Stack:** Python 3.11+, `urllib.request` (matching existing HTTP pattern), SQLite (existing DB layer), `pytest` for tests.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/db/database.py` | Modify | Add `usps_vacancy` table schema, migration, CRUD helpers, census_tract column migration for both existing tables |
| `src/ingestion/usps_vacancy.py` | Create | HUD NCWM API client, data parsing, CLI entrypoint |
| `src/enrichment/census_tract.py` | Create | Census Bureau geocoder client, tract enrichment for both tables |
| `tests/test_usps_vacancy.py` | Create | Tests for ingestion module |
| `tests/test_census_tract.py` | Create | Tests for enrichment module |
| `tests/test_database.py` | Modify | Add tests for new DB helpers |
| `requirements.txt` | No change | No new dependencies needed (stdlib only) |

---

## Task 1: Add `usps_vacancy` table and DB helpers

**Files:**
- Modify: `src/db/database.py:10-88` (SCHEMA string), `src/db/database.py:97-130` (get_db function)
- Test: `tests/test_database.py`

- [ ] **Step 1: Write failing tests for usps_vacancy table creation and CRUD**

Add to `tests/test_database.py`:

```python
from src.db.database import (
    # ... existing imports ...
    upsert_vacancy_records,
    get_vacancy_by_tract,
    get_vacancy_summary,
)

SAMPLE_VACANCY = {
    "geoid": "17163000100",
    "state_fips": "17",
    "county_fips": "163",
    "tract_code": "000100",
    "year": 2025,
    "quarter": 1,
    "total_residential": 500,
    "vacant_residential": 25,
    "vacancy_rate_residential": 5.0,
    "no_stat_residential": 10,
    "total_business": 50,
    "vacant_business": 5,
    "vacancy_rate_business": 10.0,
    "no_stat_business": 2,
    "scraped_at": "2026-03-25T10:00:00",
}


class TestUspsVacancyTable:
    def test_table_created(self, db):
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='usps_vacancy'"
        )
        assert cursor.fetchone() is not None

    def test_upsert_insert(self, db):
        count = upsert_vacancy_records(db, [SAMPLE_VACANCY])
        assert count == 1
        rows = get_vacancy_by_tract(db, "17163000100")
        assert len(rows) == 1
        assert rows[0]["total_residential"] == 500
        assert rows[0]["vacant_residential"] == 25

    def test_upsert_updates_on_conflict(self, db):
        upsert_vacancy_records(db, [SAMPLE_VACANCY])
        updated = {**SAMPLE_VACANCY, "vacant_residential": 30, "vacancy_rate_residential": 6.0}
        upsert_vacancy_records(db, [updated])
        rows = get_vacancy_by_tract(db, "17163000100")
        assert len(rows) == 1
        assert rows[0]["vacant_residential"] == 30

    def test_upsert_empty_list(self, db):
        count = upsert_vacancy_records(db, [])
        assert count == 0

    def test_get_vacancy_by_tract(self, db):
        rec_q1 = {**SAMPLE_VACANCY}
        rec_q2 = {**SAMPLE_VACANCY, "quarter": 2, "vacant_residential": 30}
        upsert_vacancy_records(db, [rec_q1, rec_q2])
        rows = get_vacancy_by_tract(db, "17163000100")
        assert len(rows) == 2

    def test_get_vacancy_summary(self, db):
        upsert_vacancy_records(db, [SAMPLE_VACANCY])
        summary = get_vacancy_summary(db, state_fips="17")
        assert len(summary) >= 1
        assert summary[0]["geoid"] == "17163000100"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py::TestUspsVacancyTable -v`
Expected: FAIL with `ImportError` (functions don't exist yet)

- [ ] **Step 3: Add usps_vacancy table to SCHEMA and create DB helpers**

In `src/db/database.py`, append to the SCHEMA string (after line 87, before the closing `"""`):

```python
CREATE TABLE IF NOT EXISTS usps_vacancy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    geoid TEXT NOT NULL,
    state_fips TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    tract_code TEXT NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    total_residential INTEGER,
    vacant_residential INTEGER,
    vacancy_rate_residential REAL,
    no_stat_residential INTEGER,
    total_business INTEGER,
    vacant_business INTEGER,
    vacancy_rate_business REAL,
    no_stat_business INTEGER,
    scraped_at TEXT,
    UNIQUE(geoid, year, quarter)
);

CREATE INDEX IF NOT EXISTS idx_vac_geoid ON usps_vacancy(geoid);
CREATE INDEX IF NOT EXISTS idx_vac_state ON usps_vacancy(state_fips);
CREATE INDEX IF NOT EXISTS idx_vac_year_qtr ON usps_vacancy(year, quarter);
```

Add these functions after `get_delinquent_overlap()` (after line 386):

```python
# ---------------------------------------------------------------------------
# USPS vacancy table helpers
# ---------------------------------------------------------------------------

def upsert_vacancy_records(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update USPS vacancy records. Keyed on (geoid, year, quarter).

    Returns number of records upserted.
    """
    if not records:
        return 0

    cols = [
        "state_fips", "county_fips", "tract_code",
        "total_residential", "vacant_residential", "vacancy_rate_residential",
        "no_stat_residential", "total_business", "vacant_business",
        "vacancy_rate_business", "no_stat_business", "scraped_at",
    ]
    update_clause = ", ".join(f"{col} = excluded.{col}" for col in cols)

    sql = f"""
        INSERT INTO usps_vacancy (geoid, year, quarter, {", ".join(cols)})
        VALUES (:geoid, :year, :quarter, {", ".join(":" + c for c in cols)})
        ON CONFLICT(geoid, year, quarter) DO UPDATE SET {update_clause}
    """

    for record in records:
        params = {
            "geoid": record["geoid"],
            "year": record["year"],
            "quarter": record["quarter"],
        }
        for col in cols:
            params[col] = record.get(col, "")
        conn.execute(sql, params)

    conn.commit()
    return len(records)


def get_vacancy_by_tract(conn: sqlite3.Connection, geoid: str) -> list[dict]:
    """Get all vacancy records for a specific census tract GEOID."""
    cursor = conn.execute(
        "SELECT * FROM usps_vacancy WHERE geoid = ? ORDER BY year, quarter",
        (geoid,),
    )
    return [dict(row) for row in cursor.fetchall()]


def get_vacancy_summary(
    conn: sqlite3.Connection,
    state_fips: str | None = None,
    county_fips: str | None = None,
    year: int | None = None,
    quarter: int | None = None,
) -> list[dict]:
    """Get vacancy summary with optional filters."""
    conditions = []
    params = []
    if state_fips:
        conditions.append("state_fips = ?")
        params.append(state_fips)
    if county_fips:
        conditions.append("county_fips = ?")
        params.append(county_fips)
    if year:
        conditions.append("year = ?")
        params.append(year)
    if quarter:
        conditions.append("quarter = ?")
        params.append(quarter)

    where = " AND ".join(conditions) if conditions else "1=1"
    cursor = conn.execute(
        f"SELECT * FROM usps_vacancy WHERE {where} ORDER BY geoid, year, quarter",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py::TestUspsVacancyTable -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy
git add src/db/database.py tests/test_database.py
git commit -m "feat: add usps_vacancy table schema and CRUD helpers"
```

---

## Task 2: Add census_tract columns to existing tables

**Files:**
- Modify: `src/db/database.py:97-130` (get_db migration section)
- Test: `tests/test_database.py`

- [ ] **Step 1: Write failing tests for census_tract column migration**

Add to `tests/test_database.py`:

```python
class TestCensusTractMigration:
    def test_properties_has_census_tract_column(self, db):
        cursor = db.execute("PRAGMA table_info(properties)")
        columns = [row[1] for row in cursor.fetchall()]
        assert "census_tract" in columns
        assert "tract_enriched_at" in columns

    def test_delinquent_has_census_tract_column(self, db):
        cursor = db.execute("PRAGMA table_info(delinquent_taxes)")
        columns = [row[1] for row in cursor.fetchall()]
        assert "census_tract" in columns
        assert "tract_enriched_at" in columns
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py::TestCensusTractMigration -v`
Expected: FAIL — columns don't exist yet

- [ ] **Step 3: Add migration for census_tract columns**

In `src/db/database.py`, in the `get_db()` function, after the valuation migration block (after line 128), add:

```python
    # Migrate: add census tract columns to properties and delinquent_taxes
    _TRACT_MIGRATIONS = [
        "ALTER TABLE properties ADD COLUMN census_tract TEXT",
        "ALTER TABLE properties ADD COLUMN tract_enriched_at TEXT",
        "ALTER TABLE delinquent_taxes ADD COLUMN census_tract TEXT",
        "ALTER TABLE delinquent_taxes ADD COLUMN tract_enriched_at TEXT",
    ]
    for stmt in _TRACT_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py::TestCensusTractMigration -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Run full test suite to ensure nothing broke**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py -v`
Expected: All existing + new tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy
git add src/db/database.py tests/test_database.py
git commit -m "feat: add census_tract columns to properties and delinquent_taxes"
```

---

## Task 3: Add census tract DB update helpers

**Files:**
- Modify: `src/db/database.py`
- Test: `tests/test_database.py`

- [ ] **Step 1: Write failing tests for tract update helpers**

Add to `tests/test_database.py`:

```python
from src.db.database import (
    # ... existing imports ...
    get_untracted_properties,
    update_property_tract,
    get_untracted_delinquent,
    update_delinquent_tract,
)


class TestCensusTractHelpers:
    def test_get_untracted_properties(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        rows = get_untracted_properties(db)
        assert len(rows) == 1
        assert rows[0]["lat"] == 38.567

    def test_get_untracted_excludes_already_tracted(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        update_property_tract(db, "2224358", "17163000100")
        rows = get_untracted_properties(db)
        assert len(rows) == 0

    def test_get_untracted_excludes_ungeocoded(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_untracted_properties(db)
        assert len(rows) == 0

    def test_update_property_tract(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_property_tract(db, "2224358", "17163000100")
        rows = get_all(db)
        assert rows[0]["census_tract"] == "17163000100"
        assert rows[0]["tract_enriched_at"] is not None

    def test_update_delinquent_tract(self, db):
        from src.db.database import upsert_delinquent_taxes
        dt_record = {
            "parcel_id": "01350402022",
            "publication_year": 2026,
            "street": "209 EDWARDS ST",
            "city": "CAHOKIA",
            "source_file": "test.pdf",
            "scraped_at": "2026-03-25T10:00:00",
        }
        upsert_delinquent_taxes(db, [dt_record])
        # Get the row ID
        row = db.execute("SELECT id FROM delinquent_taxes LIMIT 1").fetchone()
        update_delinquent_tract(db, row[0], "17163000100")
        rows = db.execute("SELECT * FROM delinquent_taxes WHERE id = ?", (row[0],)).fetchall()
        assert dict(rows[0])["census_tract"] == "17163000100"
        assert dict(rows[0])["tract_enriched_at"] is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py::TestCensusTractHelpers -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement tract update helpers**

Add to `src/db/database.py` after the vacancy helpers:

```python
# ---------------------------------------------------------------------------
# Census tract enrichment helpers
# ---------------------------------------------------------------------------

def get_untracted_properties(conn: sqlite3.Connection) -> list[dict]:
    """Get properties that have lat/lng but no census_tract yet."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE lat IS NOT NULL AND lng IS NOT NULL AND census_tract IS NULL"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_property_tract(
    conn: sqlite3.Connection, document_number: str, census_tract: str
) -> None:
    """Set census_tract and tract_enriched_at on a properties row."""
    conn.execute(
        "UPDATE properties SET census_tract = ?, tract_enriched_at = ? "
        "WHERE document_number = ?",
        (census_tract, datetime.now().isoformat(timespec="seconds"), document_number),
    )
    conn.commit()


def get_untracted_delinquent(conn: sqlite3.Connection) -> list[dict]:
    """Get delinquent_taxes rows that have lat/lng but no census_tract yet."""
    cursor = conn.execute(
        "SELECT * FROM delinquent_taxes "
        "WHERE lat IS NOT NULL AND lng IS NOT NULL AND census_tract IS NULL"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_delinquent_tract(
    conn: sqlite3.Connection, row_id: int, census_tract: str
) -> None:
    """Set census_tract and tract_enriched_at on a delinquent_taxes row."""
    conn.execute(
        "UPDATE delinquent_taxes SET census_tract = ?, tract_enriched_at = ? "
        "WHERE id = ?",
        (census_tract, datetime.now().isoformat(timespec="seconds"), row_id),
    )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_database.py::TestCensusTractHelpers -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy
git add src/db/database.py tests/test_database.py
git commit -m "feat: add census tract lookup and update helpers"
```

---

## Task 4: Create USPS vacancy ingestion module

**Files:**
- Create: `src/ingestion/usps_vacancy.py`
- Create: `tests/test_usps_vacancy.py`

- [ ] **Step 1: Write failing tests for API response parsing**

Create `tests/test_usps_vacancy.py`:

```python
"""Tests for src.ingestion.usps_vacancy — HUD NCWM API client."""

import json
import pytest

from src.ingestion.usps_vacancy import (
    parse_api_response,
    VacancyRecord,
)

# Realistic API response structure based on HUD NCWM API docs
SAMPLE_API_RESULT = {
    "TRACT_ID": "17163000100",
    "STATE_GEOID": "17",
    "COUNTY_GEOID": "163",
    "TOTAL_RESIDENTIAL_ADDRESSES": 500,
    "ACTIVE_RESIDENTIAL_ADDRESSES": 460,
    "STV_RESIDENTIAL_ADDRESSES": 15,
    "LTV_RESIDENTIAL_ADDRESSES": 10,
    "NO_STAT_RESIDENTIAL_ADDRESSES": 15,
    "TOTAL_BUSINESS_ADDRESSES": 50,
    "ACTIVE_BUSINESS_ADDRESSES": 43,
    "STV_BUSINESS_ADDRESSES": 3,
    "LTV_BUSINESS_ADDRESSES": 2,
    "NO_STAT_BUSINESS_ADDRESSES": 2,
}


class TestParseApiResponse:
    def test_parses_single_result(self):
        records = parse_api_response([SAMPLE_API_RESULT], year=2025, quarter=1)
        assert len(records) == 1
        r = records[0]
        assert r.geoid == "17163000100"
        assert r.state_fips == "17"
        assert r.county_fips == "163"
        assert r.tract_code == "000100"
        assert r.year == 2025
        assert r.quarter == 1
        assert r.total_residential == 500
        assert r.vacant_residential == 25  # STV + LTV
        assert r.no_stat_residential == 15
        assert r.total_business == 50
        assert r.vacant_business == 5  # STV + LTV

    def test_vacancy_rate_calculation(self):
        records = parse_api_response([SAMPLE_API_RESULT], year=2025, quarter=1)
        r = records[0]
        assert r.vacancy_rate_residential == pytest.approx(5.0, abs=0.1)
        assert r.vacancy_rate_business == pytest.approx(10.0, abs=0.1)

    def test_empty_list(self):
        records = parse_api_response([], year=2025, quarter=1)
        assert records == []

    def test_zero_total_no_division_error(self):
        result = {**SAMPLE_API_RESULT, "TOTAL_RESIDENTIAL_ADDRESSES": 0,
                  "ACTIVE_RESIDENTIAL_ADDRESSES": 0, "STV_RESIDENTIAL_ADDRESSES": 0,
                  "LTV_RESIDENTIAL_ADDRESSES": 0}
        records = parse_api_response([result], year=2025, quarter=1)
        assert records[0].vacancy_rate_residential == 0.0

    def test_to_dict(self):
        records = parse_api_response([SAMPLE_API_RESULT], year=2025, quarter=1)
        d = records[0].to_dict()
        assert d["geoid"] == "17163000100"
        assert "scraped_at" in d


class TestVacancyRecord:
    def test_dataclass_fields(self):
        r = VacancyRecord(
            geoid="17163000100",
            state_fips="17",
            county_fips="163",
            tract_code="000100",
            year=2025,
            quarter=1,
            total_residential=500,
            vacant_residential=25,
            vacancy_rate_residential=5.0,
            no_stat_residential=10,
            total_business=50,
            vacant_business=5,
            vacancy_rate_business=10.0,
            no_stat_business=2,
        )
        assert r.geoid == "17163000100"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_usps_vacancy.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the ingestion module**

Create `src/ingestion/usps_vacancy.py`:

```python
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
            print(f"  {year}Q{q}: FAILED — {e}")
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
        print("\n  [DRY RUN — no data written]")
        sys.exit(0)

    if args.db:
        count = records_to_db(all_records, args.db)
        print(f"\n  Wrote {count} records to DB: {args.db}")
    else:
        print("\n  No --db specified; records not saved.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_usps_vacancy.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy
git add src/ingestion/usps_vacancy.py tests/test_usps_vacancy.py
git commit -m "feat: add HUD/USPS vacancy data ingestion module"
```

---

## Task 5: Create census tract enrichment module

**Files:**
- Create: `src/enrichment/census_tract.py`
- Create: `tests/test_census_tract.py`

- [ ] **Step 1: Write failing tests for Census Bureau geocoder parsing**

Create `tests/test_census_tract.py`:

```python
"""Tests for src.enrichment.census_tract — Census Bureau geocoder client."""

import json
import pytest

from src.enrichment.census_tract import parse_geocoder_response


# Realistic Census Bureau geocoder response
SAMPLE_RESPONSE = {
    "result": {
        "input": {
            "location": {"x": -90.123, "y": 38.567},
            "benchmark": {"benchmarkName": "Public_AR_Current"},
        },
        "geographies": {
            "Census Tracts": [
                {
                    "GEOID": "17163000100",
                    "STATE": "17",
                    "COUNTY": "163",
                    "TRACT": "000100",
                    "NAME": "Census Tract 1",
                    "CENTLAT": "+38.5670000",
                    "CENTLON": "-090.1230000",
                }
            ]
        },
    }
}


class TestParseGeocoderResponse:
    def test_extracts_geoid(self):
        geoid = parse_geocoder_response(SAMPLE_RESPONSE)
        assert geoid == "17163000100"

    def test_returns_none_on_empty_tracts(self):
        response = {
            "result": {
                "geographies": {
                    "Census Tracts": []
                }
            }
        }
        assert parse_geocoder_response(response) is None

    def test_returns_none_on_missing_geographies(self):
        response = {"result": {"geographies": {}}}
        assert parse_geocoder_response(response) is None

    def test_returns_none_on_malformed_response(self):
        assert parse_geocoder_response({}) is None
        assert parse_geocoder_response({"result": {}}) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_census_tract.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the census tract enrichment module**

Create `src/enrichment/census_tract.py`:

```python
"""
Census Tract Enrichment — maps geocoded properties to census tract FIPS codes.

Uses the Census Bureau's geocoder API to look up the census tract for each
property's lat/lng coordinates, enabling joins against the usps_vacancy table.

API: https://geocoding.geo.census.gov/geocoder/geographies/coordinates
No API key required. Free. Rate-limited.

Usage:
    # Enrich properties table
    python -m src.enrichment.census_tract --db data/cheasuits.db --table properties

    # Enrich delinquent_taxes table
    python -m src.enrichment.census_tract --db data/cheasuits.db --table delinquent
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
REQUEST_DELAY = 0.3  # seconds between API calls
MAX_RETRIES = 3
DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"


def parse_geocoder_response(data: dict) -> Optional[str]:
    """Extract the 11-digit GEOID from a Census Bureau geocoder response.

    Returns the GEOID string or None if the response doesn't contain tract info.
    """
    try:
        tracts = data["result"]["geographies"].get("Census Tracts", [])
        if tracts:
            return tracts[0].get("GEOID")
    except (KeyError, TypeError, IndexError):
        pass
    return None


def fetch_census_tract(lat: float, lng: float) -> Optional[str]:
    """Look up the census tract GEOID for a coordinate pair.

    Args:
        lat: Latitude.
        lng: Longitude.

    Returns:
        11-digit FIPS GEOID string, or None on failure.
    """
    params = urllib.request.urlencode({
        "x": lng,
        "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    })
    url = f"{GEOCODER_URL}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "CheasuitsBot/1.0"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            geoid = parse_geocoder_response(data)
            if geoid:
                logger.debug(f"({lat}, {lng}) -> tract {geoid}")
            else:
                logger.warning(f"No tract found for ({lat}, {lng})")
            return geoid

        except urllib.error.HTTPError as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"HTTP {e.code} on attempt {attempt}/{MAX_RETRIES}. Retrying...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"HTTP {e.code} for ({lat}, {lng}) after {MAX_RETRIES} attempts")
                return None
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Failed for ({lat}, {lng}) after {MAX_RETRIES} attempts: {e}")
                return None

    return None


def enrich_properties(db_path: Path) -> None:
    """Fetch census tract GEOID for all geocoded but un-tracted properties."""
    from src.db.database import get_db, get_untracted_properties, update_property_tract

    conn = get_db(db_path)
    rows = get_untracted_properties(conn)

    if not rows:
        print("No properties need tract enrichment.")
        conn.close()
        return

    print(f"Enriching {len(rows)} properties with census tract...")

    enriched = 0
    failed = 0
    cache: dict[tuple[float, float], Optional[str]] = {}

    for i, row in enumerate(rows):
        lat, lng = row["lat"], row["lng"]
        key = (round(lat, 6), round(lng, 6))

        if key in cache:
            geoid = cache[key]
            if geoid:
                update_property_tract(conn, row["document_number"], geoid)
                enriched += 1
            else:
                failed += 1
            continue

        if i > 0:
            time.sleep(REQUEST_DELAY)

        geoid = fetch_census_tract(lat, lng)
        cache[key] = geoid

        if geoid:
            update_property_tract(conn, row["document_number"], geoid)
            enriched += 1
            logger.info(f"[{i+1}/{len(rows)}] ({lat}, {lng}) -> {geoid}")
        else:
            failed += 1
            logger.warning(f"[{i+1}/{len(rows)}] ({lat}, {lng}) -> FAILED")

    conn.close()
    print(f"\nEnriched {enriched}/{len(rows)} properties with census tract ({failed} failed)")


def enrich_delinquent(db_path: Path) -> None:
    """Fetch census tract GEOID for all geocoded but un-tracted delinquent tax rows."""
    from src.db.database import get_db, get_untracted_delinquent, update_delinquent_tract

    conn = get_db(db_path)
    rows = get_untracted_delinquent(conn)

    if not rows:
        print("No delinquent tax records need tract enrichment.")
        conn.close()
        return

    print(f"Enriching {len(rows)} delinquent tax records with census tract...")

    enriched = 0
    failed = 0
    cache: dict[tuple[float, float], Optional[str]] = {}

    for i, row in enumerate(rows):
        lat, lng = row["lat"], row["lng"]
        key = (round(lat, 6), round(lng, 6))

        if key in cache:
            geoid = cache[key]
            if geoid:
                update_delinquent_tract(conn, row["id"], geoid)
                enriched += 1
            else:
                failed += 1
            continue

        if i > 0:
            time.sleep(REQUEST_DELAY)

        geoid = fetch_census_tract(lat, lng)
        cache[key] = geoid

        if geoid:
            update_delinquent_tract(conn, row["id"], geoid)
            enriched += 1
            if (i + 1) % 100 == 0:
                logger.info(f"[{i+1}/{len(rows)}] Progress: {enriched} enriched, {failed} failed")
        else:
            failed += 1

    conn.close()
    print(f"\nEnriched {enriched}/{len(rows)} delinquent records with census tract ({failed} failed)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich geocoded properties with census tract FIPS codes"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--table", choices=["properties", "delinquent"], default="properties",
        help="Which table to enrich (default: properties)"
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
        enrich_delinquent(Path(args.db))
    else:
        enrich_properties(Path(args.db))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/test_census_tract.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy
git add src/enrichment/census_tract.py tests/test_census_tract.py
git commit -m "feat: add census tract enrichment module via Census Bureau geocoder"
```

---

## Task 6: Run full test suite and verify end-to-end

**Files:** None (validation only)

- [ ] **Step 1: Run all tests**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m pytest tests/ -v`
Expected: All tests PASS (existing + new)

- [ ] **Step 2: Verify CLI help works for both new modules**

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m src.ingestion.usps_vacancy --help`
Expected: Shows usage with --state, --year, --quarters, --db, --token flags

Run: `cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy && python -m src.enrichment.census_tract --help`
Expected: Shows usage with --db, --table flags

- [ ] **Step 3: Verify DB schema creates correctly**

Run:
```bash
cd /Users/kaushikandra/cheasuits/.worktrees/usps-vacancy
python -c "
from src.db.database import get_db
conn = get_db('/tmp/test_schema.db')
tables = conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()
print('Tables:', [t[0] for t in tables])
cols = conn.execute('PRAGMA table_info(usps_vacancy)').fetchall()
print('usps_vacancy columns:', [c[1] for c in cols])
prop_cols = conn.execute('PRAGMA table_info(properties)').fetchall()
print('census_tract in properties:', 'census_tract' in [c[1] for c in prop_cols])
conn.close()
import os; os.unlink('/tmp/test_schema.db')
"
```
Expected: Shows usps_vacancy table with all columns, census_tract present in properties

- [ ] **Step 4: Commit (no changes expected — just validation)**

No commit needed if no fixes were required.
