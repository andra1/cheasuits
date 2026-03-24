# Assessor Enrichment + SQLite Database Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace CSV-based data passing with a SQLite database and add a DevNetWedge assessor scraper to enrich lis pendens leads with property/tax data.

**Architecture:** SQLite DB is the shared interface between three independently-runnable modules: ava_search (ingestion) writes records, assessor (enrichment) fills in property/tax fields, prepare_data (visualization) geocodes and exports data.json for the static dashboard.

**Tech Stack:** Python 3.11+, sqlite3 (stdlib), BeautifulSoup4, urllib (stdlib), existing Next.js dashboard

**Spec:** `docs/superpowers/specs/2026-03-23-assessor-enrichment-sqlite-design.md`

---

## File Structure

| File | Responsibility |
|---|---|
| `src/utils/__init__.py` | Package init |
| `src/utils/parsing.py` | Shared `parse_legals()` and `strip_parcel_hyphens()` |
| `src/db/__init__.py` | Package init |
| `src/db/database.py` | SQLite connection, schema, upsert, query helpers |
| `src/enrichment/__init__.py` | Package init |
| `src/enrichment/assessor.py` | DevNetWedge HTML scraper + CLI |
| `src/ingestion/ava_search.py` | (modify) Add `--db` flag, DB upsert |
| `src/visualization/prepare_data.py` | (modify) Add `--db` flag, read/write DB |
| `tests/test_parsing.py` | Tests for shared parsing utils |
| `tests/test_database.py` | Tests for DB helpers |
| `tests/test_assessor.py` | Tests for assessor HTML parser |
| `requirements.txt` | (modify) Add beautifulsoup4 |
| `.gitignore` | (modify) Add data/*.db |
| `data/.gitkeep` | Ensure data dir exists |

---

## Task 1: Project scaffolding

**Files:**
- Modify: `requirements.txt`
- Modify: `.gitignore`
- Create: `data/.gitkeep`
- Create: `src/utils/__init__.py`
- Create: `src/db/__init__.py`
- Create: `src/enrichment/__init__.py`

- [ ] **Step 1: Update requirements.txt**

Add `beautifulsoup4` to `requirements.txt`:

```
playwright>=1.40.0
beautifulsoup4>=4.12.0
```

- [ ] **Step 2: Add data/*.db to .gitignore**

Add after the existing `data/processed/` line:

```
data/*.db
```

- [ ] **Step 3: Create data/.gitkeep**

```bash
touch data/.gitkeep
```

- [ ] **Step 4: Create package init files**

Create empty `__init__.py` files:
- `src/utils/__init__.py`
- `src/db/__init__.py`
- `src/enrichment/__init__.py`

- [ ] **Step 5: Install new dependency**

```bash
pip install beautifulsoup4>=4.12.0
```

- [ ] **Step 6: Commit**

```bash
git add requirements.txt .gitignore data/.gitkeep src/utils/__init__.py src/db/__init__.py src/enrichment/__init__.py
git commit -m "scaffold: add packages and dirs for DB and enrichment modules"
```

---

## Task 2: Shared parsing utilities

Extract `parse_legals` and `strip_parcel_hyphens` from `src/visualization/prepare_data.py` into a shared module so both ingestion and enrichment can use them.

**Files:**
- Create: `tests/test_parsing.py`
- Create: `src/utils/parsing.py`
- Modify: `src/visualization/prepare_data.py:53-89,119-124`

- [ ] **Step 1: Write tests for parse_legals**

Create `tests/test_parsing.py`:

```python
"""Tests for src.utils.parsing — shared legals parser and parcel utils."""

from src.utils.parsing import parse_legals, strip_parcel_hyphens


class TestParseLegals:
    def test_single_parcel_and_subdivision(self):
        legals = (
            "{'Id': 2089893, 'LegalType': 'P', 'Description': '02-29-0-205-016', "
            "'Notes': None, 'PropertyNotes': None}; "
            "{'Id': 1336823, 'LegalType': 'S', 'Description': 'GOLDEN PARK  L: 16 B: 5', "
            "'Notes': None, 'PropertyNotes': None}"
        )
        parcel_ids, subdivisions = parse_legals(legals)
        assert parcel_ids == ["02-29-0-205-016"]
        assert subdivisions == ["GOLDEN PARK  L: 16 B: 5"]

    def test_multiple_parcels(self):
        legals = (
            "{'Id': 2089335, 'LegalType': 'P', 'Description': '03-19-0-219-012', "
            "'Notes': None, 'PropertyNotes': None}; "
            "{'Id': 2089334, 'LegalType': 'P', 'Description': '03-19-0-212-035', "
            "'Notes': None, 'PropertyNotes': None}; "
            "{'Id': 1336244, 'LegalType': 'S', 'Description': 'SUMMIT SPRINGS PHASE 2A  L: 109', "
            "'Notes': None, 'PropertyNotes': None}"
        )
        parcel_ids, subdivisions = parse_legals(legals)
        assert parcel_ids == ["03-19-0-219-012", "03-19-0-212-035"]
        assert subdivisions == ["SUMMIT SPRINGS PHASE 2A  L: 109"]

    def test_empty_string(self):
        parcel_ids, subdivisions = parse_legals("")
        assert parcel_ids == []
        assert subdivisions == []

    def test_malformed_entry(self):
        parcel_ids, subdivisions = parse_legals("not a dict at all")
        assert parcel_ids == []
        assert subdivisions == []


class TestStripParcelHyphens:
    def test_standard_parcel(self):
        assert strip_parcel_hyphens("01-35-0-402-022") == "01350402022"

    def test_no_hyphens(self):
        assert strip_parcel_hyphens("01350402022") == "01350402022"

    def test_empty(self):
        assert strip_parcel_hyphens("") == ""
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_parsing.py -v
```

Expected: FAIL — `src.utils.parsing` does not exist yet.

- [ ] **Step 3: Create src/utils/parsing.py**

Move `parse_legals` (lines 53-89) and `strip_parcel_hyphens` (lines 119-124) from `src/visualization/prepare_data.py` into `src/utils/parsing.py`:

```python
"""Shared parsing utilities for the distressed RE pipeline."""

from __future__ import annotations

import ast
import logging
import re

logger = logging.getLogger(__name__)


def parse_legals(legals_str: str) -> tuple[list[str], list[str]]:
    """Parse the legals field into parcel IDs and subdivision names.

    The legals field contains semicolon-separated Python dict literals.
    LegalType='P' entries have parcel numbers, LegalType='S' have subdivision info.

    Returns:
        (parcel_ids, subdivisions)
    """
    parcel_ids = []
    subdivisions = []

    if not legals_str:
        return parcel_ids, subdivisions

    chunks = re.split(r";\s*(?=\{)", legals_str.strip())

    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            entry = ast.literal_eval(chunk)
        except (ValueError, SyntaxError):
            logger.debug(f"Could not parse legal entry: {chunk[:80]}")
            continue

        legal_type = entry.get("LegalType", "")
        description = entry.get("Description", "")

        if legal_type == "P" and description:
            parcel_ids.append(description.strip())
        elif legal_type == "S" and description:
            subdivisions.append(description.strip())

    return parcel_ids, subdivisions


def strip_parcel_hyphens(parcel_id: str) -> str:
    """Remove hyphens from parcel ID for API queries.

    '01-35-0-402-022' -> '01350402022'
    """
    return parcel_id.replace("-", "")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_parsing.py -v
```

Expected: All 7 tests PASS.

- [ ] **Step 5: Update prepare_data.py to import from shared utils**

In `src/visualization/prepare_data.py`:

1. Remove the `parse_legals` function (lines 53-89) and `strip_parcel_hyphens` function (lines 119-124).
2. Remove the `import ast` and `import re` imports (no longer needed locally).
3. Add import at the top:

```python
from src.utils.parsing import parse_legals, strip_parcel_hyphens
```

All existing call sites (`parse_legals` on line 99, `strip_parcel_hyphens` on line 154) remain unchanged.

- [ ] **Step 6: Verify prepare_data still works**

```bash
python -m src.visualization.prepare_data -v
```

Expected: Same output as before — reads CSV, geocodes, writes data.json.

- [ ] **Step 7: Commit**

```bash
git add src/utils/parsing.py tests/test_parsing.py src/visualization/prepare_data.py
git commit -m "refactor: extract parse_legals and strip_parcel_hyphens to shared utils"
```

---

## Task 3: SQLite database module

**Files:**
- Create: `tests/test_database.py`
- Create: `src/db/database.py`

- [ ] **Step 1: Write tests for database module**

Create `tests/test_database.py`:

```python
"""Tests for src.db.database — SQLite helpers."""

import sqlite3
from pathlib import Path
from datetime import datetime

import pytest

from src.db.database import (
    get_db,
    upsert_records,
    get_unenriched,
    update_enrichment,
    get_ungeocoded,
    update_geocoding,
    get_all,
)


@pytest.fixture
def db(tmp_path):
    """Create an in-memory-like DB in tmp_path for isolation."""
    db_path = tmp_path / "test.db"
    conn = get_db(db_path)
    yield conn
    conn.close()


SAMPLE_RECORD = {
    "document_number": "2224358",
    "case_number": "26-FC-121",
    "case_type": "FC",
    "case_year": "2026",
    "recorded_date": "2026-03-23",
    "party1": "CASE NO 26-FC-121",
    "party2": "ALLEN RUTH",
    "parcel_id": "01-35-0-402-022",
    "subdivision": "EDWARD PLACE  L: 28",
    "legals_raw": "{'Id': 2089863, 'LegalType': 'P', ...}",
    "source": "ava_search_stclair",
    "scraped_at": "2026-03-23T20:19:53",
}


class TestGetDb:
    def test_creates_table(self, db):
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='properties'"
        )
        assert cursor.fetchone() is not None

    def test_wal_mode(self, db):
        cursor = db.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0] == "wal"


class TestUpsertRecords:
    def test_insert_new(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_all(db)
        assert len(rows) == 1
        assert rows[0]["document_number"] == "2224358"
        assert rows[0]["party2"] == "ALLEN RUTH"

    def test_upsert_preserves_enrichment(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        # Simulate enrichment
        update_enrichment(db, "2224358", {
            "owner_name": "Ruth Allen",
            "assessed_value": 12952.0,
            "tax_status": "sold",
        })
        # Re-upsert the same record (simulating re-scrape)
        updated = {**SAMPLE_RECORD, "party2": "ALLEN RUTH E"}
        upsert_records(db, [updated])
        rows = get_all(db)
        assert len(rows) == 1
        assert rows[0]["party2"] == "ALLEN RUTH E"  # updated
        assert rows[0]["owner_name"] == "Ruth Allen"  # preserved
        assert rows[0]["assessed_value"] == 12952.0   # preserved

    def test_upsert_preserves_geocoding(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_all(db)
        assert rows[0]["lat"] == 38.567  # preserved


class TestGetUnenriched:
    def test_returns_unenriched_with_parcel(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_unenriched(db)
        assert len(rows) == 1

    def test_excludes_enriched(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_enrichment(db, "2224358", {"owner_name": "Ruth Allen"})
        rows = get_unenriched(db)
        assert len(rows) == 0

    def test_excludes_empty_parcel(self, db):
        record = {**SAMPLE_RECORD, "parcel_id": ""}
        upsert_records(db, [record])
        rows = get_unenriched(db)
        assert len(rows) == 0

    def test_excludes_errored(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        db.execute(
            "UPDATE properties SET enrichment_error = ? WHERE document_number = ?",
            ("404 not found", "2224358"),
        )
        db.commit()
        rows = get_unenriched(db)
        assert len(rows) == 0


class TestGetUngeocoded:
    def test_returns_ungeocoded_with_parcel(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_ungeocoded(db)
        assert len(rows) == 1

    def test_excludes_geocoded(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        rows = get_ungeocoded(db)
        assert len(rows) == 0


class TestUpdateEnrichment:
    def test_sets_fields_and_timestamp(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_enrichment(db, "2224358", {
            "owner_name": "Ruth Allen",
            "property_address": "209 Edwards St, Cahokia, IL 62206",
            "mailing_address": "209 Edwards St, Cahokia, IL 62206",
            "absentee_owner": 0,
            "assessed_value": 12952.0,
            "net_taxable_value": 12952.0,
            "tax_rate": 19.0222,
            "total_tax": 2463.76,
            "tax_status": "sold",
            "property_class": "0040 - Improved Lots",
            "acres": 0.25,
        })
        rows = get_all(db)
        row = rows[0]
        assert row["owner_name"] == "Ruth Allen"
        assert row["tax_status"] == "sold"
        assert row["enriched_at"] is not None


class TestUpdateGeocoding:
    def test_sets_lat_lng_and_timestamp(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567890, -90.123456)
        rows = get_all(db)
        assert rows[0]["lat"] == 38.567890
        assert rows[0]["lng"] == -90.123456
        assert rows[0]["geocoded_at"] is not None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_database.py -v
```

Expected: FAIL — `src.db.database` does not exist yet.

- [ ] **Step 3: Implement src/db/database.py**

```python
"""SQLite database helpers for the distressed RE pipeline."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_number TEXT UNIQUE NOT NULL,
    case_number TEXT DEFAULT '',
    case_type TEXT DEFAULT '',
    case_year TEXT DEFAULT '',
    recorded_date TEXT DEFAULT '',
    party1 TEXT DEFAULT '',
    party2 TEXT DEFAULT '',
    parcel_id TEXT DEFAULT '',
    subdivision TEXT DEFAULT '',
    legals_raw TEXT DEFAULT '',
    source TEXT DEFAULT '',
    scraped_at TEXT DEFAULT '',
    owner_name TEXT,
    property_address TEXT,
    mailing_address TEXT,
    absentee_owner INTEGER,
    assessed_value REAL,
    net_taxable_value REAL,
    tax_rate REAL,
    total_tax REAL,
    tax_status TEXT,
    property_class TEXT,
    acres REAL,
    enriched_at TEXT,
    enrichment_error TEXT,
    lat REAL,
    lng REAL,
    geocoded_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_parcel_id ON properties(parcel_id);
CREATE INDEX IF NOT EXISTS idx_recorded_date ON properties(recorded_date);
CREATE INDEX IF NOT EXISTS idx_enriched_at ON properties(enriched_at);
CREATE INDEX IF NOT EXISTS idx_geocoded_at ON properties(geocoded_at);
"""

# Columns owned by ava_search — only these are updated on re-upsert
INGESTION_COLUMNS = [
    "case_number", "case_type", "case_year", "recorded_date",
    "party1", "party2", "parcel_id", "subdivision", "legals_raw",
    "source", "scraped_at",
]


def get_db(db_path: str | Path) -> sqlite3.Connection:
    """Open or create the SQLite database. Enables WAL mode and creates schema."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def upsert_records(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert new records or update ingestion-owned columns on conflict.

    Uses INSERT ... ON CONFLICT to preserve enrichment and geocoding data.
    Returns number of records upserted.
    """
    if not records:
        return 0

    update_clause = ", ".join(f"{col} = excluded.{col}" for col in INGESTION_COLUMNS)

    sql = f"""
        INSERT INTO properties (document_number, {", ".join(INGESTION_COLUMNS)})
        VALUES (:document_number, {", ".join(":" + col for col in INGESTION_COLUMNS)})
        ON CONFLICT(document_number) DO UPDATE SET {update_clause}
    """

    for record in records:
        params = {"document_number": record.get("document_number", "")}
        for col in INGESTION_COLUMNS:
            params[col] = record.get(col, "")
        conn.execute(sql, params)

    conn.commit()
    return len(records)


def get_unenriched(conn: sqlite3.Connection) -> list[dict]:
    """Get rows that need assessor enrichment."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE enriched_at IS NULL AND enrichment_error IS NULL AND parcel_id != ''"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_enrichment(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update assessor enrichment fields and set enriched_at timestamp."""
    allowed = {
        "owner_name", "property_address", "mailing_address", "absentee_owner",
        "assessed_value", "net_taxable_value", "tax_rate", "total_tax",
        "tax_status", "property_class", "acres",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["enriched_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()


def set_enrichment_error(
    conn: sqlite3.Connection, document_number: str, error: str
) -> None:
    """Record an enrichment failure so the parcel is skipped on re-run."""
    conn.execute(
        "UPDATE properties SET enrichment_error = ? WHERE document_number = ?",
        (error, document_number),
    )
    conn.commit()


def get_ungeocoded(conn: sqlite3.Connection) -> list[dict]:
    """Get rows that need geocoding."""
    cursor = conn.execute(
        "SELECT * FROM properties WHERE geocoded_at IS NULL AND parcel_id != ''"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_geocoding(
    conn: sqlite3.Connection, document_number: str, lat: float, lng: float
) -> None:
    """Set lat/lng and geocoded_at timestamp."""
    conn.execute(
        "UPDATE properties SET lat = ?, lng = ?, geocoded_at = ? "
        "WHERE document_number = ?",
        (lat, lng, datetime.now().isoformat(timespec="seconds"), document_number),
    )
    conn.commit()


def get_all(conn: sqlite3.Connection) -> list[dict]:
    """Get all property rows."""
    cursor = conn.execute("SELECT * FROM properties ORDER BY recorded_date DESC")
    return [dict(row) for row in cursor.fetchall()]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_database.py -v
```

Expected: All 14 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/db/database.py tests/test_database.py
git commit -m "feat: add SQLite database module with upsert-safe schema"
```

---

## Task 4: DevNetWedge assessor scraper

**Files:**
- Create: `tests/test_assessor.py`
- Create: `src/enrichment/assessor.py`

- [ ] **Step 1: Write tests for HTML parser**

The assessor module has two distinct parts: the HTML parser (pure, testable) and the HTTP fetcher (I/O, tested via integration). We test the parser with saved HTML snippets.

Create `tests/test_assessor.py`:

```python
"""Tests for src.enrichment.assessor — DevNetWedge HTML parser."""

from src.enrichment.assessor import parse_assessor_html, AssessorRecord


# Minimal HTML fixture mimicking the DevNetWedge <dt>/<dd> structure.
SAMPLE_HTML = """
<html><body>
<dl>
  <dt>Owner Name</dt>
  <dd>ALLEN, RUTH</dd>
  <dt>Site Address</dt>
  <dd>209 EDWARDS ST CAHOKIA, IL 62206</dd>
  <dt>Mailing Address</dt>
  <dd>ALLEN, RUTH 209 EDWRADS ST CAHOKIA, IL 62206</dd>
  <dt>Property Class</dt>
  <dd>0040 - Improved Lots</dd>
  <dt>Acres</dt>
  <dd>0.2500</dd>
  <dt>Net Taxable Value</dt>
  <dd>12,952</dd>
  <dt>Tax Rate</dt>
  <dd>19.022200</dd>
  <dt>Total Tax</dt>
  <dd>$2,463.76</dd>
</dl>
</body></html>
"""

SAMPLE_HTML_TAX_SOLD = SAMPLE_HTML.replace(
    "</body>", "<span>PARCEL TAXES SOLD</span></body>"
)

SAMPLE_HTML_ABSENTEE = SAMPLE_HTML.replace(
    "<dt>Mailing Address</dt>\n  <dd>ALLEN, RUTH 209 EDWRADS ST CAHOKIA, IL 62206</dd>",
    "<dt>Mailing Address</dt>\n  <dd>ALLEN, RUTH 456 OAK AVE ST LOUIS, MO 63101</dd>",
)


class TestParseAssessorHtml:
    def test_parses_owner_name(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.owner_name == "ALLEN, RUTH"

    def test_parses_site_address(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.property_address == "209 EDWARDS ST CAHOKIA, IL 62206"

    def test_parses_assessed_value(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.net_taxable_value == 12952.0

    def test_parses_tax_rate(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.tax_rate == 19.0222

    def test_parses_total_tax(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.total_tax == 2463.76

    def test_parses_property_class(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.property_class == "0040 - Improved Lots"

    def test_parses_acres(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.acres == 0.25

    def test_detects_tax_sold(self):
        record = parse_assessor_html(SAMPLE_HTML_TAX_SOLD, "01-35-0-402-022")
        assert record.tax_status == "sold"

    def test_default_tax_status_paid(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.tax_status == "paid"

    def test_not_absentee_when_same_address(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.absentee_owner is False

    def test_absentee_when_different_address(self):
        record = parse_assessor_html(SAMPLE_HTML_ABSENTEE, "01-35-0-402-022")
        assert record.absentee_owner is True

    def test_empty_html_returns_empty_record(self):
        record = parse_assessor_html("<html><body></body></html>", "01-35-0-402-022")
        assert record.parcel_id == "01-35-0-402-022"
        assert record.owner_name == ""
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_assessor.py -v
```

Expected: FAIL — `src.enrichment.assessor` does not exist yet.

- [ ] **Step 3: Implement src/enrichment/assessor.py**

```python
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
DEFAULT_YEAR = datetime.now().year - 1
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


def _get_dd_text(soup: BeautifulSoup, dt_label: str) -> str:
    """Find a <dt> by text and return the next <dd>'s text content."""
    dt = soup.find("dt", string=re.compile(re.escape(dt_label), re.IGNORECASE))
    if dt:
        dd = dt.find_next_sibling("dd")
        if dd:
            return dd.get_text(strip=True)
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

    owner_name = _get_dd_text(soup, "Owner Name")
    property_address = _get_dd_text(soup, "Site Address")
    mailing_address = _get_dd_text(soup, "Mailing Address")
    property_class = _get_dd_text(soup, "Property Class")

    acres_text = _get_dd_text(soup, "Acres")
    acres = float(acres_text) if acres_text else None

    net_taxable_text = _get_dd_text(soup, "Net Taxable Value")
    net_taxable_value = _parse_currency(net_taxable_text)

    tax_rate_text = _get_dd_text(soup, "Tax Rate")
    tax_rate = float(tax_rate_text) if tax_rate_text else None

    total_tax_text = _get_dd_text(soup, "Total Tax")
    total_tax = _parse_currency(total_tax_text)

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
        absentee = mail_addr.strip().upper() != property_address.strip().upper()

    return AssessorRecord(
        parcel_id=parcel_id,
        owner_name=owner_name,
        property_address=property_address,
        mailing_address=mailing_address,
        absentee_owner=absentee,
        assessed_value=net_taxable_value,  # using net taxable as primary value
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
                # Permanent failure — parcel not in assessor system
                raise ValueError(f"404: parcel {parcel_id} not found") from e
            # 429, 5xx — retriable
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

        # Cache hit
        if parcel_id in cache:
            record = cache[parcel_id]
            if record:
                update_enrichment(conn, row["document_number"], record.to_db_dict())
                enriched += 1
            else:
                set_enrichment_error(conn, row["document_number"], "cached failure")
                failed += 1
            continue

        # Rate limit
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
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    enrich_from_db(Path(args.db), args.year)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_assessor.py -v
```

Expected: All 12 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/enrichment/assessor.py tests/test_assessor.py
git commit -m "feat: add DevNetWedge assessor scraper with HTML parser"
```

---

## Task 5: Modify ava_search.py to support DB output

**Files:**
- Modify: `src/ingestion/ava_search.py:533-633`

- [ ] **Step 1: Add --db flag to CLI and DB write logic**

In `src/ingestion/ava_search.py`, make these changes:

1. Add import at the top (after existing imports):

```python
from src.utils.parsing import parse_legals
```

2. Add a helper function before the `main()` function (after `export_to_csv`):

```python
def records_to_db(records: list[LisPendensRecord], db_path: str | Path) -> int:
    """Write LisPendensRecord objects to the SQLite database."""
    from src.db.database import get_db, upsert_records

    conn = get_db(db_path)
    db_records = []
    for r in records:
        d = r.to_dict()
        # Parse legals into parcel_id and subdivision
        parcel_ids, subdivisions = parse_legals(d.get("legals", ""))
        d["parcel_id"] = parcel_ids[0] if parcel_ids else ""
        d["subdivision"] = subdivisions[0] if subdivisions else ""
        d["legals_raw"] = d.pop("legals", "")
        db_records.append(d)

    count = upsert_records(conn, db_records)
    conn.close()
    logger.info(f"Upserted {count} records to {db_path}")
    return count
```

3. Add `--db` argument to the argparse block in `main()` (after the `--json` argument, around line 564):

```python
    parser.add_argument(
        "--db", type=str, default=None,
        help="SQLite database path. When provided, writes records to DB."
    )
```

4. Add DB write logic in `main()` after the CSV export block (after line 629, before the function ends):

```python
    # DB export
    if args.db:
        count = records_to_db(records, args.db)
        print(f"Wrote {count} records to DB: {args.db}")
```

- [ ] **Step 2: Test the DB integration manually**

```bash
python -m src.ingestion.ava_search --days 30 --db data/cheasuits.db --output /dev/null
```

Expected: Prints record summary, then "Wrote N records to DB: data/cheasuits.db".

Verify with:
```bash
python -c "
from src.db.database import get_db, get_all
conn = get_db('data/cheasuits.db')
rows = get_all(conn)
print(f'{len(rows)} records in DB')
if rows:
    r = rows[0]
    print(f'  First: {r[\"document_number\"]} | parcel={r[\"parcel_id\"]} | {r[\"party2\"]}')
conn.close()
"
```

Expected: Shows record count and first record with populated parcel_id.

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/ava_search.py
git commit -m "feat: add --db flag to ava_search for SQLite output"
```

---

## Task 6: Modify prepare_data.py to support DB input

**Files:**
- Modify: `src/visualization/prepare_data.py`

- [ ] **Step 1: Add DB read/write path to prepare_data.py**

In `src/visualization/prepare_data.py`, make these changes:

1. Add import at top:

```python
from src.utils.parsing import parse_legals, strip_parcel_hyphens
```

(And remove the now-redundant local definitions of these functions if not done in Task 2.)

2. Add a function to read records from the DB (after the `read_csv` function):

```python
def read_db(db_path: Path) -> list[dict]:
    """Read property records from the SQLite database."""
    from src.db.database import get_db, get_all, get_ungeocoded, update_geocoding

    conn = get_db(db_path)
    rows = get_all(conn)

    logger.info(f"Read {len(rows)} records from {db_path}")

    # Geocode rows that need it
    ungeocoded = get_ungeocoded(conn)
    if ungeocoded:
        print(f"Geocoding {len(ungeocoded)} new parcels via ArcGIS...")
        seen: dict[str, tuple[float, float] | None] = {}
        geocoded_count = 0

        for i, row in enumerate(ungeocoded):
            parcel_id = row["parcel_id"]

            if parcel_id in seen:
                coords = seen[parcel_id]
            else:
                if i > 0:
                    time.sleep(REQUEST_DELAY)
                coords = geocode_parcel(parcel_id)
                seen[parcel_id] = coords

            if coords:
                update_geocoding(conn, row["document_number"], coords[0], coords[1])
                geocoded_count += 1
                logger.info(f"[{i+1}/{len(ungeocoded)}] {parcel_id} -> ({coords[0]}, {coords[1]})")
            else:
                logger.warning(f"[{i+1}/{len(ungeocoded)}] {parcel_id} -> FAILED")

        print(f"Geocoded {geocoded_count}/{len(ungeocoded)} new parcels")

    # Re-read all rows (now with updated geocoding)
    rows = get_all(conn)
    conn.close()

    # Convert to the format build_output expects
    records = []
    for row in rows:
        records.append({
            "document_number": row["document_number"] or "",
            "case_number": row["case_number"] or "",
            "case_type": row["case_type"] or "",
            "recorded_date": row["recorded_date"] or "",
            "party2": row["party2"] or "",
            "parcel_id": row["parcel_id"] or "",
            "subdivision": row["subdivision"] or "",
            "lat": row["lat"],
            "lng": row["lng"],
            # Assessor enrichment fields
            "owner_name": row["owner_name"] or "",
            "property_address": row["property_address"] or "",
            "mailing_address": row["mailing_address"] or "",
            "absentee_owner": bool(row["absentee_owner"]) if row["absentee_owner"] is not None else False,
            "assessed_value": row["assessed_value"],
            "net_taxable_value": row["net_taxable_value"],
            "tax_status": row["tax_status"] or "",
            "property_class": row["property_class"] or "",
            "acres": row["acres"],
        })

    return records
```

3. In `build_output`, **replace only the features loop** (lines 299-311 of the existing file — from `features = []` through the end of the `for r in records` loop). Keep everything else in `build_output` unchanged (date range, type_counts, summary, geocoded_count, and the return dict):

```python
    features = []
    for r in records:
        feature = {
            "document_number": r["document_number"],
            "case_number": r["case_number"],
            "case_type": r["case_type"],
            "recorded_date": r["recorded_date"],
            "party2": r["party2"],
            "parcel_id": r.get("parcel_id", ""),
            "subdivision": r.get("subdivision", ""),
            "lat": r.get("lat"),
            "lng": r.get("lng"),
        }
        # Include assessor fields if present
        for field in ("owner_name", "property_address", "mailing_address",
                      "absentee_owner", "assessed_value", "net_taxable_value",
                      "tax_status", "property_class", "acres"):
            if field in r:
                feature[field] = r[field]
        features.append(feature)
```

4. **Replace the entire `main()` function** (lines 337-383 of the existing file) with this complete version that adds `--db` support while preserving all existing behavior:

```python
def main():
    parser = argparse.ArgumentParser(
        description="Prepare lis pendens data for the map dashboard"
    )
    parser.add_argument(
        "--input", "-i", type=str, default=None,
        help=f"Input CSV path (default: {DEFAULT_CSV.name})"
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="SQLite database path. When provided, reads from DB instead of CSV."
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

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    if args.db:
        db_path = Path(args.db)
        if not db_path.exists():
            print(f"ERROR: Database not found: {db_path}")
            sys.exit(1)
        print(f"Reading from database {db_path}...")
        records = read_db(db_path)
    else:
        csv_path = Path(args.input) if args.input else DEFAULT_CSV
        if not csv_path.exists():
            print(f"ERROR: CSV not found: {csv_path}")
            sys.exit(1)
        print(f"Reading {csv_path}...")
        records = read_csv(csv_path)

        if not records:
            print("No records found in CSV.")
            sys.exit(0)

        print(f"Geocoding {len(records)} parcels via ArcGIS...")
        records = geocode_records(records)

    if not records:
        print("No records found.")
        sys.exit(0)

    data = build_output(records)
    write_output(data, output_path)
```

- [ ] **Step 2: Test the DB pipeline end-to-end**

```bash
python -m src.visualization.prepare_data --db data/cheasuits.db -v
```

Expected: Reads from DB, geocodes any un-geocoded records, writes `dashboard/public/data.json`.

- [ ] **Step 3: Commit**

```bash
git add src/visualization/prepare_data.py
git commit -m "feat: add --db flag to prepare_data for SQLite-backed pipeline"
```

---

## Task 7: End-to-end pipeline test

**Files:** None (verification only)

- [ ] **Step 1: Run the full pipeline**

```bash
# Wipe and rebuild from scratch
rm -f data/cheasuits.db

# Step 1: Ingest
python -m src.ingestion.ava_search --days 30 --db data/cheasuits.db

# Step 2: Enrich
python -m src.enrichment.assessor --db data/cheasuits.db -v

# Step 3: Geocode + export
python -m src.visualization.prepare_data --db data/cheasuits.db
```

- [ ] **Step 2: Verify DB contents**

```bash
python -c "
from src.db.database import get_db, get_all
conn = get_db('data/cheasuits.db')
rows = get_all(conn)
enriched = sum(1 for r in rows if r['enriched_at'])
geocoded = sum(1 for r in rows if r['geocoded_at'])
tax_sold = sum(1 for r in rows if r['tax_status'] == 'sold')
absentee = sum(1 for r in rows if r['absentee_owner'])
print(f'Total: {len(rows)}')
print(f'Enriched: {enriched}')
print(f'Geocoded: {geocoded}')
print(f'Tax sold: {tax_sold}')
print(f'Absentee: {absentee}')
if rows:
    r = rows[0]
    print(f'Sample: {r[\"party2\"]} | {r[\"owner_name\"]} | {r[\"property_address\"]} | tax={r[\"tax_status\"]}')
conn.close()
"
```

Expected: All records enriched and geocoded, with real owner names and tax status.

- [ ] **Step 3: Verify dashboard data.json has enrichment fields**

```bash
python -c "
import json
data = json.load(open('dashboard/public/data.json'))
f = data['features'][0]
print('Fields:', list(f.keys()))
assert 'owner_name' in f, 'Missing owner_name'
assert 'tax_status' in f, 'Missing tax_status'
print('OK — enrichment fields present in data.json')
"
```

- [ ] **Step 4: Run all tests**

```bash
pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 5: Commit all remaining changes**

```bash
git add -A
git commit -m "feat: complete SQLite + assessor enrichment pipeline"
```

---

## Task 8: Deploy updated dashboard

- [ ] **Step 1: Deploy to Vercel**

```bash
vercel --prod
```

- [ ] **Step 2: Verify deployment**

Open the production URL and confirm the dashboard loads with data.
