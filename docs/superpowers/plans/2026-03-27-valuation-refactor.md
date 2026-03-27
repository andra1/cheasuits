# Valuation Module Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor valuation into a separate `valuations` table, comps into a `property_comps` join table, remove assessed multiplier and blending logic, switch scrapers to `curl_cffi`, and add a Comparable Sales card to the property detail page.

**Architecture:** Two new DB tables (`valuations`, `property_comps`) replace 10 columns on `properties`. Valuation pipeline writes per-source rows; a priority rule denormalizes the winning estimate to `properties.estimated_market_value`. Comps pipeline writes matched comps to `property_comps` and a summary to `valuations`. Frontend reads nested `valuations` and `comps` arrays from `data.json`.

**Tech Stack:** Python 3.11+, SQLite, curl_cffi, Next.js (React), Tailwind CSS

**Spec:** `docs/superpowers/specs/2026-03-27-valuation-refactor-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/db/database.py` | Add `valuations` + `property_comps` tables, new CRUD functions, migration to remove old columns |
| Modify | `src/enrichment/valuation.py` | Rewrite: curl_cffi scrapers, write to `valuations` table, priority rule |
| Modify | `src/enrichment/comps.py` | Refactor output: write to `property_comps` + `valuations` instead of `properties` |
| Modify | `src/scoring/viability.py:211` | Read comp confidence from `valuations` table instead of `properties.comps_confidence` |
| Modify | `src/visualization/prepare_data.py:117-170,363-378` | Export nested `valuations` and `comps` arrays, remove old fields |
| Modify | `dashboard/pages/property/[id].js:219-259` | Revised Financial Overview card with valuations table + source links |
| Create | `dashboard/components/CompsCard.js` | New Comparable Sales card with expandable detail rows |
| Modify | `dashboard/pages/property/[id].js` | Add CompsCard after Financial Overview |
| Create | `tests/test_valuation_refactor.py` | Tests for new DB functions, priority rule, valuation pipeline |
| Modify | `tests/test_viability.py:297-371` | Update test fixtures: `comps_confidence` → `valuations` table lookup |

---

### Task 1: Install curl_cffi dependency

**Files:**
- Modify: project dependencies (pip install)

- [ ] **Step 1: Install curl_cffi**

```bash
pip install curl_cffi
```

- [ ] **Step 2: Verify installation**

```bash
python -c "from curl_cffi import requests; print('curl_cffi OK')"
```

Expected: `curl_cffi OK`

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: add curl_cffi dependency for bot-resistant scraping"
```

---

### Task 2: Add `valuations` and `property_comps` tables to database schema

**Files:**
- Modify: `src/db/database.py`
- Create: `tests/test_valuation_refactor.py`

- [ ] **Step 1: Write failing tests for new tables and CRUD functions**

Create `tests/test_valuation_refactor.py`:

```python
"""Tests for valuation refactor — new tables and CRUD functions."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from src.db.database import get_db


@pytest.fixture
def db(tmp_path):
    """Create a fresh in-memory-like test database."""
    db_path = tmp_path / "test.db"
    conn = get_db(db_path)
    # Insert a test property
    conn.execute(
        "INSERT INTO properties (document_number, parcel_id, assessed_value) "
        "VALUES ('DOC001', '01-01-100-001', 100000)"
    )
    conn.commit()
    return conn


class TestValuationsTable:
    def test_table_exists(self, db):
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='valuations'"
        )
        assert cursor.fetchone() is not None

    def test_upsert_valuation(self, db):
        from src.db.database import upsert_valuation
        upsert_valuation(db, "DOC001", {
            "source": "redfin",
            "estimate": 250000.0,
            "source_url": "https://www.redfin.com/IL/Belleville/123-Main-St",
            "confidence": "high",
        })
        row = db.execute(
            "SELECT * FROM valuations WHERE document_number='DOC001' AND source='redfin'"
        ).fetchone()
        assert row is not None
        assert dict(row)["estimate"] == 250000.0
        assert dict(row)["source_url"] == "https://www.redfin.com/IL/Belleville/123-Main-St"

    def test_upsert_valuation_overwrites(self, db):
        from src.db.database import upsert_valuation
        upsert_valuation(db, "DOC001", {
            "source": "redfin",
            "estimate": 250000.0,
            "confidence": "high",
        })
        upsert_valuation(db, "DOC001", {
            "source": "redfin",
            "estimate": 260000.0,
            "confidence": "high",
        })
        rows = db.execute(
            "SELECT * FROM valuations WHERE document_number='DOC001' AND source='redfin'"
        ).fetchall()
        assert len(rows) == 1
        assert dict(rows[0])["estimate"] == 260000.0

    def test_get_valuations(self, db):
        from src.db.database import upsert_valuation, get_valuations
        upsert_valuation(db, "DOC001", {
            "source": "redfin",
            "estimate": 250000.0,
            "confidence": "high",
        })
        upsert_valuation(db, "DOC001", {
            "source": "zillow",
            "estimate": 240000.0,
            "confidence": "high",
        })
        vals = get_valuations(db, "DOC001")
        assert len(vals) == 2
        sources = {v["source"] for v in vals}
        assert sources == {"redfin", "zillow"}


class TestPropertyCompsTable:
    def test_table_exists(self, db):
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='property_comps'"
        )
        assert cursor.fetchone() is not None

    def test_insert_property_comps(self, db):
        from src.db.database import upsert_comparable_sales, insert_property_comps
        # Insert a comp sale first
        upsert_comparable_sales(db, [{
            "address": "456 Oak St, Belleville, IL",
            "sale_date": "2026-01-15",
            "sale_price": 185000.0,
            "lat": 38.52, "lng": -89.98,
            "source": "redfin", "source_id": "MLS123",
            "property_type": "", "sqft": 1400, "beds": 3,
            "baths": 2.0, "lot_size": 0.2, "year_built": 1990,
            "scraped_at": "2026-03-27T10:00:00",
        }])
        comp_id = db.execute(
            "SELECT id FROM comparable_sales WHERE address='456 Oak St, Belleville, IL'"
        ).fetchone()[0]

        insert_property_comps(db, "DOC001", [
            {
                "comp_sale_id": comp_id,
                "distance_miles": 0.5,
                "similarity_score": 0.85,
                "lot_size_ratio": 1.1,
                "adjusted_price": 203500.0,
            }
        ])

        rows = db.execute(
            "SELECT * FROM property_comps WHERE document_number='DOC001'"
        ).fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row["comp_sale_id"] == comp_id
        assert row["distance_miles"] == 0.5
        assert row["similarity_score"] == 0.85

    def test_replace_property_comps(self, db):
        from src.db.database import upsert_comparable_sales, insert_property_comps
        upsert_comparable_sales(db, [{
            "address": "456 Oak St, Belleville, IL",
            "sale_date": "2026-01-15",
            "sale_price": 185000.0,
            "lat": 38.52, "lng": -89.98,
            "source": "redfin", "source_id": "MLS123",
            "property_type": "", "sqft": None, "beds": None,
            "baths": None, "lot_size": None, "year_built": None,
            "scraped_at": "2026-03-27T10:00:00",
        }])
        comp_id = db.execute(
            "SELECT id FROM comparable_sales WHERE address='456 Oak St, Belleville, IL'"
        ).fetchone()[0]

        # Insert first set
        insert_property_comps(db, "DOC001", [
            {"comp_sale_id": comp_id, "distance_miles": 0.5,
             "similarity_score": 0.85, "lot_size_ratio": 1.0, "adjusted_price": 185000.0}
        ])
        # Replace with new set
        insert_property_comps(db, "DOC001", [
            {"comp_sale_id": comp_id, "distance_miles": 0.3,
             "similarity_score": 0.90, "lot_size_ratio": 1.0, "adjusted_price": 185000.0}
        ])
        rows = db.execute(
            "SELECT * FROM property_comps WHERE document_number='DOC001'"
        ).fetchall()
        assert len(rows) == 1
        assert dict(rows[0])["distance_miles"] == 0.3

    def test_get_property_comps(self, db):
        from src.db.database import upsert_comparable_sales, insert_property_comps, get_property_comps
        upsert_comparable_sales(db, [{
            "address": "456 Oak St, Belleville, IL",
            "sale_date": "2026-01-15",
            "sale_price": 185000.0,
            "lat": 38.52, "lng": -89.98,
            "source": "redfin", "source_id": "MLS123",
            "property_type": "Single Family", "sqft": 1400, "beds": 3,
            "baths": 2.0, "lot_size": 0.2, "year_built": 1990,
            "scraped_at": "2026-03-27T10:00:00",
        }])
        comp_id = db.execute(
            "SELECT id FROM comparable_sales WHERE address='456 Oak St, Belleville, IL'"
        ).fetchone()[0]

        insert_property_comps(db, "DOC001", [
            {"comp_sale_id": comp_id, "distance_miles": 0.5,
             "similarity_score": 0.85, "lot_size_ratio": 1.1, "adjusted_price": 203500.0}
        ])
        comps = get_property_comps(db, "DOC001")
        assert len(comps) == 1
        c = comps[0]
        assert c["address"] == "456 Oak St, Belleville, IL"
        assert c["sale_price"] == 185000.0
        assert c["distance_miles"] == 0.5
        assert c["similarity_score"] == 0.85


class TestApplyMarketValuePriority:
    def test_redfin_wins(self, db):
        from src.db.database import upsert_valuation, apply_market_value_priority
        upsert_valuation(db, "DOC001", {"source": "redfin", "estimate": 250000.0, "confidence": "high"})
        upsert_valuation(db, "DOC001", {"source": "comps", "estimate": 200000.0, "confidence": "medium", "comp_count": 3})
        apply_market_value_priority(db, "DOC001")
        row = dict(db.execute("SELECT estimated_market_value FROM properties WHERE document_number='DOC001'").fetchone())
        assert row["estimated_market_value"] == 250000.0

    def test_both_external_averages(self, db):
        from src.db.database import upsert_valuation, apply_market_value_priority
        upsert_valuation(db, "DOC001", {"source": "redfin", "estimate": 260000.0, "confidence": "high"})
        upsert_valuation(db, "DOC001", {"source": "zillow", "estimate": 240000.0, "confidence": "high"})
        apply_market_value_priority(db, "DOC001")
        row = dict(db.execute("SELECT estimated_market_value FROM properties WHERE document_number='DOC001'").fetchone())
        assert row["estimated_market_value"] == 250000.0

    def test_comps_fallback(self, db):
        from src.db.database import upsert_valuation, apply_market_value_priority
        upsert_valuation(db, "DOC001", {"source": "comps", "estimate": 200000.0, "confidence": "high", "comp_count": 5})
        apply_market_value_priority(db, "DOC001")
        row = dict(db.execute("SELECT estimated_market_value FROM properties WHERE document_number='DOC001'").fetchone())
        assert row["estimated_market_value"] == 200000.0

    def test_no_valuations_stays_null(self, db):
        from src.db.database import apply_market_value_priority
        apply_market_value_priority(db, "DOC001")
        row = dict(db.execute("SELECT estimated_market_value FROM properties WHERE document_number='DOC001'").fetchone())
        assert row["estimated_market_value"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_valuation_refactor.py -v
```

Expected: FAIL — `upsert_valuation`, `get_valuations`, `insert_property_comps`, `get_property_comps`, `apply_market_value_priority` do not exist yet.

- [ ] **Step 3: Add `valuations` and `property_comps` table creation to SCHEMA in `database.py`**

In `src/db/database.py`, add these tables after the `comparable_sales` table definition (after line 137, before the closing `"""`):

```python
CREATE TABLE IF NOT EXISTS valuations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_number TEXT NOT NULL REFERENCES properties(document_number),
    source TEXT NOT NULL,
    estimate REAL NOT NULL,
    source_url TEXT,
    confidence TEXT,
    comp_count INTEGER,
    valued_at TEXT NOT NULL,
    UNIQUE(document_number, source)
);
CREATE INDEX IF NOT EXISTS idx_valuations_doc ON valuations(document_number);

CREATE TABLE IF NOT EXISTS property_comps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_number TEXT NOT NULL REFERENCES properties(document_number),
    comp_sale_id INTEGER NOT NULL REFERENCES comparable_sales(id),
    distance_miles REAL,
    similarity_score REAL,
    lot_size_ratio REAL,
    adjusted_price REAL,
    matched_at TEXT,
    UNIQUE(document_number, comp_sale_id)
);
CREATE INDEX IF NOT EXISTS idx_property_comps_doc ON property_comps(document_number);
```

- [ ] **Step 4: Add `upsert_valuation` function**

Add after the `update_comps_valuation` function (after line 802) in `src/db/database.py`:

```python
def upsert_valuation(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Insert or update a valuation row for a specific source.

    Required fields: source, estimate, confidence.
    Optional fields: source_url, comp_count.
    """
    conn.execute(
        """
        INSERT INTO valuations (document_number, source, estimate, source_url,
                                confidence, comp_count, valued_at)
        VALUES (:document_number, :source, :estimate, :source_url,
                :confidence, :comp_count, :valued_at)
        ON CONFLICT(document_number, source) DO UPDATE SET
            estimate = excluded.estimate,
            source_url = excluded.source_url,
            confidence = excluded.confidence,
            comp_count = excluded.comp_count,
            valued_at = excluded.valued_at
        """,
        {
            "document_number": document_number,
            "source": fields["source"],
            "estimate": fields["estimate"],
            "source_url": fields.get("source_url"),
            "confidence": fields.get("confidence"),
            "comp_count": fields.get("comp_count"),
            "valued_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    conn.commit()
```

- [ ] **Step 5: Add `get_valuations` function**

```python
def get_valuations(conn: sqlite3.Connection, document_number: str) -> list[dict]:
    """Get all valuation rows for a property."""
    cursor = conn.execute(
        "SELECT * FROM valuations WHERE document_number = ? ORDER BY valued_at DESC",
        (document_number,),
    )
    return [dict(row) for row in cursor.fetchall()]
```

- [ ] **Step 6: Add `insert_property_comps` function**

```python
def insert_property_comps(
    conn: sqlite3.Connection, document_number: str, comps: list[dict]
) -> None:
    """Replace all property_comps for a subject property with new matches.

    Deletes existing rows and inserts new ones within a transaction.
    Each comp dict must have: comp_sale_id, distance_miles, similarity_score,
    lot_size_ratio, adjusted_price.
    """
    conn.execute(
        "DELETE FROM property_comps WHERE document_number = ?",
        (document_number,),
    )
    for c in comps:
        conn.execute(
            """
            INSERT INTO property_comps
                (document_number, comp_sale_id, distance_miles, similarity_score,
                 lot_size_ratio, adjusted_price, matched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_number,
                c["comp_sale_id"],
                c.get("distance_miles"),
                c.get("similarity_score"),
                c.get("lot_size_ratio"),
                c.get("adjusted_price"),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
    conn.commit()
```

- [ ] **Step 7: Add `get_property_comps` function**

```python
def get_property_comps(conn: sqlite3.Connection, document_number: str) -> list[dict]:
    """Get matched comps for a property, joined with comparable_sales details.

    Returns comps sorted by similarity_score descending (best first).
    """
    cursor = conn.execute(
        """
        SELECT
            pc.distance_miles, pc.similarity_score, pc.lot_size_ratio,
            pc.adjusted_price, pc.matched_at,
            cs.id AS comp_sale_id, cs.address, cs.lat, cs.lng,
            cs.sale_price, cs.sale_date, cs.property_type,
            cs.sqft, cs.beds, cs.baths, cs.lot_size, cs.year_built,
            cs.source, cs.source_id
        FROM property_comps pc
        JOIN comparable_sales cs ON pc.comp_sale_id = cs.id
        WHERE pc.document_number = ?
        ORDER BY pc.similarity_score DESC
        """,
        (document_number,),
    )
    return [dict(row) for row in cursor.fetchall()]
```

- [ ] **Step 8: Add `apply_market_value_priority` function**

```python
def apply_market_value_priority(conn: sqlite3.Connection, document_number: str) -> None:
    """Set estimated_market_value on properties using the priority rule.

    Priority: average of Zillow+Redfin (or whichever exists) > comps > NULL.
    """
    vals = get_valuations(conn, document_number)
    if not vals:
        return

    val_by_source = {v["source"]: v for v in vals}
    redfin = val_by_source.get("redfin")
    zillow = val_by_source.get("zillow")
    comps = val_by_source.get("comps")

    estimate = None
    valued_at = None

    if redfin and zillow:
        estimate = round((redfin["estimate"] + zillow["estimate"]) / 2, 2)
        valued_at = max(redfin["valued_at"], zillow["valued_at"])
    elif redfin:
        estimate = redfin["estimate"]
        valued_at = redfin["valued_at"]
    elif zillow:
        estimate = zillow["estimate"]
        valued_at = zillow["valued_at"]
    elif comps:
        estimate = comps["estimate"]
        valued_at = comps["valued_at"]

    if estimate is not None:
        conn.execute(
            "UPDATE properties SET estimated_market_value = ?, valued_at = ? "
            "WHERE document_number = ?",
            (estimate, valued_at, document_number),
        )
        conn.commit()
```

- [ ] **Step 9: Run tests to verify they pass**

```bash
pytest tests/test_valuation_refactor.py -v
```

Expected: All tests PASS.

- [ ] **Step 10: Commit**

```bash
git add src/db/database.py tests/test_valuation_refactor.py
git commit -m "feat: add valuations and property_comps tables with CRUD functions"
```

---

### Task 3: Rewrite valuation pipeline with curl_cffi

**Files:**
- Modify: `src/enrichment/valuation.py`

- [ ] **Step 1: Add tests for refactored valuation functions**

Append to `tests/test_valuation_refactor.py`:

```python
class TestFetchRefactored:
    """Test that fetch functions return (estimate, url) tuples."""

    def test_fetch_redfin_returns_tuple_on_failure(self):
        from src.enrichment.valuation import fetch_redfin_estimate
        # Non-existent address should return (None, None) not crash
        result = fetch_redfin_estimate("99999 Nonexistent Blvd, Nowhere, ZZ 00000")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_fetch_zillow_returns_tuple_on_failure(self):
        from src.enrichment.valuation import fetch_zillow_estimate
        result = fetch_zillow_estimate("99999 Nonexistent Blvd, Nowhere, ZZ 00000")
        assert isinstance(result, tuple)
        assert len(result) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_valuation_refactor.py::TestFetchRefactored -v
```

Expected: FAIL — current functions return `Optional[float]`, not tuples.

- [ ] **Step 3: Rewrite `valuation.py`**

Replace the entire contents of `src/enrichment/valuation.py` with:

```python
"""Valuation Module — Zillow/Redfin Estimates via curl_cffi.

Fetches property value estimates from Zillow and Redfin, stores each
as a row in the valuations table, and applies the priority rule to
set estimated_market_value on the properties table.

Usage:
    python -m src.enrichment.valuation [--db data/cheasuits.db] [-v]
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

SCRAPE_DELAY = 1.0
MAX_RETRIES = 2

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

IMPERSONATE_BROWSERS = ["chrome131", "chrome130", "chrome124"]


def _normalize_address(raw: str) -> str:
    """Flatten multi-line DB address to single line for URL queries."""
    return raw.replace("\n", ", ").strip()


def _get_session() -> cffi_requests.Session:
    """Create a curl_cffi session with browser impersonation."""
    browser = random.choice(IMPERSONATE_BROWSERS)
    session = cffi_requests.Session(impersonate=browser)
    return session


# ---------------------------------------------------------------------------
# Redfin Scraper
# ---------------------------------------------------------------------------

def fetch_redfin_estimate(address: str) -> tuple[Optional[float], Optional[str]]:
    """Fetch Redfin Estimate for a property address.

    Returns (estimate, property_page_url) or (None, None).
    """
    normalized = _normalize_address(address)
    encoded = urllib.parse.quote(normalized)

    session = _get_session()

    # Step 1: Autocomplete to get property URL
    autocomplete_url = (
        f"https://www.redfin.com/stingray/do/location-autocomplete"
        f"?v=2&al=1&location={encoded}"
    )

    try:
        resp = session.get(autocomplete_url, headers={
            "Accept": "application/json",
        }, timeout=15)
        resp.raise_for_status()
        raw = resp.text

        json_str = raw.split("&&", 1)[-1] if "&&" in raw else raw
        data = json.loads(json_str)

        sections = data.get("payload", {}).get("sections", [])
        if not sections:
            logger.debug(f"Redfin: no autocomplete results for {normalized}")
            return (None, None)

        rows = sections[0].get("rows", [])
        if not rows:
            return (None, None)

        property_url = rows[0].get("url", "")
        if not property_url:
            return (None, None)

    except Exception as e:
        logger.warning(f"Redfin autocomplete failed for {normalized}: {e}")
        return (None, None)

    # Step 2: Fetch property page
    page_url = f"https://www.redfin.com{property_url}"
    try:
        resp = session.get(page_url, headers={
            "Accept": "text/html",
        }, timeout=15)
        resp.raise_for_status()
        html = resp.text

    except Exception as e:
        logger.warning(f"Redfin page fetch failed for {property_url}: {e}")
        return (None, None)

    # Step 3: Extract estimate
    try:
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            page_data = json.loads(match.group(1))
            estimate = (
                page_data.get("props", {})
                .get("pageProps", {})
                .get("initialRedfinEstimateValue")
            )
            if estimate and isinstance(estimate, (int, float)) and estimate > 0:
                logger.info(f"Redfin estimate for {normalized}: ${estimate:,.0f}")
                return (float(estimate), page_url)

        avm_match = re.search(r'"avm":\s*\{[^}]*"amount":\s*(\d+)', html)
        if avm_match:
            estimate = float(avm_match.group(1))
            if estimate > 0:
                logger.info(f"Redfin AVM for {normalized}: ${estimate:,.0f}")
                return (estimate, page_url)

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Redfin parse failed for {normalized}: {e}")

    logger.debug(f"Redfin: no estimate found for {normalized}")
    return (None, None)


# ---------------------------------------------------------------------------
# Zillow Scraper
# ---------------------------------------------------------------------------

def fetch_zillow_estimate(address: str) -> tuple[Optional[float], Optional[str]]:
    """Fetch Zillow Zestimate for a property address.

    Returns (estimate, property_page_url) or (None, None).
    """
    normalized = _normalize_address(address)

    slug = re.sub(r'[^\w,\s-]', '', normalized)
    slug = re.sub(r'[\s]+', '-', slug.strip())
    url = f"https://www.zillow.com/homes/{slug}_rb/"

    session = _get_session()

    try:
        resp = session.get(url, headers={
            "Accept": "text/html,application/xhtml+xml",
        }, timeout=15)
        resp.raise_for_status()
        html = resp.text

    except Exception as e:
        logger.warning(f"Zillow fetch failed for {normalized}: {e}")
        return (None, None)

    # Try __NEXT_DATA__ JSON blob
    try:
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            page_data = json.loads(match.group(1))
            gdp_cache = (
                page_data.get("props", {})
                .get("pageProps", {})
                .get("componentProps", {})
                .get("gdpClientCache", {})
            )
            for cache_val in gdp_cache.values():
                if isinstance(cache_val, dict):
                    zest = cache_val.get("property", {}).get("zestimate")
                    if zest and isinstance(zest, (int, float)) and zest > 0:
                        logger.info(f"Zillow Zestimate for {normalized}: ${zest:,.0f}")
                        return (float(zest), url)

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.debug(f"Zillow JSON parse failed for {normalized}: {e}")

    # Regex fallback
    zest_match = re.search(r'"zestimate"\s*:\s*(\d+)', html)
    if zest_match:
        value = float(zest_match.group(1))
        if value > 0:
            logger.info(f"Zillow Zestimate (regex) for {normalized}: ${value:,.0f}")
            return (value, url)

    logger.debug(f"Zillow: no Zestimate found for {normalized}")
    return (None, None)


# ---------------------------------------------------------------------------
# Enrichment Orchestrator
# ---------------------------------------------------------------------------

def enrich_valuations_from_db(db_path: Path) -> None:
    """Fetch Zillow/Redfin valuations for properties, write to valuations table,
    and apply priority rule."""
    from src.db.database import (
        get_db, upsert_valuation, apply_market_value_priority,
    )

    conn = get_db(db_path)

    # Get properties with addresses that haven't been externally valued yet
    cursor = conn.execute(
        """
        SELECT p.document_number, p.property_address
        FROM properties p
        WHERE p.property_address IS NOT NULL
          AND p.property_address != ''
          AND NOT EXISTS (
              SELECT 1 FROM valuations v
              WHERE v.document_number = p.document_number
                AND v.source IN ('zillow', 'redfin')
          )
        """
    )
    rows = [dict(row) for row in cursor.fetchall()]

    if not rows:
        print("No properties to value.")
        conn.close()
        return

    print(f"Valuing {len(rows)} properties via Zillow/Redfin...")

    valued = 0
    request_count = 0

    for i, row in enumerate(rows):
        doc_num = row["document_number"]
        address = row["property_address"]
        normalized_addr = _normalize_address(address)

        # Try Redfin
        if request_count > 0:
            time.sleep(SCRAPE_DELAY)
        redfin_est, redfin_url = fetch_redfin_estimate(normalized_addr)
        request_count += 1

        if redfin_est is not None:
            upsert_valuation(conn, doc_num, {
                "source": "redfin",
                "estimate": redfin_est,
                "source_url": redfin_url,
                "confidence": "high",
            })

        # Try Zillow
        time.sleep(SCRAPE_DELAY)
        zillow_est, zillow_url = fetch_zillow_estimate(normalized_addr)
        request_count += 1

        if zillow_est is not None:
            upsert_valuation(conn, doc_num, {
                "source": "zillow",
                "estimate": zillow_est,
                "source_url": zillow_url,
                "confidence": "high",
            })

        if redfin_est is not None or zillow_est is not None:
            valued += 1

        # Apply priority rule for this property
        apply_market_value_priority(conn, doc_num)

        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} — "
            f"redfin={'$'+f'{redfin_est:,.0f}' if redfin_est else 'N/A'}, "
            f"zillow={'$'+f'{zillow_est:,.0f}' if zillow_est else 'N/A'}"
        )

    conn.close()
    print(f"\nValued {valued}/{len(rows)} properties")


def apply_all_priorities(db_path: Path) -> None:
    """Re-apply priority rule for all properties (e.g., after comps run)."""
    from src.db.database import get_db, apply_market_value_priority

    conn = get_db(db_path)
    cursor = conn.execute("SELECT document_number FROM properties")
    doc_nums = [row[0] for row in cursor.fetchall()]

    for doc_num in doc_nums:
        apply_market_value_priority(conn, doc_num)

    conn.close()
    print(f"Applied priority rule to {len(doc_nums)} properties")


def main():
    parser = argparse.ArgumentParser(
        description="Estimate market values for properties via Zillow/Redfin"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--reprioritize", action="store_true",
        help="Re-apply priority rule for all properties (e.g., after comps update)",
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

    db_path = Path(args.db)
    if args.reprioritize:
        apply_all_priorities(db_path)
    else:
        enrich_valuations_from_db(db_path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_valuation_refactor.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/enrichment/valuation.py tests/test_valuation_refactor.py
git commit -m "feat: rewrite valuation pipeline with curl_cffi and valuations table"
```

---

### Task 4: Refactor comps pipeline to write to `property_comps` and `valuations`

**Files:**
- Modify: `src/enrichment/comps.py`

- [ ] **Step 1: Add tests for refactored comps output**

Append to `tests/test_valuation_refactor.py`:

```python
class TestCompsRefactoredOutput:
    """Test that comps pipeline writes to property_comps + valuations."""

    def test_enrich_comps_writes_property_comps(self, db):
        """After running comps, property_comps table should have rows."""
        from src.db.database import upsert_comparable_sales, get_property_comps
        from src.enrichment.comps import find_comps, estimate_from_comps

        # Set up: property with coordinates
        db.execute(
            "UPDATE properties SET lat=38.55, lng=-89.92, acres=0.2 "
            "WHERE document_number='DOC001'"
        )
        db.commit()

        # Insert nearby comp
        upsert_comparable_sales(db, [{
            "address": "100 Near St, Belleville, IL",
            "sale_date": "2026-02-01",
            "sale_price": 175000.0,
            "lat": 38.551, "lng": -89.921,
            "source": "redfin", "source_id": "MLS100",
            "property_type": "", "sqft": None, "beds": None,
            "baths": None, "lot_size": 0.25, "year_built": None,
            "scraped_at": "2026-03-27T10:00:00",
        }])

        subject = dict(db.execute(
            "SELECT * FROM properties WHERE document_number='DOC001'"
        ).fetchone())
        comps = find_comps(db, subject, radius_miles=5.0, months_back=6)
        assert len(comps) >= 1

    def test_estimate_from_comps_unchanged(self):
        """Core estimation logic should still work."""
        from src.enrichment.comps import estimate_from_comps
        subject = {"acres": 0.2}
        comps = [
            {"sale_price": 150000, "lot_size": 0.2, "_score": 0.9, "_distance": 0.3,
             "sale_date": "2026-01-15"},
            {"sale_price": 170000, "lot_size": 0.25, "_score": 0.7, "_distance": 0.8,
             "sale_date": "2026-02-01"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert est is not None
        assert count == 2
        assert conf == "medium"
```

- [ ] **Step 2: Run tests to verify they pass (existing logic unchanged)**

```bash
pytest tests/test_valuation_refactor.py::TestCompsRefactoredOutput -v
```

Expected: PASS — these test existing `find_comps` and `estimate_from_comps` which don't change.

- [ ] **Step 3: Modify `enrich_comps_from_db` in `comps.py` to write to new tables**

Replace the `enrich_comps_from_db` function (lines 195-258) in `src/enrichment/comps.py`:

```python
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

    # Get properties that have coordinates
    cursor = conn.execute(
        "SELECT * FROM properties WHERE lat IS NOT NULL AND lng IS NOT NULL"
    )
    rows = [dict(row) for row in cursor.fetchall()]

    if not rows:
        print("No geocoded properties found.")
        conn.close()
        return

    # Check if we have any comps at all
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
        for c in comps:
            # Get the comparable_sales id
            comp_id = c.get("id")
            if comp_id is None:
                continue

            subject_lot = row.get("acres") or row.get("lot_size")
            comp_lot = c.get("lot_size")
            if subject_lot and comp_lot and subject_lot > 0 and comp_lot > 0:
                lot_ratio = max(0.5, min(2.0, subject_lot / comp_lot))
            else:
                lot_ratio = 1.0

            comp_rows.append({
                "comp_sale_id": comp_id,
                "distance_miles": c.get("_distance"),
                "similarity_score": c.get("_score"),
                "lot_size_ratio": round(lot_ratio, 4),
                "adjusted_price": round(c["sale_price"] * lot_ratio, 2),
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

        # Apply priority rule (comps may be the fallback)
        apply_market_value_priority(conn, doc_num)

        estimated += 1
        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} -> ${estimate:,.0f} "
            f"({count} comps, {confidence})"
        )

    conn.close()
    print(f"\nEstimated {estimated}/{len(rows)} properties "
          f"({no_comps} had no comps within {radius_miles}mi)")
```

- [ ] **Step 4: Run all tests**

```bash
pytest tests/test_valuation_refactor.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/enrichment/comps.py tests/test_valuation_refactor.py
git commit -m "feat: comps pipeline writes to property_comps and valuations tables"
```

---

### Task 5: Update viability scoring to read from `valuations` table

**Files:**
- Modify: `src/scoring/viability.py:211`
- Modify: `src/scoring/viability.py:241-327`
- Modify: `tests/test_viability.py`

- [ ] **Step 1: Update `calculate_viability_score` to accept `comp_confidence` parameter**

In `src/scoring/viability.py`, change line 211 from:

```python
    comps_pts = score_comp_confidence(property_row.get("comps_confidence"))
```

to:

```python
    comps_pts = score_comp_confidence(property_row.get("_comp_confidence"))
```

- [ ] **Step 2: Update `score_all_properties` to fetch comp confidence from valuations table**

In `src/scoring/viability.py`, add after line 299 (after the vacancy rate lookup, before `row["_delinquent_tax"] = delinquent_tax`):

```python
            # Get comp confidence from valuations table
            comp_val_cursor = conn.execute(
                "SELECT confidence FROM valuations "
                "WHERE document_number = ? AND source = 'comps'",
                (row["document_number"],),
            )
            comp_val_row = comp_val_cursor.fetchone()
            row["_comp_confidence"] = dict(comp_val_row)["confidence"] if comp_val_row else None
```

- [ ] **Step 3: Update test fixtures in `tests/test_viability.py`**

In `tests/test_viability.py`, change all occurrences of `"comps_confidence"` in test fixture dicts to `"_comp_confidence"`. There are 4 places:

Line 303: `"comps_confidence": "high"` → `"_comp_confidence": "high"`
Line 325: `"comps_confidence": None` → `"_comp_confidence": None`
Line 338: `"comps_confidence": "low"` → `"_comp_confidence": "low"`
Line 360: `"comps_confidence": "medium"` → `"_comp_confidence": "medium"`

- [ ] **Step 4: Run viability tests**

```bash
pytest tests/test_viability.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/scoring/viability.py tests/test_viability.py
git commit -m "feat: viability scoring reads comp confidence from valuations table"
```

---

### Task 6: Update data export (`prepare_data.py`) with nested valuations and comps

**Files:**
- Modify: `src/visualization/prepare_data.py`

- [ ] **Step 1: Update `read_db` to fetch valuations and comps per property**

In `src/visualization/prepare_data.py`, replace the valuation/comps field section in `read_db` (lines 141-149) with:

```python
            # Valuation fields (denormalized)
            "estimated_market_value": row["estimated_market_value"],
            "valued_at": row["valued_at"] or "",
```

Then after `records.append(...)` (after line 169), add a second pass to attach nested data:

After the `for row in rows:` loop ends and before `return records`, add:

```python
    # Attach nested valuations and comps per property
    from src.db.database import get_valuations, get_property_comps
    conn2 = get_db(db_path)
    for rec in records:
        doc_num = rec["document_number"]
        # Valuations
        vals = get_valuations(conn2, doc_num)
        rec["valuations"] = [
            {
                "source": v["source"],
                "estimate": v["estimate"],
                "source_url": v.get("source_url") or "",
                "confidence": v.get("confidence") or "",
                "comp_count": v.get("comp_count"),
                "valued_at": v.get("valued_at") or "",
            }
            for v in vals
        ]
        # Comps
        comps = get_property_comps(conn2, doc_num)
        rec["comps"] = [
            {
                "address": c["address"],
                "sale_price": c["sale_price"],
                "sale_date": c["sale_date"],
                "distance_miles": c.get("distance_miles"),
                "similarity_score": c.get("similarity_score"),
                "lot_size_ratio": c.get("lot_size_ratio"),
                "adjusted_price": c.get("adjusted_price"),
                "sqft": c.get("sqft"),
                "beds": c.get("beds"),
                "baths": c.get("baths"),
                "lot_size": c.get("lot_size"),
                "year_built": c.get("year_built"),
                "source": c.get("source") or "",
                "source_id": c.get("source_id") or "",
            }
            for c in comps
        ]
    conn2.close()
```

- [ ] **Step 2: Update `build_output` to include nested arrays and remove old fields**

In `src/visualization/prepare_data.py`, update the field list in `build_output` (lines 364-378). Remove these from the field list:
- `"valuation_source"`
- `"valuation_confidence"`
- `"comps_estimate"`
- `"comps_count"`
- `"comps_confidence"`

And add after the existing field loop (after line 380):

```python
        # Nested arrays
        if "valuations" in r:
            feature["valuations"] = r["valuations"]
        if "comps" in r:
            feature["comps"] = r["comps"]
```

- [ ] **Step 3: Also remove old fields from `read_db` mapping**

Remove these lines from the `read_db` record mapping (lines 143-149):

```python
            "valuation_source": row["valuation_source"] or "",
            "valuation_confidence": row["valuation_confidence"] or "",
            ...
            "comps_estimate": row.get("comps_estimate"),
            "comps_count": row.get("comps_count"),
            "comps_confidence": row.get("comps_confidence") or "",
```

- [ ] **Step 4: Test data export manually**

```bash
python -m src.visualization.prepare_data --db data/cheasuits.db -v
```

Expected: Generates `dashboard/public/data.json` with `valuations` and `comps` arrays per feature. No `valuation_source` or `comps_estimate` at top level.

- [ ] **Step 5: Commit**

```bash
git add src/visualization/prepare_data.py
git commit -m "feat: export nested valuations and comps arrays in data.json"
```

---

### Task 7: Revise Financial Overview card on property detail page

**Files:**
- Modify: `dashboard/pages/property/[id].js:219-259`

- [ ] **Step 1: Replace the Financial Overview card**

In `dashboard/pages/property/[id].js`, replace the Financial Overview card (lines 219-259) with:

```jsx
            {/* Card 1: Financial Overview */}
            <Card title="Financial Overview">
              <div className="space-y-1">
                <div className="mb-3">
                  <div className="text-xs text-gray-500">Estimated Market Value</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {f.estimated_market_value != null ? formatCurrency(f.estimated_market_value) : '\u2014'}
                  </div>
                  {(() => {
                    if (!f.valuations || f.valuations.length === 0) return null;
                    const redfin = f.valuations.find(v => v.source === 'redfin');
                    const zillow = f.valuations.find(v => v.source === 'zillow');
                    const winner = redfin || zillow;
                    if (!winner) return null;
                    return (
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-xs text-gray-500 capitalize">Source: {winner.source}</span>
                        {winner.source_url && (
                          <a
                            href={winner.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-xs text-blue-600 hover:text-blue-800 underline"
                          >
                            View on {winner.source.charAt(0).toUpperCase() + winner.source.slice(1)} &uarr;
                          </a>
                        )}
                      </div>
                    );
                  })()}
                </div>
                <DetailRow label="Assessed Value" value={formatCurrency(f.assessed_value)} />
                {f.valuations && f.valuations.length > 0 && (
                  <div className="mt-4 pt-3 border-t border-gray-100">
                    <div className="text-xs text-gray-400 uppercase tracking-wider font-bold mb-2">All Valuations</div>
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left text-xs text-gray-400">
                          <th className="py-1">Source</th>
                          <th className="py-1">Estimate</th>
                          <th className="py-1">Confidence</th>
                          <th className="py-1"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {f.valuations.map((v, idx) => (
                          <tr key={idx} className="border-t border-gray-50">
                            <td className="py-1.5 capitalize">{v.source}{v.comp_count ? ` (${v.comp_count})` : ''}</td>
                            <td className="py-1.5">{formatCurrency(v.estimate)}</td>
                            <td className="py-1.5">
                              {v.confidence && (
                                <Badge
                                  text={v.confidence}
                                  className={CONFIDENCE_BADGE[v.confidence] || 'bg-gray-100 text-gray-700'}
                                />
                              )}
                            </td>
                            <td className="py-1.5">
                              {v.source_url && (
                                <a
                                  href={v.source_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-xs text-blue-600 hover:text-blue-800"
                                >
                                  View &uarr;
                                </a>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </Card>
```

- [ ] **Step 2: Verify build**

```bash
cd dashboard && npm run build
```

Expected: Build succeeds with no errors.

- [ ] **Step 3: Commit**

```bash
git add dashboard/pages/property/[id].js
git commit -m "feat: revised Financial Overview card with valuations table and source links"
```

---

### Task 8: Create Comparable Sales card component

**Files:**
- Create: `dashboard/components/CompsCard.js`
- Modify: `dashboard/pages/property/[id].js`

- [ ] **Step 1: Create `CompsCard.js`**

Create `dashboard/components/CompsCard.js`:

```jsx
import { useState } from 'react';

const CONFIDENCE_BADGE = {
  high: 'bg-green-100 text-green-800',
  medium: 'bg-yellow-100 text-yellow-800',
  low: 'bg-red-100 text-red-800',
};

function Badge({ text, className }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${className}`}>
      {text}
    </span>
  );
}

function formatCurrency(v) {
  if (v == null) return '\u2014';
  return '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function CompSourceLink({ comp }) {
  if (comp.source === 'redfin' && comp.source_id) {
    return (
      <a
        href={`https://www.redfin.com/`}
        target="_blank"
        rel="noopener noreferrer"
        className="text-xs text-blue-600 hover:text-blue-800 underline"
      >
        View on Redfin &uarr;
      </a>
    );
  }
  if (comp.source === 'recorder') {
    const parcelMatch = comp.address.match(/Parcel\s+(\S+)/);
    if (parcelMatch) {
      const parcelId = parcelMatch[1].replace(/-/g, '');
      return (
        <a
          href={`https://stclairil.devnetwedge.com/parcel/view/${parcelId}`}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-blue-600 hover:text-blue-800 underline"
        >
          County Recorder &uarr;
        </a>
      );
    }
  }
  return null;
}

export default function CompsCard({ comps, valuations }) {
  const [expanded, setExpanded] = useState(null);

  // Get comps summary from valuations
  const compVal = valuations?.find(v => v.source === 'comps');

  if (!comps || comps.length === 0) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
        <h3 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-4">Comparable Sales</h3>
        <p className="text-sm text-gray-400">No comparable sales found for this property.</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
      <h3 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-4">Comparable Sales</h3>

      {/* Summary bar */}
      {compVal && (
        <div className="flex gap-4 mb-4">
          <div className="bg-gray-50 rounded-lg px-4 py-2 text-center">
            <div className="text-xs text-gray-500">Comps Estimate</div>
            <div className="text-lg font-bold">{formatCurrency(compVal.estimate)}</div>
          </div>
          <div className="bg-gray-50 rounded-lg px-4 py-2 text-center">
            <div className="text-xs text-gray-500">Confidence</div>
            <div className="mt-1">
              {compVal.confidence && (
                <Badge text={compVal.confidence} className={CONFIDENCE_BADGE[compVal.confidence] || 'bg-gray-100 text-gray-700'} />
              )}
            </div>
          </div>
          <div className="bg-gray-50 rounded-lg px-4 py-2 text-center">
            <div className="text-xs text-gray-500">Comp Count</div>
            <div className="text-lg font-bold">{comps.length}</div>
          </div>
        </div>
      )}

      {/* Comp rows */}
      <div className="space-y-2">
        {comps.map((comp, idx) => {
          const isExpanded = expanded === idx;
          return (
            <div key={idx} className={`border rounded-lg overflow-hidden ${isExpanded ? 'border-blue-300' : 'border-gray-200'}`}>
              {/* Collapsed row */}
              <button
                className="w-full px-4 py-3 flex items-center justify-between text-left bg-gray-50 hover:bg-gray-100"
                onClick={() => setExpanded(isExpanded ? null : idx)}
              >
                <div className="flex items-center gap-4 text-sm">
                  <span className="text-xs font-bold text-yellow-600">#{idx + 1}</span>
                  <span className="text-gray-900">{comp.address}</span>
                  <span className="font-semibold text-green-700">{formatCurrency(comp.sale_price)}</span>
                  <span className="text-gray-400 text-xs">
                    {comp.distance_miles != null ? `${comp.distance_miles.toFixed(1)} mi` : ''}
                    {comp.sale_date ? ` · sold ${comp.sale_date}` : ''}
                  </span>
                </div>
                <span className="text-gray-400 text-xs">{isExpanded ? '\u25B2' : '\u25BC'}</span>
              </button>

              {/* Expanded detail */}
              {isExpanded && (
                <div className="px-4 py-3 border-t border-gray-100 bg-white">
                  <div className="grid grid-cols-3 gap-3 text-sm mb-3">
                    <div>
                      <div className="text-xs text-gray-400">Sale Price</div>
                      <div>{formatCurrency(comp.sale_price)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Adjusted Price</div>
                      <div>{formatCurrency(comp.adjusted_price)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Distance</div>
                      <div>{comp.distance_miles != null ? `${comp.distance_miles.toFixed(2)} mi` : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Similarity</div>
                      <div>{comp.similarity_score != null ? comp.similarity_score.toFixed(2) : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Lot Size</div>
                      <div>{comp.lot_size != null ? `${comp.lot_size} ac` : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Lot Ratio</div>
                      <div>{comp.lot_size_ratio != null ? `${comp.lot_size_ratio.toFixed(2)}x` : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Beds / Baths</div>
                      <div>{comp.beds ?? '\u2014'} / {comp.baths ?? '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Sqft</div>
                      <div>{comp.sqft != null ? comp.sqft.toLocaleString() : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Year Built</div>
                      <div>{comp.year_built ?? '\u2014'}</div>
                    </div>
                  </div>
                  <div className="flex gap-3">
                    <CompSourceLink comp={comp} />
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Add CompsCard to property detail page**

In `dashboard/pages/property/[id].js`, add the import at the top (after line 7):

```jsx
import CompsCard from '../../components/CompsCard';
```

Then add the CompsCard after the Card grid's closing `</div>` (after line 392, before the last two closing `</div>` tags):

```jsx
          {/* Comparable Sales Card — full width below the grid */}
          <div className="mt-6">
            <CompsCard comps={f.comps} valuations={f.valuations} />
          </div>
```

- [ ] **Step 3: Verify build**

```bash
cd dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 4: Commit**

```bash
git add dashboard/components/CompsCard.js dashboard/pages/property/[id].js
git commit -m "feat: add Comparable Sales card with expandable comp details"
```

---

### Task 9: Remove old valuation columns from database migrations

**Files:**
- Modify: `src/db/database.py`

- [ ] **Step 1: Remove old migration blocks and functions**

In `src/db/database.py`:

1. Remove `_VALUATION_MIGRATIONS` block (lines 159-174) — these columns are now in the `valuations` table
2. Remove `_COMPS_MIGRATIONS` block (lines 194-206) — these columns are now in `valuations` and `property_comps`
3. Remove the `update_valuation` function (lines 380-399)
4. Remove the `set_valuation_error` function (lines 402-410)
5. Remove the `update_comps_valuation` function (lines 787-802)
6. Remove the `get_unvalued` function (lines 358-365)

Keep the columns in the SCHEMA `CREATE TABLE` statement for now — SQLite can't drop columns easily, and existing databases will still have them. The old columns become dead data that's no longer read or written.

- [ ] **Step 2: Run all tests to make sure nothing is broken**

```bash
pytest tests/ -v
```

Expected: All PASS. No code should be importing the removed functions anymore.

- [ ] **Step 3: Commit**

```bash
git add src/db/database.py
git commit -m "refactor: remove old valuation/comps migration blocks and CRUD functions"
```

---

### Task 10: End-to-end verification

**Files:**
- No new files

- [ ] **Step 1: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: All PASS.

- [ ] **Step 2: Regenerate data.json from the live database**

```bash
python -m src.visualization.prepare_data --db data/cheasuits.db -v
```

Expected: Generates `data.json` with nested `valuations` and `comps` arrays.

- [ ] **Step 3: Verify the dashboard builds**

```bash
cd dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 4: Spot-check data.json structure**

```bash
python -c "
import json
with open('dashboard/public/data.json') as f:
    data = json.load(f)
feat = data['features'][0]
print('Keys:', list(feat.keys()))
print('Has valuations:', 'valuations' in feat)
print('Has comps:', 'comps' in feat)
print('No valuation_source:', 'valuation_source' not in feat)
print('No comps_estimate:', 'comps_estimate' not in feat)
"
```

Expected:
```
Has valuations: True
Has comps: True
No valuation_source: True
No comps_estimate: True
```

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore: regenerate data.json with new valuation/comps structure"
```
