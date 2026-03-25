# AVM Valuation Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an AVM module that estimates as-is market value for lis pendens properties using a county assessed-value multiplier with Zillow/Redfin scrape fallback.

**Architecture:** New `src/enrichment/valuation.py` module follows the same enrichment pattern as `assessor.py`. Pulls unvalued properties from SQLite, computes assessed multiplier, scrapes external estimates, blends into a final value with confidence rating, writes back. `prepare_data.py` exports the new fields to `data.json`.

**Tech Stack:** Python 3.11+, `urllib.request`, `json`, `re`, `sqlite3`, `pytest`

**Spec:** `docs/superpowers/specs/2026-03-25-avm-valuation-design.md`

---

### Task 1: Add valuation columns to database schema

**Files:**
- Modify: `src/db/database.py:10-46` (SCHEMA string, add columns to properties table)
- Modify: `src/db/database.py` (add `get_unvalued`, `update_valuation`, `set_valuation_error` helpers)
- Test: `tests/test_database.py`

- [ ] **Step 1: Write failing tests for new DB helpers**

Add to `tests/test_database.py`:

```python
from src.db.database import (
    get_db,
    upsert_records,
    get_all,
    update_enrichment,
    get_unvalued,
    update_valuation,
    set_valuation_error,
)


# (reuse existing SAMPLE_RECORD and db fixture)

class TestGetUnvalued:
    def test_returns_unvalued_with_assessed_value(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_enrichment(db, "2224358", {
            "assessed_value": 12952.0,
            "property_address": "209 Edwards St\nCahokia, IL 62206",
        })
        rows = get_unvalued(db)
        assert len(rows) == 1

    def test_excludes_already_valued(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_enrichment(db, "2224358", {"assessed_value": 12952.0})
        update_valuation(db, "2224358", {
            "assessed_multiplier_value": 38856.0,
            "estimated_market_value": 38856.0,
            "valuation_source": "assessed_multiplier",
            "valuation_confidence": "medium",
        })
        rows = get_unvalued(db)
        assert len(rows) == 0

    def test_excludes_no_assessed_value(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_unvalued(db)
        assert len(rows) == 0

    def test_excludes_errored(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_enrichment(db, "2224358", {"assessed_value": 12952.0})
        set_valuation_error(db, "2224358", "all methods failed")
        rows = get_unvalued(db)
        assert len(rows) == 0


class TestUpdateValuation:
    def test_sets_fields_and_timestamp(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_valuation(db, "2224358", {
            "assessed_multiplier_value": 38856.0,
            "zillow_estimate": 42000.0,
            "estimated_market_value": 42000.0,
            "valuation_source": "zillow",
            "valuation_confidence": "high",
        })
        rows = get_all(db)
        row = rows[0]
        assert row["estimated_market_value"] == 42000.0
        assert row["valuation_source"] == "zillow"
        assert row["valuation_confidence"] == "high"
        assert row["valued_at"] is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_database.py::TestGetUnvalued tests/test_database.py::TestUpdateValuation -v`
Expected: FAIL — `ImportError: cannot import name 'get_unvalued'`

- [ ] **Step 3: Add valuation columns to SCHEMA**

In `src/db/database.py`, add these columns to the `properties` CREATE TABLE (after line 40, before the closing `);`):

```python
    assessed_multiplier_value REAL,
    zillow_estimate REAL,
    redfin_estimate REAL,
    estimated_market_value REAL,
    valuation_source TEXT,
    valuation_confidence TEXT,
    valued_at TEXT,
    valuation_error TEXT
```

Also add an index after the existing indexes (after line 46):

```python
CREATE INDEX IF NOT EXISTS idx_valued_at ON properties(valued_at);
```

**Important:** Since the existing `data/cheasuits.db` already has the `properties` table without these columns, `CREATE TABLE IF NOT EXISTS` will silently skip. Add migration logic to `get_db()` right after the `conn.executescript(SCHEMA)` call:

```python
# Migrate: add valuation columns to existing databases
_VALUATION_MIGRATIONS = [
    "ALTER TABLE properties ADD COLUMN assessed_multiplier_value REAL",
    "ALTER TABLE properties ADD COLUMN zillow_estimate REAL",
    "ALTER TABLE properties ADD COLUMN redfin_estimate REAL",
    "ALTER TABLE properties ADD COLUMN estimated_market_value REAL",
    "ALTER TABLE properties ADD COLUMN valuation_source TEXT",
    "ALTER TABLE properties ADD COLUMN valuation_confidence TEXT",
    "ALTER TABLE properties ADD COLUMN valued_at TEXT",
    "ALTER TABLE properties ADD COLUMN valuation_error TEXT",
]
for stmt in _VALUATION_MIGRATIONS:
    try:
        conn.execute(stmt)
    except sqlite3.OperationalError:
        pass  # column already exists
conn.commit()
```

Both the SCHEMA update (for fresh DBs) and the migration (for existing DBs) are needed.

- [ ] **Step 4: Add `get_unvalued` function**

In `src/db/database.py`, after `get_ungeocoded` (~line 176):

```python
def get_unvalued(conn: sqlite3.Connection) -> list[dict]:
    """Get rows that need valuation (have assessed_value but no valuation yet)."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE valued_at IS NULL AND valuation_error IS NULL "
        "AND assessed_value IS NOT NULL"
    )
    return [dict(row) for row in cursor.fetchall()]
```

- [ ] **Step 5: Add `update_valuation` function**

In `src/db/database.py`, after `update_geocoding`:

```python
def update_valuation(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update valuation fields and set valued_at timestamp."""
    allowed = {
        "assessed_multiplier_value", "zillow_estimate", "redfin_estimate",
        "estimated_market_value", "valuation_source", "valuation_confidence",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["valued_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()
```

- [ ] **Step 6: Add `set_valuation_error` function**

In `src/db/database.py`, after `update_valuation`:

```python
def set_valuation_error(
    conn: sqlite3.Connection, document_number: str, error: str
) -> None:
    """Record a valuation failure so the property is skipped on re-run."""
    conn.execute(
        "UPDATE properties SET valuation_error = ? WHERE document_number = ?",
        (error, document_number),
    )
    conn.commit()
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `pytest tests/test_database.py -v`
Expected: ALL PASS

- [ ] **Step 8: Commit**

```bash
git add src/db/database.py tests/test_database.py
git commit -m "feat: add valuation columns and DB helpers to properties schema"
```

---

### Task 2: Implement assessed value multiplier and blending logic

**Files:**
- Create: `src/enrichment/valuation.py`
- Create: `tests/test_valuation.py`

- [ ] **Step 1: Write failing tests for `compute_assessed_multiplier`**

Create `tests/test_valuation.py`:

```python
"""Tests for src.enrichment.valuation — AVM valuation module."""

import urllib.error

import pytest
from unittest.mock import patch, MagicMock

from src.enrichment.valuation import (
    compute_assessed_multiplier,
    blend_estimates,
)


class TestComputeAssessedMultiplier:
    def test_basic_calculation(self):
        # $44,000 assessed * 1.0049 state multiplier / 0.3333 = ~$132,650
        result = compute_assessed_multiplier(44000.0, 1.0049)
        assert abs(result - 132650.0) < 100  # within $100

    def test_zero_assessed(self):
        result = compute_assessed_multiplier(0.0, 1.0049)
        assert result == 0.0

    def test_multiplier_of_one(self):
        # $10,000 * 1.0 / 0.3333 = ~$30,003
        result = compute_assessed_multiplier(10000.0, 1.0)
        assert abs(result - 30003.0) < 10
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_valuation.py::TestComputeAssessedMultiplier -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.enrichment.valuation'`

- [ ] **Step 3: Implement `compute_assessed_multiplier`**

Create `src/enrichment/valuation.py`:

```python
"""AVM Valuation Module — Assessed Value Multiplier + Zillow/Redfin Hybrid.

Estimates as-is market value for properties in the pipeline database.
Uses county assessed values (with state equalization multiplier) as baseline,
supplemented by Zillow/Redfin public estimates where available.

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
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# St. Clair County, IL
# State equalization multiplier — published annually by IL Dept. of Revenue
# https://tax.illinois.gov (search "equalization factor")
# The Board of Review Equalized value from DevNetWedge already includes
# county-level equalization. This multiplier is the STATE-level correction.
STCLAIR_STATE_MULTIPLIER = 1.0049  # Tax year 2024 — verify annually
ASSESSMENT_RATIO = 1 / 3  # Illinois statutory 33.33%

SCRAPE_DELAY = 1.0  # seconds between external requests
MAX_RETRIES = 2
DIVERGENCE_THRESHOLD = 0.5  # 50% divergence triggers low confidence

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# Assessed Value Multiplier
# ---------------------------------------------------------------------------

def compute_assessed_multiplier(assessed_value: float, state_multiplier: float) -> float:
    """Compute estimated market value from county assessed value.

    Formula: assessed_value * state_multiplier / ASSESSMENT_RATIO
    """
    return round(assessed_value * state_multiplier / ASSESSMENT_RATIO, 2)
```

- [ ] **Step 4: Run multiplier tests to verify they pass**

Run: `pytest tests/test_valuation.py::TestComputeAssessedMultiplier -v`
Expected: ALL PASS

- [ ] **Step 5: Write failing tests for `blend_estimates`**

Add to `tests/test_valuation.py`:

```python
class TestBlendEstimates:
    """Tests covering all 7 rows of the decision matrix."""

    def test_both_external_within_threshold(self):
        # Both Zillow and Redfin, within 50% of assessed mult
        value, source, confidence = blend_estimates(100000.0, 110000.0, 105000.0)
        assert value == 107500.0  # avg(110000, 105000)
        assert source == "blended"
        assert confidence == "high"

    def test_both_external_exceeds_threshold(self):
        # Both external, but diverge >50% from assessed
        value, source, confidence = blend_estimates(50000.0, 110000.0, 105000.0)
        assert value == 107500.0  # still uses avg of external
        assert source == "blended"
        assert confidence == "low"

    def test_zillow_only_within_threshold(self):
        value, source, confidence = blend_estimates(100000.0, 110000.0, None)
        assert value == 110000.0
        assert source == "zillow"
        assert confidence == "high"

    def test_zillow_only_exceeds_threshold(self):
        value, source, confidence = blend_estimates(50000.0, 110000.0, None)
        assert value == 110000.0
        assert source == "zillow"
        assert confidence == "low"

    def test_redfin_only_within_threshold(self):
        value, source, confidence = blend_estimates(100000.0, None, 95000.0)
        assert value == 95000.0
        assert source == "redfin"
        assert confidence == "high"

    def test_redfin_only_exceeds_threshold(self):
        value, source, confidence = blend_estimates(50000.0, None, 120000.0)
        assert value == 120000.0
        assert source == "redfin"
        assert confidence == "low"

    def test_no_external_fallback_to_assessed(self):
        value, source, confidence = blend_estimates(100000.0, None, None)
        assert value == 100000.0
        assert source == "assessed_multiplier"
        assert confidence == "medium"
```

- [ ] **Step 6: Run blend tests to verify they fail**

Run: `pytest tests/test_valuation.py::TestBlendEstimates -v`
Expected: FAIL — `ImportError: cannot import name 'blend_estimates'`

- [ ] **Step 7: Implement `blend_estimates`**

Add to `src/enrichment/valuation.py`:

```python
# ---------------------------------------------------------------------------
# Blending Logic
# ---------------------------------------------------------------------------

def blend_estimates(
    assessed_mult: float,
    zillow: Optional[float],
    redfin: Optional[float],
) -> tuple[float, str, str]:
    """Blend valuation signals into a final estimate.

    Returns: (estimated_market_value, valuation_source, valuation_confidence)

    Decision matrix:
    - Both external available → average, check divergence from assessed
    - One external available → use it, check divergence from assessed
    - No external → fall back to assessed multiplier with "medium" confidence
    """
    if zillow is not None and redfin is not None:
        value = round((zillow + redfin) / 2, 2)
        source = "blended"
    elif zillow is not None:
        value = zillow
        source = "zillow"
    elif redfin is not None:
        value = redfin
        source = "redfin"
    else:
        return (assessed_mult, "assessed_multiplier", "medium")

    # Check divergence between external estimate and assessed multiplier
    if assessed_mult > 0:
        divergence = abs(value - assessed_mult) / assessed_mult
        confidence = "low" if divergence > DIVERGENCE_THRESHOLD else "high"
    else:
        confidence = "high"  # can't check divergence without assessed baseline

    return (value, source, confidence)
```

- [ ] **Step 8: Run all valuation tests**

Run: `pytest tests/test_valuation.py -v`
Expected: ALL PASS

- [ ] **Step 9: Commit**

```bash
git add src/enrichment/valuation.py tests/test_valuation.py
git commit -m "feat: add assessed multiplier and blending logic for AVM"
```

---

### Task 3: Implement Redfin scraper

**Files:**
- Modify: `src/enrichment/valuation.py` (add `fetch_redfin_estimate`)
- Modify: `tests/test_valuation.py` (add mocked scraper tests)

- [ ] **Step 1: Write failing test for `fetch_redfin_estimate`**

Add to `tests/test_valuation.py` (imports `urllib.error`, `patch`, `MagicMock` are already at the top from Task 2):

```python
from src.enrichment.valuation import fetch_redfin_estimate


class TestFetchRedfinEstimate:
    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_extracts_estimate_from_page(self, mock_urlopen):
        # Mock autocomplete response
        autocomplete_resp = MagicMock()
        autocomplete_resp.read.return_value = b'{}&&{"payload":{"sections":[{"rows":[{"url":"/IL/Belleville/209-Edwards-St-62220/home/12345"}]}]}}'
        autocomplete_resp.__enter__ = lambda s: s
        autocomplete_resp.__exit__ = MagicMock(return_value=False)

        # Mock property page with estimate in script tag
        page_html = b"""<html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"initialRedfinEstimateValue":125000}}}
        </script>
        </body></html>"""
        page_resp = MagicMock()
        page_resp.read.return_value = page_html
        page_resp.__enter__ = lambda s: s
        page_resp.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [autocomplete_resp, page_resp]

        result = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result == 125000.0

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_no_autocomplete(self, mock_urlopen):
        autocomplete_resp = MagicMock()
        autocomplete_resp.read.return_value = b'{}&&{"payload":{"sections":[]}}'
        autocomplete_resp.__enter__ = lambda s: s
        autocomplete_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = autocomplete_resp

        result = fetch_redfin_estimate("999 Nonexistent Rd, Nowhere, IL 00000")
        assert result is None

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "http://example.com", 403, "Forbidden", {}, None
        )
        result = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_valuation.py::TestFetchRedfinEstimate -v`
Expected: FAIL — `ImportError: cannot import name 'fetch_redfin_estimate'`

- [ ] **Step 3: Implement `fetch_redfin_estimate`**

Add to `src/enrichment/valuation.py` (note: `import random` is already at the top of the file from Task 2):

```python
# ---------------------------------------------------------------------------
# Address Normalization
# ---------------------------------------------------------------------------

def _normalize_address(raw: str) -> str:
    """Flatten multi-line DB address to single line for URL queries."""
    return raw.replace("\n", ", ").strip()


def _get_user_agent() -> str:
    """Return a random user agent string."""
    return random.choice(USER_AGENTS)


# ---------------------------------------------------------------------------
# Redfin Scraper
# ---------------------------------------------------------------------------

def fetch_redfin_estimate(address: str) -> Optional[float]:
    """Fetch Redfin Estimate for a property address.

    1. Hit autocomplete endpoint to resolve address → property URL
    2. Fetch property page
    3. Extract estimate from __NEXT_DATA__ JSON

    Returns estimated value or None if unavailable.
    """
    normalized = _normalize_address(address)
    encoded = urllib.parse.quote(normalized)

    # Step 1: Autocomplete to get property URL
    autocomplete_url = (
        f"https://www.redfin.com/stingray/do/location-autocomplete"
        f"?v=2&al=1&location={encoded}"
    )

    try:
        req = urllib.request.Request(autocomplete_url, headers={
            "User-Agent": _get_user_agent(),
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")

        # Redfin prefixes response with '{}&&'
        json_str = raw.split("&&", 1)[-1] if "&&" in raw else raw
        data = json.loads(json_str)

        sections = data.get("payload", {}).get("sections", [])
        if not sections:
            logger.debug(f"Redfin: no autocomplete results for {normalized}")
            return None

        rows = sections[0].get("rows", [])
        if not rows:
            logger.debug(f"Redfin: no rows in autocomplete for {normalized}")
            return None

        property_url = rows[0].get("url", "")
        if not property_url:
            return None

    except Exception as e:
        logger.warning(f"Redfin autocomplete failed for {normalized}: {e}")
        return None

    # Step 2: Fetch property page
    try:
        page_url = f"https://www.redfin.com{property_url}"
        req = urllib.request.Request(page_url, headers={
            "User-Agent": _get_user_agent(),
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")

    except Exception as e:
        logger.warning(f"Redfin page fetch failed for {property_url}: {e}")
        return None

    # Step 3: Extract estimate from __NEXT_DATA__ or page content
    try:
        # Try __NEXT_DATA__ JSON blob
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL
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
                return float(estimate)

        # Fallback: search for avm pattern in HTML
        avm_match = re.search(r'"avm":\s*\{[^}]*"amount":\s*(\d+)', html)
        if avm_match:
            estimate = float(avm_match.group(1))
            if estimate > 0:
                logger.info(f"Redfin AVM for {normalized}: ${estimate:,.0f}")
                return estimate

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Redfin parse failed for {normalized}: {e}")

    logger.debug(f"Redfin: no estimate found for {normalized}")
    return None
```

- [ ] **Step 4: Run Redfin tests to verify they pass**

Run: `pytest tests/test_valuation.py::TestFetchRedfinEstimate -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/enrichment/valuation.py tests/test_valuation.py
git commit -m "feat: add Redfin estimate scraper for AVM"
```

---

### Task 4: Implement Zillow scraper

**Files:**
- Modify: `src/enrichment/valuation.py` (add `fetch_zillow_estimate`)
- Modify: `tests/test_valuation.py` (add mocked scraper tests)

- [ ] **Step 1: Write failing test for `fetch_zillow_estimate`**

Add to `tests/test_valuation.py`:

```python
from src.enrichment.valuation import fetch_zillow_estimate


class TestFetchZillowEstimate:
    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_extracts_zestimate_from_json_ld(self, mock_urlopen):
        page_html = b"""<html><body>
        <script type="application/json" id="__NEXT_DATA__">
        {"props":{"pageProps":{"componentProps":{"gdpClientCache":{"\\"zpid\\"123":{"property":{"zestimate":145000}}}}}}}
        </script>
        </body></html>"""
        mock_resp = MagicMock()
        mock_resp.read.return_value = page_html
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result == 145000.0

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_extracts_zestimate_from_regex_fallback(self, mock_urlopen):
        page_html = b"""<html><body>
        <script>"zestimate":98500,"zestimateLowPercent"</script>
        </body></html>"""
        mock_resp = MagicMock()
        mock_resp.read.return_value = page_html
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result == 98500.0

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "http://example.com", 403, "Forbidden", {}, None
        )
        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result is None

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_no_estimate(self, mock_urlopen):
        page_html = b"<html><body>No data here</body></html>"
        mock_resp = MagicMock()
        mock_resp.read.return_value = page_html
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_valuation.py::TestFetchZillowEstimate -v`
Expected: FAIL — `ImportError: cannot import name 'fetch_zillow_estimate'`

- [ ] **Step 3: Implement `fetch_zillow_estimate`**

Add to `src/enrichment/valuation.py`:

```python
# ---------------------------------------------------------------------------
# Zillow Scraper
# ---------------------------------------------------------------------------

def fetch_zillow_estimate(address: str) -> Optional[float]:
    """Fetch Zillow Zestimate for a property address.

    1. Construct search URL from address
    2. Fetch page
    3. Extract Zestimate from __NEXT_DATA__ JSON or regex fallback

    Returns estimated value or None if unavailable.
    """
    normalized = _normalize_address(address)

    # Format address for Zillow URL: "209 Edwards St, Cahokia, IL 62206" -> "209-Edwards-St,-Cahokia,-IL-62206"
    slug = re.sub(r'[^\w,\s-]', '', normalized)
    slug = re.sub(r'[\s]+', '-', slug.strip())
    url = f"https://www.zillow.com/homes/{slug}_rb/"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": _get_user_agent(),
            "Accept": "text/html,application/xhtml+xml",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")

    except Exception as e:
        logger.warning(f"Zillow fetch failed for {normalized}: {e}")
        return None

    # Try __NEXT_DATA__ JSON blob
    try:
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            page_data = json.loads(match.group(1))
            # Navigate the nested gdpClientCache structure
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
                        return float(zest)

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.debug(f"Zillow JSON parse failed for {normalized}: {e}")

    # Regex fallback: find "zestimate":NNNNN in page content
    zest_match = re.search(r'"zestimate"\s*:\s*(\d+)', html)
    if zest_match:
        value = float(zest_match.group(1))
        if value > 0:
            logger.info(f"Zillow Zestimate (regex) for {normalized}: ${value:,.0f}")
            return value

    logger.debug(f"Zillow: no Zestimate found for {normalized}")
    return None
```

- [ ] **Step 4: Run Zillow tests to verify they pass**

Run: `pytest tests/test_valuation.py::TestFetchZillowEstimate -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/enrichment/valuation.py tests/test_valuation.py
git commit -m "feat: add Zillow Zestimate scraper for AVM"
```

---

### Task 5: Implement enrichment orchestrator and CLI

**Files:**
- Modify: `src/enrichment/valuation.py` (add `enrich_valuations_from_db` and `main`)
- Modify: `tests/test_valuation.py` (add integration test)

- [ ] **Step 1: Write failing test for `enrich_valuations_from_db`**

Add to `tests/test_valuation.py`:

```python
from src.enrichment.valuation import enrich_valuations_from_db
from src.db.database import (
    get_db, upsert_records, update_enrichment, get_all,
)


SAMPLE_RECORD = {
    "document_number": "2224358",
    "case_number": "26-FC-121",
    "case_type": "FC",
    "case_year": "2026",
    "recorded_date": "2026-03-23",
    "party1": "",
    "party2": "ALLEN RUTH",
    "parcel_id": "01-35-0-402-022",
    "subdivision": "EDWARD PLACE L: 28",
    "legals_raw": "",
    "source": "ava_search_stclair",
    "scraped_at": "2026-03-23T20:19:53",
}


class TestEnrichValuationsFromDb:
    @patch("src.enrichment.valuation.fetch_redfin_estimate", return_value=None)
    @patch("src.enrichment.valuation.fetch_zillow_estimate", return_value=None)
    def test_assessed_multiplier_fallback(self, mock_zillow, mock_redfin, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        upsert_records(conn, [SAMPLE_RECORD])
        update_enrichment(conn, "2224358", {
            "assessed_value": 12952.0,
            "property_address": "209 Edwards St\nCahokia, IL 62206",
        })
        conn.close()

        enrich_valuations_from_db(db_path)

        conn = get_db(db_path)
        rows = get_all(conn)
        conn.close()

        row = rows[0]
        assert row["estimated_market_value"] is not None
        assert row["estimated_market_value"] > 0
        assert row["valuation_source"] == "assessed_multiplier"
        assert row["valuation_confidence"] == "medium"
        assert row["valued_at"] is not None

    @patch("src.enrichment.valuation.fetch_redfin_estimate", return_value=42000.0)
    @patch("src.enrichment.valuation.fetch_zillow_estimate", return_value=None)
    def test_uses_redfin_when_available(self, mock_zillow, mock_redfin, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        upsert_records(conn, [SAMPLE_RECORD])
        update_enrichment(conn, "2224358", {
            "assessed_value": 12952.0,
            "property_address": "209 Edwards St\nCahokia, IL 62206",
        })
        conn.close()

        enrich_valuations_from_db(db_path)

        conn = get_db(db_path)
        rows = get_all(conn)
        conn.close()

        row = rows[0]
        assert row["estimated_market_value"] == 42000.0
        assert row["valuation_source"] == "redfin"
        assert row["redfin_estimate"] == 42000.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_valuation.py::TestEnrichValuationsFromDb -v`
Expected: FAIL — `ImportError: cannot import name 'enrich_valuations_from_db'`

- [ ] **Step 3: Implement `enrich_valuations_from_db`**

Add to `src/enrichment/valuation.py`:

```python
# ---------------------------------------------------------------------------
# Enrichment Orchestrator
# ---------------------------------------------------------------------------

def enrich_valuations_from_db(db_path: Path) -> None:
    """Fetch valuations for all unvalued properties in the database."""
    from src.db.database import (
        get_db, get_unvalued, update_valuation, set_valuation_error,
    )

    conn = get_db(db_path)
    rows = get_unvalued(conn)

    if not rows:
        print("No unvalued properties found.")
        conn.close()
        return

    print(f"Valuing {len(rows)} properties...")

    valued = 0
    failed = 0
    request_count = 0

    for i, row in enumerate(rows):
        doc_num = row["document_number"]
        assessed_value = row["assessed_value"]
        address = row.get("property_address", "")

        # Step 1: Assessed multiplier (always available)
        assessed_mult = compute_assessed_multiplier(
            assessed_value, STCLAIR_STATE_MULTIPLIER
        )

        # Step 2: Try external estimates (only if address available)
        redfin_est = None
        zillow_est = None

        if address:
            normalized_addr = _normalize_address(address)

            # Try Redfin
            if request_count > 0:
                time.sleep(SCRAPE_DELAY)
            redfin_est = fetch_redfin_estimate(normalized_addr)
            request_count += 1

            # Always try Zillow too (enables "blended" when both succeed)
            time.sleep(SCRAPE_DELAY)
            zillow_est = fetch_zillow_estimate(normalized_addr)
            request_count += 1

        # Step 3: Blend
        est_value, source, confidence = blend_estimates(
            assessed_mult, zillow_est, redfin_est
        )

        if est_value <= 0:
            set_valuation_error(conn, doc_num, "all methods returned zero or negative")
            failed += 1
            logger.warning(f"[{i+1}/{len(rows)}] {doc_num} -> FAILED (zero value)")
            continue

        fields = {
            "assessed_multiplier_value": assessed_mult,
            "estimated_market_value": est_value,
            "valuation_source": source,
            "valuation_confidence": confidence,
        }
        if zillow_est is not None:
            fields["zillow_estimate"] = zillow_est
        if redfin_est is not None:
            fields["redfin_estimate"] = redfin_est

        update_valuation(conn, doc_num, fields)
        valued += 1
        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} -> ${est_value:,.0f} "
            f"({source}, {confidence})"
        )

    conn.close()
    print(f"\nValued {valued}/{len(rows)} properties ({failed} failed)")
```

- [ ] **Step 4: Add CLI `main` function**

Add to `src/enrichment/valuation.py`:

```python
def main():
    parser = argparse.ArgumentParser(
        description="Estimate market values for properties in the pipeline DB"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})",
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

    enrich_valuations_from_db(Path(args.db))


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run enrichment tests to verify they pass**

Run: `pytest tests/test_valuation.py::TestEnrichValuationsFromDb -v`
Expected: ALL PASS

- [ ] **Step 6: Run full test suite**

Run: `pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add src/enrichment/valuation.py tests/test_valuation.py
git commit -m "feat: add valuation enrichment orchestrator and CLI"
```

---

### Task 6: Wire valuation data into prepare_data.py export

**Files:**
- Modify: `src/visualization/prepare_data.py:117-141` (`read_db` function, add valuation fields)
- Modify: `src/visualization/prepare_data.py:335-341` (`build_output` function, include valuation in features)

- [ ] **Step 1: Add valuation fields to `read_db` record mapping**

In `src/visualization/prepare_data.py`, in the `read_db` function, add after the `"acres"` line (~line 140):

```python
            # Valuation fields
            "estimated_market_value": row["estimated_market_value"],
            "valuation_source": row["valuation_source"] or "",
            "valuation_confidence": row["valuation_confidence"] or "",
            "valued_at": row["valued_at"] or "",
```

- [ ] **Step 2: Add valuation fields to `build_output` feature inclusion**

In `src/visualization/prepare_data.py`, in the `build_output` function, update the field inclusion loop (~line 336). Add the valuation fields to the tuple:

```python
        for field in ("owner_name", "property_address", "mailing_address",
                      "absentee_owner", "assessed_value", "net_taxable_value",
                      "tax_rate", "total_tax",
                      "tax_status", "property_class", "acres",
                      "estimated_market_value", "valuation_source",
                      "valuation_confidence", "valued_at"):
```

- [ ] **Step 3: Verify prepare_data still works**

Run: `python -m src.visualization.prepare_data --db data/cheasuits.db -v`
Expected: Generates `dashboard/public/data.json` without errors. New valuation fields appear as `null` for unvalued properties.

- [ ] **Step 4: Commit**

```bash
git add src/visualization/prepare_data.py
git commit -m "feat: include valuation fields in data.json export"
```

---

### Task 7: End-to-end smoke test

**Files:**
- No new files — run the full pipeline against the real database

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 2: Run valuation against the real DB (dry check)**

Run: `python -m src.enrichment.valuation --db data/cheasuits.db -v`
Expected: Outputs "Valuing N properties..." and processes them. Assessed multiplier values appear for all. External estimates may or may not succeed (depends on Redfin/Zillow availability).

- [ ] **Step 3: Verify data.json includes valuation data**

Run: `python -m src.visualization.prepare_data --db data/cheasuits.db`
Then inspect the output:
Run: `python3 -c "import json; d=json.load(open('dashboard/public/data.json')); f=d['features'][0]; print(f.get('estimated_market_value'), f.get('valuation_source'), f.get('valuation_confidence'))"`
Expected: Prints a dollar value, source, and confidence level.

- [ ] **Step 4: Commit any fixes**

If any adjustments were needed, commit them:

```bash
git add -u
git commit -m "fix: adjustments from end-to-end valuation smoke test"
```
