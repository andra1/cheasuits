# Comps Scoring Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign comps scoring to prioritize sqft similarity (50%), add a 30% sqft hard filter, replace lot-size adjustment with sqft-based adjustment, and add a Zillow recently sold scraper to expand the comps pool.

**Architecture:** Rewrite `_score_comp` and `estimate_from_comps` in `comps.py` with new weights and sqft-based logic. Update `enrich_comps_from_db` to use sqft ratio instead of lot ratio. Create `comps_zillow.py` as a new comp source following the same pattern as `comps_redfin.py`.

**Tech Stack:** Python 3.11+, SQLite, curl_cffi, BeautifulSoup (existing)

**Spec:** `docs/superpowers/specs/2026-03-27-comps-scoring-redesign.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/enrichment/comps.py` | New weights, hard sqft filter, sqft scoring, sqft price adjustment |
| Create | `src/enrichment/comps_zillow.py` | Zillow recently sold scraper |
| Create | `tests/test_comps_scoring.py` | Tests for new scoring logic |

---

### Task 1: Rewrite comps scoring with sqft-primary weights and hard filter

**Files:**
- Modify: `src/enrichment/comps.py:21-100`
- Create: `tests/test_comps_scoring.py`

- [ ] **Step 1: Write failing tests for new scoring logic**

Create `tests/test_comps_scoring.py`:

```python
"""Tests for comps scoring redesign — sqft-primary scoring."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.enrichment.comps import _score_comp, _passes_sqft_filter


class TestSqftFilter:
    def test_rejects_over_30_percent_larger(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 1400}  # 40% larger
        assert _passes_sqft_filter(subject, comp) is False

    def test_rejects_over_30_percent_smaller(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 600}  # 40% smaller
        assert _passes_sqft_filter(subject, comp) is False

    def test_accepts_within_30_percent(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 1200}  # 20% larger
        assert _passes_sqft_filter(subject, comp) is True

    def test_accepts_exact_boundary(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 1300}  # exactly 30%
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_subject_missing_sqft(self):
        subject = {}
        comp = {"sqft": 1200}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_comp_missing_sqft(self):
        subject = {"sqft": 1000}
        comp = {}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_both_missing_sqft(self):
        subject = {}
        comp = {}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_subject_sqft_zero(self):
        subject = {"sqft": 0}
        comp = {"sqft": 1200}
        assert _passes_sqft_filter(subject, comp) is True


class TestScoreComp:
    @patch("src.enrichment.comps.date")
    def test_sqft_is_dominant_factor(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 27)
        mock_date.side_effect = lambda *a, **kw: real_date(*a, **kw)

        subject = {"sqft": 1000}

        # Comp A: perfect sqft match, far away
        comp_a = {"sqft": 1000, "_distance": 2.5, "sale_date": "2026-01-01"}
        # Comp B: poor sqft match, very close
        comp_b = {"sqft": 1250, "_distance": 0.1, "sale_date": "2026-01-01"}

        score_a = _score_comp(subject, comp_a)
        score_b = _score_comp(subject, comp_b)
        assert score_a > score_b, "Perfect sqft match should beat close distance"

    @patch("src.enrichment.comps.date")
    def test_missing_sqft_gets_penalty(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 27)
        mock_date.side_effect = lambda *a, **kw: real_date(*a, **kw)

        subject = {"sqft": 1000}
        comp_with = {"sqft": 1000, "_distance": 1.0, "sale_date": "2026-01-01"}
        comp_without = {"_distance": 1.0, "sale_date": "2026-01-01"}

        score_with = _score_comp(subject, comp_with)
        score_without = _score_comp(subject, comp_without)
        assert score_with > score_without, "Missing sqft should get penalty score"

    @patch("src.enrichment.comps.date")
    def test_weights_sum_correctly(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 27)
        mock_date.side_effect = lambda *a, **kw: real_date(*a, **kw)

        # Perfect comp: identical sqft, 0 distance, sold today
        subject = {"sqft": 1000}
        comp = {"sqft": 1000, "_distance": 0, "sale_date": "2026-03-27"}
        score = _score_comp(subject, comp)
        assert score == pytest.approx(1.0, abs=0.01)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_comps_scoring.py -v
```

Expected: FAIL — `_passes_sqft_filter` does not exist.

- [ ] **Step 3: Implement new scoring logic in `comps.py`**

Replace lines 21-100 in `src/enrichment/comps.py` (the weights, `_score_comp` function) with:

```python
# Scoring weights for comp selection — sqft similarity is dominant
WEIGHT_SQFT = 0.5
WEIGHT_DISTANCE = 0.3
WEIGHT_RECENCY = 0.2

# Hard filter: reject comps with >30% sqft difference
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
# Sqft filter and scoring
# ---------------------------------------------------------------------------

def _passes_sqft_filter(subject: dict, comp: dict) -> bool:
    """Return True if comp passes the sqft hard filter.

    Rejects comps where sqft differs by more than SQFT_FILTER_THRESHOLD (30%).
    If either side is missing sqft, the comp passes (don't reject on missing data).
    """
    subject_sqft = subject.get("sqft")
    comp_sqft = comp.get("sqft")

    if not subject_sqft or not comp_sqft or subject_sqft <= 0 or comp_sqft <= 0:
        return True

    diff = abs(subject_sqft - comp_sqft) / subject_sqft
    return diff <= SQFT_FILTER_THRESHOLD


def _score_comp(subject: dict, comp: dict) -> float:
    """Score a comparable sale against the subject property (0-1, higher=better).

    Factors: sqft similarity (50%), proximity (30%), recency (20%).
    """
    from datetime import datetime, date

    # Sqft similarity score
    subject_sqft = subject.get("sqft")
    comp_sqft = comp.get("sqft")
    if subject_sqft and comp_sqft and subject_sqft > 0 and comp_sqft > 0:
        sqft_score = 1.0 - abs(subject_sqft - comp_sqft) / subject_sqft
    else:
        sqft_score = 0.3  # penalty for missing data

    # Distance score: 0 at max_dist, 1 at 0 distance
    dist = comp.get("_distance", 0)
    max_dist = 3.0  # miles (wider radius since sqft is primary filter)
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
```

- [ ] **Step 4: Update `find_comps` to apply the sqft hard filter**

Replace the `find_comps` function (lines 103-135) with:

```python
def find_comps(
    conn,
    subject: dict,
    radius_miles: float = 1.5,
    months_back: int = 6,
) -> list[dict]:
    """Find comparable sales for a subject property.

    Applies sqft hard filter, then scores and ranks remaining comps.
    Returns comps sorted by score (best first), with _distance and _score added.
    """
    from src.db.database import get_comps_near

    lat = subject.get("lat")
    lng = subject.get("lng")
    if lat is None or lng is None:
        return []

    candidates = get_comps_near(conn, lat, lng, radius_miles, months_back)

    # Post-filter with exact Haversine distance and sqft gate
    comps = []
    for c in candidates:
        if c.get("lat") is None or c.get("lng") is None:
            continue
        dist = haversine_distance(lat, lng, c["lat"], c["lng"])
        if dist > radius_miles:
            continue
        if not _passes_sqft_filter(subject, c):
            continue
        c["_distance"] = round(dist, 3)
        c["_score"] = round(_score_comp(subject, c), 3)
        comps.append(c)

    # Sort by score descending
    comps.sort(key=lambda x: x["_score"], reverse=True)
    return comps[:MAX_COMPS]
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_comps_scoring.py -v
```

Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/enrichment/comps.py tests/test_comps_scoring.py
git commit -m "feat: sqft-primary comp scoring with 30% hard filter"
```

---

### Task 2: Replace lot-size price adjustment with sqft-based adjustment

**Files:**
- Modify: `src/enrichment/comps.py:142-188` (`estimate_from_comps`)
- Modify: `src/enrichment/comps.py:247-267` (orchestrator comp_rows section)
- Modify: `tests/test_comps_scoring.py`

- [ ] **Step 1: Write failing tests for sqft-based price adjustment**

Append to `tests/test_comps_scoring.py`:

```python
class TestEstimateFromComps:
    def test_sqft_adjustment_scales_price_up(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 1200}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        # 100000 * (1200/1000) = 120000
        assert est == 120000.0
        assert count == 1

    def test_sqft_adjustment_scales_price_down(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 800}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        # 100000 * (800/1000) = 80000
        assert est == 80000.0

    def test_sqft_adjustment_clamped_at_1_3(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 2000}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        # ratio 2.0 clamped to 1.3 -> 100000 * 1.3 = 130000
        assert est == 130000.0

    def test_no_adjustment_when_sqft_missing(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 1000}
        comps = [
            {"sale_price": 100000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert est == 100000.0

    def test_confidence_high_with_3_comps(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 1000}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.9, "sale_date": "2026-01-15"},
            {"sale_price": 110000, "sqft": 1050, "_score": 0.8, "sale_date": "2026-01-20"},
            {"sale_price": 105000, "sqft": 980, "_score": 0.7, "sale_date": "2026-02-01"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert count == 3
        assert conf == "high"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_comps_scoring.py::TestEstimateFromComps -v
```

Expected: FAIL — current `estimate_from_comps` uses lot-size adjustment, not sqft.

- [ ] **Step 3: Rewrite `estimate_from_comps`**

Replace the `estimate_from_comps` function in `src/enrichment/comps.py`:

```python
def estimate_from_comps(
    subject: dict,
    comps: list[dict],
) -> tuple[float | None, int, str]:
    """Estimate value from comparable sales using score-weighted average.

    Uses sqft-based price adjustment: comp_price * (subject_sqft / comp_sqft),
    clamped to 0.7-1.3x range.

    Returns (estimated_value, comp_count, confidence).
    Confidence: "high" (3+), "medium" (2), "low" (1), None (0).
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
```

- [ ] **Step 4: Update `enrich_comps_from_db` comp_rows section to use sqft ratio**

Replace lines 247-267 in `enrich_comps_from_db` (the comp_rows building loop) with:

```python
        # Write individual comp matches to property_comps
        comp_rows = []
        for c in comps:
            comp_id = c.get("id")
            if comp_id is None:
                continue

            subject_sqft = row.get("sqft")
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
```

- [ ] **Step 5: Run all tests**

```bash
pytest tests/test_comps_scoring.py tests/test_valuation_refactor.py -v
```

Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/enrichment/comps.py tests/test_comps_scoring.py
git commit -m "feat: sqft-based price adjustment, replace lot-size adjustment"
```

---

### Task 3: Create Zillow recently sold scraper

**Files:**
- Create: `src/enrichment/comps_zillow.py`

- [ ] **Step 1: Write failing test for Zillow scraper**

Append to `tests/test_comps_scoring.py`:

```python
class TestZillowSoldScraper:
    def test_parse_zillow_result(self):
        """Test parsing a single Zillow search result dict."""
        from src.enrichment.comps_zillow import _parse_result

        result = {
            "zpid": "12345",
            "address": "123 Main St, Belleville, IL 62220",
            "addressStreet": "123 Main St",
            "addressCity": "Belleville",
            "addressState": "IL",
            "addressZipcode": "62220",
            "unformattedPrice": 185000,
            "beds": 3,
            "baths": 2,
            "area": 1400,
            "latLong": {"latitude": 38.52, "longitude": -89.98},
            "hdpData": {
                "homeInfo": {
                    "dateSold": 1706745600000,  # 2024-02-01 epoch ms
                    "livingArea": 1400,
                    "bedrooms": 3,
                    "bathrooms": 2,
                    "homeType": "SINGLE_FAMILY",
                    "lotSize": 10890,  # sqft
                    "yearBuilt": 1990,
                }
            },
        }

        record = _parse_result(result)
        assert record is not None
        assert record["address"] == "123 Main St, Belleville, IL 62220"
        assert record["sale_price"] == 185000
        assert record["sqft"] == 1400
        assert record["beds"] == 3
        assert record["baths"] == 2
        assert record["lat"] == 38.52
        assert record["lng"] == -89.98
        assert record["source"] == "zillow"
        assert record["source_id"] == "12345"
        assert record["year_built"] == 1990
        assert record["sale_date"] == "2024-02-01"
        assert record["lot_size"] == pytest.approx(0.25, abs=0.01)

    def test_parse_result_missing_price(self):
        from src.enrichment.comps_zillow import _parse_result
        result = {"zpid": "12345", "address": "123 Main St"}
        assert _parse_result(result) is None

    def test_parse_result_zero_price(self):
        from src.enrichment.comps_zillow import _parse_result
        result = {
            "zpid": "12345",
            "address": "123 Main St",
            "unformattedPrice": 0,
            "latLong": {"latitude": 38.52, "longitude": -89.98},
            "hdpData": {"homeInfo": {"dateSold": 1706745600000}},
        }
        assert _parse_result(result) is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_comps_scoring.py::TestZillowSoldScraper -v
```

Expected: FAIL — `comps_zillow` module does not exist.

- [ ] **Step 3: Create `src/enrichment/comps_zillow.py`**

```python
"""Zillow Recently Sold Scraper.

Fetches recently sold properties in St. Clair County from Zillow's
search pages and stores them in the comparable_sales table.

Usage:
    python -m src.enrichment.comps_zillow [--db data/cheasuits.db] [--days 180] [-v]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

ZILLOW_SOLD_URL = "https://www.zillow.com/st-clair-county-il/sold/"

IMPERSONATE_BROWSERS = ["chrome131", "chrome124"]
MAX_PAGES = 10
MAX_RETRIES = 3
PAGE_DELAY = 1.5  # seconds between page fetches


def _get_session() -> cffi_requests.Session:
    import random
    browser = random.choice(IMPERSONATE_BROWSERS)
    return cffi_requests.Session(impersonate=browser)


def _epoch_ms_to_date(epoch_ms: int | float | None) -> str:
    """Convert epoch milliseconds to YYYY-MM-DD string."""
    if not epoch_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, TypeError):
        return ""


def _parse_result(result: dict) -> dict | None:
    """Parse a single Zillow search result into a comparable_sales record.

    Returns None if the result is missing required fields.
    """
    price = result.get("unformattedPrice")
    if not price or price <= 0:
        return None

    address = result.get("address", "")
    if not address:
        return None

    lat_lng = result.get("latLong", {})
    lat = lat_lng.get("latitude")
    lng = lat_lng.get("longitude")

    home_info = result.get("hdpData", {}).get("homeInfo", {})

    # Date sold — epoch ms
    date_sold_ms = home_info.get("dateSold")
    sale_date = _epoch_ms_to_date(date_sold_ms)
    if not sale_date:
        # Approximate: use current date minus some offset
        sale_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    # Sqft — prefer top-level 'area', fallback to homeInfo.livingArea
    sqft = result.get("area") or home_info.get("livingArea")
    if sqft is not None:
        sqft = float(sqft)

    beds = result.get("beds") or home_info.get("bedrooms")
    if beds is not None:
        beds = int(beds)

    baths = result.get("baths") or home_info.get("bathrooms")
    if baths is not None:
        baths = float(baths)

    # Lot size — Zillow gives sqft, convert to acres
    lot_size_sqft = home_info.get("lotSize")
    lot_size = None
    if lot_size_sqft and lot_size_sqft > 0:
        lot_size = round(lot_size_sqft / 43560.0, 4)

    year_built = home_info.get("yearBuilt")
    if year_built is not None:
        year_built = int(year_built)

    home_type = home_info.get("homeType", "")

    zpid = str(result.get("zpid", ""))

    return {
        "address": address,
        "lat": lat,
        "lng": lng,
        "sale_price": float(price),
        "sale_date": sale_date,
        "property_type": home_type,
        "sqft": sqft,
        "beds": beds,
        "baths": baths,
        "lot_size": lot_size,
        "year_built": year_built,
        "source": "zillow",
        "source_id": zpid,
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
    }


def _extract_results_from_html(html: str) -> list[dict]:
    """Extract sold listing results from Zillow page HTML.

    Parses the __NEXT_DATA__ JSON blob and extracts listResults.
    """
    match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    if not match:
        logger.warning("No __NEXT_DATA__ found in Zillow page")
        return []

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse Zillow __NEXT_DATA__: {e}")
        return []

    results = (
        data.get("props", {})
        .get("pageProps", {})
        .get("searchPageState", {})
        .get("cat1", {})
        .get("searchResults", {})
        .get("listResults", [])
    )

    return results


def fetch_zillow_sold(sold_within_days: int = 180) -> list[dict]:
    """Fetch recently sold properties from Zillow for St. Clair County.

    Paginates through search results pages. Returns list of records
    ready for comparable_sales table.
    """
    session = _get_session()
    all_records = []
    seen_zpids = set()

    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = ZILLOW_SOLD_URL
        else:
            url = f"{ZILLOW_SOLD_URL}{page}_p/"

        if page > 1:
            time.sleep(PAGE_DELAY)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, headers={
                    "Accept": "text/html,application/xhtml+xml",
                }, timeout=20)
                resp.raise_for_status()
                html = resp.text
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    logger.warning(f"Zillow page {page} attempt {attempt}/{MAX_RETRIES}: {e}")
                    time.sleep(2)
                else:
                    logger.error(f"Zillow page {page} failed after {MAX_RETRIES} attempts: {e}")
                    html = ""

        if not html:
            break

        results = _extract_results_from_html(html)
        if not results:
            logger.info(f"No results on page {page}, stopping pagination")
            break

        page_count = 0
        for r in results:
            zpid = str(r.get("zpid", ""))
            if zpid in seen_zpids:
                continue
            seen_zpids.add(zpid)

            record = _parse_result(r)
            if record:
                all_records.append(record)
                page_count += 1

        logger.info(f"Page {page}: {page_count} new records (total: {len(all_records)})")

        # If we got fewer results than expected, we've probably hit the last page
        if len(results) < 20:
            break

    logger.info(f"Fetched {len(all_records)} sold records from Zillow")
    return all_records


def fetch_and_store(db_path: Path, sold_within_days: int = 180) -> int:
    """Fetch Zillow sold data and store in comparable_sales table.

    Returns number of records stored.
    """
    from src.db.database import get_db, upsert_comparable_sales

    records = fetch_zillow_sold(sold_within_days)
    if not records:
        print("No records fetched from Zillow.")
        return 0

    conn = get_db(db_path)
    count = upsert_comparable_sales(conn, records)
    conn.close()

    print(f"Stored {count} Zillow sold records in comparable_sales table")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Fetch recently sold properties from Zillow for St. Clair County"
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB),
        help=f"Database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--days", type=int, default=180,
        help="Look-back period in days (default: 180)",
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

    fetch_and_store(Path(args.db), args.days)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_comps_scoring.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/enrichment/comps_zillow.py tests/test_comps_scoring.py
git commit -m "feat: add Zillow recently sold scraper for comparable sales"
```

---

### Task 4: End-to-end verification

**Files:**
- No new files

- [ ] **Step 1: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: All PASS.

- [ ] **Step 2: Run Zillow sold scraper to populate comps**

```bash
python -m src.enrichment.comps_zillow --db data/cheasuits.db -v
```

Expected: Fetches and stores Zillow sold records.

- [ ] **Step 3: Run comps pipeline with new scoring**

```bash
python -m src.enrichment.comps --db data/cheasuits.db --radius 3.0 --months 12 -v
```

Expected: Comps matching uses sqft-primary scoring. Properties get matched with similar-sized houses.

- [ ] **Step 4: Spot-check a property's comps for sqft similarity**

```bash
python -c "
from src.db.database import get_db, get_property_comps
conn = get_db('data/cheasuits.db')
# Find a property with comps
rows = conn.execute(
    'SELECT document_number, sqft FROM properties WHERE sqft IS NOT NULL LIMIT 1'
).fetchall()
if rows:
    doc = rows[0][0]
    subj_sqft = rows[0][1]
    comps = get_property_comps(conn, doc)
    print(f'Subject sqft: {subj_sqft}')
    for c in comps:
        diff = abs(c['sqft'] - subj_sqft) / subj_sqft * 100 if c.get('sqft') and subj_sqft else None
        print(f'  Comp: {c[\"address\"]} | sqft={c.get(\"sqft\")} | diff={diff:.0f}% | dist={c[\"distance_miles\"]}mi | score={c[\"similarity_score\"]}')
conn.close()
"
```

Expected: All comps have sqft within 30% of the subject.

- [ ] **Step 5: Re-apply priority rule and regenerate data.json**

```bash
python -m src.enrichment.valuation --db data/cheasuits.db --reprioritize
python -m src.scoring.viability --db data/cheasuits.db --rescore
python -m src.visualization.prepare_data --db data/cheasuits.db
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: regenerate data with sqft-primary comps scoring and Zillow sold data"
```
