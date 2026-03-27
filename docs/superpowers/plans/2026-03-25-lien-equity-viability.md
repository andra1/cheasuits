# Implementation Plan: Lien Enrichment, Equity Spread & Deal Viability Score

**Spec**: `docs/superpowers/specs/2026-03-25-lien-equity-viability-design.md`
**Date**: 2026-03-25

---

## Step 1: Extract shared Fidlar utilities to `_fidlar.py`

**Files**: New `src/enrichment/_fidlar.py`, modify `src/enrichment/mortgage.py`

Extract from `mortgage.py`:
- `_get_token()`, `_token_cache`
- `_search_documents()`
- `_parse_date()`
- `_parse_amount()`
- `_normalize_lender()` → rename to `normalize_party_name()`
- Constants: `API_BASE`, `TOKEN_URL`, `SEARCH_URL`, `REQUEST_DELAY`, `MAX_RETRIES`

Update `mortgage.py` to import from `_fidlar`:
```python
from src.enrichment._fidlar import (
    get_token, search_documents, parse_date, parse_amount,
    normalize_party_name, REQUEST_DELAY, SEARCH_URL,
)
```

**Verification**: Run `python -m src.enrichment.mortgage --parcel 01-35-0-205-009` and confirm identical output to before refactor.

---

## Step 2: Build `src/enrichment/liens.py`

**Files**: New `src/enrichment/liens.py`

Define lien type constants:
```python
LIEN_TYPES = {
    "federal_tax":  {"codes": ["86", "85"], "release_code": "141", "release_name": "RELEASE OF FEDERAL TAX LIEN"},
    "state_tax":    {"codes": ["92", "248"], "release_code": "155", "release_name": "RELEASE OF ILLINOIS LIEN"},
    "judgment":     {"codes": ["94", "106"], "release_code": "153", "release_name": "RELEASE OF JUDGEMENT"},
}
```

Implement:
- `LienRecord` dataclass
- `fetch_lien_history(parcel_id)` — queries all lien types + releases, matches them
- `get_active_liens(parcel_id)` — filters to unreleased
- `get_total_lien_amount(parcel_id)` — sums active lien amounts
- `enrich_liens_from_db(db_path)` — batch enrichment like `mortgage.py`
- CLI with `--parcel` single lookup and `--db` batch mode

Key difference from mortgage: NO sale-date release heuristic. Liens survive transfers.

**Verification**: Run `python -m src.enrichment.liens --parcel <known-parcel>` on a parcel with known liens.

---

## Step 3: Add DB migrations

**Files**: Modify `src/db/database.py`

Add lien columns migration block:
- `federal_tax_lien_amount`, `state_tax_lien_amount`, `judgment_lien_amount`
- `total_recorded_liens`, `lien_count`, `lien_enriched_at`, `lien_error`

Add viability columns migration block:
- `total_lien_burden`, `equity_spread`, `equity_ratio`
- `viability_score`, `viability_details`, `viability_scored_at`

Add DB helper functions:
- `get_unlienned_properties(conn)` — properties needing lien enrichment
- `update_liens(conn, document_number, fields)` — write lien data
- `set_lien_error(conn, document_number, error)` — record failures
- `update_viability(conn, document_number, fields)` — write viability score

**Verification**: Open DB, confirm new columns exist.

---

## Step 4: Build `src/scoring/viability.py`

**Files**: New `src/scoring/__init__.py`, new `src/scoring/viability.py`

Implement the 6-factor scoring engine:

```python
def score_equity_spread(equity_ratio: float | None) -> int:
    """0-30 points based on equity ratio."""

def score_comp_confidence(confidence: str | None) -> int:
    """0-20 points based on comps confidence level."""

def score_neighborhood_vacancy(vacancy_rate: float | None) -> int:
    """0-15 points based on census tract vacancy rate."""

def score_buy_box(assessed_value: float | None, min_val=30000, max_val=150000) -> int:
    """0-15 points based on assessed value within buy box."""

def score_time_pressure(recorded_date: str | None) -> int:
    """0-10 points based on days since first distress signal."""

def score_owner_reachability(mailing_address: str | None) -> int:
    """0-10 points based on mailing address availability and locality."""

def calculate_viability_score(property_row: dict, vacancy_rate: float | None) -> dict:
    """Calculate full viability score. Returns dict with score, details, equity fields."""
```

Also implement:
- `calculate_equity(market_value, mortgage_debt, recorded_liens, delinquent_tax)` — computes equity spread and ratio
- `score_all_properties(db_path)` — batch scoring, queries DB for enriched properties, joins vacancy data, scores each one
- CLI entry point

**Verification**: Unit tests for each scoring function with known inputs/outputs.

---

## Step 5: Integration and testing

**Files**: Various

1. Write unit tests in `tests/test_viability.py`:
   - Each scoring function boundary cases
   - Equity calculation with various lien combinations
   - Edge cases: missing data, negative equity, zero market value

2. Run end-to-end:
   ```bash
   python -m src.enrichment.liens --db data/cheasuits.db -v
   python -m src.scoring.viability --db data/cheasuits.db -v
   ```

3. Spot-check: pick 3 properties with known characteristics, verify scores make sense.

**Verification**: All tests pass, batch run completes without errors.
