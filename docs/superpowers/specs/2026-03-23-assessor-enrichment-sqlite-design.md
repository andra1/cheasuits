# Design: DevNetWedge Assessor Scraper + SQLite Database

## Problem

The pipeline currently passes data through CSV files. Each run regenerates everything from scratch. There's no enrichment step — lis pendens records go straight to geocoding and the dashboard. Adding assessor data (owner info, tax status, property value) requires a persistent store that modules can incrementally write to.

## Solution

1. Add a SQLite database as the central data store
2. Build a DevNetWedge assessor scraper as a standalone enrichment module
3. Refactor existing modules to read/write from the database
4. Keep the dashboard as a static export, with a build step that queries the DB to produce `data.json`

## Architecture

```
ava_search (scrape) ──writes──> SQLite DB <──updates── assessor (enrich)
                                    │
                              prepare_data (query DB, geocode, write data.json)
                                    │
                              dashboard (static Next.js build)
```

Each module is independently runnable. The DB is the shared interface between them.

## Database

**Location:** `data/cheasuits.db`

**Single table: `properties`**

| Column | Type | Source | Notes |
|---|---|---|---|
| `id` | INTEGER PK | auto | |
| `document_number` | TEXT UNIQUE | ava_search | Dedup key |
| `case_number` | TEXT | ava_search | |
| `case_type` | TEXT | ava_search | FC, CH, CV |
| `case_year` | TEXT | ava_search | |
| `recorded_date` | TEXT | ava_search | YYYY-MM-DD |
| `party1` | TEXT | ava_search | Case number string |
| `party2` | TEXT | ava_search | Defendant/borrower |
| `parcel_id` | TEXT | ava_search | First parcel from legals, hyphenated (01-35-0-402-022) |
| `subdivision` | TEXT | ava_search | First subdivision from legals |
| `legals_raw` | TEXT | ava_search | Original legals field for reference |
| `source` | TEXT | ava_search | Always "ava_search_stclair" |
| `scraped_at` | TEXT | ava_search | ISO timestamp |
| `owner_name` | TEXT | assessor | From DevNetWedge |
| `property_address` | TEXT | assessor | Full street address |
| `mailing_address` | TEXT | assessor | For absentee detection |
| `absentee_owner` | INTEGER | assessor | 1 if mailing != property address |
| `assessed_value` | REAL | assessor | |
| `net_taxable_value` | REAL | assessor | |
| `tax_rate` | REAL | assessor | |
| `total_tax` | REAL | assessor | |
| `tax_status` | TEXT | assessor | "paid", "delinquent", "sold" |
| `property_class` | TEXT | assessor | e.g. "0040 - Improved Lots" |
| `acres` | REAL | assessor | |
| `enriched_at` | TEXT | assessor | ISO timestamp, NULL if not yet enriched |
| `enrichment_error` | TEXT | assessor | Error message if enrichment failed, NULL otherwise. Rows with this set are skipped on re-run. |
| `lat` | REAL | prepare_data | From ArcGIS geocoding |
| `lng` | REAL | prepare_data | |
| `geocoded_at` | TEXT | prepare_data | ISO timestamp, NULL if not yet geocoded |

**Indexes:**
- UNIQUE on `document_number`
- INDEX on `parcel_id`
- INDEX on `recorded_date`
- INDEX on `enriched_at` (for finding un-enriched rows)
- INDEX on `geocoded_at` (for finding un-geocoded rows)

## New Module: `src/db/database.py`

Thin wrapper around Python's `sqlite3`. Provides:

- `get_db(db_path) -> Connection` — opens/creates DB, enables WAL journal mode, runs schema migration if table doesn't exist
- `upsert_records(conn, records: list[dict])` — uses `INSERT ... ON CONFLICT(document_number) DO UPDATE SET` with **only** the ava_search-owned columns (case_number, case_type, etc.). This preserves enrichment and geocoding data on re-scrape. Never use `INSERT OR REPLACE` as it deletes the entire row first.
- `get_unenriched(conn) -> list[dict]` — rows where `enriched_at IS NULL` and `enrichment_error IS NULL` and `parcel_id != ''`
- `update_enrichment(conn, document_number, fields: dict)` — set assessor fields + `enriched_at`
- `get_ungeocoded(conn) -> list[dict]` — rows where `geocoded_at IS NULL` and `parcel_id != ''`
- `update_geocoding(conn, document_number, lat, lng)` — set lat/lng + `geocoded_at`
- `get_all(conn) -> list[dict]` — all rows, for dashboard export

No ORM. Plain SQL. The schema is simple enough that SQLAlchemy/Alembic would be overhead.

## New Module: `src/enrichment/assessor.py`

### Scraper

- **URL pattern:** `https://stclairil.devnetwedge.com/parcel/view/{parcel_no_hyphens}/{tax_year}`
- **Parcel ID transformation:** Strip hyphens before constructing URL (`01-35-0-402-022` -> `01350402022`). Use shared `strip_parcel_hyphens()` from `src/utils/parsing.py`.
- **Tax year default:** `datetime.now().year - 1` (assessments lag by one year). Override with `--year`.
- **Method:** HTTP GET, no auth required
- **Parser:** `BeautifulSoup` to extract fields from the HTML
- **Rate limiting:** 0.3s between requests
- **Retries:** 3 attempts per parcel with 1s backoff (matching existing geocoder pattern in `prepare_data.py`)
- **Error handling:** HTTP 404 or empty page -> log warning, skip parcel. HTTP 429/5xx -> retry with backoff. After max retries, record failure and move on. Failed parcels are skipped on re-run (see `enrichment_error` column below).
- **Caching:** In-memory dict keyed by parcel ID (same parcel can appear in multiple filings)

### Data Class

```python
@dataclass
class AssessorRecord:
    parcel_id: str
    owner_name: str = ""
    property_address: str = ""
    mailing_address: str = ""
    absentee_owner: bool = False     # stored as INTEGER 0/1 in SQLite
    assessed_value: float | None = None
    net_taxable_value: float | None = None
    tax_rate: float | None = None
    total_tax: float | None = None
    tax_status: str = ""          # "paid", "delinquent", "sold"
    property_class: str = ""
    acres: float | None = None
```

### CLI

```
python -m src.enrichment.assessor [--db data/cheasuits.db] [--year 2024] [-v]
```

- Queries DB for rows with `enriched_at IS NULL`
- Fetches assessor page for each parcel
- Updates DB with parsed fields
- Prints summary (enriched count, failed count, notable findings like tax-sold properties)

## Modified Module: `src/ingestion/ava_search.py`

Add `--db` flag:

```
python -m src.ingestion.ava_search --days 30 --db data/cheasuits.db
```

- When `--db` is provided, upserts records to the database instead of (or in addition to) CSV
- Parcel IDs and subdivisions are parsed from the legals field before insertion using shared `parse_legals` from `src/utils/parsing.py`
- `--output` CSV flag still works for backwards compatibility

## Modified Module: `src/visualization/prepare_data.py`

Add `--db` flag:

```
python -m src.visualization.prepare_data [--db data/cheasuits.db]
```

- When `--db` is provided, reads from DB instead of CSV
- Geocodes rows where `geocoded_at IS NULL`, updates DB with results
- Writes `dashboard/public/data.json` from all DB rows (enriched data included)
- The `data.json` features array gains these fields from the assessor: `owner_name`, `property_address`, `mailing_address`, `absentee_owner`, `assessed_value`, `net_taxable_value`, `tax_status`, `property_class`, `acres`. The dashboard UI does not need to change yet — extra fields in `data.json` are harmless and available when the UI is updated later.

## Pipeline

```bash
# Full pipeline (each step is idempotent)
python -m src.ingestion.ava_search --days 30 --db data/cheasuits.db
python -m src.enrichment.assessor --db data/cheasuits.db
python -m src.visualization.prepare_data --db data/cheasuits.db
vercel --prod
```

Re-running any step only processes new/missing data.

## Dependencies

New: `beautifulsoup4` (for HTML parsing of DevNetWedge pages)

No new dependencies for SQLite (stdlib) or HTTP (already using `urllib`).

## File Changes Summary

| File | Action |
|---|---|
| `src/utils/__init__.py` | New (empty) |
| `src/utils/parsing.py` | New — shared `parse_legals`, `strip_parcel_hyphens` |
| `src/db/__init__.py` | New (empty) |
| `src/db/database.py` | New — SQLite helper |
| `src/enrichment/__init__.py` | New (empty) |
| `src/enrichment/assessor.py` | New — DevNetWedge scraper |
| `src/ingestion/ava_search.py` | Modify — add `--db` flag, upsert to DB |
| `src/visualization/prepare_data.py` | Modify — add `--db` flag, read/write DB, import from `src/utils/parsing.py` |
| `requirements.txt` | Modify — add `beautifulsoup4` |
| `.gitignore` | Modify — add `data/*.db` |
| `data/.gitkeep` | New — ensure data dir exists in repo |

## Known Limitations

- **Multi-parcel filings reduced to first parcel.** Some lis pendens filings reference multiple parcels (e.g., document 2224005 has two). Only the first parcel ID is stored and enriched. Additional parcels are preserved in `legals_raw` for future use.
- **Absentee owner detection is a simple string comparison.** `absentee_owner` is set to 1 when `mailing_address != property_address` (case-insensitive, stripped). No address normalization (e.g., "St" vs "Street"). Good enough for obvious cases; can be refined later.

## Out of Scope

- Dashboard UI changes to display assessor fields (separate task)
- Scoring engine (depends on assessor data being available first)
- Sales history, payment history, tax distribution (can be added later)
- Migration from existing CSV data into DB (manual one-time import or just re-run the pipeline)
