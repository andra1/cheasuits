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

- `get_db(db_path) -> Connection` — opens/creates DB, runs schema migration if table doesn't exist
- `upsert_records(conn, records: list[dict])` — INSERT OR REPLACE by `document_number`
- `get_unenriched(conn) -> list[dict]` — rows where `enriched_at IS NULL` and `parcel_id` is not empty
- `update_enrichment(conn, document_number, fields: dict)` — set assessor fields + `enriched_at`
- `get_ungecoded(conn) -> list[dict]` — rows where `geocoded_at IS NULL` and `parcel_id` is not empty
- `update_geocoding(conn, document_number, lat, lng)` — set lat/lng + `geocoded_at`
- `get_all(conn) -> list[dict]` — all rows, for dashboard export

No ORM. Plain SQL. The schema is simple enough that SQLAlchemy/Alembic would be overhead.

## New Module: `src/enrichment/assessor.py`

### Scraper

- **URL pattern:** `https://stclairil.devnetwedge.com/parcel/view/{parcel_no_hyphens}/{tax_year}`
- **Method:** HTTP GET, no auth required
- **Parser:** `BeautifulSoup` to extract fields from the HTML
- **Rate limiting:** 0.3s between requests
- **Caching:** In-memory dict keyed by parcel ID (same parcel can appear in multiple filings)

### Data Class

```python
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
- Parcel IDs and subdivisions are parsed from the legals field before insertion (reuse `parse_legals` from prepare_data.py, or move it to a shared util)
- `--output` CSV flag still works for backwards compatibility

## Modified Module: `src/visualization/prepare_data.py`

Add `--db` flag:

```
python -m src.visualization.prepare_data [--db data/cheasuits.db]
```

- When `--db` is provided, reads from DB instead of CSV
- Geocodes rows where `geocoded_at IS NULL`, updates DB with results
- Writes `dashboard/public/data.json` from all DB rows (enriched data included)
- The `data.json` schema gains new fields from the assessor data so the dashboard can display them

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
| `src/db/__init__.py` | New (empty) |
| `src/db/database.py` | New — SQLite helper |
| `src/enrichment/__init__.py` | New (empty) |
| `src/enrichment/assessor.py` | New — DevNetWedge scraper |
| `src/ingestion/ava_search.py` | Modify — add `--db` flag, upsert to DB |
| `src/visualization/prepare_data.py` | Modify — add `--db` flag, read/write DB |
| `requirements.txt` | Modify — add `beautifulsoup4` |
| `data/.gitkeep` | New — ensure data dir exists in repo |

## Out of Scope

- Dashboard UI changes to display assessor fields (separate task)
- Scoring engine (depends on assessor data being available first)
- Sales history, payment history, tax distribution (can be added later)
- Migration from existing CSV data into DB (manual one-time import or just re-run the pipeline)
