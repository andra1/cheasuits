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
    sqft REAL,
    beds INTEGER,
    baths REAL,
    property_type TEXT,
    year_built INTEGER,
    stories INTEGER,
    property_details_source TEXT,
    property_details_at TEXT,
    property_details_error TEXT,
    enriched_at TEXT,
    enrichment_error TEXT,
    lat REAL,
    lng REAL,
    geocoded_at TEXT,
    assessed_multiplier_value REAL,
    zillow_estimate REAL,
    redfin_estimate REAL,
    estimated_market_value REAL,
    valuation_source TEXT,
    valuation_confidence TEXT,
    valued_at TEXT,
    valuation_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_parcel_id ON properties(parcel_id);
CREATE INDEX IF NOT EXISTS idx_recorded_date ON properties(recorded_date);
CREATE INDEX IF NOT EXISTS idx_enriched_at ON properties(enriched_at);
CREATE INDEX IF NOT EXISTS idx_geocoded_at ON properties(geocoded_at);

CREATE TABLE IF NOT EXISTS delinquent_taxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parcel_id TEXT NOT NULL,
    street TEXT DEFAULT '',
    city TEXT DEFAULT '',
    publication_year INTEGER NOT NULL,
    source_file TEXT DEFAULT '',
    scraped_at TEXT DEFAULT '',
    -- assessor enrichment fields (same as properties table)
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
    geocoded_at TEXT,
    UNIQUE(parcel_id, publication_year)
);

CREATE INDEX IF NOT EXISTS idx_dt_parcel_id ON delinquent_taxes(parcel_id);
CREATE INDEX IF NOT EXISTS idx_dt_city ON delinquent_taxes(city);
CREATE INDEX IF NOT EXISTS idx_dt_publication_year ON delinquent_taxes(publication_year);
CREATE INDEX IF NOT EXISTS idx_dt_enriched_at ON delinquent_taxes(enriched_at);

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

CREATE TABLE IF NOT EXISTS comparable_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    lat REAL, lng REAL,
    sale_price REAL NOT NULL,
    sale_date TEXT NOT NULL,
    property_type TEXT DEFAULT '',
    sqft REAL, beds INTEGER, baths REAL,
    lot_size REAL, year_built INTEGER,
    source TEXT NOT NULL,
    source_id TEXT DEFAULT '',
    scraped_at TEXT DEFAULT '',
    UNIQUE(address, sale_date, source)
);
CREATE INDEX IF NOT EXISTS idx_comp_lat_lng ON comparable_sales(lat, lng);
CREATE INDEX IF NOT EXISTS idx_comp_sale_date ON comparable_sales(sale_date);

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
"""

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

    # Create index on valued_at (after migration ensures column exists)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_valued_at ON properties(valued_at)")
    conn.commit()

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

    # Migrate: add comps valuation columns to properties
    _COMPS_MIGRATIONS = [
        "ALTER TABLE properties ADD COLUMN comps_estimate REAL",
        "ALTER TABLE properties ADD COLUMN comps_count INTEGER",
        "ALTER TABLE properties ADD COLUMN comps_confidence TEXT",
        "ALTER TABLE properties ADD COLUMN comps_updated_at TEXT",
    ]
    for stmt in _COMPS_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()

    # Migrate: add mortgage enrichment columns to properties
    _MORTGAGE_MIGRATIONS = [
        "ALTER TABLE properties ADD COLUMN mortgage_amount REAL",
        "ALTER TABLE properties ADD COLUMN mortgage_date TEXT",
        "ALTER TABLE properties ADD COLUMN mortgage_lender TEXT",
        "ALTER TABLE properties ADD COLUMN total_mortgage_debt REAL",
        "ALTER TABLE properties ADD COLUMN mortgage_count INTEGER",
        "ALTER TABLE properties ADD COLUMN mortgage_source TEXT",
        "ALTER TABLE properties ADD COLUMN mortgage_enriched_at TEXT",
        "ALTER TABLE properties ADD COLUMN mortgage_error TEXT",
    ]
    for stmt in _MORTGAGE_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()

    # Migrate: add lien enrichment columns to properties
    _LIEN_MIGRATIONS = [
        "ALTER TABLE properties ADD COLUMN federal_tax_lien_amount REAL",
        "ALTER TABLE properties ADD COLUMN state_tax_lien_amount REAL",
        "ALTER TABLE properties ADD COLUMN judgment_lien_amount REAL",
        "ALTER TABLE properties ADD COLUMN total_recorded_liens REAL",
        "ALTER TABLE properties ADD COLUMN lien_count INTEGER",
        "ALTER TABLE properties ADD COLUMN lien_enriched_at TEXT",
        "ALTER TABLE properties ADD COLUMN lien_error TEXT",
    ]
    for stmt in _LIEN_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()

    # Migrate: add property details columns to properties
    _DETAILS_MIGRATIONS = [
        "ALTER TABLE properties ADD COLUMN sqft REAL",
        "ALTER TABLE properties ADD COLUMN beds INTEGER",
        "ALTER TABLE properties ADD COLUMN baths REAL",
        "ALTER TABLE properties ADD COLUMN property_type TEXT",
        "ALTER TABLE properties ADD COLUMN year_built INTEGER",
        "ALTER TABLE properties ADD COLUMN stories INTEGER",
        "ALTER TABLE properties ADD COLUMN property_details_source TEXT",
        "ALTER TABLE properties ADD COLUMN property_details_at TEXT",
        "ALTER TABLE properties ADD COLUMN property_details_error TEXT",
    ]
    for stmt in _DETAILS_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()

    # Migrate: add viability scoring columns to properties
    _VIABILITY_MIGRATIONS = [
        "ALTER TABLE properties ADD COLUMN total_lien_burden REAL",
        "ALTER TABLE properties ADD COLUMN equity_spread REAL",
        "ALTER TABLE properties ADD COLUMN equity_ratio REAL",
        "ALTER TABLE properties ADD COLUMN viability_score INTEGER",
        "ALTER TABLE properties ADD COLUMN viability_details TEXT",
        "ALTER TABLE properties ADD COLUMN viability_scored_at TEXT",
    ]
    for stmt in _VIABILITY_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
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


def get_unvalued(conn: sqlite3.Connection) -> list[dict]:
    """Get rows that need valuation (have assessed_value but no valuation yet)."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE valued_at IS NULL AND valuation_error IS NULL "
        "AND assessed_value IS NOT NULL"
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


def update_valuation(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update valuation fields and set valued_at timestamp."""
    allowed = {
        "assessed_multiplier_value", "zillow_estimate", "redfin_estimate",
        "comps_estimate", "estimated_market_value", "valuation_source",
        "valuation_confidence",
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


def set_valuation_error(
    conn: sqlite3.Connection, document_number: str, error: str
) -> None:
    """Record a valuation failure so the property is skipped on re-run."""
    conn.execute(
        "UPDATE properties SET valuation_error = ? WHERE document_number = ?",
        (error, document_number),
    )
    conn.commit()


def get_all(conn: sqlite3.Connection) -> list[dict]:
    """Get all property rows."""
    cursor = conn.execute("SELECT * FROM properties ORDER BY recorded_date DESC")
    return [dict(row) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Delinquent taxes table helpers
# ---------------------------------------------------------------------------

def upsert_delinquent_taxes(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update delinquent tax records. Keyed on (parcel_id, publication_year).

    Returns number of records upserted.
    """
    if not records:
        return 0

    ingestion_cols = ["street", "city", "source_file", "scraped_at"]
    update_clause = ", ".join(f"{col} = excluded.{col}" for col in ingestion_cols)

    sql = f"""
        INSERT INTO delinquent_taxes (parcel_id, publication_year, {", ".join(ingestion_cols)})
        VALUES (:parcel_id, :publication_year, {", ".join(":" + c for c in ingestion_cols)})
        ON CONFLICT(parcel_id, publication_year) DO UPDATE SET {update_clause}
    """

    for record in records:
        params = {
            "parcel_id": record["parcel_id"],
            "publication_year": record["publication_year"],
        }
        for col in ingestion_cols:
            params[col] = record.get(col, "")
        conn.execute(sql, params)

    conn.commit()
    return len(records)


def get_unenriched_delinquent(conn: sqlite3.Connection) -> list[dict]:
    """Get delinquent tax rows that need assessor enrichment."""
    cursor = conn.execute(
        "SELECT * FROM delinquent_taxes "
        "WHERE enriched_at IS NULL AND enrichment_error IS NULL AND parcel_id != ''"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_delinquent_enrichment(
    conn: sqlite3.Connection, row_id: int, fields: dict
) -> None:
    """Update assessor enrichment fields on a delinquent_taxes row."""
    allowed = {
        "owner_name", "property_address", "mailing_address", "absentee_owner",
        "assessed_value", "net_taxable_value", "tax_rate", "total_tax",
        "tax_status", "property_class", "acres",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["enriched_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["id"] = row_id

    conn.execute(
        f"UPDATE delinquent_taxes SET {set_clause} WHERE id = :id",
        filtered,
    )
    conn.commit()


def set_delinquent_enrichment_error(
    conn: sqlite3.Connection, row_id: int, error: str
) -> None:
    """Record an enrichment failure on a delinquent_taxes row."""
    conn.execute(
        "UPDATE delinquent_taxes SET enrichment_error = ? WHERE id = ?",
        (error, row_id),
    )
    conn.commit()


def get_delinquent_all(conn: sqlite3.Connection, year: Optional[int] = None) -> list[dict]:
    """Get all delinquent tax rows, optionally filtered by publication year."""
    if year:
        cursor = conn.execute(
            "SELECT * FROM delinquent_taxes WHERE publication_year = ? ORDER BY city, street",
            (year,),
        )
    else:
        cursor = conn.execute(
            "SELECT * FROM delinquent_taxes ORDER BY publication_year DESC, city, street"
        )
    return [dict(row) for row in cursor.fetchall()]


def get_delinquent_overlap(conn: sqlite3.Connection) -> list[dict]:
    """Find parcels that appear in BOTH lis pendens and delinquent taxes.

    Handles format mismatch: lis pendens stores hyphenated parcel IDs
    (e.g. '01-35-0-402-022') while delinquent taxes stores raw digits
    ('01350402022'). Joins by stripping hyphens from the properties table.
    """
    cursor = conn.execute("""
        SELECT
            dt.parcel_id,
            dt.street,
            dt.city,
            dt.publication_year,
            dt.owner_name AS dt_owner,
            dt.assessed_value AS dt_assessed_value,
            dt.tax_status AS dt_tax_status,
            p.document_number,
            p.case_number,
            p.case_type,
            p.recorded_date,
            p.owner_name AS lp_owner
        FROM delinquent_taxes dt
        INNER JOIN properties p ON dt.parcel_id = REPLACE(p.parcel_id, '-', '')
        WHERE dt.parcel_id != '' AND p.parcel_id != ''
        ORDER BY dt.city, dt.street
    """)
    return [dict(row) for row in cursor.fetchall()]


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


# ---------------------------------------------------------------------------
# Comparable sales table helpers
# ---------------------------------------------------------------------------

def upsert_comparable_sales(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update comparable sale records.

    Keyed on (address, sale_date, source). Returns number of records upserted.
    """
    if not records:
        return 0

    cols = [
        "lat", "lng", "sale_price", "property_type", "sqft", "beds", "baths",
        "lot_size", "year_built", "source", "source_id", "scraped_at",
    ]
    update_clause = ", ".join(f"{col} = excluded.{col}" for col in cols)

    sql = f"""
        INSERT INTO comparable_sales (address, sale_date, {", ".join(cols)})
        VALUES (:address, :sale_date, {", ".join(":" + c for c in cols)})
        ON CONFLICT(address, sale_date, source) DO UPDATE SET {update_clause}
    """

    count = 0
    for record in records:
        params = {
            "address": record["address"],
            "sale_date": record["sale_date"],
        }
        for col in cols:
            params[col] = record.get(col)
        conn.execute(sql, params)
        count += 1

    conn.commit()
    return count


def get_comps_near(
    conn: sqlite3.Connection,
    lat: float,
    lng: float,
    radius_miles: float = 1.5,
    months_back: int = 6,
) -> list[dict]:
    """Get comparable sales within a bounding box, filtered by date.

    Uses a lat/lng bounding box as a SQL pre-filter, then applies Haversine
    post-filter in Python for accurate distance.
    """
    import math

    # Bounding box: ~1 degree lat ≈ 69 miles, ~1 degree lng ≈ 69 * cos(lat) miles
    lat_delta = radius_miles / 69.0
    lng_delta = radius_miles / (69.0 * math.cos(math.radians(lat)))

    min_lat = lat - lat_delta
    max_lat = lat + lat_delta
    min_lng = lng - lng_delta
    max_lng = lng + lng_delta

    cursor = conn.execute(
        """
        SELECT * FROM comparable_sales
        WHERE lat BETWEEN ? AND ?
          AND lng BETWEEN ? AND ?
          AND sale_date >= date('now', ?)
          AND sale_price > 0
        ORDER BY sale_date DESC
        """,
        (min_lat, max_lat, min_lng, max_lng, f"-{months_back} months"),
    )
    return [dict(row) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Mortgage enrichment helpers
# ---------------------------------------------------------------------------

def get_unmortgaged_properties(conn: sqlite3.Connection) -> list[dict]:
    """Get properties that need mortgage enrichment."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE mortgage_enriched_at IS NULL AND mortgage_error IS NULL "
        "AND parcel_id != ''"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_mortgage(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update mortgage fields on a property row."""
    allowed = {
        "mortgage_amount", "mortgage_date", "mortgage_lender",
        "total_mortgage_debt", "mortgage_count", "mortgage_source",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["mortgage_enriched_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()


def set_mortgage_error(
    conn: sqlite3.Connection, document_number: str, error: str
) -> None:
    """Record a mortgage enrichment failure."""
    conn.execute(
        "UPDATE properties SET mortgage_error = ?, mortgage_enriched_at = ? "
        "WHERE document_number = ?",
        (error, datetime.now().isoformat(timespec="seconds"), document_number),
    )
    conn.commit()


def update_comps_valuation(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Write comps_estimate, comps_count, comps_confidence to a property row."""
    allowed = {"comps_estimate", "comps_count", "comps_confidence"}
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["comps_updated_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Lien enrichment helpers
# ---------------------------------------------------------------------------

def get_unlienned_properties(conn: sqlite3.Connection) -> list[dict]:
    """Get properties that need lien enrichment."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE lien_enriched_at IS NULL AND lien_error IS NULL "
        "AND parcel_id != ''"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_liens(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update lien fields on a property row."""
    allowed = {
        "federal_tax_lien_amount", "state_tax_lien_amount",
        "judgment_lien_amount", "total_recorded_liens", "lien_count",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["lien_enriched_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()


def set_lien_error(
    conn: sqlite3.Connection, document_number: str, error: str
) -> None:
    """Record a lien enrichment failure."""
    conn.execute(
        "UPDATE properties SET lien_error = ?, lien_enriched_at = ? "
        "WHERE document_number = ?",
        (error, datetime.now().isoformat(timespec="seconds"), document_number),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Viability scoring helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Property details enrichment helpers
# ---------------------------------------------------------------------------

def get_undetailed_properties(conn: sqlite3.Connection) -> list[dict]:
    """Get properties that need property details enrichment."""
    cursor = conn.execute(
        "SELECT * FROM properties "
        "WHERE property_details_at IS NULL AND property_details_error IS NULL "
        "AND property_address IS NOT NULL AND property_address != ''"
    )
    return [dict(row) for row in cursor.fetchall()]


def update_property_details(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update property detail fields on a property row."""
    allowed = {
        "sqft", "beds", "baths", "property_type", "year_built",
        "stories", "property_details_source",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["property_details_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()


def set_property_details_error(
    conn: sqlite3.Connection, document_number: str, error: str
) -> None:
    """Record a property details enrichment failure."""
    conn.execute(
        "UPDATE properties SET property_details_error = ?, property_details_at = ? "
        "WHERE document_number = ?",
        (error, datetime.now().isoformat(timespec="seconds"), document_number),
    )
    conn.commit()


def update_viability(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """Update viability scoring fields on a property row."""
    allowed = {
        "total_lien_burden", "equity_spread", "equity_ratio",
        "viability_score", "viability_details",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    filtered["viability_scored_at"] = datetime.now().isoformat(timespec="seconds")

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    filtered["document_number"] = document_number

    conn.execute(
        f"UPDATE properties SET {set_clause} WHERE document_number = :document_number",
        filtered,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Valuations & property_comps helpers
# ---------------------------------------------------------------------------

def upsert_valuation(
    conn: sqlite3.Connection, document_number: str, fields: dict
) -> None:
    """INSERT OR REPLACE a valuation row.

    Required fields: source, estimate.
    Optional: source_url, confidence, comp_count.
    Always sets valued_at to now.
    """
    params = {
        "document_number": document_number,
        "source": fields["source"],
        "estimate": fields["estimate"],
        "source_url": fields.get("source_url"),
        "confidence": fields.get("confidence"),
        "comp_count": fields.get("comp_count"),
        "valued_at": datetime.now().isoformat(timespec="seconds"),
    }
    conn.execute(
        """
        INSERT INTO valuations (document_number, source, estimate, source_url, confidence, comp_count, valued_at)
        VALUES (:document_number, :source, :estimate, :source_url, :confidence, :comp_count, :valued_at)
        ON CONFLICT(document_number, source) DO UPDATE SET
            estimate = excluded.estimate,
            source_url = excluded.source_url,
            confidence = excluded.confidence,
            comp_count = excluded.comp_count,
            valued_at = excluded.valued_at
        """,
        params,
    )
    conn.commit()


def get_valuations(conn: sqlite3.Connection, document_number: str) -> list[dict]:
    """Return all valuation rows for a property, newest first."""
    cursor = conn.execute(
        "SELECT * FROM valuations WHERE document_number = ? ORDER BY valued_at DESC",
        (document_number,),
    )
    return [dict(row) for row in cursor.fetchall()]


def insert_property_comps(
    conn: sqlite3.Connection, document_number: str, comps: list[dict]
) -> None:
    """Delete existing comp matches for this doc, then insert new ones.

    Each comp dict: comp_sale_id, distance_miles, similarity_score,
    lot_size_ratio, adjusted_price. Sets matched_at to now.
    """
    with conn:
        conn.execute(
            "DELETE FROM property_comps WHERE document_number = ?", (document_number,)
        )
        now = datetime.now().isoformat(timespec="seconds")
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
                    now,
                ),
            )


def get_property_comps(conn: sqlite3.Connection, document_number: str) -> list[dict]:
    """JOIN property_comps with comparable_sales, sorted by similarity_score DESC."""
    cursor = conn.execute(
        """
        SELECT pc.*, cs.address, cs.lat, cs.lng, cs.sale_price, cs.sale_date,
               cs.property_type, cs.sqft, cs.beds, cs.baths, cs.lot_size,
               cs.year_built, cs.source AS comp_source
        FROM property_comps pc
        JOIN comparable_sales cs ON pc.comp_sale_id = cs.id
        WHERE pc.document_number = ?
        ORDER BY pc.similarity_score DESC
        """,
        (document_number,),
    )
    return [dict(row) for row in cursor.fetchall()]


def apply_market_value_priority(
    conn: sqlite3.Connection, document_number: str
) -> None:
    """Apply priority rule to set estimated_market_value on the property.

    Priority: avg(Zillow+Redfin) > single external > comps > NULL.
    Updates properties.estimated_market_value, valuation_source, and valued_at.
    """
    valuations = {
        row["source"]: row["estimate"]
        for row in get_valuations(conn, document_number)
    }

    redfin = valuations.get("redfin")
    zillow = valuations.get("zillow")
    comps = valuations.get("comps")

    market_value = None
    source_label = None

    if redfin is not None and zillow is not None:
        market_value = (redfin + zillow) / 2
        source_label = "redfin+zillow"
    elif redfin is not None:
        market_value = redfin
        source_label = "redfin"
    elif zillow is not None:
        market_value = zillow
        source_label = "zillow"
    elif comps is not None:
        market_value = comps
        source_label = "comps"

    if market_value is not None:
        conn.execute(
            "UPDATE properties SET estimated_market_value = ?, valuation_source = ?, valued_at = ? "
            "WHERE document_number = ?",
            (market_value, source_label, datetime.now().isoformat(timespec="seconds"), document_number),
        )
        conn.commit()
