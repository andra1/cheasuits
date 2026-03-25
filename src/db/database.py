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
