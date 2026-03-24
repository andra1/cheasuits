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
