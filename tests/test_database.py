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
        update_enrichment(db, "2224358", {
            "owner_name": "Ruth Allen",
            "assessed_value": 12952.0,
            "tax_status": "sold",
        })
        updated = {**SAMPLE_RECORD, "party2": "ALLEN RUTH E"}
        upsert_records(db, [updated])
        rows = get_all(db)
        assert len(rows) == 1
        assert rows[0]["party2"] == "ALLEN RUTH E"
        assert rows[0]["owner_name"] == "Ruth Allen"
        assert rows[0]["assessed_value"] == 12952.0

    def test_upsert_preserves_geocoding(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_all(db)
        assert rows[0]["lat"] == 38.567


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
