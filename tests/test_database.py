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
    upsert_vacancy_records,
    get_vacancy_by_tract,
    get_vacancy_summary,
    get_untracted_properties,
    update_property_tract,
    get_untracted_delinquent,
    update_delinquent_tract,
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


SAMPLE_VACANCY = {
    "geoid": "17163000100",
    "state_fips": "17",
    "county_fips": "163",
    "tract_code": "000100",
    "year": 2025,
    "quarter": 1,
    "total_residential": 500,
    "vacant_residential": 25,
    "vacancy_rate_residential": 5.0,
    "no_stat_residential": 10,
    "total_business": 50,
    "vacant_business": 5,
    "vacancy_rate_business": 10.0,
    "no_stat_business": 2,
    "scraped_at": "2026-03-25T10:00:00",
}


class TestUspsVacancyTable:
    def test_table_created(self, db):
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='usps_vacancy'"
        )
        assert cursor.fetchone() is not None

    def test_upsert_insert(self, db):
        count = upsert_vacancy_records(db, [SAMPLE_VACANCY])
        assert count == 1
        rows = get_vacancy_by_tract(db, "17163000100")
        assert len(rows) == 1
        assert rows[0]["total_residential"] == 500
        assert rows[0]["vacant_residential"] == 25

    def test_upsert_updates_on_conflict(self, db):
        upsert_vacancy_records(db, [SAMPLE_VACANCY])
        updated = {**SAMPLE_VACANCY, "vacant_residential": 30, "vacancy_rate_residential": 6.0}
        upsert_vacancy_records(db, [updated])
        rows = get_vacancy_by_tract(db, "17163000100")
        assert len(rows) == 1
        assert rows[0]["vacant_residential"] == 30

    def test_upsert_empty_list(self, db):
        count = upsert_vacancy_records(db, [])
        assert count == 0

    def test_get_vacancy_by_tract(self, db):
        rec_q1 = {**SAMPLE_VACANCY}
        rec_q2 = {**SAMPLE_VACANCY, "quarter": 2, "vacant_residential": 30}
        upsert_vacancy_records(db, [rec_q1, rec_q2])
        rows = get_vacancy_by_tract(db, "17163000100")
        assert len(rows) == 2

    def test_get_vacancy_summary(self, db):
        upsert_vacancy_records(db, [SAMPLE_VACANCY])
        summary = get_vacancy_summary(db, state_fips="17")
        assert len(summary) >= 1
        assert summary[0]["geoid"] == "17163000100"


class TestCensusTractMigration:
    def test_properties_has_census_tract_column(self, db):
        cursor = db.execute("PRAGMA table_info(properties)")
        columns = [row[1] for row in cursor.fetchall()]
        assert "census_tract" in columns
        assert "tract_enriched_at" in columns

    def test_delinquent_has_census_tract_column(self, db):
        cursor = db.execute("PRAGMA table_info(delinquent_taxes)")
        columns = [row[1] for row in cursor.fetchall()]
        assert "census_tract" in columns
        assert "tract_enriched_at" in columns


class TestCensusTractHelpers:
    def test_get_untracted_properties(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        rows = get_untracted_properties(db)
        assert len(rows) == 1
        assert rows[0]["lat"] == 38.567

    def test_get_untracted_excludes_already_tracted(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_geocoding(db, "2224358", 38.567, -90.123)
        update_property_tract(db, "2224358", "17163000100")
        rows = get_untracted_properties(db)
        assert len(rows) == 0

    def test_get_untracted_excludes_ungeocoded(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_untracted_properties(db)
        assert len(rows) == 0

    def test_update_property_tract(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_property_tract(db, "2224358", "17163000100")
        rows = get_all(db)
        assert rows[0]["census_tract"] == "17163000100"
        assert rows[0]["tract_enriched_at"] is not None

    def test_update_delinquent_tract(self, db):
        from src.db.database import upsert_delinquent_taxes
        dt_record = {
            "parcel_id": "01350402022",
            "publication_year": 2026,
            "street": "209 EDWARDS ST",
            "city": "CAHOKIA",
            "source_file": "test.pdf",
            "scraped_at": "2026-03-25T10:00:00",
        }
        upsert_delinquent_taxes(db, [dt_record])
        row = db.execute("SELECT id FROM delinquent_taxes LIMIT 1").fetchone()
        update_delinquent_tract(db, row[0], "17163000100")
        rows = db.execute("SELECT * FROM delinquent_taxes WHERE id = ?", (row[0],)).fetchall()
        assert dict(rows[0])["census_tract"] == "17163000100"
        assert dict(rows[0])["tract_enriched_at"] is not None
