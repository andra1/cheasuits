"""Tests for src.enrichment.mortgage — Fidlar mortgage scraper and DB integration."""

import pytest

from src.enrichment.mortgage import (
    _parse_date,
    _parse_amount,
    _normalize_lender,
    _match_releases,
    MortgageRecord,
    fetch_mortgage_history,
    get_active_mortgages,
    get_total_mortgage_debt,
)
from src.db.database import (
    get_db,
    upsert_records,
    update_mortgage,
    get_unmortgaged_properties,
    set_mortgage_error,
    get_all,
)


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_standard_format(self):
        assert _parse_date("03/15/2024 12:00:00 PM") == "2024-03-15"

    def test_iso_format(self):
        assert _parse_date("2024-03-15T12:00:00") == "2024-03-15"

    def test_short_format(self):
        assert _parse_date("03/15/2024") == "2024-03-15"

    def test_empty_string(self):
        assert _parse_date("") == ""

    def test_none_returns_empty(self):
        assert _parse_date(None) == ""

    def test_unrecognized_format_returns_raw(self):
        assert _parse_date("March 15, 2024") == "March 15, 2024"

    def test_handles_fractional_seconds(self):
        # Fidlar sometimes includes .0000000 fractional seconds
        assert _parse_date("03/15/2024 12:00:00.0000000 PM") == "2024-03-15"


# ---------------------------------------------------------------------------
# Amount parsing
# ---------------------------------------------------------------------------

class TestParseAmount:
    def test_consideration_amount(self):
        doc = {"ConsiderationAmount": "150000.00"}
        assert _parse_amount(doc) == 150000.0

    def test_consideration_amount_zero_falls_through(self):
        doc = {"ConsiderationAmount": "0", "Notes": "Mortgage amount $85,000.00"}
        assert _parse_amount(doc) == 85000.0

    def test_notes_dollar_amount(self):
        doc = {"Notes": "Original amount: $125,500.50"}
        assert _parse_amount(doc) == 125500.50

    def test_notes_dollar_with_commas(self):
        doc = {"Notes": "Loan of $1,250,000"}
        assert _parse_amount(doc) == 1250000.0

    def test_no_amount_returns_none(self):
        doc = {"Notes": "No dollar amount here"}
        assert _parse_amount(doc) is None

    def test_empty_doc_returns_none(self):
        assert _parse_amount({}) is None

    def test_none_notes_returns_none(self):
        doc = {"Notes": None}
        assert _parse_amount(doc) is None


# ---------------------------------------------------------------------------
# Lender normalization
# ---------------------------------------------------------------------------

class TestNormalizeLender:
    def test_strips_na_suffix(self):
        assert _normalize_lender("WELLS FARGO BANK NA") == "WELLS FARGO"

    def test_strips_inc(self):
        assert _normalize_lender("MERS INC") == "MERS"

    def test_strips_llc(self):
        assert _normalize_lender("QUICKEN LOANS LLC") == "QUICKEN LOANS"

    def test_strips_of_illinois(self):
        assert _normalize_lender("GCS CREDIT UNION OF ILLINOIS") == "GCS CREDIT UNION"

    def test_strips_of_ill(self):
        assert _normalize_lender("BANK OF ILL") == "BANK"

    def test_uppercase_and_strip(self):
        assert _normalize_lender("  wells fargo  ") == "WELLS FARGO"

    def test_collapses_whitespace(self):
        assert _normalize_lender("WELLS   FARGO   BANK") == "WELLS FARGO"

    def test_empty_string(self):
        assert _normalize_lender("") == ""


# ---------------------------------------------------------------------------
# Release matching
# ---------------------------------------------------------------------------

class TestMatchReleases:
    def _make_mortgage(self, doc_num, date, lender, amount=100000.0):
        return MortgageRecord(
            document_number=doc_num,
            recorded_date=date,
            borrower="SMITH, JOHN",
            lender=lender,
            amount=amount,
            parcel_id="01-35-0-205-009",
        )

    def test_match_by_associated_document(self):
        mortgages = [self._make_mortgage("M001", "2020-01-15", "WELLS FARGO BANK NA")]
        releases = [{
            "document_number": "R001",
            "recorded_date": "2024-06-01",
            "lender": "WELLS FARGO BANK NA",
            "borrower": "SMITH, JOHN",
            "associated_docs": ["M001"],
        }]
        _match_releases(mortgages, releases)
        assert mortgages[0].is_released is True

    def test_match_by_lender_name(self):
        mortgages = [self._make_mortgage("M002", "2018-05-01", "GCS CREDIT UNION")]
        releases = [{
            "document_number": "R002",
            "recorded_date": "2023-01-15",
            "lender": "GCS CREDIT UNION OF ILLINOIS",
            "borrower": "SMITH, JOHN",
            "associated_docs": [],
        }]
        _match_releases(mortgages, releases)
        assert mortgages[0].is_released is True

    def test_no_match_leaves_active(self):
        mortgages = [self._make_mortgage("M003", "2022-01-01", "QUICKEN LOANS")]
        releases = [{
            "document_number": "R003",
            "recorded_date": "2023-01-01",
            "lender": "COMPLETELY DIFFERENT BANK",
            "borrower": "SMITH, JOHN",
            "associated_docs": [],
        }]
        _match_releases(mortgages, releases)
        assert mortgages[0].is_released is False

    def test_release_before_mortgage_does_not_match(self):
        mortgages = [self._make_mortgage("M004", "2023-06-01", "BANK ONE")]
        releases = [{
            "document_number": "R004",
            "recorded_date": "2020-01-01",  # before mortgage
            "lender": "BANK ONE",
            "borrower": "SMITH, JOHN",
            "associated_docs": [],
        }]
        _match_releases(mortgages, releases)
        assert mortgages[0].is_released is False

    def test_multiple_mortgages_partial_release(self):
        m1 = self._make_mortgage("M005", "2018-01-01", "BANK A", 50000.0)
        m2 = self._make_mortgage("M006", "2022-01-01", "BANK B", 100000.0)
        releases = [{
            "document_number": "R005",
            "recorded_date": "2023-01-01",
            "lender": "BANK A",
            "borrower": "SMITH, JOHN",
            "associated_docs": ["M005"],
        }]
        _match_releases([m1, m2], releases)
        assert m1.is_released is True
        assert m2.is_released is False

    def test_empty_releases_leaves_all_active(self):
        mortgages = [
            self._make_mortgage("M007", "2020-01-01", "LENDER A"),
            self._make_mortgage("M008", "2022-01-01", "LENDER B"),
        ]
        _match_releases(mortgages, [])
        assert all(not m.is_released for m in mortgages)


# ---------------------------------------------------------------------------
# DB integration — mortgage source tracking
# ---------------------------------------------------------------------------

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
    "legals_raw": "{}",
    "source": "ava_search_stclair",
    "scraped_at": "2026-03-23T20:19:53",
}


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_db(db_path)
    yield conn
    conn.close()


class TestMortgageDbIntegration:
    def test_mortgage_source_column_exists(self, db):
        cursor = db.execute("PRAGMA table_info(properties)")
        columns = [row[1] for row in cursor.fetchall()]
        assert "mortgage_source" in columns

    def test_update_mortgage_stores_source(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_mortgage(db, "2224358", {
            "mortgage_amount": 85000.0,
            "mortgage_date": "2020-01-15",
            "mortgage_lender": "WELLS FARGO",
            "total_mortgage_debt": 85000.0,
            "mortgage_count": 1,
            "mortgage_source": "fidlar_recorder",
        })
        rows = get_all(db)
        assert rows[0]["mortgage_source"] == "fidlar_recorder"
        assert rows[0]["mortgage_amount"] == 85000.0
        assert rows[0]["mortgage_enriched_at"] is not None

    def test_update_mortgage_zero_debt(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_mortgage(db, "2224358", {
            "mortgage_amount": 0,
            "mortgage_date": None,
            "mortgage_lender": None,
            "total_mortgage_debt": 0,
            "mortgage_count": 0,
            "mortgage_source": "fidlar_recorder",
        })
        rows = get_all(db)
        assert rows[0]["mortgage_amount"] == 0
        assert rows[0]["mortgage_source"] == "fidlar_recorder"

    def test_get_unmortgaged_returns_unenriched(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        rows = get_unmortgaged_properties(db)
        assert len(rows) == 1

    def test_get_unmortgaged_excludes_enriched(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_mortgage(db, "2224358", {
            "mortgage_amount": 50000.0,
            "mortgage_source": "fidlar_recorder",
        })
        rows = get_unmortgaged_properties(db)
        assert len(rows) == 0

    def test_get_unmortgaged_excludes_errored(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        set_mortgage_error(db, "2224358", "API timeout")
        rows = get_unmortgaged_properties(db)
        assert len(rows) == 0

    def test_mortgage_source_not_set_when_omitted(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        update_mortgage(db, "2224358", {
            "mortgage_amount": 50000.0,
        })
        rows = get_all(db)
        assert rows[0]["mortgage_source"] is None

    def test_set_mortgage_error_records_timestamp(self, db):
        upsert_records(db, [SAMPLE_RECORD])
        set_mortgage_error(db, "2224358", "connection refused")
        rows = get_all(db)
        assert rows[0]["mortgage_error"] == "connection refused"
        assert rows[0]["mortgage_enriched_at"] is not None


# ---------------------------------------------------------------------------
# Live verification tests (hit actual Fidlar API)
# ---------------------------------------------------------------------------

@pytest.mark.live
class TestLiveFidlarVerification:
    """Tests that hit the live Fidlar API to verify scraping correctness.

    Run with: pytest tests/test_mortgage.py -m live -v
    Skipped by default in CI (no network access).
    """

    def test_known_parcel_has_mortgage_history(self):
        """Parcel 01-35-0-402-022 should have mortgage recordings."""
        history = fetch_mortgage_history("01-35-0-402-022")
        assert len(history) > 0, "Expected mortgage recordings for this parcel"

    def test_mortgage_records_have_required_fields(self):
        history = fetch_mortgage_history("01-35-0-402-022")
        if not history:
            pytest.skip("No mortgage data returned — network issue?")
        for m in history:
            assert m.parcel_id == "01-35-0-402-022"
            assert m.document_number, "document_number should not be empty"
            assert m.recorded_date, "recorded_date should not be empty"

    def test_active_mortgages_have_amounts(self):
        active = get_active_mortgages("01-35-0-402-022")
        for m in active:
            assert m.amount is not None, f"Active mortgage {m.document_number} has no amount"
            assert m.amount > 0, f"Active mortgage {m.document_number} has zero amount"
            assert not m.is_released, "Active mortgage should not be released"

    def test_total_debt_matches_sum_of_active(self):
        active = get_active_mortgages("01-35-0-402-022")
        if not active:
            pytest.skip("No active mortgages to verify")
        total = get_total_mortgage_debt("01-35-0-402-022")
        expected = sum(m.amount for m in active)
        assert total == expected

    def test_released_mortgages_sorted_newest_first(self):
        history = fetch_mortgage_history("01-35-0-402-022")
        if len(history) < 2:
            pytest.skip("Need at least 2 mortgages to verify sort order")
        dates = [m.recorded_date for m in history if m.recorded_date]
        assert dates == sorted(dates, reverse=True), "Should be sorted newest first"

    def test_parcel_with_no_mortgage(self):
        """Some parcels may have zero mortgages — verify None return."""
        total = get_total_mortgage_debt("99-99-9-999-999")
        assert total is None

    def test_mortgage_source_format(self):
        """Verify that mortgage records come from the expected Fidlar source."""
        history = fetch_mortgage_history("01-35-0-205-009")
        if not history:
            pytest.skip("No mortgage data returned")
        # All records should have parcel_id matching what we requested
        for m in history:
            assert m.parcel_id == "01-35-0-205-009"
