"""Tests for src.enrichment.liens — lien enrichment module."""

from __future__ import annotations

from src.enrichment.liens import LienRecord, _match_releases


class TestMatchReleases:
    """Test lien release matching logic."""

    def test_match_by_associated_docs(self):
        liens = [
            LienRecord(
                document_number="DOC-001",
                lien_type="federal_tax",
                recorded_date="2020-01-15",
                creditor="IRS",
                debtor="JOHN DOE",
                amount=15000.0,
                parcel_id="01-35-0-205-009",
            ),
            LienRecord(
                document_number="DOC-002",
                lien_type="judgment",
                recorded_date="2021-06-01",
                creditor="ACME CORP",
                debtor="JOHN DOE",
                amount=5000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        releases = [
            {
                "document_number": "REL-001",
                "recorded_date": "2022-03-01",
                "creditor": "IRS",
                "debtor": "JOHN DOE",
                "associated_docs": ["DOC-001"],
            },
        ]

        _match_releases(liens, releases)

        assert liens[0].is_released is True
        assert liens[1].is_released is False

    def test_match_by_creditor_name(self):
        liens = [
            LienRecord(
                document_number="DOC-010",
                lien_type="state_tax",
                recorded_date="2019-05-01",
                creditor="STATE OF ILLINOIS",
                debtor="JANE SMITH",
                amount=8000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        releases = [
            {
                "document_number": "REL-010",
                "recorded_date": "2021-08-15",
                "creditor": "STATE OF ILLINOIS",
                "debtor": "JANE SMITH",
                "associated_docs": [],
            },
        ]

        _match_releases(liens, releases)

        assert liens[0].is_released is True

    def test_no_match_release_before_lien(self):
        """Release dated before lien should not match."""
        liens = [
            LienRecord(
                document_number="DOC-020",
                lien_type="judgment",
                recorded_date="2022-01-01",
                creditor="BANK OF AMERICA",
                debtor="BOB JONES",
                amount=20000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        releases = [
            {
                "document_number": "REL-020",
                "recorded_date": "2021-06-01",  # before lien
                "creditor": "BANK OF AMERICA",
                "debtor": "BOB JONES",
                "associated_docs": [],
            },
        ]

        _match_releases(liens, releases)

        assert liens[0].is_released is False

    def test_no_match_different_creditor(self):
        """Different creditor should not match."""
        liens = [
            LienRecord(
                document_number="DOC-030",
                lien_type="federal_tax",
                recorded_date="2020-01-01",
                creditor="IRS",
                debtor="ALICE WALKER",
                amount=12000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        releases = [
            {
                "document_number": "REL-030",
                "recorded_date": "2021-01-01",
                "creditor": "STATE OF ILLINOIS",
                "debtor": "ALICE WALKER",
                "associated_docs": [],
            },
        ]

        _match_releases(liens, releases)

        assert liens[0].is_released is False

    def test_assoc_doc_takes_priority(self):
        """AssociatedDocuments match should take priority over name match."""
        liens = [
            LienRecord(
                document_number="DOC-040",
                lien_type="judgment",
                recorded_date="2020-01-01",
                creditor="ACME CORP",
                debtor="TOM HANKS",
                amount=10000.0,
                parcel_id="01-35-0-205-009",
            ),
            LienRecord(
                document_number="DOC-041",
                lien_type="judgment",
                recorded_date="2021-01-01",
                creditor="ACME CORP",
                debtor="TOM HANKS",
                amount=15000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        releases = [
            {
                "document_number": "REL-040",
                "recorded_date": "2022-01-01",
                "creditor": "ACME CORP",
                "debtor": "TOM HANKS",
                "associated_docs": ["DOC-040"],
            },
        ]

        _match_releases(liens, releases)

        # Only DOC-040 should be released (via assoc doc), not DOC-041
        assert liens[0].is_released is True
        assert liens[1].is_released is False

    def test_empty_liens(self):
        _match_releases([], [{"associated_docs": []}])

    def test_empty_releases(self):
        liens = [
            LienRecord(
                document_number="DOC-050",
                lien_type="federal_tax",
                recorded_date="2020-01-01",
                creditor="IRS",
                debtor="JOHN DOE",
                amount=5000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        _match_releases(liens, [])
        assert liens[0].is_released is False

    def test_multiple_releases_for_multiple_liens(self):
        liens = [
            LienRecord(
                document_number="DOC-060",
                lien_type="federal_tax",
                recorded_date="2019-01-01",
                creditor="IRS",
                debtor="JOHN DOE",
                amount=10000.0,
                parcel_id="01-35-0-205-009",
            ),
            LienRecord(
                document_number="DOC-061",
                lien_type="state_tax",
                recorded_date="2020-01-01",
                creditor="STATE OF ILLINOIS",
                debtor="JOHN DOE",
                amount=5000.0,
                parcel_id="01-35-0-205-009",
            ),
        ]
        releases = [
            {
                "document_number": "REL-060",
                "recorded_date": "2021-01-01",
                "creditor": "IRS",
                "debtor": "JOHN DOE",
                "associated_docs": ["DOC-060"],
            },
            {
                "document_number": "REL-061",
                "recorded_date": "2022-01-01",
                "creditor": "STATE OF ILLINOIS",
                "debtor": "JOHN DOE",
                "associated_docs": [],
            },
        ]

        _match_releases(liens, releases)

        assert liens[0].is_released is True
        assert liens[1].is_released is True
