"""Tests for src.utils.parsing — shared legals parser and parcel utils."""

from src.utils.parsing import parse_legals, strip_parcel_hyphens


class TestParseLegals:
    def test_single_parcel_and_subdivision(self):
        legals = (
            "{'Id': 2089893, 'LegalType': 'P', 'Description': '02-29-0-205-016', "
            "'Notes': None, 'PropertyNotes': None}; "
            "{'Id': 1336823, 'LegalType': 'S', 'Description': 'GOLDEN PARK  L: 16 B: 5', "
            "'Notes': None, 'PropertyNotes': None}"
        )
        parcel_ids, subdivisions = parse_legals(legals)
        assert parcel_ids == ["02-29-0-205-016"]
        assert subdivisions == ["GOLDEN PARK  L: 16 B: 5"]

    def test_multiple_parcels(self):
        legals = (
            "{'Id': 2089335, 'LegalType': 'P', 'Description': '03-19-0-219-012', "
            "'Notes': None, 'PropertyNotes': None}; "
            "{'Id': 2089334, 'LegalType': 'P', 'Description': '03-19-0-212-035', "
            "'Notes': None, 'PropertyNotes': None}; "
            "{'Id': 1336244, 'LegalType': 'S', 'Description': 'SUMMIT SPRINGS PHASE 2A  L: 109', "
            "'Notes': None, 'PropertyNotes': None}"
        )
        parcel_ids, subdivisions = parse_legals(legals)
        assert parcel_ids == ["03-19-0-219-012", "03-19-0-212-035"]
        assert subdivisions == ["SUMMIT SPRINGS PHASE 2A  L: 109"]

    def test_empty_string(self):
        parcel_ids, subdivisions = parse_legals("")
        assert parcel_ids == []
        assert subdivisions == []

    def test_malformed_entry(self):
        parcel_ids, subdivisions = parse_legals("not a dict at all")
        assert parcel_ids == []
        assert subdivisions == []


class TestStripParcelHyphens:
    def test_standard_parcel(self):
        assert strip_parcel_hyphens("01-35-0-402-022") == "01350402022"

    def test_no_hyphens(self):
        assert strip_parcel_hyphens("01350402022") == "01350402022"

    def test_empty(self):
        assert strip_parcel_hyphens("") == ""
