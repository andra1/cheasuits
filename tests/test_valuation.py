"""Tests for src.enrichment.valuation — AVM valuation module."""

import pytest
from unittest.mock import patch, MagicMock

from src.enrichment.valuation import (
    fetch_redfin_estimate,
    fetch_zillow_estimate,
)


class TestFetchRedfinEstimate:
    @patch("src.enrichment.valuation._get_session")
    def test_extracts_estimate_from_page(self, mock_get_session):
        import json

        autocomplete_body = json.dumps({
            "payload": {
                "sections": [{"rows": [{"url": "/IL/Belleville/209-Edwards-St-62220/home/12345"}]}]
            }
        })

        page_html = """<html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"initialRedfinEstimateValue":125000}}}
        </script>
        </body></html>"""

        autocomplete_resp = MagicMock()
        autocomplete_resp.text = "{}&&" + autocomplete_body
        autocomplete_resp.raise_for_status = MagicMock()

        page_resp = MagicMock()
        page_resp.text = page_html
        page_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.side_effect = [autocomplete_resp, page_resp]
        mock_get_session.return_value = session

        estimate, url = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate == 125000.0
        assert url is not None

    @patch("src.enrichment.valuation._get_session")
    def test_returns_none_on_no_autocomplete(self, mock_get_session):
        import json

        autocomplete_body = json.dumps({"payload": {"sections": []}})

        autocomplete_resp = MagicMock()
        autocomplete_resp.text = "{}&&" + autocomplete_body
        autocomplete_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = autocomplete_resp
        mock_get_session.return_value = session

        estimate, url = fetch_redfin_estimate("999 Nonexistent Rd, Nowhere, IL 00000")
        assert estimate is None
        assert url is None

    @patch("src.enrichment.valuation._get_session")
    def test_returns_none_on_http_error(self, mock_get_session):
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("HTTP 403")
        session.get.return_value = mock_resp
        mock_get_session.return_value = session

        estimate, url = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate is None
        assert url is None


class TestFetchZillowEstimate:
    @patch("src.enrichment.valuation._get_session")
    def test_extracts_zestimate_from_json(self, mock_get_session):
        page_html = """<html><body>
        <script type="application/json" id="__NEXT_DATA__">
        {"props":{"pageProps":{"componentProps":{"gdpClientCache":{"zpid123":{"property":{"zestimate":145000}}}}}}}
        </script>
        </body></html>"""

        mock_resp = MagicMock()
        mock_resp.text = page_html
        mock_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = mock_resp
        mock_get_session.return_value = session

        estimate, url = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate == 145000.0
        assert url is not None

    @patch("src.enrichment.valuation._get_session")
    def test_extracts_zestimate_from_regex_fallback(self, mock_get_session):
        page_html = """<html><body>
        <script>"zestimate":98500,"zestimateLowPercent"</script>
        </body></html>"""

        mock_resp = MagicMock()
        mock_resp.text = page_html
        mock_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = mock_resp
        mock_get_session.return_value = session

        estimate, url = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate == 98500.0

    @patch("src.enrichment.valuation._get_session")
    def test_returns_none_on_http_error(self, mock_get_session):
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("HTTP 403")
        session.get.return_value = mock_resp
        mock_get_session.return_value = session

        estimate, url = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate is None
        assert url is None

    @patch("src.enrichment.valuation._get_session")
    def test_returns_none_on_no_estimate(self, mock_get_session):
        page_html = "<html><body>No data here</body></html>"

        mock_resp = MagicMock()
        mock_resp.text = page_html
        mock_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = mock_resp
        mock_get_session.return_value = session

        estimate, url = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate is None
        assert url is None


from src.enrichment.valuation import enrich_valuations_from_db
from src.db.database import (
    get_db, upsert_records, update_enrichment, get_all, get_valuations,
)


SAMPLE_RECORD = {
    "document_number": "2224358",
    "case_number": "26-FC-121",
    "case_type": "FC",
    "case_year": "2026",
    "recorded_date": "2026-03-23",
    "party1": "",
    "party2": "ALLEN RUTH",
    "parcel_id": "01-35-0-402-022",
    "subdivision": "EDWARD PLACE L: 28",
    "legals_raw": "",
    "source": "ava_search_stclair",
    "scraped_at": "2026-03-23T20:19:53",
}


class TestEnrichValuationsFromDb:
    @patch("src.enrichment.valuation.fetch_redfin_estimate", return_value=(None, None))
    @patch("src.enrichment.valuation.fetch_zillow_estimate", return_value=(None, None))
    def test_no_valuations_when_both_fail(self, mock_zillow, mock_redfin, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        upsert_records(conn, [SAMPLE_RECORD])
        update_enrichment(conn, "2224358", {
            "assessed_value": 12952.0,
            "property_address": "209 Edwards St\nCahokia, IL 62206",
        })
        conn.close()

        enrich_valuations_from_db(db_path)

        conn = get_db(db_path)
        valuations = get_valuations(conn, "2224358")
        conn.close()

        assert len(valuations) == 0

    @patch("src.enrichment.valuation.fetch_redfin_estimate", return_value=(42000.0, "https://redfin.com/test"))
    @patch("src.enrichment.valuation.fetch_zillow_estimate", return_value=(None, None))
    def test_stores_redfin_valuation_in_table(self, mock_zillow, mock_redfin, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        upsert_records(conn, [SAMPLE_RECORD])
        update_enrichment(conn, "2224358", {
            "assessed_value": 12952.0,
            "property_address": "209 Edwards St\nCahokia, IL 62206",
        })
        conn.close()

        enrich_valuations_from_db(db_path)

        conn = get_db(db_path)
        valuations = get_valuations(conn, "2224358")
        rows = get_all(conn)
        conn.close()

        assert any(v["source"] == "redfin" and v["estimate"] == 42000.0 for v in valuations)
        assert rows[0]["estimated_market_value"] == 42000.0
        assert rows[0]["valuation_source"] == "redfin"
