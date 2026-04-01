"""Tests for src.enrichment.valuation — AVM valuation module."""

import pytest
from unittest.mock import patch, MagicMock

from src.enrichment.valuation import (
    fetch_redfin_estimate,
    fetch_zillow_estimate,
    _extract_redfin_estimate_from_page,
    _discover_redfin_url,
)


class TestFetchRedfinEstimate:
    @patch("src.enrichment.valuation._get_session")
    @patch("src.enrichment.valuation._discover_redfin_url")
    def test_extracts_estimate_from_page(self, mock_discover, mock_get_session):
        """DDG discovers Redfin URL, page contains predictedValue."""
        mock_discover.return_value = "https://www.redfin.com/IL/Cahokia/209-Edwards-St-62206/home/12345"

        page_html = """<html><body>
        <script>{"avmInfo":{"displayLevel":1,"propertyId":12345,"predictedValue":125000.50}}</script>
        </body></html>"""

        page_resp = MagicMock()
        page_resp.text = page_html
        page_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = page_resp
        mock_get_session.return_value = session

        estimate, url = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate == 125000.50
        assert "redfin.com" in url

    @patch("src.enrichment.valuation._get_session")
    @patch("src.enrichment.valuation._discover_redfin_url")
    def test_returns_none_when_ddg_finds_nothing(self, mock_discover, mock_get_session):
        mock_discover.return_value = None

        estimate, url = fetch_redfin_estimate("999 Nonexistent Rd, Nowhere, IL 00000")
        assert estimate is None
        assert url is None

    @patch("src.enrichment.valuation._get_session")
    @patch("src.enrichment.valuation._discover_redfin_url")
    def test_returns_none_on_http_error(self, mock_discover, mock_get_session):
        mock_discover.return_value = "https://www.redfin.com/IL/Test/123-St-62000/home/99"

        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("HTTP 403")
        session.get.return_value = mock_resp
        mock_get_session.return_value = session

        estimate, url = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert estimate is None
        assert url is None

    @patch("src.enrichment.valuation._get_session")
    @patch("src.enrichment.valuation._discover_redfin_url")
    def test_extracts_from_next_data_fallback(self, mock_discover, mock_get_session):
        """Falls back to __NEXT_DATA__ when predictedValue not in avmInfo."""
        mock_discover.return_value = "https://www.redfin.com/IL/Test/1-St-62000/home/1"

        page_html = """<html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"initialRedfinEstimateValue":98000}}}
        </script>
        </body></html>"""

        page_resp = MagicMock()
        page_resp.text = page_html
        page_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = page_resp
        mock_get_session.return_value = session

        estimate, url = fetch_redfin_estimate("1 Test St, Test, IL 62000")
        assert estimate == 98000.0


class TestExtractRedfinEstimate:
    def test_predicted_value_primary(self):
        html = '{"avmInfo":{"predictedValue":219781.81},"other":"data"}'
        assert _extract_redfin_estimate_from_page(html) == 219781.81

    def test_next_data_fallback(self):
        html = """<script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"initialRedfinEstimateValue":88000}}}
        </script>"""
        assert _extract_redfin_estimate_from_page(html) == 88000.0

    def test_avm_amount_fallback(self):
        html = '{"avm": {"display": 1, "amount": 55000}}'
        assert _extract_redfin_estimate_from_page(html) == 55000.0

    def test_returns_none_for_empty_page(self):
        assert _extract_redfin_estimate_from_page("<html></html>") is None

    def test_returns_none_for_zero_value(self):
        html = '{"avmInfo":{"predictedValue":0}}'
        assert _extract_redfin_estimate_from_page(html) is None


class TestDiscoverRedfinUrl:
    @patch("src.enrichment.valuation.time.sleep")
    def test_extracts_url_from_ddg_results(self, mock_sleep):
        import urllib.parse
        redfin_url = "https://www.redfin.com/IL/Belleville/12-Concord-Dr-62223/home/134696183"
        encoded = urllib.parse.quote(redfin_url)
        ddg_html = f'<a href="?uddg={encoded}&amp;rut=abc">Redfin</a>'

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = ddg_html
        mock_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = mock_resp

        result = _discover_redfin_url("12 Concord Dr, Belleville, IL 62223", session)
        assert result == redfin_url

    @patch("src.enrichment.valuation.time.sleep")
    def test_returns_none_when_no_redfin_url(self, mock_sleep):
        ddg_html = '<a href="?uddg=https://zillow.com/test">Not Redfin</a>'

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = ddg_html
        mock_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.return_value = mock_resp

        result = _discover_redfin_url("999 Fake St, Nowhere, IL 00000", session)
        assert result is None

    @patch("src.enrichment.valuation.time.sleep")
    def test_retries_on_rate_limit(self, mock_sleep):
        rate_limited_resp = MagicMock()
        rate_limited_resp.status_code = 202
        rate_limited_resp.text = ""

        import urllib.parse
        redfin_url = "https://www.redfin.com/IL/Test/1-St-62000/home/123"
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.text = f'<a href="?uddg={urllib.parse.quote(redfin_url)}">Result</a>'
        ok_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.side_effect = [rate_limited_resp, ok_resp]

        result = _discover_redfin_url("1 Test St, Test, IL 62000", session)
        assert result == redfin_url
        assert mock_sleep.called


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
