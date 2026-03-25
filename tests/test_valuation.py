"""Tests for src.enrichment.valuation — AVM valuation module."""

import urllib.error

import pytest
from unittest.mock import patch, MagicMock

from src.enrichment.valuation import (
    compute_assessed_multiplier,
    blend_estimates,
    fetch_redfin_estimate,
    fetch_zillow_estimate,
)


class TestComputeAssessedMultiplier:
    def test_basic_calculation(self):
        # $44,000 assessed * 1.0049 state multiplier / 0.3333 = ~$132,650
        result = compute_assessed_multiplier(44000.0, 1.0049)
        assert abs(result - 132650.0) < 100  # within $100

    def test_zero_assessed(self):
        result = compute_assessed_multiplier(0.0, 1.0049)
        assert result == 0.0

    def test_multiplier_of_one(self):
        # $10,000 * 1.0 / 0.3333 = ~$30,003
        result = compute_assessed_multiplier(10000.0, 1.0)
        assert abs(result - 30003.0) < 10


class TestBlendEstimates:
    """Tests covering all 7 rows of the decision matrix."""

    def test_both_external_within_threshold(self):
        # Both Zillow and Redfin, within 50% of assessed mult
        value, source, confidence = blend_estimates(100000.0, 110000.0, 105000.0)
        assert value == 107500.0  # avg(110000, 105000)
        assert source == "blended"
        assert confidence == "high"

    def test_both_external_exceeds_threshold(self):
        # Both external, but diverge >50% from assessed
        value, source, confidence = blend_estimates(50000.0, 110000.0, 105000.0)
        assert value == 107500.0  # still uses avg of external
        assert source == "blended"
        assert confidence == "low"

    def test_zillow_only_within_threshold(self):
        value, source, confidence = blend_estimates(100000.0, 110000.0, None)
        assert value == 110000.0
        assert source == "zillow"
        assert confidence == "high"

    def test_zillow_only_exceeds_threshold(self):
        value, source, confidence = blend_estimates(50000.0, 110000.0, None)
        assert value == 110000.0
        assert source == "zillow"
        assert confidence == "low"

    def test_redfin_only_within_threshold(self):
        value, source, confidence = blend_estimates(100000.0, None, 95000.0)
        assert value == 95000.0
        assert source == "redfin"
        assert confidence == "high"

    def test_redfin_only_exceeds_threshold(self):
        value, source, confidence = blend_estimates(50000.0, None, 120000.0)
        assert value == 120000.0
        assert source == "redfin"
        assert confidence == "low"

    def test_no_external_fallback_to_assessed(self):
        value, source, confidence = blend_estimates(100000.0, None, None)
        assert value == 100000.0
        assert source == "assessed_multiplier"
        assert confidence == "medium"


class TestFetchRedfinEstimate:
    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_extracts_estimate_from_page(self, mock_urlopen):
        # Mock autocomplete response
        autocomplete_resp = MagicMock()
        autocomplete_resp.read.return_value = b'{}&&{"payload":{"sections":[{"rows":[{"url":"/IL/Belleville/209-Edwards-St-62220/home/12345"}]}]}}'
        autocomplete_resp.__enter__ = lambda s: s
        autocomplete_resp.__exit__ = MagicMock(return_value=False)

        # Mock property page with estimate in script tag
        page_html = b"""<html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"initialRedfinEstimateValue":125000}}}
        </script>
        </body></html>"""
        page_resp = MagicMock()
        page_resp.read.return_value = page_html
        page_resp.__enter__ = lambda s: s
        page_resp.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [autocomplete_resp, page_resp]

        result = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result == 125000.0

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_no_autocomplete(self, mock_urlopen):
        autocomplete_resp = MagicMock()
        autocomplete_resp.read.return_value = b'{}&&{"payload":{"sections":[]}}'
        autocomplete_resp.__enter__ = lambda s: s
        autocomplete_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = autocomplete_resp

        result = fetch_redfin_estimate("999 Nonexistent Rd, Nowhere, IL 00000")
        assert result is None

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "http://example.com", 403, "Forbidden", {}, None
        )
        result = fetch_redfin_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result is None


class TestFetchZillowEstimate:
    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_extracts_zestimate_from_json_ld(self, mock_urlopen):
        page_html = b"""<html><body>
        <script type="application/json" id="__NEXT_DATA__">
        {"props":{"pageProps":{"componentProps":{"gdpClientCache":{"\\"zpid\\"123":{"property":{"zestimate":145000}}}}}}}
        </script>
        </body></html>"""
        mock_resp = MagicMock()
        mock_resp.read.return_value = page_html
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result == 145000.0

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_extracts_zestimate_from_regex_fallback(self, mock_urlopen):
        page_html = b"""<html><body>
        <script>"zestimate":98500,"zestimateLowPercent"</script>
        </body></html>"""
        mock_resp = MagicMock()
        mock_resp.read.return_value = page_html
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result == 98500.0

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "http://example.com", 403, "Forbidden", {}, None
        )
        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result is None

    @patch("src.enrichment.valuation.urllib.request.urlopen")
    def test_returns_none_on_no_estimate(self, mock_urlopen):
        page_html = b"<html><body>No data here</body></html>"
        mock_resp = MagicMock()
        mock_resp.read.return_value = page_html
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_zillow_estimate("209 Edwards St, Cahokia, IL 62206")
        assert result is None
