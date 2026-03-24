"""Tests for src.enrichment.assessor — DevNetWedge HTML parser."""

from src.enrichment.assessor import parse_assessor_html, AssessorRecord


# Minimal HTML fixture mimicking the DevNetWedge inner-label/inner-value structure.
SAMPLE_HTML = """
<html><body>
<table>
  <td>
    <div class="inner-label">Owner Name &amp; Address</div>
    <div class="inner-value" style="white-space:pre-line">ALLEN, RUTH
209 EDWARDS ST
CAHOKIA, IL 62206</div>
  </td>
  <td>
    <div class="inner-label">Site Address</div>
    <div class="inner-value whitespace-pre-line">209 EDWARDS ST
CAHOKIA, IL 62206</div>
  </td>
  <td>
    <div class="inner-label">Mailing Address</div>
    <div class="inner-value" style="white-space:pre-line">ALLEN, RUTH
209 EDWRADS ST
CAHOKIA, IL 62206</div>
  </td>
  <td>
    <div class="inner-label">Property Class</div>
    <div class="inner-value">0040 - Improved Lots</div>
  </td>
  <td>
    <div class="inner-label">Acres</div>
    <div class="inner-value">0.2500</div>
  </td>
  <td>
    <div class="inner-label">Net Taxable Value</div>
    <div class="inner-value">12,952</div>
  </td>
  <td>
    <div class="inner-label">Tax Rate</div>
    <div class="inner-value">19.022200</div>
  </td>
  <td>
    <div class="inner-label">Total Tax</div>
    <div class="inner-value">$2,463.76</div>
  </td>
</table>
</body></html>
"""

SAMPLE_HTML_TAX_SOLD = SAMPLE_HTML.replace(
    "</body>", "<span>PARCEL TAXES SOLD</span></body>"
)

SAMPLE_HTML_ABSENTEE = SAMPLE_HTML.replace(
    """<div class="inner-label">Mailing Address</div>
    <div class="inner-value" style="white-space:pre-line">ALLEN, RUTH
209 EDWRADS ST
CAHOKIA, IL 62206</div>""",
    """<div class="inner-label">Mailing Address</div>
    <div class="inner-value" style="white-space:pre-line">ALLEN, RUTH
456 OAK AVE
ST LOUIS, MO 63101</div>""",
)


class TestParseAssessorHtml:
    def test_parses_owner_name(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.owner_name == "ALLEN, RUTH"

    def test_parses_site_address(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert "209 EDWARDS ST" in record.property_address
        assert "CAHOKIA, IL 62206" in record.property_address

    def test_parses_assessed_value(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.net_taxable_value == 12952.0

    def test_parses_tax_rate(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.tax_rate == 19.0222

    def test_parses_total_tax(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.total_tax == 2463.76

    def test_parses_property_class(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.property_class == "0040 - Improved Lots"

    def test_parses_acres(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.acres == 0.25

    def test_detects_tax_sold(self):
        record = parse_assessor_html(SAMPLE_HTML_TAX_SOLD, "01-35-0-402-022")
        assert record.tax_status == "sold"

    def test_default_tax_status_paid(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.tax_status == "paid"

    def test_not_absentee_when_same_address(self):
        record = parse_assessor_html(SAMPLE_HTML, "01-35-0-402-022")
        assert record.absentee_owner is False

    def test_absentee_when_different_address(self):
        record = parse_assessor_html(SAMPLE_HTML_ABSENTEE, "01-35-0-402-022")
        assert record.absentee_owner is True

    def test_empty_html_returns_empty_record(self):
        record = parse_assessor_html("<html><body></body></html>", "01-35-0-402-022")
        assert record.parcel_id == "01-35-0-402-022"
        assert record.owner_name == ""
