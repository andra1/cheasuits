"""Tests for src.enrichment.valuation — AVM valuation module."""

import urllib.error

import pytest
from unittest.mock import patch, MagicMock

from src.enrichment.valuation import (
    compute_assessed_multiplier,
    blend_estimates,
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
