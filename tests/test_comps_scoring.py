"""Tests for comps scoring redesign — sqft-primary scoring."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.enrichment.comps import _score_comp, _passes_sqft_filter


class TestSqftFilter:
    def test_rejects_over_30_percent_larger(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 1400}
        assert _passes_sqft_filter(subject, comp) is False

    def test_rejects_over_30_percent_smaller(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 600}
        assert _passes_sqft_filter(subject, comp) is False

    def test_accepts_within_30_percent(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 1200}
        assert _passes_sqft_filter(subject, comp) is True

    def test_accepts_exact_boundary(self):
        subject = {"sqft": 1000}
        comp = {"sqft": 1300}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_subject_missing_sqft(self):
        subject = {}
        comp = {"sqft": 1200}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_comp_missing_sqft(self):
        subject = {"sqft": 1000}
        comp = {}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_both_missing_sqft(self):
        subject = {}
        comp = {}
        assert _passes_sqft_filter(subject, comp) is True

    def test_passes_when_subject_sqft_zero(self):
        subject = {"sqft": 0}
        comp = {"sqft": 1200}
        assert _passes_sqft_filter(subject, comp) is True


class TestScoreComp:
    @patch("src.enrichment.comps.date")
    def test_sqft_is_dominant_factor(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 27)
        mock_date.side_effect = lambda *a, **kw: real_date(*a, **kw)

        subject = {"sqft": 1000}
        comp_a = {"sqft": 1000, "_distance": 2.5, "sale_date": "2026-01-01"}
        comp_b = {"sqft": 1250, "_distance": 0.1, "sale_date": "2026-01-01"}

        score_a = _score_comp(subject, comp_a)
        score_b = _score_comp(subject, comp_b)
        assert score_a > score_b, "Perfect sqft match should beat close distance"

    @patch("src.enrichment.comps.date")
    def test_missing_sqft_gets_penalty(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 27)
        mock_date.side_effect = lambda *a, **kw: real_date(*a, **kw)

        subject = {"sqft": 1000}
        comp_with = {"sqft": 1000, "_distance": 1.0, "sale_date": "2026-01-01"}
        comp_without = {"_distance": 1.0, "sale_date": "2026-01-01"}

        score_with = _score_comp(subject, comp_with)
        score_without = _score_comp(subject, comp_without)
        assert score_with > score_without, "Missing sqft should get penalty score"

    @patch("src.enrichment.comps.date")
    def test_weights_sum_correctly(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 27)
        mock_date.side_effect = lambda *a, **kw: real_date(*a, **kw)

        subject = {"sqft": 1000}
        comp = {"sqft": 1000, "_distance": 0, "sale_date": "2026-03-27"}
        score = _score_comp(subject, comp)
        assert score == pytest.approx(1.0, abs=0.01)


class TestEstimateFromComps:
    def test_sqft_adjustment_scales_price_up(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 1200}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert est == 120000.0
        assert count == 1

    def test_sqft_adjustment_scales_price_down(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 800}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert est == 80000.0

    def test_sqft_adjustment_clamped_at_1_3(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 2000}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert est == 130000.0

    def test_no_adjustment_when_sqft_missing(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 1000}
        comps = [
            {"sale_price": 100000, "_score": 0.8, "sale_date": "2026-01-15"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert est == 100000.0

    def test_confidence_high_with_3_comps(self):
        from src.enrichment.comps import estimate_from_comps
        subject = {"sqft": 1000}
        comps = [
            {"sale_price": 100000, "sqft": 1000, "_score": 0.9, "sale_date": "2026-01-15"},
            {"sale_price": 110000, "sqft": 1050, "_score": 0.8, "sale_date": "2026-01-20"},
            {"sale_price": 105000, "sqft": 980, "_score": 0.7, "sale_date": "2026-02-01"},
        ]
        est, count, conf = estimate_from_comps(subject, comps)
        assert count == 3
        assert conf == "high"
