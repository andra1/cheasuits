"""Tests for src.scoring.viability — deal viability scoring engine."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from src.scoring.viability import (
    calculate_equity,
    score_equity_spread,
    score_comp_confidence,
    score_neighborhood_vacancy,
    score_buy_box,
    score_time_pressure,
    score_owner_reachability,
    calculate_viability_score,
)


# ---------------------------------------------------------------------------
# calculate_equity
# ---------------------------------------------------------------------------

class TestCalculateEquity:
    def test_all_present(self):
        burden, spread, ratio = calculate_equity(200000, 100000, 10000, 5000)
        assert burden == 115000.0
        assert spread == 85000.0
        assert ratio == pytest.approx(0.425, abs=0.001)

    def test_no_debts(self):
        burden, spread, ratio = calculate_equity(200000, None, None, None)
        assert burden == 0.0
        assert spread == 200000.0
        assert ratio == 1.0

    def test_negative_equity(self):
        burden, spread, ratio = calculate_equity(100000, 80000, 30000, 5000)
        assert burden == 115000.0
        assert spread == -15000.0
        assert ratio == pytest.approx(-0.15, abs=0.001)

    def test_no_market_value(self):
        result = calculate_equity(None, 100000, 10000, 5000)
        assert result == (None, None, None)

    def test_zero_market_value(self):
        result = calculate_equity(0, 100000, 10000, 5000)
        assert result == (None, None, None)

    def test_only_mortgage(self):
        burden, spread, ratio = calculate_equity(200000, 150000, None, None)
        assert burden == 150000.0
        assert spread == 50000.0
        assert ratio == 0.25

    def test_only_liens(self):
        burden, spread, ratio = calculate_equity(200000, None, 25000, None)
        assert burden == 25000.0
        assert spread == 175000.0

    def test_only_delinquent_tax(self):
        burden, spread, ratio = calculate_equity(200000, None, None, 3000)
        assert burden == 3000.0
        assert spread == 197000.0


# ---------------------------------------------------------------------------
# score_equity_spread
# ---------------------------------------------------------------------------

class TestScoreEquitySpread:
    def test_none(self):
        assert score_equity_spread(None) == 0

    def test_negative(self):
        assert score_equity_spread(-0.15) == 0

    def test_zero(self):
        assert score_equity_spread(0.0) == 10

    def test_low_positive(self):
        assert score_equity_spread(0.19) == 10

    def test_boundary_20(self):
        assert score_equity_spread(0.2) == 18

    def test_mid_range(self):
        assert score_equity_spread(0.35) == 18

    def test_boundary_40(self):
        assert score_equity_spread(0.4) == 24

    def test_upper_mid(self):
        assert score_equity_spread(0.55) == 24

    def test_boundary_60(self):
        assert score_equity_spread(0.6) == 30

    def test_high(self):
        assert score_equity_spread(0.85) == 30

    def test_full_equity(self):
        assert score_equity_spread(1.0) == 30


# ---------------------------------------------------------------------------
# score_comp_confidence
# ---------------------------------------------------------------------------

class TestScoreCompConfidence:
    def test_none(self):
        assert score_comp_confidence(None) == 0

    def test_high(self):
        assert score_comp_confidence("high") == 20

    def test_medium(self):
        assert score_comp_confidence("medium") == 12

    def test_low(self):
        assert score_comp_confidence("low") == 5

    def test_case_insensitive(self):
        assert score_comp_confidence("High") == 20
        assert score_comp_confidence("MEDIUM") == 12

    def test_unknown(self):
        assert score_comp_confidence("unknown") == 0


# ---------------------------------------------------------------------------
# score_neighborhood_vacancy
# ---------------------------------------------------------------------------

class TestScoreNeighborhoodVacancy:
    def test_none(self):
        assert score_neighborhood_vacancy(None) == 7

    def test_very_low(self):
        assert score_neighborhood_vacancy(2.0) == 15

    def test_boundary_5(self):
        assert score_neighborhood_vacancy(5.0) == 12

    def test_moderate(self):
        assert score_neighborhood_vacancy(7.0) == 12

    def test_boundary_8(self):
        assert score_neighborhood_vacancy(8.0) == 8

    def test_elevated(self):
        assert score_neighborhood_vacancy(10.0) == 8

    def test_boundary_12(self):
        assert score_neighborhood_vacancy(12.0) == 4

    def test_high(self):
        assert score_neighborhood_vacancy(14.0) == 4

    def test_boundary_15(self):
        assert score_neighborhood_vacancy(15.0) == 0

    def test_very_high(self):
        assert score_neighborhood_vacancy(25.0) == 0

    def test_zero(self):
        assert score_neighborhood_vacancy(0.0) == 15


# ---------------------------------------------------------------------------
# score_buy_box
# ---------------------------------------------------------------------------

class TestScoreBuyBox:
    def test_none(self):
        assert score_buy_box(None) == 0

    def test_in_box_min(self):
        assert score_buy_box(30000) == 15

    def test_in_box_max(self):
        assert score_buy_box(150000) == 15

    def test_in_box_mid(self):
        assert score_buy_box(75000) == 15

    def test_below_edge(self):
        # 20% below min: 30000 * 0.8 = 24000
        assert score_buy_box(25000) == 8

    def test_above_edge(self):
        # 20% above max: 150000 * 1.2 = 180000
        assert score_buy_box(170000) == 8

    def test_far_below(self):
        assert score_buy_box(20000) == 0

    def test_far_above(self):
        assert score_buy_box(200000) == 0

    def test_custom_range(self):
        assert score_buy_box(50000, min_val=40000, max_val=60000) == 15
        assert score_buy_box(35000, min_val=40000, max_val=60000) == 8
        assert score_buy_box(20000, min_val=40000, max_val=60000) == 0

    def test_zero_assessed(self):
        assert score_buy_box(0) == 0


# ---------------------------------------------------------------------------
# score_time_pressure
# ---------------------------------------------------------------------------

class TestScoreTimePressure:
    def test_none(self):
        assert score_time_pressure(None) == 0

    def test_empty_string(self):
        assert score_time_pressure("") == 0

    def test_invalid_date(self):
        assert score_time_pressure("not-a-date") == 0

    @patch("src.scoring.viability.date")
    def test_very_old(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 25)
        mock_date.side_effect = lambda *args, **kw: real_date(*args, **kw)
        # 400 days ago
        assert score_time_pressure("2025-02-18") == 10

    @patch("src.scoring.viability.date")
    def test_200_days(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 25)
        mock_date.side_effect = lambda *args, **kw: real_date(*args, **kw)
        # ~200 days ago
        assert score_time_pressure("2025-09-06") == 8

    @patch("src.scoring.viability.date")
    def test_120_days(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 25)
        mock_date.side_effect = lambda *args, **kw: real_date(*args, **kw)
        # ~120 days ago
        assert score_time_pressure("2025-11-25") == 6

    @patch("src.scoring.viability.date")
    def test_60_days(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 25)
        mock_date.side_effect = lambda *args, **kw: real_date(*args, **kw)
        # ~60 days ago
        assert score_time_pressure("2026-01-24") == 4

    @patch("src.scoring.viability.date")
    def test_10_days(self, mock_date):
        from datetime import date as real_date
        mock_date.today.return_value = real_date(2026, 3, 25)
        mock_date.side_effect = lambda *args, **kw: real_date(*args, **kw)
        # 10 days ago
        assert score_time_pressure("2026-03-15") == 2


# ---------------------------------------------------------------------------
# score_owner_reachability
# ---------------------------------------------------------------------------

class TestScoreOwnerReachability:
    def test_none(self):
        assert score_owner_reachability(None) == 0

    def test_empty_string(self):
        assert score_owner_reachability("") == 0

    def test_illinois_address(self):
        assert score_owner_reachability("123 Main St\nBelleville, IL 62220") == 10

    def test_out_of_state(self):
        assert score_owner_reachability("456 Oak Ave\nSt. Louis, MO 63101") == 6

    def test_address_no_state(self):
        assert score_owner_reachability("789 Elm Dr") == 5

    def test_illinois_inline(self):
        assert score_owner_reachability("123 Main St, Cahokia, IL 62206") == 10


# ---------------------------------------------------------------------------
# calculate_viability_score (composite)
# ---------------------------------------------------------------------------

class TestCalculateViabilityScore:
    def test_full_property(self):
        row = {
            "estimated_market_value": 100000,
            "total_mortgage_debt": 20000,
            "total_recorded_liens": 5000,
            "_delinquent_tax": 3000,
            "_comp_confidence": "high",
            "assessed_value": 35000,
            "recorded_date": "2025-06-01",
            "mailing_address": "123 Main St\nBelleville, IL 62220",
        }
        result = calculate_viability_score(row, vacancy_rate=4.0)

        assert result["total_lien_burden"] == 28000.0
        assert result["equity_spread"] == 72000.0
        assert result["equity_ratio"] == pytest.approx(0.72, abs=0.01)
        assert result["viability_score"] > 0
        assert "equity_spread" in json.loads(result["viability_details"])

    def test_no_data(self):
        row = {
            "estimated_market_value": None,
            "total_mortgage_debt": None,
            "total_recorded_liens": None,
            "_delinquent_tax": None,
            "_comp_confidence": None,
            "assessed_value": None,
            "recorded_date": None,
            "mailing_address": None,
        }
        result = calculate_viability_score(row, vacancy_rate=None)

        assert result["viability_score"] == 7  # only vacancy neutral score
        assert result["equity_spread"] is None

    def test_underwater_property(self):
        row = {
            "estimated_market_value": 50000,
            "total_mortgage_debt": 60000,
            "total_recorded_liens": 10000,
            "_delinquent_tax": 5000,
            "_comp_confidence": "low",
            "assessed_value": 15000,
            "recorded_date": "2025-01-01",
            "mailing_address": None,
        }
        result = calculate_viability_score(row, vacancy_rate=10.0)

        assert result["equity_spread"] == -25000.0
        assert result["equity_ratio"] < 0
        # equity_spread score should be 0 for negative
        details = json.loads(result["viability_details"])
        assert details["equity_spread"] == 0

    def test_details_structure(self):
        row = {
            "estimated_market_value": 100000,
            "total_mortgage_debt": 0,
            "total_recorded_liens": 0,
            "_delinquent_tax": 0,
            "_comp_confidence": "medium",
            "assessed_value": 50000,
            "recorded_date": "2026-01-01",
            "mailing_address": "123 Main, Springfield, MO 65801",
        }
        result = calculate_viability_score(row, vacancy_rate=6.0)
        details = json.loads(result["viability_details"])

        expected_keys = {
            "equity_spread", "comp_confidence", "neighborhood_vacancy",
            "buy_box", "time_pressure", "owner_reachability",
        }
        assert set(details.keys()) == expected_keys
        # Verify total equals sum of parts
        assert result["viability_score"] == sum(details.values())
