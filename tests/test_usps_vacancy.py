"""Tests for src.ingestion.usps_vacancy — HUD NCWM API client."""

import json
import pytest

from src.ingestion.usps_vacancy import (
    parse_api_response,
    VacancyRecord,
)

# Realistic API response structure based on HUD NCWM API docs
SAMPLE_API_RESULT = {
    "TRACT_ID": "17163000100",
    "STATE_GEOID": "17",
    "COUNTY_GEOID": "163",
    "TOTAL_RESIDENTIAL_ADDRESSES": 500,
    "ACTIVE_RESIDENTIAL_ADDRESSES": 460,
    "STV_RESIDENTIAL_ADDRESSES": 15,
    "LTV_RESIDENTIAL_ADDRESSES": 10,
    "NO_STAT_RESIDENTIAL_ADDRESSES": 15,
    "TOTAL_BUSINESS_ADDRESSES": 50,
    "ACTIVE_BUSINESS_ADDRESSES": 43,
    "STV_BUSINESS_ADDRESSES": 3,
    "LTV_BUSINESS_ADDRESSES": 2,
    "NO_STAT_BUSINESS_ADDRESSES": 2,
}


class TestParseApiResponse:
    def test_parses_single_result(self):
        records = parse_api_response([SAMPLE_API_RESULT], year=2025, quarter=1)
        assert len(records) == 1
        r = records[0]
        assert r.geoid == "17163000100"
        assert r.state_fips == "17"
        assert r.county_fips == "163"
        assert r.tract_code == "000100"
        assert r.year == 2025
        assert r.quarter == 1
        assert r.total_residential == 500
        assert r.vacant_residential == 25  # STV + LTV
        assert r.no_stat_residential == 15
        assert r.total_business == 50
        assert r.vacant_business == 5  # STV + LTV

    def test_vacancy_rate_calculation(self):
        records = parse_api_response([SAMPLE_API_RESULT], year=2025, quarter=1)
        r = records[0]
        assert r.vacancy_rate_residential == pytest.approx(5.0, abs=0.1)
        assert r.vacancy_rate_business == pytest.approx(10.0, abs=0.1)

    def test_empty_list(self):
        records = parse_api_response([], year=2025, quarter=1)
        assert records == []

    def test_zero_total_no_division_error(self):
        result = {**SAMPLE_API_RESULT, "TOTAL_RESIDENTIAL_ADDRESSES": 0,
                  "ACTIVE_RESIDENTIAL_ADDRESSES": 0, "STV_RESIDENTIAL_ADDRESSES": 0,
                  "LTV_RESIDENTIAL_ADDRESSES": 0}
        records = parse_api_response([result], year=2025, quarter=1)
        assert records[0].vacancy_rate_residential == 0.0

    def test_to_dict(self):
        records = parse_api_response([SAMPLE_API_RESULT], year=2025, quarter=1)
        d = records[0].to_dict()
        assert d["geoid"] == "17163000100"
        assert "scraped_at" in d


class TestVacancyRecord:
    def test_dataclass_fields(self):
        r = VacancyRecord(
            geoid="17163000100",
            state_fips="17",
            county_fips="163",
            tract_code="000100",
            year=2025,
            quarter=1,
            total_residential=500,
            vacant_residential=25,
            vacancy_rate_residential=5.0,
            no_stat_residential=10,
            total_business=50,
            vacant_business=5,
            vacancy_rate_business=10.0,
            no_stat_business=2,
        )
        assert r.geoid == "17163000100"
