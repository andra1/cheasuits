"""Tests for src.enrichment.census_tract — Census Bureau geocoder client."""

import json
import pytest

from src.enrichment.census_tract import parse_geocoder_response


# Realistic Census Bureau geocoder response
SAMPLE_RESPONSE = {
    "result": {
        "input": {
            "location": {"x": -90.123, "y": 38.567},
            "benchmark": {"benchmarkName": "Public_AR_Current"},
        },
        "geographies": {
            "Census Tracts": [
                {
                    "GEOID": "17163000100",
                    "STATE": "17",
                    "COUNTY": "163",
                    "TRACT": "000100",
                    "NAME": "Census Tract 1",
                    "CENTLAT": "+38.5670000",
                    "CENTLON": "-090.1230000",
                }
            ]
        },
    }
}


class TestParseGeocoderResponse:
    def test_extracts_geoid(self):
        geoid = parse_geocoder_response(SAMPLE_RESPONSE)
        assert geoid == "17163000100"

    def test_returns_none_on_empty_tracts(self):
        response = {
            "result": {
                "geographies": {
                    "Census Tracts": []
                }
            }
        }
        assert parse_geocoder_response(response) is None

    def test_returns_none_on_missing_geographies(self):
        response = {"result": {"geographies": {}}}
        assert parse_geocoder_response(response) is None

    def test_returns_none_on_malformed_response(self):
        assert parse_geocoder_response({}) is None
        assert parse_geocoder_response({"result": {}}) is None
