"""Tests for valuations and property_comps tables and CRUD functions."""

import sqlite3

import pytest

from src.db.database import (
    get_db,
    upsert_valuation,
    get_valuations,
    insert_property_comps,
    get_property_comps,
    apply_market_value_priority,
)


@pytest.fixture
def db(tmp_path):
    """Create a fresh DB with one test property DOC001."""
    conn = get_db(tmp_path / "test.db")
    conn.execute(
        "INSERT INTO properties (document_number, parcel_id) VALUES ('DOC001', 'P001')"
    )
    conn.commit()
    return conn


# --- valuations table ---


def test_valuations_table_exists(db):
    """The valuations table should exist after schema init."""
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='valuations'"
    )
    assert cursor.fetchone() is not None


def test_upsert_valuation_inserts(db):
    """upsert_valuation inserts a new valuation row."""
    upsert_valuation(db, "DOC001", {"source": "zillow", "estimate": 250_000})
    rows = db.execute("SELECT * FROM valuations WHERE document_number = 'DOC001'").fetchall()
    assert len(rows) == 1
    assert rows[0]["source"] == "zillow"
    assert rows[0]["estimate"] == 250_000
    assert rows[0]["valued_at"] is not None


def test_upsert_valuation_upserts(db):
    """upsert_valuation overwrites when same (document_number, source)."""
    upsert_valuation(db, "DOC001", {"source": "zillow", "estimate": 250_000})
    upsert_valuation(db, "DOC001", {"source": "zillow", "estimate": 275_000})
    rows = db.execute("SELECT * FROM valuations WHERE document_number = 'DOC001'").fetchall()
    assert len(rows) == 1
    assert rows[0]["estimate"] == 275_000


def test_get_valuations(db):
    """get_valuations returns all valuation rows for a property."""
    upsert_valuation(db, "DOC001", {"source": "zillow", "estimate": 250_000})
    upsert_valuation(db, "DOC001", {"source": "redfin", "estimate": 260_000})
    vals = get_valuations(db, "DOC001")
    assert len(vals) == 2
    sources = {v["source"] for v in vals}
    assert sources == {"zillow", "redfin"}


# --- property_comps table ---


def test_property_comps_table_exists(db):
    """The property_comps table should exist after schema init."""
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='property_comps'"
    )
    assert cursor.fetchone() is not None


def test_insert_property_comps_inserts(db):
    """insert_property_comps inserts comp match rows."""
    # Insert a comparable sale first
    db.execute(
        "INSERT INTO comparable_sales (address, sale_price, sale_date, source) "
        "VALUES ('123 Main St', 200000, '2026-01-01', 'redfin')"
    )
    db.commit()
    comp_id = db.execute("SELECT id FROM comparable_sales").fetchone()["id"]

    comps = [
        {
            "comp_sale_id": comp_id,
            "distance_miles": 0.5,
            "similarity_score": 0.9,
            "lot_size_ratio": 1.1,
            "adjusted_price": 210_000,
        }
    ]
    insert_property_comps(db, "DOC001", comps)
    rows = db.execute("SELECT * FROM property_comps WHERE document_number = 'DOC001'").fetchall()
    assert len(rows) == 1
    assert rows[0]["distance_miles"] == 0.5
    assert rows[0]["matched_at"] is not None


def test_insert_property_comps_replaces(db):
    """insert_property_comps deletes existing comps then inserts new ones."""
    db.execute(
        "INSERT INTO comparable_sales (address, sale_price, sale_date, source) "
        "VALUES ('123 Main St', 200000, '2026-01-01', 'redfin')"
    )
    db.execute(
        "INSERT INTO comparable_sales (address, sale_price, sale_date, source) "
        "VALUES ('456 Oak Ave', 220000, '2026-02-01', 'redfin')"
    )
    db.commit()
    ids = [r["id"] for r in db.execute("SELECT id FROM comparable_sales").fetchall()]

    # Insert first set
    insert_property_comps(db, "DOC001", [
        {"comp_sale_id": ids[0], "distance_miles": 0.5, "similarity_score": 0.9,
         "lot_size_ratio": 1.0, "adjusted_price": 200_000},
    ])
    # Replace with second set
    insert_property_comps(db, "DOC001", [
        {"comp_sale_id": ids[1], "distance_miles": 0.3, "similarity_score": 0.95,
         "lot_size_ratio": 1.05, "adjusted_price": 215_000},
    ])
    rows = db.execute("SELECT * FROM property_comps WHERE document_number = 'DOC001'").fetchall()
    assert len(rows) == 1
    assert rows[0]["comp_sale_id"] == ids[1]


def test_get_property_comps(db):
    """get_property_comps returns joined comp details sorted by similarity_score DESC."""
    db.execute(
        "INSERT INTO comparable_sales (address, sale_price, sale_date, source, sqft, beds, baths) "
        "VALUES ('123 Main St', 200000, '2026-01-01', 'redfin', 1500, 3, 2)"
    )
    db.execute(
        "INSERT INTO comparable_sales (address, sale_price, sale_date, source, sqft, beds, baths) "
        "VALUES ('456 Oak Ave', 220000, '2026-02-01', 'redfin', 1600, 3, 2)"
    )
    db.commit()
    ids = [r["id"] for r in db.execute("SELECT id FROM comparable_sales").fetchall()]

    insert_property_comps(db, "DOC001", [
        {"comp_sale_id": ids[0], "distance_miles": 0.5, "similarity_score": 0.8,
         "lot_size_ratio": 1.0, "adjusted_price": 200_000},
        {"comp_sale_id": ids[1], "distance_miles": 0.3, "similarity_score": 0.95,
         "lot_size_ratio": 1.05, "adjusted_price": 215_000},
    ])
    comps = get_property_comps(db, "DOC001")
    assert len(comps) == 2
    # Should be sorted by similarity_score DESC
    assert comps[0]["similarity_score"] == 0.95
    assert comps[1]["similarity_score"] == 0.8
    # Should include joined comparable_sales fields
    assert comps[0]["address"] == "456 Oak Ave"
    assert comps[0]["sale_price"] == 220_000


# --- apply_market_value_priority ---


def test_apply_market_value_priority_redfin_wins_over_comps(db):
    """Redfin valuation should win over comps-only valuation."""
    upsert_valuation(db, "DOC001", {"source": "redfin", "estimate": 300_000})
    upsert_valuation(db, "DOC001", {"source": "comps", "estimate": 280_000})
    apply_market_value_priority(db, "DOC001")
    row = db.execute("SELECT estimated_market_value, valuation_source FROM properties WHERE document_number = 'DOC001'").fetchone()
    assert row["estimated_market_value"] == 300_000
    assert row["valuation_source"] == "redfin"


def test_apply_market_value_priority_averages_redfin_zillow(db):
    """When both Redfin and Zillow exist, average them."""
    upsert_valuation(db, "DOC001", {"source": "redfin", "estimate": 300_000})
    upsert_valuation(db, "DOC001", {"source": "zillow", "estimate": 320_000})
    apply_market_value_priority(db, "DOC001")
    row = db.execute("SELECT estimated_market_value, valuation_source FROM properties WHERE document_number = 'DOC001'").fetchone()
    assert row["estimated_market_value"] == 310_000
    assert row["valuation_source"] == "redfin+zillow"


def test_apply_market_value_priority_comps_fallback(db):
    """When no external source, comps estimate is used."""
    upsert_valuation(db, "DOC001", {"source": "comps", "estimate": 280_000})
    apply_market_value_priority(db, "DOC001")
    row = db.execute("SELECT estimated_market_value, valuation_source FROM properties WHERE document_number = 'DOC001'").fetchone()
    assert row["estimated_market_value"] == 280_000
    assert row["valuation_source"] == "comps"


def test_apply_market_value_priority_no_valuations(db):
    """When no valuations exist, market value stays NULL."""
    apply_market_value_priority(db, "DOC001")
    row = db.execute("SELECT estimated_market_value, valuation_source FROM properties WHERE document_number = 'DOC001'").fetchone()
    assert row["estimated_market_value"] is None
