"""Valuation Module — Zillow/Redfin Estimates via curl_cffi.

Fetches property value estimates from Zillow and Redfin, stores each
as a row in the valuations table, and applies the priority rule to
set estimated_market_value on the properties table.

Usage:
    python -m src.enrichment.valuation [--db data/cheasuits.db] [-v]
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

SCRAPE_DELAY = 1.0
MAX_RETRIES = 2

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

IMPERSONATE_BROWSERS = ["chrome131", "chrome133", "chrome124"]


def _normalize_address(raw: str) -> str:
    return raw.replace("\n", ", ").strip()


def _get_session() -> cffi_requests.Session:
    browser = random.choice(IMPERSONATE_BROWSERS)
    return cffi_requests.Session(impersonate=browser)


def fetch_redfin_estimate(address: str) -> tuple[Optional[float], Optional[str]]:
    """Fetch Redfin Estimate. Returns (estimate, property_page_url) or (None, None)."""
    normalized = _normalize_address(address)
    encoded = urllib.parse.quote(normalized)
    session = _get_session()

    autocomplete_url = (
        f"https://www.redfin.com/stingray/do/location-autocomplete"
        f"?v=2&al=1&location={encoded}"
    )

    try:
        resp = session.get(autocomplete_url, headers={"Accept": "application/json"}, timeout=15)
        resp.raise_for_status()
        raw = resp.text
        json_str = raw.split("&&", 1)[-1] if "&&" in raw else raw
        data = json.loads(json_str)

        sections = data.get("payload", {}).get("sections", [])
        if not sections:
            return (None, None)
        rows = sections[0].get("rows", [])
        if not rows:
            return (None, None)
        property_url = rows[0].get("url", "")
        if not property_url:
            return (None, None)
    except Exception as e:
        logger.warning(f"Redfin autocomplete failed for {normalized}: {e}")
        return (None, None)

    page_url = f"https://www.redfin.com{property_url}"
    try:
        resp = session.get(page_url, headers={"Accept": "text/html"}, timeout=15)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning(f"Redfin page fetch failed for {property_url}: {e}")
        return (None, None)

    try:
        match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if match:
            page_data = json.loads(match.group(1))
            estimate = page_data.get("props", {}).get("pageProps", {}).get("initialRedfinEstimateValue")
            if estimate and isinstance(estimate, (int, float)) and estimate > 0:
                logger.info(f"Redfin estimate for {normalized}: ${estimate:,.0f}")
                return (float(estimate), page_url)

        avm_match = re.search(r'"avm":\s*\{[^}]*"amount":\s*(\d+)', html)
        if avm_match:
            estimate = float(avm_match.group(1))
            if estimate > 0:
                return (estimate, page_url)
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Redfin parse failed for {normalized}: {e}")

    return (None, None)


def fetch_zillow_estimate(address: str) -> tuple[Optional[float], Optional[str]]:
    """Fetch Zillow Zestimate. Returns (estimate, property_page_url) or (None, None)."""
    normalized = _normalize_address(address)
    slug = re.sub(r'[^\w,\s-]', '', normalized)
    slug = re.sub(r'[\s]+', '-', slug.strip())
    url = f"https://www.zillow.com/homes/{slug}_rb/"

    session = _get_session()

    try:
        resp = session.get(url, headers={"Accept": "text/html,application/xhtml+xml"}, timeout=15)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning(f"Zillow fetch failed for {normalized}: {e}")
        return (None, None)

    try:
        match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if match:
            page_data = json.loads(match.group(1))
            gdp_cache = page_data.get("props", {}).get("pageProps", {}).get("componentProps", {}).get("gdpClientCache", {})
            # gdpClientCache may be a JSON string that needs a second parse
            if isinstance(gdp_cache, str):
                gdp_cache = json.loads(gdp_cache)
            for cache_val in gdp_cache.values():
                # Values themselves may also be JSON strings
                if isinstance(cache_val, str):
                    try:
                        cache_val = json.loads(cache_val)
                    except (json.JSONDecodeError, ValueError):
                        continue
                if isinstance(cache_val, dict):
                    zest = cache_val.get("property", {}).get("zestimate")
                    if zest and isinstance(zest, (int, float)) and zest > 0:
                        logger.info(f"Zillow Zestimate for {normalized}: ${zest:,.0f}")
                        return (float(zest), url)
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.debug(f"Zillow JSON parse failed for {normalized}: {e}")

    zest_match = re.search(r'"zestimate"\s*:\s*(\d+)', html)
    if zest_match:
        value = float(zest_match.group(1))
        if value > 0:
            return (value, url)

    return (None, None)


def enrich_valuations_from_db(db_path: Path) -> None:
    """Fetch Zillow/Redfin valuations for properties, write to valuations table."""
    from src.db.database import get_db, upsert_valuation, apply_market_value_priority

    conn = get_db(db_path)
    cursor = conn.execute(
        """
        SELECT p.document_number, p.property_address
        FROM properties p
        WHERE p.property_address IS NOT NULL
          AND p.property_address != ''
          AND NOT EXISTS (
              SELECT 1 FROM valuations v
              WHERE v.document_number = p.document_number
                AND v.source IN ('zillow', 'redfin')
          )
        """
    )
    rows = [dict(row) for row in cursor.fetchall()]

    if not rows:
        print("No properties to value.")
        conn.close()
        return

    print(f"Valuing {len(rows)} properties via Zillow/Redfin...")
    valued = 0
    request_count = 0

    for i, row in enumerate(rows):
        doc_num = row["document_number"]
        address = row["property_address"]
        normalized_addr = _normalize_address(address)

        if request_count > 0:
            time.sleep(SCRAPE_DELAY)
        redfin_est, redfin_url = fetch_redfin_estimate(normalized_addr)
        request_count += 1

        if redfin_est is not None:
            upsert_valuation(conn, doc_num, {
                "source": "redfin",
                "estimate": redfin_est,
                "source_url": redfin_url,
                "confidence": "high",
            })

        time.sleep(SCRAPE_DELAY)
        zillow_est, zillow_url = fetch_zillow_estimate(normalized_addr)
        request_count += 1

        if zillow_est is not None:
            upsert_valuation(conn, doc_num, {
                "source": "zillow",
                "estimate": zillow_est,
                "source_url": zillow_url,
                "confidence": "high",
            })

        if redfin_est is not None or zillow_est is not None:
            valued += 1

        apply_market_value_priority(conn, doc_num)

        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} — "
            f"redfin={'$'+f'{redfin_est:,.0f}' if redfin_est else 'N/A'}, "
            f"zillow={'$'+f'{zillow_est:,.0f}' if zillow_est else 'N/A'}"
        )

    conn.close()
    print(f"\nValued {valued}/{len(rows)} properties")


def apply_all_priorities(db_path: Path) -> None:
    """Re-apply priority rule for all properties."""
    from src.db.database import get_db, apply_market_value_priority

    conn = get_db(db_path)
    cursor = conn.execute("SELECT document_number FROM properties")
    doc_nums = [row[0] for row in cursor.fetchall()]

    for doc_num in doc_nums:
        apply_market_value_priority(conn, doc_num)

    conn.close()
    print(f"Applied priority rule to {len(doc_nums)} properties")


def main():
    parser = argparse.ArgumentParser(description="Estimate market values via Zillow/Redfin")
    parser.add_argument("--db", type=str, default=str(DEFAULT_DB), help=f"Database path (default: {DEFAULT_DB})")
    parser.add_argument("--reprioritize", action="store_true", help="Re-apply priority rule for all properties")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db_path = Path(args.db)
    if args.reprioritize:
        apply_all_priorities(db_path)
    else:
        enrich_valuations_from_db(db_path)


if __name__ == "__main__":
    main()
