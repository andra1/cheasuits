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
REDFIN_SEARCH_DELAY = 5.0  # DuckDuckGo rate-limit protection
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


def _discover_redfin_url(address: str, session: cffi_requests.Session) -> Optional[str]:
    """Find the Redfin property page URL via DuckDuckGo search.

    Returns the full URL (e.g. https://www.redfin.com/IL/.../home/123)
    or None if not found.
    """
    query = f"site:redfin.com {address}"
    ddg_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    max_attempts = 4

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(ddg_url, headers={"Accept": "text/html"}, timeout=15)
            if resp.status_code == 202:
                # DuckDuckGo rate-limit; exponential back off
                wait = REDFIN_SEARCH_DELAY * (2 ** (attempt - 1))
                logger.debug(f"DDG rate-limited (attempt {attempt}), waiting {wait:.0f}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()

            # Extract Redfin URLs from DDG redirect params
            for match in re.finditer(r"uddg=([^&\"]+)", resp.text):
                decoded = urllib.parse.unquote(match.group(1))
                if "redfin.com" in decoded and "/home/" in decoded:
                    logger.debug(f"DDG resolved {address} -> {decoded}")
                    return decoded

            logger.debug(f"DDG returned no Redfin URL for {address}")
            return None
        except Exception as e:
            logger.warning(f"DDG search failed for {address} (attempt {attempt}): {e}")
            if attempt < max_attempts:
                time.sleep(REDFIN_SEARCH_DELAY * attempt)

    return None


def _extract_redfin_estimate_from_page(html: str) -> Optional[float]:
    """Extract the Redfin AVM predicted value from a property page HTML."""
    # Primary: predictedValue in avmInfo JSON blob
    pv_match = re.search(r'"predictedValue":([\d.]+)', html)
    if pv_match:
        try:
            value = float(pv_match.group(1))
            if value > 0:
                return value
        except ValueError:
            pass

    # Fallback: __NEXT_DATA__ initialRedfinEstimateValue
    try:
        nd_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if nd_match:
            page_data = json.loads(nd_match.group(1))
            estimate = (
                page_data.get("props", {})
                .get("pageProps", {})
                .get("initialRedfinEstimateValue")
            )
            if estimate and isinstance(estimate, (int, float)) and estimate > 0:
                return float(estimate)
    except (json.JSONDecodeError, KeyError, ValueError):
        pass

    # Fallback: avm amount
    avm_match = re.search(r'"avm":\s*\{[^}]*"amount":\s*(\d+)', html)
    if avm_match:
        try:
            value = float(avm_match.group(1))
            if value > 0:
                return value
        except ValueError:
            pass

    return None


def fetch_redfin_estimate(address: str) -> tuple[Optional[float], Optional[str]]:
    """Fetch Redfin Estimate via DuckDuckGo URL discovery + property page.

    Returns (estimate, property_page_url) or (None, None).
    """
    normalized = _normalize_address(address)
    session = _get_session()

    # Step 1: Discover the Redfin property page URL
    redfin_url = _discover_redfin_url(normalized, session)
    if not redfin_url:
        logger.info(f"No Redfin page found for {normalized}")
        return (None, None)

    # Step 2: Fetch the property page
    try:
        time.sleep(SCRAPE_DELAY)
        resp = session.get(
            redfin_url,
            headers={"Accept": "text/html,application/xhtml+xml"},
            timeout=20,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning(f"Redfin page fetch failed for {redfin_url}: {e}")
        return (None, None)

    # Step 3: Extract the AVM estimate
    estimate = _extract_redfin_estimate_from_page(html)
    if estimate is not None:
        logger.info(f"Redfin estimate for {normalized}: ${estimate:,.0f}")
        return (estimate, redfin_url)

    logger.info(f"No Redfin estimate in page for {normalized}")
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


def enrich_valuations_from_db(db_path: Path, redfin_only: bool = False) -> None:
    """Fetch Zillow/Redfin valuations for properties, write to valuations table.

    If redfin_only=True, only fetch Redfin estimates (skips Zillow).
    Stores Redfin estimates in both the valuations table and the
    redfin_estimate column on the properties table.
    """
    from src.db.database import get_db, upsert_valuation, apply_market_value_priority

    conn = get_db(db_path)

    if redfin_only:
        # Fetch Redfin for all properties that don't have a Redfin valuation yet
        cursor = conn.execute(
            """
            SELECT p.document_number, p.property_address
            FROM properties p
            WHERE p.property_address IS NOT NULL
              AND p.property_address != ''
              AND NOT EXISTS (
                  SELECT 1 FROM valuations v
                  WHERE v.document_number = p.document_number
                    AND v.source = 'redfin'
              )
            """
        )
    else:
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

    mode = "Redfin only" if redfin_only else "Zillow/Redfin"
    print(f"Valuing {len(rows)} properties via {mode}...")
    valued = 0
    request_count = 0

    for i, row in enumerate(rows):
        doc_num = row["document_number"]
        address = row["property_address"]
        normalized_addr = _normalize_address(address)

        # --- Redfin ---
        if request_count > 0:
            time.sleep(REDFIN_SEARCH_DELAY)
        redfin_est, redfin_url = fetch_redfin_estimate(normalized_addr)
        request_count += 1

        if redfin_est is not None:
            upsert_valuation(conn, doc_num, {
                "source": "redfin",
                "estimate": redfin_est,
                "source_url": redfin_url,
                "confidence": "high",
            })
            # Also store directly on the properties row
            conn.execute(
                "UPDATE properties SET redfin_estimate = ? WHERE document_number = ?",
                (redfin_est, doc_num),
            )
            conn.commit()

        # --- Zillow (skip if redfin_only) ---
        zillow_est = None
        if not redfin_only:
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

        redfin_str = f"${redfin_est:,.0f}" if redfin_est else "N/A"
        zillow_str = f"${zillow_est:,.0f}" if zillow_est else ("skip" if redfin_only else "N/A")
        logger.info(
            f"[{i+1}/{len(rows)}] {doc_num} — "
            f"redfin={redfin_str}, zillow={zillow_str}"
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
    parser.add_argument("--redfin-only", action="store_true", help="Only fetch Redfin estimates (skip Zillow)")
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
        enrich_valuations_from_db(db_path, redfin_only=args.redfin_only)


if __name__ == "__main__":
    main()
