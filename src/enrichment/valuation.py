"""AVM Valuation Module — Assessed Value Multiplier + Zillow/Redfin Hybrid.

Estimates as-is market value for properties in the pipeline database.
Uses county assessed values (with state equalization multiplier) as baseline,
supplemented by Zillow/Redfin public estimates where available.

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
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# St. Clair County, IL
# State equalization multiplier — published annually by IL Dept. of Revenue
# https://tax.illinois.gov (search "equalization factor")
# The Board of Review Equalized value from DevNetWedge already includes
# county-level equalization. This multiplier is the STATE-level correction.
STCLAIR_STATE_MULTIPLIER = 1.0049  # Tax year 2024 — verify annually
ASSESSMENT_RATIO = 1 / 3  # Illinois statutory 33.33%

SCRAPE_DELAY = 1.0  # seconds between external requests
MAX_RETRIES = 2
DIVERGENCE_THRESHOLD = 0.5  # 50% divergence triggers low confidence

DEFAULT_DB = Path(__file__).resolve().parent.parent.parent / "data" / "cheasuits.db"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# Assessed Value Multiplier
# ---------------------------------------------------------------------------

def compute_assessed_multiplier(assessed_value: float, state_multiplier: float) -> float:
    """Compute estimated market value from county assessed value.

    Formula: assessed_value * state_multiplier / ASSESSMENT_RATIO
    """
    return round(assessed_value * state_multiplier / ASSESSMENT_RATIO, 2)


# ---------------------------------------------------------------------------
# Blending Logic
# ---------------------------------------------------------------------------

def blend_estimates(
    assessed_mult: float,
    zillow: Optional[float],
    redfin: Optional[float],
) -> tuple[float, str, str]:
    """Blend valuation signals into a final estimate.

    Returns: (estimated_market_value, valuation_source, valuation_confidence)

    Decision matrix:
    - Both external available -> average, check divergence from assessed
    - One external available -> use it, check divergence from assessed
    - No external -> fall back to assessed multiplier with "medium" confidence
    """
    if zillow is not None and redfin is not None:
        value = round((zillow + redfin) / 2, 2)
        source = "blended"
    elif zillow is not None:
        value = zillow
        source = "zillow"
    elif redfin is not None:
        value = redfin
        source = "redfin"
    else:
        return (assessed_mult, "assessed_multiplier", "medium")

    # Check divergence between external estimate and assessed multiplier
    if assessed_mult > 0:
        divergence = abs(value - assessed_mult) / assessed_mult
        confidence = "low" if divergence > DIVERGENCE_THRESHOLD else "high"
    else:
        confidence = "high"  # can't check divergence without assessed baseline

    return (value, source, confidence)


# ---------------------------------------------------------------------------
# Address Normalization
# ---------------------------------------------------------------------------

def _normalize_address(raw: str) -> str:
    """Flatten multi-line DB address to single line for URL queries."""
    return raw.replace("\n", ", ").strip()


def _get_user_agent() -> str:
    """Return a random user agent string."""
    return random.choice(USER_AGENTS)


# ---------------------------------------------------------------------------
# Redfin Scraper
# ---------------------------------------------------------------------------

def fetch_redfin_estimate(address: str) -> Optional[float]:
    """Fetch Redfin Estimate for a property address.

    1. Hit autocomplete endpoint to resolve address -> property URL
    2. Fetch property page
    3. Extract estimate from __NEXT_DATA__ JSON

    Returns estimated value or None if unavailable.
    """
    normalized = _normalize_address(address)
    encoded = urllib.parse.quote(normalized)

    # Step 1: Autocomplete to get property URL
    autocomplete_url = (
        f"https://www.redfin.com/stingray/do/location-autocomplete"
        f"?v=2&al=1&location={encoded}"
    )

    try:
        req = urllib.request.Request(autocomplete_url, headers={
            "User-Agent": _get_user_agent(),
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")

        # Redfin prefixes response with '{}&&'
        json_str = raw.split("&&", 1)[-1] if "&&" in raw else raw
        data = json.loads(json_str)

        sections = data.get("payload", {}).get("sections", [])
        if not sections:
            logger.debug(f"Redfin: no autocomplete results for {normalized}")
            return None

        rows = sections[0].get("rows", [])
        if not rows:
            logger.debug(f"Redfin: no rows in autocomplete for {normalized}")
            return None

        property_url = rows[0].get("url", "")
        if not property_url:
            return None

    except Exception as e:
        logger.warning(f"Redfin autocomplete failed for {normalized}: {e}")
        return None

    # Step 2: Fetch property page
    try:
        page_url = f"https://www.redfin.com{property_url}"
        req = urllib.request.Request(page_url, headers={
            "User-Agent": _get_user_agent(),
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")

    except Exception as e:
        logger.warning(f"Redfin page fetch failed for {property_url}: {e}")
        return None

    # Step 3: Extract estimate from __NEXT_DATA__ or page content
    try:
        # Try __NEXT_DATA__ JSON blob
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if match:
            page_data = json.loads(match.group(1))
            estimate = (
                page_data.get("props", {})
                .get("pageProps", {})
                .get("initialRedfinEstimateValue")
            )
            if estimate and isinstance(estimate, (int, float)) and estimate > 0:
                logger.info(f"Redfin estimate for {normalized}: ${estimate:,.0f}")
                return float(estimate)

        # Fallback: search for avm pattern in HTML
        avm_match = re.search(r'"avm":\s*\{[^}]*"amount":\s*(\d+)', html)
        if avm_match:
            estimate = float(avm_match.group(1))
            if estimate > 0:
                logger.info(f"Redfin AVM for {normalized}: ${estimate:,.0f}")
                return estimate

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Redfin parse failed for {normalized}: {e}")

    logger.debug(f"Redfin: no estimate found for {normalized}")
    return None


# ---------------------------------------------------------------------------
# Zillow Scraper
# ---------------------------------------------------------------------------

def fetch_zillow_estimate(address: str) -> Optional[float]:
    """Fetch Zillow Zestimate for a property address.

    1. Construct search URL from address
    2. Fetch page
    3. Extract Zestimate from __NEXT_DATA__ JSON or regex fallback

    Returns estimated value or None if unavailable.
    """
    normalized = _normalize_address(address)

    # Format address for Zillow URL: "209 Edwards St, Cahokia, IL 62206" -> "209-Edwards-St,-Cahokia,-IL-62206"
    slug = re.sub(r'[^\w,\s-]', '', normalized)
    slug = re.sub(r'[\s]+', '-', slug.strip())
    url = f"https://www.zillow.com/homes/{slug}_rb/"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": _get_user_agent(),
            "Accept": "text/html,application/xhtml+xml",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")

    except Exception as e:
        logger.warning(f"Zillow fetch failed for {normalized}: {e}")
        return None

    # Try __NEXT_DATA__ JSON blob
    try:
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            page_data = json.loads(match.group(1))
            # Navigate the nested gdpClientCache structure
            gdp_cache = (
                page_data.get("props", {})
                .get("pageProps", {})
                .get("componentProps", {})
                .get("gdpClientCache", {})
            )
            for cache_val in gdp_cache.values():
                if isinstance(cache_val, dict):
                    zest = cache_val.get("property", {}).get("zestimate")
                    if zest and isinstance(zest, (int, float)) and zest > 0:
                        logger.info(f"Zillow Zestimate for {normalized}: ${zest:,.0f}")
                        return float(zest)

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.debug(f"Zillow JSON parse failed for {normalized}: {e}")

    # Regex fallback: find "zestimate":NNNNN in page content
    zest_match = re.search(r'"zestimate"\s*:\s*(\d+)', html)
    if zest_match:
        value = float(zest_match.group(1))
        if value > 0:
            logger.info(f"Zillow Zestimate (regex) for {normalized}: ${value:,.0f}")
            return value

    logger.debug(f"Zillow: no Zestimate found for {normalized}")
    return None
