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
