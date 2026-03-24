"""Shared parsing utilities for the distressed RE pipeline."""

from __future__ import annotations

import ast
import logging
import re

logger = logging.getLogger(__name__)


def parse_legals(legals_str: str) -> tuple[list[str], list[str]]:
    """Parse the legals field into parcel IDs and subdivision names.

    The legals field contains semicolon-separated Python dict literals.
    LegalType='P' entries have parcel numbers, LegalType='S' have subdivision info.

    Returns:
        (parcel_ids, subdivisions)
    """
    parcel_ids = []
    subdivisions = []

    if not legals_str:
        return parcel_ids, subdivisions

    chunks = re.split(r";\s*(?=\{)", legals_str.strip())

    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            entry = ast.literal_eval(chunk)
        except (ValueError, SyntaxError):
            logger.debug(f"Could not parse legal entry: {chunk[:80]}")
            continue

        legal_type = entry.get("LegalType", "")
        description = entry.get("Description", "")

        if legal_type == "P" and description:
            parcel_ids.append(description.strip())
        elif legal_type == "S" and description:
            subdivisions.append(description.strip())

    return parcel_ids, subdivisions


def strip_parcel_hyphens(parcel_id: str) -> str:
    """Remove hyphens from parcel ID for API queries.

    '01-35-0-402-022' -> '01350402022'
    """
    return parcel_id.replace("-", "")
