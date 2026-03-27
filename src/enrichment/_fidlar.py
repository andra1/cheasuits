"""Shared Fidlar County Recorder API utilities.

Extracted from mortgage.py to be reused by liens.py and other modules
that query the St. Clair County Recorder (Fidlar AVA Search API).
"""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

logger = logging.getLogger(__name__)

API_BASE = "https://ilstclair.fidlar.com/ILStClair/Scrap.WebService.Ava"
TOKEN_URL = f"{API_BASE}/token"
SEARCH_URL = f"{API_BASE}/breeze/Search"

REQUEST_DELAY = 0.3
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Token Management
# ---------------------------------------------------------------------------

_token_cache: dict[str, str] = {}


def get_token() -> str:
    """Obtain a Bearer token from the Fidlar token endpoint."""
    if "token" in _token_cache:
        return _token_cache["token"]

    data = urllib.parse.urlencode({
        "grant_type": "password",
        "username": "guest",
        "password": "guest",
    }).encode("utf-8")

    req = urllib.request.Request(
        TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    with urllib.request.urlopen(req, timeout=10) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        _token_cache["token"] = body["access_token"]
        logger.debug("Obtained Fidlar Bearer token")
        return _token_cache["token"]


# ---------------------------------------------------------------------------
# API Search
# ---------------------------------------------------------------------------

def search_documents(parcel_id: str, doc_type: str, doc_type_name: str) -> list[dict]:
    """Search Fidlar for documents of a given type on a parcel."""
    token = get_token()

    payload = {
        "FirstName": "",
        "LastBusinessName": "",
        "StartDate": "",
        "EndDate": "",
        "DocumentName": "",
        "DocumentType": doc_type,
        "SubdivisionName": "",
        "SubdivisionLot": "",
        "SubdivisionBlock": "",
        "MunicipalityName": "",
        "TractSection": "",
        "TractTownship": "",
        "TractRange": "",
        "TractQuarter": "",
        "TractQuarterQuarter": "",
        "AddressHouseNo": "",
        "AddressStreet": "",
        "AddressCity": "",
        "AddressZip": "",
        "ParcelNumber": parcel_id,
        "Book": "",
        "Page": "",
        "ReferenceNumber": "",
        "DisplayStartDate": "",
        "DisplayEndDate": "",
        "DocumentTypeDisplayName": doc_type_name,
    }

    body = json.dumps(payload).encode("utf-8")

    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(
                SEARCH_URL,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}",
                },
            )

            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            results = data.get("DocResults", [])
            logger.debug(
                f"Fidlar: {len(results)} {doc_type_name} docs for {parcel_id}"
            )
            return results

        except urllib.error.HTTPError as e:
            if e.code == 401 and attempt < MAX_RETRIES - 1:
                _token_cache.clear()
                token = get_token()
                continue
            logger.warning(f"Fidlar HTTP {e.code} for {parcel_id}: {e}")
            return []
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(REQUEST_DELAY * (attempt + 1))
                continue
            logger.warning(f"Fidlar error for {parcel_id}: {e}")
            return []

    return []


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_date(raw: str) -> str:
    """Parse Fidlar date string to YYYY-MM-DD."""
    if not raw:
        return ""
    # Strip fractional seconds while preserving AM/PM suffix
    cleaned = re.sub(r'\.\d+', '', raw)
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def parse_amount(doc: dict) -> float | None:
    """Extract amount from ConsiderationAmount or Notes field."""
    # Primary: ConsiderationAmount
    amount = doc.get("ConsiderationAmount")
    if amount:
        try:
            val = float(amount)
            if val > 0:
                return val
        except (ValueError, TypeError):
            pass

    # Fallback: parse dollar amount from Notes
    notes = doc.get("Notes", "") or ""
    match = re.search(r'\$\s*([\d,]+\.?\d*)', notes)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except ValueError:
            pass

    return None


def normalize_party_name(name: str) -> str:
    """Normalize a party name for fuzzy matching.

    Works for lender names, creditor names, etc.
    """
    name = name.upper().strip()
    # Strip common suffixes (order matters — strip longest first)
    for suffix in [
        " OF ILL CAHOKIA", " OF ILLINOIS", " OF ILL",
        " NA", " INC", " LLC", " CORP", " CO", " BANK",
        " BANKCENTRE", " BANKING", " FINANCIAL",
    ]:
        name = name.removesuffix(suffix)
    # Collapse whitespace
    return " ".join(name.split())
