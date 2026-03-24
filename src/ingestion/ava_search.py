"""
AVA Search Scraper — St. Clair County Lis Pendens

Scrapes the Fidlar AVA Search portal for St. Clair County, IL to pull
new Lis Pendens filings (pre-foreclosure leads).

Architecture:
    1. Playwright loads the AVA Search SPA
    2. The app auto-authenticates (Bearer JWT from /token endpoint, no login needed)
    3. We intercept the API response from /breeze/Search
    4. Results are parsed into structured LisPendensRecord objects
    5. Output to CSV and/or returned as list of dicts

Usage:
    # CLI — pull last 7 days and save to CSV
    python -m src.ingestion.ava_search --days 7 --output leads.csv

    # As a module
    from src.ingestion.ava_search import AvaSearchScraper
    async with AvaSearchScraper() as scraper:
        records = await scraper.fetch_lis_pendens(days_back=30)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, Page, Response

    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://ilstclair.fidlar.com/ILStClair/AvaWeb/#/search"
API_BASE = "https://ilstclair.fidlar.com/ILStClair/Scrap.WebService.Ava"
TOKEN_URL = f"{API_BASE}/token"
SEARCH_URL = f"{API_BASE}/breeze/Search"
DOC_TYPES_URL = f"{API_BASE}/breeze/DocumentTypes"
SEARCH_API = "/Scrap.WebService.Ava/breeze/Search"
DOC_TYPE_LIS_PENDENS = "100"

DEFAULT_TIMEOUT_MS = 30_000
NAVIGATION_TIMEOUT_MS = 15_000


# ---------------------------------------------------------------------------
# Data Model
# ---------------------------------------------------------------------------

@dataclass
class LisPendensRecord:
    """Single lis pendens filing from AVA Search."""

    document_number: str = ""
    document_type: str = ""
    recorded_date: str = ""
    party1: str = ""          # usually the case number
    party2: str = ""          # usually the defendant (borrower)
    legals: str = ""          # subdivision / legal description
    source: str = "ava_search_stclair"
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # Fields parsed from party1 (case number)
    case_number: str = ""
    case_year: str = ""
    case_type: str = ""       # FC = foreclosure, CH = chancery, CV = civil

    def __post_init__(self):
        self._parse_case_number()
        self._parse_legal_description()

    def _parse_case_number(self):
        """Extract structured case info from party1 field.

        Examples:
            'CASE NO 26-FC-121' -> year=26, type=FC, number=121
            'CASE NO 24-CV-2507' -> year=24, type=CV, number=2507
        """
        if not self.party1:
            return
        match = re.search(r"(\d{2})-([A-Z]{2})-(\d+)", self.party1)
        if match:
            self.case_year = f"20{match.group(1)}"
            self.case_type = match.group(2)
            self.case_number = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    def _parse_legal_description(self):
        """Clean up the legals field (comes truncated from index search)."""
        if isinstance(self.legals, list):
            self.legals = "; ".join(str(item) for item in self.legals)
        if self.legals:
            self.legals = self.legals.strip()

    @property
    def is_foreclosure(self) -> bool:
        """True if the case type indicates a foreclosure (FC)."""
        return self.case_type == "FC"

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class AvaSearchScraper:
    """Playwright-based scraper for the Fidlar AVA Search portal.

    Approach:
        Rather than reverse-engineering the OAuth token flow, we let the
        browser handle authentication naturally. The SPA auto-obtains a
        Bearer token on page load (no login required for AVA/index search).

        We intercept the XHR response to /breeze/Search to get structured
        JSON data, which is cleaner than parsing the HTML table.
    """

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self._browser = None

    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        return self

    async def __aexit__(self, *exc):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def fetch_lis_pendens(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days_back: int = 7,
    ) -> list[LisPendensRecord]:
        """Fetch lis pendens filings from AVA Search.

        Args:
            start_date: Explicit start date. If None, uses (today - days_back).
            end_date: Explicit end date. If None, uses today.
            days_back: Number of days to look back (default 7). Ignored if
                       start_date is provided.

        Returns:
            List of LisPendensRecord objects.
        """
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=days_back)

        start_str = start_date.strftime("%m/%d/%Y")
        end_str = end_date.strftime("%m/%d/%Y")

        logger.info(f"Fetching lis pendens: {start_str} to {end_str}")

        context = await self._browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            records = await self._execute_search(page, start_str, end_str)
            logger.info(f"Found {len(records)} lis pendens records")
            return records
        finally:
            await context.close()

    async def _execute_search(
        self, page: Page, start_str: str, end_str: str
    ) -> list[LisPendensRecord]:
        """Navigate to AVA, fill the search form, and capture results."""

        # Capture the API response
        api_response_data: dict = {}

        async def handle_response(response: Response):
            if SEARCH_API in response.url and response.status == 200:
                try:
                    body = await response.json()
                    api_response_data.update(body)
                except Exception as e:
                    logger.warning(f"Failed to parse search response: {e}")

        page.on("response", handle_response)

        # 1. Navigate to search page and wait for SPA to load
        logger.info("Loading AVA Search page...")
        await page.goto(BASE_URL, timeout=NAVIGATION_TIMEOUT_MS)
        await page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT_MS)

        # Wait for the search form to render
        await page.wait_for_selector(
            'input[placeholder="Last Name / Business Name"]',
            timeout=DEFAULT_TIMEOUT_MS,
        )
        logger.info("Search form loaded")

        # 2. Fill in date range
        start_input = page.locator('input[placeholder="MM/DD/YYYY"]').first
        end_input = page.locator('input[placeholder="MM/DD/YYYY"]').last

        await start_input.fill(start_str)
        await end_input.fill(end_str)

        # 3. Select "LIS PENDENS" document type
        doc_type_input = page.locator('input[placeholder="Document Type"]')
        await doc_type_input.fill("LIS")
        # Wait for dropdown and select
        await page.wait_for_timeout(500)
        lis_option = page.locator("text=LIS PENDENS").first
        try:
            await lis_option.click(timeout=3000)
        except Exception:
            # Fallback: the field may already accept the text directly
            logger.info("Dropdown selection failed, trying direct input")
            await doc_type_input.fill("LIS PENDENS")

        logger.info(f"Search params: {start_str} to {end_str}, type=LIS PENDENS")

        # 4. Click search button
        search_buttons = page.locator('button:has-text("Search")')
        await search_buttons.last.click()

        # 5. Wait for the API response
        logger.info("Waiting for search results...")
        try:
            await page.wait_for_function(
                "() => document.querySelectorAll('table tr, .search-result').length > 1",
                timeout=DEFAULT_TIMEOUT_MS,
            )
        except Exception:
            # Fallback: wait a fixed time for the response handler
            await page.wait_for_timeout(5000)

        # 6. Parse the captured API response
        if api_response_data and "DocResults" in api_response_data:
            return self._parse_api_response(api_response_data)

        # Fallback: scrape the HTML table if API intercept failed
        logger.warning("API intercept missed, falling back to HTML scrape")
        return await self._scrape_html_table(page)

    def _parse_api_response(self, data: dict) -> list[LisPendensRecord]:
        """Parse the JSON response from the Search API."""
        total = data.get("TotalResults", 0)
        viewable = data.get("ViewableResults", 0)
        results = data.get("DocResults", [])

        logger.info(f"API returned {total} total, {viewable} viewable, {len(results)} records")

        if total > viewable:
            logger.warning(
                f"Only {viewable} of {total} results are viewable. "
                "Consider narrowing the date range."
            )

        records = []
        for doc in results:
            record = LisPendensRecord(
                document_number=str(doc.get("DocumentNumber", doc.get("Id", ""))),
                document_type=doc.get("DocumentType", ""),
                recorded_date=self._format_date(doc.get("RecordedDateTime", "")),
                party1=doc.get("Party1", ""),
                party2=doc.get("Party2", ""),
                legals=doc.get("Legals", ""),
            )
            records.append(record)

        return records

    async def _scrape_html_table(self, page: Page) -> list[LisPendensRecord]:
        """Fallback: scrape the results table from the rendered HTML."""
        rows = await page.query_selector_all("table tr")
        records = []

        for row in rows[1:]:  # skip header
            cells = await row.query_selector_all("td")
            if len(cells) >= 6:
                record = LisPendensRecord(
                    document_number=await cells[0].inner_text(),
                    document_type=await cells[1].inner_text(),
                    recorded_date=await cells[2].inner_text(),
                    party1=await cells[3].inner_text(),
                    party2=await cells[4].inner_text(),
                    legals=await cells[5].inner_text(),
                )
                records.append(record)

        return records

    @staticmethod
    def _format_date(raw: str) -> str:
        """Normalize date strings from the API.

        Input can be like '2026-03-23T13:10:00' or '3/23/2026 1:13:10 PM'
        """
        if not raw:
            return ""
        # Try ISO format first
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
            try:
                dt = datetime.strptime(raw.split(".")[0], fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Return as-is if no format matches
        return raw.split("T")[0] if "T" in raw else raw


# ---------------------------------------------------------------------------
# Direct HTTP Client (no browser required)
# ---------------------------------------------------------------------------

class AvaSearchHTTP:
    """Direct HTTP client for the AVA Search API.

    Faster than the Playwright approach — no browser startup overhead.
    Hits the same REST API the SPA uses.

    Flow:
        1. POST to /token with grant_type=client_credentials to get Bearer JWT
        2. POST to /breeze/Search with the Bearer token + search params
        3. Parse JSON response into LisPendensRecord objects

    Usage:
        client = AvaSearchHTTP()
        records = await client.fetch_lis_pendens(days_back=30)
    """

    def __init__(self):
        self._token: Optional[str] = None

    async def _get_token(self) -> str:
        """Obtain a Bearer token from the token endpoint.

        The AVA Search API issues tokens with grant_type=client_credentials
        and no actual credentials — it's a public-access endpoint.
        """
        import urllib.request
        import urllib.parse

        # The token endpoint uses password grant with public guest credentials
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
            self._token = body["access_token"]
            logger.info(f"Obtained Bearer token (expires_in={body.get('expires_in')}s)")
            return self._token

    async def fetch_lis_pendens(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days_back: int = 7,
    ) -> list[LisPendensRecord]:
        """Fetch lis pendens filings via direct HTTP.

        Args:
            start_date: Explicit start date. If None, uses (today - days_back).
            end_date: Explicit end date. If None, uses today.
            days_back: Days to look back (default 7). Ignored if start_date set.

        Returns:
            List of LisPendensRecord objects.
        """
        import urllib.request

        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=days_back)

        if not self._token:
            await self._get_token()

        payload = {
            "FirstName": "",
            "LastBusinessName": "",
            "StartDate": start_date.strftime("%Y-%m-%d"),
            "EndDate": end_date.strftime("%Y-%m-%d"),
            "DocumentName": "",
            "DocumentType": DOC_TYPE_LIS_PENDENS,
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
            "ParcelNumber": "",
            "Book": "",
            "Page": "",
            "ReferenceNumber": "",
            "DisplayStartDate": start_date.strftime("%m/%d/%Y"),
            "DisplayEndDate": end_date.strftime("%m/%d/%Y"),
            "DocumentTypeDisplayName": "LIS PENDENS",
        }

        body = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            SEARCH_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._token}",
            },
        )

        logger.info(
            f"Searching: {start_date:%m/%d/%Y} to {end_date:%m/%d/%Y}, "
            f"type=LIS PENDENS"
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        return self._parse_response(data)

    def _parse_response(self, data: dict) -> list[LisPendensRecord]:
        """Parse API JSON into records."""
        total = data.get("TotalResults", 0)
        viewable = data.get("ViewableResults", 0)
        results = data.get("DocResults", [])

        logger.info(f"API: {total} total, {viewable} viewable, {len(results)} returned")

        if total > viewable:
            logger.warning(
                f"Only {viewable}/{total} viewable — narrow the date range"
            )

        records = []
        for doc in results:
            recorded = doc.get("RecordedDateTime", "")
            # Normalize date
            date_str = ""
            if recorded:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
                    try:
                        date_str = datetime.strptime(
                            recorded.split(".")[0], fmt
                        ).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue

            records.append(LisPendensRecord(
                document_number=str(doc.get("DocumentNumber", doc.get("Id", ""))),
                document_type=doc.get("DocumentType", ""),
                recorded_date=date_str or recorded,
                party1=doc.get("Party1", ""),
                party2=doc.get("Party2", ""),
                legals=doc.get("Legals", ""),
            ))

        return records


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

def export_to_csv(records: list[LisPendensRecord], output_path: str | Path) -> Path:
    """Write records to a CSV file."""
    output_path = Path(output_path)

    if not records:
        logger.warning("No records to export")
        return output_path

    fieldnames = list(asdict(records[0]).keys())

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_dict())

    logger.info(f"Exported {len(records)} records to {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description="Scrape St. Clair County AVA Search for Lis Pendens filings"
    )
    parser.add_argument(
        "--days", type=int, default=7,
        help="Number of days to look back (default: 7)"
    )
    parser.add_argument(
        "--start-date", type=str, default=None,
        help="Start date (MM/DD/YYYY). Overrides --days."
    )
    parser.add_argument(
        "--end-date", type=str, default=None,
        help="End date (MM/DD/YYYY). Default: today."
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Output CSV path. Default: lis_pendens_YYYY-MM-DD.csv"
    )
    parser.add_argument(
        "--mode", choices=["http", "browser"], default="http",
        help="Scraping mode: 'http' (fast, direct API) or 'browser' (Playwright). Default: http"
    )
    parser.add_argument(
        "--headed", action="store_true",
        help="Run browser in headed mode (only applies to --mode browser)"
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Print results as JSON to stdout"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_date = None
    end_date = None

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%m/%d/%Y")
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%m/%d/%Y")

    # Choose scraping mode
    if args.mode == "http":
        logger.info("Using direct HTTP mode (no browser)")
        client = AvaSearchHTTP()
        records = await client.fetch_lis_pendens(
            start_date=start_date,
            end_date=end_date,
            days_back=args.days,
        )
    else:
        if not HAS_PLAYWRIGHT:
            print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
            sys.exit(1)
        logger.info("Using Playwright browser mode")
        async with AvaSearchScraper(headless=not args.headed) as scraper:
            records = await scraper.fetch_lis_pendens(
                start_date=start_date,
                end_date=end_date,
                days_back=args.days,
            )

    if not records:
        print("No lis pendens records found for the given date range.")
        sys.exit(0)

    # Summary
    fc_count = sum(1 for r in records if r.is_foreclosure)
    print(f"\n{'='*60}")
    print(f"  Lis Pendens Results: {len(records)} total, {fc_count} foreclosures")
    print(f"{'='*60}\n")

    for r in records:
        tag = "[FC]" if r.is_foreclosure else "[--]"
        print(f"  {tag} {r.document_number}  {r.recorded_date}  "
              f"{r.case_number or r.party1:<16}  {r.party2}")

    # JSON output
    if args.json:
        print(json.dumps([r.to_dict() for r in records], indent=2))

    # CSV export
    if args.output or not args.json:
        output_path = args.output or f"lis_pendens_{datetime.now():%Y-%m-%d}.csv"
        export_to_csv(records, output_path)
        print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
