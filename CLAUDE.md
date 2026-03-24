# Distressed Real Estate Acquisition Automation

## Project Overview

This system automates the identification, analysis, and acquisition pipeline for distressed real estate properties. It targets investors who need to move quickly on undervalued assets by systematically sourcing leads from both public and private data streams, scoring them, and surfacing actionable opportunities.

The core problem: distressed property opportunities are time-sensitive and scattered across dozens of fragmented sources. Manual monitoring is too slow and inconsistent. This system creates a unified, automated pipeline from discovery to deal evaluation.

---

## Architecture (Planned)

```
Data Ingestion Layer
    ├── Public Sources (scrapers, APIs, bulk downloads)
    └── Private Sources (list providers, network feeds, MLS)
            ↓
Normalization & Deduplication
            ↓
Scoring & Ranking Engine
            ↓
Enrichment (owner info, liens, comps, demographics)
            ↓
Alert / Output Layer (dashboard, email, CRM push)
```

---

## Data Sources

### Public Sources
- **County tax records** — delinquent tax lists, assessed values, ownership history
- **Court records** — foreclosure filings (lis pendens), probate cases, bankruptcy filings
- **MLS / Zillow / Redfin** — days on market, price reduction history, expired listings
- **FEMA / flood maps** — risk overlays
- **Census / ACS** — neighborhood trajectory indicators
- **USPS vacancy data** — address vacancy flags
- **HUD datasets** — distressed asset pools (NPL sales, REO inventory)

### Private / Semi-Private Sources
- Driving for Dollars apps (DealMachine, PropStream exports)
- Skip-traced absentee owner lists
- Wholesale/investor network deal feeds (JV deals, pocket listings)
- List brokers (absentee owners, pre-foreclosure, high-equity, tired landlords)
- Direct mail response leads

---

## Property Scoring Criteria (Draft)

Each candidate property gets a composite distress/opportunity score based on:

- **Financial distress signals** — tax delinquency, liens, notice of default
- **Owner motivation signals** — absentee owner, out-of-state owner, inherited property, divorce/probate
- **Market discount** — estimated ARV vs. asking/assessed price spread
- **Equity position** — high-equity properties preferred (easier to negotiate)
- **Holding cost risk** — vacancy duration, condition flags
- **Neighborhood trajectory** — appreciation trend, crime trend, development pipeline

---

## Key Workflows

### 1. Lead Ingestion
Scheduled scrapers and API pulls populate a raw leads table. Each record captures source, timestamp, property address, and raw metadata.

### 2. Normalization
Addresses are standardized (USPS normalization), deduped by parcel ID or APN, and linked to a canonical property record.

### 3. Enrichment
Each normalized property is enriched with: owner contact info (skip trace), lien/encumbrance data, estimated ARV (comp pull), tax status, and days since last sale.

### 4. Scoring & Filtering
The scoring engine applies weighted criteria to rank leads. Configurable thresholds filter out low-probability opportunities before surfacing them.

### 5. Outreach / Action
High-scoring leads trigger downstream actions: adding to a CRM, generating a direct mail sequence, or alerting an acquisition manager for manual follow-up.

---

## Tech Stack (Python-first)

- **Language**: Python 3.11+
- **Scraping**: `playwright`, `httpx`, `BeautifulSoup`
- **Data**: `pandas`, `polars` for processing; `PostgreSQL` for persistence
- **Scheduling**: `APScheduler` or `Celery` + `Redis`
- **Geocoding / Address normalization**: `usaddress`, `geopy`, Google Maps API
- **Comps / AVM**: PropStream API, Zillow Zestimate API, or ATTOM Data
- **Skip tracing**: BatchSkipTracing or IDI/TLO API
- **Orchestration**: `Prefect` or `Airflow` (TBD)
- **Notifications**: SendGrid (email), Twilio (SMS), Slack webhooks
- **Config**: `pydantic-settings`, `.env` files

---

## Project Structure (Planned)

```
distressed-re-automation/
├── CLAUDE.md
├── README.md
├── pyproject.toml
├── .env.example
├── src/
│   ├── ingestion/          # Source-specific scrapers and API clients
│   ├── normalization/      # Address parsing, dedup, canonical records
│   ├── enrichment/         # Skip trace, lien lookup, comp pulls
│   ├── scoring/            # Distress/opportunity scoring engine
│   ├── outreach/           # CRM push, mail sequence triggers, alerts
│   ├── db/                 # Models, migrations (SQLAlchemy + Alembic)
│   └── scheduler/          # Cron jobs and pipeline orchestration
├── tests/
├── notebooks/              # Exploratory analysis, scoring calibration
└── data/
    ├── raw/                # Unprocessed source dumps
    └── processed/          # Cleaned, normalized records
```

---

## Open Questions / Still Being Defined

- Which county markets to target first (pilot scope)
- Scraping vs. paid API for court/tax records (cost vs. reliability)
- AVM methodology — use third-party or build internal comp engine
- CRM integration target (Podio, HubSpot, custom)
- Legal/compliance review for skip tracing and contact data usage
- Outreach channel mix (direct mail, cold call, SMS, email)
- Whether to build a UI or keep it headless with alerts only

---

## Development Guidelines

- All scraper modules must implement a common `BaseScraper` interface
- Each source should be independently runnable and testable in isolation
- Store raw source data before transformation (idempotent pipeline)
- Never hardcode credentials — use `.env` + `pydantic-settings`
- Write unit tests for scoring logic; integration tests for enrichment APIs
- Log all pipeline runs with source, record count, and error summary
