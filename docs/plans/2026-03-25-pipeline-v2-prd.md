# PRD: Distressed Property Pipeline v2

**Date**: 2026-03-25
**Scope**: St. Clair County, IL
**Status**: Draft

---

## Problem Statement

The current pipeline ingests from 3 sources (lis pendens, delinquent taxes, USPS vacancy) and displays results in a dashboard with rudimentary client-side scoring. Two critical problems:

1. **Narrow sourcing** — Lis pendens and delinquent taxes only capture properties already in financial distress proceedings. This misses earlier-stage motivated sellers (probate, absentee neglect, code violations) where competition is lower and negotiation leverage is higher.

2. **No real qualification** — A solo operator processing 10-20 leads/week cannot afford false positives. The current point system (40 pts tax sold, 25 delinquent, etc.) doesn't account for equity position, neighborhood viability, exit strategy fit, or owner reachability. Leads that score high on distress signals but sit in declining neighborhoods with no equity are time wasters.

## Context

- **Market**: St. Clair County, IL only (prove the system before expanding)
- **Strategy**: Buy-and-hold rentals + fix-and-flip, property-dependent
- **Outreach**: Not operationalized yet — no contact step exists in the pipeline
- **Budget**: Free/public data sources only, no paid APIs
- **Capacity**: Solo operator, ~10-20 leads/week max

## Success Criteria

- Pipeline surfaces a **weekly shortlist of ≤20 ranked leads** with enough context to decide go/no-go without manual research
- Each lead is tagged with a **recommended exit strategy** (rental vs. flip) based on neighborhood and property characteristics
- At least **5 independent distress/motivation signals** feed into scoring (up from 2)
- Zero paid API dependencies

---

## Feature 1: Expanded Lead Sourcing (St. Clair County, Free/Public)

**Goal**: Widen the funnel from 3 sources to 7+ using only free St. Clair County public records.

### 1A. Probate Case Filings

- **Source**: St. Clair County Circuit Court public case search (IL 20th Judicial Circuit)
- **Signal**: Inherited property = motivated seller who often lives out-of-state and wants quick liquidation
- **Implementation**: Scrape probate filings, extract decedent name + case number, cross-reference St. Clair County assessor (DevNetWedge) to find real property in the estate
- **Priority**: High — low competition, high motivation, and the court portal is already a known scraping target

### 1B. Municipal Code Violations

- **Source**: FOIA requests to Belleville, East St. Louis, Cahokia Heights, Fairview Heights, O'Fallon code enforcement
- **Signal**: Repeated violations = owner who can't or won't maintain. Strongest flip indicator when combined with absentee ownership
- **Implementation**: Quarterly FOIA batch requests (IL 5-day response mandate), parse CSV/Excel responses, ingest into `code_violations` table, join on address
- **Priority**: Medium — great signal quality but manual FOIA cadence creates lag

### 1C. Expired & Withdrawn Listings

- **Source**: Redfin/Realtor.com off-market filters scoped to St. Clair County zip codes (62201-62298)
- **Signal**: Failed to sell = softened price expectations, demonstrated intent to sell
- **Implementation**: Scrape recently expired/withdrawn listings in target zips, flag overlaps with existing distress tables
- **Priority**: High — warmest free leads, and Redfin scraping is already partially built in the comps pipeline

### 1D. Tax Sale Auction Results

- **Source**: St. Clair County Treasurer's annual tax sale publication (same source as the delinquent tax scraper)
- **Signal**: Sold at tax sale + approaching redemption deadline (IL gives 2-2.5 years) = maximum financial pressure
- **Implementation**: Extend `delinquent_tax.py` to also parse sale results and track redemption timelines. Add `tax_sale_date` and `redemption_deadline` columns
- **Priority**: High — natural extension of what's already scraped, and redemption deadlines create urgency

### 1E. Property-Level Vacancy Confirmation

- **Source**: FOIA to Illinois American Water (primary St. Clair utility) for disconnected/inactive accounts
- **Signal**: Confirmed vacancy at property level (upgrades tract-level USPS data to actionable)
- **Implementation**: Quarterly FOIA request for addresses with inactive water service, match against property database, add `vacancy_confirmed` flag
- **Priority**: Medium — powerful signal but dependent on FOIA response quality

---

## Feature 2: Backend Scoring & Qualification Engine

**Goal**: Replace client-side point scoring with a backend engine that synthesizes all signals into a composite score, ranks leads by actionability, and recommends an exit strategy — filtering down to ≤20 leads/week.

### 2A. Distress Signal Score (0-100)

A weighted composite of all available distress/motivation indicators per property:

| Signal | Weight | Source | Logic |
|--------|--------|--------|-------|
| Lis pendens filing | 25 | `properties` table | Active foreclosure = confirmed financial distress |
| Tax delinquency | 20 | `delinquent_taxes` | Delinquent > 1 year scores higher than recent |
| Tax sale + approaching redemption | 20 | Feature 1D | Score scales up as redemption deadline nears |
| Probate/estate case | 15 | Feature 1A | Inherited property, especially if heir is out-of-state |
| Code violations | 10 | Feature 1B | Count-weighted; 3+ violations scores max |
| Vacancy confirmed | 5 | Feature 1E / USPS | Property-level confirmation > tract-level signal |
| Expired listing | 5 | Feature 1C | Days since expiration increases score |

**Stacking bonus**: Properties appearing in 3+ signal sources get a 15-point bonus. A property that's delinquent on taxes, has code violations, AND is vacant is qualitatively different from one that's just delinquent.

### 2B. Deal Viability Score (0-100)

Distress alone isn't enough — a distressed property with no equity or in a collapsing neighborhood is a trap.

| Factor | Weight | Source | Logic |
|--------|--------|--------|-------|
| Equity spread | 30 | Valuation vs. estimated liens/owed | Higher estimated equity = more room to negotiate a discount. Properties with negative equity score 0 |
| Comp confidence | 20 | `comps.py` output | High confidence (3+ recent comps) scores full; low confidence penalized — if you can't verify value, it's risky |
| Neighborhood vacancy rate | 15 | `usps_vacancy` by census tract | Low vacancy = stable demand. Above 15% vacancy penalized heavily |
| Assessed value range | 15 | Assessor data | Filter to buy box — e.g., $30K-$150K assessed. Outside range scores 0 |
| Days on market / time pressure | 10 | Filing date, auction date, listing expiry | More urgency = better negotiation position |
| Owner reachability | 10 | Assessor mailing address | Local mailing address scores higher than out-of-state (easier to contact without skip trace) — but absentee owners score higher on *distress* |

### 2C. Composite Ranking & Exit Strategy Tagging

#### Final Lead Score

**Lead Score = (Distress Signal × 0.5) + (Deal Viability × 0.5)**

Hard floor: if either score is below 20, the property is filtered out entirely. A maximally distressed property with no deal viability is a time sink. A great deal with no motivation signal means the owner won't sell at a discount.

The weekly pipeline run scores all properties, sorts by Lead Score, and surfaces the **top 20** as the work queue.

#### Exit Strategy Tagging

Each lead in the top 20 gets tagged as **Rental**, **Flip**, or **Either**:

| Indicator | Points toward Rental | Points toward Flip |
|-----------|--------------------|--------------------|
| Assessed value <$60K | +3 | |
| Assessed value $60K-$150K | | +3 |
| Census tract vacancy <8% | +2 (stable tenant demand) | |
| Census tract vacancy 8-15% | | +2 (discount zone, sell after rehab) |
| Code violations present | | +3 (rehab scope partially known) |
| Absentee owner | +2 (likely already a rental) | |
| Probate/estate | | +2 (family wants clean exit, often sell as-is) |
| Comp confidence high | | +2 (ARV reliable enough to underwrite a flip) |

- **Rental** = total rental points > flip points by 2+
- **Flip** = total flip points > rental points by 2+
- **Either** = scores within 1 point of each other

#### Output Format

The weekly shortlist includes for each lead:

- Address, parcel ID, owner name + mailing address
- Lead Score (composite), Distress Score, Viability Score
- Exit tag (Rental / Flip / Either)
- Key signals summary (e.g., "Tax delinquent 2yr + vacant + 3 code violations")
- Estimated market value, comp count, confidence level
- Recommended next action (drives Feature 3)

---

## Feature 3: Outreach Foundation

**Goal**: Bridge the gap between "scored lead" and "first contact." Designed for a solo operator on free tools — not automation, but friction reduction.

### 3A. Weekly Lead Packet Generation

Each week after scoring runs, the system generates a **lead packet** — a single exportable document (PDF or CSV) for the top 20 leads, pre-organized by exit strategy.

Each entry includes:

- Owner name + mailing address (from assessor data)
- Property address + parcel ID
- Lead score breakdown (distress / viability / composite)
- Exit tag + reasoning summary
- Estimated market value + comp range
- All distress signals in plain English (e.g., "Filed lis pendens 2026-01-15, tax delinquent since 2024, water disconnected")

**Why a packet and not just the dashboard**: A printable artifact you can take to the courthouse, hand to a mail house, or work from at your desk. The dashboard is for exploration; the packet is for execution.

### 3B. Direct Mail List Export

For leads where the owner has a valid mailing address, generate a **mail-merge-ready CSV** with:

- Owner name, mailing address (street, city, state, zip)
- Property address
- A motivation-specific tag (foreclosure, tax sale, probate, code violation)

This lets you send the list to a print-and-mail service (e.g., Yellow Letter HQ, Ballpoint Marketing — both accept CSV uploads, pay per piece, no subscription). The tag field lets you customize letter templates per motivation type — a probate letter reads very differently from a foreclosure letter.

### 3C. Contact Research Queue

For the highest-scored leads (top 5 per week), the system flags them for **manual skip trace** — surfacing what's already known and explicitly marking what's missing.

No paid skip trace API. Instead, the system generates a checklist per lead:

- Owner name → search TruePeopleSearch, FastPeopleSearch (both free)
- Property address → check county GIS for additional owner records
- Mailing address → verify via USPS address validation (free API)

This turns "I should contact this person" into a concrete 5-minute research task per lead.

---

## Feature 4: Pipeline Orchestration & Scheduling

**Goal**: Turn the current "run each script manually" workflow into a single automated weekly cycle that ingests, enriches, scores, and delivers the lead packet without intervention.

### 4A. Pipeline Orchestrator

A single entry point (`src/scheduler/pipeline.py`) that runs the full cycle in dependency order:

```
Stage 1: INGEST (parallel where possible)
  ├── ava_search.py        → lis pendens filings
  ├── delinquent_tax.py    → tax delinquency
  ├── probate_scraper.py   → probate cases (new)
  ├── expired_listings.py  → expired/withdrawn MLS (new)
  └── tax_sale_results.py  → tax sale + redemption tracking (new)

Stage 2: ENRICH (sequential, depends on Stage 1)
  ├── assessor.py          → owner info, assessed values
  ├── census_tract.py      → FIPS codes for vacancy join
  ├── valuation.py         → market value estimates
  ├── comps_recorder.py    → historical sales discovery
  └── comps.py             → comp-based valuation

Stage 3: SCORE (depends on Stage 2)
  └── scoring_engine.py    → distress + viability + exit tag

Stage 4: DELIVER (depends on Stage 3)
  ├── lead_packet.py       → weekly PDF/CSV packet
  ├── mail_export.py       → mail-merge CSV
  └── prepare_data.py      → dashboard data.json refresh
```

Each stage logs: start time, record count processed, errors, and duration. If a stage fails, subsequent stages still run with stale data rather than blocking the entire pipeline.

### 4B. Scheduling

| Job | Frequency | Rationale |
|-----|-----------|-----------|
| Full pipeline (Stages 1-4) | Weekly, Sunday night | Fresh lead packet ready Monday morning |
| Lis pendens ingestion only | Daily | Foreclosure filings happen daily; catch them fast |
| Dashboard data refresh | Daily (after lis pendens) | Keep the map/table current for ad-hoc browsing |
| FOIA reminder | Quarterly | Notifies you to submit code violation + utility FOIA requests |

### 4C. Run Log & Health Dashboard

A `pipeline_runs` table tracking each execution:

| Column | Purpose |
|--------|---------|
| `run_id` | UUID per pipeline execution |
| `stage` | Which stage ran |
| `started_at` / `completed_at` | Timing |
| `records_processed` | Count |
| `errors` | Error messages if any |
| `status` | success / partial / failed |

Exposed as a simple "Pipeline Status" section in the dashboard.

---

## Feature 5: Data Model Changes

**Goal**: Extend the existing SQLite schema to support new sources, scoring, and pipeline operations with minimal disruption to what's already working.

### 5A. New Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `probate_cases` | Probate filings from circuit court | `case_number` (PK), `decedent_name`, `filing_date`, `case_status`, `parcel_id` (nullable, populated during enrichment) |
| `code_violations` | Municipal code enforcement records | `violation_id` (PK), `address`, `municipality`, `violation_type`, `violation_date`, `parcel_id`, `foia_batch_date` |
| `expired_listings` | Withdrawn/expired MLS listings | `listing_id` (PK), `address`, `list_price`, `listed_date`, `expired_date`, `days_on_market`, `parcel_id` |
| `tax_sale_results` | Tax sale outcomes + redemption tracking | `parcel_id` (PK), `sale_date`, `sale_amount`, `buyer`, `redemption_deadline`, `redeemed` (boolean) |
| `lead_scores` | Backend scoring output | `parcel_id` (PK), `scored_at`, `distress_score`, `viability_score`, `composite_score`, `exit_tag`, `signal_summary`, `rank` |
| `pipeline_runs` | Orchestrator health log | `run_id` (PK), `stage`, `started_at`, `completed_at`, `records_processed`, `errors`, `status` |

### 5B. Alterations to Existing Tables

**`properties`** — add columns:

- `vacancy_confirmed` (boolean) — from utility FOIA cross-reference
- `probate_case_number` (text) — links to `probate_cases`
- `code_violation_count` (integer) — denormalized count for fast scoring
- `has_expired_listing` (boolean) — flag for scoring
- `signal_count` (integer) — how many distinct distress sources reference this property

**`delinquent_taxes`** — add columns:

- `tax_sale_date` (date) — if sold at tax sale
- `redemption_deadline` (date) — calculated from sale date + IL statutory period
- `redeemed` (boolean)

### 5C. Unified Property View

The scoring engine needs all signals for a given property in one place. Rather than joining 7 tables every scoring run, build a denormalized `property_unified` table rebuilt each pipeline run:

- One row per parcel ID
- Columns from: properties, delinquent_taxes, probate_cases, code_violations, expired_listings, tax_sale_results, usps_vacancy, comparable_sales
- This is what the scoring engine reads from and what the lead packet generator queries

SQLite doesn't have materialized views. A rebuilt staging table each pipeline run is simpler and fast enough at St. Clair County scale (~10K-20K properties).

---

## Implementation Roadmap

### Phase 1: Scoring Engine + Orchestrator (Highest Impact)

**Rationale**: The pipeline already has 3 data sources feeding enriched data. The biggest bang is synthesizing what exists, not adding more raw inputs.

**Deliverables**:

- `property_unified` table builder (joins existing data)
- `scoring_engine.py` — distress score, viability score, composite rank, exit tag
- `lead_packet.py` — weekly top-20 PDF/CSV export
- `mail_export.py` — mail-merge-ready CSV
- Basic `pipeline.py` orchestrator wiring Stages 1-4 for existing sources
- `pipeline_runs` table + logging
- `lead_scores` table

**Why first**: Go from "browsing a dashboard" to "working a prioritized list Monday morning" with zero new scrapers. Immediate workflow improvement.

### Phase 2: Tax Sale Redemption Tracking

**Rationale**: Natural extension of the existing delinquent tax scraper — minimal new code, high-value signal.

**Deliverables**:

- Extend `delinquent_tax.py` to parse sale results
- `tax_sale_results` table + redemption deadline calculation
- Wire into scoring engine (redemption urgency weighting)

**Why second**: Cheapest source to add. Same portal already scraped. Redemption deadlines create time-pressure that makes outreach more effective.

### Phase 3: Probate Case Scraper

**Rationale**: Opens an entirely new lead category (inherited property) that doesn't overlap with financial distress sources.

**Deliverables**:

- `probate_scraper.py` targeting St. Clair Circuit Court
- `probate_cases` table
- Cross-reference logic to link decedent → property via assessor
- Wire into scoring engine + unified view

**Why third**: Probate leads are the best free source for finding motivated sellers before the competition. Requires a new scraper but the court portal is public.

### Phase 4: Expired Listings Scraper

**Rationale**: Warmest leads in the funnel — these sellers already tried and failed to sell.

**Deliverables**:

- `expired_listings.py` scraping Redfin for St. Clair County off-market properties
- `expired_listings` table
- Overlap detection with existing distress tables (expired + delinquent = high priority)
- Wire into scoring engine

**Why fourth**: Builds on existing Redfin scraping code in `comps_redfin.py`. Overlap detection with distress data is where the real alpha is.

### Phase 5: Code Violations + Vacancy Confirmation (FOIA-dependent)

**Rationale**: High-quality signals but depend on manual FOIA submissions with quarterly cadence.

**Deliverables**:

- FOIA request templates for St. Clair County municipalities + Illinois American Water
- Ingestion parsers for FOIA response formats (CSV/Excel)
- `code_violations` table
- `vacancy_confirmed` flag on properties
- Wire into scoring engine
- Quarterly FOIA reminder in scheduler

**Why last for automation**: FOIA introduces a human-in-the-loop delay. Better to layer in once everything else runs hands-off.

### Phase 6: Scheduling & Automation

**Rationale**: Don't automate a pipeline that's still changing shape.

**Deliverables**:

- Cron schedule (daily lis pendens, weekly full run)
- `pipeline_runs` health monitoring
- Dashboard pipeline status section
- Error alerting (email or Slack webhook — both free tier)

**Why last**: Run the pipeline manually a few times to calibrate scoring weights and catch edge cases before putting it on autopilot.
