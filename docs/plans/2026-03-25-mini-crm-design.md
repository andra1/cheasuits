# Mini CRM — Pipeline Tracking Tab

**Date:** 2026-03-25
**Status:** Approved

## Overview

Add a "Pipeline" tab to the dashboard that serves as a lightweight CRM for tracking which properties have been reached out to, qualified, or passed on. Properties are explicitly added from the Table tab into the CRM pipeline, where users manage deal stages and notes.

## Deal Stages

1. **New Lead** — just surfaced, unreviewed
2. **Reviewing** — evaluating the opportunity
3. **Contacted** — outreach sent (mail, call, etc.)
4. **Responded** — owner replied
5. **Negotiating** — active deal discussion
6. **Under Contract** — signed purchase agreement
7. **Closed** — acquired
8. **Passed** — intentionally skipped / not pursuing

Stage changes overwrite the current stage (no history tracking).

## Data Layer

### New table: `property_crm` in `cheasuits.db`

| Column | Type | Purpose |
|--------|------|---------|
| `id` | INTEGER PK | Auto-increment |
| `document_number` | TEXT UNIQUE | FK to `properties` — natural key |
| `stage` | TEXT NOT NULL | One of the 8 stages above |
| `notes` | TEXT | Free-text notes |
| `added_at` | TIMESTAMP | When property was added to CRM |
| `updated_at` | TIMESTAMP | Last stage or notes change |

### Database helpers in `database.py`

- `add_to_crm(document_number, stage='new_lead')`
- `update_crm_stage(document_number, stage)`
- `update_crm_notes(document_number, notes)`
- `remove_from_crm(document_number)`
- `get_crm_entries()` — returns all tracked properties

### Export

`prepare_data.py` joins `property_crm` onto features, adding `crm_stage`, `crm_notes`, `crm_added_at`, `crm_updated_at` fields (null for untracked properties).

## API Layer

### FastAPI server (`src/api/server.py`)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `POST` | `/api/crm/track` | Add property to CRM (`{document_number}`) |
| `PATCH` | `/api/crm/{document_number}/stage` | Update stage (`{stage}`) |
| `PATCH` | `/api/crm/{document_number}/notes` | Update notes (`{notes}`) |
| `DELETE` | `/api/crm/{document_number}` | Remove from CRM |
| `GET` | `/api/crm` | List all CRM entries (joined with property data) |

CORS middleware allows requests from `localhost:3000` (dev) and the Vercel domain.

### Data loading strategy

- Map and Table tabs continue loading from `data.json` (static, unchanged).
- Pipeline tab fetches from `GET /api/crm` on mount.

## Dashboard UI

### New "Pipeline" tab

Added as the third tab alongside Map and Table.

### Filter pills

- Row of pills at top, one per stage plus "All" (default).
- Click to toggle visibility. Active pills are highlighted.
- Each pill shows count (e.g., "Contacted (3)").

### Table columns

| Stage | Score | Owner | Address | Est. Value | Notes | Added | Actions |

- **Stage** — inline dropdown, change triggers PATCH to API.
- **Notes** — truncated to ~50 chars, click to expand/edit.
- **Added** — relative time ("3 days ago").
- **Actions** — "Remove" button with confirmation.

### Sorting

Same sortable headers as existing Table tab. Stage sorts by pipeline order (new_lead first, passed last), not alphabetically.

### Expandable rows

Reuses existing Table tab detail panel component. Adds notes textarea and stage dropdown at the top of the expanded section.

### Track button in Table tab

- Small "+" icon button at the end of each row for untracked properties.
- Already-tracked properties show a "Tracked" badge instead.
- Clicking the badge navigates to the Pipeline tab.

### Empty state

When no properties are tracked, show a message pointing users to the Table tab to start tracking.

## Scope Exclusions (YAGNI)

- No stage change history / audit log
- No outreach log (call/mail/email records)
- No follow-up reminders or due dates
- No team assignment
- No tags/labels
- No offer tracking

These can be added later if needed.
