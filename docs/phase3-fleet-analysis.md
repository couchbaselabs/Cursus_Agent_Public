# Phase 3 — Fleet Analysis

**Goal:** Shift from single-customer interrogation to fleet-wide intelligence across all customers simultaneously.

The single-customer model (Phase 2) answers "how is Amex doing?" Phase 3 answers "how is my entire portfolio doing, which accounts are at risk, and what do I act on first?"

---

## Milestone 3.1 — Fleet Query Foundation

Enable cross-org N1QL queries and agent tools that operate without an org scope, aggregating data fleet-wide.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 3.1.1 | `query_fleet_tickets` agent tool | N1QL across all orgs with `GROUP BY organization`; supports grouping by priority, status, CB version, CBSE |
| 3.1.2 | `list_at_risk_clusters` agent tool | Snapshots where `bad_items > 0` OR `warn_items > N` with no linked open ticket — pure leading indicator |
| 3.1.3 | `fleet_version_distribution` agent tool | `SELECT cb_version, COUNT(*) FROM snapshots GROUP BY cb_version ORDER BY COUNT DESC` across entire fleet |
| 3.1.4 | `fleet_cbse_impact` agent tool | Which CBSEs appear across the most unique orgs; sorted by blast radius (org count descending) |
| 3.1.5 | Extend `get_portfolio_status` | Add cluster `bad_ratio` dimension alongside existing ticket health score per org |
| 3.1.6 | Cross-org scoping guard | Agent system prompt updated: cross-org queries only allowed for fleet/portfolio tools, not ticket detail tools |

### Acceptance criteria
- `"What are the top 3 riskiest customers right now?"` triggers `get_portfolio_status` or `list_at_risk_clusters` and returns a ranked list with reasons.
- `"Which CB version is most common across all my clusters?"` returns version distribution without needing an org filter.
- `"Which CBSE is hitting the most customers?"` returns a ranked list with org counts.

---

## Milestone 3.2 — Fleet Dashboard Tab

A new top-level tab providing a visual, auto-loading overview of the entire fleet. No typing required — opens and loads.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 3.2.1 | New `Fleet` top-level tab (between Customers and Assets) | Icon: `public` or `hub` |
| 3.2.2 | CB version distribution chart | Donut or treemap; one segment per major.minor version across all snapshots |
| 3.2.3 | Open ticket count by org chart | Horizontal bar, top 15 orgs; color-coded by highest open priority |
| 3.2.4 | Priority breakdown fleet-wide chart | Stacked bar: critical / high / normal / low across all open tickets |
| 3.2.5 | Cluster bad-item heatmap | Table or scatter: org vs. cluster count vs. bad_items severity |
| 3.2.6 | 30-day ticket volume trend chart | Area chart, all orgs combined; shows fleet-wide activity trends |
| 3.2.7 | Click-through to customer | Clicking any chart element (bar, segment, row) loads that org in the Scoring tab |
| 3.2.8 | "Refresh fleet data" button + last-updated timestamp | Debounced to avoid hammering CB; timestamp shown in the header row |
| 3.2.9 | Fleet summary stats row | Total open tickets · Total orgs · Orgs with P1 · Clusters with bad items — displayed as KPI chips at the top |

### Acceptance criteria
- Fleet tab loads and renders all charts within 5 seconds on a local CB instance.
- Clicking an org in the "open tickets by org" bar navigates to that org's Scoring tab view.
- KPI chips update on refresh.

---

## Milestone 3.3 — Leading Indicators

Detect problems before tickets are opened. Surface at-risk clusters early so support can be proactive.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 3.3.1 | `detect_leading_indicators` agent tool | Returns clusters with elevated bad/warn counts + no active ticket, sorted by risk score |
| 3.3.2 | Risk score formula | `risk = (bad_items × 3 + warn_items) × recency_factor` where `recency_factor = 1 + (hours_since_scraped / 48)` |
| 3.3.3 | `fleet_anomaly_scan` agent tool | Compares current snapshot metrics to 7-day rolling baseline per cluster; flags deviations > 2σ |
| 3.3.4 | Alerts panel in Fleet Dashboard | "⚠ 3 clusters show elevated bad items with no open ticket" — links to `detect_leading_indicators` output |
| 3.3.5 | `get_cluster_risk_report(cluster_name)` agent tool | Full health history for a specific cluster across all linked tickets and snapshots |
| 3.3.6 | Risk score stored on snapshot documents | Computed at scrape time via `_compute_cluster_risk`; queryable via N1QL for fast fleet-wide ranking |

### Acceptance criteria
- `"Which clusters are most likely to generate a ticket in the next 48 hours?"` returns a ranked list with risk scores and explanations.
- Fleet Dashboard alerts panel shows a non-zero count when at-risk clusters exist.
- `get_cluster_risk_report` returns a coherent timeline linking snapshot metric changes to ticket open dates.

---

## Milestone 3.4 — Portfolio Management

Named customer groups with persistent tracking, so users can scope fleet views to their accounts.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 3.4.1 | Portfolio document schema | `saved_portfolio::{name}` in CB: `{name, orgs: [], created_at, updated_at}` |
| 3.4.2 | `create_portfolio(name, orgs)` agent tool | Upserts portfolio document; orgs is a list of org name fragments |
| 3.4.3 | `list_portfolios()` agent tool | Returns all saved portfolios with org count and last-updated timestamp |
| 3.4.4 | `get_portfolio_health(portfolio_name)` agent tool | Aggregates health score, SLA compliance, and open P1 count across all member orgs |
| 3.4.5 | Portfolio picker in Fleet Dashboard | Dropdown filters all fleet charts to portfolio members only |
| 3.4.6 | "Status of all my accounts" shortcut | Chip in Chat tab that fires `get_portfolio_health` for the default/last-used portfolio |
| 3.4.7 | Portfolio edit UI in Customers tab | Add/remove orgs from a portfolio via the directory table (checkboxes + "Add to portfolio" button) |

### Acceptance criteria
- `"Create a portfolio called Tier-1 with Amex, Goldman, and JPMorgan"` creates the document and confirms.
- `"How is my Tier-1 portfolio doing?"` returns an aggregated health summary across all three orgs.
- Fleet Dashboard filters all 5 charts to portfolio members when a portfolio is selected.

---

## Dependencies & Tech Notes

- All fleet queries use primary indexes or the existing `supportal_vector_idx` FTS index — no new indexes required for 3.1.
- Risk score computation (3.3.2) should run at snapshot upsert time, not query time, to keep the fleet dashboard fast.
- Portfolio documents live in `{scope}.tickets` (type=`saved_portfolio`) to avoid needing a new collection.
- Fleet Dashboard auto-refresh should be opt-in (button), not automatic — CB queries at fleet scale can be slow.
