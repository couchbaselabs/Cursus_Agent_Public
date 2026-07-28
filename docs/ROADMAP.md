# Cursus Agent — Roadmap

**Current version:** v2.7.60  
**Last updated:** 2026-07-27

---

## Completed

### Foundation & data pipeline
- [x] **Ticket pipeline** — full scrape, change-detection, new-ticket discovery vs. CB diff, auto-persist; deleted Zendesk tickets removed from CB (reconcile step)
- [x] **Snapshot pipeline** — listing, Analytics API stubs, topology backfill, per-ticket enrichment
- [x] **Vector search** — FTS hybrid index: BM25 + 1024-dim dot_product embeddings, RRF ranking
- [x] **Fuzzy customer resolution** — 5-step chain (LIKE → local CB → Supportal FTS → per-word → difflib)
- [x] **Auth removal** — all Supportal endpoints confirmed open (v2.6.2); cookie plumbing retained for forward-compatibility

### Agent & UX
- [x] **LLM agent** — multi-turn tool-calling, 5-round loop, status callbacks, cancel support, error classification
- [x] **Strabo dashboard** — 6 tabs, 12 chart types, 6 palettes, SVG/PNG export, drill-down analytics
- [x] **Corax chat UI** — thread sidebar, session resume, asset storage, file upload, shared history with Strabo
- [x] **Assets system** — auto-save charts/tables/reports to CB; preview, download, delete in both UIs
- [x] **Prompt library** — 28 curated prompts across 7 categories; `{customer}` injection; two-step browser in Corax
- [x] **Cursus supervisor** — watchfiles-based hot-reload; per-app restart routing; 2s debounce

### Fleet & analytics
- [x] **Fleet analytics tools** — `query_fleet_tickets`, `list_at_risk_clusters`, `fleet_version_distribution`, `fleet_cbse_impact`

### MCP server
- [x] **MCP tool server (v2.7.x)** — 40+ tools across tickets, customers, scrape jobs, assets, brand kits, report generation, and observability; stdio (Claude Desktop/Code) and SSE (remote) transports; `alwaysAllow` configured for prompt-free operation
- [x] **Report generation tools** — `generate_health_report`, `generate_ticket_report`, `generate_cluster_health_chart`
- [x] **Observability tools** — `check_data_freshness`, `get_failure_insights`, `record_feedback`, `record_insight`, `record_automation_run`
- [x] **Brand kit tools** — `save_customer_brand`, `get_customer_brand`
- [x] **Supportal analytics** — `query_supportal_analytics` (live Analytics API)

### Observability & governance
- [x] **Failure knowledge base** — `markers` collection with `failurelog::`, `toolfailure::`, `pipelinefailure::`, `freshness::`, `cronrun::` doc types; `classify_error()` stamps aggregatable `error_code`
- [x] **`get_failure_insights`** — one-call governance report over the markers collection
- [x] **LMStudio preflight** — `check_connectivity` verifies configured models are loaded
- [x] **Human feedback capture** — `record_feedback` (MCP + agent tool) into uniform `feedback` collection
- [x] **Insights collection** — `record_insight` with candidate→validated promotion governance

### Cursus Unified shell
- [x] **Cursus Unified** (`apps/unified/app.py`, port 8767) — primary active development surface
- [x] **Overview KPIs** — Open Tickets, Open P1s, At-Risk Clusters, Accounts, Opportunities
- [x] **Customers tab** — role filter chips: Primary / Supporting / Other / Pinned
- [x] **`× All Customers` descope button** — expand view to all accounts in CB
- [x] **Tickets, Data, Reports tabs**
- [x] **Embedded assistant panel** — same 40+ tools as Strabo/Corax
- [x] **`run_unified.py` launcher**

### Salesforce integration
- [x] **`supportal/sfdc_sync.py`** — `sync_all()`, `sync_accounts()`, `sync_opportunities()`, `get_account_sfdc_context()`
- [x] **SE-scoped sync** — only accounts where SE is `Primary_SE` or `Opp_SE_Supporting` on an open opportunity
- [x] **`se_name` derivation** — sourced from open opportunity `Primary_SE__c` field
- [x] **`transcripts.accounts` collection** — `org_name`, `se_name`, `supporting_se_name`, `ae_name`, `csm_name`, `arr`, `contract_end_date`, `account_type`, `active_ps_projects`, `org_aliases[]`
- [x] **`transcripts.opportunities` collection** — stage, ARR, close date, SE fields, products
- [x] **6-hour auto-sync loop** — starts on Cursus Unified startup
- [x] **MCP SFDC tools** — `get_my_sfdc_accounts`, `get_account_intelligence`, `list_sfdc_accounts`, `sync_sfdc_data`, `get_account_opportunities`, `get_sfdc_field_mapping`, `update_sfdc_field_mapping`
- [x] **`get_account_intelligence`** — correlated brief: open tickets + SFDC ARR/products/team + open opportunities

### Docker
- [x] **Docker Compose** — single `docker compose up` starts app + fully-initialised Couchbase + MCP SSE server; ports 8765/8766/8767/8768

---

## In Progress / Next

### Phase 3 — Fleet dashboard improvements
- [ ] **At-risk alerts panel** — clusters with elevated bad/warn and no open ticket, sorted by risk score `(bad * 3 + warn) * recency_factor`; surface in Unified Overview tab
- [ ] **Click-through from fleet charts** — clicking any fleet chart element loads that customer
- [ ] **Refresh controls** — "Refresh fleet data" button + last-updated timestamp in Unified

### Cluster topology in Cursus Unified
- [ ] **Node health tiles** — visual cluster topology: nodes, services, RAM, CPU per node
- [ ] **Cluster detail panel** — CB version, bad/warn items, snapshot history

### Opportunities detail
- [ ] **Opportunities detail in customer panel** — show open opps with stage, ARR, close date inline in the Customers tab detail view

---

## Backlog

### Phase 3 — Portfolio management
- [ ] **Portfolio CRUD** — `create_portfolio`, `list_portfolios`, `get_portfolio_health` agent tools; stored in CB as `saved_portfolio::<name>`
- [ ] **Portfolio health summary** — aggregate score, SLA compliance, open P1 count across member orgs
- [ ] **Portfolio switcher in Unified** — filter fleet views to a selected portfolio

### Phase 4 — PDF report generation
- [ ] Evaluate WeasyPrint (HTML→PDF) vs. Playwright PDF
- [ ] `render_pdf_report(sections)` agent tool — markdown + chart specs → downloadable PDF
- [ ] Couchbase-branded PDF template; ECharts embedded as PNG via headless Chromium
- [ ] "Export as PDF" button on Assets tab for report-type assets

### Phase 4 — Agent-driven data freshness
- [ ] **Followed customers import** — fetch Supportal's per-user "followed customers" list at profile creation; store as `profile.accounts[]`
- [ ] **Freshness thresholds** — configurable per priority: critical = 4h, normal = 24h
- [ ] **`fetch_fresh_data` tool** — agent-driven headless pipeline trigger when data is stale
- [ ] **Stale data warning** — banner on agent responses when the user skips a suggested refresh

### Phase 4 — Session management
- [ ] **Session picker** — list last 10 Corax sessions from CB; click to resume
- [ ] **`resume_session(session_id)`** — loads prior history + injects session summary into system prompt
- [ ] **Auto topic tagging** — extract and persist topic tags via `save_chat_session`

### Phase 4 — Scheduled pipeline
- [ ] **`pipeline_runner.py`** — standalone headless script; reads CB config from env vars
- [ ] **CLI:** `python pipeline_runner.py --org "Acme" --scope tickets,snapshots --embed --score`
- [ ] **Cron-compatible** — exits 0/1; structured JSON log to stdout
- [ ] **`_OP_STATUS` via CB** — persist pipeline progress to a CB doc so UIs can display detached-run state

### Phase 5 — Observability-driven governance (Jarvis loop)
- [ ] **Agent tracing (layer 2)** — OTel GenAI semantic-convention docs in a CB `traces` collection; instrument `call_llm_with_tools`
- [ ] **Watchers** — scheduled evaluators over traces/markers/tickets that emit candidate insights; never self-validating
- [ ] **Validation gate** — human / judge-quorum / precedent promotion of candidate feedback to validated
- [ ] **Orchestrator** — turns validated feedback into proposed `guardrail::` docs with staged rollout
- [ ] **Improvement loop** — validated corrections → few-shot examples + eval regression sets

### Future / exploratory
- [ ] **A2A multi-agent** — data freshness agent + analysis agent coordinating via Agent-to-Agent protocol
- [ ] **`fetch_url` tool** — unified live URL fetcher for `docs.couchbase.com` and Supportal pages; domain whitelist; in-process cache
- [ ] **Docs link registry** — pre-seed CB with known doc entry points; `search_couchbase_docs` agent tool
