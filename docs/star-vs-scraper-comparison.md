# STAR vs. Scraper — Codebase Comparison

> Generated: 2026-05-24
> STAR branch: `main` (clean)
> Scraper branch: `feature/echart-migration` (uncommitted changes included)

---

## Project Overviews

### STAR (support-ticket-analyzer)

A multi-user enterprise web application for analyzing Couchbase support tickets with AI-powered insights, deployed at `star.couchbase.com`. Restricted to `@couchbase.com` Google OAuth.

### Scraper (supportal_nicegui_app.py)

A single-user local desktop application for scraping, storing, analyzing, and querying Couchbase support tickets and cluster snapshots via a RAG-based chat interface. Runs at `localhost:8765`.

---

## Side-by-Side Overview

| | STAR | Scraper |
|---|---|---|
| **Type** | Multi-user enterprise web app | Single-user local desktop app |
| **Language** | TypeScript / Next.js 15 | Python / NiceGUI 2.0 |
| **Database** | PostgreSQL 15 (Cloud SQL) | Couchbase (NoSQL + vector index) |
| **Data Sources** | Supportal + Jira (CBSE + IDEA) | Supportal + cluster snapshots |
| **LLM** | Gemini 2.5 Flash (single provider) | Claude, Gemini, OpenAI, Ollama, LMStudio, Bedrock (pluggable) |
| **AI Approach** | Batch analysis — categorize, sentiment, root cause | RAG chat + agent tools + LLM scoring + embeddings |
| **Charting** | ECharts via echarts-for-react (29 static components) | ECharts via NiceGUI ui.echart() (static + LLM-generated at runtime) |
| **Deployment** | GCP Cloud Run (team-wide) | localhost:8765 (single user) |
| **Auth** | Google OAuth (`@couchbase.com`) | Cookie paste / browser SSO |
| **Testing** | 221 tests, GitHub Actions CI/CD | None |
| **Version** | Current (main) | v1.3.6 (feature/echart-migration) |

---

## Features

### STAR — Tabs & Pages

| Tab | What it does |
|---|---|
| **Dashboard** | KPI cards + 9 ECharts: monthly volume, priority trends, bug/CBSE rates, regional breakdown |
| **Ticket Explorer** | Searchable/filterable data table with AI analysis modal and CSV export |
| **Customer Health** | Health scores, ticket patterns, component distribution, customer drill-down |
| **Sentiment Analysis** | Sentiment scores (-1 to 1), frustration detection, trend charts |
| **Activity Heatmap** | Org × Month ticket volume matrix |
| **AI Report** | Root cause clustering, resolution trends, strategic customer narrative |
| **Product Comparison** | Capella vs Enterprise metrics side-by-side |
| **Product Signals** | Pattern analysis, repeat issue detection, bug correlation, tag clustering |
| **CBSE Explorer** | Jira service requests with P0-P3 filtering and drill-down analysis |
| **IDEA Explorer** | Jira feature requests with PM targets and priority filtering |
| **Admin** | Usage analytics (per-user, per-tab), Gemini config, sync status, data export/import |

### Scraper — Tabs & UI Sections

| Tab | What it does |
|---|---|
| **Scraping** | Customer search, Supportal auth, ticket scraping (incremental/full), topology enrichment |
| **Results** | Data table, filtering, export (CSV / Excel / Word) |
| **Chat** | RAG-based agent with 7 tools, streaming responses, inline ECharts, session memory |
| **Scoring & Analysis** | LLM scoring, analytics charts, customer profile, cross-customer comparison, cluster drill-down, cluster health |
| **Configuration** | Auth, Couchbase connection, embedding provider, chat/memory settings, AI model selection, preflight checks |
| **Customers** | Directory of all orgs with cluster/snapshot/ticket stats and last scraped timestamps |

---

## AI & LLM Integration

### STAR

- **Provider:** Google Gemini only (gemini-2.5-flash default; gemini-2.5-pro, gemini-2.0-flash available)
- **Tasks:** Ticket categorization, root cause extraction, sentiment scoring, CBSE analysis
- **Approach:** Batch processing — tickets analyzed async on ingest, results stored in PostgreSQL
- **No chat / no agent tools**

### Scraper

- **Providers:** Claude (Anthropic), Gemini, OpenAI, Ollama, LMStudio, AWS Bedrock — fully pluggable
- **Embeddings:** Ollama, OpenAI, Gemini, MLX (Apple Silicon local), LMStudio
- **LLM scoring:** Per-ticket `stars` (1-5), `temperature` (cold/warm/hot), `complexity`, `resolution_quality`, `response_timeliness`, `communication_clarity`
- **Agent tools (11):**

| Tool | What it does |
|---|---|
| `query_tickets` | N1QL search with filters (org, date, priority, status, keyword, CBSE/Jira) |
| `count_tickets` | Fast count without fetching full docs |
| `get_ticket` | Single ticket by ID with full details + snapshot topology |
| `check_data_freshness` | Age check across multiple tickets |
| `rescrape_ticket` | Live fetch from Supportal for a single ticket, merge into Couchbase |
| `rescrape_customer_tickets` | Bulk-refresh all stale tickets for a customer (configurable `stale_hours`, default 4h) |
| `list_organizations` | List orgs present in local Couchbase |
| `list_supportal_customers` | Query Supportal Analytics API for all customers (live/global) |
| `query_supportal` | Query Supportal Analytics API for live fleet-wide data |
| `generate_chart` | Renders a live interactive ECharts chart inline in chat |
| `generate_table` | Renders a data table with CSV and Excel download buttons |

- **Data source routing:** System prompt distinguishes LOCAL (Couchbase — scraped data) from LIVE/GLOBAL (Supportal Analytics API — fleet-wide live data) and routes tool selection based on query intent
- **Shared Couchbase chat history:** NiceGUI and Chainlit both read/write to the same `chat.history` Couchbase collection — conversation history is preserved across both UIs and across customer scope changes
- **MCP server (`mcp_server.py`):** 5 tools exposable to Claude Desktop (`query_tickets`, `vector_search`, `get_ticket`, `check_freshness`, `fetch_fresh_data`)

---

## Charting & Visualization

Both projects use **Apache ECharts** but in different contexts:

| | STAR | Scraper |
|---|---|---|
| **Library** | echarts-for-react 3.0.5 | NiceGUI `ui.echart()` (native) |
| **Static charts** | 29 pre-built React components | Built-in analytics and scoring tabs |
| **Dynamic / LLM-generated charts** | No | Yes — agent calls `generate_chart` tool at runtime |
| **Chart types** | Bar, line, pie, donut, heatmap, radar | Bar, horizontal bar, line, pie, donut |
| **Theme support** | Light/dark + 6 color palettes, CSS custom properties | Couchbase brand palette only |
| **Drill-down** | Click bar/cell → filtered data table | N/A (charts are output artifacts) |
| **Maximizable / draggable** | Yes (DraggableChartGrid, MaximizableChart) | No |

---

## Data Sources & Storage

| | STAR | Scraper |
|---|---|---|
| **Supportal tickets** | Yes — via sync script (VPN required) | Yes — scraped directly (cookie or browser SSO) |
| **Jira CBSE** | Yes — full issue sync via Jira API | CBSE IDs extracted from ticket text only |
| **Jira IDEA** | Yes — full feature request sync | No |
| **Cluster snapshots** | No | Yes — full topology (nodes, buckets, health, CB version) |
| **Storage** | PostgreSQL (tickets, analyses, sentiments, cbse_issues, idea_issues, page_views) | Couchbase (ticket::*, snapshot::*, chat_cache::*) |
| **Vector search** | No | Yes — semantic embeddings stored in Couchbase vector index |
| **Incremental sync** | Scheduled (Cloud Scheduler) | Change detection on status/solved date |
| **Backup/restore** | `.stardata` format (gzip + streaming JSON) | No backup mechanism |

---

## Gaps: What the Scraper Has That STAR Doesn't

| Capability | Detail |
|---|---|
| **RAG chat interface** | Conversational querying over tickets — STAR has no chat |
| **Vector / semantic search** | Find tickets by meaning, not just keyword filters |
| **Cluster snapshot topology** | Node health, bucket config, CB version, bad/warn metrics correlated with tickets |
| **Multi-provider LLM** | Supports local/offline models (Ollama, LMStudio) — STAR is Gemini-only |
| **LLM scoring** | Per-ticket stars, temperature, complexity, resolution quality |
| **CB version extraction** | Pulls exact CB version from ticket text |
| **Feature/origin classification** | Tags tickets as Proactive / Agent-Initiated / Customer-Initiated |
| **Customers directory** | Org-level view of cluster counts, snapshot counts, last scraped |
| **Couchbase Analytics API** | Cluster timeline charting via CB Analytics |
| **MCP server** | Tools exposable to Claude Desktop |
| **Chainlit chat sidecar** | Full standalone professional chat UI (port 8766) — see detail below |
| **Excel / Word export** | `.xlsx` and `.docx` output — STAR only does CSV |
| **Live rescrape from chat** | Single-ticket or bulk customer rescrape from Supportal mid-conversation |
| **Shared chat history** | Conversation history in Couchbase `chat.history`, shared across NiceGUI and Chainlit UIs |
| **Live Supportal Analytics API** | Agent can query fleet-wide live data (all customers, cluster counts, version distribution) via `list_supportal_customers` and `query_supportal` tools |

---

## Gaps: What STAR Has That the Scraper Doesn't

| Capability | Detail |
|---|---|
| **Jira CBSE Explorer** | Full service request browsing, P0-P3 filtering, drill-down — Scraper only sees IDs |
| **Jira IDEA Explorer** | Feature request tracking with PM targets and priority |
| **Product Signals** | Repeat issue detection, bug correlation, tag clustering |
| **Capella vs Enterprise comparison** | Side-by-side product-line metrics |
| **Activity heatmap** | Org × Month ticket volume matrix |
| **AI Report** | Root cause clustering, strategic customer narrative |
| **Multi-user / team visibility** | OAuth-gated, shared across support team — Scraper is single-user local |
| **Admin usage analytics** | Page views per user and per tab, daily activity, active users |
| **Formal CI/CD + testing** | 221 tests, GitHub Actions, auto-deploy on merge — Scraper has zero tests |
| **Data portability** | `.stardata` backup/restore with streaming import |
| **Cloud deployment** | GCP Cloud Run, Cloud SQL, Secret Manager, Cloud Scheduler |

---

## Biggest Strategic Gaps

1. **STAR has no conversational interface.** The Scraper can answer ad-hoc questions about tickets using RAG and agent tools. STAR users can only explore through pre-built charts and filters.

2. **STAR has no cluster topology data.** The Scraper correlates tickets with live cluster health (node count, CB version, bad/warn items). STAR's Customer Health view is derived entirely from ticket metadata — it's blind to infrastructure state.

3. **Scraper insights are invisible to the team.** Richer data — LLM scores, topology, semantic search results, session memory — lives locally and never surfaces in STAR's shared dashboards.

4. **Scraper has no Jira awareness.** It extracts CBSE IDs from ticket text but has no CBSE/IDEA explorer, no PM-facing feature request view, and no Jira sync.

5. **STAR's LLM is single-provider.** Locked to Gemini. Scraper's pluggable design supports local/offline models, which matters for cost control and experimentation.

6. **No vector search in STAR.** Ticket discovery is filter-based only. The Scraper's semantic search can surface conceptually related tickets that share no keywords.

---

## Chainlit Chat Sidecar (Scraper only)

`chainlit_app.py` is a full 520-line standalone application that runs alongside the NiceGUI app as a professional chat interface on port 8766. It is not just a button — it is a separate AI chat product sharing the same backend.

### Architecture

- **Launched separately:** `python run_chainlit.py --port 8766` (custom launcher bypasses `nest_asyncio` for Python 3.14 compatibility)
- **Shares the NiceGUI pipeline:** Imports `supportal_nicegui_app` lazily and calls the same `call_llm_with_tools` + `_AGENT_TOOLS` used by the NiceGUI chat tab
- **Shared conversation history:** Both Chainlit and NiceGUI read/write to the same Couchbase chat history store — a conversation started in NiceGUI appears in Chainlit's sidebar and vice versa

### Key Capabilities

| Feature | Detail |
|---|---|
| **Thread persistence + sidebar** | `CouchbaseDataLayer` persists all threads to Couchbase; previous conversations are resumable from a sidebar |
| **Thread resume** | On resume, customer scope and LLM config are restored from thread metadata; shared history is reloaded |
| **Chat settings panel** | In-UI settings: customer scope, LLM provider, model override, API key override — no config file needed |
| **ECharts → Plotly conversion** | LLM-generated ECharts JSON is converted to Plotly figures via `_to_plotly()` and rendered as `cl.Plotly` inline elements |
| **Pandas DataFrames** | Tables are rendered as `cl.Dataframe` interactive elements with sorting/filtering |
| **Markdown table fallback** | If pandas is not installed, tables degrade gracefully to markdown |
| **Local password auth** | Any username + any password — JWT session management via auto-generated secret persisted to `.env` |
| **Artifact parsing** | Shares `_ARTIFACT_RE` regex with NiceGUI to parse `\`\`\`echart\`\`\`` and `\`\`\`table\`\`\`` blocks from LLM output |
| **Same 7 agent tools** | `query_tickets`, `count_tickets`, `get_ticket`, `check_data_freshness`, `rescrape_ticket`, `generate_chart`, `generate_table` |
| **Supportal Analytics routing** | System prompt routes between LOCAL (Couchbase) and LIVE (Supportal Analytics API) data sources based on query intent |

### NiceGUI Chat UI — `ui.chat_message`

The NiceGUI chat tab now uses NiceGUI's native `ui.chat_message` component instead of custom `ui.row`/`ui.column` bubble layouts:

- User messages: `ui.chat_message(name="You", sent=True)` — right-aligned blue bubble
- Assistant messages: `ui.chat_message(name="Supportal", sent=False)` — left-aligned grey bubble
- Streaming responses render inside a `ui.chat_message` wrapper as they arrive
- Artifact rendering (ECharts, tables with CSV/Excel export) is preserved inside the message bubble
- Cleaner, less CSS — removes ~40 lines of manual layout/styling code

Both NiceGUI and Chainlit now write to the same `chat.history` Couchbase collection (`save_customer_chat_history` / `load_customer_chat_history`), so history is shared between the two UIs per customer and persists across restarts.

### What Chainlit adds over the NiceGUI chat tab

- Persistent sidebar with full conversation history (NiceGUI's history is session-only in the UI; Chainlit's sidebar is always visible)
- Native Plotly chart rendering (ECharts JSON converted to Plotly via `_to_plotly()`)
- Interactive DataFrames (sortable, filterable pandas `cl.Dataframe` elements)
- Per-session LLM provider/model switching via settings panel without touching config files
- A clean, shareable chat URL that can be opened independently of the NiceGUI app
- Standard Chainlit UX patterns (step indicators, author labels, settings panel)

---

## Potential Integration Opportunities

| Opportunity | Benefit |
|---|---|
| Feed Scraper's **cluster snapshot topology** into STAR's Customer Health | Infrastructure-aware health scores, not just ticket-derived |
| Push Scraper's **LLM scores** (stars, temperature, complexity) into STAR as enrichment fields | Richer ticket-level signals in shared dashboards |
| Add STAR's **Jira CBSE/IDEA data** to Scraper's RAG context | Chat answers that include service request and feature request context |
| Surface Scraper's **vector search** as a STAR feature | Semantic ticket search alongside existing keyword filters |
| Expose Scraper as a **MCP server** for STAR or Claude Desktop | Query Scraper's richer dataset from any MCP-capable client |
| Use STAR's **`/api/supportal/batch`** in Scraper | Replace Playwright HTML scraping with structured JSON — faster, no HTML parsing |
