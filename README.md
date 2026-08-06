# Cursus Agent

An AI-powered platform for Couchbase support engineers and SEs to analyse support tickets, cluster health data, and customer portfolios. It ingests ticket and cluster-topology data from Supportal and account/opportunity data from Salesforce, stores everything in a local Couchbase instance, and exposes a multi-turn tool-calling agent through three UIs: a fleet-management shell (**Cursus Unified**), a full-featured dashboard (**Strabo**), and a chat interface (**Corax**).

> **Who this is for:** Couchbase SEs and support engineers who want AI assistance across their customer portfolio — account intelligence, cluster state, ticket patterns, customer-ready reports, and automated data freshness.

---

## Architecture

> **Full interactive diagram:** open [`docs/architecture.html`](docs/architecture.html) in a browser.

```mermaid
flowchart TD
    EXT["🌐 Supportal / Zendesk\nsupportal.couchbase.com\nTickets · Snapshots · Analytics API"]

    SF["☁ Salesforce\nAccounts · Opportunities\nARR · SE/AE/CSM · Contract dates"]

    PW["🎭 Playwright Browser Automation\nHeadless cookie scrape\nHeadful SSO login\nChange-detection pipeline"]

    LLM["🤖 LLM Providers\nCloud: Claude · Gemini · OpenAI\nLocal: LMStudio :1234 · Ollama :11434\nChat + Embedding"]

    CB[("🗄 Couchbase  localhost:8091\n─────────────────────\ntranscripts.tickets\ntranscripts.snapshots\ntranscripts.assets\ntranscripts.brands\ntranscripts.markers\ntranscripts.insights\ntranscripts.accounts\ntranscripts.opportunities\n─────────────────────\nchat.history  chat.profiles\nchat.threads / steps / elements\nchat.feedback")]

    UN["🚀 Cursus Unified  localhost:8767\nFleet KPI overview\nCustomers tab (role filter chips)\nTickets, Data, Reports tabs\nEmbedded assistant panel\nDaily SFDC auto-sync (6h)\nPrimary development surface"]

    ST["🖥 Strabo  localhost:8765\nConfiguration & profiles\nScraping (tickets & snapshots)\nAgent chat with ECharts\nScoring & Analytics\nCluster health view"]

    CX["💬 Corax  localhost:8766\nPassword auth\nThread sidebar (resume sessions)\nSame agent tools as Strabo\nAsset storage (charts & tables → CB)\nShared history with Strabo"]

    MCP["🔌 Cursus MCP Server  :8768\n─────────────────────\n48 tools — query · search · score\nsmart_refresh · rescrape · health\nbriefing · assets · brand kits · reports\ncluster topology (nutshell) · freshness\nSFDC read: accounts · intel · opps · contacts\nSE worklist · manager rollup · pins\nSFDC gated WRITE: apply_se_opp_updates\n(dry-run default · field whitelist · audited)\nquery_supportal_analytics\nobservability: insights · feedback\nrecord_automation_run · failure KB\nstdio → Claude Desktop / Claude Code\nSSE  → remote MCP clients\n(see docs/mcp-architecture.md)"]

    AI["🤖 AI Clients\nClaude Desktop\nClaude Code (TUI / GUI)\nCursor · Gemini · other MCP hosts"]

    EXT -->|"Playwright scraping"| PW
    EXT -->|"Analytics API (direct HTTP)"| ST
    EXT -->|"Analytics API (direct HTTP)"| CX
    EXT -->|"Supportal API (rescrape)"| MCP
    SF -->|"OAuth REST sync (6h)"| CB
    MCP -.->|"gated SE-Section write (opt-in, dry-run default)"| SF
    PW -->|"upsert tickets & snapshots"| CB
    LLM <-->|"chat & embed inference"| ST
    LLM <-->|"chat & embed inference"| CX
    LLM <-->|"chat & embed inference"| UN
    LLM <-->|"embed & score (pipeline)"| MCP
    CB <-->|"query / FTS / vector search"| ST
    CB <-->|"thread & asset persistence"| CX
    CB <-->|"accounts, tickets, assets"| UN
    CB <-->|"read / write tickets & assets"| MCP
    ST <-->|"shared conversation history"| CX
    AI <-->|"MCP stdio / SSE"| MCP
```

---

## Quick start

> **New to this? Start here:** [`docs/quickstart-ticket-interrogation.md`](docs/quickstart-ticket-interrogation.md) — a Docker-based runbook from `git clone` to asking Claude questions about a customer's support tickets, including the Claude Desktop / Claude Code MCP setup and a copy-paste bootstrap prompt.

### Docker (recommended)

The fastest path — no local Python or Couchbase required.

**1. Copy and fill in the env file**

```bash
cp .env.example .env
```

Open `.env` and set at minimum:

| Variable | What to set |
|----------|-------------|
| `CB_PASS` | A strong password for the Couchbase admin account |
| `CHAINLIT_AUTH_SECRET` | Any random string (keeps Corax sessions valid across restarts) |

**2. Start everything**

```bash
docker compose up --build
```

On first run `couchbase-init` configures the cluster (services, memory quotas, bucket, scopes, collections, and all GSI indexes). Takes ~60 seconds the first time; subsequent starts skip init.

**3. Open the apps**

| App | URL |
|-----|-----|
| Cursus Unified (fleet dashboard) | http://localhost:8767 |
| Strabo (full dashboard) | http://localhost:8765 |
| Corax (chat) | http://localhost:8766 |
| Cursus MCP (SSE) | http://localhost:8768 |
| Couchbase Admin UI | http://localhost:8091 |

**4. Configure Strabo**

Go to **Configuration → Couchbase** and enter:

- **URL:** `couchbase://couchbase`
- **Username / Password:** values from your `.env` (defaults: `Administrator` / your `CB_PASS`)
- **Bucket:** `supportal`

Click **Save & Test** — should connect immediately.

**Stopping**

```bash
docker compose down        # stop; data volumes preserved
docker compose down -v     # stop and delete all data (full reset)
```

---

### Local (Python)

Use this when iterating on code — Cursus hot-reloads on file save.

**Prerequisites**

- Python 3.12+
- [Couchbase Server](https://www.couchbase.com/downloads/) running locally on port 8091
- At least one LLM provider configured (cloud API key **or** local LMStudio / Ollama)

**Setup**

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

**Run Cursus Unified (recommended)**

```bash
venv/bin/python run_unified.py
# Cursus Unified → http://localhost:8767
# Ctrl+C to stop
```

**Run all apps together**

```bash
venv/bin/python run_cursus.py
# Strabo → http://localhost:8765
# Corax  → http://localhost:8766
# Ctrl+C stops both
```

Cursus watches `supportal/`, `apps/strabo/`, and `apps/corax/` for `.py` changes and automatically restarts only the affected app. Use `--no-watch` to disable.

**Run individually**

```bash
venv/bin/python run_unified.py  # port 8767 — Cursus Unified
venv/bin/python run_strabo.py   # port 8765 — Strabo
venv/bin/python run_corax.py    # port 8766 — Corax
```

Configure your Couchbase connection and AI models from the Strabo **Configuration** tab or Cursus Unified settings. All UIs share the same profile.

---

## First-run walkthrough

After the app is running (Docker or local), complete these steps once to get data flowing.

### Step 1 — Connect to Couchbase

Open **Strabo** at `http://localhost:8765` → **Configuration → Couchbase**.

| Field | Docker value | Local value |
|---|---|---|
| URL | `couchbase://couchbase` | `couchbase://localhost` |
| Username | `Administrator` | your CB admin user |
| Password | value from your `.env` | your CB admin password |
| Bucket | `supportal` (or your choice) | same |

Click **Save & Test** — the status dot should turn green. Strabo creates all required scopes, collections, and GSI indexes on first connect.

---

### Step 2 — Authenticate with Supportal

Supportal (`supportal.couchbase.com`) is an internal host — **you must be on the Couchbase VPN**.

Go to **Configuration → Auth** and choose one method:

**Option A — Cookie paste (fastest)**
1. Open `supportal.couchbase.com` in your browser
2. Open DevTools → Application → Cookies → copy the `_zendesk_session` value
3. Paste it into the **Session Cookie** field in Strabo and click **Save**

**Option B — Browser SSO**
1. Click **Open Browser** in Strabo — a Chromium window launches
2. Complete the Okta SSO flow
3. Click **Confirm Login** in Strabo — the cookie is saved automatically

> Cookie sessions expire. If scraping starts failing, repeat this step.

---

### Step 3 — Configure an LLM provider

Go to **Configuration → AI Models**. You need at minimum an **embedding** provider for vector search and a **scoring** provider for LLM analysis. They can be the same or different.

| Provider | What to configure |
|---|---|
| **Ollama** (local, free) | Ensure Ollama is running; pull `nomic-embed-text` for embedding |
| **LMStudio** (local, free) | Load a 1024-dim embed model + a chat model; set the base URL |
| **Claude** | Paste your Anthropic API key |
| **Gemini** | Paste your Google API key |
| **OpenAI** | Paste your OpenAI API key |

Click **Save** after configuring. The **Preflight** tab lets you test connectivity to each provider.

---

### Step 4 — Run your first scrape

Go to **Scraping → Tickets**.

1. Type a customer name in the **Organization** field (e.g. `Western Union`)
2. Set **Max tickets** — start with `50` for a first run
3. Click **Scrape** — progress appears in real time

When the scrape finishes, Strabo automatically runs the **embed** and **score** pipeline on the fetched tickets. First run takes longer; subsequent runs use change detection to skip unchanged tickets.

Verify the data landed:
- Go to **Results** — tickets should appear in the table
- Go to **Scoring & Analysis** — charts should populate with the scraped org

---

### Step 5 — Verify Couchbase

Open the Couchbase Admin UI at `http://localhost:8091` → **Buckets** → click your bucket → **Documents**. You should see docs keyed `ticket::<zendesk_id>`.

To check counts from the Query workbench:

```sql
SELECT COUNT(*) FROM `rag`.`transcripts`.`tickets` WHERE type = 'ticket';
```

---

### Step 6 — (Optional) Configure Salesforce

To enrich your customer view with ARR, renewal dates, and SE/AE/CSM assignments, connect Cursus to Salesforce.

Open **Cursus Unified** → **Settings → Salesforce** (or set env vars):

| Env var / setting | Description |
|---|---|
| `SFDC_CLIENT_ID` | Connected App consumer key |
| `SFDC_CLIENT_SECRET` | Connected App consumer secret |
| `SFDC_USERNAME` | Your Salesforce login email |
| `SFDC_PASSWORD` | Salesforce password + security token |

Click **Sync Now** to trigger the first sync. Cursus pulls only accounts where you appear as Primary SE or Supporting SE on an open opportunity. Subsequent syncs run automatically every 6 hours.

After sync, `transcripts.accounts` and `transcripts.opportunities` are populated. Use `get_account_intelligence` from the agent or MCP to get a correlated brief (open tickets + ARR + open opps).

> If you don't have a Salesforce Connected App, ask your Salesforce admin to create one with `api` OAuth scope.

---

### Step 7 — (Optional) Wire the MCP server

To use the 48 Cursus MCP tools from Claude Desktop or Claude Code, see **[`docs/mcp-getting-started.md`](docs/mcp-getting-started.md)** for the full setup guide.

Quick summary for Claude Desktop — add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cursus": {
      "command": "/path/to/Scraper/venv/bin/python",
      "args": ["/path/to/Scraper/run_mcp.py"],
      "alwaysAllow": [
        "check_connectivity", "query_tickets", "get_ticket", "search_tickets",
        "score_ticket", "list_customers", "get_customer_health", "get_portfolio_status",
        "get_morning_briefing", "get_scrape_status", "rescrape_customer_tickets",
        "cancel_scrape_job", "list_assets", "get_asset"
      ]
    }
  }
}
```

Restart Claude Desktop — the `cursus` tools appear immediately. Call `check_connectivity` first to confirm VPN and Couchbase are reachable before triggering a rescrape.

---

## User interfaces

### Cursus Unified — `localhost:8767` ⭐ recommended

The primary development surface. Fleet-management shell with an embedded assistant panel.

| Tab | Purpose |
|-----|---------|
| **Overview** | KPI tiles: Open Tickets, Open P1s, At-Risk Clusters, Accounts, Opportunities |
| **Customers** | Customer list with role filter chips (Primary / Supporting / Other / Pinned); `× All Customers` descope button |
| **Tickets** | Cross-customer ticket view with filters |
| **Data** | Data freshness status and scrape controls |
| **Reports** | Generate and publish customer and portfolio reports |
| **Assistant** | Embedded agent panel — same 48 tools as Strabo/Corax |

Run with: `venv/bin/python run_unified.py`

---

### Strabo — `localhost:8765`

| Tab | Purpose |
|-----|---------|
| **Configuration** | Connection profiles, CB settings, auth (cookie / SSO), embedding & AI model config, preflight checks |
| **Scraping** | Ticket scraping (cookie or headful SSO), snapshot scraping (listing, Analytics API stubs, topology backfill) |
| **Results** | Filterable ticket table; CSV / Excel export |
| **Scoring & Analysis** | LLM complexity scoring, bulk rescore, analytics charts with drill-down, cluster health dashboard |
| **Customers** | Organization browser, dynamic cluster↔app alias map, health scores |
| **Assets** | Saved charts, reports, and tables from agent sessions — preview, download, or delete |
| **Chat** | Multi-turn agent with ECharts visualisations, downloadable tables, streaming output |

### Corax — `localhost:8766`

- Password-authenticated login (username becomes display name; any password accepted locally)
- Thread sidebar — click any prior session to resume it with full history and restored charts
- Same 40+ agent tools as Strabo; customer scope set from ⚙ Settings per session
- Charts and tables auto-persisted as assets in `transcripts.assets` and browsable via the **📦 Assets** quick action
- Shared conversation history with Strabo via `chat.history`

---

## MCP & agent tools

The MCP server exposes **48 tools**, and the in-app agent (Strabo / Corax / Unified) calls the same set through its tool-calling loop (up to 5 turns per message). Grouped by purpose:

### Salesforce & account intelligence (18)
| Tool | Description |
|------|-------------|
| `get_my_sfdc_accounts` | List the current SE's SFDC accounts from the local `transcripts.accounts` mirror |
| `get_account_intelligence` | Correlated brief: open tickets + SFDC ARR/products/team + open opportunities for an account |
| `get_account_opportunities` | Open opportunities for an account with stage, ARR, and close date |
| `list_sfdc_accounts` | List the local SE-scoped account mirror (your book), optional filters |
| `lookup_sfdc_account` | Ad-hoc, **live, read-only** lookup for ANY account by name (AE, type, closed-won TCV, open opps) — not just your book; ephemeral |
| `sync_sfdc_data` | Trigger an on-demand SFDC sync (accounts + opportunities); returns sync summary |
| `get_account_contacts` | Cache-first AE/TSE/CSM + renewal flags per org (marker doc → live zdorg fallback) |
| `get_sfdc_field_mapping` | Show the current SFDC-API-name → CB-schema field mapping |
| `update_sfdc_field_mapping` | Update the field mapping for a specific SFDC object type |
| `get_se_opportunities` | The SE's opportunities from the local mirror |
| `get_se_opp_worklist` | **Live, read-only** weekly SE-Section worklist (CQ+3 default) ranked by staleness, with current values + latest-entry reality-check + Opp IDs |
| `apply_se_opp_updates` | **Gated SE-Section WRITE** — the only Cursus write to a customer system: `dry_run=True` default (returns plan), field whitelist, audited to `sewrite::` marker |
| `get_se_manager_rollup` | **Live, read-only** manager "who's behind across the team" rollup, grouped by SE, ranked by total SE-Section staleness |
| `pin_opportunity` | Pin an opportunity as "owned"/"watching" — additive personal lens, never overrides SFDC |
| `pin_account` | Pin an account as owned/watching |
| `unpin` | Remove a pin |
| `list_pins` | List your pins, optional tag filter |
| `reconcile_pins` | Surface pin-vs-SFDC mismatches (pinned-not-in-SFDC / SFDC-not-pinned) — governance, never mutates SFDC |

### Tickets & customers (6)
| Tool | Description |
|------|-------------|
| `query_tickets` | N1QL ticket search with filters (org, priority, status, CB version, CBSE, feature area, date range) |
| `search_tickets` | Hybrid FTS + vector search over embedded ticket text |
| `get_ticket` | Single ticket detail; auto-fetches live from Supportal if not in CB |
| `find_customers` | Fuzzy customer-name resolution (5-step chain: LIKE → CB → FTS → per-word → difflib) |
| `list_customers` | All orgs with open ticket counts |
| `get_customer_health` | Composite health for an org: open P1/P2, active tickets, score coverage |

### Scraping, refresh & connectivity (7)
| Tool | Description |
|------|-------------|
| `smart_refresh` | **The freshness gate** — six-signal status-aware diff off the Supportal listing (new/status/solved/priority/stub/stale) and re-pulls what changed |
| `rescrape_customer_tickets` | Full rescrape: discover NEW tickets + refresh stale existing ones |
| `check_data_freshness` | Presence-only reconciliation (snapshot-referenced IDs); **informational, not the gate** |
| `check_connectivity` | Probe Supportal (VPN/GlobalProtect) + LLM provider model-loaded preflight |
| `get_scrape_status` | Progress of a background scrape job (cross-process safe via CB job doc) |
| `wait_for_scrape` | Start-and-await a scrape job for any client (cross-process safe) |
| `cancel_scrape_job` | Cancel a running background scrape job |

### Live Supportal API & cluster health (3)
| Tool | Description |
|------|-------------|
| `query_supportal_analytics` | Live Analytics API query (snapshot / cluster / customer / zdorg / nutshellresults) |
| `query_cluster_topology` | Live cluster topology: nodes, services, RAM/CPU, buckets, version |
| `generate_cluster_health_chart` | Cluster health composite chart (active clusters, real snapshot timestamps, GA-version KPIs) |

### Scoring, briefing & portfolio (3)
| Tool | Description |
|------|-------------|
| `score_ticket` | LLM complexity/sentiment score for a ticket (provider-agnostic) |
| `get_portfolio_status` | Ranked portfolio overview: health, open P1s, SLA status across customers |
| `get_morning_briefing` | Cross-account digest of recent changes for the daily routine |

### Reports, assets & brand (7)
| Tool | Description |
|------|-------------|
| `generate_health_report` | Full customer health/trend report (full-history lookbacks, period close-rate) |
| `generate_ticket_report` | Ticket-level narrative report for a customer/window |
| `export_asset` | Export a saved asset (report / chart / table) to disk |
| `get_asset` | Fetch a stored asset by id |
| `list_assets` | List saved assets |
| `save_customer_brand` | Save a customer brand kit (logo, palette) to CB |
| `get_customer_brand` | Fetch a customer brand kit |

### Observability & governance (4)
| Tool | Description |
|------|-------------|
| `get_failure_insights` | One-call governance report over the `markers` + `automation_run` collections |
| `record_automation_run` | Native run record (outcome ok/degraded/failed) for scheduled/agent runs |
| `record_feedback` | Capture human feedback into the uniform `feedback` collection |
| `record_insight` | Record an observed data pattern (candidate → validated governance) |

## Skills

Repeatable, multi-step workflows live in [`.claude/skills/`](.claude/skills/) — invoked by name in Claude Code (`/<skill>`) or auto-selected by description:

| Skill | Purpose |
|-------|---------|
| `prepare-se-opp-updates` | Weekly SE-Section hygiene: worklist → per-account context → prepared Next-Steps + Justification table + risk table, with latest-entry reality-check; prepare-first, optional gated per-opp write |
| `daily-freshness-check` | Correct-order daily monitor: connectivity → per-org `smart_refresh` gate → optional presence check → `record_automation_run` → brief |
| `portfolio-account-status` | Generate an account status report from the brand-scoped template |
| `ae-support-sync` | Reconcile AE/TSE/CSM contacts against live zdorg for an org |

## Prompt library

**35 curated prompts across 7 categories** (`supportal/prompt_library.py`): Morning Routine, Ticket Investigation, Health & SLA, Cluster & Snapshot, Fleet Analysis, Reporting, Data Freshness. `{customer}` injection; two-step browser in Corax.

---

## Data model

```
Couchbase bucket: supportal (or any bucket name you configure)
│
├── transcripts scope
│   ├── tickets        keyed  ticket::<zendesk_id>
│   ├── snapshots      keyed  snapshot::<cluster_uuid>
│   ├── assets         keyed  asset::<uuid>
│   ├── accounts       keyed  account::<sfdc_id>   (SE-scoped SFDC accounts)
│   │                         fields: org_name, se_name, supporting_se_name, ae_name,
│   │                                 csm_name, arr, contract_end_date, account_type,
│   │                                 active_ps_projects, org_aliases[]
│   ├── opportunities  keyed  opp::<sfdc_id>        (open SFDC opportunities)
│   ├── brands         keyed  brand::<org>
│   ├── markers        keyed  freshness:: / failurelog:: / cronrun:: / etc.
│   └── insights       keyed  insight::<uuid>
│
└── chat scope
    ├── history        shared Strabo ↔ Corax conversation log  keyed  history::<org>
    ├── profiles       per-user Strabo settings                keyed  profile::<name>
    ├── threads        Corax session metadata + customer scope
    ├── steps          Corax message history
    ├── elements       Corax UI elements
    └── users          Corax authenticated users
```

All timestamps are Unix epoch seconds (`last_scraped_at`, `created_at`, etc.).

---

## Supportal access

Supportal (`supportal.couchbase.com`) is Couchbase's internal support portal. All API endpoints used by this project are currently open (no authentication required). Two authentication modes are retained for forward-compatibility if auth is re-enabled:

1. **Cookie paste** — paste a `_zendesk_session` cookie from your browser DevTools into the Auth tab. Fast for ad-hoc use.
2. **Browser login (SSO)** — click *Open Browser*, complete the SSO flow in the launched Chromium window, click *Confirm Login*. Session saved to `~/.supportal_cookies.json`.

> **Note for contributors:** The cookie/auth plumbing is retained throughout the codebase even though endpoints are currently open. If Supportal adds authentication, re-enable the commented block in `supportal/api_client.py:query_supportal_analytics`.

---

## LLM configuration

Each profile stores independent LLM settings for chat and embedding:

| Provider | Chat | Embedding | Notes |
|----------|------|-----------|-------|
| **Claude** | ✅ | — | Anthropic API key required |
| **Gemini** | ✅ | ✅ | Google API key required |
| **OpenAI** | ✅ | ✅ | API key + optional base URL |
| **LMStudio** | ✅ | ✅ | Base URL (default `http://localhost:1234`) |
| **Ollama** | ✅ | ✅ | Base URL (default `http://localhost:11434`) |
| **MLX** | — | ✅ | Apple Silicon local embeddings |

Switch providers at runtime from the Strabo **AI Models** config tab or the Corax ⚙ Settings panel.

---

## Project layout

```
run_unified.py             Launch Cursus Unified (port 8767) — recommended entry point
run_cursus.py              Launch Strabo + Corax (+ MCP in SSE mode) with hot-reload
run_strabo.py              Launch Strabo only
run_corax.py               Launch Corax only
run_mcp.py                 Launch Cursus MCP server (stdio or SSE)
apps/
  unified/app.py           Cursus Unified — NiceGUI, fleet shell (primary dev surface)
  strabo/app.py            Strabo dashboard — NiceGUI, all UI logic
  corax/app.py             Corax chat handler — Chainlit, session management
  mcp/server.py            Cursus MCP server — 48 tools for Claude Desktop / Code
supportal/
  agent_tools.py           All 40+ agent tool definitions + LLM tool-calling loop
  sfdc_sync.py             Salesforce sync — sync_all(), sync_accounts(), sync_opportunities()
  api_client.py            Supportal HTTP client (ticket listing, analytics, snapshots)
  cb_helpers.py            Couchbase helpers — embedding, vector/FTS/hybrid search, RRF
  scoring.py               LLM routing, RAG context assembly, multi-step reasoning
  prompts.py               System prompts, tool guidance, follow-up suggestion logic
  prompt_library.py        28 curated prompts across 7 categories
  constants.py             Shared constants (URLs, paths, regex)
  couchbase_data_layer.py  Chainlit data layer — threads, steps, elements in CB
  llm_providers.py         Multi-provider LLM client (Claude/Gemini/OpenAI/LMStudio/Ollama)
tools/                     CLI utilities
docker/
  couchbase-init.sh        One-shot Couchbase cluster + bucket + index initialisation
docs/
  architecture.html        Interactive architecture diagram
  workflow.html            Tool workflow reference
  ROADMAP.md               Canonical roadmap (completed, in-progress, backlog)
  mcp-getting-started.md   MCP server setup guide (Claude Desktop & Claude Code)
public/custom.css          Corax UI theme overrides
requirements.txt           Python dependencies
Dockerfile                 App image (python:3.12-slim + Playwright Chromium)
docker-compose.yml         App + Couchbase + one-shot init service (ports 8765/8766/8767/8768)
.env.example               Environment variable template
```

---

## Roadmap & backlog

Items below represent the planned development trajectory. Contributors should check this section before starting work to avoid overlap.

### Completed ✅

- **Ticket pipeline** — full scrape, change-detection, new-ticket discovery vs. CB diff, auto-persist
- **Snapshot pipeline** — listing, Analytics API stubs, topology backfill, per-ticket enrichment
- **Vector search** — FTS hybrid index: BM25 + 1024-dim dot_product embeddings, RRF ranking
- **LLM agent** — multi-turn tool-calling, 5-round loop, status callbacks, cancel support, error classification
- **Strabo dashboard** — 6 tabs, 12 chart types, 6 palettes, SVG/PNG export, drill-down analytics
- **Corax chat UI** — thread sidebar, session resume, asset storage, file upload, shared history with Strabo
- **Assets system** — auto-save charts/tables/reports to CB; preview, download, delete in both UIs
- **Prompt library** — 35 curated prompts across 7 categories; customer-injection; two-step browser
- **Auth removal** — all Supportal endpoints confirmed open (v2.6.2); cookie plumbing retained
- **Fuzzy customer resolution** — 5-step chain (LIKE → local CB → Supportal FTS → per-word → difflib)
- **Cursus supervisor** — watchfiles-based hot-reload; per-app restart routing; 2s debounce
- **Fleet analytics** — `query_fleet_tickets`, `list_at_risk_clusters`, `fleet_version_distribution`, `fleet_cbse_impact`
- **MCP tool server** — 48 tools across tickets, customers, scrape jobs, assets, brand kits, report generation, SFDC, and observability; `generate_health_report`, `generate_ticket_report`, `generate_cluster_health_chart`, `smart_refresh`, `check_data_freshness`, `query_supportal_analytics`, `save/get_customer_brand`, `get_failure_insights`, `record_feedback`, `record_insight`, `record_automation_run`; stdio (Claude Desktop/Code) and SSE (remote) transports; `alwaysAllow` configured for prompt-free operation
- **Observability & governance framework (v2.7.x)** — durable failure-knowledge base in the `markers` collection (`failurelog::`, `toolfailure::`, `pipelinefailure::`, `freshness::`, `cronrun::` docs); `classify_error()` stamps every entry with an aggregatable `error_code`; `get_failure_insights` is the one-call governance report; LMStudio preflight in `check_connectivity`; human-feedback capture via `record_feedback`; `insights` collection with candidate→validated governance
- **Cursus Unified shell** — fleet-management NiceGUI app on port 8767; Overview KPIs, Customers tab with Primary/Supporting/Other/Pinned role filter chips, Tickets/Data/Reports tabs, embedded assistant panel, `× All Customers` descope button; primary active development surface
- **Salesforce integration (v2.7.x)** — `supportal/sfdc_sync.py`: `sync_all()`, `sync_accounts()`, `sync_opportunities()`, `get_account_sfdc_context()`; SE-scoped sync (only accounts where SE is `Primary_SE__c` on an open opportunity); `transcripts.accounts` + `transcripts.opportunities` CB collections; closed-won TCV rollup; 6-hour auto-sync loop on Cursus Unified startup; **faithful mirror** (SFDC stays the source of truth — never masked with local overrides); MCP tools: `get_my_sfdc_accounts`, `get_account_intelligence`, `list_sfdc_accounts`, `lookup_sfdc_account` (ad-hoc live any-account), `sync_sfdc_data`, `get_account_opportunities`, `get_account_contacts`, `get_sfdc_field_mapping`, `update_sfdc_field_mapping`
- **SE weekly-update tooling (v2.7.8x)** — `get_se_opp_worklist` (live read-only CQ+3 SE-Section worklist ranked by staleness, latest-entry reality-check, Opp IDs); `apply_se_opp_updates` — the **first and only Cursus write to a customer system of record**, gated by construction (`dry_run=True` default, SE-Section field whitelist that rejects computed rollups, audited to a `sewrite::` marker); `get_se_manager_rollup` (read-only "who's-behind across the team", rolls up to the SE manager); `prepare-se-opp-updates` skill produces the prepared Next-Steps + Justification + risk tables
- **Opportunity/account pinning (v2.7.78)** — `pin_opportunity`, `pin_account`, `unpin`, `list_pins`, `reconcile_pins`; additive personal "owned/watching" lens in `transcripts.pins`, never an override; reconciliation surfaces pin-vs-SFDC mismatches
- **Skills** — `.claude/skills/`: `prepare-se-opp-updates`, `daily-freshness-check`, `portfolio-account-status`, `ae-support-sync`
- **Docker** — single `docker compose up` starts app + fully-initialised Couchbase + MCP SSE server on :8768

### Phase 3 — Fleet dashboard (UI)

**Goal:** Shift from single-customer interrogation to fleet-wide visibility without needing to chat.

- [ ] **Fleet tab in Strabo** — top-level tab with auto-loading charts: CB version distribution (donut), open tickets by org (horizontal bar), priority breakdown (stacked bar), bad-item heatmap, 30-day ticket trend
- [ ] **Click-through** — clicking any fleet chart element loads that customer in the Scoring tab
- [ ] **Refresh controls** — "Refresh fleet data" button + last-updated timestamp
- [ ] **At-risk alerts panel** — clusters with elevated bad/warn and no open ticket, sorted by risk score `(bad * 3 + warn) * recency_factor`

### Phase 3 — Portfolio management

**Goal:** Let users define named groups of customers and get aggregate health across the group.

- [ ] **Portfolio CRUD** — `create_portfolio`, `list_portfolios`, `get_portfolio_health` agent tools; stored in CB as `saved_portfolio::<name>`
- [ ] **Portfolio health summary** — aggregate score, SLA compliance, open P1 count across member orgs
- [ ] **Fleet dashboard portfolio switcher** — filter all fleet charts to a selected portfolio

### Phase 4 — Agent-driven data freshness

**Goal:** The agent proactively keeps data fresh without the user having to trigger scrapes manually.

- [ ] **Followed customers import** — fetch Supportal's per-user "followed customers" list at profile creation; store as `profile.accounts[]`; drive freshness automation and morning briefing scope from this list
- [ ] **Freshness thresholds** — configurable per priority: critical = 4h, normal = 24h
- [ ] **`fetch_fresh_data` tool** — calls `run_ticket_pipeline` / `run_snapshot_pipeline` headlessly from within an agent turn when data is stale
- [ ] **Stale data warning** — banner on agent responses when the user skips a suggested refresh
- [ ] **System prompt guidance** — "always call `check_freshness` before answering staleness-sensitive questions"

### Phase 4 — PDF report generation

- [ ] Evaluate WeasyPrint (HTML→PDF) vs. Playwright PDF
- [ ] `render_pdf_report(sections)` agent tool — markdown + chart specs → downloadable PDF
- [ ] Couchbase-branded PDF template; ECharts embedded as PNG via headless Chromium
- [ ] "Export as PDF" button on Assets tab for report-type assets
- [ ] Corax: PDF sent as file attachment after generation

### Phase 4 — Session management

- [ ] **Session picker** — list last 10 Corax sessions from CB, click to resume with context
- [ ] **`resume_session(session_id)`** — loads prior history + injects session summary into system prompt
- [ ] **Prior context chip** — collapsible summary block at top of resumed chat
- [ ] **Auto topic tagging** — extract and persist topic tags via `save_chat_session`

### Phase 4 — Scheduled pipeline

**Goal:** Headless background operation; no UI required for data refresh.

- [ ] **`pipeline_runner.py`** — standalone script; reads CB config from env vars
- [ ] **CLI:** `python pipeline_runner.py --org "Amex" --scope tickets,snapshots --embed --score`
- [ ] **Cron-compatible** — exits 0/1; structured JSON log to stdout
- [ ] **Change detection** — only re-embed/re-score when status changed or data is stale
- [ ] **`_OP_STATUS` via CB** — persist pipeline progress to a CB doc so Strabo/Corax can display in-progress state from a detached run

### Phase 5 — Observability-driven governance (Jarvis loop)

**Goal:** The system learns from its own behavior and human feedback, with validation gates preventing self-reinforcing drift. Everything stays Couchbase-native — no external observability databases.

- [ ] **Agent tracing (layer 2)** — OTel GenAI semantic-convention docs in a CB `traces` collection (`trace::` / `span::` per LLM call and tool execution); instrument `call_llm_with_tools`; FTS + vector search over trace summaries for semantic search of agent behavior history
- [ ] **Watchers** — scheduled evaluators over traces/markers/tickets that emit *candidate* insights (`record_insight`); never self-validating
- [ ] **Validation gate** — human / judge-quorum / precedent promotion of candidate feedback and insights to validated; only validated knowledge may influence behavior
- [ ] **Orchestrator** — turns validated feedback into *proposed* `guardrail::` docs; staged rollout: human-approve → shadow mode → enforce after track record
- [ ] **Improvement loop consumption** — validated corrections become few-shot examples and eval regression sets; correction pairs exported as DPO/fine-tune data for local LMStudio models
- [ ] **Client portability** — split the support-ticket monitor into deterministic steps (pure Python, any cron scheduler) and judgment steps (local LMStudio); interactive clients (Claude, Antigravity, Cursor) remain interchangeable frontends, never load-bearing

### Future / exploratory

- [ ] **A2A multi-agent** — data freshness agent + analysis agent coordinating via Agent-to-Agent protocol
- [ ] **`fetch_url` tool** — unified live URL fetcher for `docs.couchbase.com` and Supportal pages; domain whitelist; 5–15 min in-process cache; CB URL registry (links/metadata only, no content stored)
- [ ] **Docs link registry** — pre-seed CB with known doc entry points (SDK overviews, release notes, search index); `search_couchbase_docs` agent tool

---

## Contributing

### Getting oriented

1. Read the architecture diagram (`docs/architecture.html`) first
2. All agent tools are defined in `supportal/agent_tools.py` — each is a dict in `AGENT_TOOLS` plus a dispatch branch in `_execute_agent_tool()` in `apps/strabo/app.py`
3. Shared library code lives in `supportal/`; UI-coupled code stays in `apps/strabo/app.py` or `apps/corax/app.py`
4. The Couchbase scope for tickets/snapshots/assets is typically `transcripts`; chat persistence uses the `chat` scope

### Adding a new agent tool

1. Add the tool definition dict to `AGENT_TOOLS` in `supportal/agent_tools.py`
2. Add a dispatch branch in `_execute_agent_tool()` in `apps/strabo/app.py`
3. If the tool should be available via MCP, add it to `apps/mcp/server.py`
4. Update the tool description in this README's agent tools table
5. Bump `__version__` in `apps/strabo/app.py`

### Branch conventions

- `main` — protected; stable releases only
- `scaling` — active development branch; merge here first
- Feature branches: `feature/<short-description>`

### Secrets

Credentials never go in the repo. The settings file (`~/.supportal_settings.json`) and cookies (`~/.supportal_cookies.json`) live in `$HOME` and are never committed. See `.gitignore` for the full exclusion list.
