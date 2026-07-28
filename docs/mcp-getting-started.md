# Cursus MCP — Getting Started

The Cursus MCP server exposes Supportal ticket data, customer health signals, scrape job management, and saved assets as MCP tools consumable by Claude Code, Claude Desktop, Cursor, Gemini, or any MCP-compatible client.

---

## Prerequisites

### 1. Couchbase — local or Capella

Cursus stores all ticket and chat data in Couchbase. You need a running instance with:

| Requirement | Detail |
|---|---|
| Version | 8.0.1+ Enterprise Edition |
| Services | Data, Query, Search (FTS), Index |
| Admin credentials | Any username/password with full bucket access |
| Bucket | `supportal` in the shipped Docker setup; `rag` is the code fallback. Configurable — see note below. |

> **On the bucket name:** the `docker compose` stack creates a bucket named **`supportal`** and the app container is pointed at it automatically (`CB_BUCKET=supportal`). If you run the MCP server standalone with no config, it falls back to **`rag`**. Either way it's overridable via the `CB_BUCKET` env var or your Strabo profile — just make sure the server and the bucket agree. The examples below use `rag`; substitute your bucket name.

**Easiest local setup:** the repo ships a `docker-compose.yml` that starts a pre-configured Couchbase instance:

```bash
docker compose up -d couchbase
```

Wait ~30 seconds, then open `http://localhost:8091` and verify all services are green.

**Capella:** a free trial cluster works. Ensure the Data, Query, Search, and Index services are enabled on at least one node.

---

### 2. Required scopes and collections

The following are created automatically by Strabo or Corax on first run. If you are connecting the MCP server to a fresh Couchbase instance **without** running Strabo/Corax first, create them manually via the UI or the script below.

#### Bucket: `rag` (or your configured bucket name)

| Scope | Collection | Purpose |
|---|---|---|
| `transcripts` | `tickets` | Support ticket documents (one doc per ticket) |
| `transcripts` | `snapshots` | Cluster topology snapshots linked to tickets |
| `transcripts` | `assets` | Saved charts, tables, reports, images, PDFs |
| `transcripts` | `brands` | Customer brand kits (colors, logo, terminology) |
| `transcripts` | `markers` | Freshness checks, failure logs, pipeline errors, automation run records |
| `transcripts` | `insights` | Observed customer/ticket patterns (candidate → validated) |
| `chat` | `users` | Corax user accounts |
| `chat` | `threads` | Corax conversation threads |
| `chat` | `steps` | Corax message steps (tool calls, messages) |
| `chat` | `elements` | Corax sidebar elements |
| `chat` | `feedback` | Corax message feedback |
| `chat` | `assets` | Corax asset references per thread |

> **Note:** The `chat.*` collections are only needed if you are also running Corax. The MCP server's ticket, customer, job, and asset tools only require `transcripts.*`.

**One-shot setup script** (run once against a fresh bucket):

```bash
cd /path/to/your/clone     # wherever you cloned it — the dir name doesn't matter
venv/bin/python - <<'EOF'
from datetime import timedelta
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.collections import CollectionSpec

CB_URL    = "couchbase://localhost"
BUCKET    = "rag"
USERNAME  = "Administrator"
PASSWORD  = "your-cb-password"   # replace with your actual password

c = Cluster(CB_URL, ClusterOptions(PasswordAuthenticator(USERNAME, PASSWORD)))
c.wait_until_ready(timedelta(seconds=15))
bkt = c.bucket(BUCKET)
cm  = bkt.collections()

existing = {s.name: {col.name for col in s.collections} for s in cm.get_all_scopes()}

for scope, cols in {
    "transcripts": ["tickets", "snapshots", "assets", "brands", "markers", "insights"],
    "chat":        ["users", "threads", "steps", "elements", "feedback", "assets"],
}.items():
    if scope not in existing:
        cm.create_scope(scope)
        print(f"Created scope: {scope}")
    for col in cols:
        if col not in existing.get(scope, set()):
            cm.create_collection(CollectionSpec(col, scope_name=scope))
            print(f"Created collection: {scope}.{col}")

print("Done.")
EOF
```

---

### 3. Vector (FTS) index

Semantic search (`search_tickets`) requires a vector FTS index on the `tickets` collection. Strabo creates this automatically when you first run a scrape. To create it manually:

1. Open the Couchbase UI → **Search** → **Add Index**
2. Or trigger it from within Strabo by running any scrape job — the index is created before the first embed write.

The index is named `tickets_vector_idx` and lives under `rag.transcripts`.

> **Tip:** If you only need structured search (`query_tickets`) and not semantic search (`search_tickets`), skip the FTS index — `query_tickets` uses N1QL only.

---

### 4. Python environment

```bash
git clone https://github.com/couchbaselabs/Cursus_Agent_Public.git
cd Cursus_Agent_Public

python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

Verify the MCP server starts cleanly:

```bash
venv/bin/python run_mcp.py --help
```

---

### 5. Strabo profile (recommended) or environment variables

The MCP server reads connection settings from the active **Strabo profile** (`~/.scraper_settings.json`). If you have already configured Strabo, nothing extra is needed.

If you are using the MCP server standalone (without Strabo), set environment variables instead:

| Variable | Default | Description |
|---|---|---|
| `CB_URL` | `couchbase://localhost` | CB connection string |
| `CB_BUCKET` | `rag` | Bucket name |
| `CB_USER` | _(from profile)_ | CB username |
| `CB_PASS` | _(from profile)_ | CB password |
| `CB_TLS` | `false` | Set `true` for Capella or TLS clusters |
| `CB_SCOPE` | `transcripts` | Scope containing ticket/asset collections |
| `CB_COLLECTION` | `tickets` | Primary ticket collection name |
| `CB_COOKIE` | _(from profile)_ | Supportal session cookie — required only for `rescrape_customer_tickets` |

**Capella example:**

```bash
export CB_URL="couchbases://cb.xxxx.cloud.couchbase.com"
export CB_TLS=true
export CB_USER="your-db-user"
export CB_PASS="your-db-password"
```

---

### 6. Embedding provider (optional — for semantic search)

`search_tickets` embeds your query before running vector search. The embedding provider is also read from the Strabo profile. To use it standalone, the provider is configured via profile only (no env var override currently). Supported providers:

| Provider | How to enable |
|---|---|
| **Ollama** (local, default) | `ollama pull nomic-embed-text` and ensure Ollama is running on `:11434` |
| **LMStudio** (local) | Load a 1024-dim embedding model and start the local server on `:1234` |
| **Gemini** | Set `emb_gemini_key` in your Strabo profile |
| **OpenAI** | Set `emb_openai_key` in your Strabo profile |

If no embedding provider is configured, `search_tickets` will error — all other tools continue to work.

---

## Claude Code setup

There are two ways to connect: **stdio** (Claude Code launches the server itself — best for local dev) and **SSE** (connect to an already-running server — best when the stack is up via Docker). The `claude mcp add` CLI is the easiest path for both.

### Option A — stdio (local, recommended)

Claude Code starts and stops the server for you; no separate process to manage. The repo self-locates, so it can live at **any path under any directory name** — you just tell the client where your clone is.

**Easiest — run this from inside your clone** (`$(pwd)` fills in the absolute path for you):

```bash
cd /wherever/you/cloned/it
claude mcp add cursus --scope user -- "$(pwd)/venv/bin/python" "$(pwd)/run_mcp.py"
```

Or spell the paths out explicitly:

```bash
claude mcp add cursus --scope user -- \
  /abs/path/to/<your-clone>/venv/bin/python \
  /abs/path/to/<your-clone>/run_mcp.py
```

- `--scope user` makes it available in every project. Use `--scope project` to share it with your team via a checked-in `.mcp.json`, or `--scope local` (default) for just this project.
- Both paths must be absolute — the venv Python and `run_mcp.py`. `$(pwd)` guarantees that.
- No `--transport` needed: stdio is the default.

### Option B — SSE (connect to the Docker stack)

If you're running the stack with `docker compose up`, the MCP server is already listening on `:8768`. Point Claude Code at it instead of launching a second copy:

```bash
claude mcp add --transport sse cursus http://localhost:8768/sse --scope user
```

### Verify it's connected

```bash
claude mcp list          # cursus should show ✓ connected
```

Or inside a Claude Code session, `/mcp` lists connected servers and their tools. If you'd rather edit config by hand, `claude mcp add` writes to `~/.claude.json` (user scope) — the equivalent block is:

```json
{
  "mcpServers": {
    "cursus": {
      "command": "/absolute/path/to/Cursus_Agent_Public/venv/bin/python",
      "args": ["/absolute/path/to/Cursus_Agent_Public/run_mcp.py"]
    }
  }
}
```

### Skip the approval prompts (optional)

To let `mcp__cursus__*` tools run without a per-call permission prompt, add to `~/.claude/settings.json`:

```json
{
  "permissions": {
    "allow": ["mcp__cursus__*"]
  }
}
```

---

## Claude Desktop setup

Add to `~/.claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cursus": {
      "command": "/path/to/Scraper/venv/bin/python",
      "args": ["/path/to/Scraper/run_mcp.py"]
    }
  }
}
```

Restart Claude Desktop. The Cursus tools appear in the tool picker.

---

## Remote / SSE setup (Gemini, custom clients)

```bash
venv/bin/python run_mcp.py --transport sse --port 8768
```

Point your MCP client at `http://localhost:8768/sse`. For remote access, expose the port via a reverse proxy with appropriate auth — the SSE transport itself has no built-in authentication.

---

## Available tools

| Tool | Description |
|---|---|
| `check_connectivity` | Check VPN status + Couchbase + Supportal reachability. Run this first if scraping fails. |
| `query_tickets` | Structured filter search — org, status, priority, keyword, days open |
| `get_ticket` | Full ticket detail — description, comments, topology, CBSEs, scores |
| `search_tickets` | Semantic vector search — finds tickets by meaning |
| `score_ticket` | Run LLM scoring on a ticket and save results to CB |
| `list_customers` | All orgs with ticket counts |
| `get_customer_health` | Open tickets, P1/P2 counts, top CBSEs for one customer |
| `get_portfolio_status` | Fleet-wide ranking by P1/P2 open count |
| `get_morning_briefing` | Fleet briefing across key accounts — active tickets, scores, CBSEs, summaries |
| `get_scrape_status` | Check running or recent scrape job status |
| `rescrape_customer_tickets` | Trigger a background Supportal refresh for a customer |
| `cancel_scrape_job` | Cancel a running scrape job by job ID |
| `list_assets` | List saved charts, tables, reports filtered by org/type |
| `get_asset` | Fetch a single asset with full content |
| `generate_health_report` | Generate a full customer health report HTML from live CB data, apply brand colors, save as asset |
| `generate_ticket_report` | Generate a response cadence visualization for a single ticket, save as HTML asset |
| `generate_cluster_health_report` | Build a per-cluster health chart enriched with live Supportal cluster names, save as asset |
| `save_customer_brand` | Save a customer brand kit (colors, logo, terminology) to CB |
| `get_customer_brand` | Retrieve a saved brand kit for a customer |
| `check_data_freshness` | Compare live Supportal ticket IDs vs local CB cache; write a freshness marker; flag orgs needing rescrape |
| `query_supportal_analytics` | Run a SELECT-only SQL++ query against the live Supportal Analytics API to cross-check local data |
| `get_failure_insights` | One-call governance report — pipeline failures, tool errors, error classification, automation run health |
| `record_insight` | Capture an observed customer/ticket pattern as a candidate insight in CB for later validation |
| `record_feedback` | Persist human feedback on a tool response or agent output to the feedback collection |
| `record_automation_run` | Log the outcome of a scheduled automation run (success/failure, counts, errors) for health tracking |
| `smart_refresh` | Lightweight freshness diff — compares Supportal vs local CB on six signals (new/status/solved/priority/stub/stale-open), pulls only what changed, and kicks off background enrichment. Prefer over `rescrape_customer_tickets` |
| `query_cluster_topology` | Per-node cluster layout from Supportal nutshellresults — services, RAM, disk, CB version. Handles old/new snapshot formats. (CPU count pending an upstream analytics fix) |
| `find_customers` | Fuzzy customer search by topic/keyword across feature area, component path, subject, tags, version |
| `get_account_contacts` | AE / TSE / PSE per org, sourced from live Supportal zdorg |
| `get_my_sfdc_accounts` | Salesforce accounts where you are the SE (opportunity-level, pre-filtered to your saved SFDC user) |
| `list_sfdc_accounts` | All synced Salesforce accounts, filterable by account-level SE name |
| `get_account_opportunities` | Open opportunities + ARR for an account, with per-opportunity Primary SE |
| `get_account_intelligence` | Blended account view — SFDC account/opps + support health + contacts in one call |
| `get_se_opportunities` | Opportunities across an SE's book of business |
| `sync_sfdc_data` | Read-only refresh of the local Salesforce mirror (accounts + opportunities). Never writes to Salesforce |

> **Read-only Salesforce:** all SFDC tools only ever read from Salesforce into the local Couchbase mirror. Nothing writes back to SFDC.

## VPN requirement

`rescrape_customer_tickets` scrapes Supportal, which is an internal host and requires the **Couchbase corporate VPN**. All other tools only need Couchbase to be reachable and work without VPN.

Before triggering a rescrape, call `check_connectivity` — it lists any named VPN services and their state via `scutil` (macOS, vendor-agnostic), TCP-probes Couchbase and Supportal, and returns a plain-English summary. Supportal reachability is treated as the authoritative VPN indicator since named-service detection varies by VPN client (GlobalProtect, AnyConnect, WireGuard, etc.).

## Available resources

| URI | Description |
|---|---|
| `customers://list` | All customers with open ticket counts |
| `tickets://{organization}` | Open tickets for a specific customer |
| `health://{organization}` | Health summary for a specific customer |

---

## Verifying the connection

From Claude Code, ask:

> "List all customers with open tickets" — calls `list_customers`

> "What's the health of Western Union?" — calls `get_customer_health`

> "Are there any running scrape jobs?" — calls `get_scrape_status`

If the tools return a CB connection error, check your credentials and ensure the Couchbase Data and Query services are reachable from the machine running the MCP server.
