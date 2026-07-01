# Cursus MCP — Getting Started

The Cursus MCP server exposes Supportal ticket data, customer health signals, scrape job management, and saved assets as MCP tools consumable by Claude Code, Claude Desktop, Cursor, Gemini, or any MCP-compatible client.

---

## Prerequisites

### 1. Couchbase — local or Capella

Cursus stores all ticket and chat data in Couchbase. You need a running instance with:

| Requirement | Detail |
|---|---|
| Version | 7.2+ (Enterprise or Community; 8.0.1 recommended) |
| Services | Data, Query, Search (FTS), Index |
| Admin credentials | Any username/password with full bucket access |
| Default bucket | `rag` (configurable — any name works) |

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
| `chat` | `users` | Corax user accounts |
| `chat` | `threads` | Corax conversation threads |
| `chat` | `steps` | Corax message steps (tool calls, messages) |
| `chat` | `elements` | Corax sidebar elements |
| `chat` | `feedback` | Corax message feedback |
| `chat` | `assets` | Corax asset references per thread |

> **Note:** The `chat.*` collections are only needed if you are also running Corax. The MCP server's ticket, customer, job, and asset tools only require `transcripts.*`.

**One-shot setup script** (run once against a fresh bucket):

```bash
cd /path/to/Scraper
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
    "transcripts": ["tickets", "snapshots", "assets"],
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

### Step 1 — Add to `~/.claude.json`

Open `~/.claude.json` and add a `cursus` entry under `mcpServers`:

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

Replace `/path/to/Scraper` with the absolute path to your clone.

### Step 2 — Enable the server

Add `"cursus"` to `enabledMcpjsonServers` in `~/.claude/settings.json`:

```json
{
  "enabledMcpjsonServers": ["cursus", "couchbase", "github"],
  "permissions": {
    "allow": ["mcp__cursus__*"]
  }
}
```

### Step 3 — Restart Claude Code

MCP servers are loaded at session start. Quit and reopen Claude Code — `mcp__cursus__*` tools will appear.

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
| `save_customer_brand` | Save a customer brand kit (colors, logo, terminology) to CB |
| `get_customer_brand` | Retrieve a saved brand kit for a customer |

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
