# Quickstart — from empty Couchbase to interrogatable support tickets

This runbook takes you from a **fresh `git clone`** to **asking Claude questions about a
customer's support tickets** through the Cursus MCP server — all via Docker.

Two parts:
1. **Part A — Stand up the stack + ingest tickets** (Docker, one customer, 0 → N tickets).
2. **Part B — Connect Claude Desktop / Claude Code** and interrogate.

There's also a **copy-paste bootstrap prompt** at the end that drives the whole ingest →
verify → interrogate flow once the MCP is connected.

> **Prerequisites**
> - Docker + Docker Compose
> - **Couchbase corporate VPN (GlobalProtect)** — Supportal is internal; scraping needs it. (Couchbase Analytics/SFDC endpoints don't.)
> - Your **Supportal session cookie** (or SSO login) — see Step A3.
> - One **LLM/embedding provider**: local **Ollama** or **LMStudio** (free), or a **Claude / Gemini / OpenAI** API key.

---

## Part A — Stack up + ingest

### A1. Clone and configure

```bash
git clone https://github.com/couchbaselabs/Cursus_Agent_Public.git
cd Cursus_Agent_Public
cp .env.example .env
```

Edit `.env` and set at minimum:

| Variable | Value |
|---|---|
| `CB_PASS` | a strong Couchbase admin password |
| `CHAINLIT_AUTH_SECRET` | any random string (keeps Corax sessions valid across restarts) |

### A2. Start everything

```bash
docker compose up --build
```

On first run `couchbase-init` provisions the cluster: services, memory quotas, the
`supportal` bucket, the `transcripts` + `chat` scopes, all collections, and GSI indexes
(~60s the first time; skipped on later starts). When it settles you have:

| Surface | URL |
|---|---|
| Cursus Unified (fleet dashboard) | http://localhost:8767 |
| Strabo (full dashboard) | http://localhost:8765 |
| Corax (chat) | http://localhost:8766 |
| **Cursus MCP (SSE)** | **http://localhost:8768/sse** ← Claude connects here |
| Couchbase Admin UI | http://localhost:8091 |

The app container is pre-pointed at the `supportal` bucket (`CB_BUCKET=supportal`), so the
MCP server connects automatically — no manual bucket config needed for the MCP path.

### A3. Authenticate to Supportal (Strabo)

Open **Strabo → Configuration → Auth**. Two options:

- **Cookie paste (fastest):** in your browser open `supportal.couchbase.com` → DevTools →
  Application → Cookies → copy the `_zendesk_session` value → paste into Strabo → **Save**.
- **Browser SSO:** click **Open Browser**, complete Okta, click **Confirm Login**.

> Cookies expire — if scraping starts failing later, repeat this.

### A4. Configure an LLM/embedding provider (Strabo)

**Strabo → Configuration → AI Models.** You need an **embedding** provider (for semantic
search) and, optionally, a **scoring** provider (for LLM ticket scoring). Same or different.

| Provider | Setup |
|---|---|
| Ollama (local, free) | run Ollama, `ollama pull nomic-embed-text` |
| LMStudio (local, free) | load a 1024-dim embed model + a chat model; set the base URL |
| Claude / Gemini / OpenAI | paste the API key |

Use the **Preflight** tab to test each connection.

### A5. Ingest a customer's tickets (0 → N)

**Either** from the Strabo UI (**Scraping → Tickets**, type an org e.g. `Western Union`,
run), **or** — the recommended path — drive it from Claude once the MCP is connected (Part
B + the bootstrap prompt). The MCP tool `rescrape_customer_tickets` scrapes Supportal into
`supportal.transcripts.tickets`; if a scoring/embedding provider is configured, the
pipeline embeds and scores as it ingests.

Verify tickets landed (Admin UI → Query, or ask Claude "how many tickets for \<org\>?"):

```sql
SELECT COUNT(*) FROM `supportal`.`transcripts`.`tickets`;
```

---

## Part B — Connect Claude and interrogate

The MCP server is already running in Docker on **`http://localhost:8768/sse`**. Point your
client at it.

### Claude Code (SSE — connect to the Docker server)

```bash
claude mcp add --transport sse cursus http://localhost:8768/sse --scope user
claude mcp list          # cursus should show ✓ connected
```

*(Optional — skip per-call approval prompts.)* In `~/.claude/settings.json`:

```json
{ "permissions": { "allow": ["mcp__cursus__*"] } }
```

### Claude Desktop (SSE)

Add to `~/.claude/claude_desktop_config.json`, then restart Desktop:

```json
{
  "mcpServers": {
    "cursus": { "transport": "sse", "url": "http://localhost:8768/sse" }
  }
}
```

> **Prefer stdio (Claude launches the server itself, no Docker)?** Run the app locally in a
> venv instead and use:
> ```bash
> claude mcp add cursus --scope user -- "$(pwd)/venv/bin/python" "$(pwd)/run_mcp.py"
> ```
> The repo self-locates, so any clone path works.

### Verify the connection

In a Claude session, ask:

> "List all customers with open tickets."  → calls `list_customers`
> "What's the health of \<org\>?"          → calls `get_customer_health`

If you get a CB connection error, confirm the Docker stack is up and the Data + Query
services are green in the Admin UI.

---

## The bootstrap prompt (0 tickets → interrogatable)

Paste this into a Claude session that has the **cursus** MCP connected. Replace
`<ORG>`. It checks connectivity, ingests only what's needed, confirms enrichment, and
then proves interrogation works.

```
You have the Cursus MCP tools. Bootstrap <ORG>'s support tickets from empty into a
fully interrogatable local set, then demonstrate interrogation. Work in this order and
report results at each step:

1. Run check_connectivity. If Supportal is unreachable, stop and tell me to connect the
   Couchbase VPN (GlobalProtect) — do not proceed.
2. Run get_customer_health for "<ORG>" to see the current local state (likely 0 tickets).
3. Ingest: if this is a first load, call rescrape_customer_tickets for "<ORG>"; otherwise
   call smart_refresh for "<ORG>" to pull only new/changed tickets. Use wait_for_scrape /
   get_scrape_status to wait for it to finish. Report how many tickets were scraped.
4. Verify freshness with check_data_freshness for "<ORG>" — confirm local matches Supportal
   and report any drift.
5. Confirm enrichment: report how many tickets have scores and embeddings. If embeddings
   are missing, note that semantic search (search_tickets) won't work until an embedding
   provider is configured, and continue with structured tools.
6. Prove interrogation with three queries and show the results:
   a. query_tickets for "<ORG>" open + priority-sorted — the current open workload.
   b. search_tickets for "<ORG>" on a meaningful theme (e.g. "backup failures" or
      "timeout") — semantic recall (skip if no embeddings).
   c. get_ticket on the single highest-priority open ticket — full detail + topology.
7. Finish with a 3-bullet summary: total tickets, open P1/P2 count, and the top risk theme
   you'd raise with the account team.

Never fabricate ticket data — every number must come from a tool call. If a tool errors,
show the error and what you'd check, don't paper over it.
```

**What "fully interrogatable" means after this runs:**

| Capability | Tool | Needs |
|---|---|---|
| Structured filter (org/status/priority/age/keyword) | `query_tickets` | tickets only |
| Full ticket detail (comments, topology, scores) | `get_ticket` | tickets only |
| Semantic / "by meaning" search | `search_tickets` | embeddings (embed provider) |
| Customer health rollup | `get_customer_health` | tickets only |
| LLM quality/risk score | `score_ticket` | scoring provider |
| Lightweight refresh (diff + pull only changes) | `smart_refresh` | Supportal reachable |

---

## Troubleshooting

- **`No customers` / 0 tickets after scrape** — Supportal auth expired (redo A3) or VPN
  down (`check_connectivity`).
- **Container can't reach Supportal even with VPN up** — GlobalProtect split-tunnel may not
  route Docker's bridge network. Confirm with `check_connectivity`; if it fails only inside
  Docker, run the scrape from the host (local venv + stdio MCP) instead, or use full-tunnel.
- **`search_tickets` errors** — no embedding provider configured (A4). Structured tools
  still work.
- **Bucket mismatch** — Docker uses `supportal`; a standalone (non-Docker) MCP falls back to
  `rag`. Set `CB_BUCKET` to match if you run the server yourself.
- **New MCP tools don't appear** — restart the Claude client so it reloads the server.

See also: [`mcp-getting-started.md`](mcp-getting-started.md) (deep MCP setup) and
[`mcp-architecture.md`](mcp-architecture.md) (tool catalog + customization).
