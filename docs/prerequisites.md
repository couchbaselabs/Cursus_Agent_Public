# Prerequisites & Connectors

What you need to set up **before** running Cursus and pointing an LLM client at it. There are two independent layers:

1. **Cursus's own dependencies** — services the 48 MCP tools talk to (Couchbase, Supportal/VPN, an LLM provider, Salesforce). Configured via env vars or the profile.
2. **LLM-client connectors** — external MCP connectors (Gmail, Google Drive, Jira, Slack, GitHub…) that you enable **inside your LLM client** (Claude Desktop/Code/web, or another MCP host). These are *not* configured in this repo.

You do not need everything to start. The table's **"If missing"** column tells you exactly what degrades.

---

## Layer 1 — Cursus dependencies

Configuration is read from **environment variables first**, then the active Strabo profile at `~/.supportal_settings.json` (env wins). In Docker, put values in `.env`; locally, either export them or fill them in via **Strabo → Configuration**.

### 1. Couchbase — **required (everything)**

The data layer. Nearly every tool reads or writes Couchbase (tickets, customers, assets, the SFDC mirror, pins, freshness markers). The Docker Compose stack provisions this for you automatically.

| Key | Default | Notes |
|---|---|---|
| `CB_URL` | `couchbase://localhost` | `couchbase://couchbase` inside Docker |
| `CB_USER` / `CB_PASS` | `Administrator` / — | set by `couchbase-init` on first Docker run |
| `CB_BUCKET` | `supportal` | |
| `CB_SCOPE` / `CB_COLLECTION` | `transcripts` / `supportal` | |
| `CB_TLS` | `false` | `true` uses port 18091 |

**If missing:** nothing works — the server can't start a session.

### 2. Corporate VPN (GlobalProtect) + Supportal — **required for live/refresh tools**

Supportal (`supportal.couchbase.com`) is internal. The VPN must be connected for any tool that pulls live data.

| Item | Notes |
|---|---|
| VPN | Couchbase **GlobalProtect** must be connected. Run `check_connectivity` first — it probes Supportal reachability. |
| `SUPPORTAL_COOKIE` | **Optional** — Supportal endpoints are currently open; cookie plumbing is retained for forward-compatibility. Paste from DevTools if ever needed, or set in Strabo. |

**Powers:** `rescrape_customer_tickets`, `smart_refresh`, `query_supportal_analytics`, `query_cluster_topology`, `get_account_contacts`, `check_data_freshness`, live `get_ticket`.
**If missing:** live/refresh tools fail cleanly; tools that read the local Couchbase cache still work offline.

### 3. LLM provider — **required for scoring & semantic search**

Used for embeddings, ticket scoring, and the in-app assistant. Configure **one** (cloud or local).

| Provider | Key / endpoint |
|---|---|
| Anthropic (Claude) | `ANTHROPIC_API_KEY` |
| OpenAI | `OPENAI_API_KEY` |
| Google Gemini | `GEMINI_API_KEY` |
| LMStudio (local) | endpoint `http://<host>:1234` in profile (`/v1/models` must list your embed + score models) |
| Ollama (local) | endpoint `http://<host>:11434` in profile |

**Powers:** `score_ticket`, `search_tickets` (vector), the embedded agent.
**If missing:** scoring/semantic tools are unavailable; structured N1QL query tools (`query_tickets`, health, portfolio) still work.

> Local-model note: the scoring LLM is expected on the designated PC endpoint, not the Mac — see `check_connectivity`'s model-loaded preflight, which verifies both the embed and score models are actually loaded before a pipeline run.

### 4. Salesforce — **required for the 18 SFDC/SE tools**

Client-credentials OAuth against the Couchbase org. Used by all SFDC read tools, the SE weekly-update tools, the gated write, and pins.

| Key | Value / notes |
|---|---|
| `SFDC_TOKEN_HOST` | `https://couchbase.my.salesforce.com` |
| `SFDC_CONSUMER_KEY` | Connected-App consumer key |
| `SFDC_CONSUMER_SECRET` | Connected-App consumer secret |
| `SFDC_AUTH_FLOW` | `client_credentials` (default) |
| `sfdc_user_id` *(profile)* | **Your** SFDC `User.Id` — scopes `get_se_opp_worklist` / your book to you |
| `sfdc_user_name` *(profile)* | Your SFDC full name — fallback identity + note initials (e.g. `AG`) |

**Powers:** `get_my_sfdc_accounts`, `get_account_intelligence`, `get_account_opportunities`, `list_sfdc_accounts`, `lookup_sfdc_account`, `get_account_contacts`, `get_sfdc_field_mapping`, `update_sfdc_field_mapping`, `sync_sfdc_data`, `get_se_opportunities`, `get_se_opp_worklist`, `apply_se_opp_updates` (gated write), `get_se_manager_rollup`, and the pin tools.
**If missing:** all SFDC tools return an auth error; the rest of the server is unaffected.

> The one write: `apply_se_opp_updates` is the only tool that writes to a customer system of record. It defaults to `dry_run=True`, enforces an SE-Section field whitelist, and audits every applied change.

### Minimum viable setups

- **Read local data only:** Couchbase.
- **+ Refresh from Supportal:** Couchbase + VPN.
- **+ Scoring / semantic search:** add an LLM provider.
- **+ SFDC & SE tooling (all 48 tools):** add Salesforce creds + your `sfdc_user_id`.

---

## Layer 2 — LLM-client connectors (configure in your LLM client)

These are external MCP connectors, authorized through each service's own OAuth **inside your LLM client** — not in this repo, not via env vars. Enable the ones you need before a session that relies on them.

### In Claude (Desktop / Code / web)

Settings → **Connectors** → add / sign in. Common ones:

| Connector | Auth | What it gives you |
|---|---|---|
| **Google Drive** | Google OAuth | read/write Docs, Sheets, files |
| **Gmail** | Google OAuth | search/read/draft mail, labels |
| **Google Calendar** | Google OAuth | events, availability |
| **Slack** | Slack OAuth | channels, threads, canvases, DMs |
| **Atlassian (Jira / Confluence / Rovo)** | Atlassian OAuth | Jira issues, Confluence pages, Teamwork Graph |
| **GitHub** | GitHub OAuth / PAT | repos, PRs, issues, code search |
| **Couchbase MCP** | cluster credentials | direct N1QL / admin against a cluster |
| **Playwright** | none (local browser) | browser automation |

**And add Cursus itself** as an MCP server (stdio for Desktop/Code, SSE `:8768` for remote) — see [`docs/mcp-getting-started.md`](mcp-getting-started.md).

### Other LLM clients / MCP hosts

Cursus speaks standard MCP, so any MCP-capable host works:
- **Cursor, Gemini, or other MCP hosts** — point them at Cursus over stdio or SSE the same way (see the getting-started guide). Their *own* connector catalogs vary — Gmail/Drive/Jira availability depends on the host, not on Cursus.

### Caveats

- **Headless / scheduled runs:** interactively-authorized connectors (Google, Slack, Atlassian) rely on an OAuth session and may be **absent in cron/headless contexts**. Don't assume they're present in the daily monitor.
- **Connectors ≠ Cursus tools:** enabling the Gmail connector does **not** give Cursus access to mail. The harvester roadmap is what will fold Gmail/Calendar/Slack into Cursus as first-class source-providers (and unblock drafting SFDC activities from real meetings). Until then they remain client-side only.

---

## Quick checklist

- [ ] Couchbase reachable (`docker compose up`, or local cluster + `CB_*`)
- [ ] GlobalProtect VPN connected (for live Supportal tools)
- [ ] One LLM provider key or local endpoint configured
- [ ] Salesforce Connected-App creds + your `sfdc_user_id` in the profile
- [ ] Cursus registered as an MCP server in your LLM client
- [ ] Any external connectors (Gmail, Drive, Jira, Slack…) signed in **in the client**
- [ ] Run `check_connectivity` — confirms VPN, Couchbase, Supportal, and LLM model-loaded state in one call
