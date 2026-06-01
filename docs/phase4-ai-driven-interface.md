# Phase 4 — AI-driven Interface

**Goal:** The agent becomes the primary interface. Manual button workflows become fallbacks. Data freshness is automatic — the user never manually triggers a rescrape to ensure accurate answers.

This phase completes the original vision: a chat-first platform where the agent owns the full loop from "is my data fresh?" through "here is your answer with a chart and a PDF."

---

## Milestone 4.1 — MCP Tool Server

Expose all Supportal agent tools as a proper MCP server so Claude Desktop and other MCP clients can use them natively.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 4.1.1 | Promote `mcp_server.py` from skeleton to production | All 29+ `_AGENT_TOOLS` exposed as MCP tools via `FastMCP` or `mcp` package |
| 4.1.2 | stdio transport end-to-end test with Claude Desktop | Config: `~/.claude/claude_desktop_config.json` with `supportal` server entry |
| 4.1.3 | Auth via environment variables | CB URL, bucket, user, password via `SUPPORTAL_CB_*` env vars; no hardcoding |
| 4.1.4 | Long-running tool progress streaming | Scrape/embed tools emit progress events; MCP client shows live status |
| 4.1.5 | Tool result size guard | Truncate/paginate large N1QL results before returning to avoid MCP message size limits |
| 4.1.6 | README: Claude Desktop setup | Step-by-step: install, config file, validation query, example prompts |
| 4.1.7 | `mcp` package install validation | Startup check with friendly error message if package missing |
| 4.1.8 | Preflight tab: MCP server status | Shows running/stopped, port, last tool call timestamp |

### Acceptance criteria
- Claude Desktop can call `query_tickets`, `generate_chart`, and `get_customer_health_score` without the NiceGUI UI open.
- CB credentials come entirely from environment — no values in source or config files.
- A Claude Desktop conversation starting with "What are the open P1s for Amex?" returns correct results end-to-end.

---

## Milestone 4.2 — Agent-driven Data Freshness

The agent checks whether data is stale and triggers rescrapes autonomously. The user never manually initiates a scrape to ensure accurate output.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 4.2.1 | `fetch_fresh_data(org, scope)` agent tool (production) | Calls `run_ticket_pipeline` / `run_snapshot_pipeline` headlessly from within an agent turn; returns scrape summary |
| 4.2.2 | Freshness threshold config | Per-priority thresholds: critical = 4h, high = 24h, normal = 72h; configurable via CB document |
| 4.2.3 | Agent system prompt: freshness rule | "For any question about ticket status or cluster health, call `check_freshness` first. If stale, call `fetch_fresh_data` before answering." |
| 4.2.4 | Stale data warning banner | If agent detects staleness but user declines refresh (or it fails), render a ⚠ inline banner on the response |
| 4.2.5 | `last_scraped_at` shown in Results tab | Display as "scraped N hours ago" with colour coding (green < 24h, amber < 72h, red > 72h) |
| 4.2.6 | Scrape progress in agent status strip | `fetch_fresh_data` emits progress to `_set_chat_status` so the spinner strip shows "Scraping Amex tickets (42/150)…" |
| 4.2.7 | Incremental scrape from agent | Pass `incremental=True` flag to pipeline when called from agent — only fetches changed/new tickets |

### Acceptance criteria
- `"What's the current status of Amex's P1 tickets?"` triggers `check_freshness`, detects staleness, calls `fetch_fresh_data`, then answers with fresh data — all in one turn.
- The agent turn completes in under 60 seconds for a 200-ticket incremental scrape.
- If scrape fails, the agent answers from cached data with a clear staleness warning.

---

## Milestone 4.3 — PDF Report Generation

Proper branded PDF output — not browser print-to-PDF. Downloadable from the Assets tab, sendable via Chainlit.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 4.3.1 | Evaluate PDF backend | WeasyPrint (HTML→PDF, pure Python, no headless browser) preferred; Playwright PDF as fallback |
| 4.3.2 | `render_pdf_report(sections)` agent tool | Accepts list of sections: `{type: "markdown"\|"chart"\|"table", content: str}` |
| 4.3.3 | PDF template | Couchbase-branded: logo header, customer name, date, section dividers, page numbers, footer |
| 4.3.4 | Chart embedding in PDF | ECharts option → PNG via `echarts-node` (Node.js subprocess) or `pyecharts` server-side render; PNG embedded in PDF |
| 4.3.5 | "Export as PDF" button on Assets tab | Available for `report`-type assets; triggers server-side render and downloads file |
| 4.3.6 | Chainlit: PDF as file attachment | After `render_pdf_report` completes, Chainlit sends the PDF as a `cl.File` element |
| 4.3.7 | PDF saved to CB assets collection | Same `_save_asset_to_cb` flow with `asset_type="pdf"` and base64-encoded content |
| 4.3.8 | `requirements.txt` updated | Add `weasyprint` (or equivalent) with install notes for system dependencies (libpango etc.) |

### Acceptance criteria
- `"Generate a PDF report for Amex and send it to me"` produces a downloadable PDF with health score, SLA table, open tickets table, and at least one chart.
- PDF renders correctly on macOS Preview and Adobe Reader.
- The generated PDF appears as an asset in the Assets tab with a thumbnail.

---

## Milestone 4.4 — Session Management

Pick up prior conversations, maintain context across logins, and surface session history in the UI.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 4.4.1 | Session picker panel in Chat tab | Collapsible panel above the chat log; lists last 10 sessions from CB (`chat` scope) |
| 4.4.2 | Session metadata card | Per session: customer name, turn count, tools used, topic tags, last active timestamp |
| 4.4.3 | `resume_session(session_id)` | Loads prior history into `state["chat_history"]`; injects session summary block into system prompt |
| 4.4.4 | Prior context chip at top of chat | Collapsible "📋 Prior context from [date]" chip showing `prior_session_block`; replaces the hidden system-prompt injection |
| 4.4.5 | Session auto-save on turn completion | `save_chat_session` called after every agent turn (already exists) — verify it fires for all modes (All Tickets, Hybrid, Batch) |
| 4.4.6 | Topic tag extraction | `save_chat_session` passes last Q&A to a lightweight LLM call for 2-3 topic tags (e.g. "SLA", "cluster health", "P1 escalation") |
| 4.4.7 | Session search | Filter session picker by customer name or topic tag |
| 4.4.8 | Chainlit session picker | Thread sidebar already shows sessions; add "Resume in NiceGUI" deep-link via session ID URL param |

### Acceptance criteria
- User can see a list of prior sessions and click one to resume where they left off.
- After resuming, the first agent response references prior context without the user re-explaining the situation.
- Session picker filters correctly by customer name.

---

## Milestone 4.5 — Scheduled Pipeline

Headless scraping without the UI open. Supports cron jobs, CI pipelines, and automated data freshness maintenance.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 4.5.1 | `pipeline_runner.py` standalone script | Imports `run_ticket_pipeline` / `run_snapshot_pipeline`; reads CB config from env vars |
| 4.5.2 | CLI interface | `python pipeline_runner.py --org "Amex" --scope tickets,snapshots --embed --score --incremental` |
| 4.5.3 | Cron-compatible exit codes | Exit 0 on success, 1 on partial failure, 2 on total failure; structured JSON log to stdout |
| 4.5.4 | Change detection mode | `--incremental`: only scrapes and re-embeds tickets where `status` changed or `last_scraped_at` > threshold |
| 4.5.5 | Multi-org mode | `--org all` iterates all known orgs from CB `list_organizations` query |
| 4.5.6 | `_OP_STATUS` via CB document | Pipeline writes progress to `pipeline_status::current` in CB so the NiceGUI UI can reconnect and show live progress |
| 4.5.7 | Launchd / cron example | `docs/scheduling.md` with macOS launchd plist + Linux cron examples |
| 4.5.8 | Pipeline dry-run mode | `--dry-run`: prints what would be scraped/embedded without executing; useful for validation |
| 4.5.9 | Notification hook | `--notify-url`: POST JSON summary to a webhook URL on completion (Slack, Teams, etc.) |

### Acceptance criteria
- `python pipeline_runner.py --org "Amex" --incremental --embed` runs to completion from the terminal with no UI.
- The NiceGUI app, if open, shows live scrape progress from the pipeline runner via the CB status document.
- Exit code is 0 when all tickets are processed, 1 when some fail with errors logged.
- Launchd config in `docs/scheduling.md` successfully runs the pipeline on a schedule on macOS.

---

## Milestone 4.6 — Chainlit as Primary Chat Interface

**Goal:** Make Chainlit the chat-first face of Supportal. Surface the Assets, Fleet summary, and scrape-status content that currently lives in NiceGUI tabs directly inside Chainlit conversations, so users never need to leave the chat to see results. NiceGUI becomes the admin/config/dashboard tier; Chainlit becomes the agent-facing UI.

This milestone is the prerequisite for any future frontend swap (React, Lovable, etc.) — it validates the pattern of the Python pipeline layer being a service the chat UI consumes rather than code that lives inside the UI.

### Backlog

| # | Item | Notes |
|---|------|-------|
| 4.6.1 | Assets as `cl.File` / `cl.Image` elements | After `generate_customer_report` or `render_pdf_report`, send the asset as a Chainlit file attachment in the same message turn |
| 4.6.2 | Chart rendering in Chainlit | ECharts option → PNG server-side (pyecharts or echarts-node); send as `cl.Image`; agent narrates alongside |
| 4.6.3 | Scrape-status inline card | When a background scrape job is running, render a `cl.Text` progress card that auto-refreshes via `cl.Message.update()` until the job completes |
| 4.6.4 | Fleet summary card on session start | On first turn of a new Chainlit session, auto-fetch top-5 customer briefing and render as a collapsible `cl.Text` element (mirrors NiceGUI 3.5.5 briefing card) |
| 4.6.5 | Asset history panel | Chainlit custom element listing recent assets from CB `assets` collection; click to re-send as attachment |
| 4.6.6 | "Open in Dashboard" deep-link | Each Chainlit response that references a customer includes a link to the NiceGUI Scoring tab for that org (`/?org=<name>`) |
| 4.6.7 | Chainlit config in NiceGUI Preflight | Show Chainlit running/stopped status; link to its URL; start/stop button |
| 4.6.8 | Live background job push updates | Capture asyncio loop + Chainlit session context when job starts; background thread calls `asyncio.run_coroutine_threadsafe` to update a `cl.Message` in-place — eliminates the need to poll for scrape status |

### Acceptance criteria
- A user who only uses Chainlit can trigger a scrape, watch it complete via status updates in chat, and receive the results as a formatted table and downloadable asset — without opening NiceGUI.
- Charts generated by the agent appear inline in the Chainlit conversation as images.
- The scrape-status card updates in place (no new message per tick) while a background job runs.

### Design note
NiceGUI tabs (Scraping, Scoring, Fleet, Assets) remain fully functional as the power-user/admin interface. The goal is feature parity for the chat workflow, not replacement. Once 4.1 (MCP server) is complete, the Chainlit app can call tool implementations via the MCP client rather than importing them directly — that's the clean API boundary that would also enable a Lovable/React frontend later.

---

## Dependencies & Tech Notes

- **4.1** requires the `mcp` package (`pip install mcp`). The MCP server runs as a subprocess of Claude Desktop — it does not need NiceGUI running.
- **4.2** `fetch_fresh_data` must run in a thread (via `run.io_bound`) since scraping is blocking I/O. Progress events bridge via the existing `_OP_STATUS` mechanism.
- **4.3** WeasyPrint requires system libraries (`libpango`, `libcairo`) — document in `README.md`. Chart PNG rendering may require Node.js for `echarts-node`; evaluate `pyecharts` as a pure-Python alternative first.
- **4.4** Session picker depends on `fetch_prior_session_context` (already in v1.5.0) and the `chat.sessions` CB collection already being populated.
- **4.5** `pipeline_runner.py` should import from `supportal_nicegui_app.py` module-level functions (`run_ticket_pipeline`, `run_snapshot_pipeline`) without importing UI code — keep UI imports behind `if __name__ == "__main__"` guards or extract pipeline logic to `pipeline.py`.

## Sequencing Recommendation

```
3.1 → 3.2 → 4.5    # Fleet data + dashboard + scheduled freshness (data layer first)
        ↓
      3.3 → 3.4    # Leading indicators + portfolios (built on fleet queries)
        ↓
      4.2 → 4.1    # Agent-driven freshness + MCP server (agent interface)
        ↓
      4.4 → 4.3    # Session management + PDF (UX polish last)
        ↓
        4.6         # Chainlit as primary chat interface (depends on 4.1 for clean API boundary)
```
