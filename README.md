# SupportalAIAgent

A NiceGUI desktop app for scraping, storing, and AI-driven analysis of Couchbase Supportal tickets and cluster snapshots.

## Features

- **Ticket scraping** — full re-scrape or change-only mode (detects status/solved changes)
- **Snapshot scraping** — listing-page enumeration, Analytics API fast-fetch, or topology backfill
- **Couchbase persistence** — tickets as `ticket::<id>`, snapshots as `snapshot::<cluster_uuid>::<version>`
- **Vector search** — embed ticket/snapshot content, store in CB vector index, semantic query
- **LLM analysis** — cluster health scoring, CBSE pattern detection, sentiment analysis via Claude, Gemini, OpenAI, Ollama, or LMStudio
- **Preflight checks** — Supportal reachability, Analytics API, embedding model, LLM, Couchbase SDK

## Auth modes

1. **Cookie paste** — paste a `_session` cookie; uses `requests` + headless Playwright
2. **Browser login** — headful Playwright opens Supportal → you log in → session saved to `.playwright_supportal/` → headless scrape proceeds

## Setup

```bash
# Python 3.14 recommended
python3.14 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Run

```bash
venv/bin/python supportal_nicegui_app.py
# → http://localhost:8765
```

## Architecture

```
supportal_nicegui_app.py   # single-file app — UI + pipeline logic
```

Two logical components (being decoupled in `feature/pipeline-refactor`):

- **Data pipeline** — scrape → normalize → vectorize → persist (callable without UI)
- **Inspection agent** — queries CB data, never scrapes directly; MCP tool server planned

## Couchbase data model

| Key pattern | Contents |
|---|---|
| `ticket::<ticket_id>` | Ticket metadata, description, comments, embeddings |
| `snapshot::<cluster_uuid>::<version_int>` | Full topology doc, node/bucket/service state |

Timestamps are epoch seconds (`last_scraped_at`, etc.).

## Roadmap

- [ ] Auto-persist toggle + `last_scraped_at` on all records
- [ ] Decouple pipeline from UI → module-level orchestration functions
- [ ] MCP tool server: `query_tickets`, `vector_search`, `fetch_fresh_data`, `generate_chart`, `render_pdf`
- [ ] Fleet analysis: cross-customer snapshot queries, version distribution, CBSE trends
- [ ] Chat agent: natural language → N1QL/vector query, PDF report generation
