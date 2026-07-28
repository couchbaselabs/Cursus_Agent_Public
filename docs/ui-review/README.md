# Handoff: Cursus Unified — dashboard-first app with an embedded assistant

## Overview
Cursus today ships as **two separate apps** over one Supportal dataset:
- **Strabo** (`apps/strabo/app.py`, NiceGUI, :8765) — management dashboard: onboarding/scraping, configuration, results table, scoring/analytics, customers/cluster health, assets.
- **Corax** (`apps/corax/app.py`, Chainlit, :8766) — chat agent: starters, quick actions, thread sidebar/session resume, asset browser.

The two overlap heavily (both run the same agent + draw the same charts), look different, and give users no obvious "front door." This handoff describes the agreed direction from the UX review: **one product, dashboard-first, with the agent collapsed into a persistent embedded assistant panel** that shares the app's global customer scope, conversation history, and chart style. Everything the assistant produces lands in a single **Reports & Automation** hub.

Implement this on a **new branch** (suggested: `feat/cursus-unified-shell`).

## About the design files
The files in this bundle are **design references created in HTML** — an interactive prototype showing intended layout, hierarchy, and behavior. **They are not production code to copy directly.** The task is to **recreate this design in the target environment**. Two realistic options for this codebase:
1. **Stay in NiceGUI** (Strabo's stack) — build the unified shell as NiceGUI layout: `ui.header`, a left `ui.tab_panels`-style content area, and a right splitter/drawer hosting the assistant. Reuse Strabo's existing ECharts + agent tool calls; embed Corax's agent loop into the right panel instead of a separate Chainlit app.
2. **New React/Vue SPA** talking to the existing Python agent/tools over an API — if the team wants to leave NiceGUI/Chainlit behind. Only choose this if a frontend framework is being introduced deliberately.
Prefer option 1 for lowest risk; it reuses everything already in `apps/strabo`.

## Fidelity
**Medium-to-high.** Colors, type, spacing, radii, and interaction model are specified and should be followed. Chart *data* is illustrative placeholder — wire the real agent/query functions in. Treat the layout, component structure, states, and copy as the spec; treat the numbers as sample data.

## Layout — the unified shell
Full-height (100vh) flex column:

1. **Top bar** (height ~44px, `#141518` bg, white text, flex row, `padding:11px 20px`):
   - Left: logo mark (22px `#ea2328` rounded square, white "C", Archivo 800) + wordmark "Cursus" (Archivo 16px).
   - Nav tabs (left-aligned, 16px gap from logo): **Overview · Customers · Tickets · Data · Reports & Automation**. Active tab = `#ea2328` bg, white, `border-radius:7px`, `padding:6px 13px`. Inactive = transparent, `#c9ccd2`, hover `#26282d`.
   - Right (margin-left:auto): **global customer selector** pill (`#26282d` bg, `#34373d` border, `border-radius:20px`, `padding:6px 12px`; a status dot colored by health + name + ▾; hover border `#ea2328`) and a Couchbase connection indicator (green dot `#4ec27f` with glow + "Couchbase").
   - Customer selector opens a dropdown (`#1b1d21`, `border-radius:10px`, rows with health dot + name + score).

2. **Body** (flex row, fills remaining height):
   - **Main canvas** (flex:1, scrollable, `padding:24px 28px`, bg `#ece8e1`). Renders the active tab.
   - **Assistant panel** (fixed, right, 352px wide, white, `border-left:1px solid #ddd8ce`, full height). Collapsible → a 44px rail with vertical "Assistant ⟨" label; expanding restores it.

### Global scope contract (important)
The customer selector in the top bar is the **single source of truth** for scope. Selecting a customer updates: all canvas KPIs/charts, the assistant's scope badge, and the assistant input placeholder. The assistant never has its own hidden customer setting (this fixes Corax's "hidden ⚙ customer" problem).

## Screens / views

### 1. Overview — "Fleet at a glance" (default landing)
- Header row: title "Fleet at a glance" (Archivo 800, 22px, `nowrap`) + subtitle "Scoped to `<customer>`"; right side "last refresh 3m ago · live" (IBM Plex Mono 11px, "live" in `#2f8f5b`).
- **KPI row** (4-col grid, 12px gap): FLEET HEALTH, OPEN P1, AT-RISK CLUSTERS, SLA BREACH. Each is a white card (`border:1px solid #e0dbd0`, `border-radius:11px`, `padding:15px`): mono 10px label + Archivo 800 30px value. Value colors: health `#fb8c00`, P1 `#e53935`, at-risk `#c98a12`, sla `#1b1d21`. **Cards are buttons** — clicking primes the assistant with a scoped question (and opens the panel); the OPEN P1 card also sets a `filter` (active state = `#fdecec` bg, `#ea2328` border).
- **Charts row** (grid 1.3fr / 1fr): "Open tickets by org" (horizontal bars, each a button that switches the global customer) and "CB version spread" (donut via conic-gradient + legend).
- Footer hint (mono 11px, `#9a9ea6`): "▸ click any KPI or bar → filters the view + primes the assistant".

### 2. Customers & cluster health
- Two-col grid (260px / 1fr).
- Left: org list; each row is a button (white card, `border-radius:8px`, selected = `#ea2328` border + left accent). Shows name + "`<n>` open · health `<n>`".
- Right: 3 KPI cards (HEALTH / OPEN P1 / NODES) + a "Cluster topology · services / RAM" card with a wrap of node tiles (88×56, healthy = `#eef3fb`/`#bcd4f0`, degraded = `#fdeaea`/`#f0bcbc`). The degraded node is a button → primes the assistant ("Analyze node cbse-4").

### 3. Tickets (filtered ticket table)
- Header: title "Tickets" + "Scoped to `<customer>`". Table is always scoped to the global customer.
- **Agent-written summary banner** (white card, `border-left:3px solid #ea2328`): a "▸ assistant" tag + a live one-line summary computed from the *currently filtered* rows (count, open-P1 count, versions present, highest-score subject) + an "Ask ↗" button that primes the assistant with the current view. Regenerate this from the real agent in production.
- **Filter bar**: priority chips (P1/P2/P3, colored `#e53935`/`#fb8c00`/`#43a047`) and status chips (Open/Pending/Solved, `#c02620`/`#c98a12`/`#2f8f5b`), each a toggle (active = filled, its color/white text); a divider; a right-aligned search input (matches subject or ID) + a green **Export** button (`#2f7d4f`) that asks the assistant to export the current view to CSV.
- **Table** (white card, radius 11px): columns `ID | Subject | Pri | Status | CB ver | Score` (grid `64px 1fr 62px 78px 74px 56px`). Header row `#f4f2ec`, mono uppercase labels. Body rows zebra (`#fff`/`#faf8f4`); ID in mono `#ea2328`; priority as a colored pill; status as colored text; score in Archivo 700 colored by band (≥70 `#c02620`, ≥40 `#c98a12`, else `#6b6f76`). **Each row is a button** → `primeFrom('Explain ticket <id> — <subject>')` (opens the assistant with the ticket). Empty state: "No tickets match — clear a filter."
- Footer hint: "▸ click a row → hand the ticket to the assistant".
- **Deep-link**: the OPEN P1 KPI (Overview & Customers) calls `goTickets('P1','Open')` → switches to this tab with those filters pre-applied.

### 4. Data & onboarding
- "Scraper" card: customer field (pre-filled from global scope) + search button (`#ea2328`), a "Scrape" button + progress bar (`#4ec27f` fill) + "62% · 310/500", and a dark log console (`#1b1d21` bg, `#8ee0a8` mono text).
- "Freshness" card: per-customer freshness chips (STALE = `#c98a12`, FRESH = `#2f8f5b`).

### 5. Reports & Automation (the unified deliverables + jobs hub)
- Two-col grid (1.15fr / 0.85fr).
- **Deliverables** list: each item = icon tile + title + meta ("report/chart/table · source · time") + a CTA (Export PDF / Open / CSV). Newly generated items animate in (`cuFade`) with a subtle fresh highlight (`#fffafa` bg, `#f3d3d1` border).
- **Automation & jobs**: running job (progress bar + `RUNNING` badge `#e8f6ee`/`#2f8f5b`), STALE freshness badge, SCHEDULED badge, and a dashed "＋ Ask the assistant to schedule a refresh" button that primes the assistant.

### 6. Assistant panel (persistent, right)
- Header: red dot + "Assistant" (Archivo 14px) + scope badge ("scope: `<first word of customer>`", mono, `#f4f2ec` pill) + collapse chevron.
- **Starter chips** (wrap): "Morning briefing", "What's new?", "Open P1s", "Generate report". Chip = `#f4f2ec` bg, `#e4dfd4` border, `border-radius:20px`; hover border+text `#ea2328`.
- **Message list**: user bubbles left-aligned (`#f4f2ec`, radius `11px 11px 11px 3px`); agent bubbles right-aligned (white + `#eee9df` border, radius `11px 11px 3px 11px`). Agent bubbles may include: a **tool-call trace** line (mono 10px `#ea2328`, "▸ `<tool_name>`"), an optional inline chart, and an optional **action button** ("Generate report", outlined `#ea2328`, hover fills).
- **Typing indicator**: three `#ea2328` dots, `cuBlink` staggered animation.
- **Composer**: pill input ("Ask about `<customer>`…") + round red send button (↑). Enter submits.

## Interactions & behavior
- **Tab switch**: `setTab(id)` swaps canvas content; top-bar active style updates.
- **Select customer** (top bar dropdown OR clicking an org bar/row): `pickCustomer(name)` sets global `customer`, closes the menu, clears `filter`; all scoped views + assistant scope/placeholder update.
- **Prime assistant** (`primeFrom(text)`): opens the panel if collapsed and submits `text` as if the user typed it. Triggered by KPI cards, org bars, degraded node, schedule button.
- **Send message** (`send` / Enter, or a starter's `run`): appends the user bubble, shows the typing indicator, then calls **`window.claude.complete`** (model `claude-sonnet-4-5`) with the conversation history, a scope-aware system prompt, and three in-page **client tools** — `query_tickets`, `get_customer_health_score`, `generate_customer_report` — whose `run` handlers read the local `DATA`/`TICKETS` mocks. The tool names actually invoked drive the "▸ `<tool>`" trace; the final text is revealed with a typewriter effect (`streamIn`, blinking caret while `streaming`). If `window.claude` is unavailable (opened outside the host), it falls back to `cannedAgent` keyword replies so the prototype still runs offline. **In production: replace `window.claude.complete` + the three mock tools with the real backend agent** (the existing `apps/strabo`/`apps/corax` tool set) and stream real tokens/tool events; the tool names/shapes here are the intended contract.
- **Generate report** (action button `genReport`): prepend a new report to the Reports list (fresh-highlight + `cuFade`), switch to the Reports tab, and post an agent confirmation. In production: call the existing `generate_customer_report` tool, persist to the assets/reports collection, then navigate.
- **Collapse/expand panel**: `togglePanel()`.
- Animations: `cuFade` (0.2–0.3s ease) on new messages/reports; `cuBlink` (1s infinite, 0/0.2/0.4s stagger) on typing dots. Hover transitions ~0.12s.

## State management
Prototype state (recreate with the framework's equivalent; server-side state in NiceGUI, store/signals in an SPA):
- `tab` — active tab id (`overview|customers|tickets|data|reports`).
- `tFilter` — `{priority, status, q}` for the Tickets table; `goTickets(priority,status)` sets tab+filters, `toggleTF(key,val)` toggles a chip, `setTQ` updates search.
- `customer` — global scope (drives all scoped data).
- `panelOpen` — assistant panel visibility.
- `custMenu` — customer dropdown open.
- `input` — composer text.
- `thinking` — agent working (typing indicator).

- `messages[]` — `{id, role:'user'|'agent', text, tool?, chart?, action?, streaming?}` (`streaming` drives the typewriter caret; `streamIn` reveals text progressively).
- `reports[]` — `{id, icon, bg, title, meta, cta, fresh?}`.
Data fetching: replace the static `DATA` map, the `TICKETS` array, + canned replies with real calls to the shared pipeline/agent tools already in `apps/strabo` and `apps/corax` (query_tickets, get_customer_health_score, generate_customer_report, scrape, freshness, rank_portfolio, etc.). Conversation history should read/write the same `chat.history` store both apps use today, keyed by customer.

## Design tokens
**Colors**
- Canvas bg `#ece8e1`; card bg `#ffffff`; card border `#e0dbd0` / `#eee9df` / `#e4dfd4`.
- Dark surfaces: top bar `#141518`, dropdown/console `#1b1d21`, chips-in-dark `#26282d`, dark border `#34373d`.
- Brand red (primary/accent): `#ea2328`; hover `#c9201d` / `#b81a1e`; tints `#f28b82`, `#f6b8b3`, `#fdecec`, `#fffafa`, `#f3d3d1`.
- Text: primary `#1b1d21`, secondary `#3c4046` / `#4a4e54` / `#6b6f76`, muted `#9a9ea6`, on-dark `#c9ccd2` / `#8a8f98`.
- Status: healthy/green `#2f8f5b` / `#4ec27f` / `#e8f6ee` / `#8ee0a8`; warn/amber `#c98a12` / `#fb8c00` / `#fdf1e3`; danger `#e53935` / `#c02620`; scheduled `#5b6572` / `#eef1f5`.
- Cluster tiles: healthy `#eef3fb` bg / `#bcd4f0` border; degraded `#fdeaea` / `#f0bcbc`.

**Typography**
- Display/UI headings: **Archivo** (500/600/700/800). Body/UI: **IBM Plex Sans** (400/500/600). Mono/labels/traces: **IBM Plex Mono** (400/500).
- Scale: KPI value 30px/800; screen title 22px/800; card heading 13px/700; body 13px; meta/label 10–12px; mono labels often uppercase, letter-spacing ~.06em.

**Radii**: 5px (logo), 7px (small cards/tiles), 8–9px (fields/list items), 11px (KPI/content cards), 20–22px (pills/chips/inputs), 50% (dots/send button). **Shadow**: dropdown `0 12px 40px rgba(0,0,0,.5)`. **Spacing**: 8/9/11/12/14/16/24/28px rhythm.

## Assets
No external image assets. Logo is a CSS square + letter. Icons are Unicode glyphs (🔍 ▸ ● → ↑ ⟨ ⟩ ＋) and emoji tiles (📄 📊 ▤) — **replace with the codebase's real icon set** (and Couchbase's real logo/brand) on implementation. Fonts load from Google Fonts; swap to the app's font pipeline if it self-hosts.

## Files
- `Cursus Unified.dc.html` — the interactive prototype (this design). Layout is in the template; all state/behavior/sample data is in the `Component` logic class near the bottom (`renderVals()` + the `overview/customersTab/dataTab/reportsTab` builders + `reply/genReport/primeFrom` handlers).
- `Dual-Interface Review.dc.html` — the preceding UX review canvas: the full rationale (overlap matrix, per-screen critique of Strabo & Corax, the four tensions, the staged roadmap). Read this for *why* the unified direction was chosen.

### Reference to the current codebase
- `couchbaselabs/Cursus_Agent_Public` @ `scaling` — `apps/strabo/app.py` (dashboard, charts, scraping, assets), `apps/corax/app.py` (chat agent, starters, threads). Reuse their pipeline/agent/tool functions; do not reimplement the agent.
