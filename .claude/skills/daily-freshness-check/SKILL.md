---
name: daily-freshness-check
description: Run the daily support-data freshness routine across the tracked accounts — the correct, status-aware order. Use for "run the daily check", "morning freshness run", "monitor accounts", "refresh my accounts and brief me", or the scheduled daily monitor. Gates on smart_refresh (status-aware, self-healing), NOT check_data_freshness (which is presence-only and misses status drift).
---

# Daily Freshness Check (corrected order)

The daily monitor's job: ensure each tracked account's local ticket data is current, then brief. The **order and the freshness signal matter** — a prior version gated on `check_data_freshness`, which is **presence-only** (it detects missing ticket IDs, not status changes), so accounts whose tickets drifted open→solved/archived were reported "fresh" and silently went stale. This routine fixes that.

**Core rule: the freshness gate is `smart_refresh`, not `check_data_freshness`.** `smart_refresh` diffs the live Supportal listing (which exposes status/solved/priority) on six signals — new, status change, solved change, priority escalation, stub, stale-open — and **re-pulls what changed**. `check_data_freshness` only catches the narrow "snapshot-referenced ticket missing entirely" case and must never be the gate.

## Step 1 — Connectivity first (bail cleanly if down)

Call `mcp__cursus__check_connectivity`. If Supportal is unreachable (VPN/GlobalProtect down), **stop** and report that — do NOT run freshness checks or mark anything, because a failed live read must not be recorded as "no changes." Say the VPN needs connecting and end.

## Step 2 — Determine the account list

The tracked accounts are the user's book (their pinned/active accounts). Use the accounts the user names, or `mcp__cursus__list_sfdc_accounts` / their pinned set. Don't invent a list.

## Step 3 — Per account: smart_refresh (the gate)

For each account, call `mcp__cursus__smart_refresh(organization=...)`. This is the status-aware gate AND the fix in one call — it detects and re-pulls new + status/solved/priority-changed + stale-open tickets, and kicks off background enrichment (embed/score/SFDC) on them. Record per account what it reported changed (new count, status-changed count, etc.).

- If `smart_refresh` reports a large stub count or a big gap (e.g. many tickets never fully fetched), THEN escalate that one account to a full `mcp__cursus__rescrape_customer_tickets` — but only on that signal, not by default.
- Keep it lightweight otherwise; smart_refresh is designed to touch only what changed.

## Step 4 — (Optional, informational) presence check

Only if you specifically want the "any snapshot-referenced tickets missing entirely?" signal, call `mcp__cursus__check_data_freshness(organization=...)`. Treat its result as **informational, never as the gate** — a `fresh` result there does not mean statuses are current (its own output now says so). Skip this step unless there's a reason to run it.

## Step 5 — Confirm the refresh landed (cross-process safe)

If smart_refresh started a background job, confirm it via `mcp__cursus__wait_for_scrape(job_id=...)` or `mcp__cursus__get_scrape_status(job_id=...)`. These now fall back to the shared Couchbase job record, so a job started on one MCP process is visible even if a different process answers — a prior "no jobs tracked" error was that cross-process gap. If a job legitimately can't be confirmed, say so rather than assuming success.

## Step 6 — Record and brief

- Call `mcp__cursus__record_automation_run` with the outcome (accounts checked, per-account changes, any escalations/errors) so the daily run is auditable.
- Then produce the briefing from the now-current data (`mcp__cursus__get_morning_briefing` / `get_portfolio_status`), noting for each account what changed today.

## Rules

- **smart_refresh is the gate. Never gate on check_data_freshness.**
- **Connectivity failure ≠ fresh.** If the live read fails, report it and stop; do not mark data fresh or record "no changes."
- **Escalate to full rescrape only on a real signal** (stub/gap), not by default — smart_refresh handles the common case.
- **Confirm background jobs** via the cross-process-safe status tools before claiming the refresh completed.
- **No fabrication.** Report exactly what the tools returned per account; if a step failed, say which and why.
