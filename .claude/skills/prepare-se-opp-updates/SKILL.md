---
name: prepare-se-opp-updates
description: Prepare weekly SE-Section opportunity updates for the current SE — a review package of proposed SE Next Steps + validated SE Technical Risk for the opportunities whose SE Section is going stale, so the SE can apply them in Salesforce fast. Use when the user asks to "prep my SE updates", "what SE opps am I behind on", "update my opportunities", "SE section updates", or wants to catch up their weekly Salesforce SE hygiene. v0.1 — SE-Section fields only (activity logging comes later). READ-ONLY: never writes to Salesforce; produces edits for the human to apply.
---

# Prepare SE-Section Opportunity Updates (v0.1)

Every SE must keep the **SE Section** of their opportunities current in Salesforce each week (Next Steps, Technical Risk, POC status…) and everyone falls behind. This skill finds the opportunities going stale and **prepares** the updates from real context — the SE reviews and applies them in Salesforce. It **never writes to Salesforce** (assist tier; a gated write may come later).

**Scope of v0.1:** the **SE-Section fields** only — primarily `SE Next Steps` (drafted) and `SE Technical Risk` (validated). Activity logging (Events/Tasks from meetings) is a later increment that depends on the harvester; do not attempt it here.

## Step 1 — Get the worklist

Call `mcp__cursus__get_se_opp_worklist`:
- Default `window_quarters=3` (CQ+3 — this fiscal quarter + next 3). This is the standing default; only change it if the user asks for a different horizon.
- If the user says they're "behind" / wants the catch-up list, pass `behind_days` (e.g. 7) to return only opps whose SE Section is at least that stale.
- Single opportunity: if the user names one opp, still call the worklist and focus on that row.

The result is your opportunities ranked by **SE Section Days Since Last Update** (`SE_Update_Age__c`), with the current `SE Next Steps` / `SE Technical Risk` and a deep-link per opp. **`SE_Update_Age__c` is SFDC-computed — never propose editing it.** The point is meaningful updates that legitimately reset the clock, not gaming the timestamp.

If the worklist is empty, say so plainly (nothing stale in the window) and stop.

## Step 2 — Gather recent context per opportunity

For each opportunity in scope, pull the account's recent support signals (this is what makes the drafted Next Steps real, not generic). Resolve the account by name:
- `mcp__cursus__get_customer_health(organization=...)` — open P1/P2 counts, active tickets.
- `mcp__cursus__query_tickets(organization=..., status="open")` — the actual open tickets/themes.
- `mcp__cursus__query_cluster_topology(...)` or `mcp__cursus__generate_cluster_health_chart(...)` only if a cluster/technical-risk question is live.

Keep it light — a couple of calls per account. If an account has no local ticket data, note that and draft from the opp's own fields (stage, current Next Steps) rather than inventing signals. **Never fabricate a ticket, number, or event** — every claim in a drafted update must trace to a tool result.

## Step 3 — Prepare the two SE-Section fields

Per opportunity, produce a proposed update:

- **SE Next Steps** (`SE_Next_Steps__c`) — DRAFT concrete next steps from: the current Next Steps (what was planned), the sales stage, and the open tickets/signals from Step 2. One to three specific, dated-where-possible actions. Make it something the SE would actually paste.
- **SE Technical Risk** (`SE_Technical_Risk__c`) — VALIDATE, don't assert: show the current value, then a *suggested* value/annotation derived from signals (e.g. open P1s or degraded cluster health → elevated risk; clean → low). Frame it as "confirm/adjust", because risk is a judgment call.

Do NOT touch the SFDC-computed rollups (SE Section Days Since Last Update, POC Days Open, the "days"/"Last Updated" fields), and in v0.1 leave the other SE-Section fields (SDK type, POC dates, Tech Win, etc.) alone unless the user asks — keep the output focused and trustworthy.

## Step 4 — Present the review package

Render one block per opportunity, ranked by staleness (most stale first):

```
### <Account> — <Opportunity>   ·   stale <N> days   ·   <stage>, close <date>
Open <SFDC link>

SE Next Steps
  current:  <current or "(empty)">
  proposed: <drafted next steps>
  because:  <the tickets/signals that justify it>

SE Technical Risk
  current:  <current or "(empty)">
  suggested: <value/annotation>  — confirm or adjust
```

End with a one-line summary: how many opps are in the list, how many are ≥7 days stale, and the single most-overdue one to do first.

## Step 5 (optional) — Apply, only on explicit confirmation (gated write)

By default this skill is **prepare-only**: hand the human the values + deep-links and let them apply in Salesforce. A gated write exists but is **opt-in and confirmation-required**:

- Only if the user **explicitly asks to apply/write** the updates (e.g. "go ahead and update it", "apply these"), use `mcp__cursus__apply_se_opp_updates`.
- **Always dry-run first**: call it with `dry_run=true` (the default) and show the plan (current → proposed) for the specific opp.
- **Get explicit confirmation of the exact values** from the user, then — and only then — call again with `dry_run=false` for that one opp. One opportunity at a time; never batch-write without per-opp confirmation.
- Only SE-Section whitelist fields can be written (the tool rejects anything else, including computed rollups). If the user asks to write a non-writable field, explain it's not permitted.
- After a write, confirm what changed (the tool returns the applied diff + audit status).

Do NOT write on your own initiative. Prepare-and-hand-off is the default; writing happens only when the user asks and confirms the values.

## Rules

- **Prepare-first, write only on explicit confirmation.** Default output is edits for the human to apply via the deep-link. The gated write (Step 5) fires only when the user asks to apply AND confirms the exact values, dry-run first, one opp at a time.
- **No fabrication.** Every drafted value must be grounded in a tool result; where context is thin, say so and keep the draft conservative.
- **Meaningful, not clock-gaming.** Prepare substantive updates; don't propose trivial edits whose only purpose is resetting `SE_Update_Age__c`.
- **Per-caller.** The worklist is scoped to whoever is running it (their `sfdc_user_id`/`sfdc_user_name` in settings) — never hardcode a person or a saved report.
