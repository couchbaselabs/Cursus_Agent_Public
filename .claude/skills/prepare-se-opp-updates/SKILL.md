---
name: prepare-se-opp-updates
description: Prepare weekly SE-Section opportunity updates for the current SE — a prepared-updates table (Account | Opportunity | Opp ID | Last Update | Proposed Next Steps | Justification, with Next Steps and Justification as separate columns and the current on-file value reiterated as a reality check) plus a risk table (Account | Opportunity | Opp ID | Stage | Close | Current Risk | Recommended Risk | Why) for the opportunities whose SE Section is going stale, so the SE can apply them in Salesforce fast. Use when the user asks to "prep my SE updates", "what SE opps am I behind on", "update my opportunities", "SE section updates", or wants to catch up their weekly Salesforce SE hygiene. v0.2 — SE-Section fields only (activity logging comes later). Prepare-first; optional gated per-opp write on explicit confirmation.
---

# Prepare SE-Section Opportunity Updates (v0.2)

Every SE must keep the **SE Section** of their opportunities current in Salesforce each week (Next Steps, Technical Risk, POC status…) and everyone falls behind. This skill finds the opportunities going stale and **prepares** the updates from real context — the SE reviews and applies them in Salesforce. It is **prepare-first**: by default it writes nothing; an **opt-in, per-opp gated write** (Step 5) applies a confirmed value only when you explicitly ask.

**Scope of v0.2:** the **SE-Section fields** only — primarily `SE Next Steps` (drafted) and `SE Technical Risk` (validated). Activity logging (Events/Tasks from meetings) is a later increment that depends on the harvester; do not attempt it here.

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

## Step 3 — Prepare the two SE-Section fields (CODIFIED FORMAT)

Per opportunity, produce a proposed update in **exactly** these two shapes.

### 3a — Reality-check against the last update FIRST

`SE_Next_Steps__c` is an **append-log**: SEs stack dated entries newest-first and never overwrite. The worklist surfaces the **latest entry** (plus a count of prior entries) in its "Current SE Next Steps — latest entry (reality check)" block, with the last-updated date. Before drafting anything, **restate that latest entry** and anchor the new draft to it:
- The new draft must be **consistent with and advance from** what's actually on file — it continues the story, it doesn't contradict or silently discard the prior state. If your Step-2 signals conflict with the last entry, that conflict is itself worth surfacing (something changed), not papering over.
- **The prepared value PREPENDS a new dated line above the existing log — it never replaces it.** When presenting (and if ever applying) the update, preserve the prior entries; overwriting the field would destroy the note history.
- **If none exists on file** (empty / "(none on file)"), there's no baseline to check against — that's fine; draft fresh from Step 2, and the rest of the logic proceeds normally.

### 3b — SE Next Steps (`SE_Next_Steps__c`) — the dated narrative note

DRAFT a concrete update from the last update (3a), the sales stage, and the signals from Step 2. **Every drafted note follows this template verbatim:**

```
<YYYY-MM-DD> <SE-initials>: <2–5 sentence narrative grounded in real signals — what
moved this week, who's involved, what the state is>. Next: <1–3 specific, named actions>.
```

- **Date** = today (ISO). **Initials** = the running SE's initials (derive from `sfdc_user_name` in settings — e.g. "Austin Gonyou" → `AG`). This mirrors how SEs hand-stamp the field, so a human can paste it as-is.
- The narrative must name real people, dates, ticket/meeting references from Step 2 — **never generic filler**. If context is thin, say so plainly and keep it short rather than padding.
- **Always end with a `Next:` clause** — the forward actions are the point of the field. One to three, specific and named ("push Travis's team for TSO sign-off", not "follow up").
- **Meaningful, not clock-gaming** — the note must describe a real change; do not draft a cosmetic edit whose only purpose is resetting `SE_Update_Age__c`. If nothing genuinely moved, say the opp is **genuinely stalled** (see Step 4) rather than manufacturing an update.
- **Keep the forward actions and the evidence separable** — the `Next:` clause is the actions; the evidence/reasoning that justifies them is presented in its own column (Step 4a), not fused into one blob.

### 3c — Shared-account motion (group, don't repeat)

When several opps sit on **one account motion** (e.g. NetDocuments' 4 opps all driven by one POC thread, GoDaddy's 6 on one biweekly-sync motion), write the shared context **once** as a short account preamble, then per-opp notes that carry only that opp's **delta** (its specific phase/scope/next-step). Don't paste the same paragraph into every opp — the per-opp `Next:` is where they diverge.

### 3d — SE Technical Risk (`SE_Technical_Risk__c`) — assessed, not asserted

Recommend a value for **every** opp — including resolving every opp that currently shows `—`/empty to a real assessed value (`Low`/`Medium`/`High`). Risk can move in **either direction** on evidence:
- **Down** when signals de-risk it: a logged Technical Win, a clean cluster, verbal/procurement reached.
- **Up** when signals elevate it: open P1s, an unresolved competitive comparison ("stonewalled"), or **no activity trace for months** (a stalled opp is *higher* risk than a merely stale one, not lower).
- **Unchanged** is a valid recommendation — state it explicitly with the reason.
Every recommendation needs a one-line evidence-based **Why**. Risk is a judgment call, so frame it as "confirm/adjust", but always commit to a recommended value.

Do NOT touch the SFDC-computed rollups (SE Section Days Since Last Update, POC Days Open, any "days"/"Last Updated" field). In v0.2 leave the other SE-Section fields (SDK type, POC dates, Tech Win, etc.) alone unless the user asks.

## Step 4 — Present the review package (CODIFIED OUTPUT)

Two artifacts, in this order.

### 4a — The prepared-updates table (FIXED COLUMNS, in this order)

Group by account (shared-motion preamble once per Step 3c), most-stale account/opp first. Render this exact table — **Next Steps and Justification are separate columns**, and the **Last Update** column reiterates the current on-file value as the reality-check:

```
| Account | Opportunity | Opp ID | Last Update (current) | Proposed Next Steps | Justification |
```

- **Last Update (current)** = the untruncated current `SE_Next_Steps__c` from the worklist's full-text reality-check block, condensed to its gist + the last-updated date (e.g. "6/22: right-sized env, awaiting…"). If none on file, put `— (none on file)` — that's the "no baseline, draft fresh" case, and the rest of the row proceeds normally.
- **Proposed Next Steps** = the dated note from Step 3b — the value to paste into `SE_Next_Steps__c` (narrative + `Next:` actions).
- **Justification** = the evidence/reasoning that grounds the proposed step — the tickets, meetings, wins, signals from Step 2 (e.g. "7/31 PATH mobile rebuy win; biweekly sync 8/18"). This is the "because", kept in its own column, **never** fused into the Next Steps blob.
- **Opp ID** = the 18-char SFDC Id straight from the worklist's `Opp ID` column. Never infer or fabricate an Id.

### 4b — The consolidated risk table (FIXED COLUMNS, in this order)

Always render this exact table too — one row per opp in scope, same order as 4a:

```
| Account | Opportunity | Opp ID | Stage | Close | Current Risk | Recommended Risk | Why |
```

- **Opp ID** = same as 4a (the deep-link is `https://couchbase.my.salesforce.com/<id>`); if an opp wasn't in the worklist, cross-check it via `lookup_sfdc_account` before putting an Id in the table.
- **Current Risk** = the live `SE_Technical_Risk__c` value (`—` if empty). **Recommended Risk** = your Step 3d assessment. **Why** = the one-line evidence.
- When Recommended ≠ Current, that delta is the actionable signal — surface the notable ones (biggest risk increases, especially newly-`—`→High) as one or two explicit call-outs beneath the table, not buried in a row.

End with a one-line summary: how many opps, how many are ≥7 days stale, the single most-overdue to do first, and any opp flagged **genuinely stalled** (real deal risk, not just update-hygiene) that needs a human check-in rather than a note.

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
