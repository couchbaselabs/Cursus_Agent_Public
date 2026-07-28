---
name: portfolio-account-status
description: Generate and publish a Couchbase-branded "Portfolio Account Status" report — a per-account table of AE/TSE, open opportunities, ARR, and support ticket load across all of Austin Gonyou's confirmed Salesforce accounts. Use when the user asks for a "portfolio report", "account status report", "my accounts", "my opportunities", or wants a refreshed view of their SFDC book of business alongside support health. Always re-verify the account list per the process below rather than reusing a previously-confirmed list from memory, since SFDC data changes.
---

# Portfolio Account Status

Produce a single-page HTML report covering every account where Austin is genuinely the SE, cross-referenced against support ticket health. The hard part of this skill isn't formatting — it's getting the account list right, because the SFDC data synced into Couchbase has repeatedly been wrong in ways that looked authoritative. Treat every SE-name field with suspicion until cross-checked.

## Step 1: Determine the confirmed account list

Query all three of these — they draw on different SFDC fields and have disagreed with each other before:

1. `mcp__cursus__get_my_sfdc_accounts` — opportunity-level, pre-filtered to the saved `sfdc_user_name`
2. `mcp__cursus__list_sfdc_accounts(se_name="Austin Gonyou")` — account-level "SE" field (this is a *different* field from `Primary_SE__c` — an account-team role assignment, not the opportunity's SE Primary — so it legitimately disagreeing with #1 for one account isn't automatically a bug)
3. `mcp__cursus__get_account_opportunities(organization=...)` per candidate account — opportunity-level `Primary_SE__c`, spelled out per-opportunity

Before trusting any of this, read `/Users/austin.gonyou/.claude/projects/-Users-austin-gonyou-Downloads-Apps/memory/feedback_sfdc_se_field_unreliable.md` — it names specific accounts already confirmed wrong (as of this writing: Sabre, American Airlines, Paycom — the opportunity-level `Primary_SE__c` field showed "Austin Gonyou" for all three, but live SFDC review showed otherwise, and a `sync_sfdc_data` refresh fixed the account-level field but *not* the opportunity-level one for at least one of them). Don't assume that memory file's list is exhaustive or still accurate — it's a record of what's been caught, not a permanent exclusion list. If an account isn't mentioned there, it still needs the same scrutiny below.

**If the user pushes back on any account** ("I don't think that's me", "check again"), that's a signal worth taking seriously immediately — this has happened before and the tool data was wrong, not the user. Show them the exact tool call and literal output you based the claim on, but don't re-assert it as fact. Ask them to check the live SFDC "SE Opp Primary" panel on that opportunity (Setup on the Opportunity record → "SE Related Information" section) and go with what they report. If it turns out wrong, update the memory file above with the new evidence so the next run of this skill starts smarter.

## Step 2: Gather data per confirmed account

- **AE / TSE**: `mcp__cursus__get_account_contacts(organizations=[...])`. If it returns "org not found" (Supportal's zdorg doesn't have every SFDC account name), fall back to the AE column from `list_sfdc_accounts` and note in the report that the AE came from SFDC directly rather than Supportal.
- **Opportunities + ARR**: `mcp__cursus__get_account_opportunities(organization=...)` — sum ARR for the account's opportunity-value column, list opp names for reference.
- **Lifetime ticket totals**: `mcp__cursus__get_customer_health(organization=...)`.
- **Freshness before reporting ticket data**: `mcp__cursus__check_data_freshness(organization=...)` first; if it reports drift, `mcp__cursus__rescrape_customer_tickets(organization=...)` and give it a beat before reading ticket data for that org.
- **Last TSE/Customer Ticket Activity** (its own report column — see Step 3 for why the naming matters):
  - If the account has open tickets, pull them via `mcp__cursus__get_morning_briefing(organizations=[...], active_only=true)` and take the most recent `last_comment_at` across them — this is high-confidence, label it accordingly.
  - If it has zero open tickets, there's no clean way to find the true most-recent ticket — `query_tickets` and `get_morning_briefing(active_only=false)` do not sort by recency and are capped, so a sample can easily miss the actual latest comment. Report whatever the sample turns up, but label it as an approximation, not a fact.

## Step 3: Two different "last activity" columns — don't conflate them

The report has two separate columns and they measure genuinely different things:

- **"Last TSE/Customer Ticket Activity"** — the ticket-side data from Step 2 above. The people commenting on Zendesk tickets are support engineers (TSEs) and customers — never the AE or SE. This column was mislabeled "Last AE/SE Ticket Activity" once already, which caused real confusion, so keep the naming as-is.
- **"Last AE/SE SFDC Update"** — a distinct thing the user cares about: when did the Account Executive or Sales Engineer last touch this opportunity/account *in Salesforce itself* (Chatter, a notes field, `LastActivityDate`). As of this writing that data does not exist anywhere in the sync: `mcp__couchbase__get_schema_for_collection` on bucket `rag`, scope `transcripts`, collection `opportunities` shows only `ae_name/email`, `se_name/email`, `supporting_se_name/email`, `arr`, `stage`, `close_date`, `products`, `sfdc_opp_id/account_id`, and `last_synced` (the sync's own ingestion timestamp, not an SFDC activity date).

Render this column as "—" / "not synced" for every account rather than quietly substituting the ticket-side date — that substitution is exactly the kind of thing that erodes trust once someone checks it against the real SFDC record. Say plainly that it isn't available in the current sync. Getting this data would mean adding a new field to the scraper's SFDC sync pipeline (a code change to the Scraper project itself, not something achievable through these MCP tools) — mention that as the actual next step if the user wants it badly enough.

## Step 4: Fill the template and publish

The report template lives at `/Users/austin.gonyou/Downloads/Apps/Scraper/docs/templates/portfolio_account_status_template.html` — Couchbase-red branded, with placeholder tokens (`ORG_NAME`, `AE_NAME`, `OPP_COUNT`, etc.) and inline HTML-comment instructions next to each one explaining exactly which tool call it comes from and how to compute it. Read that file before filling it in; don't rebuild the layout from memory.

- Copy the template, replace every placeholder, and repeat the `<!-- ACCOUNT_ROW -->` block once per confirmed account (delete the comment markers once real rows are in).
- Compute the four summary stats (accounts, open opportunities, total opportunity value formatted like "$12.7M", lifetime tickets) from the same verified account list — don't let these drift out of sync with the table rows, which has happened before (a row got removed from the table but the summary count wasn't updated to match).
- Write the filled HTML to disk and read it back to confirm the content actually landed before publishing — never hand-copy report HTML between tool outputs, that's how fields silently go missing.
- Publish with the Artifact tool. If this report has been published before in the current conversation, republish to the *same* file path (or pass the existing artifact's `url` if this is a fresh conversation) so the link stays stable instead of minting a new one each time.
