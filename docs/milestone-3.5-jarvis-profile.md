# Milestone 3.5 — JARVIS Profile & Proactive Briefing

**Goal:** The agent becomes ambient-aware. It knows who the user cares about before being asked, surfaces urgent changes autonomously, and notifies the user of critical events without waiting for a question.

This milestone bridges Phase 3 (fleet data) and Phase 4 (AI-driven interface) by giving the agent a persistent model of the user's world — which accounts they care about, what "normal" looks like for each, and what has changed since they last looked.

---

## Backlog

| # | Item | Notes |
|---|------|-------|
| 3.5.1 | Customer usage profile in CB | `chat.profiles` collection; `{top_customers: [{name, access_count, last_accessed_at, validated_at}]}` |
| 3.5.2 | Access tracking hook | `_record_customer_access(org)` called from `_execute_agent_tool` on every customer-scoped tool call; background thread |
| 3.5.3 | Profile validation | `_validate_customer_profile()` re-checks top customer names against Analytics LIKE search; marks `is_valid=false` on misses; runs max once per 24h |
| 3.5.4 | Top customers injected into system prompt | On every agent turn, top 5 customer names from profile prepended to system prompt so agent knows them without being asked |
| 3.5.5 | Session startup briefing card | On Chat tab open (once per session), auto-run lightweight health check across top 5 customers; render as collapsible card above chat |
| 3.5.6 | `get_briefing` agent tool | Agent-callable version: loads top customers, runs health + P1 check + staleness; returns formatted briefing the agent can narrate |
| 3.5.7 | Proactive alert timer | `ui.timer` at 15-min interval; compares P1 count + health score vs. last snapshot; `ui.notify` + alert chip in chat on change |
| 3.5.8 | Alert thresholds config | Per-user configurable: new_p1 (default: any), score_drop (default: >10pts), data_stale_hours (default: 12h) |
| 3.5.9 | "Start Briefing" button in chat empty state | Manual trigger for the briefing in case auto-run is disabled or the user wants a refresh mid-session |

---

## Design decisions (defaults)

| Decision | Default | Rationale |
|---|---|---|
| Profile storage | `chat.profiles` CB collection | Separate from `chat.users`; keeps auth and preference data distinct |
| Briefing trigger | Auto on Chat tab open, once per session | Low friction; user can dismiss; button available for manual re-run |
| Staleness handling | Report + flag, no auto-scrape | Briefing is read-only; agent can suggest/call `fetch_fresh_data` if needed |
| Alert threshold | New P1, score drop >10pts, data >12h stale | Tunable; errs toward signal over noise |
| Profile top-N | Top 5 by `(access_count × recency_weight)` | Recency weight = `1 / (1 + days_since_access)` |
| Validation cadence | Once per 24h per profile | Avoids hammering Analytics on every session |

---

## Data model

```json
// key: profile::{username}  in chat.profiles
{
  "username": "agonyou",
  "top_customers": [
    {
      "name": "NetDocuments Inc",
      "access_count": 42,
      "last_accessed_at": 1748600000,
      "validated_at": 1748600000,
      "is_valid": true
    }
  ],
  "alert_thresholds": {
    "new_p1": true,
    "score_drop_pts": 10,
    "stale_hours": 12
  },
  "last_validated_at": 1748600000,
  "updated_at": 1748600000
}
```

---

## Acceptance criteria

- Opening the Chat tab for the first time in a session shows a collapsed briefing card within 5 seconds listing top customers with current P1 count, health score, and data age.
- The agent, asked "what should I know today?", calls `get_briefing` and returns a narrated summary without the user naming any customer.
- A new P1 ticket for a top customer triggers a `ui.notify` toast and an alert chip in the chat within 15 minutes.
- Profile tracks access frequency correctly: repeatedly querying "Amex" in the chat moves it toward the top of the profile list.
- Customer names in the profile are validated against Analytics at most once per 24h; invalid names are flagged and excluded from the briefing.

---

## Dependencies

- Requires CB connection to be configured (gracefully degrades — no profile = no briefing, no tracking).
- 3.5.7 (alert timer) depends on 3.5.1–3.5.5 being complete.
- `get_briefing` calls `_compute_health_score` (already exists, v1.6.0).
- Briefing staleness check uses `last_scraped_at` epoch field (on all tickets since v1.1.0).
