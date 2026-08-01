# Cursus → Capability Lifecycle — Design Document

**Status:** design draft (v0.1, 2026-08-01). No code. This is the spine document the
phased build hangs from.
**Blueprint:** Jarvis — a system that, on hitting something it can't cover, has a
*repeatable protocol* to acquire → validate → remember → adapt the capability, and knows
when to ask a human. Toolbox → metabolism.

> Companion memory (the reasoning that produced this): `project_capability_lifecycle`,
> `project_coverage_by_design`, `project_sfdc_pinning`, and the Jarvis/observability arc in
> `project_scraper_roadmap`.

---

## 0. The finding, in one paragraph

Everything reduces to **three nouns**: one **substrate** (a typed *declaration record* —
provenance-bound, salience/authority-weighted, verifiable, reconsolidable, with a matchable
key + embedding), one **loop** (Observe → Reason → Command → Reinforce → Adapt), and one
**meta-allocator** (provisions differentiated sub-agents with *subsets* of declarations,
short-circuits by matching against the resident set, adapts by reconsolidating them).
Sources, methods, and rationale are the *same declaration primitive* instantiated three
times. Every hard problem in the system is the **same tension** — expressive enough to cover
real cases vs. constrained enough to verify/match — so solving it once, in the declaration
contract, pays off three times. That contract is the first artifact.

---

## 1. Where we are today (honest baseline)

| Asset | What it is | Role in the target |
|---|---|---|
| **Cursus** (this repo, v2.7.80) | 46 MCP tools + 70 agent-tool registry entries; support/SFDC/topology/report domain; CB as data+vector+FTS backend | Domain **Source** + **Method** provider; the first real workload |
| CB collections `transcripts.*` | tickets, snapshots, assets, brands, **markers**, **insights**, accounts, opportunities, pins | markers = Observe substrate; insights/feedback = Reinforce substrate; the rest = domain data Sources |
| `call_llm_with_tools` | the agentic tool-calling loop (claude/gemini/openai/ollama/lmstudio/bedrock) | the runtime the loop executes inside |
| Memory system (`MEMORY.md` + files) | hand-maintained resident index → retrievable store | the **primitive** of the resident salience-set + cue store |
| Skills (`publish-report`, `portfolio-account-status`, `ae-support-sync`) | packaged procedures | early **Methods** |
| **Official CB MCP** — `couchbase/mcp-server-couchbase` (v1.0, Enterprise) | CRUD, N1QL, health, index-advisor, 7 query-perf tools; read-only mode, elicitation, OAuth 2.1, STDIO+HTTP. Enterprise **AI Data Plane** ships **Couchbase Agent Memory** + **Couchbase Agent Catalog** | Data/query/health **Source**; **Agent Memory/Catalog are candidate substrate products** (see §5) |
| **Chris Ahrendt's Extended MCP** — `celticht32/MCP-Couchbase` | **167 admin tools / 17 categories** (bucket, RBAC, nodes, rebalance, failover, XDCR, GSI, FTS, Eventing, Analytics, Backup, encryption, Capella v4, 8.x vector indexes), MIT, preserves official safety primitives | Cluster-admin **Source/Method** provider — huge real-time coverage of the Couchbase authority |
| **Chris's Couchbase Skills** — `celticht32/Couchbase-Skills-for-Claude.ai` | 20+ CB skills (sizing, FTS, security-hardening, observability, migration, …) + his own pattern: "core set loaded + dynamically load individual ones + a skill to manage skills + skills per sub-agent" | Pre-built **Methods**; his loader pattern is a working sketch of the **meta-allocator** |

**Read:** we already have three MCP surfaces (Cursus domain, official CB data, Chris's CB
admin), a memory primitive, a skill library, and — via the AI Data Plane — Couchbase-native
memory + catalog products. The target is not built from scratch; it is **composed** from
these under one declaration contract + loop + allocator.

---

## 2. The substrate — the Declaration record contract (the spine)

A single typed record. Everything registers as one.

```
Declaration {
  id
  kind            // "source" | "method" | "principle"     (the three vocabularies)
  key             // the MATCHABLE class key (stable, structured — not prose)
  authority       // which system-of-record this speaks for (SFDC | Supportal | CB-cluster | DockerHub-GA | Zendesk | derived)
  channel         // "persisted" | "realtime" | "both"      (how it's satisfied)
  covers[]        // fact-classes / entities this declaration is responsible for
  contract        // typed I/O: for source = retrieve(query, fact_classes)->candidates;
                  //            for method = compose(primitives)->action w/ pre/postconditions;
                  //            for principle = {verdict_bias, basis[], when-it-applies}
  authority_weight// how strongly it wins vs other declarations for the same fact-class
  provenance      // links, not restated content
  salience        // decayed weight; refreshes on use  (drives residency + retrieval)
  outcome_stats   // times applied / validated / contradicted  (Reinforce feedback)
  supersedes      // pointer to prior version it revised  (reconsolidation chain, not append)
  embedding       // vector of key+covers+contract  (cue-based retrieval)
  status          // "candidate" | "shadow" | "enforced"  (staged autonomy)
}
```

**Why one record for three things:** a *source* declares coverage; a *method* declares a
composable action; a *principle* declares a rationale ("why we prune/promote"). All three
must be **matchable** (retrieve the relevant few), **verifiable** (checked against the
contract/grammar), **authority-weighted** (system-of-record beats cache — this is how the
faithful-mirror rule survives inside fusion), **salience-ranked** (kept in focus without
holding everything), and **reconsolidable** (edited in place, growth O(classes) not
O(instances)). The contract is where the one recurring tension — expressive vs. verifiable —
is resolved once.

**Fusion rule (critical):** when multiple declarations answer the same fact-class, RRF fuses
their candidate lists **weighted by `authority_weight` + `salience`**, never by rank alone —
so a stale mirror can never silently outrank live SFDC.

---

## 3. The loop (maps to what exists)

| Phase | Does | Built on today |
|---|---|---|
| **Observe** | senses gaps (uncovered-axis), failures, drift, corrections, **novelty** | `markers` collection, `check_data_freshness`, freshness markers — *novelty detection is the new piece* |
| **Reason** (spine) | classifies the signal, checks coverage (derived map), formulates the ask; routes to one of 4 branches | the coverage map derives from registered Source declarations |
| **Command** | acts. **Known → deterministic** (invoke registered tool/skill). **Unknown → dynamic** (bounded tile synthesis: decompose→generate→compose→prune→promote, composing *method primitives*) | MCP tool calls (Cursus/official/Chris) = the deterministic branch; the dynamic branch is new and gated |
| **Reinforce** | evaluates outcome (feedback / judge-quorum / precedent-match); promotes candidate→validated | `feedback`/`insights` collections + planned validation gate |
| **Adapt** | writes learnings **as data** (new declaration row, revised principle, extended coverage) — staged candidate→shadow→enforce | guardrails-as-data; MEMORY.md is the manual precedent |

**Bidirectional questioning = a routing decision.** Every gap resolves to: known tool → act;
generatable method → compose; another agent → delegate; **the human → ask**. "Tony asks
Jarvis / Jarvis asks Tony" is branches 1 and 4. Branch choice = f(authority, confidence,
reversibility) = earn-autonomy-incrementally.

**Practicality: bounded tiles + short-circuits, not deep recursion.** Depth capped, breadth
composes, **satisfice** (stop at desired outcome). Most candidate tiles die/graduate on cheap
signals (source-data + inferred-outcome + guardrail); only survivors get expensive reasoning.
Each loop yields a **reasoning tile** `{partial result, verdict, WHY, provenance, cost}`; the
outcome is a mosaic of promoted tiles. **Remember the WHY** — stored rationale is what lets
the next loop short-circuit; the system is **self-amortizing** (cost per decision falls as
principle-memory grows). This is the scaling answer.

---

## 4. The meta-allocator (not a swarm)

Intelligence is in **provisioning differentiated agents**, not emergent crowd behavior. The
allocator decides, per task: *is this worth a loop at all?* → *which sub-agent(s), imbued with
which declarations (data + method + guardrails)?* → *spawn/command or just observe?* Chris's
skill-loader pattern ("core set + dynamic load + skill-to-manage-skills + per-sub-agent
skills") is a working sketch of exactly this — **imbuing = declaration-subset selection.**

Two open tensions (both the same expressive-vs-verifiable shape):
- **Imbuing calibration** — too many declarations per agent = expensive/unfocused; too few =
  wrong. Right-sizing is the allocator's hardest job, a per-task coverage+authority decision.
- **Rationale as matchable pattern** — a stored "why" must be *comparable* to drive a
  short-circuit; structured key + embedding, not prose.

---

## 5. Protocol & infrastructure recommendations

### 5.1 MCP vs A2A — complementary layers, not either/or
- **MCP = the capability/data plane.** Keep it. Cursus, official CB, and Chris's Extended all
  already speak MCP; each is a Source/Method provider. MCP is client-portable (satisfies
  "nothing load-bearing requires Claude"), supports STDIO + Streamable HTTP + OAuth 2.1.
- **A2A = the coordination plane** (agent↔agent: the allocator delegating to imbued
  sub-agents). **Recommendation: do NOT adopt A2A yet.** Start the allocator as an
  *in-process orchestrator* over subagents (deterministic, already available). Adopt A2A only
  when coordination must cross process/vendor boundaries. Premature A2A is itself a point-fix.
- Net: **MCP now (data/capability), in-process orchestration for the allocator, A2A later** if
  interop demands it.

### 5.2 Storage — Couchbase-only, and evaluate the native products first
Consistent with the standing CB-only decision (no Postgres/ClickHouse/SQLite):
- **Declarations** → a new CB collection (or `sources`/`methods`/`principles`), vector-indexed
  for cue retrieval, FTS for keyword, N1QL for structured coverage queries.
- **Resident salience-set** → small hot subset, cheap to load each loop (the MEMORY.md idea,
  made queryable + decayed).
- **`declaration_history`** → supersession audit (cold), so reconsolidation is reversible.
- **Reinforce signals** → reuse `markers` / `insights` / `feedback`.
- **Traces/provenance** → the OTel-in-CB plan already recorded (traces/spans collections).
- **EVALUATE FIRST: Couchbase Agent Catalog + Agent Memory** (official MCP / AI Data Plane).
  Agent Catalog is a versioned tool/prompt registry in CB; Agent Memory is agent memory in CB.
  These map directly onto the declaration store + method registry + rationale memory. If they
  fit, we adopt them as substrate (dogfooding + no reinvention) rather than hand-rolling
  collections. **First technical spike of the build.**

### 5.3 Agents
- **Differentiated specialist sub-agents**, provisioned by the allocator — NOT a homogeneous
  swarm. Bounded, budget-capped fan-out. Fast-path/slow-path model tiering: cheap local model
  (LMStudio/Ollama) for short-circuit gates + salience matching; frontier model (Claude) for
  slow-path deliberation + generative reconsolidation.

### 5.4 Existing MCP surfaces as the first registered Sources
Register, don't rebuild: **Cursus** (support/SFDC/topology domain), **official CB MCP**
(data/query/health, safety-reviewed), **Chris's Extended MCP** (167 admin tools — cluster
authority), **Chris's Skills** (pre-built Methods). Each becomes Declaration rows on day one.

---

## 6. Phased plan

Each phase is bounded, delivers standalone value, and folds into the existing
observability/Jarvis roadmap. **Do not build the whole loop as a reflex.**

- **Phase 0 — this doc + the Declaration contract spec.** Resolve expressive-vs-verifiable
  once: finalize the record, the matchable-key scheme, the fusion rule, the reconsolidation +
  staging rules. *Deliverable: contract spec. No code.*
- **Phase 1 — Sources + coverage map + gap signals** (smallest end-to-end slice; directly
  kills the recurring-gap class). Register existing MCP surfaces as Source declarations; derive
  the coverage map; wire the Reason step to emit uncovered-axis signals. **Spike: does Agent
  Catalog/Memory serve as the store?**
- **Phase 2 — Rationale/why layer.** Emit structured `principle` declarations per decision;
  resident salience-set injection + cue retrieval; start log-only, then gated reconsolidation.
- **Phase 3 — Method vocabulary + Command dynamic branch.** Define method primitives (Source
  contract is primitive family #1); bounded tile synthesis with prune/promote; import Chris's
  skills as seed Methods.
- **Phase 4 — Meta-allocator.** Differentiated sub-agent provisioning; imbuing calibration;
  budget-bound fan-out; adopt A2A only if needed.
- **Phase 5 — Loop closure + autonomy staging.** Full Observe→…→Adapt with candidate→shadow→
  enforce gates; human-authority escalation on irreversibles.

---

## 7. Jarvis blueprint checklist (the correctness test)

| Jarvis property | Falls out of |
|---|---|
| Bidirectional questioning | Router's 4 branches (act / compose / delegate / **ask human**) |
| Always-available recall | Salience-resident declarations + cue retrieval |
| Anticipation | Observe/novelty + accumulated *why* enabling proactive short-circuit |
| Graceful degradation | Coverage map + uncovered-axis signals + satisfice |
| Earns autonomy incrementally | Staged reconsolidation / guardrail gates on Adapt |

Nothing in the blueprint requires a mechanism outside {substrate, loop, allocator}. That is
the signal the decomposition is right.

---

## 8. Risks / open questions

1. **The one tension** (expressive vs. verifiable) — if the declaration contract gets it
   wrong, no loop machinery compensates. Phase 0 must nail it.
2. **Agent Catalog/Memory fit** — unknown until the Phase 1 spike; if they don't fit, fall
   back to hand-rolled CB collections (still CB-only).
3. **Reconsolidation drift** — editable memory can lose valid distinctions; guarded by staged
   gates + supersession provenance (propose→shadow→enforce for memory).
4. **Imbuing calibration** — no closed-form answer; likely learned from `outcome_stats` over
   time (the allocator itself gets cheaper as patterns accrue).
5. **Governance / supply-chain** (per Couchbase AI-skills discussion): Methods/skills must be
   vetted; candidate status + human-enforce gate is the control.

---

## 9. Collaboration note

Chris Ahrendt (`celticht32`, Head of CS/PS Americas) has independently built: the Extended
admin MCP (167 tools), a 20+ Couchbase skills library, a migration accelerator, and is already
reasoning about dynamic skill-loading + per-sub-agent skills + "a skill to manage skills" —
i.e. the allocator. Strong candidate collaborator; his admin MCP + skills are direct
first-class registrations in Phase 1/3. (Note the internal governance thread: skills for
customer distribution need SME/InfoSec vetting — respect the candidate→enforce gate.)
