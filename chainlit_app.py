"""
Supportal Chainlit Chat — professional AI chat sidecar.
Shares all pipeline functions and agent tools with the NiceGUI app.

Run alongside the main app:
    chainlit run chainlit_app.py --port 8766

Then click "Open Chainlit Chat" in the NiceGUI Agent tab.
The NiceGUI app (port 8765) continues running unchanged.
"""

from __future__ import annotations

import asyncio
import base64
import importlib
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

# ── Python 3.14 + nest_asyncio + anyio compatibility patch ───────────────────
# Chainlit's CLI applies nest_asyncio which patches asyncio's event loop in a
# way that breaks sniffio's backend detection under Python 3.14.
# anyio.to_thread.run_sync (used by starlette FileResponse) therefore raises
# NoEventLoopError on every request.  Patching get_async_backend() to fall
# back to asyncio when sniffio fails to detect the running loop fixes this.
def _patch_anyio_backend() -> None:
    try:
        import anyio._core._eventloop as _ael
        _orig = _ael.get_async_backend

        def _safe_get_backend(asynclib_name=None):
            try:
                return _orig(asynclib_name)
            except Exception:
                if asynclib_name is None:
                    try:
                        asyncio.get_running_loop()
                        # There IS a running asyncio loop — sniffio just can't see it
                        return _orig("asyncio")
                    except RuntimeError:
                        pass
                raise

        _ael.get_async_backend = _safe_get_backend
    except Exception as _e:
        print(f"[chainlit] anyio patch skipped: {_e}")

_patch_anyio_backend()

import chainlit as cl
import chainlit.data as cl_data

# ── Pipeline is imported lazily to avoid NiceGUI event-loop side-effects ─────
# Importing supportal_nicegui_app at module level can initialise NiceGUI's
# asyncio state before Chainlit's anyio backend is running, causing
# NoEventLoopError on static file requests.  We load it on first handler call.
sys.path.insert(0, str(Path(__file__).parent))
_pipeline: Any = None


def _get_pipeline():
    global _pipeline
    if _pipeline is None:
        _pipeline = importlib.import_module("supportal_nicegui_app")
    return _pipeline


# ── Scrape-job persistence helpers ───────────────────────────────────────────
def _cb_get_job(job_id: str, cb_url: str, bucket: str, username: str,
                password: str, use_tls: bool, scope: str, collection: str) -> dict | None:
    """Read a single scrape_job doc from CB. Returns None on any error."""
    if not cb_url or not username:
        return None
    try:
        app = _get_pipeline()
        from couchbase.cluster import Cluster as _Cl, ClusterOptions as _CO
        from couchbase.auth import PasswordAuthenticator as _PA
        from datetime import timedelta as _td
        conn = app._cb_conn_str(cb_url, use_tls)
        _c   = _Cl(conn, _CO(_PA(username, password)))
        _c.wait_until_ready(_td(seconds=5))
        doc  = _c.bucket(bucket).scope(scope).collection(collection).get(
            f"scrape_job::{job_id}"
        ).content_as[dict]
        _c.close()
        return doc
    except Exception:
        return None


def _cb_query_active_jobs(cb_url: str, bucket: str, username: str,
                           password: str, use_tls: bool, scope: str,
                           collection: str) -> list[dict]:
    """Return all scrape_job docs that are running or finished within the last 6h."""
    if not cb_url or not username:
        return []
    try:
        import time as _t
        app = _get_pipeline()
        from couchbase.cluster import Cluster as _Cl, ClusterOptions as _CO
        from couchbase.auth import PasswordAuthenticator as _PA
        from couchbase.options import QueryOptions as _QO
        from datetime import timedelta as _td
        conn = app._cb_conn_str(cb_url, use_tls)
        _c   = _Cl(conn, _CO(_PA(username, password)))
        _c.wait_until_ready(_td(seconds=5))
        cutoff = _t.time() - 6 * 3600
        ks = f"`{bucket}`.`{scope}`.`{collection}`"
        rows = list(_c.query(
            f"SELECT j.* FROM {ks} AS j "
            f"WHERE j.type = 'scrape_job' "
            f"AND (j.status = 'running' OR j.started_at > {cutoff}) "
            f"ORDER BY j.started_at DESC LIMIT 20",
            _QO(timeout=_td(seconds=10)),
        ))
        _c.close()
        return rows
    except Exception:
        return []


# ── Couchbase data layer (thread persistence / sidebar) ──────────────────────
def _load_cb_settings() -> dict:
    """Read active profile without importing the full pipeline module."""
    try:
        app = _get_pipeline()
        s = app._load_settings_file()
        active = s.get("__last__", "")
        return s.get(active, {}) if active else {}
    except Exception:
        return {}


def _cb_hist_args(p: dict) -> tuple:
    return (
        p.get("cb_url", "couchbase://localhost"),
        p.get("cb_bucket", "rag"),
        p.get("cb_user", ""),
        p.get("cb_pass", ""),
        p.get("cb_tls", False),
    )


async def _load_shared_history(customer: str, profile: dict) -> list[dict]:
    app = _get_pipeline()
    try:
        return await asyncio.to_thread(
            app.load_customer_chat_history, customer, *_cb_hist_args(profile)
        )
    except Exception:
        return []


async def _save_shared_history(customer: str, history: list[dict], profile: dict) -> None:
    app = _get_pipeline()
    try:
        await asyncio.to_thread(
            app.save_customer_chat_history, customer, history, *_cb_hist_args(profile)
        )
    except Exception:
        pass


@cl.data_layer
def _make_data_layer():
    """Factory registered with Chainlit — called once on first data access."""
    from couchbase_data_layer import CouchbaseDataLayer
    p = _load_cb_settings()
    if not p.get("cb_user") or not p.get("cb_bucket"):
        return None  # No profile configured — sidebar disabled
    return CouchbaseDataLayer(
        cb_url=p.get("cb_url", "couchbase://localhost"),
        cb_bucket=p.get("cb_bucket", "rag"),
        cb_user=p.get("cb_user", ""),
        cb_pass=p.get("cb_pass", ""),
        cb_tls=p.get("cb_tls", False),
    )


# ── Local auth (username = your name, any password) ──────────────────────────
@cl.password_auth_callback
def _auth(username: str, password: str) -> "cl.User | None":
    if username and username.strip():
        return cl.User(identifier=username.strip(), metadata={"role": "local"})
    return None


# ── Optional deps ────────────────────────────────────────────────────────────
try:
    import plotly.graph_objects as go
    _PLOTLY = True
except ImportError:
    _PLOTLY = False

try:
    import pandas as pd
    _PANDAS = True
except ImportError:
    _PANDAS = False


# ── Profile helpers ──────────────────────────────────────────────────────────
def _active_profile() -> dict:
    app = _get_pipeline()
    s = app._load_settings_file()
    active = s.get("__last__", "")
    return s.get(active, {}) if active else {}


def _cb_args(p: dict) -> tuple:
    return (
        p.get("cb_url", "couchbase://localhost"),
        p.get("cb_bucket", "rag"),
        p.get("cb_user", ""),
        p.get("cb_pass", ""),
        p.get("cb_tls", False),
        p.get("cb_scope", "transcripts"),
        p.get("cb_collection", "tickets"),
    )


def _emb_config(p: dict) -> tuple[str, str, str, str, int]:
    """Return (provider, model, api_key, base_url, dims) for the configured embedding provider."""
    provider = (p.get("emb_provider") or "Ollama").strip()
    plo = provider.lower()
    if plo == "lmstudio":
        return (
            "lmstudio",
            (p.get("emb_lms_model") or "").strip(),
            "",
            (p.get("emb_lms_url") or "http://localhost:1234").rstrip("/"),
            int(p.get("emb_lms_dims") or 768),
        )
    elif plo == "gemini":
        return (
            "gemini",
            (p.get("emb_gemini_model") or "text-embedding-004").strip(),
            p.get("emb_gemini_key") or "",
            "",
            int(p.get("emb_gemini_dims") or 768),
        )
    elif plo == "openai":
        return (
            "openai",
            (p.get("emb_openai_model") or "text-embedding-3-small").strip(),
            p.get("emb_openai_key") or "",
            "",
            int(p.get("emb_openai_dims") or 1536),
        )
    elif plo == "mlx":
        return (
            "mlx",
            (p.get("emb_mlx_model") or "mixedbread-ai/mxbai-embed-large-v1").strip(),
            "",
            "",
            int(p.get("emb_mlx_dims") or 1024),
        )
    else:  # ollama (default)
        return (
            "ollama",
            (p.get("emb_ollama_model") or "nomic-embed-text").strip(),
            "",
            (p.get("emb_ollama_url") or "http://localhost:11434").rstrip("/"),
            int(p.get("emb_ollama_dims") or 1024),
        )


def _llm_config(p: dict, overrides: dict) -> tuple[str, str, str, str]:
    provider = (overrides.get("provider") or p.get("llm_provider", "claude")).lower()
    if provider == "claude":
        model    = overrides.get("model") or p.get("claude_model") or "claude-sonnet-4-6"
        api_key  = overrides.get("api_key") or p.get("claude_key", "")
        base_url = ""
    elif provider == "gemini":
        model    = overrides.get("model") or p.get("gemini_llm_model") or "gemini-2.0-flash"
        api_key  = overrides.get("api_key") or p.get("gemini_llm_key", "")
        base_url = ""
    elif provider == "lmstudio":
        model    = overrides.get("model") or p.get("lms_model") or "local-model"
        api_key  = "lm-studio"
        base_url = overrides.get("base_url") or p.get("emb_lms_url") or "http://localhost:1234"
    elif provider == "ollama":
        model    = overrides.get("model") or p.get("ollama_chat_model") or "llama3.2"
        api_key  = ""
        base_url = overrides.get("base_url") or p.get("emb_ollama_url") or "http://localhost:11434"
    else:  # openai
        model    = overrides.get("model") or p.get("openai_llm_model") or "gpt-4o"
        api_key  = overrides.get("api_key") or p.get("emb_openai_key", "")
        base_url = ""
    return provider, model, api_key, base_url


def _system_prompt(customer: str) -> str:
    today = date.today().isoformat()
    scope = f" for {customer}" if customer else ""
    prompt = (
        f"You are a Couchbase support ticket analyst. Today is {today}. "
        f"You have access to tools that query a live Couchbase database containing "
        f"Zendesk support tickets{scope}. "
        "Use the available tools to answer questions accurately. "
        "Always call tools to retrieve data — never guess at counts or ticket details.\n\n"
        "TOOL GUIDANCE:\n"
        "DATA SOURCE ROUTING — choose based on what the user is asking about:\n"
        "  LOCAL (your Couchbase): list_organizations, query_tickets, count_tickets, get_ticket\n"
        "    → 'what customers are you aware of?', 'what do you have locally?', 'which orgs have I scraped?'\n"
        "  LIVE/GLOBAL (Supportal Analytics API): list_supportal_customers, query_supportal\n"
        "    → 'how many customers get support today?', 'what's in Supportal globally?', "
        "'how many clusters exist?', 'live snapshot data', 'version distribution across all customers'\n"
        "When ambiguous, prefer LOCAL unless the user says 'today', 'live', 'Supportal', 'globally', or 'all customers'.\n"
        "- count_tickets: for total/count questions\n"
        "- query_tickets: to list or filter tickets\n"
        "- get_ticket: full detail on one ticket including cluster topology — node count, "
        "CB version, services, buckets, RAM, auto-failover, bad/warn health counts\n"
        "- check_data_freshness: call when the user asks about 'current status' or 'latest'\n"
        "- rescrape_ticket: refresh a single ticket from Supportal and save to CB\n"
        "- rescrape_customer_tickets: bulk-refresh all stale tickets for a customer — "
        "call this when the user says 'rescrape all', 'refresh everything', 'update all tickets'. "
        "Use stale_hours=0 to force-refresh all regardless of age.\n"
        "- generate_chart: MANDATORY when the user asks for any chart or visualization. "
        "Call this tool — do NOT describe a chart in text. "
        "Types: bar, horizontal_bar, line, area, stacked_bar, scatter, combo, pie, donut, gauge, treemap, funnel. "
        "Use area/line for time trends; gauge for a single KPI; stacked_bar for part-of-whole; "
        "horizontal_bar for ranked lists; scatter for correlations; treemap for hierarchy. "
        "Extra params: height, stacked, show_labels, description (insight caption), color_scheme.\n"
        "- generate_table: MANDATORY when the user asks for a table or exportable data. "
        "Call this tool — do NOT render markdown tables.\n"
        "INGESTION & ENRICHMENT TOOLS (use when data is missing or stale):\n"
        "- vector_search: semantic/similarity search — finds tickets by meaning, not just keywords.\n"
        "- get_cluster_health: cluster health summary from stored snapshots (versions, nodes, bad/warn). "
        "Auto-triggers sync_snapshots if no local snapshots exist and a cookie is available.\n"
        "- sync_snapshots: ONE-STEP snapshot sync — fetches stubs then enriches topology. "
        "Prefer this over calling fetch_snapshots + backfill_snapshot_topology separately.\n"
        "- fetch_snapshots: pull snapshot listing from Analytics API and save stubs to CB.\n"
        "- backfill_snapshot_topology: enrich snapshot stubs with topology detail. Call after fetch_snapshots.\n"
        "- scrape_customer_tickets: scrape fresh tickets from Supportal (capped). Use when tickets are missing.\n"
        "- score_ticket: LLM-score a single ticket for stars, temperature, complexity.\n"
        "- batch_score_tickets: score up to 10 tickets at once — pass ticket_ids list OR organization+limit. "
        "Prefer over calling score_ticket repeatedly.\n"
        "- batch_rescrape_tickets: re-fetch up to 20 tickets from Supportal in one call. "
        "Prefer over calling rescrape_ticket in a loop.\n"
        "CUSTOMER INTELLIGENCE (v1.6.0):\n"
        "- get_customer_health_score: 0-100 composite score (P1s, escalations, resolution, freshness). "
        "Call for any 'how is X doing', 'status of X', 'health of X' question.\n"
        "- check_sla_compliance: SLA compliance % by priority for a customer.\n"
        "- get_portfolio_status: ranked overview of ALL customers by urgency — for fleet/portfolio questions. "
        "Use for 'morning briefing', 'what should I focus on today', 'portfolio status', 'top customers', "
        "'any urgent issues across all accounts' — call it with no required args.\n"
        "- get_digest: what's new/changed for a specific named customer in the last N hours.\n"
        "- tag_ticket: apply tags to a ticket (e.g. 'performance', 'upgrade').\n"
        "- save_query / list_saved_queries: bookmark and recall queries.\n"
        "- generate_customer_report: full markdown report (health + SLA + open tickets + digest).\n"
    )
    if customer and customer.lower() != "all customers":
        prompt += (
            f"\n\nSCOPING RULE: Customer is scoped to \"{customer}\". "
            f"You MUST include customer=\"{customer}\" in every query_tickets and count_tickets call. "
            f"Never ask the user for the customer name — it is already set.\n"
            f"DISCOVERY EXCEPTIONS (cross-customer queries are allowed ONLY for):\n"
            f"  1. list_organizations — always exempt, always runs across all customers.\n"
            f"  2. Discovering what customers exist ('what orgs are in the system', "
            f"'what other customers are there', 'update the customer list').\n"
            f"  3. Getting a basic ticket count or summary for a specific other customer "
            f"the user names explicitly.\n"
            f"For all analysis, trends, ticket details, and comparisons: stay scoped to \"{customer}\"."
        )
    return prompt


# ── ECharts → Plotly converter ───────────────────────────────────────────────
def _to_plotly(option: dict):
    """Convert an ECharts option dict (from _build_agent_echart_option) to Plotly."""
    if not _PLOTLY:
        return None
    title = (option.get("title") or {}).get("text", "")
    series = option.get("series") or []
    if not series:
        return None

    s0 = series[0]
    stype = s0.get("type", "bar")

    if stype == "pie":
        pie_data = s0.get("data") or []
        labels = [d.get("name", "") for d in pie_data]
        values = [d.get("value", 0) for d in pie_data]
        radius = s0.get("radius", "60%")
        hole = 0.4 if isinstance(radius, list) else 0
        fig = go.Figure(go.Pie(labels=labels, values=values, hole=hole))

    elif stype in ("bar", "line"):
        x_axis = option.get("xAxis") or {}
        y_axis = option.get("yAxis") or {}
        is_horizontal = y_axis.get("type") == "category"

        if is_horizontal:
            cats = y_axis.get("data") or []
            fig = go.Figure()
            for s in series:
                fig.add_trace(go.Bar(x=s.get("data") or [], y=cats,
                                     orientation="h", name=s.get("name", "")))
        elif stype == "line":
            cats = x_axis.get("data") or []
            fig = go.Figure()
            for s in series:
                fig.add_trace(go.Scatter(x=cats, y=s.get("data") or [],
                                         mode="lines+markers", name=s.get("name", "")))
        else:
            cats = x_axis.get("data") or []
            fig = go.Figure()
            for s in series:
                fig.add_trace(go.Bar(x=cats, y=s.get("data") or [], name=s.get("name", "")))
    else:
        return None

    fig.update_layout(
        title=title, template="plotly_white", height=380,
        barmode="group", margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


# ── Artifact parsing ─────────────────────────────────────────────────────────
def _parse_artifacts(answer: str) -> tuple[str, list, list]:
    """Split answer into (clean_text, [cl.Element, ...], [raw_artifact_dict, ...]).

    raw_artifact_dict: {type, data, title} — stored as base64 blobs in CB assets.
    """
    app = _get_pipeline()
    artifact_re = app._ARTIFACT_RE

    elements: list = []
    raw_artifacts: list = []
    text_parts: list[str] = []
    last = 0

    for m in artifact_re.finditer(answer):
        pre = answer[last:m.start()].strip()
        if pre:
            text_parts.append(pre)
        atype, araw = m.group(1), m.group(2)
        last = m.end()

        try:
            ap = json.loads(araw)
        except Exception:
            continue

        if atype == "echart":
            title = (ap.get("title") or {}).get("text", "chart")
            raw_artifacts.append({"type": "echart", "data": ap, "title": title})
            fig = _to_plotly(ap)
            if fig:
                elements.append(cl.Plotly(name=title or "chart", figure=fig, display="inline"))
            else:
                text_parts.append(
                    f"*[Chart: {title} — install plotly for visual rendering: "
                    f"`pip install plotly`]*"
                )

        elif atype == "table":
            cols = ap.get("columns") or []
            rows = ap.get("rows") or []
            tname = ap.get("title", "table")
            raw_artifacts.append({"type": "table", "data": ap, "title": tname})
            if tname:
                text_parts.append(f"**{tname}**")
            if _PANDAS and cols:
                df = pd.DataFrame(rows, columns=cols)
                elements.append(cl.Dataframe(name=tname, data=df, display="inline"))
            else:
                # Markdown table fallback
                md = "| " + " | ".join(cols) + " |\n"
                md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
                for row in rows:
                    md += "| " + " | ".join(str(c) for c in row) + " |\n"
                text_parts.append(md)

    post = answer[last:].strip()
    if post:
        text_parts.append(post)

    clean = "\n\n".join(text_parts) if text_parts else ("" if elements else answer)
    return clean, elements, raw_artifacts


# ── Helpers ──────────────────────────────────────────────────────────────────
async def _save_thread_meta(customer: str, overrides: dict) -> None:
    """Persist customer scope + overrides in the thread so resume can restore them."""
    dl = cl_data.get_data_layer()
    if dl is None:
        return
    try:
        tid = cl.context.session.thread_id
        await dl.update_thread(
            tid,
            metadata={"customer": customer, "overrides": overrides},
        )
    except Exception:
        pass


async def _save_assets(thread_id: str, prompt: str, raw_artifacts: list) -> None:
    """Persist generated charts/tables as base64-encoded JSON blobs in CB."""
    if not raw_artifacts:
        return
    dl = cl_data.get_data_layer()
    if dl is None or not hasattr(dl, "save_asset"):
        return
    for art in raw_artifacts:
        try:
            content_b64 = base64.b64encode(
                json.dumps(art["data"]).encode()
            ).decode()
            await dl.save_asset({
                "thread_id": thread_id,
                "prompt": prompt[:500],
                "type": art["type"],
                "title": art.get("title", ""),
                "content_b64": content_b64,
                "mime_type": "application/json",
            })
        except Exception:
            pass


async def _restore_assets(thread_id: str) -> None:
    """Re-display stored charts/tables for a resumed thread."""
    dl = cl_data.get_data_layer()
    if dl is None or not hasattr(dl, "get_assets_for_thread"):
        return
    try:
        assets = await dl.get_assets_for_thread(thread_id)
    except Exception:
        return
    for asset in assets:
        try:
            data = json.loads(base64.b64decode(asset["content_b64"]))
            snippet = (asset.get("prompt") or "")[:80]
            label = f'*Restored from: "{snippet}"*' if snippet else "*Restored asset*"
            atype = asset.get("type", "")
            if atype == "echart":
                fig = _to_plotly(data)
                if fig:
                    await cl.Message(
                        content=label,
                        elements=[cl.Plotly(
                            name=asset.get("title", "chart"),
                            figure=fig,
                            display="inline",
                        )],
                        author="Supportal (restored)",
                    ).send()
            elif atype == "table":
                cols = data.get("columns") or []
                rows = data.get("rows") or []
                tname = asset.get("title", "table")
                if _PANDAS and cols:
                    df = pd.DataFrame(rows, columns=cols)
                    await cl.Message(
                        content=label,
                        elements=[cl.Dataframe(name=tname, data=df, display="inline")],
                        author="Supportal (restored)",
                    ).send()
        except Exception:
            pass


# ── Quick-action helpers ──────────────────────────────────────────────────────
async def _fetch_saved_queries(customer: str, profile: dict) -> list[dict]:
    """Return list of saved query dicts ({name, question, organization}) for customer."""
    app = _get_pipeline()
    if not hasattr(app, "_list_saved_queries") or not customer:
        return []
    try:
        return await asyncio.to_thread(
            app._list_saved_queries, *_cb_args(profile), customer
        ) or []
    except Exception:
        return []


async def _send_quick_actions(customer: str) -> None:
    """Send a Quick Actions bar. Customer-specific actions added when customer is set."""
    actions = [
        cl.Action(
            name="morning_briefing", value="morning_briefing", payload={},
            label="☀ Morning Briefing",
            description="Fleet-wide briefing for top customers",
        ),
        cl.Action(
            name="quick_dashboard", value=customer or "all",
            payload={"customer": customer or "all"},
            label="📊 Quick Dashboard",
            description="Health score summary",
        ),
    ]
    if customer:
        actions.insert(
            1,
            cl.Action(
                name="whats_new", value=customer, payload={"customer": customer},
                label=f"🆕 What's New? ({customer})",
                description=f"Changes for {customer} in the last 24 hours",
            ),
        )
        actions.append(
            cl.Action(
                name="show_saved_queries", value=customer, payload={"customer": customer},
                label="🔖 Saved Queries",
                description=f"Load saved queries for {customer}",
            )
        )
    await cl.Message(content="**Quick Actions**", actions=actions, author="Supportal").send()


# ── Chat starters (shown before first message) ────────────────────────────────
@cl.set_starters
async def set_starters():
    return [
        cl.Starter(
            label="Morning Briefing",
            message="Run get_portfolio_status to show me a ranked portfolio overview — my morning briefing across all customers.",
        ),
        cl.Starter(
            label="What's New?",
            message="What's new for my customer in the last 24 hours?",
        ),
        cl.Starter(
            label="Open P1 Tickets",
            message="Show all open P1 tickets",
        ),
        cl.Starter(
            label="Customer Health Dashboard",
            message="Show me a quick health score dashboard",
        ),
    ]


# ── Chainlit handlers ────────────────────────────────────────────────────────
async def _resume_scrape_job_monitors(profile: dict) -> None:
    """Find running/recent scrape jobs (memory + CB) and spawn monitor tasks."""
    app    = _get_pipeline()
    cb     = _cb_args(profile)
    cb_url = cb[0]

    # Collect running jobs from both sources, deduplicated by job_id
    seen: set[str] = set()
    running: list[tuple[str, dict]] = []

    # 1. In-process memory (same-process jobs or already-rehydrated)
    for jid, job in list(app._SCRAPE_JOBS.items()):
        if job.get("status") == "running" and jid not in seen:
            seen.add(jid)
            running.append((jid, job))

    # 2. Couchbase (cross-process, or jobs from before a server restart)
    if cb_url:
        cb_jobs = await asyncio.to_thread(_cb_query_active_jobs, *cb)
        for job in cb_jobs:
            jid = job.get("job_id", "")
            if not jid or jid in seen:
                continue
            seen.add(jid)
            if job.get("status") == "running":
                app._SCRAPE_JOBS[jid] = job   # rehydrate into memory
                running.append((jid, job))

    if not running:
        return

    summary_lines = [f"Found **{len(running)} scrape job(s)** still running — resuming live monitoring:\n"]
    for jid, job in running:
        summary_lines.append(f"- Job **{jid}** — {job.get('org')} ({job.get('mode')}), phase: {job.get('phase')}")
    await cl.Message(content="\n".join(summary_lines), author="Job Monitor").send()

    for jid, _ in running:
        asyncio.create_task(_monitor_job(jid, app, cb))


@cl.on_chat_resume
async def on_resume(thread: dict):
    """Restore session state when user clicks a previous thread in the sidebar."""
    meta = thread.get("metadata") or {}
    customer  = meta.get("customer", "")
    overrides = meta.get("overrides", {})
    profile   = _load_cb_settings()

    # Load from the shared history store (NiceGUI + Chainlit both write here)
    history = await _load_shared_history(customer, profile)

    # Fall back to step-based reconstruction if shared store is empty
    if not history:
        for step in (thread.get("steps") or []):
            if not isinstance(step, dict):
                continue
            stype   = step.get("type", "")
            content = step.get("output", "")
            if stype == "user_message" and content:
                history.append({"role": "user", "content": content})
            elif stype == "assistant_message" and content:
                history.append({"role": "assistant", "content": content})

    cl.user_session.set("profile", profile)
    cl.user_session.set("customer", customer)
    cl.user_session.set("overrides", overrides)
    cl.user_session.set("history", history)
    # Mark as named so on_message doesn't clobber an existing thread name
    cl.user_session.set("thread_named", True)

    # Re-display any charts/tables that were saved during this thread
    await _restore_assets(thread.get("id", ""))

    # Resume monitoring for any scrape jobs that were running when the session dropped
    await _resume_scrape_job_monitors(profile)


@cl.on_chat_start
async def on_start():
    # First async context — safe to load pipeline now
    profile = _active_profile()
    provider = profile.get("llm_provider", "claude").lower()
    provider_choices = ["claude", "gemini", "openai", "lmstudio", "ollama"]
    provider_idx = provider_choices.index(provider) if provider in provider_choices else 0

    await cl.ChatSettings([
        cl.input_widget.TextInput(
            id="customer", label="Customer",
            description="Scope all queries to this customer. Leave blank for all.",
            initial="",
        ),
        cl.input_widget.Select(
            id="provider", label="LLM Provider",
            values=provider_choices,
            initial_index=provider_idx,
        ),
        cl.input_widget.TextInput(
            id="model", label="Model override",
            description="Leave blank to use the model saved in your NiceGUI profile.",
            initial="",
        ),
        cl.input_widget.TextInput(
            id="api_key", label="API Key override",
            description="Leave blank to use the key saved in your NiceGUI profile.",
            initial="",
        ),
        cl.input_widget.Slider(  # AFTER v1.5.0: history depth control
            id="agent_context_depth", label="History depth",
            description="Number of prior messages included in each agent call (default 10).",
            initial=10, min=2, max=40, step=2,
        ),
    ]).send()

    cl.user_session.set("profile", profile)
    cl.user_session.set("customer", "")
    cl.user_session.set("overrides", {"provider": provider})
    cl.user_session.set("history", [])

    # Associate this thread with the current user so it appears in the sidebar
    dl = cl_data.get_data_layer()
    if dl:
        try:
            user = cl.context.session.user
            uid = getattr(user, "id", None)
            if uid:
                await dl.update_thread(cl.context.session.thread_id, user_id=uid)
        except Exception:
            pass

    _ver = getattr(_get_pipeline(), "__version__", "")
    _ver_str = f" `v{_ver}`" if _ver else ""
    await cl.Message(
        content=(
            f"**Supportal Agent**{_ver_str} — professional AI chat\n\n"
            "Open the **⚙ Settings** panel to set your customer and LLM provider.\n"
            "Your Couchbase connection is loaded from the active NiceGUI profile automatically.\n\n"
            "Ask anything about your support tickets, request charts or tables, "
            "or ask me to refresh a specific ticket from Supportal."
        ),
        author="Supportal",
    ).send()
    await _send_quick_actions("")
    # Pick up any jobs that started in NiceGUI or a prior Chainlit session
    await _resume_scrape_job_monitors(profile)


@cl.on_settings_update
async def on_settings_update(settings: dict):
    customer  = (settings.get("customer") or "").strip()
    overrides = {
        "provider":            settings.get("provider") or "",
        "model":               (settings.get("model") or "").strip(),
        "api_key":             (settings.get("api_key") or "").strip(),
        "agent_context_depth": int(settings.get("agent_context_depth") or 10),  # AFTER v1.5.0
    }
    old_customer = cl.user_session.get("customer", "")
    cl.user_session.set("customer", customer)
    cl.user_session.set("overrides", overrides)
    await _save_thread_meta(customer, overrides)

    # Load shared history for the new customer (shared with NiceGUI chat)
    if customer != old_customer:
        profile = cl.user_session.get("profile") or _load_cb_settings()
        history = await _load_shared_history(customer, profile)
        cl.user_session.set("history", history)
        note = f" Loaded {len(history) // 2} prior exchange(s) from history." if history else ""
    else:
        note = ""

    await cl.Message(
        content=f"Settings saved. Customer: **{customer or 'all'}**.{note}",
        author="System",
    ).send()
    await _send_quick_actions(customer)


@cl.on_message
async def on_message(message: cl.Message):
    app = _get_pipeline()

    profile   = cl.user_session.get("profile") or _active_profile()
    customer  = cl.user_session.get("customer", "")
    overrides = cl.user_session.get("overrides", {})
    history: list[dict] = cl.user_session.get("history", [])

    provider, model, api_key, base_url = _llm_config(profile, overrides)
    cb = _cb_args(profile)
    emb_provider, emb_model, emb_api_key, emb_base_url, emb_dims = _emb_config(profile)
    # AFTER v1.5.0: carry session log across turns so call_llm_with_tools can
    # inject it into the system prompt (reduces redundant re-calls by the LLM).
    agent_ctx = {
        "provider": provider, "model": model,
        "api_key": api_key, "base_url": base_url,
        "emb_provider": emb_provider,
        "emb_model":    emb_model,
        "emb_api_key":  emb_api_key,
        "emb_base_url": emb_base_url,
        "emb_dims":     emb_dims,
        "cookie":       profile.get("cookie") or "",
        "_session_log": cl.user_session.get("_session_log", {}),  # AFTER v1.5.0
    }
    # BEFORE v1.5.0: no _session_log key

    # Name the thread from the first user message, before the agent call so it
    # persists even if the agent errors out. Use a session flag instead of
    # checking history length — history may already have items from shared store.
    if not cl.user_session.get("thread_named"):
        cl.user_session.set("thread_named", True)
        prefix = f"[{customer}] " if customer else ""
        thread_name = (prefix + message.content[:60].replace("\n", " ")).strip()
        dl = cl_data.get_data_layer()
        if dl:
            try:
                await dl.update_thread(cl.context.session.thread_id, name=thread_name)
            except Exception:
                pass

    # AFTER v1.5.0: history depth — configurable via settings, default 10
    _ctx_depth = int(overrides.get("agent_context_depth") or profile.get("agent_context_depth") or 10)
    msgs = [{"role": "system", "content": _system_prompt(customer)}]
    msgs.extend(history[-_ctx_depth:])
    msgs.append({"role": "user", "content": message.content})

    # AFTER v1.5.0: live tool-call status message
    # We update this message as each tool fires so users see progress.
    # It stays visible after the agent finishes as a tool trace.
    loop = asyncio.get_event_loop()
    _tool_log: list[str] = []
    status_msg = await cl.Message(content="⏳ Agent starting…", author="Supportal").send()

    def _cl_status_cb(tool_name: str):
        _tool_log.append(tool_name)
        async def _update():
            status_msg.content = "🔧 " + " → ".join(f"`{t}`" for t in _tool_log) + " …"
            await status_msg.update()
        asyncio.run_coroutine_threadsafe(_update(), loop)

    # Run the agent in a thread executor. We intentionally do NOT wrap this in
    # cl.Step — run-type steps stored in Couchbase cause a JS destructuring error
    # in the frontend when the thread is later resumed.
    answer = ""
    try:
        answer = await loop.run_in_executor(
            None,
            lambda: app.call_llm_with_tools(
                msgs, app._AGENT_TOOLS,
                *cb,
                provider, model, api_key, base_url,
                8192, 5, customer, agent_ctx,
                _cl_status_cb,   # AFTER v1.5.0: live tool status
            ),
        )
        # Update status to show completed tool trace
        if _tool_log:
            status_msg.content = "✅ " + " → ".join(f"`{t}`" for t in _tool_log)
        else:
            status_msg.content = "✅ Done"
        await status_msg.update()

    except Exception as exc:
        # AFTER v1.5.0: friendly error + retry action
        _friendly = app._classify_agent_error(exc)
        status_msg.content = f"❌ {_friendly}"
        await status_msg.update()
        retry_action = cl.Action(name="retry", value=message.content, payload={"value": message.content}, label="Retry", description="Re-run the same question")
        await cl.Message(content=_friendly, actions=[retry_action], author="Supportal").send()
        return

    cl.user_session.set("_session_log", agent_ctx.get("_session_log", {}))  # AFTER v1.5.0

    # Spawn a live-updating monitor message for any scrape/rescrape jobs started this turn.
    _cb_tuple = _cb_args(profile)
    for _jid in agent_ctx.get("_started_jobs", []):
        asyncio.create_task(_monitor_job(_jid, app, _cb_tuple))

    history.append({"role": "user",      "content": message.content})
    history.append({"role": "assistant", "content": answer})
    cl.user_session.set("history", history)

    # Persist to shared history so NiceGUI chat sees the same conversation
    profile = cl.user_session.get("profile") or _load_cb_settings()
    await _save_shared_history(customer, history, profile)

    clean_text, elements, raw_artifacts = _parse_artifacts(answer)

    # Persist charts / tables to CB so they survive session restarts
    await _save_assets(cl.context.session.thread_id, message.content, raw_artifacts)

    # AFTER v1.5.0: follow-up suggestion chips as Actions
    _sugs = await loop.run_in_executor(
        None,
        lambda: app._generate_followup_suggestions(
            message.content, answer, provider, model, api_key, base_url
        ),
    )
    _actions = [
        cl.Action(name="followup", value=s, payload={"value": s}, label=s, description="Ask this follow-up")
        for s in _sugs
    ]
    await cl.Message(content=clean_text, elements=elements, actions=_actions, author="Supportal").send()


async def _monitor_job(job_id: str, app: Any, cb: tuple | None = None) -> None:
    """Poll a scrape job every 3s and update a dedicated Chainlit message.

    Reads from app._SCRAPE_JOBS (in-memory, same process) first; falls back to
    Couchbase when the job isn't present (cross-process or post-restart).
    cb = (cb_url, bucket, username, password, use_tls, scope, collection).
    """
    import time as _time

    def _fmt(job: dict) -> str:
        now   = _time.time()
        proc  = job.get("processed") or 0
        total = job.get("total")
        pct   = f" ({proc/total:.0%})" if total else ""
        elap  = int(now - job.get("started_at", now))
        icon  = "🔄" if job["status"] == "running" else ("✅" if job["status"] == "done" else "❌")
        lines = [
            f"{icon} **Job {job['job_id']}** — {job['org']} ({job['mode']})",
            f"Phase: **{job.get('phase') or 'done'}** | {proc}/{total or '?'} tickets{pct}",
            f"Elapsed: {elap}s",
        ]
        if job.get("last_message"):
            lines.append(f"_{job['last_message']}_")
        if job["status"] != "running" and job.get("finished_at"):
            dur  = int(job["finished_at"] - job.get("started_at", job["finished_at"]))
            errs = job.get("errors", 0)
            lines.append(
                f"Done in {dur}s — {proc} scraped, {job.get('saved',0)} saved, "
                f"{job.get('embedded',0)} embedded, {job.get('scored',0)} scored"
                + (f", {errs} errors" if errs else "")
            )
        return "\n".join(lines)

    async def _get_job() -> dict | None:
        j = app._SCRAPE_JOBS.get(job_id)
        if j is not None:
            return j
        if cb:
            return await asyncio.to_thread(_cb_get_job, job_id, *cb)
        return None

    job = await _get_job()
    if not job:
        return
    # Re-hydrate in-memory dict so subsequent in-process reads are fast
    if job_id not in app._SCRAPE_JOBS:
        app._SCRAPE_JOBS[job_id] = job

    msg = await cl.Message(content=_fmt(job), author="Job Monitor").send()
    while job and job.get("status") == "running":
        await asyncio.sleep(3)
        job = await _get_job()
        if job:
            app._SCRAPE_JOBS[job_id] = job
        try:
            msg.content = _fmt(job) if job else f"Job {job_id} — status unknown"
            await msg.update()
        except Exception:
            break  # WebSocket gone; stop updating but don't crash
    # Final update
    if job:
        try:
            msg.content = _fmt(job)
            await msg.update()
        except Exception:
            pass


@cl.action_callback("retry")
async def on_retry(action: cl.Action):
    """Re-run the original question when the user clicks Retry after an error."""
    fake_msg = cl.Message(content=action.value, author="User")
    await on_message(fake_msg)


@cl.action_callback("followup")
async def on_followup(action: cl.Action):
    """Send a follow-up suggestion as a new user message."""
    fake_msg = cl.Message(content=action.value, author="User")
    await on_message(fake_msg)


@cl.action_callback("morning_briefing")
async def on_morning_briefing(action: cl.Action):
    fake_msg = cl.Message(content="Run get_portfolio_status to show me a ranked portfolio overview — my morning briefing across all customers.", author="User")
    await on_message(fake_msg)


@cl.action_callback("whats_new")
async def on_whats_new(action: cl.Action):
    customer = action.payload.get("customer") or cl.user_session.get("customer", "")
    if not customer:
        await cl.Message(content="Set a customer in ⚙ Settings first.", author="System").send()
        return
    fake_msg = cl.Message(content=f"What's new for {customer} in the last 24 hours?", author="User")
    await on_message(fake_msg)


@cl.action_callback("quick_dashboard")
async def on_quick_dashboard(action: cl.Action):
    customer = action.payload.get("customer") or cl.user_session.get("customer", "")
    if customer and customer != "all":
        query = f"Show me a quick health score dashboard for {customer}"
    else:
        query = "Show me a portfolio health score dashboard for all customers"
    fake_msg = cl.Message(content=query, author="User")
    await on_message(fake_msg)


@cl.action_callback("show_saved_queries")
async def on_show_saved_queries(action: cl.Action):
    customer = action.payload.get("customer") or cl.user_session.get("customer", "")
    profile = cl.user_session.get("profile") or _load_cb_settings()
    queries = await _fetch_saved_queries(customer, profile)
    if not queries:
        await cl.Message(
            content=f"No saved queries found for **{customer or 'this customer'}**.",
            author="Supportal",
        ).send()
        return
    sq_actions = [
        cl.Action(
            name="run_saved_query",
            value=q.get("question", ""),
            payload={"query": q.get("question", ""), "customer": customer},
            label=(q.get("name") or q.get("question") or "")[:60],
            description=q.get("question", ""),
        )
        for q in queries[:10]
        if q.get("question")
    ]
    if not sq_actions:
        await cl.Message(content="No runnable saved queries found.", author="Supportal").send()
        return
    await cl.Message(
        content=f"**Saved Queries for {customer}** — click to run:",
        actions=sq_actions,
        author="Supportal",
    ).send()


@cl.action_callback("run_saved_query")
async def on_run_saved_query(action: cl.Action):
    query = action.payload.get("query") or action.value
    if not query:
        return
    fake_msg = cl.Message(content=query, author="User")
    await on_message(fake_msg)
