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
        "Call this tool — do NOT describe a chart in text.\n"
        "- generate_table: MANDATORY when the user asks for a table or exportable data. "
        "Call this tool — do NOT render markdown tables.\n"
        "INGESTION & ENRICHMENT TOOLS (use when data is missing or stale):\n"
        "- vector_search: semantic/similarity search — finds tickets by meaning, not just keywords.\n"
        "- get_cluster_health: cluster health summary from stored snapshots (versions, nodes, bad/warn).\n"
        "- fetch_snapshots: pull snapshot listing from Analytics API and save stubs to CB.\n"
        "- backfill_snapshot_topology: enrich snapshot stubs with topology detail. Call after fetch_snapshots.\n"
        "- scrape_customer_tickets: scrape fresh tickets from Supportal (capped). Use when tickets are missing.\n"
        "- score_ticket: LLM-score a single ticket for stars, temperature, complexity.\n"
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


# ── Chainlit handlers ────────────────────────────────────────────────────────
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
        for step in thread.get("steps", []):
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

    # Re-display any charts/tables that were saved during this thread
    await _restore_assets(thread.get("id", ""))


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

    await cl.Message(
        content=(
            "**Supportal Agent** — professional AI chat\n\n"
            "Open the **⚙ Settings** panel to set your customer and LLM provider.\n"
            "Your Couchbase connection is loaded from the active NiceGUI profile automatically.\n\n"
            "Ask anything about your support tickets, request charts or tables, "
            "or ask me to refresh a specific ticket from Supportal."
        ),
        author="Supportal",
    ).send()


@cl.on_settings_update
async def on_settings_update(settings: dict):
    customer  = (settings.get("customer") or "").strip()
    overrides = {
        "provider": settings.get("provider") or "",
        "model":    (settings.get("model") or "").strip(),
        "api_key":  (settings.get("api_key") or "").strip(),
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


@cl.on_message
async def on_message(message: cl.Message):
    app = _get_pipeline()

    profile   = cl.user_session.get("profile") or _active_profile()
    customer  = cl.user_session.get("customer", "")
    overrides = cl.user_session.get("overrides", {})
    history: list[dict] = cl.user_session.get("history", [])

    provider, model, api_key, base_url = _llm_config(profile, overrides)
    cb = _cb_args(profile)
    agent_ctx = {
        "provider": provider, "model": model,
        "api_key": api_key, "base_url": base_url,
        "emb_provider": profile.get("emb_provider", "ollama"),
        "emb_model":    profile.get("emb_ollama_model") or profile.get("emb_lms_model") or
                        profile.get("emb_gemini_model") or profile.get("emb_openai_model") or "nomic-embed-text",
        "emb_api_key":  profile.get("emb_gemini_key") or profile.get("emb_openai_key") or "",
        "emb_base_url": profile.get("emb_ollama_url") or profile.get("emb_lms_url") or "",
        "emb_dims":     int(profile.get("emb_ollama_dims") or profile.get("emb_lms_dims") or
                            profile.get("emb_gemini_dims") or profile.get("emb_openai_dims") or 1024),
        "cookie":       profile.get("cookie") or "",
    }

    msgs = [{"role": "system", "content": _system_prompt(customer)}]
    msgs.extend(history[-10:])
    msgs.append({"role": "user", "content": message.content})

    answer = ""
    async with cl.Step(name="Agent", type="run", show_input=False) as step:
        step.output = f"Running with {provider} / {model} …"
        loop = asyncio.get_event_loop()
        try:
            answer = await loop.run_in_executor(
                None,
                lambda: app.call_llm_with_tools(
                    msgs, app._AGENT_TOOLS,
                    *cb,
                    provider, model, api_key, base_url,
                    8192, 5, customer, agent_ctx,
                ),
            )
            step.output = "Done."
        except Exception as exc:
            step.output = f"Error: {exc}"
            await cl.Message(content=f"Agent error: {exc}", author="Supportal").send()
            return

    history.append({"role": "user",      "content": message.content})
    history.append({"role": "assistant", "content": answer})
    cl.user_session.set("history", history)

    # Persist to shared history so NiceGUI chat sees the same conversation
    profile = cl.user_session.get("profile") or _load_cb_settings()
    await _save_shared_history(customer, history, profile)

    # Name the thread from the first user message (prefix with customer if set)
    if len(history) == 2:
        prefix = f"[{customer}] " if customer else ""
        thread_name = (prefix + message.content[:60].replace("\n", " ")).strip()
        dl = cl_data.get_data_layer()
        if dl:
            try:
                await dl.update_thread(cl.context.session.thread_id, name=thread_name)
            except Exception:
                pass

    clean_text, elements, raw_artifacts = _parse_artifacts(answer)

    # Persist charts / tables to CB so they survive session restarts
    await _save_assets(cl.context.session.thread_id, message.content, raw_artifacts)

    await cl.Message(content=clean_text, elements=elements, author="Supportal").send()
