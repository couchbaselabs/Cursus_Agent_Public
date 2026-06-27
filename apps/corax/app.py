"""
Corax — Supportal AI Chat
Shares all pipeline functions and agent tools with the Strabo app.

Run alongside the main app:
    python run_corax.py --port 8766

Then click "Open Corax ↗" in the Strabo Agent tab.
The Strabo app (port 8765) continues running unchanged.
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

# Chainlit persists uploaded/generated files under .files/{session_uuid}/.
# mkdir(exist_ok=True) without parents=True fails if .files/ itself is missing.
(Path(__file__).parent.parent.parent / ".files").mkdir(exist_ok=True)

import chainlit as cl
import chainlit.data as cl_data

# ── Pipeline is imported lazily to avoid Strabo event-loop side-effects ──────
# Importing apps.strabo.app at module level can initialise NiceGUI's
# asyncio state before Chainlit's anyio backend is running, causing
# NoEventLoopError on static file requests.  We load it on first handler call.
_pipeline: Any = None


def _get_pipeline():
    global _pipeline
    if _pipeline is None:
        _pipeline = importlib.import_module("apps.strabo.app")
    return _pipeline


# ── Scrape-job persistence helpers ───────────────────────────────────────────
def _cb_get_job(job_id: str, cb_url: str, bucket: str, username: str,
                password: str, use_tls: bool, scope: str, collection: str) -> dict | None:
    """Read a single scrape_job doc from CB. Returns None on any error."""
    if not cb_url or not username:
        return None
    try:
        app = _get_pipeline()
        from couchbase.cluster import Cluster as _Cl
        from couchbase.options import ClusterOptions as _CO
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
        from couchbase.cluster import Cluster as _Cl
        from couchbase.options import ClusterOptions as _CO
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
    from supportal.couchbase_data_layer import CouchbaseDataLayer
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


def _cb_args_assets(p: dict) -> tuple:
    """CB args for the assets collection (no trailing collection name)."""
    return (
        p.get("cb_url", "couchbase://localhost"),
        p.get("cb_bucket", "rag"),
        p.get("cb_user", ""),
        p.get("cb_pass", ""),
        p.get("cb_tls", False),
        p.get("cb_scope", "transcripts"),
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


def _system_prompt(
    customer: str,
    profile_hint: str = "",
    prior_session_block: str = "",
    few_shot_examples: list[dict] | None = None,
) -> str:
    from supportal.prompts import build_agent_system_prompt
    base = build_agent_system_prompt(
        customer=customer,
        profile_hint=profile_hint,
        prior_session_block=prior_session_block,
    )
    if few_shot_examples:
        lines = ["\n\nEXAMPLES OF WELL-RECEIVED ANSWERS (use as style/depth reference):"]
        for i, ex in enumerate(few_shot_examples, 1):
            q = (ex.get("question_text") or "").strip()
            a = (ex.get("answer_text") or "").strip()
            note = (ex.get("comment") or "").strip()
            if not q:
                continue
            lines.append(f"\nExample {i}:")
            lines.append(f"  Q: {q[:300]}")
            if a:
                lines.append(f"  A: {a[:600]}")
            if note:
                lines.append(f"  (User noted: {note})")
        if len(lines) > 1:
            base += "\n".join(lines)
    return base


async def _embed_pending_feedback(profile: dict) -> None:
    """Embed any positive feedback docs that don't have a vector yet."""
    dl = cl_data.get_data_layer()
    if dl is None or not hasattr(dl, "get_unembedded_positive_feedback"):
        return
    try:
        pending = await dl.get_unembedded_positive_feedback()
    except Exception:
        return
    if not pending:
        return
    app = _get_pipeline()
    provider, model, api_key, base_url, dims = _emb_config(profile)
    for doc in pending:
        text = (doc.get("question_text") or "").strip()
        if not text:
            continue
        try:
            vector = await asyncio.to_thread(
                app.embed_text, text, provider, model, api_key, base_url, dims
            )
            await dl.update_feedback_embedding(doc["id"], vector)
        except Exception:
            pass


# ── ECharts element wrapper ───────────────────────────────────────────────────
def _echart_element(option: dict, title: str) -> cl.CustomElement:
    """Wrap an ECharts option dict as a Chainlit CustomElement rendered by public/elements/EChart.jsx."""
    height = option.get("_height", 320)
    return cl.CustomElement(
        name="EChart",
        props={"option": option, "height": height},
        display="inline",
    )


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
            elements.append(_echart_element(ap, title))

        elif atype == "table":
            cols = ap.get("columns") or []
            rows = ap.get("rows") or []
            tname = ap.get("title", "table")
            # Normalize columns — LLMs sometimes emit [{"name": "Col"}] instead of ["Col"]
            cols = [c.get("name") or c.get("label") or str(c) if isinstance(c, dict) else str(c) for c in cols]
            raw_artifacts.append({"type": "table", "data": ap, "title": tname})
            if tname:
                text_parts.append(f"**{tname}**")
            if _PANDAS and cols:
                try:
                    df = pd.DataFrame(rows, columns=cols)
                    elements.append(cl.Dataframe(name=tname, data=df, display="inline"))
                except Exception:
                    # Fallback to markdown if DataFrame construction fails
                    md = "| " + " | ".join(cols) + " |\n"
                    md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
                    for row in rows:
                        md += "| " + " | ".join(str(c) for c in (row.values() if isinstance(row, dict) else row)) + " |\n"
                    text_parts.append(md)
            else:
                # Markdown table fallback
                md = "| " + " | ".join(cols) + " |\n"
                md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
                for row in rows:
                    md += "| " + " | ".join(str(c) for c in (row.values() if isinstance(row, dict) else row)) + " |\n"
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


async def _push_sidebar(new_elements: list | None = None, title: str | None = None) -> None:
    """Add elements to the sidebar and refresh it. Maintains a session-level list."""
    registry: list = cl.user_session.get("_sidebar_elements") or []
    if new_elements:
        registry.extend(new_elements)
        cl.user_session.set("_sidebar_elements", registry)
    if not registry:
        return
    count = len(registry)
    sidebar_title = title or f"📦 Artifacts ({count})"
    await cl.ElementSidebar.set_title(sidebar_title)
    await cl.ElementSidebar.set_elements(registry)


async def _restore_assets(thread_id: str) -> None:
    """Rebuild the sidebar from assets saved for this thread in CB."""
    from supportal.agent_tools import _list_assets_from_cb, _get_asset_content_from_cb

    profile = cl.user_session.get("profile") or _load_cb_settings()
    cb_a = _cb_args_assets(profile)
    if not cb_a[2]:
        return

    # Fetch asset list for this thread via the thread_id stored in session_id field
    try:
        all_assets = await asyncio.to_thread(_list_assets_from_cb, *cb_a, "", "", 100)
        thread_assets = [a for a in all_assets if a.get("session_id") == thread_id]
    except Exception:
        return

    if not thread_assets:
        return

    elements: list = []
    for a in thread_assets:
        try:
            aid   = a.get("id") or ""
            atype = a.get("asset_type") or ""
            title = a.get("title") or a.get("filename") or aid[:8]
            doc   = await asyncio.to_thread(_get_asset_content_from_cb, *cb_a, aid)
            content = doc.get("content") or ""
            if atype in ("chart", "echart"):
                option = json.loads(content)
                elements.append(_echart_element(option, title))
            elif atype == "table":
                data = json.loads(content)
                cols = data.get("columns") or []
                rows = data.get("rows") or []
                cols = [c.get("name") or c.get("label") or str(c) if isinstance(c, dict) else str(c) for c in cols]
                if _PANDAS and cols:
                    elements.append(cl.Dataframe(name=title, data=pd.DataFrame(rows, columns=cols), display="side"))
                else:
                    elements.append(cl.Text(name=title, content=content[:2000], display="side"))
            elif atype in ("report", "html"):
                elements.append(cl.Text(name=title, content=content[:4000], display="side"))
            else:
                elements.append(cl.Text(name=title, content=content[:2000], display="side"))
        except Exception:
            continue

    if elements:
        cl.user_session.set("_sidebar_elements", elements)
        await _push_sidebar(title=f"📦 Artifacts — restored ({len(elements)})")


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
    actions.append(
        cl.Action(
            name="prompt_library", value=customer or "", payload={"customer": customer or ""},
            label="📚 Prompt Library",
            description="Browse curated prompts by category",
        )
    )
    actions.append(
        cl.Action(
            name="show_assets", value=customer or "", payload={"customer": customer or ""},
            label="📦 Assets",
            description="Browse saved charts, reports, and tables",
        )
    )
    await cl.Message(content="**Quick Actions**", actions=actions, author="Corax").send()


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
            description="Leave blank to use the model saved in your Strabo profile.",
            initial="",
        ),
        cl.input_widget.TextInput(
            id="api_key", label="API Key override",
            description="Leave blank to use the key saved in your Strabo profile.",
            initial="",
        ),
        cl.input_widget.Slider(
            id="agent_context_depth", label="History depth",
            description="Number of prior messages included in each agent call (default 10).",
            initial=10, min=2, max=40, step=2,
        ),
        cl.input_widget.Slider(
            id="top_k", label="Top-K retrieval",
            description="Documents returned from RRF fusion before the LLM context is built (default 10).",
            initial=10, min=1, max=100, step=1,
        ),
    ]).send()

    cl.user_session.set("profile", profile)
    cl.user_session.set("customer", "")
    cl.user_session.set("overrides", {"provider": provider})
    cl.user_session.set("history", [])

    # Associate this thread with the current user so it appears in the sidebar.
    # Mark it with a 24-hour TTL — if no user message arrives, Couchbase will
    # automatically expire the doc.  The first on_message update_thread() call
    # does a plain upsert (no preserve_expiry), which resets TTL to 0 (permanent).
    dl = cl_data.get_data_layer()
    if dl:
        try:
            user = cl.context.session.user
            uid = getattr(user, "id", None)
            if uid:
                await dl.update_thread(cl.context.session.thread_id, user_id=uid)
            if hasattr(dl, "touch_thread_ttl"):
                await dl.touch_thread_ttl(cl.context.session.thread_id, 86400)
        except Exception:
            pass

    _ver = getattr(_get_pipeline(), "__version__", "")
    _ver_str = f" `v{_ver}`" if _ver else ""
    await cl.Message(
        content=(
            f"**Corax**{_ver_str} — Supportal AI chat\n\n"
            "Open the **⚙ Settings** panel to set your customer, LLM provider, and Top-K retrieval.\n"
            "Your Couchbase connection is loaded from the active Strabo profile automatically.\n\n"
            "Ask anything about your support tickets, request charts or tables, "
            "or ask me to refresh a specific ticket from Supportal."
        ),
        author="Corax",
    ).send()
    await _send_quick_actions("")
    # Embed any positive feedback that hasn't been vectorised yet (fire-and-forget)
    asyncio.ensure_future(_embed_pending_feedback(profile))
    # Pick up any jobs that started in NiceGUI or a prior Chainlit session
    await _resume_scrape_job_monitors(profile)


@cl.on_settings_update
async def on_settings_update(settings: dict):
    customer  = (settings.get("customer") or "").strip()
    overrides = {
        "provider":            settings.get("provider") or "",
        "model":               (settings.get("model") or "").strip(),
        "api_key":             (settings.get("api_key") or "").strip(),
        "agent_context_depth": int(settings.get("agent_context_depth") or 10),
        "top_k":               int(settings.get("top_k") or 10),
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
    _prior_block = cl.user_session.get("prior_session_block", "")
    _profile_hint = cl.user_session.get("profile_hint", "")

    # Few-shot injection — find similar positively-rated Q&A pairs from past feedback
    _few_shot: list[dict] = []
    _dl = cl_data.get_data_layer()
    if _dl and hasattr(_dl, "search_similar_positive_feedback"):
        try:
            _app = _get_pipeline()
            _ep, _em, _ek, _eu, _ed = _emb_config(profile)
            _qvec = await asyncio.to_thread(
                _app.embed_text, message.content, _ep, _em, _ek, _eu, _ed
            )
            _few_shot = await _dl.search_similar_positive_feedback(_qvec, top_k=3)
        except Exception:
            pass

    # ── File upload handling ──────────────────────────────────────────────────
    # Chainlit delivers uploaded files as message.elements with type "file".
    # Extract text content, append to LLM message, and add to the sidebar.
    _file_blocks: list[str] = []
    _upload_sidebar_els: list = []
    for _el in (message.elements or []):
        _el_type = getattr(_el, "type", "") or ""
        _el_path = getattr(_el, "path", None) or getattr(_el, "url", None) or ""
        _el_name = getattr(_el, "name", "") or "attachment"
        if _el_type not in ("file", "text") and not _el_path:
            continue
        try:
            if _el_path and not _el_path.startswith("http"):
                with open(_el_path, "r", errors="replace") as _fh:
                    _content = _fh.read(64_000)  # cap at 64K chars
                _file_blocks.append(
                    f'<file name="{_el_name}">\n{_content}\n</file>'
                )
                _upload_sidebar_els.append(
                    cl.Text(name=f"📎 {_el_name}", content=_content[:4000], display="side")
                )
        except Exception as _fe:
            print(f"[corax] file read failed ({_el_name}): {_fe}")
    if _upload_sidebar_els:
        await _push_sidebar(_upload_sidebar_els)

    _user_content = message.content or ""
    if _file_blocks:
        _user_content = _user_content + "\n\n" + "\n\n".join(_file_blocks)

    msgs = [{"role": "system", "content": _system_prompt(customer, _profile_hint, _prior_block, _few_shot)}]
    msgs.extend(history[-_ctx_depth:])
    msgs.append({"role": "user", "content": _user_content})

    # AFTER v1.5.0: live tool-call status message
    # We update this message as each tool fires so users see progress.
    # It stays visible after the agent finishes as a tool trace.
    loop = asyncio.get_event_loop()
    _tool_log: list[str] = []
    status_msg = await cl.Message(content="⏳ Agent starting…", author="Corax").send()

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
        await cl.Message(content=_friendly, actions=[retry_action], author="Corax").send()
        return

    cl.user_session.set("_session_log", agent_ctx.get("_session_log", {}))  # AFTER v1.5.0

    # Spawn a live-updating monitor message for any scrape/rescrape jobs started this turn.
    _cb_tuple = _cb_args(profile)
    for _jid in agent_ctx.get("_started_jobs", []):
        asyncio.create_task(_monitor_job(_jid, app, _cb_tuple))

    history.append({"role": "user",      "content": message.content})
    history.append({"role": "assistant", "content": answer})
    cl.user_session.set("history", history)

    # Persist to shared history — isolated so a CB write failure never kills the session
    try:
        profile = cl.user_session.get("profile") or _load_cb_settings()
        await _save_shared_history(customer, history, profile)
    except Exception as _se:
        print(f"[corax] _save_shared_history failed (non-fatal): {_se}")

    clean_text, elements, raw_artifacts = _parse_artifacts(answer)

    # Push new artifacts into the sidebar immediately
    if elements:
        await _push_sidebar(elements)

    # Persist charts / tables to CB so they survive session restarts
    try:
        await _save_assets(cl.context.session.thread_id, message.content, raw_artifacts)
    except Exception as _ae:
        print(f"[corax] _save_assets failed (non-fatal): {_ae}")

    # Follow-up suggestion chips — isolated so an LLM error here never resets the session
    _sugs: list = []
    try:
        _sugs = await loop.run_in_executor(
            None,
            lambda: app._generate_followup_suggestions(
                message.content, answer, provider, model, api_key, base_url
            ),
        )
    except Exception as _fe:
        print(f"[corax] _generate_followup_suggestions failed (non-fatal): {_fe}")

    _actions = [
        cl.Action(name="followup", value=s, payload={"value": s}, label=s, description="Ask this follow-up")
        for s in _sugs
    ]
    await cl.Message(content=clean_text, elements=elements, actions=_actions, author="Corax").send()


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
            author="Corax",
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
        await cl.Message(content="No runnable saved queries found.", author="Corax").send()
        return
    await cl.Message(
        content=f"**Saved Queries for {customer}** — click to run:",
        actions=sq_actions,
        author="Corax",
    ).send()


@cl.action_callback("run_saved_query")
async def on_run_saved_query(action: cl.Action):
    query = action.payload.get("query") or action.value
    if not query:
        return
    fake_msg = cl.Message(content=query, author="User")
    await on_message(fake_msg)


# ── Prompt Library ────────────────────────────────────────────────────────────

@cl.action_callback("prompt_library")
async def on_prompt_library(action: cl.Action):
    """Show category selection for the prompt library."""
    from supportal.prompt_library import PROMPT_LIBRARY
    customer = action.payload.get("customer") or cl.user_session.get("customer", "")
    cat_actions = [
        cl.Action(
            name="prompt_library_category",
            value=cat["category"],
            payload={"category": cat["category"], "customer": customer},
            label=f"{cat['category']}",
            description=f"{len(cat['prompts'])} prompts",
        )
        for cat in PROMPT_LIBRARY
    ]
    cust_note = f" · customer: **{customer}**" if customer else " · no customer set — customer-specific prompts will work once you set one in ⚙ Settings"
    await cl.Message(
        content=f"**📚 Prompt Library**{cust_note}\n\nChoose a category:",
        actions=cat_actions,
        author="Corax",
    ).send()


@cl.action_callback("prompt_library_category")
async def on_prompt_library_category(action: cl.Action):
    """Show prompts for the selected category."""
    from supportal.prompt_library import get_prompts_for_category, inject_customer
    category = action.payload.get("category") or action.value
    customer = action.payload.get("customer") or cl.user_session.get("customer", "")
    prompts = get_prompts_for_category(category)
    if not prompts:
        await cl.Message(content=f"No prompts found for **{category}**.", author="Corax").send()
        return

    prompt_actions = []
    skipped = []
    for p in prompts:
        if p.get("customer_required") and not customer:
            skipped.append(p["label"])
            continue
        filled = inject_customer(p["prompt"], customer)
        label = inject_customer(p["label"], customer)
        prompt_actions.append(
            cl.Action(
                name="run_library_prompt",
                value=filled,
                payload={"prompt": filled},
                label=label,
                description=filled[:100],
            )
        )
    # Always offer back button
    back_actions = [
        cl.Action(
            name="prompt_library",
            value=customer,
            payload={"customer": customer},
            label="← Back to categories",
            description="Return to category list",
        )
    ]

    lines = [f"**{category}** — click a prompt to run it:"]
    if skipped:
        lines.append(f"\n*{len(skipped)} prompt(s) hidden — set a customer in ⚙ Settings to unlock them.*")

    await cl.Message(
        content="\n".join(lines),
        actions=prompt_actions + back_actions,
        author="Corax",
    ).send()


@cl.action_callback("run_library_prompt")
async def on_run_library_prompt(action: cl.Action):
    """Run a prompt from the library."""
    prompt = action.payload.get("prompt") or action.value
    if not prompt:
        return
    fake_msg = cl.Message(content=prompt, author="User")
    await on_message(fake_msg)


# ── Assets panel ──────────────────────────────────────────────────────────────

_ASSET_EMOJI = {
    "chart": "📊", "echart": "📊",
    "table": "📋", "csv": "📋",
    "report": "📄",
    "json": "📁", "js": "📁", "javascript": "📁", "html": "🌐",
}


def _fmt_asset_date(ts: int) -> str:
    try:
        import datetime as _dt
        return _dt.datetime.fromtimestamp(ts).strftime("%b %d %H:%M")
    except Exception:
        return ""


@cl.action_callback("show_assets")
async def on_show_assets(action: cl.Action):
    """Open the sidebar with all saved CB assets for the current customer."""
    from supportal.agent_tools import _list_assets_from_cb, _get_asset_content_from_cb

    customer = action.payload.get("customer") or cl.user_session.get("customer", "")
    profile  = cl.user_session.get("profile") or _load_cb_settings()
    cb_a     = _cb_args_assets(profile)

    if not cb_a[2]:
        await cl.Message(
            content="⚠ No Couchbase profile configured — assets cannot be loaded.",
            author="Corax",
        ).send()
        return

    try:
        assets = await asyncio.to_thread(_list_assets_from_cb, *cb_a, customer, "", 50)
    except Exception as exc:
        await cl.Message(content=f"⚠ Could not load assets: {exc}", author="Corax").send()
        return

    if not assets:
        label = f" for **{customer}**" if customer else ""
        await cl.Message(
            content=f"No saved assets found{label}. Charts, tables, and reports are saved here automatically as the agent generates them.",
            author="Corax",
        ).send()
        return

    # Fetch content and build sidebar elements
    sidebar_els: list = []
    for a in assets:
        aid   = a.get("id") or ""
        atype = a.get("asset_type") or ""
        title = a.get("title") or a.get("filename") or aid[:8]
        ts    = _fmt_asset_date(a.get("created_at") or 0)
        label = f"{_ASSET_EMOJI.get(atype, '📁')} {title} · {ts}"
        if not aid:
            continue
        try:
            doc     = await asyncio.to_thread(_get_asset_content_from_cb, *cb_a, aid)
            content = doc.get("content") or ""
            if atype in ("chart", "echart"):
                sidebar_els.append(_echart_element(json.loads(content), label))
            elif atype == "table":
                data = json.loads(content)
                cols = data.get("columns") or []
                rows = data.get("rows") or []
                cols = [c.get("name") or c.get("label") or str(c) if isinstance(c, dict) else str(c) for c in cols]
                if _PANDAS and cols:
                    sidebar_els.append(cl.Dataframe(name=label, data=pd.DataFrame(rows, columns=cols), display="side"))
                else:
                    sidebar_els.append(cl.Text(name=label, content=content[:2000], display="side"))
            else:
                sidebar_els.append(cl.Text(name=label, content=content[:4000], display="side"))
        except Exception:
            continue

    if not sidebar_els:
        await cl.Message(content="Assets found but could not be rendered.", author="Corax").send()
        return

    cust_label = f" · {customer}" if customer else ""
    await cl.ElementSidebar.set_title(f"📦 All Assets ({len(sidebar_els)}){cust_label}")
    await cl.ElementSidebar.set_elements(sidebar_els)


@cl.action_callback("preview_asset")
async def on_preview_asset(action: cl.Action):
    """Fetch a single asset from CB and render it inline."""
    from supportal.agent_tools import _get_asset_content_from_cb

    aid    = action.payload.get("asset_id") or action.value
    atype  = action.payload.get("asset_type") or ""
    title  = action.payload.get("title") or aid

    profile = cl.user_session.get("profile") or _load_cb_settings()
    cb_a = _cb_args_assets(profile)

    try:
        doc = await asyncio.to_thread(_get_asset_content_from_cb, *cb_a, aid)
    except Exception as exc:
        await cl.Message(content=f"⚠ Could not fetch asset: {exc}", author="Corax").send()
        return

    if not doc:
        await cl.Message(content=f"Asset `{aid}` not found.", author="Corax").send()
        return

    content = doc.get("content") or ""
    atype   = doc.get("asset_type") or atype

    if atype in ("chart", "echart"):
        try:
            option = json.loads(content)
            await cl.Message(
                content=f"**{title}**",
                elements=[_echart_element(option, title)],
                author="Corax",
            ).send()
        except Exception:
            await cl.Message(content=f"**{title}**\n```json\n{content[:2000]}\n```", author="Corax").send()

    elif atype == "table":
        try:
            data = json.loads(content)
            cols = data.get("columns") or []
            rows = data.get("rows") or []
            cols = [c.get("name") or c.get("label") or str(c) if isinstance(c, dict) else str(c) for c in cols]
            if _PANDAS and cols:
                df = pd.DataFrame(rows, columns=cols)
                await cl.Message(
                    content=f"**{title}**",
                    elements=[cl.Dataframe(name=title, data=df, display="inline")],
                    author="Corax",
                ).send()
            else:
                await cl.Message(content=f"**{title}**\n```\n{content[:3000]}\n```", author="Corax").send()
        except Exception:
            await cl.Message(content=f"**{title}**\n```\n{content[:3000]}\n```", author="Corax").send()

    elif atype in ("report", "html", "js", "javascript"):
        snippet = content[:4000]
        await cl.Message(content=f"**{title}**\n\n{snippet}", author="Corax").send()

    else:
        await cl.Message(content=f"**{title}**\n```\n{content[:3000]}\n```", author="Corax").send()


@cl.action_callback("delete_asset")
async def on_delete_asset(action: cl.Action):
    """Delete a single asset from CB."""
    from supportal.agent_tools import _delete_asset_from_cb

    aid   = action.payload.get("asset_id") or action.value
    title = action.payload.get("title") or aid

    profile = cl.user_session.get("profile") or _load_cb_settings()
    cb_a = _cb_args_assets(profile)

    try:
        ok = await asyncio.to_thread(_delete_asset_from_cb, *cb_a, aid)
    except Exception as exc:
        await cl.Message(content=f"⚠ Delete failed: {exc}", author="Corax").send()
        return

    if ok:
        await cl.Message(content=f"🗑 Deleted **{title}**.", author="Corax").send()
    else:
        await cl.Message(content=f"Could not delete **{title}** — it may have already been removed.", author="Corax").send()
