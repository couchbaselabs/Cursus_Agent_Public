"""
LLM call routing, RAG context building, and scoring utilities.
"""

from __future__ import annotations

import datetime
import json
import re
import threading
import time
from typing import Callable

import requests

from supportal.prompts import (
    CLASSIFY_PROMPT,
    CRITIQUE_PROMPT,
    EXTRACT_PROMPT,
    RERANK_PROMPT,
    SYSTEM_PROMPT_TEMPLATE,
)
from supportal.snapshot_parser import _topo_str
from supportal.ticket_parser import (
    _extract_ticket_ids,
    _parse_ticket_fields,
)

# ── Optional LLM provider packages ───────────────────────────────────────────
try:
    import anthropic as _anthropic_mod
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _anthropic_mod = None
    _ANTHROPIC_AVAILABLE = False

try:
    from google import genai as _genai_mod
    _GEMINI_AVAILABLE = True
except ImportError:
    _genai_mod = None
    _GEMINI_AVAILABLE = False

try:
    import openai as _openai_mod
    _OPENAI_AVAILABLE = True
except ImportError:
    _openai_mod = None
    _OPENAI_AVAILABLE = False

try:
    import boto3 as _boto3_mod
    _BOTO3_AVAILABLE = True
except ImportError:
    _boto3_mod = None
    _BOTO3_AVAILABLE = False

# ── OpenAI client helpers (also used by embed_text in main) ──────────────────

def _openai_base_url(raw: str, default: str) -> str:
    """Normalise a user-supplied URL so it ends with exactly one /v1."""
    url = (raw or default).rstrip("/")
    if url.endswith("/v1"):
        return url
    return url + "/v1"


_tls_openai = threading.local()


def _get_openai_client(api_key: str, base_url: str):
    """Return a thread-local OpenAI client, creating one if needed."""
    if not _OPENAI_AVAILABLE:
        raise RuntimeError("openai package not installed: venv/bin/pip install openai")
    key = (api_key, base_url)
    if getattr(_tls_openai, "client_key", None) != key:
        _tls_openai.client_key = key
        _tls_openai.client = _openai_mod.OpenAI(
            api_key=api_key or "lmstudio",
            base_url=base_url or None,
        )
    return _tls_openai.client


# ── Dynamic cluster↔application alias maps ────────────────────────────────────
_cluster_app_dynamic: dict[str, str] = {}
_app_cluster_dynamic: dict[str, list[str]] = {}

_APP_CLUSTER_ALIASES_SEED: dict[str, list[str]] = {
    "mle":              ["peuse1cbecpsd2000083", "peusw1cbecpsd2000129", "peuse1cbecpsd000069"],
    "merchant list":    ["peuse1cbecpsd2000083", "peusw1cbecpsd2000129", "peuse1cbecpsd000069"],
    "merchant":         ["peuse1cbecpsd2000083", "peusw1cbecpsd2000129"],
    "safekey":          ["peusw1cbecpsd000102", "peuse1cbecpsd000103"],
    "griffin":          ["peusw1cbecpsd2000303"],
    "digital payments": ["peusw1cbecpsd2000086", "peuse1cbecpsd2000081"],
}


def _get_cluster_to_app() -> dict[str, str]:
    """Merge static seed + dynamic CB data → cluster_name → app label."""
    merged: dict[str, str] = {
        host: app
        for app, hosts in _APP_CLUSTER_ALIASES_SEED.items()
        for host in hosts
        if app not in ("merchant list", "merchant")
    }
    merged.update(_cluster_app_dynamic)
    return merged


def _get_app_cluster_aliases() -> dict[str, list[str]]:
    """Merge static seed + dynamic CB data → app label → [cluster_names]."""
    merged: dict[str, list[str]] = dict(_APP_CLUSTER_ALIASES_SEED)
    for app, hosts in _app_cluster_dynamic.items():
        if app in merged:
            existing = merged[app]
            for h in hosts:
                if h not in existing:
                    existing.append(h)
        else:
            merged[app] = list(hosts)
    return merged


# ── LLM call routing ─────────────────────────────────────────────────────────

def call_llm(
    messages: list[dict],
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
    max_tokens: int = 4096,
    num_ctx: int | None = None,
    no_think: bool = False,
) -> str:
    """Send a messages list to the selected provider and return the response text.

    no_think — when True and provider is "ollama", uses the native /api/chat endpoint
    with think=false instead of the OpenAI-compat path.
    """
    if provider == "claude":
        if not _ANTHROPIC_AVAILABLE:
            raise RuntimeError("anthropic package not installed: venv/bin/pip install anthropic")
        client = _anthropic_mod.Anthropic(api_key=api_key or None)
        system   = next((m["content"] for m in messages if m["role"] == "system"), None)
        user_msgs = [m for m in messages if m["role"] != "system"]
        kwargs: dict = {"model": model, "max_tokens": max_tokens, "messages": user_msgs}
        if system:
            kwargs["system"] = system
        resp = client.messages.create(**kwargs)
        return resp.content[0].text

    elif provider == "gemini":
        if not _GEMINI_AVAILABLE:
            raise RuntimeError("google-genai not installed: venv/bin/pip install google-genai")
        client  = _genai_mod.Client(api_key=api_key)
        system  = next((m["content"] for m in messages if m["role"] == "system"), None)
        non_sys = [m for m in messages if m["role"] != "system"]
        contents = [
            {"role": "user" if m["role"] == "user" else "model", "parts": [{"text": m["content"]}]}
            for m in non_sys
        ]
        config = {"max_output_tokens": max_tokens}
        if system:
            config["system_instruction"] = system
        resp = client.models.generate_content(model=model, contents=contents, config=config)
        return resp.text

    elif provider in ("ollama", "lmstudio"):
        if not _OPENAI_AVAILABLE:
            raise RuntimeError("openai package not installed: venv/bin/pip install openai")
        default = "http://localhost:1234" if provider == "lmstudio" else "http://localhost:11434"

        if no_think and provider == "ollama":
            base = (base_url or default).rstrip("/")
            payload: dict = {
                "model":    model,
                "messages": messages,
                "think":    False,
                "stream":   False,
                "options":  {"num_predict": max_tokens},
            }
            resp = requests.post(f"{base}/api/chat", json=payload, timeout=600, verify=False)
            resp.raise_for_status()
            return resp.json()["message"]["content"]

        _timeout = _openai_mod.Timeout(
            timeout=600.0, connect=180.0
        ) if provider == "lmstudio" else None
        client = _openai_mod.OpenAI(
            api_key=api_key or "lmstudio",
            base_url=_openai_base_url(base_url, default),
            timeout=_timeout,
        )
        kwargs: dict = {"model": model, "messages": messages, "max_tokens": max_tokens}
        if num_ctx and provider == "ollama":
            kwargs["extra_body"] = {"num_ctx": num_ctx}
        # LMStudio's LM Link peer connection times out during idle; the first
        # request after idle 400s with "peer_keepalive_timeout" and the link
        # re-establishes itself, so a short-delay retry succeeds.
        for _attempt in range(3):
            try:
                resp = client.chat.completions.create(**kwargs)
                break
            except Exception as _exc:
                if "lm link" in str(_exc).lower() and _attempt < 2:
                    time.sleep(2 * (_attempt + 1))
                    continue
                raise
        _content = resp.choices[0].message.content
        if isinstance(_content, list):
            _content = "".join(
                p.get("text", "") if isinstance(p, dict) else str(p)
                for p in _content
            )
        return _content or ""

    elif provider == "bedrock":
        if not _BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not installed: venv/bin/pip install boto3")
        region = base_url.strip() if base_url and base_url.strip() else "us-east-1"
        client = _boto3_mod.client("bedrock-runtime", region_name=region)
        system_text = next((m["content"] for m in messages if m["role"] == "system"), None)
        converse_msgs = [
            {"role": m["role"], "content": [{"text": m["content"]}]}
            for m in messages if m["role"] in ("user", "assistant")
        ]
        kwargs: dict = {
            "modelId": model,
            "messages": converse_msgs,
            "inferenceConfig": {"maxTokens": max_tokens},
        }
        if system_text:
            kwargs["system"] = [{"text": system_text}]
        resp = client.converse(**kwargs)
        return resp["output"]["message"]["content"][0]["text"]

    else:
        raise ValueError(f"Unknown LLM provider: {provider!r}")


# ── Query rewriting ───────────────────────────────────────────────────────────

def rewrite_query_for_retrieval(
    question: str,
    chat_history: list[dict],
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
) -> str:
    """Rewrite any question into a focused, self-contained retrieval query."""
    _turns: list[str] = []
    for msg in (chat_history or [])[-6:]:
        role = msg.get("role", "")
        if role == "user":
            _turns.append(f"User: {msg['content']}")
        elif role == "assistant":
            _turns.append(f"Assistant: {msg['content'][:400]}")
    history_block = ("\nConversation so far:\n" + "\n".join(_turns)) if _turns else ""

    _today = datetime.date.today()
    _today_str = _today.isoformat()
    _yr = _today.year
    _lookback = (_today - datetime.timedelta(days=90)).isoformat()
    _prompt = (
        f"You are a query rewriter for a support-ticket retrieval system.\n"
        f"Today's date is {_today_str}.\n"
        f"Your job: extract ONLY the search intent from the user's message — "
        f"what topics, applications, error types, priorities, ticket IDs, or "
        f"time ranges to find — and output a concise retrieval query.\n"
        f"STRIP all output-format instructions (tables, timelines, summaries, "
        f"columns, 'please', 'give me', 'in a table', etc.) — those are for the "
        f"answer formatter, not the retriever.\n"
        f"DATE RESOLUTION (critical): Convert every relative or ambiguous date "
        f"reference to explicit ISO-8601 dates (YYYY-MM-DD). Examples:\n"
        f"  'this year' -> 'from {_yr}-01-01'\n"
        f"  'since January' -> 'from {_yr}-01-01'\n"
        f"  'last quarter' -> compute the previous calendar quarter start/end\n"
        f"  'recent' / 'lately' -> from {_lookback}\n"
        f"  'last month' -> from first day of the previous calendar month\n"
        f"  'in 2025' -> 'from 2025-01-01 to 2025-12-31'\n"
        f"Always include the resolved date range in the output query as "
        f"'from YYYY-MM-DD' or 'from YYYY-MM-DD to YYYY-MM-DD'.\n"
        f"Output ONLY the rewritten query — no explanation, no prefix, no quotes.\n"
        f"{history_block}\n\n"
        f"User message: {question}\n\n"
        f"Retrieval query:"
    )
    try:
        result = call_llm(
            [{"role": "user", "content": _prompt}],
            provider, model, api_key, base_url,
            max_tokens=120,
        )
        rewritten = result.strip().strip('"').strip("'")
        if rewritten and len(rewritten) > 5:
            return rewritten
    except Exception:
        pass
    return question


# ── Ticket date helpers ───────────────────────────────────────────────────────

def _ticket_date(t: dict) -> str:
    """Return the best available ISO date string for a ticket (empty string if none)."""
    return (t.get("created") or t.get("created_at") or t.get("date") or "").strip()


def _parse_ticket_date(t: dict):
    """Parse the ticket date into a datetime, or None."""
    raw = _ticket_date(t)
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(raw[:19], fmt)
        except ValueError:
            continue
    return None


def _ticket_cluster_ids(t: dict) -> list[str]:
    """Extract cluster hostnames/IDs from a ticket."""
    ids: list[str] = []
    topo = t.get("snapshot_topology") or {}
    if isinstance(topo, str):
        try:
            topo = json.loads(topo)
        except Exception:
            topo = {}
    _cname = _topo_str(topo.get("cluster_name"))
    if _cname and _cname not in ids:
        ids.append(_cname)
    _cuuid = _topo_str(topo.get("cluster_uuid"))
    if _cuuid and _cuuid not in ids:
        ids.append(_cuuid)
    for sid in t.get("snap_ids") or []:
        cid = sid.split("::")[0]
        if cid and cid not in ids:
            ids.append(cid)
    _score = t.get("score") or {}
    for cname in (_score.get("cluster_names") or []):
        cname = (cname or "").strip()
        if cname and cname not in ids:
            ids.append(cname)
    _comments_raw = t.get("comments") or []
    if isinstance(_comments_raw, list):
        _comments_text = " ".join(
            str(c.get("body") or c.get("content") or c) for c in _comments_raw
        )[:2000]
    else:
        _comments_text = str(_comments_raw)[:2000]
    _text = " ".join([
        (t.get("subject") or ""),
        (t.get("description") or "")[:500],
        _comments_text,
    ]).lower()
    for host in _get_cluster_to_app():
        if host in _text and host not in ids:
            ids.append(host)
    return ids


# ── Dataset stats and context builders ───────────────────────────────────────

def build_dataset_stats(tickets: list[dict], today_dt: datetime.datetime) -> str:
    """
    Return a pre-computed stats block that grounds the LLM on today's date,
    dataset structure, and a full monthly ticket index.
    """
    if not tickets:
        return ""

    sorted_by_date = sorted(tickets, key=lambda t: _ticket_date(t), reverse=True)

    monthly: dict[str, int] = {}
    for t in tickets:
        dt = _parse_ticket_date(t)
        if dt:
            ym = dt.strftime("%Y-%m")
            monthly[ym] = monthly.get(ym, 0) + 1

    month_lines: list[str] = []
    for ym in sorted(monthly, reverse=True)[:36]:
        yr, mo = int(ym[:4]), int(ym[5:])
        month_name = datetime.date(yr, mo, 1).strftime("%b %Y")
        month_lines.append(f"  {ym} ({month_name}): {monthly[ym]} tickets")

    prio_counts: dict[str, int] = {}
    for t in tickets:
        p = (t.get("priority") or "unknown").strip().upper()
        prio_counts[p] = prio_counts.get(p, 0) + 1

    status_counts: dict[str, int] = {}
    for t in tickets:
        s = (t.get("status") or "unknown").strip().lower()
        status_counts[s] = status_counts.get(s, 0) + 1

    dates = sorted(filter(None, (_ticket_date(t) for t in tickets)))
    date_range = f"{dates[0][:10]} → {dates[-1][:10]}" if dates else "unknown"

    all_cluster_ids: set[str] = set()
    for t in tickets:
        for cid in _ticket_cluster_ids(t):
            all_cluster_ids.add(cid)

    def _days_ago(t: dict) -> int | None:
        dt = _parse_ticket_date(t)
        return (today_dt - dt).days if dt else None

    window_7  = sum(1 for t in tickets if (_days_ago(t) is not None and _days_ago(t) <= 7))
    window_30 = sum(1 for t in tickets if (_days_ago(t) is not None and _days_ago(t) <= 30))
    window_60 = sum(1 for t in tickets if (_days_ago(t) is not None and _days_ago(t) <= 60))
    window_90 = sum(1 for t in tickets if (_days_ago(t) is not None and _days_ago(t) <= 90))

    recent_lines = [
        f"  #{t.get('ticket_id','?')} [{(t.get('priority') or '?').upper()}|{t.get('status','?')}] "
        f"{_ticket_date(t)[:10]} — {(t.get('subject') or '')[:80]}"
        for t in sorted_by_date[:10]
    ]

    most_recent_by_prio: dict[str, dict] = {}
    for t in sorted_by_date:
        p = (t.get("priority") or "unknown").strip().upper()
        if p not in most_recent_by_prio:
            most_recent_by_prio[p] = t
        if len(most_recent_by_prio) >= 6:
            break
    prio_recent_lines = [
        f"  {p}: #{t.get('ticket_id','?')} on {_ticket_date(t)[:10]} — {(t.get('subject') or '')[:70]}"
        for p, t in sorted(most_recent_by_prio.items())
    ]

    prio_str   = " | ".join(f"{k}: {v}" for k, v in sorted(prio_counts.items()))
    status_str = " | ".join(f"{k}: {v}" for k, v in sorted(status_counts.items()))

    lines = [
        "### Dataset Summary",
        "# Use this section for ALL time-based and count-based questions.",
        "# TODAY is the reference point for 'last N months/weeks/days' calculations.",
        f"TODAY:            {today_dt.strftime('%Y-%m-%d (%A)')}",
        f"TOTAL TICKETS:    {len(tickets)}",
        f"DATE RANGE:       {date_range}",
        f"PRIORITY:         {prio_str}",
        f"STATUS:           {status_str}",
        f"UNIQUE CLUSTERS:  {len(all_cluster_ids)}",
        "",
        "### Rolling Window Counts (computed from TODAY — use these for 'last N days/weeks/months' questions)",
        "# 'Last week' = 7 days. 'Last month' = 30 days. 'Last 2 months' = 60 days. 'Last quarter' = 90 days.",
        f"  Last  7 days:  {window_7} tickets",
        f"  Last 30 days:  {window_30} tickets",
        f"  Last 60 days:  {window_60} tickets",
        f"  Last 90 days:  {window_90} tickets",
        "",
        "### Most Recent Ticket Per Priority",
        "# Use this to answer 'most recent P1/P2/P3/P4' questions accurately.",
        *prio_recent_lines,
        "",
        "### Monthly Ticket Counts (calendar months, newest first)",
        "# NOTE: These are CALENDAR month buckets, not rolling windows.",
        "# 'Last month' in a rolling sense = Last 30 days above (not just this calendar month).",
        "# Use Monthly Counts only when the question explicitly names a calendar month or quarter.",
        *month_lines,
        "",
        "### 10 Most Recent Tickets — BACKGROUND REFERENCE ONLY",
        "# These are dataset-wide background context. Do NOT use these IDs to answer",
        "# 'what are the ticket IDs' questions — use only the Retrieved Ticket Context below.",
        *recent_lines,
    ]
    return "\n".join(lines)


def prefilter_for_query(question: str, tickets: list[dict]) -> tuple[list[dict], str]:
    """
    Prepare the ticket list for the LLM context window (sort newest-first,
    pin explicit ticket IDs, filter by explicit priority, apply hard N-limit).
    """
    q = question.lower()
    note_parts: list[str] = []

    mentioned_ids = _extract_ticket_ids(question)
    sorted_all = sorted(tickets, key=lambda t: _ticket_date(t), reverse=True)

    if mentioned_ids:
        pinned  = [t for t in sorted_all if str(t.get("ticket_id", "")) in mentioned_ids]
        rest    = [t for t in sorted_all if str(t.get("ticket_id", "")) not in mentioned_ids]
        result  = pinned + rest
        note_parts.append(f"pinned #{', #'.join(sorted(mentioned_ids))}")
    else:
        result = sorted_all

    prio_map = {"p1": "P1", "p2": "P2", "p3": "P3", "p4": "P4",
                "priority 1": "P1", "priority 2": "P2", "priority 3": "P3",
                "priority 4": "P4"}
    matched_prios = [v for k, v in prio_map.items() if k in q]
    if matched_prios and not mentioned_ids:
        result = [t for t in result
                  if (t.get("priority") or "").strip().upper() in matched_prios]
        note_parts.append(f"{'/'.join(matched_prios)}: {len(result)} tickets")

    m_lim = re.search(r"\b(?:last|top|first|recent|show)\s+(\d+)\b", q)
    if m_lim and not mentioned_ids:
        n = int(m_lim.group(1))
        result = result[:n]
        note_parts.append(f"limited to {n}")

    note = "Context: " + ", ".join(note_parts) + " (sorted newest-first)" if note_parts else "sorted newest-first"
    return result, note


def compute_aggregations(question: str, tickets: list[dict]) -> str:
    """
    For questions requiring counting/grouping/time arithmetic, compute in Python
    and return a pre-computed block the LLM can cite directly.
    """
    q = question.lower()
    today = datetime.datetime.now()
    lines: list[str] = []

    if any(k in q for k in ("cluster", "clusters", "how many cluster")):
        all_cids: set[str] = set()
        for t in tickets:
            for cid in _ticket_cluster_ids(t):
                all_cids.add(cid)
        lines.append(f"UNIQUE CLUSTERS REFERENCED: {len(all_cids)}")
        if all_cids and len(all_cids) <= 20:
            lines.append("CLUSTER IDs: " + ", ".join(sorted(all_cids)))

    if any(k in q for k in ("longest", "longest open", "time", "rca", "resolution", "how long")):
        timed: list[tuple[datetime.timedelta, dict]] = []
        for t in tickets:
            created = _parse_ticket_date(t)
            if not created:
                continue
            closed_raw = (t.get("solved") or t.get("solved_at") or t.get("closed_at") or t.get("updated") or "").strip()
            closed = None
            if closed_raw:
                closed = _parse_ticket_date({"created": closed_raw})
            delta = (closed or today) - created
            timed.append((delta, t))
        if timed:
            timed.sort(key=lambda x: x[0], reverse=True)
            lines.append("\nTICKETS BY OPEN DURATION (longest first):")
            for delta, t in timed[:10]:
                days = delta.days
                still = "" if (t.get("status") or "").lower() in ("solved", "closed") else " (still open)"
                lines.append(
                    f"  #{t.get('ticket_id','?')} [{(t.get('priority') or '?').upper()}] "
                    f"{days}d{still} — {(t.get('subject') or '')[:60]}"
                )

    if any(k in q for k in ("priority", "p1", "p2", "p3", "how many")):
        prio: dict[str, int] = {}
        for t in tickets:
            p = (t.get("priority") or "unknown").strip().upper()
            prio[p] = prio.get(p, 0) + 1
        if prio:
            lines.append("\nPRIORITY DISTRIBUTION (this ticket set):")
            for k, v in sorted(prio.items()):
                lines.append(f"  {k}: {v}")

    if not lines:
        return ""
    return "### Pre-computed Analysis\n" + "\n".join(lines) + "\n"


def build_rag_context(
    tickets: list[dict],
    customer_name: str = "",
    compact: bool = False,
    filter_note: str = "",
    snapshot_map: "dict[str, dict] | None" = None,
) -> str:
    """
    Format a list of ticket dicts as a context block for the LLM system prompt.

    compact=True → single line per ticket; ≤5 tickets → deep-dive; >5 → standard.
    """
    header = "### Retrieved Ticket Context"
    if customer_name:
        header += f" — Customer: {customer_name}"
    if filter_note:
        header += f"\n# This set: {filter_note}. Answer questions using THESE tickets, not the stats summary above."
    elif tickets:
        header += f"\n# {len(tickets)} ticket(s) below — sorted newest-first."
    lines = [header + "\n"]

    deep = (not compact) and len(tickets) <= 5
    _c2a = _get_cluster_to_app()

    for t in tickets:
        tid = t.get("ticket_id", "?")
        cluster_ids = _ticket_cluster_ids(t)
        _cluster_parts = []
        for _cid in cluster_ids[:5]:
            _app = _c2a.get(_cid, "")
            _cluster_parts.append(f"{_cid} ({_app.upper()})" if _app else _cid)
        cluster_str = ", ".join(_cluster_parts) if _cluster_parts else "—"

        _created_str  = _ticket_date(t)[:10]
        _solved_raw   = (t.get("solved") or t.get("solved_at") or t.get("closed_at") or "").strip()
        _resolved_str = _solved_raw[:10] if _solved_raw else ""
        if not _resolved_str and t.get("status", "").lower() in ("closed", "solved"):
            _resolved_str = (t.get("updated") or "").strip()[:10]
        _days_str = ""
        if _created_str and _resolved_str:
            try:
                _c = datetime.datetime.strptime(_created_str, "%Y-%m-%d")
                _r = datetime.datetime.strptime(_resolved_str, "%Y-%m-%d")
                _days_str = f"{max(0, (_r - _c).days)}d"
            except Exception:
                pass

        _app_labels = list({
            _c2a[_cid].upper()
            for _cid in cluster_ids
            if _c2a.get(_cid)
        })
        if not _app_labels:
            _score_t = t.get("score") or {}
            _analytics_labels = _score_t.get("analytics_app_labels") or []
            if _analytics_labels:
                _app_labels = [str(lbl).upper() for lbl in _analytics_labels]
        _app_str = ", ".join(sorted(_app_labels)) if _app_labels else ""

        if compact:
            desc = (t.get("description") or "")[:200].replace("\n", " ")
            _app_tag = f"[Application: {_app_str}]" if _app_str else "[Application: ?]"
            _score_c   = t.get("score") or {}
            _summary_c = (
                t.get("summary_text")
                or t.get("interaction_summary")
                or _score_c.get("interaction_summary")
                or ""
            ).strip()
            _compact_line = (
                f"#{tid} [{(t.get('priority') or '?').upper()}|{t.get('status','?')}] "
                f"{_app_tag} requester:{t.get('requester','?')} "
                f"created:{_created_str or '?'} resolved:{_resolved_str or '?'} "
                f"time:{_days_str or '?'} assignee:{t.get('assignee','?')} "
                f"clusters:{cluster_str} — {t.get('subject','N/A')}"
            )
            _topo_c = t.get("snapshot_topology") or {}
            if isinstance(_topo_c, str):
                try:
                    _topo_c = json.loads(_topo_c)
                except Exception:
                    _topo_c = {}
            if _topo_c and (_topo_c.get("total_nodes") or _topo_c.get("data_nodes") or _topo_c.get("cb_version")):
                _tv   = _topo_c.get("cb_version") or ""
                _tn   = _topo_c.get("total_nodes") or _topo_c.get("node_count") or "?"
                _tbc  = _topo_c.get("bad_count") or len(_topo_c.get("bad_items") or [])
                _twc  = _topo_c.get("warn_count") or len(_topo_c.get("warn_items") or [])
                _tram = _topo_c.get("ram_per_node_mib")
                _tcpu = _topo_c.get("cpus_per_node")
                _snap_note = f" [Snap: {_tn}nodes CB={(_tv or '?')[:12]} bad={_tbc} warn={_twc}"
                if _tram:
                    _snap_note += f" RAM/node={round(int(_tram)/1024)}GB"
                if _tcpu:
                    _snap_note += f" CPU/node={_tcpu}"
                _snap_note += "]"
                _compact_line += _snap_note
            _cbses_c = t.get("cbses") or []
            _jiras_c = t.get("jira_issues") or []
            if _cbses_c:
                _compact_line += f" | CBSEs: {', '.join(_cbses_c) if isinstance(_cbses_c, list) else _cbses_c}"
            if _jiras_c:
                _compact_line += f" | Jira: {', '.join(_jiras_c) if isinstance(_jiras_c, list) else _jiras_c}"
            if _summary_c:
                _compact_line += f" | Summary: {_summary_c[:300].replace(chr(10), ' ')}"
            elif desc:
                _compact_line += f" — {desc}"
            lines.append(_compact_line)
            continue

        _subj_line = f"**Ticket #{tid}** — {t.get('subject', 'N/A')}"
        if _app_str:
            _subj_line += f"  [Application: {_app_str}]"
        lines.append(_subj_line)
        lines.append(
            f"Priority: {(t.get('priority') or '?').upper()} | Status: {t.get('status','?')} "
            f"| Created: {_created_str or '?'} | Resolved: {_resolved_str or '?'} "
            f"| Time-taken: {_days_str or '?'} | Assignee: {t.get('assignee','?')}"
        )
        lines.append(f"Requester: {t.get('requester','?')} | Clusters: {cluster_str}")

        def _render_topo_snap(snap: dict, label: str = "Snapshot") -> None:
            _topo = snap.get("topology") or {}
            def _f(key: str):
                return snap.get(key) or _topo.get(key)
            _svc_parts = []
            for _svc in ("data", "index", "query", "fts", "eventing", "analytics"):
                _n = _f(f"{_svc}_nodes") or 0
                if _n:
                    _svc_parts.append(f"{_n} {_svc}")
            _nodes   = _f("total_nodes") or _f("node_count") or "?"
            _vers    = _f("cb_version") or ""
            _buckets = _f("bucket_names") or []
            _ram_mib = _f("ram_per_node_mib")
            _cpus    = _f("cpus_per_node")
            _groups  = _f("server_groups") or []
            _afo     = _f("auto_failover_seconds")
            _topo_line = (
                f"  {label} [{(snap.get('date') or '?')[:10]}]: "
                f"Cluster={snap.get('cluster_name') or '?'} | "
                f"Nodes={_nodes} ({', '.join(_svc_parts) if _svc_parts else '?'}) | "
                f"CB={_vers or '?'}"
            )
            if _ram_mib:
                _topo_line += f" | RAM/node={round(int(_ram_mib)/1024)}GB"
            if _cpus:
                _topo_line += f" | CPU/node={_cpus}"
            if _groups:
                _topo_line += f" | ServerGroups={len(_groups)}({','.join(str(g) for g in _groups[:4])})"
            if _afo:
                _topo_line += f" | AutoFailover={_afo}s"
            _warns = _f("warn_items") or []
            _bads  = _f("bad_items")  or []
            if _bads:
                _topo_line += f" | Issues({len(_bads)}): {', '.join(str(b) for b in _bads[:5])}"
            if _warns:
                _topo_line += f" | Warnings({len(_warns)}): {', '.join(str(w) for w in _warns[:5])}"
            if _buckets:
                _topo_line += f" | Buckets: {', '.join(str(b) for b in _buckets[:6])}"
            lines.append(_topo_line)

        _topo_rendered = False
        if snapshot_map and cluster_ids:
            for _cid in cluster_ids[:3]:
                _snap = snapshot_map.get(_cid)
                if _snap and (_snap.get("node_count") or _snap.get("cb_version")):
                    _render_topo_snap(_snap)
                    _topo_rendered = True

        if not _topo_rendered:
            _topo_inline = t.get("snapshot_topology") or {}
            if isinstance(_topo_inline, str):
                try:
                    _topo_inline = json.loads(_topo_inline)
                except Exception:
                    _topo_inline = {}
            if _topo_inline and (
                _topo_inline.get("total_nodes") or _topo_inline.get("node_count")
                or _topo_inline.get("data_nodes") or _topo_inline.get("cb_version")
            ):
                _render_topo_snap(_topo_inline, label="Cluster Snapshot")

        if t.get("tags"):
            lines.append(f"Tags: {t['tags']}")

        tf = _parse_ticket_fields(t)
        if tf:
            tf_pairs = [f"{k}: {v}" for k, v in tf.items() if v and str(v).strip()]
            if tf_pairs:
                lines.append("Fields: " + " | ".join(tf_pairs[:20 if deep else 8]))

        if t.get("escalations"):
            lines.append(f"Escalations: {str(t['escalations'])[:500]}")
        _cbses_r = t.get("cbses")
        if _cbses_r:
            _cbses_str = ", ".join(_cbses_r) if isinstance(_cbses_r, list) else str(_cbses_r)
            lines.append(f"CBSEs: {_cbses_str}")
        _jira_r = t.get("jira_issues")
        if _jira_r:
            _jira_str_r = ", ".join(_jira_r) if isinstance(_jira_r, list) else str(_jira_r)
            lines.append(f"Jira Issues: {_jira_str_r}")
        if deep and t.get("snapshots"):
            lines.append(f"Snapshots: {str(t['snapshots'])[:500]}")

        _score_s  = t.get("score") or {}
        _summary  = (
            t.get("summary_text")
            or t.get("interaction_summary")
            or _score_s.get("interaction_summary")
            or ""
        ).strip()
        if _summary:
            lines.append(f"Summary: {_summary}")
        if t.get("description"):
            desc = t["description"] if deep else t["description"][:1_500]
            if deep or not _summary:
                lines.append(f"Description:\n{desc}")

        comments_raw = t.get("comments")
        if comments_raw:
            try:
                comments = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
                comments = sorted(comments, key=lambda c: c.get("timestamp") or "")
                max_comments = len(comments) if deep else 8
                for c in comments[:max_comments]:
                    body = (c.get("body") or "").strip()
                    if not body:
                        continue
                    body = body if deep else body[:600]
                    lines.append(f"  [{c.get('timestamp','')}] {c.get('author','')}: {body}")
            except Exception:
                pass

        lines.append("")

    lines.append("--- END CONTEXT ---")
    return "\n".join(lines)


# ── Multi-step reasoning pipeline ─────────────────────────────────────────────

def classify_query(
    question: str,
    provider: str, model: str, api_key: str, base_url: str,
) -> str:
    """Return one of: FACTUAL AGGREGATION RANKING TREND COMPARISON OPEN."""
    try:
        resp = call_llm(
            [{"role": "user", "content": CLASSIFY_PROMPT.format(question=question)}],
            provider, model, api_key, base_url,
            max_tokens=16, no_think=True,
        )
        word = resp.strip().upper().split()[0] if resp.strip() else "OPEN"
        valid = {"FACTUAL", "AGGREGATION", "RANKING", "TREND", "COMPARISON", "OPEN"}
        return word if word in valid else "OPEN"
    except Exception:
        return "OPEN"


def extract_ticket_fields(
    question: str,
    tickets: list[dict],
    provider: str, model: str, api_key: str, base_url: str,
) -> list[dict]:
    """First LLM pass: extract structured fields from raw ticket dicts."""
    if not tickets:
        return tickets
    sample = tickets[:40]
    tickets_json = json.dumps([
        {k: t.get(k) for k in ("ticket_id", "subject", "status", "priority",
                                "cluster_id", "created_at", "description")}
        for t in sample
    ], ensure_ascii=False)
    try:
        raw = call_llm(
            [{"role": "user", "content": EXTRACT_PROMPT.format(
                question=question, tickets_json=tickets_json)}],
            provider, model, api_key, base_url,
            max_tokens=4096, no_think=True,
        )
        clean = re.sub(r"^```[a-z]*\n?", "", raw.strip(), flags=re.IGNORECASE)
        clean = re.sub(r"\n?```$", "", clean.strip())
        extracted = json.loads(clean)
        if isinstance(extracted, list) and extracted:
            return extracted
    except Exception:
        pass
    return sample


def rerank_tickets(
    question: str,
    tickets: list[dict],
    provider: str, model: str, api_key: str, base_url: str,
    top_k: int = 10,
) -> list[dict]:
    """Second LLM pass: score each ticket for relevance; return top_k reranked."""
    if not tickets:
        return tickets
    summaries = []
    for t in tickets:
        tid  = t.get("ticket_id") or t.get("ticket_id", "?")
        subj = (t.get("subject") or "")[:80]
        pri  = t.get("priority") or "?"
        sta  = t.get("status")   or "?"
        cid  = t.get("cluster_id") or t.get("cluster_name") or "?"
        dt   = (t.get("created_at") or "")[:10]
        summaries.append(f"{tid}: [{pri}] [{sta}] {subj} | cluster={cid} date={dt}")

    prompt = RERANK_PROMPT.format(
        question=question,
        ticket_summaries="\n".join(summaries),
    )
    try:
        raw = call_llm(
            [{"role": "user", "content": prompt}],
            provider, model, api_key, base_url,
            max_tokens=len(tickets) * 8 + 64, no_think=True,
        )
        scores: dict[str, float] = {}
        for line in raw.strip().splitlines():
            m = re.match(r"(\S+?):\s*(\d+)", line.strip())
            if m:
                scores[m.group(1)] = float(m.group(2))

        def _score(t):
            tid = str(t.get("ticket_id", ""))
            return scores.get(tid, 0.0)

        ranked = sorted(tickets, key=_score, reverse=True)
        return ranked[:top_k]
    except Exception:
        return tickets[:top_k]


def self_critique_answer(
    question: str,
    answer: str,
    context: str,
    provider: str, model: str, api_key: str, base_url: str,
) -> str:
    """Third LLM pass: verify correctness; return revised answer or original."""
    if not answer.strip():
        return answer
    try:
        resp = call_llm(
            [{"role": "user", "content": CRITIQUE_PROMPT.format(
                question=question, context=context[:3000], answer=answer)}],
            provider, model, api_key, base_url,
            max_tokens=2048, no_think=True,
        )
        if resp.strip().startswith("APPROVED"):
            return answer
        return resp.strip() or answer
    except Exception:
        return answer


def run_deep_reasoning(
    question: str,
    tickets: list[dict],
    today_str: str,
    stats_block: str,
    provider: str, model: str, api_key: str, base_url: str,
    progress_cb: Callable[[str], None] | None = None,
) -> str:
    """Multi-stage pipeline for small/low-density models."""
    def _log(msg: str):
        if progress_cb:
            progress_cb(msg)

    _log("Deep Reasoning — Stage 1: classifying query …")
    q_type = classify_query(question, provider, model, api_key, base_url)
    _log(f"Deep Reasoning — query type: {q_type}")

    context_tickets, _pf_note = prefilter_for_query(question, tickets)
    _log(f"Deep Reasoning — Stage 2: {len(context_tickets)} tickets ({_pf_note})")
    agg_block = compute_aggregations(question, context_tickets)

    type_hints = {
        "FACTUAL":     "Focus on exact details: ticket ID, date, cluster, resolution.",
        "AGGREGATION": "Narrate the pre-computed counts from the Dataset Summary and "
                       "Pre-computed Analysis. Do NOT recount manually.",
        "RANKING":     "List items in the order shown in Pre-computed Analysis.",
        "TREND":       "Describe the pattern using the Monthly Ticket Counts table. "
                       "Sum months as needed — that is valid, not invented.",
        "COMPARISON":  "Compare the two groups side-by-side using the data provided.",
        "OPEN":        "Give a concise answer grounded in the ticket data.",
    }

    if q_type in ("AGGREGATION", "TREND"):
        _log("Deep Reasoning — Stage 3: synthesizing (aggregation fast-path) …")
        context_block = (agg_block + "\n" if agg_block else "") + \
                        build_rag_context(context_tickets[:20], "", compact=True)
        system_msg = SYSTEM_PROMPT_TEMPLATE.format(
            today=today_str, stats=stats_block, context=context_block,
        ) + f"\n\nQuery type: {q_type}. {type_hints[q_type]}"
        answer = call_llm(
            [{"role": "system", "content": system_msg},
             {"role": "user",   "content": question}],
            provider, model, api_key, base_url, max_tokens=1024,
        )
        _log(f"Deep Reasoning — complete (3 stages, {q_type} fast-path)")
        return answer

    if q_type == "RANKING":
        _log("Deep Reasoning — Stage 3: reranking …")
        top_tickets = rerank_tickets(
            question, context_tickets, provider, model, api_key, base_url, top_k=12
        )
        _log("Deep Reasoning — Stage 4: synthesizing …")
        context_block = (agg_block + "\n" if agg_block else "") + \
                        build_rag_context(top_tickets, "", compact=True)
        system_msg = SYSTEM_PROMPT_TEMPLATE.format(
            today=today_str, stats=stats_block, context=context_block,
        ) + f"\n\nQuery type: RANKING. {type_hints['RANKING']}"
        answer = call_llm(
            [{"role": "system", "content": system_msg},
             {"role": "user",   "content": question}],
            provider, model, api_key, base_url, max_tokens=2048,
        )
        _log("Deep Reasoning — complete (4 stages, RANKING path)")
        return answer

    _log("Deep Reasoning — Stage 3: extracting structured fields …")
    extract_ticket_fields(question, context_tickets, provider, model, api_key, base_url)

    _log("Deep Reasoning — Stage 4: reranking by relevance …")
    top_tickets = rerank_tickets(
        question, context_tickets, provider, model, api_key, base_url, top_k=12
    )

    _log("Deep Reasoning — Stage 5: synthesizing answer …")
    context_block = (agg_block + "\n" if agg_block else "") + \
                    build_rag_context(top_tickets, "", compact=True)
    system_msg = SYSTEM_PROMPT_TEMPLATE.format(
        today=today_str, stats=stats_block, context=context_block,
    ) + f"\n\nQuery type: {q_type}. {type_hints.get(q_type, '')}"
    answer = call_llm(
        [{"role": "system", "content": system_msg},
         {"role": "user",   "content": question}],
        provider, model, api_key, base_url, max_tokens=4096,
    )

    _log("Deep Reasoning — Stage 6: self-critique …")
    answer = self_critique_answer(
        question, answer, context_block, provider, model, api_key, base_url
    )

    _log(f"Deep Reasoning — complete (6 stages, {q_type} path)")
    return answer


def _build_memory_section(memories: list[dict]) -> str:
    """Format a list of chat memory dicts into a ### Previous Session Memory block."""
    if not memories:
        return ""
    lines = ["### Previous Session Memory\n"
             "The following summaries are from prior chat sessions with this ticket corpus. "
             "Use them for continuity but treat the ticket context above as authoritative.\n"]
    for m in memories:
        q   = (m.get("question") or "").strip()
        ans = (m.get("answer_summary") or "").strip()
        ts  = (m.get("created_at") or "").strip()
        if q and ans:
            ts_part = f" [{ts}]" if ts else ""
            lines.append(f"**Q{ts_part}:** {q}\n**A (summary):** {ans}\n")
    return "\n".join(lines)


_FOLLOWUP_TRIGGERS = re.compile(
    r"\b(of (those|them|the(se|m)?|those issues|those tickets|the issues?|the tickets?)"
    r"|out of|from those|from them|among those|among them"
    r"|how many (of|were|had|have|did|do)"
    r"|which (of|ones|tickets?|issues?)"
    r"|what (about|were|was|is|are) (those|them|the)"
    r"|same (tickets?|issues?|period|year|month)"
    r")\b",
    re.IGNORECASE,
)


def contextualize_question(
    question: str,
    chat_history: list[dict],
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
) -> str:
    """Rewrite a follow-up question to be self-contained using recent conversation history."""
    if not provider or not model or not chat_history:
        return question
    if not _FOLLOWUP_TRIGGERS.search(question):
        return question

    recent = [m for m in chat_history[-4:] if m.get("role") in ("user", "assistant")]
    if not recent:
        return question

    history_text = "\n".join(
        f"{'User' if m['role'] == 'user' else 'Assistant'}: "
        f"{(m.get('content') or '')[:600]}"
        for m in recent
    )
    prompt = (
        "Given the conversation excerpt below, rewrite the LAST USER QUESTION so it is "
        "fully self-contained: replace pronouns and vague references ('those', 'them', "
        "'the issues opened', 'out of those', etc.) with the explicit date ranges, "
        "application names, ticket IDs, or other context they refer to. "
        "If the question is already unambiguous, return it unchanged. "
        "Return ONLY the rewritten question — no explanation, no quotes, no preamble.\n\n"
        f"Conversation:\n{history_text}\n\n"
        f"Last user question: {question}"
    )
    try:
        rewritten = call_llm(
            [{"role": "user", "content": prompt}],
            provider, model, api_key, base_url,
            max_tokens=150,
        ).strip().strip('"\'')
        if rewritten and rewritten.lower() != question.lower():
            print(f"[contextualize] '{question}' → '{rewritten}'")
            return rewritten
    except Exception as exc:
        print(f"[contextualize] failed: {exc}")
    return question


def chat_batch_map_reduce(
    question: str,
    tickets: list[dict],
    batch_size: int,
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
    progress_cb,
    compact: bool = False,
    max_workers: int = 1,
) -> str:
    """
    Map-reduce RAG: query each batch of tickets with the question, then synthesise.
    Returns the final synthesised answer string.
    """
    import concurrent.futures

    batches = [tickets[i: i + batch_size] for i in range(0, len(tickets), batch_size)]
    partial_answers: list[tuple[int, str]] = []
    _today_dt  = datetime.datetime.now()
    _today_str = _today_dt.strftime("%Y-%m-%d (%A)")
    _stats_block = build_dataset_stats(tickets, _today_dt)
    lock = threading.Lock()
    completed = [0]

    _BATCH_NO_MATCH = "NO_MATCH"
    _batch_instruction = (
        "\n\n━━ BATCH MODE RULES ━━\n"
        "You are processing ONE slice of a larger dataset. Most slices will not contain "
        "tickets matching the question — that is normal and expected.\n"
        "RULE B1 — If NO tickets in this slice match the question, respond with exactly the "
        "two words: NO_MATCH — nothing else.\n"
        "RULE B2 — Only include a ticket if it DIRECTLY matches. Do NOT include tickets "
        "because they share infrastructure, patterns, or implied relationships with matching "
        "tickets. [Application: X] labels are authoritative — do not override them.\n"
        "RULE B3 — Never infer that a ticket belongs to an application unless its header "
        "explicitly shows [Application: THAT_APP] or its subject/description names it directly."
    )

    def _run_batch(idx: int, batch: list[dict]) -> tuple[int, str]:
        context = build_rag_context(batch, "", compact=compact)
        system  = SYSTEM_PROMPT_TEMPLATE.format(today=_today_str, stats=_stats_block, context=context)
        system += _batch_instruction
        msgs    = [
            {"role": "system", "content": system},
            {"role": "user",   "content": question},
        ]
        try:
            ans = call_llm(msgs, provider, model, api_key, base_url, max_tokens=4096)
            return idx, ans.strip()
        except Exception as exc:
            return idx, f"ERROR: {exc}"

    effective = max(1, min(max_workers, len(batches)))
    progress_cb(
        f"Processing {len(batches)} batch(es) × {batch_size} tickets"
        + (f" (parallel={effective})" if effective > 1 else "") + " …"
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=effective) as pool:
        futures = {pool.submit(_run_batch, i, b): i for i, b in enumerate(batches)}
        for fut in concurrent.futures.as_completed(futures):
            idx, ans = fut.result()
            with lock:
                partial_answers.append((idx, ans))
                completed[0] += 1
                progress_cb(f"Batch {completed[0]}/{len(batches)} complete …")

    partial_answers.sort(key=lambda x: x[0])

    _NO_RESULT_PHRASES = (
        "no_match", "no match", "no tickets", "no matching tickets",
        "no relevant", "no results", "none found", "not found",
        "no ticket", "no support ticket",
    )

    def _is_empty_batch(ans: str) -> bool:
        if ans.startswith("ERROR:"):
            return True
        _lower = ans.strip().lower()
        if _lower.rstrip(".,! ") == "no_match":
            return True
        if len(_lower) < 120 and "#" not in ans:
            return True
        if "#" not in ans and any(p in _lower for p in _NO_RESULT_PHRASES):
            return True
        return False

    matching = [(idx, ans) for idx, ans in partial_answers if not _is_empty_batch(ans)]
    n_empty = len(partial_answers) - len(matching)
    ordered = [f"[Batch {idx + 1}]\n{ans}" for idx, ans in matching]

    if not ordered:
        return "No matching tickets found across all batches."

    progress_cb(f"Synthesising {len(ordered)} batch answer(s) ({n_empty} empty batches excluded) …")
    combined = "\n\n".join(ordered)
    synthesis_system = (
        "You are a senior Couchbase support analyst performing the final synthesis step of a "
        "map-reduce analysis. You have been given partial answers from batches that found "
        "MATCHING tickets — batches that found no matches were already excluded.\n\n"
        "CRITICAL RULES:\n"
        "1. Batches that found no results are NOT contradictions — they simply did not contain "
        "matching tickets. Treat all provided partial answers as additive evidence.\n"
        "2. Synthesise into a single coherent answer. Remove exact duplicates but preserve "
        "all unique ticket IDs.\n"
        "3. Never include a ticket from one application in results for a different application. "
        "[Application: X] labels are authoritative.\n"
        "4. Do NOT infer relationships between tickets based on infrastructure patterns. "
        "Only report tickets explicitly identified as matching in the partial answers.\n"
        "5. Cite ticket IDs wherever relevant. Use markdown tables for ticket lists."
    )
    synthesis_msgs = [
        {"role": "system",  "content": synthesis_system},
        {"role": "user",    "content": f"Original question: {question}\n\nPartial answers:\n{combined}"},
    ]
    return call_llm(synthesis_msgs, provider, model, api_key, base_url, max_tokens=8192)
