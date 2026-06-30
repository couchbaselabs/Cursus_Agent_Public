"""
Cursus MCP Server
─────────────────
Exposes internal Supportal/Couchbase tooling as MCP tools consumable by
Claude Desktop, Gemini, Cursor, or any MCP-compatible client.

Transport:
  stdio  (default) — for Claude Desktop / Cursor / local clients
  sse              — for remote clients over HTTP (Gemini, custom shells)

Config:
  Loaded automatically from the active Strabo profile (~/.scraper_settings.json).
  Override any value with environment variables (CB_URL, CB_USER, etc.).

Usage:
  # stdio (Claude Desktop / Cursor)
  venv/bin/python run_mcp.py

  # SSE for remote clients
  venv/bin/python run_mcp.py --transport sse --port 8768

Claude Desktop config (~/.claude/claude_desktop_config.json):
  {
    "mcpServers": {
      "cursus": {
        "command": "/path/to/venv/bin/python",
        "args": ["/path/to/Scraper/run_mcp.py"]
      }
    }
  }
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

# ── Path: ensure project root is importable ───────────────────────────────────
_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

mcp = FastMCP(
    "cursus",
    instructions=(
        "Cursus gives you access to Supportal support ticket data, customer health "
        "signals, scrape job management, and saved assets — all backed by Couchbase. "
        "Use query_tickets or search_tickets to find tickets, get_customer_health for "
        "a single account, get_portfolio_status for a ranked fleet view, and "
        "rescrape_customer_tickets to refresh stale data from Supportal."
    ),
)

# ── Lazy pipeline loader ──────────────────────────────────────────────────────
_pipeline: Any = None


def _app() -> Any:
    global _pipeline
    if _pipeline is None:
        _pipeline = importlib.import_module("apps.strabo.app")
    return _pipeline


# ── Embedding field helpers — pick the right keys based on active provider ────
def _emb_model(profile: dict) -> str:
    p = (profile.get("emb_provider") or "ollama").lower()
    if p == "lmstudio":   return profile.get("emb_lms_model")    or "nomic-embed-text"
    if p == "gemini":     return profile.get("emb_gemini_model")  or "text-embedding-004"
    if p == "openai":     return profile.get("emb_openai_model")  or "text-embedding-3-small"
    if p == "mlx":        return profile.get("emb_mlx_model")     or "nomic-embed-text"
    return profile.get("emb_ollama_model") or "nomic-embed-text"


def _emb_base_url(profile: dict) -> str:
    p = (profile.get("emb_provider") or "ollama").lower()
    if p == "lmstudio":   return profile.get("emb_lms_url")    or "http://localhost:1234"
    return profile.get("emb_ollama_url") or "http://localhost:11434"


def _emb_dims(profile: dict) -> int:
    p = (profile.get("emb_provider") or "ollama").lower()
    if p == "lmstudio":   return int(profile.get("emb_lms_dims")    or 1024)
    if p == "gemini":     return int(profile.get("emb_gemini_dims")  or 768)
    if p == "openai":     return int(profile.get("emb_openai_dims")  or 1536)
    if p == "mlx":        return int(profile.get("emb_mlx_dims")     or 1024)
    return int(profile.get("emb_ollama_dims") or 1024)


# ── Config: active Strabo profile → env var overrides ────────────────────────
def _cfg() -> dict:
    """Load CB + embed config from the active Strabo profile, then apply env overrides."""
    try:
        app = _app()
        s       = app._load_settings_file()
        active  = s.get("__last__", "")
        profile = s.get(active, {}) if active else {}
    except Exception:
        profile = {}

    return {
        "cb_url":     os.environ.get("CB_URL",        profile.get("cb_url",        "couchbase://localhost")),
        "bucket":     os.environ.get("CB_BUCKET",     profile.get("cb_bucket",     "rag")),
        "username":   os.environ.get("CB_USER",       profile.get("cb_user",       "")),
        "password":   os.environ.get("CB_PASS",       profile.get("cb_pass",       "")),
        "use_tls":    os.environ.get("CB_TLS",        str(profile.get("cb_tls", False))).lower() == "true",
        "scope":      os.environ.get("CB_SCOPE",      profile.get("cb_scope",      "transcripts")),
        "collection": os.environ.get("CB_COLLECTION", profile.get("cb_collection", "tickets")),
        "cookie":     os.environ.get("CB_COOKIE",     profile.get("cookie",        "")),
        # Embedding — resolve model/url/dims based on active provider
        "emb_provider": profile.get("emb_provider", "ollama"),
        "emb_model":    _emb_model(profile),
        "emb_api_key":  profile.get("emb_gemini_key") or profile.get("emb_openai_key") or "",
        "emb_base_url": _emb_base_url(profile),
        "emb_dims":     _emb_dims(profile),
        # Scoring
        "score_provider":  profile.get("llm_provider", ""),
        "score_model":     profile.get("lms_model") or profile.get("ollama_chat_model") or profile.get("claude_model") or "",
        "score_api_key":   profile.get("claude_api_key") or profile.get("gemini_llm_key") or profile.get("openai_api_key") or "",
        "score_base_url":  profile.get("emb_lms_url") or profile.get("emb_ollama_url") or "",
        "score_ctx":       int(profile.get("score_ctx") or 0) or None,
        "score_no_think":  bool(profile.get("score_no_think", False)),
        "embed_parallel":  int(profile.get("embed_parallel") or 1),
        "pipeline_embed":  bool(profile.get("pipeline_embed", True)),
        "pipeline_score":  bool(profile.get("pipeline_score", True)),
    }


def _cb_tuple(cfg: dict) -> tuple:
    return (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg["use_tls"], cfg["scope"], cfg["collection"])


# ─────────────────────────────────────────────────────────────────────────────
# TICKETS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def query_tickets(
    organization: str = "",
    status: str = "",
    priority: str = "",
    keyword: str = "",
    days_open: int = 0,
    limit: int = 50,
) -> str:
    """
    Search support tickets using structured filters.
    Returns ticket summaries including subject, status, priority, dates, CBSEs, and JIRA issues.

    Args:
        organization: Customer/org name (partial match). Leave blank for all customers.
        status:       open | pending | solved | closed | on-hold
        priority:     low | normal | high | urgent | p1 | p2
        keyword:      Text to match in subject or description.
        days_open:    Only return tickets open longer than N days (0 = no filter).
        limit:        Max results (default 50, max 200).
    """
    cfg  = _cfg()
    app  = _app()
    args: dict = {}
    if organization: args["organization"] = organization
    if status:       args["status"]       = status
    if priority:     args["priority"]     = priority
    if keyword:      args["keyword"]      = keyword
    if days_open:    args["days_open"]    = days_open
    args["limit"] = min(int(limit), 200)

    try:
        results = app.tool_query_tickets(args, *_cb_tuple(cfg), limit=args["limit"])
        return json.dumps({"tickets": results, "count": len(results)}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def get_ticket(ticket_id: str) -> str:
    """
    Fetch a single ticket by ID with full detail — description, comments, cluster topology,
    CBSEs, JIRA issues, snapshot info, and LLM-generated scores.

    Args:
        ticket_id: Zendesk ticket ID (numeric string, e.g. "12345").
    """
    cfg = _cfg()
    app = _app()
    try:
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        from couchbase.auth import PasswordAuthenticator
        from datetime import timedelta
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        c    = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        c.wait_until_ready(timedelta(seconds=10))
        doc  = c.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"]).get(
            f"ticket::{ticket_id}"
        ).content_as[dict]
        c.close()
        doc.pop("embedding", None)  # strip vector — too large for tool output
        return json.dumps(doc, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc), "ticket_id": ticket_id})


@mcp.tool()
def search_tickets(query: str, organization: str = "", top_k: int = 20) -> str:
    """
    Semantic (vector) search over ticket embeddings — finds tickets by meaning,
    not just keyword match. Great for "authentication failures" or "rebalance issues".

    Args:
        query:        Natural language query.
        organization: Optional customer filter applied after vector search.
        top_k:        Number of results (default 20, max 100).
    """
    cfg = _cfg()
    app = _app()
    try:
        vec  = app.embed_text(
            query,
            cfg["emb_provider"], cfg["emb_model"],
            cfg["emb_api_key"],  cfg["emb_base_url"], cfg["emb_dims"],
        )
        keys = app.vector_search_cb(vec, *_cb_tuple(cfg), min(int(top_k), 100))
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        from couchbase.auth import PasswordAuthenticator
        from datetime import timedelta
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        c    = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        c.wait_until_ready(timedelta(seconds=10))
        col  = c.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"])
        results = []
        org_lo  = organization.lower()
        for key in keys:
            try:
                doc = col.get(key).content_as[dict]
                if org_lo and org_lo not in (doc.get("organization") or "").lower():
                    continue
                doc.pop("embedding", None)
                results.append({k: doc[k] for k in
                    ("ticket_id", "subject", "status", "priority", "organization",
                     "created_at", "last_scraped_at", "cbses", "jira_issues")
                    if k in doc})
            except Exception:
                pass
        c.close()
        return json.dumps({"tickets": results, "count": len(results)}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def score_ticket(ticket_id: str) -> str:
    """
    Run LLM scoring on a ticket — generates star rating (1-5), temperature
    (cold/warm/hot), complexity, resolution_quality, and communication_clarity.
    Saves scores back to Couchbase.

    Args:
        ticket_id: Zendesk ticket ID.
    """
    cfg = _cfg()
    app = _app()
    try:
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        from couchbase.auth import PasswordAuthenticator
        from datetime import timedelta
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        c    = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        c.wait_until_ready(timedelta(seconds=10))
        doc  = c.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"]).get(
            f"ticket::{ticket_id}"
        ).content_as[dict]
        c.close()
        doc.pop("embedding", None)
        scores = app.score_tickets_batch(
            [doc],
            cfg["score_provider"], cfg["score_model"],
            cfg["score_api_key"],  cfg["score_base_url"],
        )
        # Save score back to CB
        if scores:
            from couchbase.cluster import Cluster as _Cl2
            from couchbase.options import ClusterOptions as _CO2
            from couchbase.auth import PasswordAuthenticator as _PA2
            import time as _t
            _c2 = _Cl2(app._cb_conn_str(cfg["cb_url"], cfg["use_tls"]),
                       _CO2(_PA2(cfg["username"], cfg["password"])))
            _c2.wait_until_ready(timedelta(seconds=10))
            _col2 = _c2.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"])
            _sdoc = _col2.get(f"ticket::{ticket_id}").content_as[dict]
            _sdoc["score"] = {**(_sdoc.get("score") or {}), **scores[0], "scored_at": int(_t.time())}
            _col2.upsert(f"ticket::{ticket_id}", _sdoc)
            _c2.close()
        return json.dumps({"ticket_id": ticket_id, "scores": scores}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOMERS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def list_customers(limit: int = 100) -> str:
    """
    List all distinct customer organizations that have tickets in Couchbase,
    with open ticket counts.

    Args:
        limit: Max organizations to return (default 100).
    """
    cfg = _cfg()
    app = _app()
    try:
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, QueryOptions
        from couchbase.auth import PasswordAuthenticator
        from datetime import timedelta
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        c    = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        c.wait_until_ready(timedelta(seconds=10))
        ks   = f"`{cfg['bucket']}`.`{cfg['scope']}`.`{cfg['collection']}`"
        rows = list(c.query(
            f"SELECT t.organization, COUNT(*) AS total_tickets, "
            f"SUM(CASE WHEN t.status IN ['open','pending','on-hold'] THEN 1 ELSE 0 END) AS open_tickets "
            f"FROM {ks} t WHERE t.type='ticket' AND t.organization IS NOT NULL "
            f"GROUP BY t.organization ORDER BY open_tickets DESC LIMIT {int(limit)}",
            QueryOptions(timeout=timedelta(seconds=15)),
        ))
        c.close()
        return json.dumps({"customers": rows, "count": len(rows)}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def get_customer_health(organization: str) -> str:
    """
    Return a health summary for a single customer — open ticket counts by priority,
    average score, recent activity, top CBSEs, and oldest unresolved tickets.

    Args:
        organization: Customer/org name (exact or partial match).
    """
    cfg = _cfg()
    app = _app()
    try:
        result = app.tool_query_tickets(
            {"organization": organization, "limit": 200},
            *_cb_tuple(cfg),
            limit=200,
        )
        if not result:
            return json.dumps({"error": f"No tickets found for '{organization}'"})

        open_tickets  = [t for t in result if t.get("status") in ("open", "pending", "on-hold")]
        p1            = [t for t in open_tickets if (t.get("priority") or "").lower() in ("urgent", "p1")]
        p2            = [t for t in open_tickets if (t.get("priority") or "").lower() in ("high", "p2")]
        scores        = [t.get("score", {}).get("stars") for t in result if t.get("score", {}).get("stars")]
        avg_score     = round(sum(scores) / len(scores), 2) if scores else None

        cbses: dict = {}
        for t in open_tickets:
            for cb in (t.get("cbses") or []):
                cbses[cb] = cbses.get(cb, 0) + 1
        top_cbses = sorted(cbses.items(), key=lambda x: x[1], reverse=True)[:5]

        return json.dumps({
            "organization":  organization,
            "total_tickets": len(result),
            "open_tickets":  len(open_tickets),
            "p1_open":       len(p1),
            "p2_open":       len(p2),
            "avg_score":     avg_score,
            "top_cbses":     [{"cbse": k, "count": v} for k, v in top_cbses],
            "sample_open":   [{"id": t.get("ticket_id"), "subject": t.get("subject"),
                               "priority": t.get("priority"), "status": t.get("status")}
                              for t in p1[:5]],
        }, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def get_portfolio_status(limit: int = 20) -> str:
    """
    Ranked portfolio overview across all customers — sorted by open P1/P2 ticket count.
    Use this as a morning briefing or fleet health check.

    Args:
        limit: Max customers to include (default 20).
    """
    cfg = _cfg()
    app = _app()
    try:
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, QueryOptions
        from couchbase.auth import PasswordAuthenticator
        from datetime import timedelta
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        c    = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        c.wait_until_ready(timedelta(seconds=10))
        ks   = f"`{cfg['bucket']}`.`{cfg['scope']}`.`{cfg['collection']}`"
        rows = list(c.query(
            f"SELECT t.organization, "
            f"COUNT(*) AS total, "
            f"SUM(CASE WHEN t.status IN ['open','pending','on-hold'] THEN 1 ELSE 0 END) AS open_count, "
            f"SUM(CASE WHEN t.status IN ['open','pending','on-hold'] AND t.priority IN ['urgent','p1'] THEN 1 ELSE 0 END) AS p1_open, "
            f"SUM(CASE WHEN t.status IN ['open','pending','on-hold'] AND t.priority IN ['high','p2'] THEN 1 ELSE 0 END) AS p2_open, "
            f"AVG(t.score.stars) AS avg_stars "
            f"FROM {ks} t WHERE t.type='ticket' AND t.organization IS NOT NULL "
            f"GROUP BY t.organization ORDER BY p1_open DESC, p2_open DESC LIMIT {int(limit)}",
            QueryOptions(timeout=timedelta(seconds=20)),
        ))
        c.close()
        return json.dumps({"portfolio": rows, "count": len(rows)}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def get_morning_briefing(
    organizations: list[str] | None = None,
    active_only: bool = True,
    tickets_per_org: int = 100,
) -> str:
    """
    Fleet-wide morning briefing — active ticket summary across your key accounts.
    Returns one entry per org with open/pending/on-hold tickets including subject,
    assignee, priority, LLM scores (temperature, stars), CBSEs, JIRA issues, and
    the AI-generated interaction summary. Runs server-side — no shell execution needed.

    Args:
        organizations:   List of org names to include. Omit for the default fleet
                         (American Express, Western Union, NetDocuments, DaVita,
                          GoDaddy, Convera).
        active_only:     When True (default) only open/pending/on-hold tickets are
                         returned. Set False to include solved/closed for context.
        tickets_per_org: Max tickets to query per org (default 100).
    """
    _ACTIVE = {"open", "pending", "on-hold", "hold", "on_hold"}
    _DEFAULT_FLEET = [
        "American Express",
        "Western Union",
        "NetDocuments",
        "DaVita",
        "GoDaddy",
        "Convera",
    ]

    cfg   = _cfg()
    app   = _app()
    orgs  = organizations if organizations else _DEFAULT_FLEET
    limit = min(int(tickets_per_org), 200)

    briefing: list[dict] = []

    for org in orgs:
        try:
            all_tickets = app.tool_query_tickets(
                {"organization": org, "limit": limit},
                *_cb_tuple(cfg),
                limit=limit,
            )
        except Exception as exc:
            briefing.append({"organization": org, "error": str(exc)})
            continue

        if active_only:
            tickets = [t for t in all_tickets
                       if (t.get("status") or "").lower() in _ACTIVE]
        else:
            tickets = all_tickets

        tickets.sort(key=lambda t: t.get("created") or "", reverse=True)

        ticket_summaries = []
        for t in tickets:
            sc = t.get("score") or {}
            ticket_summaries.append({
                "ticket_id":      t.get("ticket_id"),
                "subject":        t.get("subject"),
                "status":         t.get("status"),
                "priority":       t.get("priority"),
                "assignee":       t.get("assignee"),
                "feature_area":   t.get("feature_area"),
                "created":        (t.get("created") or "")[:10],
                "last_comment_at": (t.get("last_comment_at") or "")[:10],
                "cbses":          t.get("cbses") or [],
                "jira_issues":    t.get("jira_issues") or [],
                "temperature":    sc.get("temperature"),
                "stars":          sc.get("stars"),
                "complexity":     sc.get("complexity"),
                "interaction_summary": sc.get("interaction_summary"),
            })

        p1 = [t for t in tickets if (t.get("priority") or "").lower() in ("urgent", "p1")]
        p2 = [t for t in tickets if (t.get("priority") or "").lower() in ("high", "p2")]

        briefing.append({
            "organization":   org,
            "active_count":   len(tickets),
            "p1_count":       len(p1),
            "p2_count":       len(p2),
            "tickets":        ticket_summaries,
        })

    total_active = sum(e.get("active_count", 0) for e in briefing)
    total_p1     = sum(e.get("p1_count", 0) for e in briefing)

    return json.dumps({
        "briefing_date": __import__("datetime").date.today().isoformat(),
        "orgs_queried":  len(orgs),
        "total_active":  total_active,
        "total_p1":      total_p1,
        "fleet":         briefing,
    }, default=str)


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTIVITY
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def check_connectivity() -> str:
    """
    Check VPN status and reachability of Couchbase and Supportal.

    Returns VPN connection state (macOS scutil), whether Couchbase is reachable,
    and whether Supportal is reachable. Call this before rescrape_customer_tickets
    to confirm the VPN is active — Supportal requires the Couchbase corporate VPN.
    """
    import socket
    import subprocess

    result: dict = {}

    # ── VPN — named services via scutil (macOS), vendor-agnostic ─────────────
    try:
        out = subprocess.run(
            ["scutil", "--nc", "list"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        all_services   = [l.strip() for l in out.splitlines() if l.strip() and "Available" not in l]
        connected_svcs = [l for l in all_services if "(Connected)" in l]
        result["vpn_services"] = {
            "connected": bool(connected_svcs),
            "connected_services": connected_svcs or [],
            "all_services": all_services,
            "note": "Supportal reachability below is the authoritative VPN check.",
        }
    except FileNotFoundError:
        result["vpn_services"] = {"connected": None, "note": "scutil not available (non-macOS)"}
    except Exception as exc:
        result["vpn_services"] = {"connected": None, "error": str(exc)}

    # ── Couchbase ─────────────────────────────────────────────────────────────
    cfg = _cfg()
    cb_host = cfg["cb_url"].replace("couchbases://", "").replace("couchbase://", "").split("/")[0]
    cb_port = 18091 if cfg["use_tls"] else 8091
    try:
        s = socket.create_connection((cb_host, cb_port), timeout=4)
        s.close()
        result["couchbase"] = {"reachable": True, "host": cb_host, "port": cb_port}
    except Exception as exc:
        result["couchbase"] = {"reachable": False, "host": cb_host, "port": cb_port, "error": str(exc)}

    # ── Supportal ─────────────────────────────────────────────────────────────
    try:
        s = socket.create_connection(("supportal.couchbase.com", 443), timeout=5)
        s.close()
        result["supportal"] = {"reachable": True, "host": "supportal.couchbase.com:443"}
    except Exception as exc:
        result["supportal"] = {
            "reachable": False,
            "host": "supportal.couchbase.com:443",
            "error": str(exc),
            "fix": "Connect to the Couchbase corporate VPN — Supportal is an internal host.",
        }

    # ── Summary ───────────────────────────────────────────────────────────────
    vpn_ok       = result.get("vpn_services", {}).get("connected")
    cb_ok        = result.get("couchbase", {}).get("reachable", False)
    supportal_ok = result.get("supportal", {}).get("reachable", False)

    if cb_ok and supportal_ok:
        result["summary"] = "All systems reachable. Scrape tools are ready."
    elif cb_ok and not supportal_ok:
        result["summary"] = "Couchbase OK. Supportal unreachable — connect VPN before scraping."
    elif not cb_ok:
        result["summary"] = "Couchbase unreachable — check CB is running and credentials are correct."
    else:
        result["summary"] = "No connectivity — check VPN and local services."

    return json.dumps(result, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE JOBS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_scrape_status(job_id: str = "") -> str:
    """
    Check the status of background scrape/rescrape jobs.

    Args:
        job_id: Specific 6-char job ID to check. Omit to see all recent jobs.
    """
    app = _app()
    jobs = app._SCRAPE_JOBS
    if not jobs:
        return json.dumps({"message": "No scrape jobs in this session."})

    now = time.time()
    if job_id and job_id in jobs:
        jobs_to_show = [jobs[job_id]]
    elif job_id:
        return json.dumps({"error": f"Job '{job_id}' not found.",
                           "recent_ids": list(jobs)[-5:]})
    else:
        jobs_to_show = list(reversed(list(jobs.values())))[:10]

    out = []
    for j in jobs_to_show:
        elapsed = int(now - j.get("started_at", now))
        out.append({
            "job_id":   j["job_id"],
            "org":      j["org"],
            "mode":     j["mode"],
            "status":   j["status"],
            "phase":    j.get("phase"),
            "processed": j.get("processed", 0),
            "total":    j.get("total"),
            "saved":    j.get("saved", 0),
            "scored":   j.get("scored", 0),
            "errors":   j.get("errors", 0),
            "elapsed_s": elapsed,
            "last_message": j.get("last_message"),
        })
    return json.dumps({"jobs": out}, default=str)


@mcp.tool()
def rescrape_customer_tickets(
    organization: str,
    max_tickets: int = 50,
    stale_hours: int = 24,
    embed: bool = True,
    score: bool = True,
    enrich_snapshots: bool = True,
) -> str:
    """
    Trigger a background rescrape job to refresh stale tickets for a customer
    from Supportal. Returns immediately with a job_id — use get_scrape_status to poll.

    Pipeline stages (all on by default, can be disabled individually):
      1. Scrape     — always runs; fetches fresh ticket data from Supportal
      2. Enrich     — fetches cluster snapshot topology for each ticket (enrich_snapshots)
      3. Embed      — generates vector embeddings for semantic search (embed)
      4. Score      — runs LLM scoring: stars, temperature, complexity (score)

    To resume an interrupted job: set stale_hours=1 so already-refreshed tickets
    (which have fresh timestamps) are skipped automatically.

    Args:
        organization:     Customer org name.
        max_tickets:      Max tickets to refresh (default 50, max 2000).
        stale_hours:      Only refresh tickets not scraped in the last N hours (default 24).
                          Set to 0 to force-refresh everything, 1 to resume an interrupted job.
        embed:            Run embedding after scrape (default True). Set False to skip — useful
                          when LMStudio/Ollama is not running or you want raw data only.
        score:            Run LLM scoring after embed (default True). Set False to skip.
        enrich_snapshots: Fetch cluster snapshot topology for each ticket (default True).
                          Set False for fastest possible scrape with no external calls.
    """
    cfg = _cfg()
    app = _app()
    if not cfg["cookie"]:
        return json.dumps({"error": "No session cookie configured — set CB_COOKIE env var or update your Strabo profile."})
    try:
        run_embed = embed and cfg.get("pipeline_embed", True)
        run_score = score and cfg.get("pipeline_score", True)
        ctx = {
            "cookie":             cfg["cookie"],
            "emb_provider":       cfg["emb_provider"]  if run_embed else "",
            "emb_model":          cfg["emb_model"]     if run_embed else "",
            "emb_api_key":        cfg["emb_api_key"],
            "emb_base_url":       cfg["emb_base_url"],
            "emb_dims":           cfg["emb_dims"],
            "embed_parallel":     cfg.get("embed_parallel", 1),
            "provider":           cfg["score_provider"]  if run_score else "",
            "model":              cfg["score_model"]     if run_score else "",
            "api_key":            cfg["score_api_key"],
            "base_url":           cfg["score_base_url"],
            "score_ctx":          cfg.get("score_ctx"),
            "score_no_think":     cfg.get("score_no_think", False),
            "skip_enrichment":    not enrich_snapshots,
        }
        result = app._execute_agent_tool(
            "rescrape_customer_tickets",
            {"organization": organization, "max_tickets": max_tickets, "stale_hours": stale_hours},
            *_cb_tuple(cfg),
            ctx=ctx,
        )
        stages = []
        if not enrich_snapshots: stages.append("no snapshot enrichment")
        if not run_embed:        stages.append("no embedding")
        if not run_score:        stages.append("no scoring")
        suffix = f" [{', '.join(stages)}]" if stages else ""
        return json.dumps({"result": result, "pipeline": suffix.strip("[]") or "full"}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def cancel_scrape_job(job_id: str) -> str:
    """
    Cancel a running scrape or rescrape job. Tickets already refreshed keep their data.
    After cancellation, resume with rescrape_customer_tickets(stale_hours=1).

    Args:
        job_id: The 6-character job ID to cancel (e.g. "e02827").
    """
    app = _app()
    cfg = _cfg()
    cancel_events = getattr(app, "_JOB_CANCEL_EVENTS", {})
    jobs          = app._SCRAPE_JOBS

    ev = cancel_events.get(job_id)
    if ev:
        ev.set()

    job = jobs.get(job_id)
    if job and job.get("status") == "running":
        job["status"]      = "cancelled"
        job["phase"]       = None
        job["finished_at"] = time.time()
        job["last_message"] = f"Cancelled via MCP at {job.get('processed',0)}/{job.get('total','?')} tickets."
        return json.dumps({"cancelled": True, "job_id": job_id,
                           "processed": job.get("processed", 0),
                           "saved": job.get("saved", 0)})
    return json.dumps({"error": f"Job '{job_id}' not running or not found."})


# ─────────────────────────────────────────────────────────────────────────────
# ASSETS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def list_assets(
    organization: str = "",
    asset_type: str = "",
    limit: int = 50,
) -> str:
    """
    List saved charts, tables, reports, and files stored in Couchbase.

    Args:
        organization: Filter by customer/org (partial match). Leave blank for all.
        asset_type:   echart | table | image | pdf | report | html
        limit:        Max results (default 50).
    """
    from supportal.agent_tools import _list_assets_from_cb
    cfg  = _cfg()
    cb_a = (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg["use_tls"], cfg["scope"])
    try:
        assets = _list_assets_from_cb(*cb_a, organization, asset_type, int(limit))
        # Strip content (large) from listing — use get_asset for full content
        for a in assets:
            a.pop("content", None)
        return json.dumps({"assets": assets, "count": len(assets)}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def get_asset(asset_id: str) -> str:
    """
    Fetch a single asset with full content (chart JSON, table data, report text, etc.).

    Args:
        asset_id: Asset UUID (from list_assets).
    """
    from supportal.agent_tools import _get_asset_content_from_cb
    cfg  = _cfg()
    cb_a = (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg["use_tls"], cfg["scope"])
    try:
        doc = _get_asset_content_from_cb(*cb_a, asset_id)
        if not doc:
            return json.dumps({"error": f"Asset '{asset_id}' not found."})
        # For binary types (image, pdf), omit raw content — too large for tool output
        atype = doc.get("asset_type", "")
        if atype in ("image", "pdf") and len(doc.get("content", "")) > 10_000:
            doc["content"] = f"[{atype.upper()} binary content — {len(doc.get('content',''))} chars base64]"
        return json.dumps(doc, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ─────────────────────────────────────────────────────────────────────────────
# RESOURCES  (read-only data endpoints — appear in clients as browseable sources)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.resource("customers://list")
def resource_customers() -> str:
    """All customers with open ticket counts."""
    return list_customers(limit=200)


@mcp.resource("tickets://{organization}")
def resource_tickets(organization: str) -> str:
    """Open tickets for a specific customer."""
    return query_tickets(organization=organization, status="open", limit=100)


@mcp.resource("health://{organization}")
def resource_health(organization: str) -> str:
    """Health summary for a specific customer."""
    return get_customer_health(organization=organization)
