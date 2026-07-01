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
            {"customer": organization, "max_tickets": max_tickets, "stale_hours": stale_hours},
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
# BRAND / REPORTS
# ─────────────────────────────────────────────────────────────────────────────

_TEMPLATES_DIR = _ROOT / "docs" / "templates"


def _load_template(name: str) -> str:
    return (_TEMPLATES_DIR / name).read_text(encoding="utf-8")


def _ensure_brands_collection(cluster: Any, bucket: str, scope: str = "transcripts") -> None:
    try:
        cm = cluster.bucket(bucket).collections()
        existing = {s.name: {c.name for c in s.collections} for s in cm.get_all_scopes()}
        if "brands" not in existing.get(scope, set()):
            from couchbase.management.collections import CollectionSpec
            cm.create_collection(CollectionSpec("brands", scope_name=scope))
    except Exception:
        pass


@mcp.tool()
def save_customer_brand(
    organization: str,
    primary_color: str = "",
    secondary_color: str = "",
    accent_color: str = "",
    logo_url: str = "",
    font_family: str = "",
    terminology: dict | None = None,
) -> str:
    """
    Save a customer brand kit to Couchbase. The kit is applied when generating
    health or ticket reports for that organization (colors override the default
    Couchbase blue palette; terminology replaces generic labels like 'ticket').

    Args:
        organization:    Customer org name (exact match used for lookups).
        primary_color:   CSS color for the brand's primary shade (e.g. '#0050A0').
        secondary_color: CSS color for the secondary accent.
        accent_color:    CSS color for highlights/CTAs.
        logo_url:        URL or data-URI for the customer logo image.
        font_family:     CSS font-family string to override the default sans-serif.
        terminology:     Dict of label overrides, e.g. {'ticket': 'case', 'P1': 'Sev1'}.
    """
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    from datetime import timedelta
    cfg = _cfg()
    app = _app()
    conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
    try:
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        cl.wait_until_ready(timedelta(seconds=10))
        _ensure_brands_collection(cl, cfg["bucket"], cfg["scope"])
        doc = {
            "type":            "brand",
            "organization":    organization,
            "primary_color":   primary_color,
            "secondary_color": secondary_color,
            "accent_color":    accent_color,
            "logo_url":        logo_url,
            "font_family":     font_family,
            "terminology":     terminology or {},
            "updated_at":      int(time.time()),
        }
        key = f"brand::{organization.lower().replace(' ', '_')}"
        cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("brands").upsert(key, doc)
        cl.close()
        return json.dumps({"saved": True, "organization": organization, "key": key})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@mcp.tool()
def get_customer_brand(organization: str) -> str:
    """
    Retrieve a previously saved brand kit for a customer.

    Args:
        organization: Customer org name.
    """
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    from datetime import timedelta
    cfg = _cfg()
    app = _app()
    conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
    try:
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        cl.wait_until_ready(timedelta(seconds=10))
        key = f"brand::{organization.lower().replace(' ', '_')}"
        result = cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("brands").get(key)
        cl.close()
        return json.dumps(result.content_as[dict], default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc), "organization": organization})


# ── Report HTML builders ──────────────────────────────────────────────────────

def _brand_css_overrides(brand: dict) -> str:
    """Return a <style> block overriding CSS vars from the brand kit (empty if no brand)."""
    lines = []
    if brand.get("primary_color"):
        lines.append(f"    --cb: {brand['primary_color']};")
        lines.append(f"    --cb-light: {brand['primary_color']}22;")
    if brand.get("secondary_color"):
        lines.append(f"    --good: {brand['secondary_color']};")
    if brand.get("accent_color"):
        lines.append(f"    --warn: {brand['accent_color']};")
    if brand.get("font_family"):
        lines.append(f"    font-family: {brand['font_family']}, -apple-system, sans-serif;")
    if not lines:
        return ""
    inner = "\n".join(lines)
    return f"\n<style>\n  :root {{\n{inner}\n  }}\n</style>"


def _build_health_report_html(org: str, tickets: list[dict], report_date: str, brand: dict) -> str:
    """Generate a full health report HTML document from live CB ticket data."""
    import datetime as _dt
    from collections import Counter, defaultdict

    term = brand.get("terminology") or {}
    t_ticket = term.get("ticket", "Ticket")
    t_p1 = term.get("P1", "P1")

    now_ts = _dt.datetime.utcnow()
    all_t = tickets

    # ── Aggregate stats ──────────────────────────────────────────────────────
    total = len(all_t)
    open_t = [t for t in all_t if (t.get("status") or "").lower() in ("open", "pending", "on-hold", "hold")]
    closed_t = [t for t in all_t if (t.get("status") or "").lower() in ("solved", "closed")]
    closed_rate = f"{len(closed_t)/total*100:.1f}%" if total else "—"

    def _priority(t: dict) -> str:
        p = (t.get("priority") or "").lower()
        if p in ("urgent", "p1"): return "P1"
        if p in ("high", "p2"):   return "P2"
        if p in ("normal", "p3"): return "P3"
        if p in ("low", "p4"):    return "P4"
        return "P?"

    priority_counts: Counter = Counter(_priority(t) for t in all_t)
    open_priority: Counter = Counter(_priority(t) for t in open_t)

    # P1 tickets in last 12 months
    cutoff_12mo = (now_ts - _dt.timedelta(days=365)).isoformat()
    p1_12mo = [t for t in all_t if _priority(t) == "P1" and (t.get("created") or "") >= cutoff_12mo]
    p1_90d  = [t for t in all_t if _priority(t) == "P1" and (t.get("created") or "") >= (now_ts - _dt.timedelta(days=90)).isoformat()]

    # Resolution time for solved P1s
    def _res_days(t: dict) -> float | None:
        c = t.get("created") or ""
        u = t.get("updated") or ""
        if c and u:
            try:
                d = (_dt.datetime.fromisoformat(u[:19]) - _dt.datetime.fromisoformat(c[:19])).days
                return max(d, 0)
            except Exception:
                pass
        return None

    solved_p1 = [t for t in p1_12mo if (t.get("status") or "").lower() in ("solved", "closed")]
    p1_res = [_res_days(t) for t in solved_p1 if _res_days(t) is not None]
    avg_p1_res = f"{sum(p1_res)/len(p1_res):.1f}d" if p1_res else "—"

    # Monthly volume — last 10 months
    monthly: dict[str, int] = defaultdict(int)
    for t in all_t:
        c = t.get("created") or ""
        if len(c) >= 7:
            monthly[c[:7]] += 1
    sorted_months = sorted(monthly.keys())[-10:]
    month_counts = [monthly[m] for m in sorted_months]
    max_cnt = max(month_counts, default=1) or 1
    month_labels = [_dt.datetime.strptime(m, "%Y-%m").strftime("%b") for m in sorted_months]

    trend_bars_html = ""
    for i, (lbl, cnt) in enumerate(zip(month_labels, month_counts)):
        cls = "trend-bar current" if i == len(month_labels) - 1 else "trend-bar"
        pct = max(int(cnt / max_cnt * 100), 4)
        trend_bars_html += f'      <div class="trend-col"><div class="{cls}" style="height:{pct}%;"><span class="trend-bar-val">{cnt}</span></div></div>\n'
    trend_labels_html = "".join(f'      <span class="trend-lbl">{l}</span>\n' for l in month_labels)

    # Priority mix bar
    tot_nonzero = max(sum(priority_counts.values()), 1)
    def _pct(k): return f"{priority_counts.get(k, 0) / tot_nonzero * 100:.1f}"
    mix_bar_html = f"""
      <div class="mix-seg" style="width:{_pct('P1')}%;background:var(--crit);" title="P1:{priority_counts.get('P1',0)}">P1</div>
      <div class="mix-seg" style="width:{_pct('P2')}%;background:var(--warn);" title="P2:{priority_counts.get('P2',0)}">P2</div>
      <div class="mix-seg" style="width:{_pct('P3')}%;background:var(--cb);" title="P3:{priority_counts.get('P3',0)}">P3</div>
      <div class="mix-seg" style="width:{_pct('P4')}%;background:var(--neutral);" title="P4:{priority_counts.get('P4',0)}">P4</div>"""

    mix_legend_html = "".join(
        f'      <div class="mix-legend-item"><span class="mix-swatch" style="background:var(--{col});"></span> {pri} — {priority_counts.get(pri,0)} ({_pct(pri)}%)</div>\n'
        for pri, col in [("P1","crit"),("P2","warn"),("P3","cb"),("P4","neutral")]
    )

    # Feature area bars — top 6 lifetime
    area_counter: Counter = Counter()
    for t in all_t:
        fa = (t.get("feature_area") or "").strip()
        if fa:
            area_counter[fa] += 1
    top_areas = area_counter.most_common(6)
    max_area = top_areas[0][1] if top_areas else 1
    hbar_html = ""
    for area, cnt in top_areas:
        pct = int(cnt / max_area * 100)
        hbar_html += f'    <div class="hbar-row"><div class="hbar-label">{area}</div><div class="hbar-track"><div class="hbar-fill" style="width:{pct}%;"></div></div><div class="hbar-val">{cnt}</div></div>\n'

    # Recent 90-day feature areas
    cutoff_90 = (now_ts - _dt.timedelta(days=90)).isoformat()
    recent_t = [t for t in all_t if (t.get("created") or "") >= cutoff_90]
    recent_area: Counter = Counter()
    for t in recent_t:
        fa = (t.get("feature_area") or "").strip()
        if fa:
            recent_area[fa] += 1
    top_recent = recent_area.most_common(4)
    max_recent = top_recent[0][1] if top_recent else 1
    hbar_recent_html = ""
    for area, cnt in top_recent:
        pct = int(cnt / max_recent * 100)
        hbar_recent_html += f'    <div class="hbar-row"><div class="hbar-label">{area}</div><div class="hbar-track"><div class="hbar-fill recent" style="width:{pct}%;"></div></div><div class="hbar-val">{cnt}</div></div>\n'

    # P1 incident log
    p1_rows_html = ""
    for t in sorted(p1_12mo, key=lambda x: x.get("created") or "", reverse=True)[:10]:
        tid = t.get("ticket_id", "")
        subj = (t.get("subject") or "—")[:70]
        assignee = t.get("assignee") or "—"
        opened = (t.get("created") or "")[:10]
        status = (t.get("status") or "").lower()
        if status in ("solved", "closed"):
            rd = _res_days(t)
            rd_str = f"{rd:.0f} days" if rd is not None else "—"
            pill_cls = "days-fast" if rd is not None and rd < 2 else ("days-mid" if rd is not None and rd < 14 else "days-slow")
            res_cell = f'<span class="days-pill {pill_cls}">{rd_str}</span>'
        else:
            res_cell = '<span class="days-pill days-open">Open</span>'
        p1_rows_html += f"          <tr><td>#{tid}</td><td><strong>{subj}</strong></td><td>{assignee}</td><td>{opened}</td><td>{res_cell}</td></tr>\n"

    # Open ticket list
    open_rows_html = ""
    for t in sorted(open_t, key=lambda x: x.get("created") or "")[:8]:
        tid = t.get("ticket_id", "")
        subj = (t.get("subject") or "—")[:65]
        pri = _priority(t)
        pri_cls = {"P1":"crit","P2":"warn","P3":"cb","P4":"neutral"}.get(pri, "neutral")
        created = t.get("created") or ""
        age_days = (now_ts - _dt.datetime.fromisoformat(created[:19])).days if created else 0
        age_str = f"{age_days} days old"
        open_rows_html += f'      <div class="mini-row"><span class="mini-id">#{tid}</span><span class="mini-subject">{subj}</span><span class="pill pill-{pri_cls}">{pri}</span><span class="mini-age">{age_str}</span></div>\n'

    # KPI open breakdown
    open_breakdown = " · ".join(f"{v} {k}" for k, v in sorted(open_priority.items()) if v)

    brand_css = _brand_css_overrides(brand)
    logo_html = f'<img src="{brand["logo_url"]}" alt="{org} logo" style="height:28px;object-fit:contain;">' if brand.get("logo_url") else ""

    tpl = _load_template("customer_health_report_template.html")

    # Inject brand CSS after <head>
    if brand_css:
        tpl = tpl.replace("</head>", f"{brand_css}\n</head>", 1)

    # Simple token replacements
    tpl = tpl.replace("ORG_NAME", org)
    tpl = tpl.replace("REPORT_DATE", report_date)

    # Swap in dynamic sections via known sentinel comments in the template
    # (We regenerate the data sections wholesale rather than per-value substitution)
    # Replace trend chart
    import re as _re
    tpl = _re.sub(
        r'(<div class="trend-chart">).*?(</div>\s*\n\s*<div class="trend-labels">).*?(</div>\s*\n\s*</div>)',
        lambda m: f'{m.group(1)}\n{trend_bars_html}    {m.group(2)}\n{trend_labels_html}    {m.group(3)}',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Replace mix bar
    tpl = _re.sub(
        r'(<div class="mix-bar">).*?(</div>\s*\n\s*<div class="mix-legend">).*?(</div>\s*\n\s*</div>)',
        lambda m: f'{m.group(1)}{mix_bar_html}\n    {m.group(2)}\n{mix_legend_html}    {m.group(3)}',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Inject KPI values (page-subtitle)
    tpl = _re.sub(
        r'(<div class="page-subtitle">)[^<]*(</div>)',
        f'\\g<1>{total} {t_ticket.lower()}s on record · Generated {report_date}\\g<2>',
        tpl, count=1,
    )

    # Replace KPI tiles dynamically
    kpi_block = f"""  <div class="kpi-grid">
    <div class="kpi-tile"><span class="kpi-val cb">{total}</span><span class="kpi-lbl">Total {t_ticket}s</span></div>
    <div class="kpi-tile"><span class="kpi-val crit">{len(open_t)}</span><span class="kpi-lbl">Open Now</span><span class="kpi-sub">{open_breakdown or "—"}</span></div>
    <div class="kpi-tile"><span class="kpi-val warn">{priority_counts.get('P1',0)}</span><span class="kpi-lbl">{t_p1}s Lifetime</span><span class="kpi-sub">{_pct('P1')}% of all tickets</span></div>
    <div class="kpi-tile"><span class="kpi-val warn">{len(p1_90d)}</span><span class="kpi-lbl">{t_p1}s Last 90 Days</span></div>
    <div class="kpi-tile"><span class="kpi-val good">{avg_p1_res}</span><span class="kpi-lbl">Avg {t_p1} Resolution</span><span class="kpi-sub">solved in last 12 mo, n={len(solved_p1)}</span></div>
    <div class="kpi-tile"><span class="kpi-val good">{closed_rate}</span><span class="kpi-lbl">Closed Rate</span><span class="kpi-sub">{len(closed_t)} closed / {total} total</span></div>
  </div>"""
    tpl = _re.sub(r'<div class="kpi-grid">.*?</div>\s*\n\s*\n', kpi_block + "\n\n", tpl, flags=_re.DOTALL, count=1)

    # Replace hbar section
    tpl = _re.sub(
        r'(<div class="card-title">Feature area — lifetime[^<]*</div>\s*\n)(.*?)(<div class="hbar-group-title"[^>]*>[^<]*</div>)(.*?)(<div class="callout)',
        lambda m: f'{m.group(1)}{hbar_html}    {m.group(3)}\n{hbar_recent_html}    {m.group(5)}',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Replace P1 incident table rows
    tpl = _re.sub(
        r'(<tbody>\s*\n)(.*?)(</tbody>)',
        lambda m: f'{m.group(1)}{p1_rows_html}        {m.group(3)}',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Replace open ticket mini-list
    tpl = _re.sub(
        r'(<div class="mini-list">\s*\n)(.*?)(</div>\s*\n\s*</div>\s*\n\s*<!--.*?RECOMMENDATIONS)',
        lambda m: f'{m.group(1)}{open_rows_html}    {m.group(3)}',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Update open count in section label
    tpl = _re.sub(
        r'Currently Open \(\d+ Tickets?\)',
        f'Currently Open ({len(open_t)} {t_ticket}s)',
        tpl, count=1,
    )

    # Inject logo if brand provides one
    if logo_html:
        tpl = tpl.replace(
            '<div class="fleet-logo">',
            f'<div class="fleet-logo">{logo_html}',
            1,
        )

    return tpl


def _build_cadence_report_html(ticket: dict, brand: dict) -> str:
    """Generate a response cadence visualization for a single ticket."""
    import datetime as _dt

    tid     = ticket.get("ticket_id", "?")
    subject = ticket.get("subject", "—")
    org     = ticket.get("organization", "")

    comments = ticket.get("comments") or []
    if not comments:
        return f"<html><body><p>No comment history for ticket #{tid}.</p></body></html>"

    # Parse timestamps and label each comment as CB or CX
    events: list[dict] = []
    for c in comments:
        ts_raw = c.get("created_at") or c.get("timestamp") or ""
        try:
            ts = _dt.datetime.fromisoformat(ts_raw[:19])
        except Exception:
            continue
        author = (c.get("author") or c.get("author_name") or "").strip()
        author_type = c.get("author_type") or ("cb" if c.get("is_internal") or c.get("internal") else "cx")
        events.append({"ts": ts, "author": author, "type": author_type, "body": (c.get("body") or "")[:120]})

    events.sort(key=lambda e: e["ts"])

    if len(events) < 2:
        return f"<html><body><p>Insufficient comment history for ticket #{tid}.</p></body></html>"

    # Compute gaps between consecutive events
    gaps: list[dict] = []
    for i in range(len(events) - 1):
        a, b = events[i], events[i + 1]
        wall_h = (b["ts"] - a["ts"]).total_seconds() / 3600
        # Rough biz hours: count weekday hours 9-17 UTC in the gap
        biz_h = 0.0
        cur = a["ts"]
        end = b["ts"]
        while cur < end:
            nxt = min(cur + _dt.timedelta(hours=1), end)
            if cur.weekday() < 5 and 9 <= cur.hour < 17:
                biz_h += (nxt - cur).total_seconds() / 3600
            cur = nxt
        off_h = wall_h - biz_h
        is_switch = b["type"] != a["type"]
        is_warn = is_switch and biz_h > 8 and b["type"] == "cb"
        gaps.append({
            "from": a, "to": b,
            "wall_h": wall_h, "biz_h": biz_h, "off_h": off_h,
            "switch": is_switch, "warn": is_warn,
        })

    max_gap_h = max((g["wall_h"] for g in gaps), default=1) or 1
    total_h   = (events[-1]["ts"] - events[0]["ts"]).total_seconds() / 3600
    date_range = f"{events[0]['ts'].strftime('%b %d')} – {events[-1]['ts'].strftime('%b %d, %Y')}"

    cb_engineers = sorted({e["author"] for e in events if e["type"] == "cb"})
    cx_names     = sorted({e["author"] for e in events if e["type"] == "cx"})

    brand_css = _brand_css_overrides(brand)

    tpl = _load_template("cadence_template.html")

    if brand_css:
        tpl = tpl.replace("</head>", f"{brand_css}\n</head>", 1) if "</head>" in tpl else brand_css + tpl

    tpl = tpl.replace("TICKET_ID", str(tid))
    tpl = tpl.replace("TICKET_SUBJECT", subject[:80])
    tpl = tpl.replace("ORG_NAME", org)
    tpl = tpl.replace("CB_ENGINEERS", ", ".join(cb_engineers) or "Couchbase Support")
    tpl = tpl.replace("CX_NAMES", ", ".join(cx_names) or org)
    tpl = tpl.replace("TIMEZONE_NOTE", "Timestamps UTC")
    tpl = tpl.replace("BIZ_HOURS_LABEL", "Mon–Fri 09:00–17:00 UTC")
    tpl = tpl.replace("TOTAL_HOURS", f"{total_h:.1f}h")
    tpl = tpl.replace("DATE_RANGE", date_range)
    tpl = tpl.replace("MAX_GAP", f"{max_gap_h:.1f}h")

    # Build gap rows
    gap_rows_html = ""
    for g in gaps:
        biz_pct = int(g["biz_h"] / max_gap_h * 100) if max_gap_h else 0
        off_pct = int(g["off_h"] / max_gap_h * 100) if max_gap_h else 0
        wall = f"{g['wall_h']:.1f}h"
        biz  = f"{g['biz_h']:.1f}h"
        off  = f"{g['off_h']:.1f}h"
        who  = g["to"]["author"] or g["to"]["type"].upper()
        when = g["to"]["ts"].strftime("%b %d %H:%M")
        note = (g["to"]["body"] or "")[:80]

        row_cls = "gap-row switch" if g["switch"] else "gap-row"
        if g["warn"]:
            row_cls += " warn-row"

        side   = g["to"]["type"]
        arrow  = "↩" if g["switch"] else "↓"
        seg_cls = "warn-active" if g["warn"] else "active"
        num_cls = "warn-c" if g["warn"] else "act-c"
        label_cls = g["to"]["type"]

        gap_rows_html += f"""  <div class="{row_cls}">
    <div class="gap-label">
      <span class="gap-who {label_cls}">{arrow} {who}</span>
      <span class="gap-note">{note}</span>
    </div>
    <div class="bar-group">
      <div class="bar-track">
        <div class="bar-seg {seg_cls}" style="width:{biz_pct}%"></div>
        <div class="bar-seg off" style="width:{off_pct}%"></div>
      </div>
      <div class="bar-meta">
        <span>{biz} biz hrs</span><span>·</span><span>{off} off</span>
        <span style="margin-left:auto">{when}</span>
      </div>
    </div>
    <div class="num-cell {num_cls}">{wall}</div>
    <div class="num-cell {num_cls}">{biz}</div>
    <div class="num-cell off-c">{off}</div>
    <div></div>
  </div>\n"""

    # Replace example gap rows in template with dynamic rows
    import re as _re
    tpl = _re.sub(
        r'<!-- EXAMPLE:.*?<!-- EXAMPLE: Awaiting.*?</div>\s*\n\n',
        gap_rows_html + "\n",
        tpl, flags=_re.DOTALL, count=1,
    )

    return tpl


@mcp.tool()
def generate_health_report(
    organization: str,
    ae_name: str = "",
    ae_email: str = "",
    tse_name: str = "",
    pse_name: str = "",
    max_tickets: int = 500,
) -> str:
    """
    Generate a customer health report HTML document from live Couchbase ticket data
    and save it as an asset. Customer brand colors/logo are applied automatically
    if a brand kit exists for the org (see save_customer_brand).

    Returns the saved asset ID and a download/view path.

    Args:
        organization: Customer org name.
        ae_name:      Account Executive name for the report header.
        ae_email:     AE email.
        tse_name:     Technical Support Engineer name.
        pse_name:     Principal/Field SE name.
        max_tickets:  Max tickets to pull for stats (default 500).
    """
    import datetime as _dt
    from supportal.agent_tools import _save_asset_to_cb

    cfg  = _cfg()
    app  = _app()
    report_date = _dt.date.today().strftime("%B %-d, %Y")

    # Pull tickets
    try:
        tickets = app.tool_query_tickets(
            {"organization": organization, "limit": min(max_tickets, 500)},
            *_cb_tuple(cfg),
            limit=min(max_tickets, 500),
        )
    except Exception as exc:
        return json.dumps({"error": f"Failed to query tickets: {exc}"})

    if not tickets:
        return json.dumps({"error": f"No tickets found for organization '{organization}'."})

    # Fetch brand (optional)
    brand: dict = {}
    try:
        raw = get_customer_brand(organization)
        brand = json.loads(raw)
        if "error" in brand:
            brand = {}
    except Exception:
        pass

    # Build HTML
    try:
        html = _build_health_report_html(organization, tickets, report_date, brand)
    except Exception as exc:
        return json.dumps({"error": f"Failed to build report: {exc}"})

    # Stamp in team contacts
    html = html.replace("AE_NAME", ae_name or "—")
    html = html.replace("AE_EMAIL", ae_email or "")
    html = html.replace("TSE_NAME", tse_name or "—")
    html = html.replace("PSE_NAME", pse_name or "—")

    # Save to CB
    try:
        cb_a = (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                cfg["use_tls"], cfg["scope"])
        safe_org = organization.lower().replace(" ", "_")
        fname = f"health_report_{safe_org}_{_dt.date.today().isoformat()}.html"
        asset_id = _save_asset_to_cb(
            *cb_a,
            asset_type="html",
            title=f"{organization} Health Report — {report_date}",
            content=html,
            org=organization,
            filename=fname,
        )
    except Exception as exc:
        return json.dumps({"error": f"Failed to save asset: {exc}", "html_length": len(html)})

    return json.dumps({
        "saved": True,
        "asset_id": asset_id,
        "filename": fname,
        "organization": organization,
        "ticket_count": len(tickets),
        "report_date": report_date,
    })


@mcp.tool()
def generate_ticket_report(ticket_id: str) -> str:
    """
    Generate a response cadence visualization for a support ticket and save it as
    an HTML asset. Shows the back-and-forth timeline between Couchbase engineers
    and the customer, with gap analysis (wall hours, business hours, off hours).
    Customer brand is applied automatically if a brand kit exists.

    Args:
        ticket_id: Zendesk ticket ID (numeric string).
    """
    import datetime as _dt
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    from datetime import timedelta
    from supportal.agent_tools import _save_asset_to_cb

    cfg = _cfg()
    app = _app()

    # Fetch full ticket
    try:
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        cl.wait_until_ready(timedelta(seconds=10))
        col = cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"])
        result = col.get(f"ticket::{ticket_id}")
        ticket = result.content_as[dict]
        cl.close()
    except Exception as exc:
        return json.dumps({"error": f"Could not fetch ticket {ticket_id}: {exc}"})

    org = ticket.get("organization", "")

    # Fetch brand (optional)
    brand: dict = {}
    if org:
        try:
            raw = get_customer_brand(org)
            brand = json.loads(raw)
            if "error" in brand:
                brand = {}
        except Exception:
            pass

    # Build HTML
    try:
        html = _build_cadence_report_html(ticket, brand)
    except Exception as exc:
        return json.dumps({"error": f"Failed to build cadence report: {exc}"})

    # Save to CB
    try:
        cb_a = (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                cfg["use_tls"], cfg["scope"])
        report_date = _dt.date.today().isoformat()
        fname = f"cadence_{ticket_id}_{report_date}.html"
        asset_id = _save_asset_to_cb(
            *cb_a,
            asset_type="html",
            title=f"Ticket #{ticket_id} Cadence — {ticket.get('subject','')[:50]}",
            content=html,
            org=org,
            filename=fname,
        )
    except Exception as exc:
        return json.dumps({"error": f"Failed to save asset: {exc}", "html_length": len(html)})

    comments = ticket.get("comments") or []
    return json.dumps({
        "saved": True,
        "asset_id": asset_id,
        "filename": fname,
        "ticket_id": ticket_id,
        "organization": org,
        "comment_count": len(comments),
    })


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
