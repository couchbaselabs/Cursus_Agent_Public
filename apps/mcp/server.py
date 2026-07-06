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
import urllib.request
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
        _log_tool_failure("score_ticket", exc, f"ticket:{ticket_id}")
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
            (cfg["score_provider"] or "").lower().strip(), cfg["score_model"],
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

    # ── LLM provider (embedding + scoring) ────────────────────────────────────
    # The rescrape pipeline's embed/score stages need the local model server
    # up with the configured models actually loaded — probe it so failures are
    # caught (and loggable) BEFORE a pipeline run silently degrades.
    emb_provider   = (cfg.get("emb_provider") or "").lower().strip()
    score_provider = (cfg.get("score_provider") or "").lower().strip()
    if "lmstudio" in (emb_provider, score_provider):
        lms_base = (cfg.get("emb_base_url") or cfg.get("score_base_url") or "http://localhost:1234").rstrip("/")
        if not lms_base.startswith("http"):
            lms_base = f"http://{lms_base}"
        probe: dict = {"provider": "lmstudio", "base_url": lms_base,
                       "emb_model": cfg.get("emb_model") or "",
                       "score_model": cfg.get("score_model") or ""}
        try:
            import urllib.request as _ur
            models_url = lms_base + ("/v1/models" if not lms_base.endswith("/v1") else "/models")
            with _ur.urlopen(_ur.Request(models_url), timeout=5) as resp:
                loaded = {m.get("id") for m in json.load(resp).get("data", [])}
            probe["reachable"] = True
            probe["loaded_models"] = sorted(loaded)
            missing = [m for m in (probe["emb_model"], probe["score_model"])
                       if m and m not in loaded]
            probe["models_ok"] = not missing
            if missing:
                probe["missing_models"] = missing
                probe["fix"] = "Load the missing model(s) in LMStudio before running embed/score pipelines."
        except Exception as exc:
            probe["reachable"] = False
            probe["models_ok"] = False
            probe["error"] = str(exc)
            probe["fix"] = "Start LMStudio (or its server) — embed/score stages will fail until it is up."
        result["llm_provider"] = probe

    # ── Summary ───────────────────────────────────────────────────────────────
    vpn_ok       = result.get("vpn_services", {}).get("connected")
    cb_ok        = result.get("couchbase", {}).get("reachable", False)
    supportal_ok = result.get("supportal", {}).get("reachable", False)
    llm          = result.get("llm_provider")
    llm_ok       = llm.get("models_ok", False) if llm else None

    if cb_ok and supportal_ok:
        result["summary"] = "All systems reachable. Scrape tools are ready."
    elif cb_ok and not supportal_ok:
        result["summary"] = "Couchbase OK. Supportal unreachable — connect VPN before scraping."
    elif not cb_ok:
        result["summary"] = "Couchbase unreachable — check CB is running and credentials are correct."
    else:
        result["summary"] = "No connectivity — check VPN and local services."
    if llm is not None and not llm_ok:
        result["summary"] += " WARNING: LLM provider not ready — embed/score stages will fail (see llm_provider)."

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
def wait_for_scrape(job_id: str = "", timeout_s: int = 240) -> str:
    """
    Block until a scrape/rescrape job concludes (done/error/cancelled), then
    return its final state — so callers get start-job → await-result semantics
    instead of hand-rolled polling loops.

    Waits on ALL running jobs when job_id is omitted. Returns current progress
    if the timeout elapses first (timed_out: true) — safe to call again.
    Also reaps lost job runs: any permanent jobrun:: record still 'started'
    past its computed deadline is marked lost, so silently dropped jobs are
    recorded rather than vanishing.

    Args:
        job_id:    Specific 6-char job ID. Omit to wait for all running jobs.
        timeout_s: Max seconds to block (default 240, capped at 570 to stay
                   under typical MCP client timeouts).
    """
    app = _app()
    timeout_s = min(max(int(timeout_s), 5), 570)
    deadline = time.time() + timeout_s

    def _watched() -> list[dict]:
        jobs = app._SCRAPE_JOBS
        if job_id:
            return [jobs[job_id]] if job_id in jobs else []
        return [j for j in jobs.values()]

    if job_id and not _watched():
        return json.dumps({"error": f"Job '{job_id}' not found.",
                           "recent_ids": list(app._SCRAPE_JOBS)[-5:]})

    while time.time() < deadline:
        running = [j for j in _watched() if j.get("status") == "running"]
        if not running:
            break
        time.sleep(3)

    reaped = _reap_lost_jobruns()
    still_running = [j["job_id"] for j in _watched() if j.get("status") == "running"]
    out = {
        "timed_out": bool(still_running),
        "still_running": still_running,
        "jobs": [{
            "job_id": j["job_id"], "org": j["org"], "mode": j["mode"],
            "status": j["status"], "processed": j.get("processed", 0),
            "total": j.get("total"), "saved": j.get("saved", 0),
            "embedded": j.get("embedded", 0), "scored": j.get("scored", 0),
            "errors": j.get("errors", 0), "last_message": j.get("last_message"),
        } for j in _watched()],
    }
    if reaped:
        out["reaped_lost_jobs"] = reaped
    return json.dumps(out, default=str)


def _reap_lost_jobruns() -> list[str]:
    """Mark jobrun:: records still 'started' past their computed deadline as
    lost — the durable evidence of a silently dropped job. A job whose
    in-memory record is still heartbeating gets its deadline extended instead.
    Returns reaped job_ids. Never raises."""
    reaped: list[str] = []
    try:
        cfg = _cfg()
        app = _app()
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        ks = f"`{cfg['bucket']}`.`{cfg['scope']}`.`markers`"
        now = time.time()
        rows = list(cl.query(
            f"SELECT META(m).id AS k, m.* FROM {ks} m "
            f"WHERE m.`type` = 'job_run' AND m.`status` = 'started' "
            f"AND m.`expected_deadline` < {now}"
        ))
        col = cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("markers")
        for r in rows:
            jid = r.get("job_id", "")
            live = app._SCRAPE_JOBS.get(jid)
            doc = {k: v for k, v in r.items() if k != "k"}
            if live and now - live.get("heartbeat_at", 0) < 600:
                doc["expected_deadline"] = now + 1800  # still heartbeating — extend
            else:
                doc.update({
                    "status":    "lost",
                    "reaped_at": now,
                    "reason":    "no conclusion recorded by computed deadline "
                                 "— job presumed silently dropped",
                })
                reaped.append(jid)
            col.upsert(r["k"], doc)
    except Exception:
        pass
    return reaped


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
        _log_tool_failure("rescrape_customer_tickets", exc, organization)
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


@mcp.tool()
def query_supportal_analytics(statement: str, limit_rows: int = 100) -> str:
    """
    Run a read-only SQL++ statement against the LIVE Supportal Analytics API
    (POST /api/support360/query) — the production system of record, not the
    local Couchbase cache this MCP server otherwise reads from.

    Use this to independently cross-check numbers computed from the local
    Couchbase copy (e.g. ticket counts, per-customer aggregates) against
    Supportal's own live data. As of v2.6.2 the endpoint requires no auth.

    Schema note: this queries Supportal's own collections (ticket, snapshot,
    cluster, customer), which use different field/collection names than the
    local `supportal` collection this MCP server scrapes into — don't assume
    field names carry over 1:1.

    Args:
        statement:  A SELECT-only SQL++ statement (mutating statements are rejected).
        limit_rows: Max rows to return (default 100, max 500).
    """
    stmt = statement.strip()
    if not stmt.lower().startswith("select"):
        return json.dumps({"error": "Only SELECT statements are allowed against the live Supportal Analytics API."})

    app = _app()
    try:
        rows = app.query_supportal_analytics(stmt, "")
    except Exception as exc:
        _log_tool_failure("query_supportal_analytics", exc)
        return json.dumps({"error": f"Supportal Analytics query failed: {exc}"})

    rows = rows[: min(int(limit_rows or 100), 500)]
    return json.dumps({"row_count": len(rows), "rows": rows}, default=str)


def _ensure_markers_collection(cluster: Any, bucket: str, scope: str = "transcripts") -> None:
    try:
        cm = cluster.bucket(bucket).collections()
        existing = {s.name: {c.name for c in s.collections} for s in cm.get_all_scopes()}
        if "markers" not in existing.get(scope, set()):
            from couchbase.management.collections import CollectionSpec
            cm.create_collection(CollectionSpec("markers", scope_name=scope))
    except Exception:
        pass


def _log_tool_failure(tool: str, exc, organization: str = "") -> None:
    """Persist an MCP tool failure to the markers collection so tool errors
    become queryable failure knowledge instead of a transient return value.

    One doc per tool per day (key toolfailure::<tool>::<YYYY-MM-DD>), entries
    appended with an abridged error code/message. Never raises — logging must
    not mask or replace the tool's own error response.
    """
    import datetime as _dt
    try:
        from supportal.cb_helpers import classify_error
        cfg = _cfg()
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        _ensure_markers_collection(cl, cfg["bucket"], cfg["scope"])
        col = cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("markers")
        now = _dt.datetime.now(_dt.timezone.utc)
        key = f"toolfailure::{tool}::{now.date().isoformat()}"
        entry = {"at": now.isoformat().replace("+00:00", "Z"),
                 "organization": organization, **classify_error(exc)}
        try:
            doc = col.get(key).content_as[dict]
        except Exception:
            doc = {"type": "tool_failure", "tool": tool, "date": now.date().isoformat(), "entries": []}
        if len(doc.get("entries", [])) < 100:
            doc.setdefault("entries", []).append(entry)
        doc["count"] = doc.get("count", 0) + 1
        col.upsert(key, doc)
    except Exception:
        pass


@mcp.tool()
def record_feedback(
    subject_kind: str,
    subject_ref: str,
    verdict: str,
    details: str = "",
    correction_field: str = "",
    correction_old: str = "",
    correction_new: str = "",
    organization: str = "",
) -> str:
    """
    Record human feedback on something this system produced — the raw material
    for the improvement loop (few-shot examples, eval regression sets, and
    preference pairs for fine-tuning the local scoring/embedding models).

    Call this whenever the user corrects an output, rates a result, or states
    a preference — e.g. "that P1 count is wrong", "this report is exactly what
    I wanted", "stars should be 2 not 4".

    Args:
        subject_kind:     What kind of output: score | report | answer | tool_call | data
        subject_ref:      Reference, e.g. "ticket:78964", "asset:<id>", "org:Western Union"
        verdict:          positive | negative | corrected
        details:          What the human actually said / what was wrong.
        correction_field: If a specific field was corrected, its name (e.g. "stars").
        correction_old:   The system's original value.
        correction_new:   The human's corrected value.
        organization:     Customer org for context, if applicable.
    """
    cfg = _cfg()
    correction = None
    if correction_field or correction_old or correction_new:
        correction = {"field": correction_field, "old": correction_old, "new": correction_new}
    try:
        from supportal.cb_helpers import save_feedback
        key = save_feedback(
            cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg["use_tls"], cfg["scope"],
            source="mcp", kind="correction" if correction else "rating",
            subject_kind=subject_kind, subject_ref=subject_ref,
            verdict=verdict, details=details, correction=correction,
            organization=organization,
        )
        return json.dumps({"saved": True, "key": key})
    except Exception as exc:
        _log_tool_failure("record_feedback", exc, organization)
        return json.dumps({"error": f"Failed to save feedback: {exc}"})


@mcp.tool()
def record_automation_run(
    task_id: str,
    outcome: str,
    run_kind: str = "",
    summary: str = "",
    errors_observed: str = "",
    metrics: str = "",
    notification_sent: str = "",
) -> str:
    """
    Persist a durable record of an automation run (scheduled task, watcher,
    orchestrator cycle) to the `markers` collection — the native way for any
    automation to make its behavior auditable, instead of hand-assembling
    docs via generic upserts.

    Key: cronrun::<task_id>::<UTC timestamp to the minute>. Doc type:
    "automation_run". These records feed get_failure_insights and are the
    raw material for run-over-run anomaly detection (candidate insights).

    Args:
        task_id:           Automation identifier, e.g. "support-ticket-monitor".
        outcome:           ok | degraded | failed. "degraded" = completed but with
                           tool errors/timeouts or skipped sub-steps.
        run_kind:          Optional label, e.g. "9am-full" | "intraday".
        summary:           One-line human-readable result of the run.
        errors_observed:   Optional JSON array string of error objects hit during
                           the run, e.g. '[{"step":"freshness","error_code":"HTTP_5XX","abridged":"..."}]'.
        metrics:           Optional JSON object string of run metrics, e.g.
                           '{"customer_replies_found":0,"rescrapes_triggered":["GoDaddy"]}'.
        notification_sent: "true" | "false" | "suppressed-user-active" | "".
    """
    import datetime as _dt

    outcome_n = (outcome or "").lower().strip()
    if outcome_n not in ("ok", "degraded", "failed"):
        return json.dumps({"error": f"outcome must be ok|degraded|failed, got {outcome!r}"})

    def _lenient_json(s: str, fallback):
        if not (s or "").strip():
            return fallback
        try:
            return json.loads(s)
        except Exception:
            return [{"raw": s[:500]}] if isinstance(fallback, list) else {"raw": s[:500]}

    cfg = _cfg()
    try:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        _ensure_markers_collection(cl, cfg["bucket"], cfg["scope"])
        now = _dt.datetime.now(_dt.timezone.utc)
        key = f"cronrun::{task_id.strip()}::{now.strftime('%Y-%m-%dT%H:%M')}"
        errs = _lenient_json(errors_observed, [])
        if not isinstance(errs, list):
            errs = [errs]
        mets = _lenient_json(metrics, {})
        if not isinstance(mets, dict):
            mets = {"value": mets}
        doc = {
            "type":              "automation_run",
            "task_id":           task_id.strip(),
            "ran_at":            now.isoformat().replace("+00:00", "Z"),
            "run_kind":          run_kind.strip(),
            "outcome":           outcome_n,
            "summary":           summary.strip()[:1000],
            "errors_observed":   errs[:50],
            "metrics":           mets,
            "notification_sent": notification_sent.strip(),
        }
        cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("markers").upsert(key, doc)
        return json.dumps({"saved": True, "key": key, "outcome": outcome_n})
    except Exception as exc:
        _log_tool_failure("record_automation_run", exc, task_id)
        return json.dumps({"error": f"Failed to save automation run: {exc}"})


@mcp.tool()
def record_insight(
    pattern: str,
    summary: str,
    evidence: str = "",
    organizations: str = "",
    proposed_action: str = "",
    source: str = "watcher",
) -> str:
    """
    Persist a CANDIDATE insight — an observed pattern in customer/ticket data
    worth remembering across sessions (e.g. "backup failures recurring across
    4 orgs"). Insights are the substrate for next-step suggestions.

    Governance: anything recorded here starts as status="candidate". It must
    be promoted to "validated" by a human (or a validation gate) before any
    automation treats it as truth — machine-generated insights never
    self-promote. Docs land in the `insights` collection (key insight::<id>).

    Args:
        pattern:         Short snake_case pattern name (e.g. "recurring_backup_failures").
        summary:         One-paragraph statement of the observation.
        evidence:        Comma-separated refs, e.g. "ticket:78641,cluster:3f18...".
        organizations:   Comma-separated org names involved.
        proposed_action: Suggested next step, if any.
        source:          watcher | session | scheduled (human-validated inserts happen elsewhere).
    """
    import datetime as _dt
    import uuid as _uuid

    cfg = _cfg()
    try:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        try:
            cm = cl.bucket(cfg["bucket"]).collections()
            existing = {s.name: {cc.name for cc in s.collections} for s in cm.get_all_scopes()}
            if "insights" not in existing.get(cfg["scope"], set()):
                from couchbase.management.collections import CollectionSpec
                cm.create_collection(CollectionSpec("insights", scope_name=cfg["scope"]))
        except Exception:
            pass
        key = f"insight::{_uuid.uuid4().hex[:12]}"
        doc = {
            "type":            "insight",
            "status":          "candidate",
            "source":          (source or "watcher").lower().strip(),
            "pattern":         pattern.strip(),
            "summary":         summary.strip()[:2000],
            "evidence":        [e.strip() for e in evidence.split(",") if e.strip()],
            "organizations":   [o.strip() for o in organizations.split(",") if o.strip()],
            "proposed_action": proposed_action.strip()[:1000],
            "created_at":      _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("insights").upsert(key, doc)
        return json.dumps({"saved": True, "key": key, "status": "candidate",
                           "note": "Requires human validation before automations may act on it."})
    except Exception as exc:
        _log_tool_failure("record_insight", exc, organizations)
        return json.dumps({"error": f"Failed to save insight: {exc}"})


@mcp.tool()
def get_failure_insights(days: int = 7, organization: str = "") -> str:
    """
    Observability report over the failure-knowledge base (the `markers`
    collection) — the governance surface for "how is our own tooling failing
    and is our data trustworthy?"

    Aggregates all four marker types written by the pipeline and tools:
      - freshness::<org>            — latest cache-vs-live reconciliation per org
      - pipelinefailure::<date>     — LMStudio/model preflight failures
      - failurelog::<job_id>        — per-job scrape/embed/score failures
      - toolfailure::<tool>::<date> — MCP tool except-path failures

    Returns per-org freshness status, failures grouped by error_code and by
    stage/tool (so recurring failure modes stand out), and the most recent
    raw entries for drill-down.

    Args:
        days:         Lookback window for failure docs (default 7).
        organization: Optional org filter (partial match, case-insensitive).
    """
    import datetime as _dt

    cfg = _cfg()
    try:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        ks = f"`{cfg['bucket']}`.`{cfg['scope']}`.`markers`"
        _reap_lost_jobruns()
        docs = [r for r in cl.query(f"SELECT META(m).id AS _key, m.* FROM {ks} m")]
    except Exception as exc:
        return json.dumps({"error": f"Could not read markers collection: {exc}"})

    now = _dt.datetime.now(_dt.timezone.utc)
    cutoff_epoch = (now - _dt.timedelta(days=days)).timestamp()
    cutoff_date = (now - _dt.timedelta(days=days)).date().isoformat()
    org_f = organization.lower().strip()

    def _org_match(v: str) -> bool:
        return not org_f or org_f in (v or "").lower()

    freshness, code_counts, stage_counts, tool_counts = [], {}, {}, {}
    recent_entries = []
    pipeline_failures = []
    run_outcomes: dict = {}
    recent_runs = []
    job_outcomes: dict = {}
    lost_jobs = []

    for d in docs:
        dtype = d.get("type")
        if dtype == "job_run" and _org_match(d.get("organization")):
            started = d.get("started_at") or 0
            if started and started < cutoff_epoch:
                continue
            job_outcomes[d.get("status", "?")] = job_outcomes.get(d.get("status", "?"), 0) + 1
            if d.get("status") == "lost":
                lost_jobs.append({k: d.get(k) for k in
                                  ("job_id", "organization", "mode", "started_at", "reason")})
        elif dtype == "automation_run":
            if (d.get("ran_at") or "") < cutoff_date:
                continue
            run_outcomes[d.get("outcome", "?")] = run_outcomes.get(d.get("outcome", "?"), 0) + 1
            recent_runs.append({k: d.get(k) for k in ("task_id", "ran_at", "run_kind", "outcome", "summary")})
            for e in d.get("errors_observed") or []:
                if isinstance(e, dict) and len(recent_entries) < 25:
                    recent_entries.append({"source": f"run:{d.get('task_id')}", "at": d.get("ran_at"), **e})
        elif dtype == "freshness" and _org_match(d.get("organization")):
            freshness.append({k: d.get(k) for k in
                              ("organization", "status", "checked_at", "missing_count",
                               "live_ticket_ids", "local_tickets")})
        elif dtype == "pipeline_failure":
            if (d.get("checked_at") or "") >= cutoff_date:
                pipeline_failures.append({"checked_at": d.get("checked_at"),
                                          "stage": d.get("stage"),
                                          "impact": d.get("impact")})
        elif dtype == "failure_log" and _org_match(d.get("organization")):
            fin = d.get("finished_at") or 0
            if fin and fin < cutoff_epoch:
                continue
            for e in d.get("error_log") or []:
                code = e.get("error_code", "UNKNOWN")
                stage = e.get("stage", "?")
                code_counts[code] = code_counts.get(code, 0) + 1
                stage_counts[stage] = stage_counts.get(stage, 0) + 1
                if len(recent_entries) < 25:
                    recent_entries.append({"source": f"job:{d.get('job_id')}",
                                           "org": d.get("organization"), **e})
        elif dtype == "tool_failure":
            if (d.get("date") or "") < cutoff_date:
                continue
            tool = d.get("tool", "?")
            for e in d.get("entries") or []:
                if not _org_match(e.get("organization")):
                    continue
                code = e.get("error_code", "UNKNOWN")
                code_counts[code] = code_counts.get(code, 0) + 1
                tool_counts[tool] = tool_counts.get(tool, 0) + 1
                if len(recent_entries) < 25:
                    recent_entries.append({"source": f"tool:{tool}", **e})

    freshness.sort(key=lambda f: (f.get("status") != "stale", f.get("organization") or ""))
    recent_entries.sort(key=lambda e: str(e.get("at") or ""), reverse=True)
    recent_runs.sort(key=lambda r: str(r.get("ran_at") or ""), reverse=True)

    stale = [f["organization"] for f in freshness if f.get("status") == "stale"]
    total_failures = sum(code_counts.values())
    not_ok_runs = sum(v for k, v in run_outcomes.items() if k != "ok")
    summary = (
        f"Last {days}d: {total_failures} failure(s) recorded"
        + (f", top code: {max(code_counts, key=code_counts.get)}" if code_counts else "")
        + (f". STALE cache: {', '.join(stale)}" if stale else ". All checked orgs fresh.")
        + (f" {len(pipeline_failures)} model-preflight failure(s)." if pipeline_failures else "")
        + (f" Automation runs: {sum(run_outcomes.values())} ({not_ok_runs} degraded/failed)." if run_outcomes else "")
        + (f" ⚠ {len(lost_jobs)} LOST job(s) — started but never concluded." if lost_jobs else "")
    )
    return json.dumps({
        "window_days":        days,
        "summary":            summary,
        "failures_by_code":   dict(sorted(code_counts.items(), key=lambda kv: -kv[1])),
        "failures_by_stage":  dict(sorted(stage_counts.items(), key=lambda kv: -kv[1])),
        "failures_by_tool":   dict(sorted(tool_counts.items(), key=lambda kv: -kv[1])),
        "pipeline_failures":  pipeline_failures,
        "automation_runs":    {"by_outcome": run_outcomes, "recent": recent_runs[:20]},
        "scrape_job_runs":    {"by_status": job_outcomes, "lost": lost_jobs[:20]},
        "freshness":          freshness,
        "recent_entries":     recent_entries,
    }, default=str)


@mcp.tool()
def check_data_freshness(organization: str) -> str:
    """
    Verify the local ticket cache against LIVE Supportal data and persist a
    freshness marker documenting the result.

    Compares the set of ticket IDs referenced by the org's live snapshots
    (snapshot.zendesk[] via the Supportal Analytics API) against ticket IDs
    present in the local Couchbase cache. Any ID present live but missing
    locally means the local cache is behind and a rescrape is warranted.

    Note the check is one-directional by design: local tickets that never
    produced a snapshot won't appear in live snapshot.zendesk[] arrays, so
    "local has more than live" is normal and NOT treated as drift.

    Writes a marker doc `freshness::<org>` (collection `markers`) with
    checked_at, live/local counts, missing IDs, and a status of
    fresh | stale — so downstream reports and automations can verify when
    the data was last reconciled instead of trusting the cache blindly.

    Args:
        organization: Customer org name (same matching rules as rescrape).
    """
    cfg = _cfg()
    try:
        from supportal.cb_helpers import compute_and_mark_freshness
        marker = compute_and_mark_freshness(
            organization,
            cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg.get("use_tls", False), cfg["scope"], cfg["collection"],
            cookie=cfg["cookie"],
            verified_by="tool:check_data_freshness",
        )
        return json.dumps(marker, default=str)
    except Exception as exc:
        _log_tool_failure("check_data_freshness", exc, organization)
        return json.dumps({"error": f"Freshness check failed: {exc}", "organization": organization})


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


@mcp.tool()
def export_asset(asset_id: str, output_path: str) -> str:
    """
    Write an asset's content from Couchbase directly to a local file and
    return verification data (bytes written, sha256, extracted <title> for
    HTML). This is the ONLY sanctioned path for getting report HTML onto
    disk for publishing — never copy content by hand between tool outputs,
    which risks silently dropping or staling fields.

    Args:
        asset_id:    Asset UUID (from list_assets or a generate_* result).
        output_path: Absolute path to write the file to.
    """
    import hashlib
    import re as _re2
    from supportal.agent_tools import _get_asset_content_from_cb
    cfg  = _cfg()
    cb_a = (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg["use_tls"], cfg["scope"])
    try:
        doc = _get_asset_content_from_cb(*cb_a, asset_id)
        if not doc or not doc.get("content"):
            return json.dumps({"error": f"Asset '{asset_id}' not found or has no content."})
        content = doc["content"]
        atype = doc.get("asset_type", "")
        p = Path(output_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if atype in ("image", "pdf"):
            import base64
            raw = base64.b64decode(content)
            p.write_bytes(raw)
            digest = hashlib.sha256(raw).hexdigest()
            size = len(raw)
            title = ""
        else:
            p.write_text(content, encoding="utf-8")
            digest = hashlib.sha256(content.encode()).hexdigest()
            size = len(content.encode())
            m = _re2.search(r"<title>([^<]*)</title>", content)
            title = m.group(1) if m else ""
        return json.dumps({
            "exported":   True,
            "asset_id":   asset_id,
            "filename":   doc.get("filename", ""),
            "output_path": str(p),
            "bytes":      size,
            "sha256":     digest,
            "title":      title,
            "organization": doc.get("organization", ""),
        })
    except Exception as exc:
        _log_tool_failure("export_asset", exc)
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
def _logo_to_data_uri(source: str) -> str:
    """Resolve a logo source (bare domain like 'westernunion.com' or an http URL)
    to an embedded data: URI. Reports must self-contain images — remote URLs are
    blocked by the Artifact CSP and unreliable in printed PDFs.

    Fetched logos are cached permanently in the `brands` collection
    (logo::<domain-or-url>), so a logo hits the network exactly once —
    later brand saves and re-provisioning resolve from Couchbase.

    For a bare domain, tries the Clearbit logo API then the Google favicon
    service. Returns "" when nothing resolves."""
    import base64
    import requests as _rq
    source = (source or "").strip()
    if not source or source.startswith("data:"):
        return source
    if source.startswith("http"):
        cache_slug = source
        candidates = [source]
    else:
        dom = source.lower().removeprefix("https://").removeprefix("http://").removeprefix("www.").split("/")[0]
        cache_slug = dom
        candidates = [
            f"https://logo.clearbit.com/{dom}",
            f"https://www.google.com/s2/favicons?domain={dom}&sz=128",
        ]
    cache_key = f"logo::{cache_slug.lower().replace('/', '_')}"[:240]

    # Cache lookup — brands collection, permanent
    _col = None
    try:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        cfg = _cfg()
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        _col = cl.bucket(cfg["bucket"]).scope(cfg["scope"]).collection("brands")
        cached = _col.get(cache_key).content_as[dict]
        if cached.get("data_uri"):
            return cached["data_uri"]
    except Exception:
        pass  # miss or CB unavailable — fetch live

    for url in candidates:
        try:
            r = _rq.get(url, timeout=10)
            ctype = (r.headers.get("content-type") or "").split(";")[0].strip()
            if r.ok and r.content and ctype.startswith("image/") and len(r.content) > 500:
                uri = f"data:{ctype};base64,{base64.b64encode(r.content).decode()}"
                if _col is not None:
                    try:
                        _col.upsert(cache_key, {
                            "type": "logo_cache", "source": cache_slug,
                            "fetched_from": url, "fetched_at": int(time.time()),
                            "data_uri": uri,
                        })
                    except Exception:
                        pass
                return uri
        except Exception:
            continue
    return ""


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
    Couchbase blue palette; terminology replaces generic labels like 'ticket';
    the logo renders in the report's title banner).

    Args:
        organization:    Customer org name (exact match used for lookups).
        primary_color:   CSS color for the brand's primary shade (e.g. '#0050A0').
        secondary_color: CSS color for the secondary accent.
        accent_color:    CSS color for highlights/CTAs.
        logo_url:        Customer logo — pass a bare domain (e.g. 'westernunion.com')
                         or an image URL and it is fetched and embedded as a data:
                         URI so reports stay self-contained; data: URIs pass through.
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
        logo_note = ""
        if logo_url and not logo_url.startswith("data:"):
            resolved = _logo_to_data_uri(logo_url)
            if resolved:
                logo_note = f"logo fetched and embedded ({len(resolved) // 1024} KB data URI)"
                logo_url = resolved
            else:
                logo_note = f"could not fetch a logo from '{logo_url}' — saved without one"
                logo_url = ""
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
        out = {"saved": True, "organization": organization, "key": key}
        if logo_note:
            out["logo"] = logo_note
        return json.dumps(out)
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


def _build_health_report_html(org: str, tickets: list[dict], report_date: str, brand: dict, org_meta: dict | None = None, true_total: int = 0) -> str:
    """Generate a full health report HTML document from live CB ticket data.

    true_total is the org's real lifetime ticket count. When it exceeds
    len(tickets), the analyzed set is a most-recent window and every
    lifetime-sounding figure must disclose the from→to date range instead
    of implying full history.
    """
    import datetime as _dt
    from collections import Counter, defaultdict

    term = brand.get("terminology") or {}
    t_ticket = term.get("ticket", "Ticket")
    t_p1 = term.get("P1", "P1")

    now_ts = _dt.datetime.utcnow()
    all_t = tickets

    # ── Aggregate stats ──────────────────────────────────────────────────────
    total = len(all_t)
    true_total = max(true_total or total, total)
    windowed = true_total > total
    _dates = sorted((t.get("created") or "")[:10] for t in all_t if t.get("created"))
    window_from, window_to = (_dates[0], _dates[-1]) if _dates else ("?", "?")
    window_note = f"{window_from} → {window_to}"
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
        # "solved" is the actual resolution timestamp; "updated" is frequently null
        # on these ticket docs and should only be used as a last-resort fallback.
        u = t.get("solved") or t.get("updated") or ""
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

    # ── P1 year-over-year comparison — count + avg resolution per calendar
    # year, so long-term accounts can see whether P1 frequency AND response
    # speed are improving. Uses the same solved-date resolution math.
    p1_by_year: dict[str, list] = defaultdict(list)
    for t in all_t:
        if _priority(t) == "P1" and (t.get("created") or "")[:4].isdigit():
            p1_by_year[(t.get("created") or "")[:4]].append(t)
    p1_year_html = ""
    if len(p1_by_year) >= 2:
        years = sorted(p1_by_year.keys())[-10:]
        year_stats = []
        for y in years:
            yt = p1_by_year[y]
            res = [_res_days(t) for t in yt
                   if (t.get("status") or "").lower() in ("solved", "closed")]
            res = [r for r in res if r is not None]
            year_stats.append({
                "year": y, "count": len(yt),
                "avg_res": (sum(res) / len(res)) if res else None,
                "n_solved": len(res),
            })
        max_y_cnt = max(s["count"] for s in year_stats) or 1
        cols = ""
        for s in year_stats:
            hpct = max(int(s["count"] / max_y_cnt * 100), 4)
            res_lbl = f'{s["avg_res"]:.0f}d' if s["avg_res"] is not None else "—"
            cols += (
                f'      <div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;">'
                f'<span style="font-size:10px;font-weight:700;color:var(--text-2);font-variant-numeric:tabular-nums;">{s["count"]}</span>'
                f'<div style="width:100%;max-width:34px;height:{hpct}px;background:var(--crit);'
                f'border-radius:3px 3px 0 0;opacity:.85;" title="{s["count"]} P1s in {s["year"]}"></div>'
                f'<span style="font-size:10px;color:var(--text-3);font-weight:600;">{s["year"]}</span>'
                f'<span style="font-size:9.5px;font-weight:700;color:#475569;" '
                f'title="avg resolution, n={s["n_solved"]} solved">⌀ {res_lbl}</span></div>\n'
            )
        _yr_note = ""
        _resolved_years = [s for s in year_stats if s["avg_res"] is not None]
        if len(_resolved_years) >= 2:
            _first, _last = _resolved_years[0], _resolved_years[-1]
            _dirn = "improved" if _last["avg_res"] < _first["avg_res"] else "slowed"
            _yr_note = (f'<div style="font-size:11px;color:var(--text-2);margin-top:10px;">'
                        f'Avg resolution has <strong>{_dirn}</strong> from {_first["avg_res"]:.0f}d '
                        f'({_first["year"]}) to {_last["avg_res"]:.0f}d ({_last["year"]}). '
                        f'⌀ = avg days to resolve the {t_p1}s opened that year.</div>')
        p1_year_html = (
            f'\n  <div class="card">\n    <div class="card-title">{t_p1}s by year — volume and resolution speed</div>\n'
            f'    <div style="display:flex;align-items:flex-end;gap:6px;">\n{cols}    </div>\n'
            f'    {_yr_note}\n  </div>\n'
        )

    # Monthly volume — last 10 months, stacked by priority so P1/P2 presence
    # per month is visible (validates window claims like "0 P1s last 90d").
    monthly: dict[str, int] = defaultdict(int)
    monthly_pri: dict[str, Counter] = defaultdict(Counter)
    for t in all_t:
        c = t.get("created") or ""
        if len(c) >= 7:
            monthly[c[:7]] += 1
            monthly_pri[c[:7]][_priority(t)] += 1
    sorted_months = sorted(monthly.keys())[-10:]
    month_counts = [monthly[m] for m in sorted_months]
    max_cnt = max(month_counts, default=1) or 1
    month_labels = [_dt.datetime.strptime(m, "%Y-%m").strftime("%b") for m in sorted_months]

    _PRI_SEGS = [("P1", "var(--crit)"), ("P2", "var(--warn)"),
                 ("P3", "var(--cb)"), ("P4", "var(--neutral)"), ("P?", "#C9D0DC")]
    trend_bars_html = ""
    for i, (mkey, cnt) in enumerate(zip(sorted_months, month_counts)):
        pct = max(int(cnt / max_cnt * 100), 4)
        cur = "border-color:#0D4FA0;" if i == len(sorted_months) - 1 else ""
        segs = "".join(
            f'<div style="height:{monthly_pri[mkey][pri] / cnt * 100:.1f}%;background:{color};" '
            f'title="{pri}: {monthly_pri[mkey][pri]}"></div>'
            for pri, color in _PRI_SEGS if monthly_pri[mkey].get(pri)
        ) if cnt else ""
        trend_bars_html += (
            f'      <div class="trend-col"><div style="position:relative;width:100%;max-width:36px;height:{pct}%;">'
            f'<span class="trend-bar-val">{cnt}</span>'
            f'<div class="trend-bar" style="height:100%;{cur}display:flex;flex-direction:column;'
            f'justify-content:flex-end;overflow:hidden;background:var(--bg);">{segs}</div></div></div>\n'
        )
    trend_labels_html = "".join(f'      <span class="trend-lbl">{l}</span>\n' for l in month_labels)
    _any_unknown = any(monthly_pri[m].get("P?") for m in sorted_months)
    trend_legend_html = (
        '<div style="display:flex;gap:14px;margin-top:8px;font-size:10px;color:var(--text-2);">'
        + "".join(f'<span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;'
                  f'background:{c};margin-right:4px;"></span>{p if p != "P?" else "No priority"}</span>'
                  for p, c in _PRI_SEGS if p != "P?" or _any_unknown)
        + "</div>"
    )

    # ── Time-window breakdown (30/60/90/YTD/lifetime) ───────────────────────
    # Marker/window colors are deliberately FIXED hex values, never brand vars,
    # so they stay distinguishable from any customer color scheme.
    _WINDOW_COLORS = {"30d": "#7C3AED", "60d": "#DB2777", "90d": "#0891B2", "YTD": "#475569"}
    ytd_start = f"{now_ts.year}-01-01"
    window_bounds = {
        "30d": (now_ts - _dt.timedelta(days=30)).isoformat(),
        "60d": (now_ts - _dt.timedelta(days=60)).isoformat(),
        "90d": (now_ts - _dt.timedelta(days=90)).isoformat(),
        "YTD": ytd_start,
    }
    window_stats: dict[str, dict] = {}
    for wlbl, wcut in window_bounds.items():
        w_t = [t for t in all_t if (t.get("created") or "") >= wcut]
        window_stats[wlbl] = {
            "count": len(w_t),
            "p1":    sum(1 for t in w_t if _priority(t) == "P1"),
            "open":  sum(1 for t in w_t if (t.get("status") or "").lower() in ("open", "pending", "on-hold", "hold")),
        }

    def _chip(lbl: str, cnt, p1c, opn, color: str) -> str:
        return (f'<div style="flex:1;min-width:88px;border:1px solid var(--border-light);'
                f'border-left:3px solid {color};border-radius:6px;padding:6px 10px;">'
                f'<div style="font-size:15px;font-weight:800;font-variant-numeric:tabular-nums;">{cnt}</div>'
                f'<div style="font-size:9.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--text-3);font-weight:700;">{lbl}</div>'
                f'<div style="font-size:9.5px;color:var(--text-2);">{p1c} {t_p1} · {opn} open</div></div>')

    window_chips_html = (
        '<div style="display:flex;gap:8px;margin-top:14px;flex-wrap:wrap;">'
        + "".join(_chip(f"Last {w}" if w != "YTD" else "Year to date",
                        window_stats[w]["count"], window_stats[w]["p1"],
                        window_stats[w]["open"], _WINDOW_COLORS[w])
                  for w in ("30d", "60d", "90d", "YTD"))
        + _chip("Lifetime", true_total, priority_counts.get("P1", 0),
                len(open_t), "var(--text-3)")
        + "</div>"
    )

    # Vertical marker lines on the trend chart at each window boundary —
    # dashed, arrow-labelled, fixed colors distinct from the brand palette.
    trend_markers_html = ""
    if sorted_months:
        span_start = _dt.datetime.strptime(sorted_months[0], "%Y-%m")
        span_days = max((now_ts - span_start).days, 1)
        stagger = 0
        for wlbl in ("YTD", "90d", "60d", "30d"):
            try:
                bdate = _dt.datetime.fromisoformat(window_bounds[wlbl][:19])
            except ValueError:
                continue
            if bdate <= span_start:
                continue
            pos = (bdate - span_start).days / span_days * 100
            color = _WINDOW_COLORS[wlbl]
            top_off = -30 - (stagger % 2) * 12
            stagger += 1
            trend_markers_html += (
                f'      <div style="position:absolute;left:{pos:.1f}%;top:0;bottom:0;'
                f'border-left:2px dashed {color};z-index:5;pointer-events:none;">'
                f'<span style="position:absolute;top:{top_off}px;left:-8px;font-size:9px;'
                f'font-weight:800;color:{color};white-space:nowrap;">▼ {wlbl}</span></div>\n'
            )

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

    # YTD feature areas with a lifetime view of the same causes — the near-term
    # signal (what's biting NOW) shown against how chronic each cause is.
    # Slate fill is fixed, not brand-derived, matching the YTD window marker.
    ytd_t = [t for t in all_t if (t.get("created") or "") >= ytd_start]
    ytd_area: Counter = Counter()
    for t in ytd_t:
        fa = (t.get("feature_area") or "").strip()
        if fa:
            ytd_area[fa] += 1
    top_ytd = ytd_area.most_common(5)
    max_ytd = top_ytd[0][1] if top_ytd else 1
    hbar_ytd_html = ""
    for area, cnt in top_ytd:
        pct = int(cnt / max_ytd * 100)
        life = area_counter.get(area, 0)
        life_pct = f"{cnt / life * 100:.0f}%" if life else "—"
        hbar_ytd_html += (
            f'    <div class="hbar-row"><div class="hbar-label">{area} '
            f'<span style="color:var(--text-3);font-size:10px;">· {life} lifetime ({life_pct} of them YTD)</span></div>'
            f'<div class="hbar-track"><div class="hbar-fill" style="width:{pct}%;background:#475569;"></div></div>'
            f'<div class="hbar-val">{cnt}</div></div>\n'
        )

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
    # Replace trend chart — position:relative hosts the window marker lines
    # (extra top margin clears the arrow labels); window chips render below.
    import re as _re
    tpl = _re.sub(
        r'(<div class="trend-chart">).*?(</div>\s*\n\s*<div class="trend-labels">).*?(</div>\s*\n\s*</div>)',
        lambda m: (f'<div class="trend-chart" style="position:relative;margin-top:30px;">\n'
                   f'{trend_bars_html}{trend_markers_html}    {m.group(2)}\n'
                   f'{trend_labels_html}    </div>\n  {trend_legend_html}\n  {window_chips_html}\n  </div>'),
        tpl, flags=_re.DOTALL, count=1,
    )

    # Replace mix bar
    tpl = _re.sub(
        r'(<div class="mix-bar">).*?(</div>\s*\n\s*<div class="mix-legend">).*?(</div>\s*\n\s*</div>)',
        lambda m: f'{m.group(1)}{mix_bar_html}\n    {m.group(2)}\n{mix_legend_html}    {m.group(3)}',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Inject KPI values (page-subtitle) — disclose the analysis window when
    # the ticket set is a most-recent subset rather than full history.
    if windowed:
        _subtitle = (f"{true_total} {t_ticket.lower()}s lifetime · analyzing the {total} most recent "
                     f"({window_note}) · Generated {report_date}")
    else:
        _subtitle = f"{total} {t_ticket.lower()}s on record · Generated {report_date}"
    tpl = _re.sub(
        r'(<div class="page-subtitle">)[^<]*(</div>)',
        f'\\g<1>{_subtitle}\\g<2>',
        tpl, count=1,
    )

    _p1_scope_lbl = f"{t_p1}s Since {window_from}" if windowed else f"{t_p1}s Lifetime"
    _p1_scope_sub = f"{_pct('P1')}% of analyzed" if windowed else f"{_pct('P1')}% of all tickets"
    # Replace KPI tiles dynamically
    kpi_block = f"""  <div class="kpi-grid">
    <div class="kpi-tile"><span class="kpi-val cb">{true_total}</span><span class="kpi-lbl">Total {t_ticket}s</span>{f'<span class="kpi-sub">{total} analyzed: {window_note}</span>' if windowed else ''}</div>
    <div class="kpi-tile"><span class="kpi-val crit">{len(open_t)}</span><span class="kpi-lbl">Open Now</span><span class="kpi-sub">{open_breakdown or "—"}</span></div>
    <div class="kpi-tile"><span class="kpi-val warn">{priority_counts.get('P1',0)}</span><span class="kpi-lbl">{_p1_scope_lbl}</span><span class="kpi-sub">{_p1_scope_sub}</span></div>
    <div class="kpi-tile"><span class="kpi-val warn">{len(p1_90d)}</span><span class="kpi-lbl">{t_p1}s Last 90 Days</span></div>
    <div class="kpi-tile"><span class="kpi-val good">{avg_p1_res}</span><span class="kpi-lbl">Avg {t_p1} Resolution</span><span class="kpi-sub">solved in last 12 mo, n={len(solved_p1)}</span></div>
    <div class="kpi-tile"><span class="kpi-val good">{closed_rate}</span><span class="kpi-lbl">Closed Rate</span><span class="kpi-sub">{len(closed_t)} closed / {total} {"analyzed" if windowed else "total"}</span></div>
  </div>"""
    tpl = _re.sub(r'<div class="kpi-grid">.*?</div>\s*\n\s*\n', kpi_block + "\n\n", tpl, flags=_re.DOTALL, count=1)

    # Replace hbar section — lifetime, last-90d, and the YTD-vs-lifetime group
    tpl = _re.sub(
        r'(<div class="card-title">Feature area — lifetime[^<]*</div>\s*\n)(.*?)(<div class="hbar-group-title"[^>]*>[^<]*</div>)(.*?)(<div class="callout)',
        lambda m: (f'{m.group(1)}{hbar_html}    {m.group(3)}\n{hbar_recent_html}'
                   f'    <div class="hbar-group-title" style="margin-top:12px;color:#475569;">'
                   f'Year to date — near-term causes vs their lifetime totals</div>\n'
                   f'{hbar_ytd_html}    {m.group(5)}'),
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

    # ── Conditional renewal badge (only if any ticket is actually tagged critical_renewal) ──
    # Authoritative critical_renewal comes from the live zdorg record (org_meta),
    # NOT from ticket tags — a ticket's "critical_renewal" tag reflects the org's
    # status at ticket-creation time and can be stale. Fall back to the tag-scan
    # heuristic only if the live lookup wasn't available.
    if org_meta and "critical_renewal" in org_meta:
        is_critical_renewal = bool(org_meta.get("critical_renewal"))
    else:
        is_critical_renewal = any("critical_renewal" in (t.get("tags") or "") for t in all_t)
    renewal_badge_html = '<span class="renewal-badge internal-only">⚠ Critical Renewal</span>' if is_critical_renewal else ""
    tpl = tpl.replace('<span class="renewal-badge internal-only">⚠ Critical Renewal</span>', renewal_badge_html, 1)

    # ── Dynamic narrative (previously static example prose leaked from the WU template) ──
    open_p1_ct = open_priority.get('P1', 0)
    health_word = "stable" if open_p1_ct == 0 and float(closed_rate.rstrip('%') or 0) >= 90 else "needs attention"
    lede = (
        f"{org}'s account health is <strong>{health_word}</strong>. "
        f"{len(open_t)} {t_ticket.lower()}{'s' if len(open_t) != 1 else ''} currently open"
        f"{' (' + open_breakdown + ')' if open_breakdown else ''}, "
        f"against a lifetime closed rate of {closed_rate}."
    )

    if p1_res:
        point1 = (
            f"<strong>{t_p1} resolution is tracked.</strong> Average resolution across the last "
            f"{len(solved_p1)} solved {t_p1}{'s' if len(solved_p1) != 1 else ''} is {avg_p1_res}."
        )
        point1_icon_cls = "good"
        point1_icon = "✓"
    else:
        point1 = f"<strong>No {t_p1}s solved in the last 12 months</strong> — insufficient data to assess resolution speed."
        point1_icon_cls = "cb"
        point1_icon = "ℹ"

    # Component drill-down for the leading area — broad classifier buckets like
    # 'Capella / Cloud Platform' break down by the ticket's full Component path
    # (e.g. Capella::Private Networking::Private endpoints).
    trend_breakdown = ""
    if top_recent:
        top_area, top_area_cnt = top_recent[0]
        _comp_counter: Counter = Counter()
        for t in recent_t:
            if (t.get("feature_area") or "").strip() == top_area:
                comp = ((t.get("ticket_fields") or {}).get("Component") or "").strip()
                if comp:
                    parts = comp.split("::")
                    _comp_counter[" › ".join(parts[1:]) or parts[0]] += 1
        _subs = _comp_counter.most_common(4)
        if len(_subs) >= 2:
            trend_breakdown = (" Within it: "
                               + ", ".join(f"{s} ({n})" for s, n in _subs)
                               + ("." if sum(n for _, n in _subs) >= top_area_cnt
                                  else f" — plus {top_area_cnt - sum(n for _, n in _subs)} untagged."))

    if top_recent:
        recent_share = top_area_cnt / max(len(recent_t), 1) * 100
        lifetime_share = area_counter.get(top_area, 0) / max(total, 1) * 100
        point2 = (
            f"<strong>'{top_area}' leads recent volume.</strong> {top_area_cnt} of {len(recent_t)} tickets "
            f"in the last 90 days ({recent_share:.0f}%) vs a {lifetime_share:.0f}% share of {'the analyzed window' if windowed else 'lifetime'}."
            + trend_breakdown
        )
    else:
        point2 = "<strong>No ticket volume in the last 90 days</strong> to establish a feature-area trend."

    if open_t:
        oldest = min(open_t, key=lambda t: t.get("created") or "")
        o_created = oldest.get("created") or ""
        try:
            o_age = (now_ts - _dt.datetime.fromisoformat(o_created[:19])).days
        except Exception:
            o_age = None
        point3 = (
            f"<strong>Oldest open item is #{oldest.get('ticket_id','')}.</strong> "
            f"{(oldest.get('subject') or '—')[:70]}"
            + (f" — open {o_age} days." if o_age is not None else ".")
        )
    else:
        point3 = f"<strong>No open {t_ticket.lower()}s.</strong> Queue is clear as of this report."

    exec_block = f"""  <div class="card exec-summary">
    <p class="exec-lede">{lede}</p>

    <div class="exec-points">
      <div class="exec-point">
        <span class="exec-point-icon {point1_icon_cls}">{point1_icon}</span>
        <div class="exec-point-body">{point1}</div>
      </div>
      <div class="exec-point">
        <span class="exec-point-icon warn">↗</span>
        <div class="exec-point-body">{point2}</div>
      </div>
      <div class="exec-point">
        <span class="exec-point-icon cb">→</span>
        <div class="exec-point-body">{point3}</div>
      </div>
    </div>
  </div>"""
    tpl = _re.sub(
        r'<div class="card exec-summary">.*?(?=\n\s*<!-- ── KPI STRIP)',
        exec_block + "\n", tpl, flags=_re.DOTALL, count=1,
    )

    # Feature-area trend callout (reuses point2's computed numbers, worded as a standalone note)
    if top_recent:
        trend_note = (
            f"<strong>Trend:</strong> '{top_area}' accounts for {recent_share:.0f}% of the last 90 days of tickets "
            f"({top_area_cnt} of {len(recent_t)}), vs a {lifetime_share:.0f}% share of {'the analyzed window' if windowed else 'lifetime'}."
            + trend_breakdown
        )
    else:
        trend_note = "<strong>Trend:</strong> not enough recent ticket volume to establish a feature-area shift."
    tpl = _re.sub(
        r'<div class="callout callout-info">.*?</div>\s*\n\s*</div>',
        f'<div class="callout callout-info">\n    <span class="callout-icon">ℹ</span>\n    <div class="callout-body">{trend_note}</div>\n  </div>',
        tpl, flags=_re.DOTALL, count=1,
    )

    # P1 year-over-year card — inserted just before the P1 log card (only
    # when the account spans 2+ years of P1 history).
    if p1_year_html:
        tpl = _re.sub(
            r'(<div class="card">\s*<div class="card-title">\d+ P1s since)',
            lambda m: p1_year_html + m.group(1),
            tpl, count=1,
        )

    # P1 log card title — dynamic count
    tpl = _re.sub(
        r'<div class="card-title">\d+ P1s since [^<]*</div>',
        f'<div class="card-title">{len(p1_12mo)} {t_p1}s in the last 12 months</div>',
        tpl, count=1,
    )

    # P1 health callout — dynamic
    if p1_res:
        p1_callout_body = (
            f"<strong>{t_p1} response {'is healthy' if float(avg_p1_res.rstrip('d') or 999) < 5 else 'is tracked'}.</strong> "
            f"Average resolution across the last {len(solved_p1)} solved {t_p1}{'s' if len(solved_p1) != 1 else ''} is {avg_p1_res}."
        )
        p1_callout_cls, p1_callout_icon = "callout-good", "✓"
    else:
        p1_callout_body = f"<strong>No {t_p1}s solved in the last 12 months</strong> — nothing to measure resolution time against yet."
        p1_callout_cls, p1_callout_icon = "callout-info", "ℹ"
    tpl = _re.sub(
        r'<div class="callout callout-good">.*?</div>\s*\n\s*</div>',
        f'<div class="callout {p1_callout_cls}">\n    <span class="callout-icon">{p1_callout_icon}</span>\n    <div class="callout-body">{p1_callout_body}</div>\n  </div>',
        tpl, flags=_re.DOTALL, count=1,
    )

    # Recommendations — built from real signals only, no hardcoded ticket refs.
    # Each ticket-specific item carries enough detail to act on without opening
    # the ticket: priority, area, age, last activity, assignee, concrete ask.
    rec_items: list[tuple[str, str]] = []
    if open_t:
        oldest = min(open_t, key=lambda t: t.get("created") or "")
        o_pri = _priority(oldest)
        o_area = (oldest.get("feature_area") or "").strip()
        o_assignee = (oldest.get("assignee") or "").strip() or "unassigned"
        o_last_act = (oldest.get("last_comment_at") or "")[:10]
        try:
            o_age_days = (now_ts - _dt.datetime.fromisoformat((oldest.get("created") or "")[:19])).days
            _age_txt = f"open {o_age_days} days (since {(oldest.get('created') or '')[:10]})"
        except Exception:
            _age_txt = f"open since {(oldest.get('created') or '')[:10]}"
        rec_items.append((
            "both",
            f"<strong>Review oldest open item #{oldest.get('ticket_id','')}"
            f" ({o_pri}{', ' + o_area if o_area else ''}).</strong> "
            f"“{(oldest.get('subject') or '—')[:80]}” — {_age_txt}; "
            f"last activity {o_last_act or 'not recorded'}; assigned to {o_assignee}. "
            f"Ask: confirm it is still reproducible and agree a close-or-escalate path.",
        ))
    _stall_cut = (now_ts - _dt.timedelta(days=7)).isoformat()
    stalled = sorted(
        [t for t in open_t if (t.get("last_comment_at") or t.get("created") or "") < _stall_cut],
        key=lambda t: t.get("last_comment_at") or "",
    )
    _oldest_id = (min(open_t, key=lambda t: t.get("created") or "").get("ticket_id") if open_t else None)
    stalled = [t for t in stalled if t.get("ticket_id") != _oldest_id]
    if stalled:
        _refs = ", ".join(
            f"#{t.get('ticket_id','')} ({_priority(t)}, quiet since {(t.get('last_comment_at') or '')[:10] or '?'})"
            for t in stalled[:3]
        )
        _more = f" and {len(stalled) - 3} more" if len(stalled) > 3 else ""
        rec_items.append((
            "cb",
            f"<strong>{len(stalled)} open item{'s' if len(stalled) != 1 else ''} with no activity in 7+ days:</strong> "
            f"{_refs}{_more}. Post a status update or confirm the blocker on each.",
        ))
    if open_p1_ct > 0:
        _p1_refs = ", ".join(
            f"#{t.get('ticket_id','')} ({(t.get('subject') or '')[:45]}…)"
            for t in open_t if _priority(t) == "P1"
        )[:220]
        rec_items.append((
            "cb",
            f"<strong>{open_p1_ct} open {t_p1.lower()}{'s' if open_p1_ct != 1 else ''} need active tracking:</strong> "
            f"{_p1_refs} — daily check-ins until resolved.",
        ))
    if not rec_items:
        rec_items.append((
            "both",
            f"<strong>No urgent action items.</strong> Queue is clear and recent {t_p1.lower()} resolution has been timely.",
        ))
    rec_html = "\n".join(
        f'      <div class="next-step">\n        <span class="step-owner {owner}">{owner.upper() if owner != "both" else "CB + Customer"}</span>\n        <div class="step-body">{body}</div>\n      </div>'
        for owner, body in rec_items
    )
    tpl = _re.sub(
        r'<div class="next-steps">.*?(?=\n\n</div>\s*\n</body>)',
        f'<div class="next-steps">\n{rec_html}\n    </div>\n  </div>',
        tpl, flags=_re.DOTALL, count=1,
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

    # Customer logo in the header, same as the health report banner
    if brand.get("logo_url"):
        tpl = tpl.replace(
            '<div class="header">',
            f'<div class="header"><img src="{brand["logo_url"]}" alt="{org} logo" '
            f'style="height:24px;object-fit:contain;vertical-align:middle;margin-right:10px;">',
            1,
        )

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
    max_tickets: int = 2000,
    date_from: str = "",
    date_to: str = "",
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
        max_tickets:  Max tickets to analyze (default 2000, ceiling 5000).
                      If the org has more lifetime tickets than this, the
                      report analyzes the most recent N and discloses the
                      from→to date window on every lifetime-sounding figure.
        date_from:    Optional ISO date (YYYY-MM-DD) — analyze only tickets
                      created on/after this date ("last 3 years", "July→August"
                      style reports). The window is disclosed in the report.
        date_to:      Optional ISO date — analyze only tickets created on/before
                      this date.
    """
    import datetime as _dt
    from supportal.agent_tools import _save_asset_to_cb

    cfg  = _cfg()
    app  = _app()
    report_date = _dt.date.today().strftime("%B %-d, %Y")
    cap = min(max_tickets, 5000)

    # Pull tickets
    try:
        tickets = app.tool_query_tickets(
            {"organization": organization, "limit": cap},
            *_cb_tuple(cfg),
            limit=cap,
        )
    except Exception as exc:
        _log_tool_failure("generate_health_report", exc, organization)
        return json.dumps({"error": f"Failed to query tickets: {exc}"})

    if not tickets:
        return json.dumps({"error": f"No tickets found for organization '{organization}'."})

    # Optional explicit date range — the true_total stays lifetime, so the
    # windowed-disclosure path in the builder announces the range automatically.
    if date_from:
        tickets = [t for t in tickets if (t.get("created") or "")[:10] >= date_from[:10]]
    if date_to:
        tickets = [t for t in tickets if (t.get("created") or "")[:10] <= date_to[:10]]
    if not tickets:
        return json.dumps({"error": f"No tickets for '{organization}' between "
                                    f"'{date_from or 'beginning'}' and '{date_to or 'now'}'."})

    # True lifetime count — never present a capped window as full history.
    true_total = len(tickets)
    try:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, QueryOptions
        _conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        _cl = Cluster(_conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        _ks = f"`{cfg['bucket']}`.`{cfg['scope']}`.`{cfg['collection']}`"
        _orgs = sorted({organization.lower().strip(),
                        (tickets[0].get("organization") or organization).lower().strip()})
        _rows = list(_cl.query(
            f"SELECT RAW COUNT(*) FROM {_ks} t WHERE LOWER(t.organization) IN $orgs",
            QueryOptions(named_parameters={"orgs": _orgs}),
        ))
        if _rows and isinstance(_rows[0], int):
            true_total = max(_rows[0], len(tickets))
    except Exception:
        pass  # best-effort — fall back to analyzed count

    # ── Authoritative account metadata from live Supportal (zdorg) ──────────
    # Ticket-level tags (e.g. a stale "critical_renewal" tag copied at ticket-creation
    # time) are NOT reliable for current account status — always prefer the live
    # zdorg.organization_fields record, which is Zendesk's current source of truth.
    org_meta: dict = {}
    try:
        safe_org_name = organization.replace('"', '\\"')
        meta_stmt = f"""
SELECT zo.`organization_fields`.`critical_renewal` AS critical_renewal,
       zo.`organization_fields`.`strategic` AS strategic,
       zo.`organization_fields`.`carr` AS carr,
       zo.`organization_fields`.`ase` AS ase,
       zo.`organization_fields`.`account_owner` AS account_owner,
       zo.`organization_fields`.`account_owner_email` AS account_owner_email
FROM customer cu
LEFT JOIN zdorg zo ON META(zo).id = ("ZendeskSupport/organizations::" || TO_STRING(cu.`zendeskorg`))
WHERE cu.`name` = "{safe_org_name}"
LIMIT 1
""".strip()
        meta_rows = app.query_supportal_analytics(meta_stmt, "")
        if meta_rows:
            org_meta = meta_rows[0]
    except Exception:
        pass  # Live lookup is best-effort — fall back to caller-supplied args / no badge.

    # Auto-fill AE/TSE from the authoritative record if the caller didn't specify them.
    if not ae_name and org_meta.get("account_owner"):
        ae_name = org_meta["account_owner"]
    if not ae_email and org_meta.get("account_owner_email"):
        ae_email = org_meta["account_owner_email"]
    if not tse_name and org_meta.get("ase"):
        tse_name = org_meta["ase"]

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
        html = _build_health_report_html(organization, tickets, report_date, brand, org_meta, true_total=true_total)
    except Exception as exc:
        _log_tool_failure("generate_health_report", exc, organization)
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
        _log_tool_failure("generate_ticket_report", exc, f"ticket:{ticket_id}")
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
        _log_tool_failure("generate_ticket_report", exc, f"ticket:{ticket_id}")
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


def _version_currency(version_history: list[str]) -> tuple[str, int]:
    """Rough version-currency classification from the latest known CB version.
    Boundaries are approximate (Couchbase's public EOL policy, not a live feed)
    — good enough to flag clearly-outdated clusters, not a precise SLA check."""
    import re as _re2
    if not version_history:
        return ("Unknown", 50)
    m = _re2.match(r"(\d+)\.(\d+)", version_history[-1] or "")
    if not m:
        return ("Unknown", 50)
    major, minor = int(m.group(1)), int(m.group(2))
    if major < 7 or (major == 7 and minor < 2):
        return ("End of Life", 20)
    if major == 7 and minor < 6:
        return ("Aging", 60)
    return ("Current", 100)


def _cluster_priority(t: dict) -> str:
    p = (t.get("priority") or "").lower()
    return {"urgent": "P1", "p1": "P1", "high": "P2", "p2": "P2",
            "normal": "P3", "p3": "P3", "low": "P4", "p4": "P4"}.get(p, "P?")


_GA_VERSION_CACHE: dict[str, Any] = {"version": None, "fetched_at": 0.0}
_GA_VERSION_CACHE_TTL = 6 * 3600  # refetch at most every 6h


def _current_ga_version() -> str | None:
    """Latest published Couchbase Server GA version, per Docker Hub's official
    `couchbase` image tags — the closest thing to a live feed for "what's
    the current release on couchbase.com" without scraping the marketing site.
    """
    now = time.time()
    if _GA_VERSION_CACHE["version"] and (now - _GA_VERSION_CACHE["fetched_at"]) < _GA_VERSION_CACHE_TTL:
        return _GA_VERSION_CACHE["version"]
    import re as _re

    def _ver_tuple(v: str) -> tuple[int, int, int]:
        m = _re.match(r"(\d+)\.(\d+)\.(\d+)$", v or "")
        return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)

    try:
        req = urllib.request.Request(
            "https://hub.docker.com/v2/repositories/library/couchbase/tags?page_size=100",
            headers={"User-Agent": "supportal-scraper/1.0"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.load(resp)
        tags = [r["name"] for r in data.get("results", []) if _re.match(r"^\d+\.\d+\.\d+$", r.get("name", ""))]
        latest = max(tags, key=_ver_tuple, default=None)
        if latest:
            _GA_VERSION_CACHE["version"] = latest
            _GA_VERSION_CACHE["fetched_at"] = now
        return latest
    except Exception:
        return _GA_VERSION_CACHE["version"]  # stale cache beats nothing on a transient failure


def _build_cluster_health_chart_html(org: str, health: dict, report_date: str, max_clusters: int) -> str:
    """Render an HTML cluster-health report: composite health score (issues +
    version currency + ticket correlation) with a transparent breakdown, named
    recurring issues, and a bad/warn-over-time SVG chart. Worst clusters first."""
    import html as _html
    import datetime as _dt
    import re as _re

    ci = health.get("cluster_index") or {}
    by_cluster = health.get("by_cluster") or {}
    ticket_by_cid = health.get("ticket_by_cid") or {}

    def _snap_index(snap_id: str) -> int:
        try:
            return int(str(snap_id).rsplit("::", 1)[-1])
        except Exception:
            return -1

    def _latest_point(cid: str) -> dict | None:
        """The true most-recent snapshot for a cluster. Prefers the real
        collection `date` (Supportal's snapshot.timestamp) when present —
        that's ground truth and immune to rescrapes reassigning snap_id
        suffixes out of chronological order. Falls back to snapshot index
        (ClusterUUID::N) only for older docs scraped before `date` was
        backfilled — never list order or version_history, which only
        records the FIRST-seen (often oldest) distinct version."""
        points = by_cluster.get(cid) or []
        if not points:
            return None
        dated = [p for p in points if p.get("date")]
        if dated:
            return max(dated, key=lambda p: p["date"])
        return max(points, key=lambda p: _snap_index(p.get("snap_id", "")))

    def _version_since(cid: str, version: str) -> str | None:
        """Date the cluster's current version first appeared, walking back
        from the latest snapshot while the version stays the same. Used to
        scope ticket correlation to the cluster's CURRENT version — tickets
        from a prior version aren't this version's problem."""
        points = sorted(
            [p for p in (by_cluster.get(cid) or []) if p.get("date")],
            key=lambda p: p["date"],
        )
        since = None
        for p in reversed(points):
            if p.get("cb_version") == version:
                since = p["date"]
            else:
                break
        return since

    scored = []
    for c in ci.values():
        cid = c.get("cluster_id", "")

        # Only report against the latest snapshot, not whatever happens to be
        # first/last in version_history (which is oldest-seen, not newest).
        latest = _latest_point(cid)
        latest_version = latest.get("cb_version") if latest else (c.get("version_history") or ["—"])[0] if c.get("version_history") else "—"
        latest_nodes = latest.get("node_count") if latest else c.get("node_count_last")
        version_since = _version_since(cid, latest_version) if latest_version and latest_version != "—" else None

        # 1. Issue health — point-in-time snapshot of the latest bad/warn
        # counts only, not a history-wide average (a health score should
        # reflect the cluster's current state, not penalize it forever for
        # issues that were already resolved in earlier snapshots).
        latest_bad = latest.get("bad_count", 0) if latest else 0
        latest_warn = latest.get("warn_count", 0) if latest else 0
        issue_score = max(0, 100 - latest_bad * 15 - latest_warn * 3)

        # 2. Version currency — from the latest snapshot only.
        ver_label, ver_score = _version_currency([latest_version] if latest_version and latest_version != "—" else [])

        # 3. Ticket correlation — linked ticket volume + P1 weight, scoped to
        # tickets filed since the cluster started running its CURRENT
        # version. A ticket from two major versions ago isn't this version's
        # problem and shouldn't count against it.
        all_linked = ticket_by_cid.get(cid) or []
        linked = (
            [t for t in all_linked if (t.get("created") or "") >= version_since]
            if version_since else all_linked
        )
        p1_count = sum(1 for t in linked if _cluster_priority(t) == "P1")
        ticket_score = max(0, 100 - len(linked) * 4 - p1_count * 15)

        # Weighting: version currency is the dominant driver of health (EOL
        # software is the biggest real risk), issue counts are a minor signal
        # (checker noise fluctuates snapshot to snapshot), tickets matter but
        # less than running an unsupported version.
        composite = round(issue_score * 0.05 + ver_score * 0.65 + ticket_score * 0.30)
        if composite >= 80:
            grade, grade_cls = "Healthy", "pill-good"
        elif composite >= 60:
            grade, grade_cls = "Watch", "pill-warn"
        elif composite >= 40:
            grade, grade_cls = "At Risk", "pill-warn"
        else:
            grade, grade_cls = "Critical", "pill-crit"

        # Likely-deprecated: not already flagged via successor detection, but
        # BOTH snapshot activity and ticket activity have gone quiet for a long
        # stretch (~10 months) — reads as abandoned infra, not "just stale".
        _DEPRECATION_QUIET_DAYS = 300
        last_ticket_date = max((t.get("created") or "" for t in linked), default="")
        _now = _dt.datetime.now(_dt.timezone.utc)
        def _days_since(iso: str) -> float | None:
            if not iso:
                return None
            try:
                d = _dt.datetime.fromisoformat(iso[:19])
                if d.tzinfo is None:
                    d = d.replace(tzinfo=_dt.timezone.utc)
                return (_now - d).days
            except Exception:
                return None
        snap_quiet_days = _days_since(c.get("last_seen") or "")
        ticket_quiet_days = _days_since(last_ticket_date)
        is_likely_deprecated = (
            not c.get("is_deprecated")
            and not c.get("is_active")
            and (snap_quiet_days is None or snap_quiet_days > _DEPRECATION_QUIET_DAYS)
            and (ticket_quiet_days is None or ticket_quiet_days > _DEPRECATION_QUIET_DAYS)
        )

        scored.append({
            **c,
            "_composite": composite, "_grade": grade, "_grade_cls": grade_cls,
            "_issue_score": round(issue_score), "_ver_label": ver_label, "_ver_score": ver_score,
            "_ticket_score": round(ticket_score), "_linked_tickets": len(linked), "_p1_count": p1_count,
            "_is_likely_deprecated": is_likely_deprecated,
            "_quiet_days": min(d for d in (snap_quiet_days, ticket_quiet_days) if d is not None) if (snap_quiet_days or ticket_quiet_days) else None,
            "_latest_version": latest_version, "_latest_nodes": latest_nodes,
            "_latest_bad": latest_bad, "_latest_warn": latest_warn,
            "_version_since": version_since,
        })

    # Report only against unique, genuinely active clusters — a deprecated or
    # long-quiet cluster's bad score isn't actionable, it's noise.
    active_scored = [c for c in scored if c.get("is_active") and not c.get("is_deprecated") and not c["_is_likely_deprecated"]]
    excluded_count = len(scored) - len(active_scored)

    # Most-recent version actually observed in this org's active fleet — a
    # grounded reference point, not a claim about Couchbase's official release
    # calendar (we have no live feed for that).
    def _ver_tuple(v: str) -> tuple[int, int, int]:
        m = _re.match(r"(\d+)\.(\d+)\.(\d+)", v or "")
        return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)
    fleet_versions = [c["_latest_version"] for c in active_scored if c.get("_latest_version") and c["_latest_version"] != "—"]
    fleet_latest_version = max(fleet_versions, key=_ver_tuple, default=None)
    fleet_latest_tuple = _ver_tuple(fleet_latest_version) if fleet_latest_version else (0, 0, 0)

    # Current Couchbase Server GA release, per the official Docker Hub image
    # tags — an actual "what's shipping today on couchbase.com" reference,
    # distinct from fleet_latest_version above (which is just this org's own
    # newest-observed version, not the product's release calendar).
    current_ga_version = _current_ga_version()
    current_ga_tuple = _ver_tuple(current_ga_version) if current_ga_version else (0, 0, 0)

    clusters = sorted(active_scored, key=lambda c: c["_composite"])[:max_clusters]

    def _bar_chart_svg(cid: str) -> str:
        points = by_cluster.get(cid) or []
        if not points:
            return '<div class="callout callout-info"><span class="callout-icon">ℹ</span><div class="callout-body">No snapshot history to chart.</div></div>'
        w, h, pad_l, pad_b, pad_t = 700, 160, 34, 26, 10
        plot_w, plot_h = w - pad_l - 10, h - pad_t - pad_b
        max_val = max((p.get("bad_count", 0) + p.get("warn_count", 0) for p in points), default=1) or 1
        n = len(points)
        bw = max(plot_w / n * 0.6, 3)
        gap = plot_w / n
        bars = []
        labels = []
        for i, p in enumerate(points):
            x = pad_l + i * gap + (gap - bw) / 2
            bad = p.get("bad_count", 0)
            warn = p.get("warn_count", 0)
            bad_h = (bad / max_val) * plot_h
            warn_h = (warn / max_val) * plot_h
            y_bad = pad_t + plot_h - bad_h
            y_warn = y_bad - warn_h
            if bad:
                bars.append(f'<rect x="{x:.1f}" y="{y_bad:.1f}" width="{bw:.1f}" height="{bad_h:.1f}" fill="var(--crit)" stroke="#7A1A1A" stroke-width="0.5"><title>{_html.escape(p.get("date","")[:10])}: {bad} bad</title></rect>')
            if warn:
                bars.append(f'<rect x="{x:.1f}" y="{y_warn:.1f}" width="{bw:.1f}" height="{warn_h:.1f}" fill="var(--warn)" stroke="#7A4A06" stroke-width="0.5"><title>{_html.escape(p.get("date","")[:10])}: {warn} warn</title></rect>')
            if n <= 12 or i % max(1, n // 10) == 0:
                labels.append(f'<text x="{x + bw/2:.1f}" y="{h - 8}" font-size="9" fill="var(--text-3)" text-anchor="middle">{_html.escape(p.get("date","")[5:10])}</text>')
        axis = f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{w - 10}" y2="{pad_t + plot_h}" stroke="var(--border)" stroke-width="1"/>'
        return (
            f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" xmlns="http://www.w3.org/2000/svg">'
            f'{axis}{"".join(bars)}{"".join(labels)}</svg>'
        )

    def _score_chip_cls(score: int) -> str:
        if score >= 80: return "score-good"
        if score >= 50: return "score-warn"
        return "score-crit"

    def _named_issues_html(c: dict) -> str:
        bad_items = (c.get("top_bad_items") or [])[:5]
        warn_items = (c.get("top_warn_items") or [])[:5]
        if not bad_items and not warn_items:
            return '<span style="font-size:11px; color:var(--text-3);">No recurring named issues.</span>'
        chips = "".join(f'<span class="issue-chip issue-chip-bad">{_html.escape(x)}</span>' for x in bad_items)
        chips += "".join(f'<span class="issue-chip issue-chip-warn">{_html.escape(x)}</span>' for x in warn_items)
        return chips

    def _letter_grade(score: int) -> str:
        if score >= 85: return "A"
        if score >= 70: return "B"
        if score >= 55: return "C"
        if score >= 40: return "D"
        return "F"

    def _cause_phrase(c: dict) -> str:
        drivers = [
            ("Issues", c["_issue_score"], f'high issue volume in latest snapshot ({c.get("_latest_bad",0)} bad / {c.get("_latest_warn",0)} warn)'),
            ("Version", c["_ver_score"], f'an {c["_ver_label"].lower()} version ({c.get("_latest_version") or "—"})'),
            ("Tickets", c["_ticket_score"], f'ticket correlation on current version ({c["_linked_tickets"]} linked, {c["_p1_count"]} P1)'),
        ]
        drivers.sort(key=lambda d: d[1])
        worst = drivers[0]
        second = drivers[1]
        phrase = f"driven by {worst[2]}"
        if second[1] < 50:
            phrase += f", plus {second[2]}"
        return phrase

    def _issue_trend(points: list[dict]) -> tuple[str, str, str]:
        """Compare issue-severity score (bad/warn only) across the first vs
        second half of snapshot history. Returns (label, css_class, arrow)."""
        if len(points) < 4:
            return ("Not enough history", "trend-flat", "—")
        def _issue_score_pt(p: dict) -> float:
            return max(0, 100 - p.get("bad_count", 0) * 15 - p.get("warn_count", 0) * 3)
        mid = len(points) // 2
        first_avg = sum(_issue_score_pt(p) for p in points[:mid]) / mid
        second_avg = sum(_issue_score_pt(p) for p in points[mid:]) / (len(points) - mid)
        delta = second_avg - first_avg
        if delta > 8:
            return ("Improving", "trend-good", "↑")
        if delta < -8:
            return ("Worsening", "trend-bad", "↓")
        return ("Stable", "trend-flat", "→")

    def _version_gap_note(c: dict) -> str:
        if not c.get("_latest_version"):
            return ""
        this_tuple = _ver_tuple(c["_latest_version"])
        if current_ga_version and this_tuple < current_ga_tuple:
            return f' <span style="color:var(--text-3);">(current GA: {_html.escape(current_ga_version)})</span>'
        if fleet_latest_version and this_tuple < fleet_latest_tuple:
            return f' <span style="color:var(--text-3);">(fleet newest: {_html.escape(fleet_latest_version)})</span>'
        return ""

    def _sdk_note(cid: str) -> str:
        """Best-effort only — SDK version isn't tracked in snapshot health-check
        data at all, only sporadically on individual tickets. Don't imply
        systematic SDK-deprecation coverage; just surface what's mentioned."""
        linked = ticket_by_cid.get(cid) or []
        sdks = set()
        for t in linked:
            raw_fields = t.get("ticket_fields")
            if isinstance(raw_fields, str):
                try:
                    raw_fields = json.loads(raw_fields)
                except Exception:
                    raw_fields = {}
            sdk = (raw_fields or {}).get("Couchbase_Server_SDK_or_Connector")
            if sdk:
                sdks.add(sdk)
        if not sdks:
            return ""
        chips = "".join(f'<span class="issue-chip issue-chip-warn">{_html.escape(s)}</span>' for s in sorted(sdks))
        return f'<div style="margin-top:6px; font-size:10px; color:var(--text-3);">SDKs mentioned in linked tickets (not systematically tracked): {chips}</div>'

    cluster_cards = []
    for c in clusters:
        cid = c.get("cluster_id", "")
        name = c.get("cluster_name") or cid[:12]
        if c.get("is_deprecated"):
            lifecycle = "Deprecated"
        elif c.get("_is_likely_deprecated"):
            lifecycle = "Likely Deprecated"
        elif c.get("is_active"):
            lifecycle = "Active"
        else:
            lifecycle = "Stale"
        lifecycle_cls = {
            "Active": "pill-good", "Stale": "pill-warn",
            "Deprecated": "pill-neutral", "Likely Deprecated": "pill-neutral",
        }[lifecycle]
        nodes = c.get("_latest_nodes") or c.get("node_count_last") or "?"
        last_seen = (c.get("last_seen") or "")[:10]
        quiet_note = (
            f' <span style="font-size:10px; color:var(--text-3); font-weight:400; text-transform:none;">(~{c["_quiet_days"]//30}mo quiet)</span>'
            if lifecycle == "Likely Deprecated" and c.get("_quiet_days") else ""
        )
        ticket_note = (
            f' · <strong>{c["_linked_tickets"]}</strong> linked tickets'
            + (f' (<strong style="color:var(--crit);">{c["_p1_count"]} P1</strong>)' if c["_p1_count"] else "")
        ) if c["_linked_tickets"] else " · no linked tickets"

        trend_label, trend_cls, trend_arrow = _issue_trend(by_cluster.get(cid) or [])

        cluster_cards.append(f"""
  <div class="card">
    <div class="card-title" style="display:flex; justify-content:space-between; align-items:center;">
      <span>{_html.escape(name)} <span style="font-family: var(--mono); font-weight: 400; color: var(--text-3);">({_html.escape(cid[:16])})</span></span>
      <span style="display:flex; gap:6px; align-items:center;">
        <span class="pill {lifecycle_cls}">{lifecycle}</span>{quiet_note}
        <span class="score-badge {c['_grade_cls']}">{_letter_grade(c['_composite'])}</span>
        <span class="pill {c['_grade_cls']}">{c['_grade']} · {c['_composite']}/100</span>
        <span class="trend-pill {trend_cls}">{trend_arrow} {trend_label}</span>
      </span>
    </div>
    <div class="cause-phrase">{_html.escape(_cause_phrase(c))}</div>
    <div class="score-breakdown">
      <div class="score-item"><span class="score-chip {_score_chip_cls(c['_issue_score'])}">{c['_issue_score']}</span><span class="score-lbl">Issues (5%)<br><span style="color:var(--text-3);">latest: {c.get('_latest_bad',0)} bad / {c.get('_latest_warn',0)} warn</span></span></div>
      <div class="score-item"><span class="score-chip {_score_chip_cls(c['_ver_score'])}">{c['_ver_score']}</span><span class="score-lbl">Version (65%)<br><span style="color:var(--text-3);">{_html.escape(c['_ver_label'])} — {_html.escape(str(c.get('_latest_version') or '—'))}</span>{_version_gap_note(c)}</span></div>
      <div class="score-item"><span class="score-chip {_score_chip_cls(c['_ticket_score'])}">{c['_ticket_score']}</span><span class="score-lbl">Tickets (30%)<br><span style="color:var(--text-3);">{c['_linked_tickets']} linked, {c['_p1_count']} P1{f" (since {_html.escape(c['_version_since'][:10])})" if c.get('_version_since') else ""}</span></span></div>
    </div>
    <div class="ticket-meta" style="margin: 10px 0 6px;">
      <span class="ticket-meta-item">Nodes <strong>{nodes}</strong></span>
      <span class="ticket-meta-item">Snapshots <strong>{c.get("snapshot_count", 0)}</strong></span>
      <span class="ticket-meta-item">Last Seen <strong>{_html.escape(last_seen)}</strong></span>
    </div>
    <div class="named-issues">{_named_issues_html(c)}</div>
    {_sdk_note(cid)}
    {_bar_chart_svg(cid)}
  </div>""")

    kpi = f"""  <div class="kpi-grid">
    <div class="kpi-tile"><span class="kpi-val cb">{health.get("total_clusters",0)}</span><span class="kpi-lbl">Total Clusters</span></div>
    <div class="kpi-tile"><span class="kpi-val good">{health.get("active_clusters",0)}</span><span class="kpi-lbl">Active</span></div>
    <div class="kpi-tile"><span class="kpi-val warn">{health.get("stale_clusters",0)}</span><span class="kpi-lbl">Stale</span></div>
    <div class="kpi-tile"><span class="kpi-val cb">{health.get("deprecated_clusters",0)}</span><span class="kpi-lbl">Deprecated</span></div>
    <div class="kpi-tile"><span class="kpi-val {'good' if current_ga_version else 'warn'}" style="font-size:16px;">{_html.escape(current_ga_version or "unavailable")}</span><span class="kpi-lbl">Current GA Version</span><span class="kpi-sub" style="font-size:10px; color:var(--text-3);">latest Couchbase Server release (Docker Hub)</span></div>
    <div class="kpi-tile"><span class="kpi-val good" style="font-size:16px;">{_html.escape(fleet_latest_version or "—")}</span><span class="kpi-lbl">Newest Fleet Version</span><span class="kpi-sub" style="font-size:10px; color:var(--text-3);">most recent seen in this org's active clusters</span></div>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_html.escape(org)} — Cluster Health · {_html.escape(report_date)}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --bg: #F0F3F7; --surface: #FFFFFF; --text: #0D1926; --text-2: #5C6880; --text-3: #8C96A8;
    --border: #D4DAE6; --border-light: #E6EAF2; --cb: #1A6FD4; --cb-light: #EAF1FB;
    --good: #1DAA72; --good-light: #E6F7F0; --warn: #E8920A; --warn-light: #FEF3E2;
    --crit: #CC2E2E; --crit-light: #FDEAEA; --neutral: #6B7A99; --neutral-light: #EEF0F5;
    --mono: "SF Mono", "Cascadia Code", "Consolas", monospace;
  }}
  body {{ font-family: -apple-system, "Segoe UI", "Helvetica Neue", Arial, sans-serif; font-size: 14px; line-height: 1.6; color: var(--text); background: var(--bg); }}
  .page {{ max-width: 860px; margin: 0 auto; padding: 28px 24px 64px; display: flex; flex-direction: column; gap: 12px; }}
  .page-title {{ font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }}
  .page-subtitle {{ font-size: 13px; color: var(--text-2); margin-top: 3px; }}
  .section-label {{ font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.09em; color: #fff; background: var(--cb); padding: 7px 14px; border-radius: 6px; margin-top: 8px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; }}
  .kpi-tile {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; display: flex; flex-direction: column; gap: 4px; }}
  .kpi-val {{ font-size: 22px; font-weight: 800; line-height: 1; }}
  .kpi-val.crit {{ color: var(--crit); }} .kpi-val.warn {{ color: var(--warn); }} .kpi-val.good {{ color: var(--good); }} .kpi-val.cb {{ color: var(--cb); }}
  .kpi-lbl {{ font-size: 10px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-3); font-weight: 600; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px 18px; }}
  .card-title {{ font-size: 13px; font-weight: 700; color: var(--text); margin-bottom: 6px; }}
  .ticket-meta {{ display: flex; gap: 16px; flex-wrap: wrap; font-size: 11px; color: var(--text-2); }}
  .ticket-meta-item strong {{ color: var(--text); font-weight: 600; }}
  .pill {{ display: inline-flex; align-items: center; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; padding: 2px 8px; border-radius: 3px; }}
  .pill-good {{ background: var(--good-light); color: var(--good); border: 1px solid #A8DFC6; }}
  .pill-warn {{ background: var(--warn-light); color: var(--warn); border: 1px solid #F5D89A; }}
  .pill-neutral {{ background: var(--neutral-light); color: var(--neutral); border: 1px solid #C8CEDC; }}
  .pill-crit {{ background: var(--crit-light); color: var(--crit); border: 1px solid #F0BABA; }}
  .callout {{ border-radius: 6px; padding: 12px 14px; display: flex; gap: 10px; font-size: 12px; }}
  .callout-info {{ background: var(--cb-light); border: 1px solid #AECDF5; color: #0D3A6B; }}
  .score-breakdown {{ display: flex; gap: 16px; padding: 10px 0; border-top: 1px solid var(--border-light); border-bottom: 1px solid var(--border-light); }}
  .score-item {{ display: flex; align-items: center; gap: 8px; flex: 1; }}
  .score-chip {{ width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 800; flex-shrink: 0; }}
  .score-good {{ background: var(--good-light); color: var(--good); }}
  .score-warn {{ background: var(--warn-light); color: var(--warn); }}
  .score-crit {{ background: var(--crit-light); color: var(--crit); }}
  .score-lbl {{ font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-2); line-height: 1.5; }}
  .named-issues {{ display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 10px; }}
  .issue-chip {{ font-size: 10px; font-weight: 600; padding: 2px 7px; border-radius: 3px; }}
  .issue-chip-bad {{ background: var(--crit-light); color: var(--crit); border: 1px solid #F0BABA; }}
  .issue-chip-warn {{ background: var(--warn-light); color: var(--warn); border: 1px solid #F5D89A; }}
  .score-badge {{ width: 24px; height: 24px; border-radius: 5px; display: inline-flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 800; }}
  .score-badge.pill-good {{ background: var(--good); color: #fff; }}
  .score-badge.pill-warn {{ background: var(--warn); color: #fff; }}
  .score-badge.pill-crit {{ background: var(--crit); color: #fff; }}
  .score-badge.pill-neutral {{ background: var(--neutral); color: #fff; }}
  .cause-phrase {{ font-size: 12px; color: var(--text-2); font-style: italic; margin: 6px 0 10px; }}
  .trend-pill {{ display: inline-flex; align-items: center; gap: 3px; font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 3px; }}
  .trend-good {{ background: var(--good-light); color: var(--good); }}
  .trend-bad {{ background: var(--crit-light); color: var(--crit); }}
  .trend-flat {{ background: var(--neutral-light); color: var(--neutral); }}
</style>
</head>
<body>
<div class="page">
  <div>
    <div class="page-title">Cluster Health — {_html.escape(org)}</div>
    <div class="page-subtitle">Generated {_html.escape(report_date)} · showing {len(clusters)} of {len(active_scored)} active clusters (lowest composite score first) · {excluded_count} stale/deprecated clusters excluded from ranking</div>
  </div>
  <div class="section-label">Fleet Summary</div>
{kpi}
  <div class="section-label">Cluster Health — Worst First (score = 5% issues + 65% version + 30% tickets)</div>
{"".join(cluster_cards) if cluster_cards else '<div class="callout callout-info"><span class="callout-icon">ℹ</span><div class="callout-body">No cluster data found.</div></div>'}
</div>
</body>
</html>
"""


@mcp.tool()
def generate_cluster_health_chart(organization: str, max_clusters: int = 8) -> str:
    """
    Generate a cluster health report with per-cluster bad/warn-issue timeline
    charts, from local Couchbase snapshot + ticket data, and save it as an
    HTML asset.

    Shows fleet-wide KPIs (total/active/stale/deprecated clusters, total
    snapshots) plus one chart per cluster (most problematic first) showing
    checker "bad"/"warn" counts over time, alongside version/node/last-seen
    metadata.

    Args:
        organization: Customer org name (partial match).
        max_clusters: Max number of clusters to chart, most issues first (default 8).
    """
    import datetime as _dt
    from datetime import timedelta
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions, QueryOptions
    from couchbase.auth import PasswordAuthenticator
    from supportal.agent_tools import _save_asset_to_cb

    cfg = _cfg()
    app = _app()
    report_date = _dt.date.today().strftime("%B %-d, %Y")

    try:
        conn = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        cl.wait_until_ready(timedelta(seconds=15))
        snap_ks = f"`{cfg['bucket']}`.`{cfg['scope']}`.`snapshots`"
        snapshots = list(cl.query(
            f"SELECT s.* FROM {snap_ks} AS s WHERE LOWER(s.organization) LIKE $org "
            f"ORDER BY s.date DESC LIMIT 500",
            QueryOptions(named_parameters={"org": f"%{organization.lower()}%"}, timeout=timedelta(seconds=30)),
        ))
        cl.close()
    except Exception as exc:
        _log_tool_failure("generate_cluster_health_chart", exc, organization)
        return json.dumps({"error": f"Failed to load snapshots: {exc}"})

    if not snapshots:
        return json.dumps({"error": f"No snapshot data found for '{organization}'."})

    try:
        tickets = app.tool_query_tickets(
            {"organization": organization, "limit": 500}, *_cb_tuple(cfg), limit=500,
        )
    except Exception as exc:
        _log_tool_failure("generate_cluster_health_chart", exc, organization)
        return json.dumps({"error": f"Failed to load tickets: {exc}"})

    health = app.build_cluster_health_data(snapshots, tickets)

    # Enrich with live, human-assigned cluster names from Supportal Analytics
    # (cluster.ui_name) — local snapshot data usually only has the raw UUID.
    try:
        safe_org = organization.replace('"', '\\"')
        name_stmt = f"""
SELECT cl.`uuid` AS cluster_uuid, cl.`ui_name` AS ui_name
FROM cluster cl
JOIN customer cu ON cl.`customer` = cu.`name`
WHERE cu.`name` = "{safe_org}"
""".strip()
        name_rows = app.query_supportal_analytics(name_stmt, "")
        ci = health.get("cluster_index") or {}
        for r in name_rows:
            cid = r.get("cluster_uuid") or ""
            ui_name = r.get("ui_name") or ""
            # Only override when Supportal has a real friendly name, not the UUID fallback.
            if cid in ci and ui_name and ui_name != cid:
                ci[cid]["cluster_name"] = ui_name
    except Exception:
        pass  # Live enrichment is best-effort — local names/UUIDs still work as a fallback.

    html = _build_cluster_health_chart_html(organization, health, report_date, max_clusters)

    # Account-scoped reports always carry the account's brand colors (Austin,
    # Jul 6 2026). Window/marker colors stay fixed hex and are unaffected.
    try:
        brand = json.loads(get_customer_brand(organization))
        if "error" not in brand:
            brand_css = _brand_css_overrides(brand)
            if brand_css and "</head>" in html:
                html = html.replace("</head>", f"{brand_css}\n</head>", 1)
    except Exception:
        pass

    try:
        cb_a = (cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"], cfg["use_tls"], cfg["scope"])
        safe_org = organization.lower().replace(" ", "_")
        fname = f"cluster_health_{safe_org}_{_dt.date.today().isoformat()}.html"
        asset_id = _save_asset_to_cb(
            *cb_a, asset_type="html",
            title=f"{organization} Cluster Health — {report_date}",
            content=html, org=organization, filename=fname,
        )
    except Exception as exc:
        return json.dumps({"error": f"Failed to save asset: {exc}"})

    return json.dumps({
        "saved": True,
        "asset_id": asset_id,
        "filename": fname,
        "organization": organization,
        "total_clusters": health.get("total_clusters", 0),
        "charted_clusters": min(max_clusters, health.get("total_clusters", 0)),
        "report_date": report_date,
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
