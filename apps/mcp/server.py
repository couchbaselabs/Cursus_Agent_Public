"""
Supportal MCP Server — Phase 2

Exposes pipeline functions as MCP tools so Claude (or any MCP client) can call
them natively rather than relying on the Strabo button-driven workflow.

Tools:
  query_tickets       — structured N1QL search with filters
  vector_search       — semantic search via CB vector index
  get_ticket          — single-ticket fetch by ID
  check_freshness     — returns ticket IDs updated since a given epoch
  fetch_fresh_data    — triggers the scrape pipeline for a customer

Usage:
  python mcp_server.py [--host HOST] [--port PORT]
  or add to Claude Desktop's mcp_servers config as a stdio server.

Environment variables (override CLI defaults):
  CB_URL, CB_BUCKET, CB_USER, CB_PASS, CB_TLS, CB_SCOPE, CB_COLLECTION
  EMBED_PROVIDER, EMBED_MODEL, EMBED_API_KEY, EMBED_BASE_URL, EMBED_DIMS
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

# ── Path setup: import pipeline functions from the main app ─────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

# These imports are used lazily inside tool handlers to avoid startup errors
# when optional deps (couchbase, openai) are not installed.
_app: Any = None


def _load_app() -> Any:
    global _app
    if _app is None:
        import importlib
        _app = importlib.import_module("apps.strabo.app")
    return _app


# ── CB config from env (fallbacks to empty string, tools will surface errors) ─
def _cb_config() -> dict:
    return {
        "cb_url":    os.environ.get("CB_URL",        "couchbase://localhost"),
        "bucket":    os.environ.get("CB_BUCKET",     "rag"),
        "username":  os.environ.get("CB_USER",       ""),
        "password":  os.environ.get("CB_PASS",       ""),
        "use_tls":   os.environ.get("CB_TLS",        "false").lower() == "true",
        "scope":     os.environ.get("CB_SCOPE",      "transcripts"),
        "collection":os.environ.get("CB_COLLECTION", "supportal"),
    }


def _embed_config() -> dict:
    return {
        "provider": os.environ.get("EMBED_PROVIDER", "ollama"),
        "model":    os.environ.get("EMBED_MODEL",    "nomic-embed-text"),
        "api_key":  os.environ.get("EMBED_API_KEY",  ""),
        "base_url": os.environ.get("EMBED_BASE_URL", "http://localhost:11434"),
        "dims":     int(os.environ.get("EMBED_DIMS", "768")),
    }


# ── MCP server setup ────────────────────────────────────────────────────────
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import mcp.types as types
    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False
    print(
        "mcp package not found — install with: pip install mcp\n"
        "Running in dry-run mode (tool schemas printed, not served).",
        file=sys.stderr,
    )

log = logging.getLogger("supportal-mcp")

if _MCP_AVAILABLE:
    server = Server("supportal-mcp")

    # ── Tool: query_tickets ─────────────────────────────────────────────────
    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="query_tickets",
                description=(
                    "Search support tickets in Couchbase using structured filters. "
                    "Supports filtering by customer, date range, priority, status, "
                    "keywords, ticket IDs, CBSEs, and JIRA issues. Returns ticket dicts "
                    "with subject, status, priority, created_at, cluster info, and tags."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question — filters are extracted automatically.",
                        },
                        "customer": {
                            "type": "string",
                            "description": "Customer/organization name to scope results.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max tickets to return (default 50).",
                            "default": 50,
                        },
                    },
                    "required": ["question"],
                },
            ),
            types.Tool(
                name="vector_search",
                description=(
                    "Semantic similarity search over ticket embeddings in Couchbase. "
                    "Returns the top-K tickets most relevant to the query string."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Query text to embed and search against.",
                        },
                        "customer": {
                            "type": "string",
                            "description": "Optional customer name for post-filter.",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Number of results (default 20).",
                            "default": 20,
                        },
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="get_ticket",
                description=(
                    "Fetch a single ticket document by ID from Couchbase. "
                    "Returns full ticket including status, priority, description, "
                    "cluster info, snapshot_topology, cbses, jira_issues, and score."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticket_id": {
                            "type": "string",
                            "description": "Zendesk ticket ID (numeric string).",
                        },
                    },
                    "required": ["ticket_id"],
                },
            ),
            types.Tool(
                name="check_freshness",
                description=(
                    "Given a list of ticket IDs and an epoch timestamp, return the IDs "
                    "of tickets that have been re-scraped (last_scraped_at > since_epoch). "
                    "Use this to detect data drift before reasoning from a cached session."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticket_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Ticket IDs to check.",
                        },
                        "since_epoch": {
                            "type": "integer",
                            "description": "Unix epoch seconds; tickets updated after this are returned.",
                        },
                    },
                    "required": ["ticket_ids", "since_epoch"],
                },
            ),
            types.Tool(
                name="fetch_fresh_data",
                description=(
                    "Trigger the scrape pipeline to refresh ticket data for a customer. "
                    "Returns a summary of what was scraped and the new last_scraped_at epoch. "
                    "Requires session cookie to be configured via CB_COOKIE env var."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "customer": {
                            "type": "string",
                            "description": "Customer/organization name to scrape.",
                        },
                        "incremental": {
                            "type": "boolean",
                            "description": "If true, only scrape changed/new tickets (default true).",
                            "default": True,
                        },
                    },
                    "required": ["customer"],
                },
            ),
        ]

    # ── Tool call dispatcher ────────────────────────────────────────────────
    @server.call_tool()
    async def call_tool(
        name: str, arguments: dict
    ) -> list[types.TextContent]:
        try:
            result = await _dispatch(name, arguments)
            return [types.TextContent(type="text", text=json.dumps(result, default=str))]
        except Exception as exc:
            log.exception("Tool %s failed", name)
            return [types.TextContent(type="text", text=json.dumps({"error": str(exc)}))]


async def _dispatch(name: str, args: dict) -> Any:
    app = _load_app()
    cfg = _cb_config()
    cb_args = (
        cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
        cfg["use_tls"], cfg["scope"], cfg["collection"],
    )

    if name == "query_tickets":
        question = args["question"]
        customer = args.get("customer", "")
        limit    = int(args.get("limit", 50))
        filters  = app.build_structured_query(question)
        if customer:
            filters["organization"] = customer
        results = app.structured_search_cb(filters, *cb_args, limit)
        # Resolve keys to full ticket dicts
        tickets = []
        if results:
            import concurrent.futures
            from couchbase.options import GetOptions  # type: ignore
            from couchbase.cluster import Cluster  # type: ignore
            from couchbase.options import ClusterOptions  # type: ignore
            from couchbase.auth import PasswordAuthenticator  # type: ignore
            from datetime import timedelta
            conn_str = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
            cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
            cluster.wait_until_ready(timedelta(seconds=10))
            col = cluster.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"])
            for key in results[:limit]:
                try:
                    doc = col.get(key).content_as[dict]
                    # Return summary fields only to keep token count manageable
                    tickets.append({
                        "ticket_id":     doc.get("ticket_id"),
                        "subject":       doc.get("subject"),
                        "status":        doc.get("status"),
                        "priority":      doc.get("priority"),
                        "organization":  doc.get("organization"),
                        "created_at":    doc.get("created_at"),
                        "last_scraped_at": doc.get("last_scraped_at"),
                        "cbses":         doc.get("cbses") or [],
                        "jira_issues":   doc.get("jira_issues") or [],
                        "tags":          (doc.get("score") or {}).get("cluster_names") or [],
                    })
                except Exception:
                    pass
            cluster.close()
        return {"tickets": tickets, "count": len(tickets), "filters_applied": filters}

    elif name == "vector_search":
        ec = _embed_config()
        query_vec = app.embed_text(
            args["query"], ec["provider"], ec["model"],
            ec["api_key"], ec["base_url"], ec["dims"],
        )
        raw_keys = app.vector_search_cb(query_vec, *cb_args, int(args.get("top_k", 20)))
        customer = args.get("customer", "").lower()
        # Resolve to summary dicts (same pattern as query_tickets)
        from couchbase.cluster import Cluster  # type: ignore
        from couchbase.options import ClusterOptions  # type: ignore
        from couchbase.auth import PasswordAuthenticator  # type: ignore
        from datetime import timedelta
        conn_str = app._cb_conn_str(cfg["cb_url"], cfg["use_tls"])
        cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        cluster.wait_until_ready(timedelta(seconds=10))
        col = cluster.bucket(cfg["bucket"]).scope(cfg["scope"]).collection(cfg["collection"])
        tickets = []
        for key in raw_keys:
            try:
                doc = col.get(key).content_as[dict]
                if customer and (doc.get("organization") or "").lower() not in customer:
                    continue
                tickets.append({
                    "ticket_id":   doc.get("ticket_id"),
                    "subject":     doc.get("subject"),
                    "status":      doc.get("status"),
                    "priority":    doc.get("priority"),
                    "organization":doc.get("organization"),
                    "created_at":  doc.get("created_at"),
                    "last_scraped_at": doc.get("last_scraped_at"),
                })
            except Exception:
                pass
        cluster.close()
        return {"tickets": tickets, "count": len(tickets)}

    elif name == "get_ticket":
        ticket_id = str(args["ticket_id"])
        doc = app.chat_cache_get(f"ticket::{ticket_id}", *cb_args)
        if doc is None:
            # Try without the prefix (some docs stored as just the numeric ID)
            doc = app.chat_cache_get(ticket_id, *cb_args)
        if doc is None:
            return {"error": f"Ticket {ticket_id} not found"}
        # Return full doc minus the raw embedding vector (too large)
        doc.pop("embedding", None)
        return doc

    elif name == "check_freshness":
        stale = app.check_freshness(
            args["ticket_ids"],
            int(args["since_epoch"]),
            *cb_args,
        )
        return {
            "stale_ticket_ids": stale,
            "stale_count":      len(stale),
            "checked_count":    len(args["ticket_ids"]),
        }

    elif name == "fetch_fresh_data":
        customer    = args["customer"]
        incremental = bool(args.get("incremental", True))
        cookie      = os.environ.get("CB_COOKIE", "")
        if not cookie:
            return {"error": "CB_COOKIE env var not set — cannot scrape without session cookie"}

        cb_cfg = app.CbConfig(
            url=cfg["cb_url"], bucket=cfg["bucket"],
            username=cfg["username"], password=cfg["password"],
            use_tls=cfg["use_tls"], scope=cfg["scope"],
            collection=cfg["collection"],
        )
        scraped: list[dict] = []
        errors:  list[str]  = []

        def _progress(msg: str) -> None:
            log.info("[fetch_fresh_data] %s", msg)

        try:
            results = app.run_ticket_pipeline(
                organization=customer,
                cookie=cookie,
                cb_cfg=cb_cfg,
                incremental=incremental,
                progress_cb=_progress,
            )
            scraped = results if isinstance(results, list) else []
        except Exception as exc:
            errors.append(str(exc))

        return {
            "customer":        customer,
            "scraped_count":   len(scraped),
            "last_scraped_at": int(time.time()),
            "incremental":     incremental,
            "errors":          errors,
        }

    raise ValueError(f"Unknown tool: {name}")


# ── Entry point ─────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Supportal MCP Server")
    parser.add_argument("--log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not _MCP_AVAILABLE:
        print("Install the mcp package first: pip install mcp", file=sys.stderr)
        sys.exit(1)

    import asyncio

    async def _run() -> None:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )

    asyncio.run(_run())


if __name__ == "__main__":
    main()
