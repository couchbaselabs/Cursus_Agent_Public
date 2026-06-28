"""
supportal/agent_tools.py — Agent tool definitions, LLM tool-calling loop,
fleet/health analytics helpers, and asset management utilities.

All functions are pure (config passed as parameters); UI-coupled functions
(_execute_agent_tool, _run_scrape_job_bg, etc.) remain in the main app.
"""
import re
import json
import time
import hashlib
import threading
import datetime
import uuid
import html
import importlib
from collections import Counter
from datetime import timedelta
from supportal.scoring import call_llm
from supportal.cb_helpers import _cb_conn_str
from supportal.llm_providers import lmstudio_ensure_model_loaded

try:
    from couchbase.cluster import Cluster          # type: ignore
    from couchbase.options import ClusterOptions   # type: ignore
    from couchbase.auth import PasswordAuthenticator  # type: ignore
except ImportError:
    Cluster = ClusterOptions = PasswordAuthenticator = None  # type: ignore

def _get_main_app():
    """Lazy import of the Strabo app to avoid circular imports at load time."""
    return importlib.import_module("apps.strabo.app")

# ──────────────────────────── Phase 2b: Agent Tool Calling ───────────────────

_AGENT_TOOLS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "query_tickets",
            "description": (
                "Query support tickets from Couchbase using structured filters. "
                "Returns a markdown table of matching tickets with key fields. "
                "Use this to find, list, or analyze tickets matching specific criteria."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name (partial match, case-insensitive).",
                    },
                    "cbse_only": {
                        "type": "boolean",
                        "description": "If true, only return tickets that have formal CBSE bug links.",
                    },
                    "jira_only": {
                        "type": "boolean",
                        "description": "If true, only return tickets that have formal Jira issue links.",
                    },
                    "cbse_id": {
                        "type": "string",
                        "description": "Specific CBSE ID to search for (e.g. 'MB-12345'). Partial match.",
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["P1", "P2", "P3", "P4", "URGENT", "HIGH", "NORMAL", "LOW"],
                        "description": "Filter by ticket priority.",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["open", "pending", "solved", "closed", "hold"],
                        "description": "Filter by ticket status.",
                    },
                    "date_from": {
                        "type": "string",
                        "description": "ISO date lower bound for ticket creation (e.g. '2024-01-01').",
                    },
                    "date_to": {
                        "type": "string",
                        "description": "ISO date upper bound for ticket creation (e.g. '2024-12-31').",
                    },
                    "keyword": {
                        "type": "string",
                        "description": "Text keyword to search in subject, description, and cluster names.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of tickets to return (default 50, max 200).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "count_tickets",
            "description": (
                "Count support tickets matching the given filters. "
                "Returns just the count. Prefer this over query_tickets when you "
                "only need a total, not individual ticket details."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {"type": "string", "description": "Customer name (partial match)."},
                    "cbse_only": {"type": "boolean", "description": "Only tickets with formal CBSE links."},
                    "jira_only": {"type": "boolean", "description": "Only tickets with formal Jira links."},
                    "priority": {
                        "type": "string",
                        "enum": ["P1", "P2", "P3", "P4", "URGENT", "HIGH", "NORMAL", "LOW"],
                        "description": "Filter by priority.",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["open", "pending", "solved", "closed", "hold"],
                        "description": "Filter by status.",
                    },
                    "date_from": {"type": "string", "description": "ISO date lower bound."},
                    "date_to": {"type": "string", "description": "ISO date upper bound."},
                    "keyword": {"type": "string", "description": "Text keyword to match in subject/description."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_ticket",
            "description": (
                "Fetch full details for a single support ticket by its numeric ticket ID. "
                "Returns description, comments, CBSEs, Jira issues, AI summary, data freshness, "
                "and cluster topology from the linked snapshot — including node count, CB version, "
                "service layout, bucket names, RAM per node, auto-failover setting, and health "
                "(bad/warn item counts). Use this when the user asks about cluster configuration, "
                "node count, topology, or any infrastructure details for a specific ticket. "
                "If the ticket is not in local Couchbase, it is automatically fetched live from "
                "Supportal and saved — do NOT give up or say the ticket doesn't exist before calling this."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_id": {
                        "type": "string",
                        "description": "The numeric ticket ID (e.g. '123456').",
                    }
                },
                "required": ["ticket_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_data_freshness",
            "description": (
                "Check how recently ticket data was scraped from Supportal. "
                "Use this whenever the user asks about 'current', 'live', 'latest', "
                "or 'today's' status. Returns last_scraped_at age in hours and the "
                "Supportal Analytics URL for manual live verification."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Ticket IDs to check freshness for (from a prior query_tickets call).",
                    },
                },
                "required": ["ticket_ids"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rescrape_customer_tickets",
            "description": (
                "Bulk re-scrape tickets for a customer from Supportal and update Couchbase. "
                "Automatically discovers NEW tickets from Supportal that are not yet in the local database — "
                "these are always scraped regardless of stale_hours. Existing stale tickets are also refreshed. "
                "After scraping, ALL refreshed tickets are re-embedded and re-scored automatically. "
                "Use when the user says 'refresh', 'rescrape', 'full rescrape', or 'rescore' for a customer. "
                "When the user says 'all tickets', 'everything', or 'full rescrape', set max_tickets=2000 and stale_hours=0. "
                "By default only re-scrapes tickets older than 4 hours (stale_hours=4). "
                "The job summary reports 'N new + M stale tickets updated, K scored'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer": {
                        "type": "string",
                        "description": "Customer/organization name to rescrape. Defaults to the currently scoped customer.",
                    },
                    "stale_hours": {
                        "type": "number",
                        "description": "Only rescrape tickets not updated within this many hours (default 4). Set to 0 to force-rescrape all.",
                    },
                    "max_tickets": {
                        "type": "integer",
                        "description": "Max tickets to rescrape in one call (default 50, max 2000). Set to 2000 when the user says 'all tickets', 'full rescrape', or 'everything'.",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["open", "pending", "solved", "closed", "hold"],
                        "description": "Only rescrape tickets with this status. Leave blank for all statuses.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rescrape_ticket",
            "description": (
                "Re-fetch a SINGLE ticket directly from Supportal and update Couchbase "
                "with the latest status, priority, comments, and metadata. Call once per "
                "ticket — pass ONE ticket_id string per call. "
                "To refresh all stale tickets for a customer at once use rescrape_customer_tickets instead."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_id": {
                        "type": "string",
                        "description": "A single numeric ticket ID, e.g. \"12345\". One call per ticket.",
                    },
                },
                "required": ["ticket_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_chart",
            "description": (
                "Renders a real interactive chart in the chat UI. "
                "MUST be called whenever the user asks for a chart, graph, or visualization — never substitute with text. "
                "TYPE GUIDE: bar=counts by category; horizontal_bar=ranked lists (long labels); "
                "line=trend over time; area=cumulative/filled trend; stacked_bar=part-of-whole breakdown; "
                "scatter=correlation (x vs y); combo=bar+line overlay; pie=proportions (<6 slices); "
                "donut=proportions with center space; gauge=single KPI value (0–100 or custom scale); "
                "treemap=hierarchical proportions; funnel=stage/pipeline flow. "
                "For time-series data prefer area or line. For a single number prefer gauge. "
                "For multi-series data pass series instead of labels+values."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_type": {
                        "type": "string",
                        "enum": ["bar", "horizontal_bar", "line", "area", "stacked_bar",
                                 "scatter", "combo", "pie", "donut", "gauge", "treemap", "funnel"],
                        "description": "Chart type — see tool description for guidance.",
                    },
                    "title": {"type": "string", "description": "Chart title."},
                    "description": {"type": "string", "description": "Optional caption rendered below the chart (1-2 sentences of insight)."},
                    "labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Category labels (x-axis for bar/line, slice names for pie/donut/funnel).",
                    },
                    "values": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Numeric values — one per label. Use for single-series charts.",
                    },
                    "series": {
                        "type": "array",
                        "description": "Multi-series data. Each item: {name, data: [numbers], chart_type?: 'bar'|'line', right_axis?: bool}.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "data": {"type": "array", "items": {"type": "number"}},
                                "chart_type": {"type": "string", "description": "Per-series type for combo charts (bar or line)."},
                                "right_axis": {"type": "boolean", "description": "Plot on secondary y-axis (combo only)."},
                            },
                        },
                    },
                    "data_points": {
                        "type": "array",
                        "description": "Scatter chart data. Each item: {x: number, y: number, name?: string}.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "x": {"type": "number"}, "y": {"type": "number"}, "name": {"type": "string"},
                            },
                        },
                    },
                    "x_label": {"type": "string", "description": "X-axis label."},
                    "y_label": {"type": "string", "description": "Y-axis label."},
                    "value":     {"type": "number", "description": "Gauge: the single value to display."},
                    "min_value": {"type": "number", "description": "Gauge: scale minimum (default 0)."},
                    "max_value": {"type": "number", "description": "Gauge: scale maximum (default 100)."},
                    "height":    {"type": "integer", "description": "Chart height in px (default 320, range 200-700)."},
                    "stacked":   {"type": "boolean", "description": "Stack bar series (shorthand for stacked_bar)."},
                    "show_labels": {"type": "boolean", "description": "Show data value labels on bars/slices."},
                    "color_scheme": {
                        "type": "string",
                        "enum": ["default", "warm", "cool", "traffic", "couchbase", "monochrome"],
                        "description": "Color palette. traffic=green/yellow/red for severity; couchbase=brand colors.",
                    },
                },
                "required": ["chart_type", "title"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_supportal_customers",
            "description": (
                "[LIVE / GLOBAL — hits Supportal Analytics API, not local Couchbase] "
                "Returns every customer Supportal is aware of globally, with snapshot and "
                "linked ticket counts. Use for questions like: 'how many customers get support?', "
                "'what customers are in Supportal?', 'show me all customers globally', "
                "'how many orgs does Couchbase support?'. "
                "Do NOT use for questions about locally scraped data — use list_organizations for that. "
                "Requires a valid session cookie in the saved profile."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sort_by": {
                        "type": "string",
                        "enum": ["name", "snapshots", "tickets"],
                        "description": "Sort order: alphabetical by name, by snapshot count, or by linked ticket count. Default: name.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max customers to return (default 200).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_supportal",
            "description": (
                "[LIVE / GLOBAL — hits Supportal Analytics API, requires a valid session cookie] "
                "Run a SQL++ query against the live Supportal Analytics API. "
                "Use for global/live questions that need live data: customer lists from Supportal, "
                "ticket-to-cluster mappings, counts of clusters/snapshots as seen by Supportal today. "
                "Do NOT use for locally scraped ticket data — use query_tickets/count_tickets for that. "
                "Do NOT use for cluster hardware topology, memory, CPU, or recent snapshot data — "
                "use query_local_snapshots or get_cluster_health (both query local CB without a cookie).\n\n"
                "SCHEMA (scope: v1):\n"
                "  customer   — name (string). Key: Customer::{id}\n"
                "  cluster    — ui_name (string), customer (string, customer id). Key: Cluster::{uuid}\n"
                "  snapshot   — timestamp (ISO string), uuid (cluster uuid), zendesk (array of int ticket IDs). Key: Snapshot::{id}\n\n"
                "JOIN PATTERNS:\n"
                "  cluster→customer:  JOIN customer cu ON META(cu).id = (\"Customer::\" || cl.customer)\n"
                "  snapshot→cluster:  JOIN cluster cl ON META(cl).id = (\"Cluster::\" || sn.uuid)\n"
                "  snapshot ticket IDs: UNNEST sn.zendesk AS t_id\n\n"
                "EXAMPLE QUERIES:\n"
                "  All customers: SELECT name FROM customer ORDER BY name\n"
                "  Snapshots per customer: SELECT cu.name, COUNT(*) AS snaps FROM snapshot sn "
                "JOIN cluster cl ON META(cl).id=(\"Cluster::\"|sn.uuid) "
                "JOIN customer cu ON META(cu).id=(\"Customer::\"|cl.customer) GROUP BY cu.name ORDER BY snaps DESC\n"
                "  Clusters for customer: SELECT cl.ui_name FROM cluster cl "
                "JOIN customer cu ON META(cu).id=(\"Customer::\"|cl.customer) WHERE cu.name=\"Acme Corp\"\n"
                "  Recent snapshots: SELECT sn.timestamp, cl.ui_name FROM snapshot sn "
                "JOIN cluster cl ON META(cl).id=(\"Cluster::\"|sn.uuid) ORDER BY sn.timestamp DESC LIMIT 20\n"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "statement": {
                        "type": "string",
                        "description": "The SQL++ query to execute against Supportal Analytics.",
                    },
                    "limit_rows": {
                        "type": "integer",
                        "description": "Truncate result to this many rows before returning (default 100). Add LIMIT in your SQL for best performance.",
                    },
                },
                "required": ["statement"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_organizations",
            "description": (
                "[LOCAL — queries your configured Couchbase instance, not Supportal] "
                "Returns every customer/organization that has tickets stored in the local "
                "Couchbase database, with ticket counts. Use for questions like: "
                "'what customers are you aware of?', 'what orgs do you have data for?', "
                "'which customers have I scraped?', 'who has the most tickets locally?'. "
                "Always exempt from customer scoping — always returns all orgs. "
                "Do NOT use for global Supportal data — use list_supportal_customers for that."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "min_tickets": {
                        "type": "integer",
                        "description": "Only include organizations with at least this many tickets (default 1).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_customer_names",
            "description": (
                "Search for Supportal customer names matching a partial or fuzzy query. "
                "Use this when NO customer is currently configured and the user mentions a "
                "company name — call this first to find the exact match, then confirm with "
                "the user before proceeding. Also useful for 'is X a customer?', 'find "
                "customers named like Y', or resolving ambiguous company names. "
                "Searches Analytics (live), local Couchbase, and Supportal search in order."
            ),
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_briefing",
            "description": (
                "Return a proactive briefing across the user's top accounts — health scores, "
                "open P1/P2 counts, and data staleness. Call this for 'what should I know today?', "
                "'morning briefing', 'what's going on across my accounts?', 'any urgent issues?', "
                "or any open-ended status request with no specific customer. "
                "Uses the user's access profile to know which accounts to check — no customer name needed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "top_n": {
                        "type": "integer",
                        "description": "How many top customers to include (default 5, max 10).",
                    },
                },
                "required": [],
            },
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Partial or full customer name to search for.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default 10).",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_table",
            "description": (
                "Renders a real data table in the chat UI with CSV and Excel download buttons. "
                "MUST be called when the user asks for a table, spreadsheet, or list of tickets "
                "to export — never substitute with a markdown table. "
                "Call this before your final text so the table appears above the explanation."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Table title (also used as filename)."},
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column header names.",
                    },
                    "rows": {
                        "type": "array",
                        "description": "Data rows — each row is an array of cell values.",
                        "items": {"type": "array", "items": {}},
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional short description shown above the table.",
                    },
                },
                "required": ["title", "columns", "rows"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "vector_search",
            "description": (
                "Semantic / similarity search over tickets using vector embeddings. "
                "Use this when the user wants to find tickets related to a concept, error message, "
                "or symptom — even if exact keywords don't match. "
                "Complements query_tickets (keyword/filter) with meaning-based retrieval."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language description of what to find, e.g. 'memory eviction issues on data nodes'.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default 10, max 30).",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_cluster_health",
            "description": (
                "Return a cluster health summary for a customer using stored snapshot topology data. "
                "Shows active clusters, CB versions, node counts, CPUs/node, RAM/node, bad/warn item counts, "
                "and deprecation status. Use this when the user asks about cluster state, infrastructure health, "
                "version distribution, or hardware specs (CPU cores, RAM per node). "
                "Preferred over get_ticket when the question is about hardware or cross-snapshot cluster config. "
                "NOTE: bad_count and warn_count are counts of diagnostic health CHECK MESSAGES from Couchbase "
                "Support diagnostics — they are NOT node counts. A bad_count of 5 means 5 diagnostic items "
                "are flagged as bad, not 5 nodes. Node count is a separate 'Nodes' field. "
                "For a visual chart of hardware stats for clusters with open tickets, prefer cluster_hw_chart."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name.",
                    },
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cluster_hw_chart",
            "description": (
                "Generate a hardware comparison chart (EChart) for clusters linked to open/pending tickets. "
                "Shows node count, CPUs/node, and RAM/node (GiB) as grouped bars per cluster. "
                "Clusters that have CBSE-linked tickets are marked with a ● dot in their label. "
                "Returns a rendered chart — use this whenever the user asks to: "
                "'show hardware stats for clusters with open tickets', 'compare nodes/CPU/RAM across clusters', "
                "'graph hardware for tickets', 'which clusters have CBSEs on the chart', "
                "'show me a chart of cluster topology for tickets'. "
                "IMPORTANT: bad_count and warn_count are health MESSAGE counts (diagnostic items), NOT node counts. "
                "This tool shows the real hardware specs: physical node count, CPU cores per node, RAM per node."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name.",
                    },
                    "status_filter": {
                        "type": "string",
                        "enum": ["open", "pending", "open_or_pending", "all"],
                        "description": "Filter clusters by linked ticket status (default: open_or_pending).",
                    },
                    "height": {
                        "type": "integer",
                        "description": "Chart height in pixels (default: auto based on cluster count).",
                    },
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_local_snapshots",
            "description": (
                "[LOCAL — queries your Couchbase snapshots collection directly, no API cookie needed] "
                "Query the local snapshot topology database for cluster hardware specs and health. "
                "Returns node count, CB version, CPUs/node, RAM/node, disk, bad/warn item counts per cluster. "
                "Use for questions like: 'show me recent clusters', 'which clusters have snapshots in the last 30 days', "
                "'what are the hardware specs of clusters', 'topology of recent clusters', "
                "'memory/CPU/disk for clusters'. "
                "Supports optional organization filter and days filter. "
                "Prefer this over query_supportal for topology/hardware/resource questions — no cookie needed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Optional customer/organization filter (fuzzy match). Omit to search all orgs.",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Only return snapshots scraped within this many days (default 30).",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return (default 50).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_snapshot",
            "description": (
                "Fetch a snapshot's full topology live from Supportal and return a structured health summary. "
                "Use when the user provides a snap_id (e.g. from a ticket or cluster name) and wants real-time analysis. "
                "Optionally save analysis notes back to the snapshot record in Couchbase. "
                "Returns: cluster name, CB version, node/CPU/RAM, bad items, warn items, bucket list."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "snap_id": {
                        "type": "string",
                        "description": "The snapshot ID (e.g. 'abc123::0'). Found in ticket snap_ids or snapshot listings.",
                    },
                    "analysis_notes": {
                        "type": "string",
                        "description": "Optional free-text analysis or findings to attach to the snapshot record.",
                    },
                    "save_notes": {
                        "type": "boolean",
                        "description": "If true, saves the topology and analysis_notes back to the snapshot doc in Couchbase (default false).",
                    },
                },
                "required": ["snap_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_snapshots",
            "description": (
                "Fetch snapshot listing for a customer from the Supportal Analytics API and save stubs to Couchbase. "
                "This is a fast single-query operation that returns snapshot IDs and associated ticket IDs. "
                "Topology detail (CB version, node layout) is NOT fetched — call backfill_snapshot_topology after this. "
                "Use when the user says 'get snapshots', 'refresh snapshot list', or before checking cluster health."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max snapshots to fetch (default 100).",
                    },
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "backfill_snapshot_topology",
            "description": (
                "Fetch full cluster topology (CB version, nodes, services, bad/warn items) for snapshot stubs "
                "stored in Couchbase that are missing topology detail. "
                "Call this after fetch_snapshots to enrich the stubs. "
                "Processes up to max_stubs snapshots per call to keep response time reasonable."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name.",
                    },
                    "max_stubs": {
                        "type": "integer",
                        "description": "Max stubs to enrich in this call (default 10, max 25).",
                    },
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "backfill_last_comment_at",
            "description": (
                "Backfill the last_comment_at field on existing ticket documents in Couchbase by deriving it "
                "from the stored comments array. Run this once after upgrading to populate the 'Last Reply' "
                "column in query_tickets output without needing to re-scrape. "
                "Skips tickets that already have the field set."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Limit backfill to a specific org (optional — omit to backfill all tickets).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_scrape_status",
            "description": (
                "Check the status of background scrape/rescrape jobs. "
                "Call this when the user asks about scrape progress, how many tickets have been processed, "
                "how many are left, or whether a scrape is still running. "
                "Returns a summary of all recent jobs with progress counts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Specific job ID to check (optional — omit to see all recent jobs).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cancel_scrape_job",
            "description": (
                "Cancel a running scrape or rescrape job. "
                "Call this when the user says 'kill', 'stop', 'cancel', or 'abort' a job, "
                "or when a job appears stuck or needs to be terminated. "
                "After cancellation, tickets already refreshed retain their new data. "
                "To resume from the stopping point, rescrape with stale_hours=1 — "
                "already-refreshed tickets have fresh timestamps and will be skipped."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The 6-character job ID to cancel (e.g. 'e02827').",
                    },
                },
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scrape_customer_tickets",
            "description": (
                "Scrape fresh tickets for a customer directly from Supportal and save them to Couchbase. "
                "Use when the user wants to pull new tickets that aren't in the local DB yet, or do a fresh scrape. "
                "Capped at max_tickets per call to stay fast. For refreshing existing stale tickets use rescrape_customer_tickets instead."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name as it appears in Supportal.",
                    },
                    "max_tickets": {
                        "type": "integer",
                        "description": "Max tickets to scrape (default 25, max 50).",
                    },
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "score_ticket",
            "description": (
                "Run LLM scoring on a single ticket to generate stars (1-5), temperature (cold/warm/hot), "
                "complexity, resolution_quality, and communication_clarity scores. "
                "Use when the user asks about ticket quality, complexity, or when a ticket has no scores yet. "
                "To score multiple tickets at once use batch_score_tickets instead."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_id": {
                        "type": "string",
                        "description": "The numeric ticket ID to score.",
                    },
                },
                "required": ["ticket_id"],
            },
        },
    },
    # ── BEFORE v1.5.0: fetch_snapshots + backfill_snapshot_topology were two separate ──
    # ── calls that the agent often only half-completed. sync_snapshots wraps both.    ──
    {
        "type": "function",
        "function": {
            "name": "sync_snapshots",
            "description": (
                "Fetch AND enrich snapshot data for a customer in one step — "
                "runs fetch_snapshots (Analytics API stub list) then immediately runs "
                "backfill_snapshot_topology (REST topology per stub). "
                "PREFER this over calling fetch_snapshots + backfill_snapshot_topology separately. "
                "Use when the user says 'get snapshots', 'refresh snapshot data', 'sync cluster info', "
                "'show cluster health' when no local snapshots exist, or any time you need fresh topology."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {
                        "type": "string",
                        "description": "Customer/organization name.",
                    },
                    "max_stubs": {
                        "type": "integer",
                        "description": "Max stubs to enrich with topology (default 10, max 25).",
                    },
                },
                "required": ["organization"],
            },
        },
    },
    # ── BEFORE v1.5.0: score_ticket and rescrape_ticket were single-ID tools that ────
    # ── burned the 5-turn limit fast on bulk requests. These batch versions fix that. ─
    {
        "type": "function",
        "function": {
            "name": "batch_score_tickets",
            "description": (
                "Score multiple tickets for quality/complexity in one call. "
                "Use when the user asks to score, RE-score, or refresh scores for tickets. "
                "Returns a score summary table. "
                "Far more efficient than calling score_ticket once per ticket. "
                "Limit is 50 per call. "
                "IMPORTANT: when the user asks to RE-score or rescore (i.e. score again), "
                "set unscored_only=false so already-scored tickets are included."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit list of numeric ticket IDs to score. Takes priority over organization mode.",
                    },
                    "organization": {
                        "type": "string",
                        "description": "Score tickets for this customer (used when ticket_ids is empty).",
                    },
                    "unscored_only": {
                        "type": "boolean",
                        "description": "Only score tickets without existing scores (default true). Set false when the user explicitly asks to RE-score or refresh existing scores.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max tickets to score in this call (default 10, max 50).",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["open", "pending", "solved", "closed", "hold"],
                        "description": "Filter by status when using organization mode.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "batch_rescrape_tickets",
            "description": (
                "Re-fetch multiple specific tickets from Supportal in one call. "
                "Use when the user provides a list of ticket IDs to refresh. "
                "For refreshing all stale tickets for a customer use rescrape_customer_tickets instead. "
                "Returns a per-ticket result summary. Limit is 20 per call."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of numeric ticket IDs to re-scrape from Supportal.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Safety cap on how many to process (default 10, max 20).",
                    },
                },
                "required": ["ticket_ids"],
            },
        },
    },
    # ── v1.6.0: Feature set tools ─────────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "get_customer_health_score",
            "description": (
                "Compute a 0-100 composite health score for a customer. "
                "Considers: open P1/P2 count, escalation rate, avg resolution time, "
                "cluster bad/warn ratio, and data freshness. "
                "Returns score, grade (Healthy/At Risk/Critical), and dimension breakdown. "
                "Call proactively when the user asks about a customer's status or health."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {"type": "string", "description": "Customer name."},
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_sla_compliance",
            "description": (
                "Calculate SLA compliance for a customer — what % of tickets were resolved "
                "within the SLA threshold for each priority "
                "(P1/critical=4h, P2/high=24h, P3/normal=72h, P4/low=168h). "
                "Returns compliance % per priority, average resolution hours, and total analyzed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {"type": "string"},
                    "date_from": {"type": "string", "description": "Filter start date YYYY-MM-DD."},
                    "date_to":   {"type": "string", "description": "Filter end date YYYY-MM-DD."},
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_portfolio_status",
            "description": (
                "Return a ranked status overview of ALL customers — health scores, open P1 counts, "
                "data freshness, and cluster bad-item ratio. Use for 'status of all my accounts', "
                "'which customers need attention', 'who has open P1s', 'show me the fleet'. "
                "Returns a list sorted by urgency (critical first)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "limit":          {"type": "integer", "description": "Max customers to return (default 20)."},
                    "include_cluster":{"type": "boolean", "description": "Include cluster bad-item breakdown per org (default false — slower)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_fleet_tickets",
            "description": (
                "Aggregate ticket counts across ALL customers grouped by a chosen dimension. "
                "Use for fleet-wide questions: 'how many open tickets by priority?', "
                "'which org has the most tickets?', 'ticket breakdown by CB version', "
                "'which CBSEs appear most?'. Never requires an org filter."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "group_by":      {"type": "string", "enum": ["organization", "priority", "status", "cb_version", "cbse"], "description": "Dimension to group by."},
                    "status_filter": {"type": "string", "enum": ["open", "solved", "all"],   "description": "Ticket status scope (default: open)."},
                    "limit":         {"type": "integer", "description": "Max rows to return (default 30)."},
                },
                "required": ["group_by"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_at_risk_clusters",
            "description": (
                "Return clusters with elevated bad or warn item counts that have NO linked open ticket — "
                "the leading indicator for tickets that haven't been opened yet. "
                "Risk score = bad_items × 3 + warn_items. "
                "Use for: 'which clusters are likely to generate a ticket?', 'leading indicators', "
                "'at-risk clusters', 'proactive alerts'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "bad_threshold":  {"type": "integer", "description": "Minimum bad_items to include (default 0 = any)."},
                    "warn_threshold": {"type": "integer", "description": "Minimum warn_items to include (default 3)."},
                    "limit":          {"type": "integer", "description": "Max clusters to return (default 25)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fleet_version_distribution",
            "description": (
                "Return the distribution of Couchbase versions across all clusters in the fleet. "
                "Use for: 'what CB versions are in the wild?', 'how many clusters on 7.6?', "
                "'version spread', 'upgrade coverage'."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fleet_cbse_impact",
            "description": (
                "Rank known CBSEs (Couchbase Support Escalations) by the number of unique customer "
                "orgs affected — blast radius. "
                "Use for: 'which CBSE is hitting the most customers?', 'most impactful bugs', "
                "'CBSE blast radius', 'cross-customer bug analysis'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max CBSEs to return (default 20)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tag_ticket",
            "description": (
                "Add user-defined tags to a ticket stored in Couchbase. "
                "Tags are stored in ticket.tags as a list of strings. "
                "Use to categorize tickets (e.g. 'performance', 'upgrade', 'security', 'escalated'). "
                "Set replace=true to overwrite all existing tags instead of merging."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ticket_id": {"type": "string", "description": "Numeric ticket ID."},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags to apply.",
                    },
                    "replace": {"type": "boolean", "description": "If true, replace existing tags. Default false (merge)."},
                },
                "required": ["ticket_id", "tags"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_digest",
            "description": (
                "Return a 'what's new' digest for a customer — new tickets opened, "
                "recently resolved tickets, and stale open tickets not refreshed in the window. "
                "Use when the user asks 'what's new', 'what changed', 'any updates', "
                "'what happened since last time'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {"type": "string", "description": "Customer name. Leave blank for all customers."},
                    "since_hours": {"type": "integer", "description": "Look-back window in hours (default 24)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_query",
            "description": (
                "Bookmark a natural-language query for reuse. "
                "Saves the question to Couchbase so it can be listed and re-run later. "
                "Call when the user says 'save this query', 'bookmark this', 'remember this question'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name":         {"type": "string", "description": "Short memorable name for this query."},
                    "question":     {"type": "string", "description": "The question to save."},
                    "organization": {"type": "string", "description": "Customer scope for this query (optional)."},
                },
                "required": ["name", "question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_saved_queries",
            "description": "List all saved/bookmarked queries stored in Couchbase.",
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {"type": "string", "description": "Filter by customer (optional)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_customer_report",
            "description": (
                "Generate a structured markdown customer report from Couchbase data — no LLM required. "
                "Includes: health score, SLA compliance, open ticket table, new/resolved digest (72h). "
                "Call when the user asks for a 'report', 'summary', 'briefing', or 'status report' for a customer."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "organization": {"type": "string", "description": "Customer name."},
                },
                "required": ["organization"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_artifact",
            "description": (
                "Save any content as a named, retrievable asset persisted to Couchbase. "
                "Assets appear in the Assets tab and can be downloaded, printed, or previewed later. "
                "Use for reports, CSV exports, JSON data, JavaScript snippets, or HTML documents. "
                "Charts are auto-saved from echart blocks; call this for text-based artifacts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title":      {"type": "string",  "description": "Human-readable title for the asset."},
                    "asset_type": {
                        "type": "string",
                        "enum": ["report", "csv", "json", "js", "html"],
                        "description": "Content type — determines icon, preview renderer, and download extension.",
                    },
                    "content":  {"type": "string", "description": "Full text content to save."},
                    "filename": {"type": "string", "description": "Optional filename with extension (e.g. 'amex_report.md')."},
                },
                "required": ["title", "asset_type", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": (
                "Returns the current date and time. Call this whenever you need to know "
                "today's date, the current time, day of week, week number, or quarter — "
                "for example when computing 'last 7 days', 'this month', 'this quarter', "
                "or any other relative time range."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "timezone": {
                        "type": "string",
                        "description": "IANA timezone name (e.g. 'America/New_York'). Defaults to server local time.",
                    }
                },
                "required": [],
            },
        },
    },
]



_SUPPORTAL_TICKET_URL = "https://supportal.couchbase.com/zendesk/ticket/{ticket_id}"
_SUPPORTAL_CUSTOMER_URL = "https://supportal.couchbase.com/customer/{customer}"

# ── Chat artifact rendering ──────────────────────────────────────────────────
# Fenced blocks ```echart ... ``` and ```table ... ``` are embedded in agent
# responses and rendered as live ECharts elements or HTML tables with download
# buttons by _render_chat.
_ARTIFACT_RE = re.compile(r"```(echart|table)\n(.*?)\n```", re.DOTALL)

# Asset types auto-detected in responses for background-save to the Assets tab
_CODE_ASSET_RE = re.compile(r"```(csv|json|javascript|js|html)\n(.*?)\n```", re.DOTALL)

_ASSET_MIME: dict[str, str] = {
    "chart":      "application/json",
    "report":     "text/markdown",
    "table":      "text/csv",
    "csv":        "text/csv",
    "json":       "application/json",
    "js":         "text/javascript",
    "javascript": "text/javascript",
    "html":       "text/html",
}

_ASSET_ICONS: dict[str, str] = {
    "chart":      "bar_chart",
    "report":     "article",
    "table":      "table_chart",
    "csv":        "grid_on",
    "json":       "data_object",
    "js":         "javascript",
    "javascript": "javascript",
    "html":       "html",
}


_DARK_TEXT   = "#d1d5db"   # gray-300 — readable on dark backgrounds
_DARK_TITLE  = "#f3f4f6"   # gray-100
_DARK_SUBTEXT= "#9ca3af"   # gray-400
_DARK_AXIS   = "#4b5563"   # gray-600
_DARK_GRID   = "#2d3748"   # very subtle grid lines
_DARK_TT_BG  = "rgba(15,23,42,0.96)"  # slate-950


def _apply_echart_theme(option: dict) -> dict:
    """
    Post-process an ECharts option dict for readability on a dark UI background.
    Patches text/axis/tooltip colors and fixes legend-title overlap.
    Mutates and returns the dict.
    """
    # Global text fallback
    option.setdefault("textStyle", {})["color"] = _DARK_TEXT

    # Title
    t = option.get("title")
    if isinstance(t, dict):
        t.setdefault("textStyle", {})["color"] = _DARK_TITLE
        t.setdefault("subtextStyle", {})["color"] = _DARK_SUBTEXT
        # Push title up a bit; grid.top will keep it from overlapping the plot area
        t.setdefault("top", 6)

    # Legend — move below title so they don't collide
    lg = option.get("legend")
    if isinstance(lg, dict):
        lg.setdefault("textStyle", {})["color"] = _DARK_TEXT
        # Only override top if it's still default (0 / missing)
        lg.setdefault("top", 32)
        lg.setdefault("padding", [4, 12])

    # Tooltip
    tt = option.get("tooltip")
    if isinstance(tt, dict):
        tt["backgroundColor"] = _DARK_TT_BG
        tt.setdefault("textStyle", {})["color"] = _DARK_TITLE
        tt["borderColor"] = "#374151"

    # Axis helper
    def _patch_axis(ax: dict) -> None:
        ax.setdefault("axisLabel", {}).update({"color": _DARK_TEXT, "fontSize": 11})
        ax.setdefault("axisLine", {}).setdefault("lineStyle", {})["color"] = _DARK_AXIS
        ax.setdefault("axisTick", {}).setdefault("lineStyle", {})["color"] = _DARK_AXIS
        ax.setdefault("splitLine", {}).setdefault("lineStyle", {}).update({"color": _DARK_GRID, "type": "dashed"})
        ax.setdefault("nameTextStyle", {})["color"] = _DARK_TEXT

    for ak in ("xAxis", "yAxis"):
        axval = option.get(ak)
        if isinstance(axval, list):
            for a in axval:
                _patch_axis(a)
        elif isinstance(axval, dict):
            _patch_axis(axval)

    # Grid — ensure enough top clearance for title + legend
    option.setdefault("grid", {}).setdefault("top", 68)

    return option


def _auto_log_scale(option: dict, series: list[dict]) -> None:
    """
    If multiple series have wildly different magnitudes (ratio > 50×),
    switch the primary y-axis to log scale so all bars are visible.
    Only applied to bar/combo charts with a value yAxis.
    """
    if not series:
        return
    maxima = []
    for s in series:
        data = s.get("data") or []
        nums = [v for v in data if isinstance(v, (int, float)) and v > 0]
        if nums:
            maxima.append(max(nums))
    if len(maxima) < 2:
        return
    ratio = max(maxima) / min(maxima)
    if ratio < 50:
        return
    yax = option.get("yAxis")
    if isinstance(yax, dict) and yax.get("type") == "value":
        yax["type"] = "log"
        yax["logBase"] = 10
        yax.setdefault("name", "(log scale)")
    elif isinstance(yax, list) and yax and yax[0].get("type") == "value":
        yax[0]["type"] = "log"
        yax[0]["logBase"] = 10
        yax[0].setdefault("name", "(log scale)")


def _build_agent_echart_option(args: dict) -> dict:
    """Build an ECharts option dict from generate_chart tool arguments."""
    chart_type   = (args.get("chart_type") or "bar").lower()
    title        = args.get("title") or ""
    labels       = args.get("labels") or []
    values       = args.get("values") or []
    series       = args.get("series") or []
    x_label      = args.get("x_label") or ""
    y_label      = args.get("y_label") or ""
    height       = max(200, min(700, int(args.get("height") or 320)))
    stacked      = bool(args.get("stacked"))
    show_labels  = bool(args.get("show_labels"))
    description  = args.get("description") or ""
    color_scheme = (args.get("color_scheme") or "default").lower()

    _PALETTES: dict = {
        "default":    ["#3B82F6","#10B981","#F59E0B","#EF4444","#8B5CF6","#06B6D4","#F97316","#84CC16","#EC4899","#6B7280"],
        "warm":       ["#EF4444","#F97316","#F59E0B","#EAB308","#84CC16","#22C55E","#F43F5E","#FB7185","#FBBF24","#A3E635"],
        "cool":       ["#06B6D4","#3B82F6","#8B5CF6","#A855F7","#EC4899","#10B981","#0EA5E9","#6366F1","#7C3AED","#14B8A6"],
        "traffic":    ["#22C55E","#F59E0B","#EF4444","#84CC16","#FBBF24","#F87171"],
        "couchbase":  ["#EA2328","#F59E0B","#3B82F6","#10B981","#8B5CF6","#06B6D4","#F97316","#6B7280"],
        "monochrome": ["#1e3a8a","#1d4ed8","#2563eb","#3b82f6","#60a5fa","#93c5fd","#bfdbfe","#dbeafe"],
    }
    _PAL = _PALETTES.get(color_scheme, _PALETTES["default"])

    def _out(opt: dict) -> dict:
        """Apply dark theme + auto-log scale, then return."""
        _apply_echart_theme(opt)
        # Auto-log for bar-family charts with multi-magnitude series
        if chart_type not in ("gauge", "treemap", "funnel", "scatter", "pie", "donut"):
            _auto_log_scale(opt, opt.get("series") or [])
        return opt

    # ── Gauge ──────────────────────────────────────────────────────────────────
    if chart_type == "gauge":
        gval = float(args.get("value") or (values[0] if values else 0))
        gmin = float(args.get("min_value") or 0)
        gmax = float(args.get("max_value") or 100)
        return _out({
            "title":   {"text": title, "left": "center", "top": "bottom"},
            "series":  [{
                "type": "gauge", "min": gmin, "max": gmax,
                "detail": {"formatter": "{value}", "fontSize": 22, "color": "#60a5fa"},
                "data": [{"value": round(gval, 2), "name": title}],
                "axisLine": {"lineStyle": {"width": 22, "color": [
                    [0.3, "#22C55E"], [0.7, "#F59E0B"], [1.0, "#EF4444"],
                ]}},
                "pointer": {"itemStyle": {"color": "auto"}},
                "axisTick": {"distance": -22, "length": 6, "lineStyle": {"color": "#fff", "width": 2}},
                "splitLine": {"distance": -22, "length": 14, "lineStyle": {"color": "#fff", "width": 3}},
                "axisLabel": {"color": _DARK_TEXT, "distance": 28, "fontSize": 11},
            }],
            "_height": height, "_description": description,
        })

    # ── Treemap ─────────────────────────────────────────────────────────────────
    if chart_type == "treemap":
        tree_data = (args.get("data_points")
                     or [{"name": l, "value": v} for l, v in zip(labels, values)])
        return _out({
            "title":  {"text": title, "left": "center"},
            "color":  _PAL,
            "series": [{"type": "treemap", "data": tree_data,
                        "label": {"show": True, "formatter": "{b}: {c}", "color": "#fff"},
                        "emphasis": {"label": {"fontSize": 14}}}],
            "_height": height, "_description": description,
        })

    # ── Funnel ──────────────────────────────────────────────────────────────────
    if chart_type == "funnel":
        funnel_data = ([{"name": s["name"], "value": sum(s.get("data") or [0])} for s in series]
                       if series else [{"name": l, "value": v} for l, v in zip(labels, values)])
        return _out({
            "title":   {"text": title, "left": "center"},
            "tooltip": {"trigger": "item", "formatter": "{b}: {c} ({d}%)"},
            "legend":  {"orient": "vertical", "left": "left"},
            "color":   _PAL,
            "series":  [{"type": "funnel", "data": funnel_data,
                         "label": {"position": "inside", "formatter": "{b}: {c}", "color": "#fff"}}],
            "_height": height, "_description": description,
        })

    # ── Scatter ─────────────────────────────────────────────────────────────────
    if chart_type == "scatter":
        pts = (args.get("data_points")
               or [{"x": i, "y": v, "name": l} for i, (l, v) in enumerate(zip(labels, values))])
        return _out({
            "title":   {"text": title},
            "tooltip": {"trigger": "item"},
            "color":   _PAL,
            "xAxis":   {"type": "value", "name": x_label or "X", "scale": True},
            "yAxis":   {"type": "value", "name": y_label or "Y", "scale": True},
            "series":  [{"type": "scatter", "symbolSize": 12,
                         "data": [[p.get("x", 0), p.get("y", 0)] for p in pts],
                         "label": {"show": show_labels, "formatter": "function(p){return p.data[0]+', '+p.data[1];}"}}],
            "_height": height, "_description": description,
        })

    # ── Pie / Donut ─────────────────────────────────────────────────────────────
    if chart_type in ("pie", "donut"):
        pie_data = ([{"name": s["name"], "value": sum(s.get("data") or [0])} for s in series]
                    if series else [{"name": l, "value": v} for l, v in zip(labels, values)])
        radius   = ["40%", "70%"] if chart_type == "donut" else "60%"
        lbl_opt  = {"formatter": "{b}: {c} ({d}%)", "color": _DARK_TEXT} if show_labels else {"show": False}
        return _out({
            "title":   {"text": title, "left": "center"},
            "tooltip": {"trigger": "item", "formatter": "{b}: {c} ({d}%)"},
            "legend":  {"orient": "vertical", "left": "left"},
            "color":   _PAL,
            "series":  [{"type": "pie", "radius": radius, "data": pie_data, "label": lbl_opt}],
            "_height": height, "_description": description,
        })

    # ── Bar / Line / Area / Stacked bar / Combo ──────────────────────────────────
    _is_combo    = chart_type == "combo"
    _is_stacked  = stacked or chart_type == "stacked_bar"
    _default_type = "line" if chart_type in ("line", "area") else "bar"

    def _mk_series(s_name, s_data, s_ctype=None):
        st = s_ctype or _default_type
        es: dict = {"name": s_name, "type": st, "data": s_data}
        if st == "line" or chart_type == "area":
            es["smooth"] = True
        if chart_type == "area":
            es["areaStyle"] = {"opacity": 0.25}
        if _is_stacked:
            es["stack"] = "total"
            if show_labels:
                es["label"] = {"show": True, "position": "inside", "color": "#fff"}
        elif show_labels:
            es["label"] = {"show": True, "position": "top", "color": _DARK_TEXT}
        return es

    if series:
        ec_series = [
            _mk_series(s["name"], s.get("data") or [], s.get("chart_type") if _is_combo else None)
            for s in series
        ]
        legend_data = [s["name"] for s in series]
        has_right = _is_combo and any(s.get("right_axis") for s in series)
        if has_right:
            for i, s in enumerate(series):
                if s.get("right_axis"):
                    ec_series[i]["yAxisIndex"] = 1
    else:
        ec_series   = [_mk_series("", values)]
        legend_data = []
        has_right   = False

    cat_axis: dict = {"type": "category", "data": labels}
    val_axis: dict = {"type": "value"}
    if x_label: cat_axis["name"] = x_label
    if y_label: val_axis["name"] = y_label

    if chart_type == "horizontal_bar":
        # If LLM passed one series per category (no labels), flatten into a single series.
        if not labels and series and all(len(s.get("data") or []) == 1 for s in series):
            flat_labels = [s["name"] for s in series]
            flat_values = [(s.get("data") or [0])[0] for s in series]
            cat_axis = {"type": "category", "data": flat_labels}
            ec_series = [{"type": "bar", "data": flat_values}]
            legend_data = []
        return _out({
            "title":   {"text": title},
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "legend":  {"data": legend_data} if legend_data else {},
            "color":   _PAL,
            "grid":    {"left": "22%", "right": "6%", "top": 68},
            "xAxis":   val_axis,
            "yAxis":   cat_axis,
            "series":  ec_series,
            "_height": height, "_description": description,
        })

    base: dict = {
        "title":   {"text": title},
        "tooltip": {"trigger": "axis"},
        "legend":  {"data": legend_data} if legend_data else {},
        "color":   _PAL,
        "xAxis":   cat_axis,
        "yAxis":   [val_axis, {"type": "value"}] if has_right else val_axis,
        "series":  ec_series,
        "_height": height, "_description": description,
    }
    return _out(base)


def _agent_filters_from_args(args: dict) -> dict:
    """Map agent tool args to the filters dict expected by tool_query_tickets."""
    filters: dict = {}
    if args.get("organization"):
        filters["organization"] = args["organization"]
    if args.get("date_from"):
        filters["date_from"] = args["date_from"]
    if args.get("date_to"):
        filters["date_to"] = args["date_to"]
    if args.get("priority"):
        filters["priorities"] = [args["priority"].upper()]
    if args.get("status"):
        filters["statuses"] = [args["status"].lower()]
    struct_kws: list[str] = []
    if args.get("cbse_only"):
        struct_kws.append("cbse")
    if args.get("jira_only"):
        struct_kws.append("jira")
    if args.get("cbse_id"):
        struct_kws.append(args["cbse_id"].lower())
    if args.get("keyword"):
        struct_kws.append(args["keyword"].lower())
    if struct_kws:
        filters["struct_keywords"] = struct_kws
    return filters



_TC_PATTERNS = [
    # Qwen/LMStudio native: <|tool_call>call:name{...}<tool_call|>
    re.compile(r"<\|tool_call\>call:(\w+)\s*(\{.*?\})\s*<tool_call\|>", re.DOTALL),
    # Hermes / ChatML: <tool_call>{"name":"...","arguments":{...}}</tool_call>
    re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.DOTALL),
    # Qwen3 formal: <|tool_call|>{...}<|/tool_call|>
    re.compile(r"<\|tool_call\|>\s*(\{.*?\})\s*<\|/tool_call\|>", re.DOTALL),
]


def _extract_text_tool_calls(content: str) -> list[tuple[str, dict]]:
    """
    Parse text-encoded tool calls from model content.
    Returns [(tool_name, args_dict), ...] for each detected call.
    Falls back to empty list if nothing parseable is found.
    """
    import json as _j, re as _re

    results: list[tuple[str, dict]] = []

    def _try_parse_args(raw: str) -> dict:
        """Best-effort JSON parse with light cleanup for common model quirks."""
        raw = raw.strip()
        # Replace <|"|> (Qwen string-escape artifact) with real quotes
        raw = raw.replace("<|\"|\">", '"').replace('<|"|>', '"')
        try:
            return _j.loads(raw)
        except Exception:
            pass
        # Try quoting unquoted keys: {Ticket ID: 123} → {"Ticket ID": 123}
        fixed = _re.sub(r'([{,])\s*([A-Za-z_][A-Za-z0-9_ ]*)\s*:', r'\1"\2":', raw)
        try:
            return _j.loads(fixed)
        except Exception:
            return {}

    for pat in _TC_PATTERNS:
        for m in pat.finditer(content):
            groups = m.groups()
            if len(groups) == 2 and not groups[0].startswith("{"):
                # Pattern 1: (name, args_block)
                name, args_raw = groups[0].strip(), groups[1]
                args = _try_parse_args(args_raw)
                results.append((name, args))
            else:
                # Patterns 2/3: single JSON blob with name + arguments
                blob = _try_parse_args(groups[0])
                if "name" in blob:
                    results.append((blob["name"], blob.get("arguments") or blob.get("args") or {}))

    return results


def _normalise_tool_args(name: str, args: dict) -> dict:
    """
    Fix common arg-format mismatches from models that don't follow the schema.
    generate_table: model may pass data=[{col:val,...}] instead of columns+rows.
    generate_chart: model may pass data=[...] instead of labels+values.
    """
    if name == "generate_table":
        if "data" in args and not args.get("columns") and not args.get("rows"):
            data = args["data"]
            if isinstance(data, list) and data:
                if isinstance(data[0], dict):
                    cols = list(data[0].keys())
                    rows = [[str(row.get(c, "")) for c in cols] for row in data]
                    args = {**args, "columns": cols, "rows": rows}
    if name == "generate_chart":
        if "data" in args and not args.get("values") and not args.get("series"):
            data = args["data"]
            if isinstance(data, list) and data and isinstance(data[0], dict):
                keys = list(data[0].keys())
                if len(keys) >= 2:
                    args = {**args,
                            "labels": [str(row.get(keys[0], "")) for row in data],
                            "values": [float(row.get(keys[1], 0) or 0) for row in data]}
    return args


def call_llm_with_tools(
    messages: list[dict],
    tools: list[dict],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    provider: str, model: str, api_key: str, base_url: str,
    max_tokens: int = 8192,
    max_rounds: int = 5,
    default_customer: str = "",
    ctx: dict | None = None,
    # ── AFTER v1.5.0: live status + interrupt ────────────────────────────────
    status_callback=None,    # callable(tool_name: str) — fired before each tool execution
    cancel_event=None,       # threading.Event — checked at start of each round; raises on set
    # ── BEFORE v1.5.0: no status_callback or cancel_event params ─────────────
) -> str:
    """
    Agentic tool-calling loop. Sends messages + tools to the LLM, executes
    any tool calls, appends results, and loops until the model produces a
    final text answer or max_rounds is reached.

    OpenAI-compatible function calling: lmstudio, ollama, openai, gemini.
    Native Anthropic tool use: claude (requires anthropic package).
    All others: falls back to plain call_llm (no tools).
    """
    import json as _json
    import traceback as _tb

    # ── AFTER v1.5.0: session log + system-message injection ─────────────────
    # ctx is the mutable dict threaded through the whole agent loop. We keep a
    # running tally of every tool called (tool_name → call_count) in _session_log
    # and prepend a compact summary to the system message so the LLM knows what
    # it has already done this session — reducing redundant re-calls.
    ctx = ctx or {}
    _slog: dict = ctx.setdefault("_session_log", {})

    def _inject_slog(msgs: list[dict]) -> list[dict]:
        """Return a copy of msgs with the session-tool-log appended to the first system message."""
        if not _slog:
            return msgs
        _block = "\n\nTOOLS USED THIS SESSION (do not re-call unnecessarily):\n" + "\n".join(
            f"  - {t}: {c}x" for t, c in sorted(_slog.items())
        )
        out = []
        _injected = False
        for _m in msgs:
            if _m.get("role") == "system" and not _injected:
                out.append({**_m, "content": (_m.get("content") or "") + _block})
                _injected = True
            else:
                out.append(_m)
        return out
    # ── BEFORE v1.5.0: no session log, ctx was caller-supplied or None ────────

    def _check_cancel():
        if cancel_event and cancel_event.is_set():
            raise InterruptedError("Agent run cancelled by user.")

    def _notify_tool(tool_name: str):
        if status_callback:
            try:
                status_callback(tool_name)
            except Exception:
                pass

    # ── Artifact stash shared by all provider paths ──────────────────────────
    # generate_chart / generate_table return echart/table fenced blocks.
    # Models often write prose as their final turn instead of echoing the block,
    # so we collect every artifact produced by tool execution and prepend it to
    # whatever text the model returns as its final answer.
    _artifact_stash: list[str] = []

    def _collect_artifact(result: str) -> None:
        if "```echart" in result or "```table" in result:
            _artifact_stash.append(result)

    def _apply_stash(content: str) -> str:
        prefix_parts = [a for a in _artifact_stash if a not in content]
        if not prefix_parts:
            return content
        return "\n\n".join(prefix_parts) + ("\n\n" + content if content else "")

    _openai_compat_providers = ("lmstudio", "ollama", "openai", "gemini")

    if provider in _openai_compat_providers:
        _base = (base_url or "").rstrip("/")
        if provider == "lmstudio":
            _base = _base or "http://localhost:1234/v1"
            if not _base.endswith("/v1"):
                _base += "/v1"
        elif provider == "ollama":
            _base = _base or "http://localhost:11434/v1"
            if not _base.endswith("/v1"):
                _base += "/v1"
        elif provider == "gemini":
            _base = _base or "https://generativelanguage.googleapis.com/v1beta/openai"

        import openai as _oai
        print(f"[agent] base_url={_base!r}  → will POST to {_base}/chat/completions")
        client = _oai.OpenAI(api_key=api_key or "lm-studio", base_url=_base)

        # LMStudio: ensure a model is loaded before the first round
        if provider == "lmstudio":
            _lms_base = _base.rstrip("/v1").rstrip("/")
            _loaded_id = lmstudio_ensure_model_loaded(_lms_base, model or "", timeout_s=120, model_type="llm")
            if _loaded_id:
                if _loaded_id != model:
                    print(f"[LMStudio] Using loaded LLM '{_loaded_id}' (configured '{model}')")
                model = _loaded_id

        def _safe_choice(r):
            """Return the first choice or raise with a clear diagnostic."""
            choices = getattr(r, "choices", None)
            if not choices:
                _err = getattr(r, "error", None)
                print(f"[agent] EMPTY choices — raw resp: {r!r}")
                if _err:
                    raise RuntimeError(
                        f"LMStudio rejected the request: {_err}\n"
                        "To fix: in LMStudio → select your model → Server tab → "
                        "enable 'Tool Use' (function calling) → restart server."
                    )
                raise RuntimeError(
                    "LLM returned no choices — model may not support function calling. "
                    "In LMStudio: select model → Server tab → enable 'Tool Use' → restart."
                )
            return choices[0]

        _msgs: list[dict] = _inject_slog(list(messages))
        _tool_calls_made = False

        try:
            for _round in range(max_rounds):
                _check_cancel()
                print(f"[agent] round={_round} msgs={len(_msgs)} tools_active={not _tool_calls_made}")
                # Per LMStudio docs: after tool results are in the history,
                # send the final request WITHOUT tools so the model writes
                # a natural-language answer rather than calling more tools.
                _req_tools = tools if not _tool_calls_made else None
                _kwargs: dict = {"model": model, "messages": _msgs, "max_tokens": max_tokens}
                if _req_tools:
                    _kwargs["tools"] = _req_tools
                resp = client.chat.completions.create(**_kwargs)
                choice = _safe_choice(resp)
                _tc_count = len(choice.message.tool_calls) if choice.message.tool_calls else 0
                print(f"[agent] finish_reason={choice.finish_reason!r} "
                      f"tool_calls_count={_tc_count} "
                      f"content={repr((choice.message.content or '')[:120])}")

                # Check tool_calls first — some models return finish_reason='stop'
                # even when they've made tool calls (Gemma, some Qwen variants).
                if not choice.message.tool_calls:
                    _raw_content = choice.message.content or ""
                    # Detect text-encoded tool calls in content (Qwen/Hermes/LMStudio
                    # native formats that bypass the tool_calls field entirely).
                    _text_calls = _extract_text_tool_calls(_raw_content)
                    if not _text_calls:
                        # Strip any residual noise from prior rounds before returning
                        for _tp in _TC_PATTERNS:
                            _raw_content = _tp.sub("", _raw_content).strip()
                        return _apply_stash(_raw_content)

                    # Execute each text-encoded tool call and inject results
                    print(f"[agent] detected {len(_text_calls)} text-encoded tool call(s) in content — executing and retrying")
                    _msgs.append({"role": "assistant", "content": _raw_content})
                    for _tc_name, _tc_args in _text_calls:
                        _tc_args = _normalise_tool_args(_tc_name, _tc_args)
                        _notify_tool(_tc_name)
                        print(f"[agent] text-call executing tool={_tc_name!r} args_keys={list(_tc_args.keys())}")
                        _tc_result = _get_main_app()._execute_agent_tool(
                            _tc_name, _tc_args,
                            cb_url, bucket, username, password, use_tls, scope, collection,
                            default_customer=default_customer, ctx=ctx,
                        )
                        _slog[_tc_name] = _slog.get(_tc_name, 0) + 1  # AFTER v1.5.0: session log
                        print(f"[agent] text-call result length={len(_tc_result)}")
                        _collect_artifact(_tc_result)
                        # Inject as a user-visible tool result so the model can reference it
                        _msgs.append({"role": "user", "content": f"[Tool result for {_tc_name}]:\n{_tc_result}"})
                    _tool_calls_made = True
                    continue  # retry: model will now write a clean final response

                # Build assistant turn dict — omit content when null (matches LMStudio format)
                _tool_calls_serial = []
                for tc in choice.message.tool_calls:
                    _fn = getattr(tc, "function", None)
                    if _fn is None:
                        print(f"[agent] WARNING: tool_call {tc!r} has no .function, skipping")
                        continue
                    print(f"[agent] serializing tool_call: name={_fn.name!r} id={tc.id!r}")
                    _tool_calls_serial.append({
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": _fn.name, "arguments": _fn.arguments or "{}"},
                    })
                _asst_msg: dict = {"role": "assistant", "tool_calls": _tool_calls_serial}
                if choice.message.content:
                    _asst_msg["content"] = choice.message.content
                _msgs.append(_asst_msg)

                for tc in choice.message.tool_calls:
                    _fn = getattr(tc, "function", None)
                    if _fn is None:
                        continue
                    try:
                        _args = _json.loads(_fn.arguments or "{}")
                    except _json.JSONDecodeError:
                        _args = {}
                    _notify_tool(_fn.name)
                    print(f"[agent] executing tool={_fn.name!r} args={_args}")
                    result = _get_main_app()._execute_agent_tool(
                        _fn.name, _args,
                        cb_url, bucket, username, password, use_tls, scope, collection,
                        default_customer=default_customer, ctx=ctx,
                    )
                    _slog[_fn.name] = _slog.get(_fn.name, 0) + 1  # AFTER v1.5.0: session log
                    print(f"[agent] tool result length={len(result)}")
                    _collect_artifact(result)
                    _msgs.append({"role": "tool", "tool_call_id": tc.id, "content": result})

                _tool_calls_made = True  # next round: no tools in request

            # Exhausted rounds — final answer without tools
            resp = client.chat.completions.create(
                model=model, messages=_msgs, max_tokens=max_tokens,
            )
            _final = _safe_choice(resp)
            _final_content = _final.message.content or ""
            # Strip any residual text-encoded tool call blocks from the output
            for _tp in _TC_PATTERNS:
                _final_content = _tp.sub("", _final_content).strip()
            return _apply_stash(_final_content)

        except Exception:
            _tb.print_exc()
            raise

    elif provider == "claude":
        try:
            import anthropic as _ant
        except ImportError:
            raise RuntimeError("anthropic package not installed — pip install anthropic")

        _ant_client = _ant.Anthropic(api_key=api_key or "")
        # Convert OpenAI-format tools → Anthropic format
        _ant_tools = [
            {
                "name": t["function"]["name"],
                "description": t["function"]["description"],
                "input_schema": t["function"]["parameters"],
            }
            for t in tools
        ]
        _msgs_injected = _inject_slog(messages)  # AFTER v1.5.0: slog in system prompt
        _sys = next((m["content"] for m in _msgs_injected if m["role"] == "system"), "")
        _conv = [m for m in _msgs_injected if m["role"] != "system"]

        for _round in range(max_rounds):
            _check_cancel()
            resp = _ant_client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=_sys,
                messages=_conv,
                tools=_ant_tools,
            )
            if resp.stop_reason == "end_turn":
                return _apply_stash(next((b.text for b in resp.content if hasattr(b, "text")), ""))

            tool_use_blocks = [b for b in resp.content if b.type == "tool_use"]
            if not tool_use_blocks:
                return _apply_stash(next((b.text for b in resp.content if hasattr(b, "text")), ""))

            _conv.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for tb in tool_use_blocks:
                _args = tb.input if isinstance(tb.input, dict) else {}
                _notify_tool(tb.name)
                print(f"[agent/claude] tool={tb.name} args={_args}")
                result = _get_main_app()._execute_agent_tool(
                    tb.name, _args,
                    cb_url, bucket, username, password, use_tls, scope, collection,
                    default_customer=default_customer, ctx=ctx,
                )
                _slog[tb.name] = _slog.get(tb.name, 0) + 1  # AFTER v1.5.0: session log
                _collect_artifact(result)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tb.id,
                    "content": result,
                })
            _conv.append({"role": "user", "content": tool_results})

        _final_text = next(
            (b.text for b in resp.content if hasattr(b, "text")), ""
        ) if resp else ""
        return _apply_stash(_final_text) or "Max tool-calling rounds reached without a final answer."

    else:
        # Bedrock or unknown — strip tools and fall back to plain call_llm
        return call_llm(messages, provider, model, api_key, base_url, max_tokens)


# ── AFTER v1.5.0: agent chat-flow helpers ────────────────────────────────────

_AGENT_ERROR_HINTS: list[tuple] = [
    # (match_str_lower, friendly_message)
    ("incorrect api key",     "Invalid API key — check your LLM settings."),
    ("authentication",        "Authentication failed — check your API key."),
    ("rate limit",            "Rate limit reached — wait a moment and try again."),
    ("quota",                 "API quota exceeded — check your provider usage limits."),
    ("connection refused",    "Can't reach the LLM server — is it running?"),
    ("name or service not known", "LLM server hostname not found — check the base URL in settings."),
    ("tool use",              "This model doesn't support function calling. In LMStudio: select model → Server tab → enable 'Tool Use' → restart server."),
    ("function calling",      "Function calling not supported by this model. Enable it in your LLM server settings."),
    ("cancelled by user",     "Agent run was cancelled."),
    ("max tool-calling",      "Agent reached its step limit without a final answer — try a more specific question."),
    ("context length",        "The conversation is too long for this model's context window — start a new chat or reduce history depth."),
]

def _classify_agent_error(exc: Exception) -> str:
    """Map a raw exception to a user-friendly message + hint."""
    raw = str(exc).lower()
    for keyword, friendly in _AGENT_ERROR_HINTS:
        if keyword in raw:
            return friendly
    return f"Agent error: {exc}"


def _generate_followup_suggestions(
    question: str, answer: str,
    provider: str, model: str, api_key: str, base_url: str,
    max_suggestions: int = 3,
) -> list[str]:
    """
    Ask the LLM for short follow-up question suggestions given the Q&A pair.
    Returns a list of strings (empty on any failure).
    """
    import json as _j
    prompt = (
        f"Given this question and answer, suggest {max_suggestions} short follow-up questions "
        f"a user might ask next. Output ONLY a JSON array of strings, nothing else.\n\n"
        f"Question: {question[:300]}\n\nAnswer: {answer[:600]}"
    )
    try:
        raw = call_llm(
            [{"role": "user", "content": prompt}],
            provider, model, api_key, base_url,
            max_tokens=256,
        )
        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        suggestions = _j.loads(raw.strip())
        if isinstance(suggestions, list):
            return [str(s).strip() for s in suggestions[:max_suggestions] if s]
    except Exception:
        pass
    return []


# ── Feature set helpers (v1.6.0) ─────────────────────────────────────────────

# Priority synonym sets — Zendesk uses "critical"/"high" etc.; many orgs store
# "P1"/"P2" directly.  Both must be recognised everywhere we filter by priority.
_P1_VALS: frozenset[str] = frozenset({"critical", "urgent", "p1"})
_P2_VALS: frozenset[str] = frozenset({"high", "p2"})
_P3_VALS: frozenset[str] = frozenset({"normal", "medium", "p3"})
_P4_VALS: frozenset[str] = frozenset({"low", "p4"})

# Statuses that mean "not yet resolved" — pending counts as open.
_OPEN_STATUSES: frozenset[str] = frozenset({"open", "pending", "new", "hold", "on-hold"})
_CLOSED_STATUSES: frozenset[str] = frozenset({"solved", "closed"})

def _canonical_priority(p: str) -> str:
    """Map any priority alias to canonical form used by _SLA_HOURS."""
    pl = (p or "").lower().strip()
    if pl in _P1_VALS:
        return "critical"
    if pl in _P2_VALS:
        return "high"
    if pl in _P3_VALS:
        return "normal"
    if pl in _P4_VALS:
        return "low"
    return "unknown"

_SLA_HOURS: dict[str, float] = {"critical": 4, "high": 24, "normal": 72, "low": 168, "unknown": 168}


def _compute_health_score(
    org: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    snap_collection: str = "snapshots",
) -> dict:
    """
    Compute a 0-100 composite health score for a customer.
    Returns a dict with the score, grade, dimensions, and a summary string.
    """
    import datetime as _dt
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))

    def _q(sql, **kw):
        from couchbase.options import QueryOptions as _QO
        return list(cl_.query(sql, _QO(named_parameters=kw)))

    _org_like = f"%{org.lower()}%"

    # Open P1/P2 counts
    pri_rows = _q(
        f"SELECT t.priority, t.status FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND LOWER(t.organization) LIKE $org",
        org=_org_like,
    )
    open_p1 = sum(1 for r in pri_rows
                  if (r.get("priority") or "").lower() in _P1_VALS
                  and (r.get("status") or "").lower() not in _CLOSED_STATUSES)
    open_p2 = sum(1 for r in pri_rows
                  if (r.get("priority") or "").lower() in _P2_VALS
                  and (r.get("status") or "").lower() not in _CLOSED_STATUSES)
    total   = len(pri_rows)

    # Escalation rate
    esc_rows = _q(
        f"SELECT COUNT(*) AS n FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND LOWER(t.organization) LIKE $org AND ARRAY_LENGTH(t.escalations) > 0",
        org=_org_like,
    )
    escalated    = (esc_rows[0].get("n") or 0) if esc_rows else 0
    esc_rate     = (escalated / total) if total else 0.0

    # Avg resolution days
    res_rows = _q(
        f"SELECT t.created, t.closed_at FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND LOWER(t.organization) LIKE $org "
        f"AND t.closed_at IS NOT NULL AND t.created IS NOT NULL LIMIT 200",
        org=_org_like,
    )
    days_list = []
    for r in res_rows:
        try:
            c = _dt.datetime.fromisoformat(r["created"][:19])
            x = _dt.datetime.fromisoformat(r["closed_at"][:19])
            d = (x - c).days
            if 0 <= d <= 365:
                days_list.append(d)
        except Exception:
            pass
    avg_days = sum(days_list) / len(days_list) if days_list else 0.0

    # Data freshness (hours since last scraped)
    fresh_rows = _q(
        f"SELECT MAX(t.last_scraped_at) AS ts FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND LOWER(t.organization) LIKE $org",
        org=_org_like,
    )
    last_ts     = (fresh_rows[0].get("ts") or 0) if fresh_rows else 0
    hours_stale = (_dt.datetime.now().timestamp() - last_ts) / 3600 if last_ts else 9999

    # Cluster bad/warn ratio from snapshots
    snap_rows = _q(
        f"SELECT s.bad_count, s.warn_count, s.node_count FROM `{bucket}`.`{scope}`.`{snap_collection}` s "
        f"WHERE LOWER(s.organization) LIKE $org AND s.node_count > 0 LIMIT 20",
        org=_org_like,
    )
    total_nodes = sum(r.get("node_count") or 0 for r in snap_rows)
    total_bad   = sum(r.get("bad_count") or 0 for r in snap_rows)
    cluster_ratio = (total_bad / total_nodes) if total_nodes else 0.0

    # Score computation (penalty model, 0-100)
    p1_penalty      = min(50, open_p1 * 20)
    p2_penalty      = min(15, open_p2 * 5)
    esc_penalty     = min(15, esc_rate * 20)
    res_penalty     = min(10, max(0, avg_days - 5) * 0.5)
    cluster_penalty = min(10, cluster_ratio * 50)
    stale_penalty   = min(10, max(0, (hours_stale - 24) / 48) * 10)
    score = max(0, round(100 - p1_penalty - p2_penalty - esc_penalty - res_penalty - cluster_penalty - stale_penalty))

    grade = "🟢 Healthy" if score >= 70 else ("🟡 At Risk" if score >= 40 else "🔴 Critical")
    summary = (
        f"Health: {score}/100 ({grade}) | "
        f"Open P1: {open_p1} | Open P2: {open_p2} | "
        f"Escalation rate: {esc_rate*100:.0f}% | "
        f"Avg resolution: {avg_days:.1f}d | "
        f"Data age: {hours_stale:.0f}h"
    )
    return {
        "organization": org, "score": score, "grade": grade, "summary": summary,
        "open_p1": open_p1, "open_p2": open_p2, "total_tickets": total,
        "escalation_rate_pct": round(esc_rate * 100, 1),
        "avg_resolution_days": round(avg_days, 1),
        "hours_since_scraped": round(hours_stale, 1),
        "cluster_bad_ratio": round(cluster_ratio, 3),
    }


def _compute_sla_compliance(
    org: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    date_from: str = "", date_to: str = "",
) -> dict:
    """Compute SLA compliance by priority for an org. Returns per-priority stats + overall."""
    import datetime as _dt
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))

    def _q(sql, **kw):
        from couchbase.options import QueryOptions as _QO
        return list(cl_.query(sql, _QO(named_parameters=kw)))

    _org_like = f"%{org.lower()}%"
    _date_filter = ""
    if date_from:
        _date_filter += f" AND t.created >= '{date_from}'"
    if date_to:
        _date_filter += f" AND t.created <= '{date_to}'"

    rows = _q(
        f"SELECT t.priority, t.created, t.closed_at FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND LOWER(t.organization) LIKE $org "
        f"AND t.closed_at IS NOT NULL AND t.created IS NOT NULL {_date_filter} LIMIT 1000",
        org=_org_like,
    )

    from collections import defaultdict
    stats: dict = defaultdict(lambda: {"met": 0, "breached": 0, "total": 0, "avg_hours": []})
    for r in rows:
        pri   = _canonical_priority(r.get("priority") or "unknown")
        limit = _SLA_HOURS.get(pri, 168)
        try:
            c = _dt.datetime.fromisoformat(r["created"][:19])
            x = _dt.datetime.fromisoformat(r["closed_at"][:19])
            hrs = (x - c).total_seconds() / 3600
            stats[pri]["total"] += 1
            stats[pri]["avg_hours"].append(hrs)
            if hrs <= limit:
                stats[pri]["met"] += 1
            else:
                stats[pri]["breached"] += 1
        except Exception:
            pass

    result = {}
    all_met = all_total = 0
    for pri, s in sorted(stats.items()):
        avg_h = sum(s["avg_hours"]) / len(s["avg_hours"]) if s["avg_hours"] else 0
        pct   = round(s["met"] / s["total"] * 100, 1) if s["total"] else None
        result[pri] = {
            "priority": pri, "met": s["met"], "breached": s["breached"],
            "total": s["total"], "compliance_pct": pct,
            "avg_resolution_hours": round(avg_h, 1),
            "sla_threshold_hours": _SLA_HOURS.get(pri, 168),
        }
        all_met   += s["met"]
        all_total += s["total"]

    overall = round(all_met / all_total * 100, 1) if all_total else None
    return {"organization": org, "overall_compliance_pct": overall,
            "by_priority": result, "tickets_analyzed": all_total}


def _get_digest(
    org: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    since_hours: int = 24,
) -> dict:
    """Return new/changed/resolved tickets for an org in the last N hours."""
    import datetime as _dt
    cutoff_epoch = _dt.datetime.now().timestamp() - since_hours * 3600
    cutoff_iso   = _dt.datetime.fromtimestamp(cutoff_epoch).isoformat()

    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))

    def _q(sql, **kw):
        from couchbase.options import QueryOptions as _QO
        return list(cl_.query(sql, _QO(named_parameters=kw)))

    _org_filter = f"AND LOWER(t.organization) LIKE '%{org.lower()}%'" if org else ""

    new_rows = _q(
        f"SELECT t.ticket_id, t.subject, t.priority, t.status, t.organization, t.created "
        f"FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND t.created >= $cutoff {_org_filter} "
        f"ORDER BY t.created DESC LIMIT 50",
        cutoff=cutoff_iso,
    )
    resolved_rows = _q(
        f"SELECT t.ticket_id, t.subject, t.priority, t.organization, t.closed_at "
        f"FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND t.closed_at >= $cutoff {_org_filter} "
        f"ORDER BY t.closed_at DESC LIMIT 50",
        cutoff=cutoff_iso,
    )
    stale_rows = _q(
        f"SELECT t.ticket_id, t.subject, t.priority, t.status, t.organization, t.last_scraped_at "
        f"FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND t.status NOT IN ['solved','closed'] {_org_filter} "
        f"AND t.last_scraped_at < $cutoff_epoch ORDER BY t.last_scraped_at ASC LIMIT 20",
        cutoff_epoch=cutoff_epoch,
    )
    return {
        "organization": org or "all", "since_hours": since_hours,
        "new_tickets": new_rows, "resolved_tickets": resolved_rows,
        "stale_open_tickets": stale_rows,
    }


def _save_query_to_cb(
    name: str, question: str, org: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
) -> str:
    """Save a named query to CB. Returns the doc key."""
    import uuid as _uuid
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))
    key  = f"saved_query::{name.lower().replace(' ', '_')}"
    doc  = {"type": "saved_query", "name": name, "question": question,
            "organization": org, "created_at": datetime.now(timezone.utc).isoformat()}
    cl_.bucket(bucket).scope(scope).collection(collection).upsert(key, doc)
    return key


def _list_saved_queries(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    org: str = "",
) -> list[dict]:
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))

    def _q(sql, **kw):
        from couchbase.options import QueryOptions as _QO
        return list(cl_.query(sql, _QO(named_parameters=kw)))

    _org_filter = "AND LOWER(q.organization) LIKE $org" if org else ""
    return _q(
        f"SELECT q.name, q.question, q.organization, q.created_at "
        f"FROM `{bucket}`.`{scope}`.`{collection}` q "
        f"WHERE q.type='saved_query' {_org_filter} ORDER BY q.created_at DESC",
        **({"org": f"%{org.lower()}%"} if org else {}),
    )


def _tag_ticket_in_cb(
    ticket_id: str, tags: list[str],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    replace: bool = False,
) -> str:
    conn = _cb_conn_str(cb_url, use_tls)
    col  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))\
           .bucket(bucket).scope(scope).collection(collection)
    try:
        doc = col.get(f"ticket::{ticket_id}").content_as[dict]
    except Exception:
        return f"Ticket {ticket_id} not found in Couchbase."
    existing = [] if replace else (doc.get("tags") or [])
    merged   = list(dict.fromkeys(existing + [t.strip() for t in tags if t.strip()]))
    doc["tags"] = merged
    col.upsert(f"ticket::{ticket_id}", doc)
    return f"Tags on {ticket_id} updated: {merged}"


def _generate_customer_report(
    org: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    snap_collection: str = "snapshots",
) -> str:
    """
    Build a structured markdown customer report from CB data (no LLM required).
    The agent can call this then append its own analysis.
    """
    from collections import Counter
    import datetime as _dt

    health = _compute_health_score(org, cb_url, bucket, username, password,
                                    use_tls, scope, collection, snap_collection)
    sla    = _compute_sla_compliance(org, cb_url, bucket, username, password,
                                      use_tls, scope, collection)
    digest = _get_digest(org, cb_url, bucket, username, password,
                          use_tls, scope, collection, since_hours=72)

    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))

    def _q(sql, **kw):
        from couchbase.options import QueryOptions as _QO
        return list(cl_.query(sql, _QO(named_parameters=kw)))

    _org_like = f"%{org.lower()}%"
    open_rows = _q(
        f"SELECT t.ticket_id, t.subject, t.priority, t.status, t.created, t.comment_count "
        f"FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"WHERE t.type='ticket' AND LOWER(t.organization) LIKE $org "
        f"AND t.status NOT IN ['solved','closed'] "
        f"ORDER BY CASE LOWER(t.priority) "
        f"WHEN 'critical' THEN 0 WHEN 'urgent' THEN 0 WHEN 'p1' THEN 0 "
        f"WHEN 'high' THEN 1 WHEN 'p2' THEN 1 "
        f"WHEN 'normal' THEN 2 WHEN 'medium' THEN 2 WHEN 'p3' THEN 2 "
        f"ELSE 3 END, t.created DESC "
        f"LIMIT 20",
        org=_org_like,
    )

    today  = _dt.date.today().isoformat()
    lines  = [
        f"# Customer Report: {org}",
        f"*Generated {today}*\n",
        f"## Health Score",
        f"**{health['score']}/100** — {health['grade']}",
        f"",
        f"| Dimension | Value |",
        f"|---|---|",
        f"| Open P1 tickets | {health['open_p1']} |",
        f"| Open P2 tickets | {health['open_p2']} |",
        f"| Escalation rate | {health['escalation_rate_pct']}% |",
        f"| Avg resolution | {health['avg_resolution_days']} days |",
        f"| Data freshness | {health['hours_since_scraped']}h ago |",
        f"",
        f"## SLA Compliance",
        f"Overall: **{sla['overall_compliance_pct']}%** ({sla['tickets_analyzed']} closed tickets analyzed)\n",
    ]
    for pri, s in sla.get("by_priority", {}).items():
        lines.append(f"- **{pri.capitalize()}**: {s['compliance_pct']}% met ({s['met']}/{s['total']}, threshold {s['sla_threshold_hours']}h, avg {s['avg_resolution_hours']}h)")

    lines += ["", "## Open Tickets", ""]
    if open_rows:
        lines.append("| ID | Subject | Priority | Status | Created | Comments |")
        lines.append("|---|---|---|---|---|---|")
        for t in open_rows:
            subj = (t.get("subject") or "")[:60]
            lines.append(f"| {t.get('ticket_id','')} | {subj} | {t.get('priority','')} | {t.get('status','')} | {(t.get('created') or '')[:10]} | {t.get('comment_count',0)} |")
    else:
        lines.append("*No open tickets found.*")

    if digest["new_tickets"]:
        lines += ["", "## New Tickets (last 72h)", ""]
        for t in digest["new_tickets"][:10]:
            lines.append(f"- [{t.get('ticket_id','')}] {(t.get('subject') or '')[:70]} ({t.get('priority','')})")

    if digest["resolved_tickets"]:
        lines += ["", "## Recently Resolved (last 72h)", ""]
        for t in digest["resolved_tickets"][:10]:
            lines.append(f"- [{t.get('ticket_id','')}] {(t.get('subject') or '')[:70]}")

    return "\n".join(lines)


# ── Assets persistence ────────────────────────────────────────────────────────

def _make_asset_thumbnail(asset_type: str, content: str) -> str:
    """
    Generate a compact thumbnail representation stored alongside the asset.
    Charts → stripped ECharts JSON (series/axes/color only, no decorations).
    Text types → first 280 characters of raw content.
    """
    if asset_type in ("chart", "echart"):
        try:
            opt   = json.loads(content)
            thumb = {k: opt[k] for k in ("series", "xAxis", "yAxis", "color", "radiusAxis", "angleAxis") if k in opt}
            thumb["animation"] = False
            thumb["grid"]      = [{"left": 2, "right": 2, "top": 2, "bottom": 2, "containLabel": False}]
            # Strip labels, tooltips, and text from every series so the thumbnail stays clean
            for s in (thumb.get("series") or []):
                s.pop("label", None)
                s.pop("emphasis", None)
            return json.dumps(thumb)
        except Exception:
            return ""
    # Text types: plain snippet (strip markdown fences / leading whitespace)
    snippet = content.strip()[:280]
    return snippet


def _ensure_assets_collection(cluster, bucket_name: str, scope: str) -> None:
    """Create the `assets` collection inside scope if it does not already exist."""
    try:
        from couchbase.management.collections import CollectionSpec
        cm = cluster.bucket(bucket_name).collections()
        existing = {s.name: {c.name for c in s.collections} for s in cm.get_all_scopes()}
        if "assets" not in existing.get(scope, set()):
            cm.create_collection(CollectionSpec("assets", scope_name=scope))
            try:
                cluster.query(
                    f"CREATE PRIMARY INDEX IF NOT EXISTS ON `{bucket_name}`.`{scope}`.`assets`"
                ).execute()
            except Exception:
                pass
    except Exception:
        pass


def _save_asset_to_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str,
    asset_type: str, title: str, content: str,
    session_id: str = "", org: str = "", filename: str = "",
) -> str:
    """Persist an artifact to the CB assets collection. Returns the asset ID."""
    import time as _time
    conn = _cb_conn_str(cb_url, use_tls)
    cl_ = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))
    _ensure_assets_collection(cl_, bucket, scope)
    aid   = str(uuid.uuid4())
    ext   = {"chart": "json", "report": "md", "table": "csv", "csv": "csv",
              "json": "json", "js": "js", "javascript": "js", "html": "html"}.get(asset_type, "txt")
    fname = filename or f"{title.lower().replace(' ', '_')}.{ext}"
    doc   = {
        "id":         aid,
        "type":       "asset",
        "asset_type": asset_type,
        "title":      title,
        "filename":   fname,
        "content":    content,
        "thumbnail":  _make_asset_thumbnail(asset_type, content),
        "org":        org,
        "session_id": session_id,
        "created_at": int(_time.time()),
        "mime_type":  _ASSET_MIME.get(asset_type, "text/plain"),
        "size_bytes": len(content.encode()),
    }
    cl_.bucket(bucket).scope(scope).collection("assets").upsert(f"asset::{aid}", doc)
    cl_.close()
    return aid


def _list_assets_from_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str,
    org: str = "", asset_type: str = "", limit: int = 200,
) -> list[dict]:
    """List saved assets from CB, newest first."""
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))
    _ensure_assets_collection(cl_, bucket, scope)
    from couchbase.options import QueryOptions as _QO
    wheres: list[str] = ["a.type='asset'"]
    params: dict      = {}
    if org:
        wheres.append("LOWER(a.org) LIKE $org")
        params["org"] = f"%{org.lower()}%"
    if asset_type:
        wheres.append("a.asset_type = $asset_type")
        params["asset_type"] = asset_type
    rows = list(cl_.query(
        f"SELECT a.id, a.asset_type, a.title, a.filename, a.org, "
        f"a.session_id, a.created_at, a.mime_type, a.size_bytes, a.thumbnail "
        f"FROM `{bucket}`.`{scope}`.`assets` a "
        f"WHERE {' AND '.join(wheres)} ORDER BY a.created_at DESC LIMIT {limit}",
        _QO(named_parameters=params),
    ))
    cl_.close()
    return rows


def _get_asset_content_from_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, asset_id: str,
) -> dict:
    """Fetch full asset document including content field."""
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))
    try:
        return cl_.bucket(bucket).scope(scope).collection("assets").get(
            f"asset::{asset_id}"
        ).content_as[dict]
    except Exception:
        return {}
    finally:
        cl_.close()


def _delete_asset_from_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, asset_id: str,
) -> bool:
    """Delete a single asset document. Returns True on success."""
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))
    try:
        cl_.bucket(bucket).scope(scope).collection("assets").remove(f"asset::{asset_id}")
        return True
    except Exception:
        return False
    finally:
        cl_.close()


# ── Phase 3.1: Fleet Analysis helpers ────────────────────────────────────────

def _fleet_query(cb_url, bucket, username, password, use_tls, scope, sql, **params):
    """Run a N1QL query against the fleet and return rows."""
    conn = _cb_conn_str(cb_url, use_tls)
    cl_  = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    cl_.wait_until_ready(timedelta(seconds=10))
    from couchbase.options import QueryOptions as _FQO
    rows = list(cl_.query(sql, _FQO(named_parameters=params) if params else _FQO()))
    cl_.close()
    return rows


def _query_fleet_tickets(
    cb_url, bucket, username, password, use_tls, scope, collection,
    group_by="organization", status_filter="open", limit=30,
):
    """
    Aggregate ticket counts across all orgs.
    group_by: organization | priority | status | cb_version | cbse
    status_filter: open | all | solved
    """
    _status_clause = {
        "open":   "AND LOWER(t.status) NOT IN ['solved','closed']",
        "solved": "AND LOWER(t.status) IN ['solved','closed']",
        "all":    "",
    }.get(status_filter, "")

    _group_field = {
        "organization": "t.organization",
        "priority":     "t.priority",
        "status":       "t.status",
        "cb_version":   "t.cb_version",
        "cbse":         "UNNEST(t.cbses) AS cbse_id",
    }.get(group_by, "t.organization")

    if group_by == "cbse":
        sql = (
            f"SELECT cbse_id AS label, COUNT(*) AS ticket_count, "
            f"COUNT(DISTINCT t.organization) AS org_count "
            f"FROM `{bucket}`.`{scope}`.`{collection}` t "
            f"UNNEST t.cbses AS cbse_id "
            f"WHERE t.type='ticket' AND cbse_id IS NOT NULL {_status_clause} "
            f"GROUP BY cbse_id ORDER BY org_count DESC, ticket_count DESC LIMIT {limit}"
        )
    else:
        sql = (
            f"SELECT {_group_field} AS label, "
            f"COUNT(*) AS ticket_count, "
            f"SUM(CASE WHEN LOWER(t.priority) IN ['critical','urgent','p1'] THEN 1 ELSE 0 END) AS p1_count, "
            f"SUM(CASE WHEN LOWER(t.priority) IN ['high','p2'] THEN 1 ELSE 0 END) AS p2_count "
            f"FROM `{bucket}`.`{scope}`.`{collection}` t "
            f"WHERE t.type='ticket' {_status_clause} AND {_group_field} IS NOT NULL "
            f"GROUP BY {_group_field} ORDER BY ticket_count DESC LIMIT {limit}"
        )
    return _fleet_query(cb_url, bucket, username, password, use_tls, scope, sql)


def _list_at_risk_clusters(
    cb_url, bucket, username, password, use_tls, scope, snap_collection="snapshots",
    ticket_collection="tickets", bad_threshold=0, warn_threshold=3, limit=25,
):
    """
    Return clusters with elevated bad/warn item counts that have no linked open ticket.
    Risk score = bad_items * 3 + warn_items.
    """
    sql = (
        f"SELECT s.cluster_name, s.organization, s.cb_version, "
        f"s.bad_items, s.warn_items, s.last_scraped_at, "
        f"(s.bad_items * 3 + s.warn_items) AS risk_score "
        f"FROM `{bucket}`.`{scope}`.`{snap_collection}` s "
        f"WHERE s.type='snapshot' "
        f"AND (s.bad_items > {bad_threshold} OR s.warn_items > {warn_threshold}) "
        f"AND NOT EXISTS ("
        f"  SELECT 1 FROM `{bucket}`.`{scope}`.`{ticket_collection}` t "
        f"  WHERE t.type='ticket' "
        f"  AND LOWER(t.status) NOT IN ['solved','closed'] "
        f"  AND LOWER(t.organization) = LOWER(s.organization) "
        f") "
        f"ORDER BY risk_score DESC LIMIT {limit}"
    )
    return _fleet_query(cb_url, bucket, username, password, use_tls, scope, sql)


def _fleet_version_distribution(
    cb_url, bucket, username, password, use_tls, scope, snap_collection="snapshots",
):
    """Count snapshots grouped by CB version across the entire fleet."""
    sql = (
        f"SELECT s.cb_version AS version, "
        f"COUNT(*) AS cluster_count, "
        f"COUNT(DISTINCT s.organization) AS org_count "
        f"FROM `{bucket}`.`{scope}`.`{snap_collection}` s "
        f"WHERE s.type='snapshot' AND s.cb_version IS NOT NULL "
        f"GROUP BY s.cb_version ORDER BY cluster_count DESC LIMIT 30"
    )
    return _fleet_query(cb_url, bucket, username, password, use_tls, scope, sql)


def _fleet_cbse_impact(
    cb_url, bucket, username, password, use_tls, scope, collection,
    limit=20,
):
    """Rank CBSEs by number of unique orgs affected (blast radius)."""
    sql = (
        f"SELECT cbse_id AS cbse, "
        f"COUNT(DISTINCT t.organization) AS org_count, "
        f"COUNT(*) AS ticket_count "
        f"FROM `{bucket}`.`{scope}`.`{collection}` t "
        f"UNNEST t.cbses AS cbse_id "
        f"WHERE t.type='ticket' AND cbse_id IS NOT NULL "
        f"GROUP BY cbse_id ORDER BY org_count DESC, ticket_count DESC LIMIT {limit}"
    )
    return _fleet_query(cb_url, bucket, username, password, use_tls, scope, sql)


def _compute_health_score_with_cluster(
    org, cb_url, bucket, username, password, use_tls, scope, collection,
    snap_collection="snapshots",
):
    """
    Extension of _compute_health_score that adds cluster bad_ratio dimension.
    Returns the same dict as _compute_health_score plus cluster_bad_ratio and cluster_count.
    """
    h = _compute_health_score(org, cb_url, bucket, username, password,
                               use_tls, scope, collection, snap_collection)
    # Add per-cluster bad_ratio breakdown
    try:
        rows = _fleet_query(
            cb_url, bucket, username, password, use_tls, scope,
            f"SELECT s.cluster_name, s.bad_items, s.warn_items "
            f"FROM `{bucket}`.`{scope}`.`{snap_collection}` s "
            f"WHERE s.type='snapshot' AND LOWER(s.organization) LIKE $org "
            f"ORDER BY s.bad_items DESC LIMIT 10",
            org=f"%{org.lower()}%",
        )
        h["cluster_breakdown"] = rows
        h["cluster_count"]     = len(rows)
    except Exception:
        h["cluster_breakdown"] = []
        h["cluster_count"]     = 0
    return h


# ─────────────────────────── Phase 3: Scoring & Analytics ────────────────────

# ── Few-shot scoring prompt ───────────────────────────────────────────────────
# (moved to supportal/ package, imported at top of file)



