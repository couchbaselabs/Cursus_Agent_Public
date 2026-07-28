#!/usr/bin/env python3
import os
import sys
import json
import time
import datetime
import threading
import uuid
import re
import asyncio
from pathlib import Path
import mistune

from nicegui import ui, run, app as ng_app
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from supportal.constants import BASE_URL, SETTINGS_FILE, COOKIES_FILE
from supportal.cb_helpers import (
    _cb_conn_str, tool_query_tickets, fetch_tickets_by_keys, vector_search_cb,
)
from supportal.agent_tools import (
    _AGENT_TOOLS as _ALL_AGENT_TOOLS,
    call_llm_with_tools,
    _query_fleet_tickets,
    _fleet_version_distribution,
    _fleet_ticket_freshness,
    _list_at_risk_clusters,
    _compute_health_score,
    _agent_filters_from_args,
    _normalise_tool_args,
    _generate_customer_report,
    _fleet_query,
    _fleet_cbse_impact,
    _compute_sla_compliance,
    _get_digest,
    _save_query_to_cb,
    _list_saved_queries,
    _tag_ticket_in_cb,
    _save_asset_to_cb,
    _list_assets_from_cb,
    _build_agent_echart_option,
    _compute_health_score_with_cluster,
)
from supportal.scoring import call_llm
from supportal.prompts import build_agent_system_prompt

_AGENT_TOOLS = _ALL_AGENT_TOOLS + [
    {
        "type": "function",
        "function": {
            "name": "list_organizations",
            "description": (
                "List all organizations in the local Couchbase database with their ticket counts. "
                "Use when the user asks 'what accounts do you have?', 'list all customers', "
                "'what organizations are in the system?', or before a fleet-wide operation "
                "to discover what orgs exist."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_my_sfdc_accounts",
            "description": (
                "Get the open Salesforce opportunities and account assignments for the current user — "
                "the SE whose name is configured in Settings → Salesforce → My Salesforce Identity. "
                "Use when the user asks 'what accounts am I on?', 'show me my pipeline', "
                "'my opportunities', 'my deals', 'what customers am I responsible for?', or "
                "'who are my accounts?'. No arguments needed — identity comes from the active profile."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]

_TOOL_LABELS: dict[str, str] = {
    "query_tickets":               "query tickets",
    "count_tickets":               "count tickets",
    "get_ticket":                  "fetch ticket",
    "search_tickets":              "search tickets",
    "rescrape_customer_tickets":   "refresh tickets",
    "scrape_customer_tickets":     "scrape tickets",
    "rescrape_ticket":             "refresh ticket",
    "batch_rescrape_tickets":      "batch refresh",
    "smart_refresh":               "smart refresh",
    "get_scrape_status":           "scrape status",
    "cancel_scrape_job":           "cancel scrape",
    "check_data_freshness":        "check data freshness",
    "get_portfolio_status":        "portfolio status",
    "get_customer_health_score":   "customer health",
    "get_digest":                  "account digest",
    "get_briefing":                "morning briefing",
    "generate_health_report":      "generate report",
    "generate_cluster_health_chart": "cluster chart",
    "fleet_version_distribution":  "version distribution",
    "fleet_cbse_impact":           "CBSE impact",
    "check_sla_compliance":        "SLA compliance",
    "search_customer_names":       "find account",
    "list_supportal_customers":    "list accounts",
    "vector_search":               "semantic search",
    "get_cluster_health":          "cluster health",
    "sync_snapshots":              "sync snapshots",
    "analyze_snapshot":            "analyze snapshot",
    "query_local_snapshots":       "query snapshots",
    "backfill_snapshot_topology":  "backfill topology",
    "backfill_last_comment_at":    "backfill timestamps",
    "score_ticket":                "score ticket",
    "batch_score_tickets":         "batch score",
    "tag_ticket":                  "tag ticket",
    "record_feedback":             "record feedback",
    "save_artifact":               "save artifact",
    "list_saved_queries":          "saved queries",
    "generate_table":              "build table",
    "get_current_time":            "current time",
    "list_organizations":          "list accounts",
    "get_account_opportunities":   "account opportunities",
    "get_se_opportunities":        "SE pipeline",
    "list_sfdc_accounts":          "SFDC accounts",
    "get_sfdc_field_mapping":      "field mapping",
    "update_sfdc_field_mapping":   "update field mapping",
    "sync_sfdc_data":              "sync Salesforce",
    "get_my_sfdc_accounts":        "my pipeline",
}

try:
    from couchbase.cluster import Cluster, ClusterOptions
    from couchbase.options import QueryOptions
    from couchbase.auth import PasswordAuthenticator
    _CB_AVAILABLE = True
except ImportError:
    _CB_AVAILABLE = False

_SERVER_STATE = {
    "customers": [],
    "customers_ts": 0.0,
    "version_dist": [],
    "all_orgs": [],        # full org list for pinned-account selector
    "all_orgs_ts": 0.0,
    "sfdc_accounts": {},   # org_name.lower() → account doc from accounts collection
    "sfdc_acct_count": 0,  # SE-scoped account count from last _load_customers_bg
    "sfdc_opp_count":  0,  # opportunity count from last _load_customers_bg
}

_ORG_CACHE_TTL = 86_400   # 24 h — org names rarely change

_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Archivo:wght@400;500;600;700;800&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
:root {
  --cbr: #ea2328; --cbr-h: #c9201d; --cbr-dk: #b81a1e;
  --cbr-t1: #fdecec; --cbr-t2: #fffafa; --cbr-t3: #f3d3d1;
  --dark: #141518; --darker: #1b1d21; --chip: #26282d; --chip-b: #34373d;
  --canvas: #ece8e1; --card: #ffffff;
  --b1: #e0dbd0; --b2: #eee9df; --b3: #e4dfd4;
  --t1: #1b1d21; --t2: #3c4046; --t3: #6b6f76; --t4: #9a9ea6;
  --ton-d: #c9ccd2; --ton-d2: #8a8f98;
  --green: #2f8f5b; --green-b: #4ec27f; --green-bg: #e8f6ee; --green-m: #8ee0a8;
  --amber: #c98a12; --amber-b: #fb8c00; --amber-bg: #fdf1e3;
  --danger: #e53935; --danger-d: #c02620;
  --sched: #5b6572; --sched-bg: #eef1f5;
  --cl-h: #eef3fb; --cl-hb: #bcd4f0; --cl-d: #fdeaea; --cl-db: #f0bcbc;
}
* { box-sizing: border-box; }
body, .nicegui-content { font-family: 'IBM Plex Sans', sans-serif; background: var(--canvas) !important; margin: 0; padding: 0; }
.cu-header { background: var(--dark) !important; height: 44px !important; min-height: 44px !important; padding: 0 20px !important; display: flex !important; align-items: center !important; gap: 0 !important; box-shadow: none !important; }
.cu-logo-mark { width: 22px; height: 22px; background: var(--cbr); border-radius: 5px; display: flex; align-items: center; justify-content: center; font-family: 'Archivo', sans-serif; font-weight: 800; font-size: 13px; color: white; flex-shrink: 0; }
.cu-wordmark { font-family: 'Archivo', sans-serif; font-weight: 600; font-size: 16px; color: white; margin-left: 8px; flex-shrink: 0; }
.cu-nav { display: flex; align-items: center; gap: 4px; margin-left: 16px; }
.cu-nav-tab { font-family: 'IBM Plex Sans', sans-serif; font-size: 13px; color: var(--ton-d); padding: 6px 13px; border-radius: 7px; cursor: pointer; transition: all 0.12s; white-space: nowrap; border: none; background: transparent; }
.cu-nav-tab:hover { background: #26282d; }
.cu-nav-tab.active { background: var(--cbr); color: white; }
.cu-spacer { flex: 1; }
.cu-cust-pill { background: var(--chip); border: 1px solid var(--chip-b); border-radius: 20px; padding: 5px 12px; display: flex; align-items: center; gap: 6px; cursor: pointer; font-family: 'IBM Plex Sans', sans-serif; font-size: 13px; color: var(--ton-d); transition: border-color 0.12s; position: relative; }
.cu-cust-pill:hover { border-color: var(--cbr); }
.cu-cust-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--amber-b); flex-shrink: 0; }
.cu-cb-status { display: flex; align-items: center; gap: 5px; margin-left: 12px; }
.cu-cb-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green-b); box-shadow: 0 0 4px var(--green-b); }
.cu-cb-label { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--ton-d2); }
.cu-body { display: flex; flex-direction: row; height: calc(100vh - 44px); overflow: hidden; width: 100%; }
.cu-canvas { flex: 1; overflow-y: auto; padding: 24px 28px; background: var(--canvas); }
.cu-tab-hidden { display: none !important; }
.cu-section-title { font-family: 'Archivo', sans-serif; font-weight: 800; font-size: 22px; color: var(--t1); }
.cu-section-sub { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t4); margin-top: 2px; }
.cu-section-hdr { display: flex; align-items: baseline; gap: 12px; margin-bottom: 14px; flex-wrap: wrap; }
.cu-kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-top: 18px; }
.cu-kpi { background: var(--card); border: 1px solid var(--b1); border-radius: 11px; padding: 15px; cursor: pointer; transition: all 0.12s; }
.cu-kpi:hover { border-color: var(--cbr); }
.cu-kpi.active { background: var(--cbr-t1); border-color: var(--cbr); }
.cu-kpi-label { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: var(--t4); text-transform: uppercase; letter-spacing: .06em; }
.cu-kpi-value { font-family: 'Archivo', sans-serif; font-weight: 800; font-size: 30px; margin-top: 4px; }
.cu-charts-row { display: grid; grid-template-columns: 1.3fr 1fr; gap: 12px; margin-top: 12px; }
.cu-card { background: var(--card); border: 1px solid var(--b1); border-radius: 11px; padding: 16px; }
.cu-card-title { font-family: 'Archivo', sans-serif; font-size: 13px; font-weight: 700; color: var(--t1); margin-bottom: 12px; }
.cu-hint { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t4); margin-top: 10px; }
.cu-bar-row { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; cursor: pointer; padding: 4px 6px; border-radius: 6px; transition: background 0.1s; }
.cu-bar-row:hover { background: #f4f2ec; }
.cu-bar-label { font-size: 12px; color: var(--t2); width: 120px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex-shrink: 0; }
.cu-bar-track { flex: 1; height: 8px; background: #f0ece6; border-radius: 4px; }
.cu-bar-fill { height: 100%; border-radius: 4px; background: var(--cbr); }
.cu-bar-count { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t3); width: 30px; text-align: right; }
.cu-ver-row { display: flex; align-items: center; gap: 6px; margin-bottom: 4px; }
.cu-ver-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
.cu-ver-name { font-size: 12px; color: var(--t2); flex: 1; }
.cu-ver-count { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t4); }
.cu-fresh-table { display: flex; flex-direction: column; gap: 5px; margin-top: 10px; }
.cu-fresh-head { display: grid; grid-template-columns: 1fr 130px 70px 72px 56px; gap: 8px; padding: 4px 10px; font-family: 'IBM Plex Mono', monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .06em; color: var(--t4); }
.cu-fresh-row { display: grid; grid-template-columns: 1fr 130px 70px 72px 56px; align-items: center; gap: 8px; padding: 7px 10px; background: #faf8f4; border-radius: 7px; border: 1px solid var(--b1); }
.cu-fresh-org { font-size: 13px; color: var(--t1); font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cu-fresh-date { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t3); }
.cu-fresh-age { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t3); }
.cu-fresh-badge { font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 10px; text-align: center; }
.cu-badge-fresh { background: #e8f6ee; color: #2f8f5b; }
.cu-badge-aging { background: #fdf1e3; color: #c98a12; }
.cu-badge-stale { background: #fdecec; color: #c02620; }
.cu-badge-unknown { background: #f4f2ec; color: var(--t4); }
.cu-fresh-ask { font-size: 11px; color: var(--cbr); background: none; border: 1px solid var(--cbr); border-radius: 6px; padding: 2px 8px; cursor: pointer; transition: all .12s; white-space: nowrap; }
.cu-fresh-ask:hover { background: var(--cbr); color: white; }
.cu-two-col { display: grid; grid-template-columns: 260px 1fr; gap: 16px; }
.cu-org-list { display: flex; flex-direction: column; gap: 6px; }
.cu-org-item { background: var(--card); border: 1px solid var(--b1); border-radius: 8px; padding: 10px 12px; cursor: pointer; transition: all 0.12s; }
.cu-org-item:hover { border-color: var(--b3); }
.cu-org-item.selected { border-color: var(--cbr); border-left: 3px solid var(--cbr); }
.cu-org-name { font-size: 13px; font-weight: 600; color: var(--t1); }
.cu-org-meta { font-size: 11px; color: var(--t4); margin-top: 2px; }
.cu-filter-bar { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; margin-bottom: 14px; }
.cu-filter-chip { border-radius: 20px; padding: 4px 12px; font-size: 12px; font-weight: 600; cursor: pointer; border: 1.5px solid; transition: all 0.12s; background: transparent; }
.cu-filter-chip.p1 { border-color: var(--danger); color: var(--danger); }
.cu-filter-chip.p1.on { background: var(--danger); color: white; }
.cu-filter-chip.p2 { border-color: var(--amber-b); color: var(--amber-b); }
.cu-filter-chip.p2.on { background: var(--amber-b); color: white; }
.cu-filter-chip.p3 { border-color: #43a047; color: #43a047; }
.cu-filter-chip.p3.on { background: #43a047; color: white; }
.cu-filter-chip.open { border-color: var(--danger-d); color: var(--danger-d); }
.cu-filter-chip.open.on { background: var(--danger-d); color: white; }
.cu-filter-chip.pending { border-color: var(--amber); color: var(--amber); }
.cu-filter-chip.pending.on { background: var(--amber); color: white; }
.cu-filter-chip.solved { border-color: var(--green); color: var(--green); }
.cu-filter-chip.solved.on { background: var(--green); color: white; }
.cu-divider { width: 1px; height: 20px; background: var(--b1); margin: 0 4px; }
.cu-search { border: 1px solid var(--b1); border-radius: 8px; padding: 5px 10px; font-size: 12px; font-family: 'IBM Plex Sans', sans-serif; outline: none; }
.cu-search:focus { border-color: var(--cbr); }
.cu-table-wrap { background: var(--card); border: 1px solid var(--b1); border-radius: 11px; overflow: hidden; }
.cu-table-head { display: grid; grid-template-columns: 64px 130px 1fr 62px 78px 74px 56px; background: #e8e4da; padding: 8px 12px; font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: var(--t1); text-transform: uppercase; letter-spacing: .06em; border-bottom: 2px solid var(--b1); }
.cu-table-head-cell { display: flex; align-items: center; gap: 4px; }
.cu-table-head-cell.sortable { cursor: pointer; user-select: none; }
.cu-table-head-cell.sortable:hover { color: var(--cbr); }
.cu-table-head-cell.sort-active { color: var(--cbr); font-weight: 700; }
.cu-sort-arrow { font-size: 9px; opacity: 0.5; }
.cu-sort-arrow.active { opacity: 1; }
.cu-table-row { display: grid; grid-template-columns: 64px 130px 1fr 62px 78px 74px 56px; padding: 9px 12px; font-size: 13px; border-top: 1px solid var(--b2); cursor: pointer; transition: background 0.1s; }
.cu-table-row:hover { background: #faf8f4; }
.cu-table-row:nth-child(even) { background: #faf8f4; }
.cu-table-row.flyout-open { background: var(--cbr-t1) !important; }
.cu-acct-chip { font-size: 11px; color: var(--cbr); font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: pointer; transition: color .1s; }
.cu-acct-chip:hover { color: var(--cbr-h); text-decoration: underline; }
/* Ticket flyout drawer */
.cu-flyout-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,.15); z-index: 998; }
.cu-flyout { position: fixed; top: 44px; right: 0; width: 420px; height: calc(100vh - 44px); background: var(--card); border-left: 1px solid var(--b1); box-shadow: -4px 0 24px rgba(0,0,0,.08); z-index: 999; display: flex; flex-direction: column; overflow: hidden; transform: translateX(100%); transition: transform .2s cubic-bezier(.4,0,.2,1); }
.cu-flyout.open { transform: translateX(0); }
.cu-flyout-header { background: var(--dark); padding: 14px 16px; display: flex; align-items: flex-start; gap: 10px; flex-shrink: 0; }
.cu-flyout-tid { font-family: 'IBM Plex Mono', monospace; font-size: 12px; color: var(--cbr); flex-shrink: 0; margin-top: 2px; }
.cu-flyout-subj { font-family: 'Archivo', sans-serif; font-size: 14px; font-weight: 600; color: white; flex: 1; line-height: 1.35; }
.cu-flyout-close { background: none; border: none; color: var(--ton-d2); font-size: 18px; cursor: pointer; padding: 0; flex-shrink: 0; line-height: 1; }
.cu-flyout-close:hover { color: white; }
.cu-flyout-body { flex: 1; overflow-y: auto; padding: 16px; }
.cu-flyout-meta { display: grid; grid-template-columns: 90px 1fr; row-gap: 8px; column-gap: 10px; font-size: 12px; margin-bottom: 16px; }
.cu-flyout-key { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: var(--t4); text-transform: uppercase; letter-spacing: .04em; padding-top: 2px; }
.cu-flyout-val { font-weight: 500; color: var(--t1); }
.cu-flyout-section { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: var(--t4); text-transform: uppercase; letter-spacing: .06em; margin: 14px 0 6px; padding-bottom: 4px; border-bottom: 1px solid var(--b2); }
.cu-flyout-desc { font-size: 12px; color: var(--t2); line-height: 1.55; white-space: pre-wrap; word-break: break-word; }
.cu-flyout-actions { padding: 12px 16px; border-top: 1px solid var(--b1); display: flex; gap: 8px; flex-shrink: 0; flex-wrap: wrap; }
.cu-flyout-btn { border-radius: 7px; padding: 7px 14px; font-size: 12px; font-weight: 600; font-family: 'IBM Plex Sans', sans-serif; cursor: pointer; transition: all .12s; border: 1.5px solid; }
.cu-flyout-btn.primary { background: var(--cbr); border-color: var(--cbr); color: white; }
.cu-flyout-btn.primary:hover { background: var(--cbr-h); border-color: var(--cbr-h); }
.cu-flyout-btn.outline { background: transparent; border-color: var(--b1); color: var(--t2); }
.cu-flyout-btn.outline:hover { border-color: var(--cbr); color: var(--cbr); }
.cu-flyout-btn.scope { background: var(--green-bg); border-color: var(--green-b); color: var(--green); }
.cu-flyout-btn.scope:hover { background: var(--green-b); color: white; }
.cu-ticket-id { font-family: 'IBM Plex Mono', monospace; color: var(--cbr); font-size: 12px; }
.cu-subj-cell { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; }
.cu-status-cell { font-size: 12px; color: var(--t3); }
.cu-date-cell { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t4); }
.cu-recent-row { display: flex; align-items: center; gap: 8px; padding: 6px 0; border-bottom: 1px solid var(--b2); cursor: pointer; }
.cu-recent-subj { flex: 1; font-size: 12px; color: var(--t2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cu-hint { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t4); margin-top: 8px; }
.cu-pri-pill { border-radius: 20px; padding: 2px 8px; font-size: 11px; font-weight: 600; display: inline-block; }
.cu-pri-p1 { background: var(--cbr-t1); color: var(--danger-d); }
.cu-pri-p2 { background: var(--amber-bg); color: var(--amber); }
.cu-pri-p3 { background: var(--green-bg); color: var(--green); }
.cu-score-hi { color: var(--danger-d); font-weight: 700; }
.cu-score-md { color: var(--amber); font-weight: 700; }
.cu-score-lo { color: var(--t3); font-weight: 700; }
.cu-config-row { display: flex; gap: 12px; padding: 4px 0; border-bottom: 1px solid var(--b2); align-items: center; }
.cu-config-key { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--t4); width: 110px; flex-shrink: 0; text-transform: uppercase; letter-spacing: .04em; }
.cu-config-val { font-size: 12px; color: var(--t1); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cu-summary-banner { background: var(--card); border: 1px solid var(--b1); border-left: 3px solid var(--cbr); border-radius: 8px; padding: 10px 14px; margin-bottom: 12px; display: flex; align-items: flex-start; gap: 10px; }
.cu-summary-tag { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: var(--cbr); white-space: nowrap; margin-top: 2px; }
.cu-summary-text { font-size: 13px; color: var(--t2); flex: 1; }
.cu-cust-menu { position: absolute; top: 38px; right: 0; background: var(--darker); border-radius: 10px; box-shadow: 0 12px 40px rgba(0,0,0,.5); min-width: 240px; max-height: 420px; overflow-y: auto; z-index: 9999; padding: 6px; }
.cu-cust-menu-hdr { color: var(--ton-d2); font-size: 11px; padding: 6px 10px; font-family: 'IBM Plex Mono', monospace; text-transform: uppercase; letter-spacing: .04em; }
.cu-cust-menu-item { display: flex; align-items: center; gap: 8px; padding: 8px 10px; border-radius: 7px; cursor: pointer; color: var(--ton-d); font-size: 13px; transition: background 0.1s; }
.cu-cust-menu-p1 { margin-left: auto; font-size: 11px; color: var(--ton-d2); font-family: 'IBM Plex Mono', monospace; }
.cu-cust-menu-item:hover { background: var(--chip); }
.cu-pin-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--cbr); flex-shrink: 0; }
.cu-cust-menu-search { width: 100%; background: var(--chip); border: 1px solid var(--chip-b); border-radius: 7px; color: var(--ton-d); font-family: 'IBM Plex Sans',sans-serif; font-size: 12px; padding: 5px 9px; margin-bottom: 4px; outline: none; box-sizing: border-box; }
.cu-cust-menu-search::placeholder { color: var(--ton-d2); }
.cu-gear-btn { background: none; border: none; cursor: pointer; color: var(--ton-d2); font-size: 22px; padding: 4px 8px; border-radius: 6px; transition: color .12s, background .12s; margin-left: 4px; }
.cu-gear-btn:hover { color: var(--ton-d); background: var(--chip); }
.cu-clear-scope { background: var(--chip); border: 1px solid var(--chip-b); color: var(--ton-d); border-radius: 14px; padding: 3px 9px; font-size: 11px; cursor: pointer; transition: all .12s; white-space: nowrap; margin-left: 4px; display: flex; align-items: center; gap: 4px; }
.cu-clear-scope:hover { border-color: var(--cbr); color: var(--cbr); }
.cu-role-badge { font-size: 9px; font-weight: 700; border-radius: 3px; padding: 1px 5px; vertical-align: middle; margin-left: 4px; display: inline-block; }
.cu-role-primary { background: #e8f6ee; color: #2f8f5b; border: 1px solid #b0dfbf; }
.cu-role-supporting { background: #e8f0fc; color: #1a56b0; border: 1px solid #a8c4f0; }
.cu-org-item.primary { border-left: 3px solid var(--green-b); }
.cu-org-item.supporting { border-left: 3px solid #4a90e2; }
.cu-bar-role-dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; margin-right: 4px; flex-shrink: 0; }
.cu-all-custs-item { background: transparent; border: 1.5px dashed var(--b1); border-radius: 8px; padding: 8px 12px; cursor: pointer; transition: all 0.12s; display: flex; align-items: center; gap: 6px; }
.cu-all-custs-item:hover { border-color: var(--cbr); }
.cu-all-custs-item.active { border-color: var(--cbr); background: var(--cbr-t1); }
.cu-role-chip { border-radius: 14px; padding: 4px 12px; font-size: 11px; font-weight: 600; cursor: pointer; border: 1.5px solid; transition: all 0.12s; background: transparent; white-space: nowrap; }
.cu-role-chip.all  { border-color: var(--b1); color: var(--t3); }
.cu-role-chip.all.on  { background: var(--chip); border-color: var(--chip-b); color: var(--ton-d); }
.cu-role-chip.primary   { border-color: var(--green-b); color: var(--green); }
.cu-role-chip.primary.on   { background: var(--green-bg); border-color: var(--green); color: var(--green); }
.cu-role-chip.supporting { border-color: #4a90e2; color: #1a56b0; }
.cu-role-chip.supporting.on { background: #e8f0fc; border-color: #4a90e2; color: #1a56b0; }
.cu-role-chip.other  { border-color: var(--b1); color: var(--t3); }
.cu-role-chip.other.on  { background: #f4f2ec; border-color: var(--b3); color: var(--t2); }
.cu-pin-bar-label { font-size: 11px; color: var(--cbr); font-family: 'IBM Plex Mono',monospace; margin-left: 4px; }
@keyframes cuFade { from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:none} }
.cu-fresh { animation: cuFade 0.25s ease; }
/* Force dialog surfaces and their Quasar internals to be readable in light mode */
.q-dialog .q-card, .q-dialog__inner > div { background: #ffffff !important; color: #111827 !important; }
.q-dialog .q-tab { color: #374151 !important; }
.q-dialog .q-tab__label { color: inherit !important; }
.q-dialog .q-tab-panels, .q-dialog .q-tab-panel { background: #ffffff !important; }
"""


def _load_settings() -> dict:
    path = Path(SETTINGS_FILE)
    if path.exists():
        with path.open() as f:
            return json.load(f)
    return {}


def _active_profile(settings: dict) -> dict:
    last = settings.get("__last__", "")
    return settings.get(last, {})


def _load_pinned_accounts() -> list[str]:
    s = _load_settings()
    return s.get("pinned_accounts", [])


def _save_pinned_accounts(names: list[str]) -> None:
    path = Path(SETTINGS_FILE)
    try:
        s = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        s = {}
    s["pinned_accounts"] = sorted(set(names))
    s["__org_cache__"] = s.get("__org_cache__", [])
    s["__org_cache_ts__"] = s.get("__org_cache_ts__", 0)
    path.write_text(json.dumps(s, indent=2))


def _load_extra_accounts() -> list[str]:
    """Org names manually added to track beyond SFDC primary-SE scope."""
    return _load_settings().get("extra_accounts", [])


def _save_extra_accounts(names: list[str]) -> None:
    path = Path(SETTINGS_FILE)
    try:
        s = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        s = {}
    s["extra_accounts"] = sorted(set(n for n in names if n))
    path.write_text(json.dumps(s, indent=2))


def _load_org_cache() -> tuple[list[str], float]:
    """Return (org_names, timestamp) from settings cache."""
    s = _load_settings()
    return s.get("__org_cache__", []), float(s.get("__org_cache_ts__", 0))


def _save_org_cache(names: list[str]) -> None:
    path = Path(SETTINGS_FILE)
    try:
        s = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        s = {}
    s["__org_cache__"] = sorted(names)
    s["__org_cache_ts__"] = time.time()
    path.write_text(json.dumps(s, indent=2))


def _load_sfdc_creds() -> dict:
    s = _load_settings()
    prof = _active_profile(s)
    return {
        "token_host":      prof.get("sfdc_token_host",      "https://couchbase.my.salesforce.com"),
        "consumer_key":    prof.get("sfdc_consumer_key",    ""),
        "consumer_secret": prof.get("sfdc_consumer_secret", ""),
        "auth_flow":       prof.get("sfdc_auth_flow",       "client_credentials"),
        "user_name":       prof.get("sfdc_user_name",       ""),
        "user_email":      prof.get("sfdc_user_email",      ""),
        "user_id":         prof.get("sfdc_user_id",         ""),
    }


def _save_sfdc_creds(token_host: str, consumer_key: str, consumer_secret: str, auth_flow: str) -> None:
    path = Path(SETTINGS_FILE)
    try:
        s = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        s = {}
    last = s.get("__last__", "default")
    if last not in s:
        s[last] = {}
    s[last]["sfdc_token_host"]      = token_host
    s[last]["sfdc_consumer_key"]    = consumer_key
    s[last]["sfdc_consumer_secret"] = consumer_secret
    s[last]["sfdc_auth_flow"]       = auth_flow
    path.write_text(json.dumps(s, indent=2))
    # Mirror to Couchbase (best-effort)
    try:
        _mirror_sfdc_creds_to_cb(s[last], {
            "token_host": token_host, "consumer_key": consumer_key,
            "consumer_secret": consumer_secret, "auth_flow": auth_flow,
        })
    except Exception:
        pass


def _save_sfdc_identity(user_name: str, user_email: str = "", user_id: str = "") -> None:
    """Write SFDC identity fields to the active profile. Only overwrites non-empty values."""
    path = Path(SETTINGS_FILE)
    try:
        s = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        s = {}
    last = s.get("__last__", "default")
    if last not in s:
        s[last] = {}
    s[last]["sfdc_user_name"] = user_name
    if user_email:
        s[last]["sfdc_user_email"] = user_email
    if user_id:
        s[last]["sfdc_user_id"] = user_id
    path.write_text(json.dumps(s, indent=2))


def _mirror_sfdc_creds_to_cb(prof: dict, creds: dict) -> None:
    try:
        from couchbase.cluster import Cluster
        from couchbase.auth import PasswordAuthenticator
        from couchbase.options import ClusterOptions
        import datetime as _dt
        conn = _cb_conn_str(prof.get("cb_url", "localhost"), prof.get("cb_tls", False))
        cluster = Cluster(conn, ClusterOptions(PasswordAuthenticator(
            prof.get("cb_user", ""), prof.get("cb_pass", ""),
        )))
        cluster.wait_until_ready(_dt.timedelta(seconds=5))
        scope = prof.get("cb_scope", "transcripts")
        col = cluster.bucket(prof.get("cb_bucket", "rag")).scope(scope).collection("accounts")
        col.upsert("config::sfdc_credentials", creds)
    except Exception:
        pass


def _execute_agent_tool(
    name: str, args: dict,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    default_customer: str = "", ctx=None,
) -> str:
    args = _normalise_tool_args(name, args)

    # ── Scraping / job tools — dispatched via shared scrape workers ───────────
    _scrape_tools = {
        "rescrape_customer_tickets", "scrape_customer_tickets", "cancel_scrape_job",
        "get_scrape_status", "batch_rescrape_tickets", "smart_refresh",
    }
    if name in _scrape_tools:
        from supportal.scrape_dispatch import execute_scrape_tool
        return execute_scrape_tool(
            name, args, cb_url, bucket, username, password,
            use_tls, scope, collection,
            default_customer=default_customer, ctx=ctx,
        )

    # ── Other strabo-only tools — not available in unified shell ───────────────
    _unavailable = {
        "batch_score_tickets", "sync_snapshots", "backfill_snapshot_topology",
        "backfill_last_comment_at", "score_ticket", "fetch_snapshots",
        "analyze_snapshot", "query_local_snapshots", "cluster_hw_chart",
        "query_supportal", "list_supportal_customers",
    }
    if name in _unavailable:
        return f"'{name}' is not available in the unified shell."

    # ── query_tickets ──────────────────────────────────────────────────────────
    if name == "query_tickets":
        filters = _agent_filters_from_args(args)
        if default_customer and not filters.get("organization"):
            filters["organization"] = default_customer
        try:
            rows = tool_query_tickets(filters, cb_url, bucket, username, password, use_tls, scope, collection)
            if not rows:
                return "No tickets matched those filters."
            lines = ["| # | Subject | Priority | Status | Created |",
                     "|---|---------|----------|--------|---------|"]
            for r in rows[:50]:
                lines.append(
                    f"| {r.get('ticket_id','')} "
                    f"| {(r.get('subject') or '')[:60]} "
                    f"| {r.get('priority','')} "
                    f"| {r.get('status','')} "
                    f"| {(r.get('created') or '')[:10]} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"query_tickets error: {exc}"

    # ── count_tickets ──────────────────────────────────────────────────────────
    if name == "count_tickets":
        filters = _agent_filters_from_args(args)
        if default_customer and not filters.get("organization"):
            filters["organization"] = default_customer
        try:
            rows = tool_query_tickets(filters, cb_url, bucket, username, password, use_tls, scope, collection)
            return str(len(rows))
        except Exception as exc:
            return f"count_tickets error: {exc}"

    # ── get_ticket ─────────────────────────────────────────────────────────────
    if name == "get_ticket":
        ticket_id = str(args.get("ticket_id", "")).strip()
        if not ticket_id:
            return "ticket_id is required."
        try:
            docs = fetch_tickets_by_keys(
                [f"ticket::{ticket_id}"],
                cb_url, bucket, username, password, use_tls, scope, collection,
            )
            if not docs:
                return f"Ticket {ticket_id} not found."
            d = docs[0]
            lines = [f"**Ticket #{ticket_id}**"]
            for k in ("subject", "status", "priority", "organization", "created", "updated", "description"):
                val = d.get(k)
                if val is not None:
                    lines.append(f"- **{k}**: {str(val)[:300]}")
            return "\n".join(lines)
        except Exception as exc:
            return f"get_ticket error: {exc}"

    # ── vector_search ──────────────────────────────────────────────────────────
    if name == "vector_search":
        return (
            "vector_search requires a pre-computed embedding vector. "
            "Use query_tickets or search_customer_names for text-based lookups."
        )

    # ── list_organizations ─────────────────────────────────────────────────────
    if name == "list_organizations":
        try:
            rows = _query_fleet_tickets(
                cb_url, bucket, username, password, use_tls, scope, collection,
                group_by="organization", status_filter="all", limit=100,
            )
            if not rows:
                return "No organizations found."
            lines = ["| Organization | Tickets |", "|-------------|---------|"]
            for r in rows:
                lines.append(f"| {r.get('label','')} | {r.get('ticket_count',0)} |")
            return "\n".join(lines)
        except Exception as exc:
            return f"list_organizations error: {exc}"

    # ── search_customer_names ──────────────────────────────────────────────────
    if name == "search_customer_names":
        query = args.get("query", "")
        try:
            rows = _fleet_query(
                cb_url, bucket, username, password, use_tls, scope,
                f"SELECT DISTINCT t.organization FROM `{bucket}`.`{scope}`.`{collection}` t "
                f"WHERE LOWER(t.organization) LIKE '%' || LOWER($query) || '%' LIMIT 20",
                query=query,
            )
            if not rows:
                return f"No organizations matching '{query}'."
            names = [r.get("organization", "") for r in rows if r.get("organization")]
            return "\n".join(f"- {n}" for n in names)
        except Exception as exc:
            return f"search_customer_names error: {exc}"

    # ── get_briefing ───────────────────────────────────────────────────────────
    if name == "get_briefing":
        try:
            rows = _query_fleet_tickets(
                cb_url, bucket, username, password, use_tls, scope, collection,
                group_by="organization", status_filter="open", limit=10,
            )
            if not rows:
                return "No open tickets across the fleet."
            lines = ["| Org | Open | P1 |", "|-----|------|----|"]
            for r in rows:
                lines.append(
                    f"| {r.get('label','')} | {r.get('ticket_count',0)} | {r.get('p1_count',0)} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"get_briefing error: {exc}"

    # ── generate_chart ─────────────────────────────────────────────────────────
    if name == "generate_chart":
        try:
            option = _build_agent_echart_option(args)
            return json.dumps(option, indent=2)
        except Exception as exc:
            return f"generate_chart error: {exc}"

    # ── generate_table ─────────────────────────────────────────────────────────
    if name == "generate_table":
        headers = args.get("headers", [])
        rows = args.get("rows", [])
        if not headers:
            return "No headers provided."
        sep = "|" + "|".join("---" for _ in headers) + "|"
        hdr = "|" + "|".join(str(h) for h in headers) + "|"
        body_lines = []
        for row in rows:
            if isinstance(row, (list, tuple)):
                body_lines.append("|" + "|".join(str(c) for c in row) + "|")
            elif isinstance(row, dict):
                body_lines.append("|" + "|".join(str(row.get(h, "")) for h in headers) + "|")
        return "\n".join([hdr, sep] + body_lines)

    # ── check_sla_compliance ───────────────────────────────────────────────────
    if name == "check_sla_compliance":
        org = args.get("organization") or default_customer
        if not org:
            return "organization is required."
        try:
            result = _compute_sla_compliance(org, cb_url, bucket, username, password, use_tls, scope, collection)
            return json.dumps(result, indent=2)
        except Exception as exc:
            return f"check_sla_compliance error: {exc}"

    # ── get_portfolio_status ───────────────────────────────────────────────────
    if name == "get_portfolio_status":
        try:
            rows = _query_fleet_tickets(
                cb_url, bucket, username, password, use_tls, scope, collection,
                group_by="organization", status_filter="open", limit=50,
            )
            if not rows:
                return "No open tickets in the fleet."
            lines = ["| Org | Tickets | P1 | P2 |", "|-----|---------|----|----|"]
            for r in rows:
                lines.append(
                    f"| {r.get('label','')} "
                    f"| {r.get('ticket_count',0)} "
                    f"| {r.get('p1_count',0)} "
                    f"| {r.get('p2_count',0)} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"get_portfolio_status error: {exc}"

    # ── query_fleet_tickets ────────────────────────────────────────────────────
    if name == "query_fleet_tickets":
        try:
            rows = _query_fleet_tickets(
                cb_url, bucket, username, password, use_tls, scope, collection,
                group_by=args.get("group_by", "organization"),
                status_filter=args.get("status_filter", "open"),
                limit=args.get("limit", 30),
            )
            if not rows:
                return "No results."
            lines = ["| Org | Tickets | P1 | P2 |", "|-----|---------|----|----|"]
            for r in rows:
                lines.append(
                    f"| {r.get('label','')} "
                    f"| {r.get('ticket_count',0)} "
                    f"| {r.get('p1_count',0)} "
                    f"| {r.get('p2_count',0)} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"query_fleet_tickets error: {exc}"

    # ── fleet_version_distribution ─────────────────────────────────────────────
    if name == "fleet_version_distribution":
        try:
            rows = _fleet_version_distribution(
                cb_url, bucket, username, password, use_tls, scope,
            )
            if not rows:
                return "No version data available."
            lines = ["| Version | Clusters |", "|---------|----------|"]
            for r in rows:
                lines.append(f"| {r.get('version','unknown')} | {r.get('cluster_count',0)} |")
            return "\n".join(lines)
        except Exception as exc:
            return f"fleet_version_distribution error: {exc}"

    # ── fleet_cbse_impact ──────────────────────────────────────────────────────
    if name == "fleet_cbse_impact":
        try:
            rows = _fleet_cbse_impact(
                cb_url, bucket, username, password, use_tls, scope, collection,
                limit=args.get("limit", 20),
            )
            if not rows:
                return "No CBSE impact data."
            lines = ["| CBSE | Orgs | Tickets |", "|------|------|---------|"]
            for r in rows:
                lines.append(
                    f"| {r.get('cbse_id','')} | {r.get('org_count',0)} | {r.get('ticket_count',0)} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"fleet_cbse_impact error: {exc}"

    # ── get_digest ─────────────────────────────────────────────────────────────
    if name == "get_digest":
        org = args.get("organization") or default_customer
        if not org:
            return "organization is required."
        try:
            result = _get_digest(
                org, cb_url, bucket, username, password, use_tls, scope, collection,
                hours=args.get("hours", 48),
            )
            return str(result)
        except Exception as exc:
            return f"get_digest error: {exc}"

    # ── tag_ticket ─────────────────────────────────────────────────────────────
    if name == "tag_ticket":
        ticket_id = str(args.get("ticket_id", "")).strip()
        tags = args.get("tags", [])
        if not ticket_id:
            return "ticket_id is required."
        try:
            result = _tag_ticket_in_cb(
                ticket_id, tags, cb_url, bucket, username, password, use_tls, scope, collection,
            )
            return str(result)
        except Exception as exc:
            return f"tag_ticket error: {exc}"

    # ── save_query ─────────────────────────────────────────────────────────────
    if name == "save_query":
        try:
            result = _save_query_to_cb(
                args.get("name", ""),
                args.get("query", "") or args.get("question", ""),
                args.get("organization", "") or default_customer,
                cb_url, bucket, username, password, use_tls, scope, collection,
            )
            return str(result)
        except Exception as exc:
            return f"save_query error: {exc}"

    # ── list_saved_queries ─────────────────────────────────────────────────────
    if name == "list_saved_queries":
        try:
            rows = _list_saved_queries(cb_url, bucket, username, password, use_tls, scope, collection)
            if not rows:
                return "No saved queries."
            lines = []
            for r in rows:
                lines.append(f"- **{r.get('name','')}** ({r.get('organization','')}): {r.get('query','')[:80]}")
            return "\n".join(lines)
        except Exception as exc:
            return f"list_saved_queries error: {exc}"

    # ── save_artifact ──────────────────────────────────────────────────────────
    if name == "save_artifact":
        try:
            result = _save_asset_to_cb(args, cb_url, bucket, username, password, use_tls, scope)
            return str(result)
        except Exception as exc:
            return f"save_artifact error: {exc}"

    # ── get_current_time ───────────────────────────────────────────────────────
    if name == "get_current_time":
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    # ── get_cluster_health ─────────────────────────────────────────────────────
    if name == "get_cluster_health":
        org = args.get("organization") or default_customer
        if not org:
            return "organization is required."
        try:
            result = _compute_health_score_with_cluster(
                org, cb_url, bucket, username, password, use_tls, scope, collection,
                args.get("snap_collection", "snapshots"),
            )
            return json.dumps(result, indent=2)
        except Exception as exc:
            return f"get_cluster_health error: {exc}"

    # ── get_customer_health_score ──────────────────────────────────────────────
    if name == "get_customer_health_score":
        org = args.get("organization") or default_customer
        if not org:
            return "organization is required."
        try:
            result = _compute_health_score(org, cb_url, bucket, username, password, use_tls, scope, collection)
            return json.dumps(result, indent=2)
        except Exception as exc:
            return f"get_customer_health_score error: {exc}"

    # ── generate_customer_report ───────────────────────────────────────────────
    if name == "generate_customer_report":
        org = args.get("organization") or default_customer
        if not org:
            return "organization is required."
        try:
            return _generate_customer_report(org, cb_url, bucket, username, password, use_tls, scope, collection)
        except Exception as exc:
            return f"generate_customer_report error: {exc}"

    # ── get_fleet_status ───────────────────────────────────────────────────────
    if name == "get_fleet_status":
        try:
            rows = _query_fleet_tickets(cb_url, bucket, username, password, use_tls, scope, collection)
            if not rows:
                return "No fleet data available."
            lines = ["| Org | Tickets | P1 | P2 |", "|-----|---------|----|----|"]
            for r in rows[:30]:
                lines.append(
                    f"| {r.get('label','')} "
                    f"| {r.get('ticket_count',0)} "
                    f"| {r.get('p1_count',0)} "
                    f"| {r.get('p2_count',0)} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"get_fleet_status error: {exc}"

    # ── list_at_risk_clusters ──────────────────────────────────────────────────
    if name == "list_at_risk_clusters":
        try:
            rows = _list_at_risk_clusters(cb_url, bucket, username, password, use_tls, scope)
            if not rows:
                return "No at-risk clusters found."
            lines = ["| Cluster | Org | Version | Risk |", "|---------|-----|---------|------|"]
            for r in rows[:20]:
                lines.append(
                    f"| {r.get('cluster_name','')} "
                    f"| {r.get('organization','')} "
                    f"| {r.get('cb_version','')} "
                    f"| {r.get('risk_score',0)} |"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"list_at_risk_clusters error: {exc}"

    # ── record_feedback ────────────────────────────────────────────────────────
    if name == "record_feedback":
        try:
            if _CB_AVAILABLE:
                from datetime import timezone
                conn_str = _cb_conn_str(cb_url, use_tls)
                from couchbase.cluster import Cluster, ClusterOptions
                from couchbase.auth import PasswordAuthenticator
                from datetime import timedelta
                cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
                cluster.wait_until_ready(timedelta(seconds=10))
                try:
                    col = cluster.bucket(bucket).scope(scope).collection("feedback")
                except Exception:
                    col = cluster.bucket(bucket).default_collection()
                doc = {
                    "type": "feedback",
                    "text": args.get("text", "") or args.get("feedback", ""),
                    "rating": args.get("rating"),
                    "organization": args.get("organization") or default_customer,
                    "created_at": datetime.datetime.utcnow().isoformat(),
                }
                col.upsert(f"feedback::{uuid.uuid4()}", doc)
            return "Feedback recorded."
        except Exception as exc:
            return f"record_feedback error: {exc}"

    # ── SFDC tools ────────────────────────────────────────────────────────────
    if name == "get_account_opportunities":
        org = args.get("organization") or default_customer
        if not org:
            return "organization is required."
        try:
            from supportal.sfdc_sync import query_account_opportunities
            return query_account_opportunities(org, cb_url, bucket, username, password, use_tls, scope)
        except Exception as exc:
            return f"get_account_opportunities error: {exc}"

    if name == "get_se_opportunities":
        se = args.get("se_name", "")
        if not se:
            return "se_name is required."
        try:
            from supportal.sfdc_sync import query_se_opportunities
            return query_se_opportunities(se, cb_url, bucket, username, password, use_tls, scope)
        except Exception as exc:
            return f"get_se_opportunities error: {exc}"

    if name == "list_sfdc_accounts":
        se = args.get("se_name", "")
        try:
            from supportal.sfdc_sync import query_sfdc_accounts
            return query_sfdc_accounts(se, cb_url, bucket, username, password, use_tls, scope)
        except Exception as exc:
            return f"list_sfdc_accounts error: {exc}"

    if name == "get_sfdc_field_mapping":
        try:
            from supportal.sfdc_sync import get_field_mapping_text
            return get_field_mapping_text(cb_url, bucket, username, password, use_tls, scope)
        except Exception as exc:
            return f"get_sfdc_field_mapping error: {exc}"

    if name == "update_sfdc_field_mapping":
        key = args.get("logical_key", "")
        val = args.get("sfdc_value", "")
        if not key or not val:
            return "logical_key and sfdc_value are both required."
        try:
            from supportal.sfdc_sync import update_field_mapping_entry
            return update_field_mapping_entry(key, val, cb_url, bucket, username, password, use_tls, scope)
        except Exception as exc:
            return f"update_sfdc_field_mapping error: {exc}"

    if name == "sync_sfdc_data":
        try:
            import threading
            from supportal.sfdc_sync import sync_all
            threading.Thread(target=sync_all, daemon=True).start()
            return "SFDC sync started in the background. Ask me for account data in a few minutes once it completes."
        except Exception as exc:
            return f"sync_sfdc_data error: {exc}"

    if name == "get_my_sfdc_accounts":
        try:
            s = _load_settings()
            prof = _active_profile(s)
            se_name = prof.get("sfdc_user_name", "").strip()
            if not se_name:
                return (
                    "No Salesforce identity is configured. "
                    "Open Settings → Salesforce → 'Look up from credentials' to set your name, then ask again."
                )
            from supportal.sfdc_sync import query_se_opportunities
            return query_se_opportunities(se_name, cb_url, bucket, username, password, use_tls, scope)
        except Exception as exc:
            return f"get_my_sfdc_accounts error: {exc}"

    return f"Tool '{name}' is not available in the unified shell."


import supportal.agent_tools as _at_mod
_at_mod._get_main_app = lambda: sys.modules[__name__]


def _pri_css(priority: str) -> str:
    p = (priority or "").lower()
    if p in ("critical", "urgent", "p1"):
        return "cu-pri-p1"
    if p in ("high", "p2"):
        return "cu-pri-p2"
    return "cu-pri-p3"


def _score_css(score) -> str:
    try:
        s = float(score)
    except (TypeError, ValueError):
        return "cu-score-lo"
    if s >= 7:
        return "cu-score-hi"
    if s >= 4:
        return "cu-score-md"
    return "cu-score-lo"


def _load_customers_bg(cfg: dict):
    # Small delay lets NiceGUI finish build_response before the Couchbase SDK
    # registers asyncio tasks — prevents "dictionary changed size during iteration".
    time.sleep(1.5)
    if not _CB_AVAILABLE:
        return
    try:
        rows = _query_fleet_tickets(
            cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
            cfg["use_tls"], cfg["scope"], cfg["collection"],
            group_by="organization", status_filter="open", limit=500,
        )
        customers: list[tuple] = []
        ticket_orgs: set[str] = set()
        for r in rows:
            label = r.get("label") or ""
            if label:
                customers.append((label, r.get("ticket_count", 0), r.get("p1_count", 0)))
                ticket_orgs.add(label.lower())
        customers.sort(key=lambda x: (-x[2], -x[1]))

        # Load SFDC accounts and merge in any not already represented by ticket data.
        try:
            from couchbase.cluster import Cluster
            from couchbase.options import ClusterOptions, QueryOptions
            from couchbase.auth import PasswordAuthenticator
            from datetime import timedelta as _td
            _conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
            _cl = Cluster(_conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
            _cl.wait_until_ready(_td(seconds=10))
            # After a scoped sync the collection only contains SE-scoped accounts.
            # Filter by se_name/supporting_se_name guards against stale unscoped data.
            _se_name = _active_profile(_load_settings()).get("sfdc_user_name", "").strip()
            _se_filter = (
                f" AND (a.se_name = '{_se_name}' OR a.supporting_se_name = '{_se_name}')"
                if _se_name else ""
            )
            # Ensure opportunities has a primary index (created once, idempotent).
            try:
                _cl.query(
                    f"CREATE PRIMARY INDEX IF NOT EXISTS `#primary_opportunities` "
                    f"ON `{cfg['bucket']}`.`{cfg['scope']}`.`opportunities`"
                ).execute()
            except Exception:
                pass
            acct_rows = list(_cl.query(
                f"SELECT a.org_name, a.org_aliases, a.ae_name, a.se_name, a.csm_name, "
                f"a.arr, a.contract_end_date, a.active_ps_projects, a.account_type "
                f"FROM `{cfg['bucket']}`.`{cfg['scope']}`.`accounts` a "
                f"WHERE a._type = 'account' AND a.org_name IS NOT MISSING{_se_filter} "
                f"ORDER BY a.org_name LIMIT 1000",
                QueryOptions(timeout=_td(seconds=30)),
            ))
            # Index by org_name and every alias (lowercased) for O(1) lookup in detail panel.
            sfdc: dict[str, dict] = {}
            for a in acct_rows:
                n = a.get("org_name", "")
                if n:
                    sfdc[n.lower()] = a
                    for alias in (a.get("org_aliases") or []):
                        if alias:
                            sfdc[alias.lower()] = a
            _SERVER_STATE["sfdc_acct_count"] = len(acct_rows)

            # Opportunity count — query while we have a live CB connection in this thread.
            try:
                opp_rows = list(_cl.query(
                    f"SELECT RAW COUNT(*) "
                    f"FROM `{cfg['bucket']}`.`{cfg['scope']}`.`opportunities` "
                    f"WHERE _type = 'opportunity'",
                    QueryOptions(timeout=_td(seconds=15)),
                ))
                _SERVER_STATE["sfdc_opp_count"] = int(opp_rows[0]) if opp_rows else 0
            except Exception:
                pass

            # Append SFDC-only accounts (no matching ticket org) sorted by name.
            # Use normalized name matching to prevent duplicates like "DaVita" vs "DaVita Inc."
            def _norm_name(s: str) -> str:
                n = s.lower().strip()
                for suf in (' inc.', ' inc', ' llc.', ' llc', ' ltd.', ' ltd',
                            ' corp.', ' corp', ' corporation', ' co.', ' company'):
                    if n.endswith(suf):
                        n = n[:-len(suf)].strip(' ,.')
                        break
                return n.strip(' ,.')

            norm_ticket = {_norm_name(t): t for t in ticket_orgs}
            seen: set[str] = set(ticket_orgs)
            sfdc_only: list[tuple] = []
            for a in acct_rows:
                n = a.get("org_name", "")
                if not n:
                    continue
                n_norm = _norm_name(n)
                matched_ticket = norm_ticket.get(n_norm)
                matched = (
                    n.lower() in seen
                    or any((alias or "").lower() in seen for alias in (a.get("org_aliases") or []))
                    or matched_ticket is not None
                )
                # Cross-index: make SFDC doc reachable via the ticket org name too
                if matched_ticket and matched_ticket not in sfdc:
                    sfdc[matched_ticket] = a
                if not matched:
                    sfdc_only.append((n, 0, 0))
                    seen.add(n.lower())
            sfdc_only.sort(key=lambda x: x[0].lower())
            customers.extend(sfdc_only)
            # Re-set after potential cross-indexing additions
            _SERVER_STATE["sfdc_accounts"] = sfdc
        except Exception:
            pass  # SFDC accounts collection not yet populated — that's fine

        # Merge manually-added extra accounts (additive, separate from SFDC sync).
        # Removing from the extra list never touches SFDC-synced or ticket accounts.
        extra = _load_extra_accounts()
        if extra:
            seen_all = {n.lower() for n, *_ in customers}
            for name in sorted(extra):
                if name.lower() not in seen_all:
                    customers.append((name, 0, 0))
                    seen_all.add(name.lower())

        _SERVER_STATE["customers"] = customers
        _SERVER_STATE["customers_ts"] = time.time()
    except Exception:
        pass


def _load_all_orgs_bg(cfg: dict):
    """Populate _SERVER_STATE['all_orgs'] from CB, with 24h disk cache."""
    time.sleep(2.0)  # Let NiceGUI finish build_response before CB SDK work
    # Check in-memory first
    if _SERVER_STATE["all_orgs"] and (time.time() - _SERVER_STATE["all_orgs_ts"]) < _ORG_CACHE_TTL:
        return
    # Try disk cache
    cached, ts = _load_org_cache()
    if cached and (time.time() - ts) < _ORG_CACHE_TTL:
        _SERVER_STATE["all_orgs"] = cached
        _SERVER_STATE["all_orgs_ts"] = ts
        return
    if not _CB_AVAILABLE:
        return
    try:
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, QueryOptions
        from couchbase.auth import PasswordAuthenticator
        from datetime import timedelta
        conn = cfg["cb_url"] if "://" in cfg["cb_url"] else f"couchbase://{cfg['cb_url']}"
        cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])))
        cl.wait_until_ready(timedelta(seconds=10))
        sql = (
            f"SELECT DISTINCT t.organization AS org "
            f"FROM `{cfg['bucket']}`.`{cfg['scope']}`.`{cfg['collection']}` t "
            f"WHERE t.organization IS NOT NULL AND t.organization != '' "
            f"ORDER BY t.organization"
        )
        rows = [r["org"] for r in cl.query(sql) if r.get("org")]
        _SERVER_STATE["all_orgs"] = rows
        _SERVER_STATE["all_orgs_ts"] = time.time()
        _save_org_cache(rows)
    except Exception:
        pass


# Serve static assets (web components, etc.)
ng_app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


@ng_app.post("/api/agent")
async def api_agent(request: Request):
    """JSON agent endpoint consumed by <cursus-assistant> web component."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"text": "Bad request", "tool": None}, status_code=400)

    text = (body.get("text") or "").strip()
    customer = (body.get("customer") or "").strip()
    history = body.get("history") or []

    if not text:
        return JSONResponse({"text": "", "tool": None})

    settings = _load_settings()
    prof = _active_profile(settings)
    _provider = (prof.get("llm_provider") or "claude").lower().strip()
    _model = (
        prof.get("claude_model") if _provider == "claude"
        else prof.get("lms_model") if _provider == "lmstudio"
        else prof.get("ollama_chat_model") if _provider == "ollama"
        else prof.get("gemini_llm_model") if _provider == "gemini"
        else prof.get("openai_llm_model") if _provider == "openai"
        else None
    ) or "claude-sonnet-4-6"
    _api_key = (prof.get("claude_key") if _provider == "claude"
                else prof.get("gemini_llm_key") if _provider == "gemini"
                else "") or ""
    _base_url = (prof.get("emb_lms_url") if _provider == "lmstudio"
                 else prof.get("emb_ollama_url") if _provider == "ollama"
                 else "") or ""
    cfg_ep = {
        "cb_url": prof.get("cb_url", "localhost"),
        "bucket": prof.get("cb_bucket", "rag"),
        "username": prof.get("cb_user", ""),
        "password": prof.get("cb_pass", ""),
        "use_tls": prof.get("cb_tls", False),
        "scope": prof.get("cb_scope", "transcripts"),
        "collection": prof.get("cb_collection", "supportal"),
        "snap_coll": prof.get("ch_snap_coll", "snapshots"),
        "cookie": prof.get("cookie", ""),
    }

    conv_history = []
    for m in history:
        role = "assistant" if m.get("role") == "agent" else "user"
        txt = (m.get("text") or "").strip()
        if txt and not m.get("streaming"):
            conv_history.append({"role": role, "content": txt})

    tools_used: list[str] = []

    def _tool_cb(name):
        tools_used.append(name)

    # Build messages list for call_llm_with_tools
    from supportal.prompts import build_agent_system_prompt as _bsp
    _pinned = _load_pinned_accounts()
    _profile_hint = ", ".join(_pinned) if _pinned else ""
    sys_content = _bsp(customer=customer or "", profile_hint=_profile_hint)
    msgs = [{"role": "system", "content": sys_content}]
    msgs.extend(conv_history)
    msgs.append({"role": "user", "content": text})

    result_text = ""
    try:
        result_text = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: call_llm_with_tools(
                msgs, _AGENT_TOOLS,
                cfg_ep["cb_url"], cfg_ep["bucket"], cfg_ep["username"], cfg_ep["password"],
                cfg_ep["use_tls"], cfg_ep["scope"], cfg_ep["collection"],
                _provider, _model, _api_key, _base_url,
                default_customer=customer or None,
                status_callback=_tool_cb,
                tool_choice="required",
            )
        )
    except Exception as exc:
        result_text = f"Agent error: {exc}"

    tool_label = " · ".join(
        _TOOL_LABELS.get(t, t.replace("_", " ")) for t in dict.fromkeys(tools_used)
    ) if tools_used else None
    raw = result_text or ""
    try:
        rendered_html = mistune.html(raw)
    except Exception:
        rendered_html = raw
    return JSONResponse({"text": raw, "html": rendered_html, "tool": tool_label})


@ui.page("/")
async def main_page():
    ui.add_head_html(f"<style>{_CSS}</style>")
    ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>')
    ui.add_head_html('<script src="/static/cursus-assistant.js"></script>')

    settings = _load_settings()
    prof = _active_profile(settings)

    _provider = (prof.get("llm_provider") or "claude").lower().strip()
    _model = (
        prof.get("claude_model") if _provider == "claude"
        else prof.get("ollama_chat_model") if _provider == "ollama"
        else prof.get("lms_model") if _provider == "lmstudio"
        else prof.get("gemini_llm_model") if _provider == "gemini"
        else prof.get("openai_llm_model") if _provider == "openai"
        else None
    ) or "claude-sonnet-4-5"
    _api_key = (
        prof.get("claude_key") if _provider == "claude"
        else prof.get("gemini_llm_key") if _provider == "gemini"
        else prof.get("emb_openai_key") if _provider == "openai"
        else ""
    ) or ""
    _base_url = (
        prof.get("emb_ollama_url") if _provider == "ollama"
        else prof.get("emb_lms_url") if _provider == "lmstudio"
        else ""
    ) or ""

    cfg = {
        "cb_url":     prof.get("cb_url", "couchbase://localhost"),
        "bucket":     prof.get("cb_bucket", "rag"),
        "username":   prof.get("cb_user", ""),
        "password":   prof.get("cb_pass", ""),
        "use_tls":    prof.get("cb_tls", False),
        "scope":      prof.get("cb_scope", "transcripts"),
        "collection": prof.get("cb_collection", "tickets"),
        "snap_coll":  prof.get("ch_snap_coll", "snapshots"),
        "provider":   _provider,
        "model":      _model,
        "api_key":    _api_key,
        "base_url":   _base_url,
        "cookie":     prof.get("cookie", ""),
    }

    ps = {
        "tab": "overview",
        "customer": "",
        "t_filter": {"priority": None, "status": None, "q": ""},
        "tickets": [],
        "tickets_ts": 0.0,
        "_menu_open": False,
        "cust_page": 0,
        "cust_page_size": 25,
        "cust_role_filter": "",       # "" = all, "primary", "supporting", "other"
        "_last_detail_customer": None,
        "flyout_ticket": None,        # full ticket row dict when flyout is open
        "tickets_sort_col": "created",
        "tickets_sort_dir": "desc",   # "asc" or "desc"
    }

    refs = {}

    def _cb_cfg():
        return cfg

    def _prime_assistant(prompt: str):
        safe = prompt.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
        ui.run_javascript(f'document.getElementById("cursus-ai")?.ask(`{safe}`)')

    def _pick_customer(name: str):
        ps["customer"] = name
        ps["_last_detail_customer"] = None  # force detail re-render
        _refresh_customer_pill()
        _refresh_overview()
        _refresh_customers_tab()
        if name:
            _prime_assistant(
                f"Call query_tickets for organization='{name}' with status=open. "
                f"Return the full tool result table without reformatting. "
                f"Then in 1-2 sentences describe the current support health for {name}."
            )

    def _go_tickets(priority=None, status=None):
        ps["tab"] = "tickets"
        ps["t_filter"]["priority"] = priority
        ps["t_filter"]["status"] = status
        _set_tab("tickets")
        _load_tickets()

    def _set_tab(tab_id: str):
        ps["tab"] = tab_id
        for tid, el in refs.get("tab_panels", {}).items():
            if tid == tab_id:
                el.classes(remove="cu-tab-hidden")
            else:
                el.classes(add="cu-tab-hidden")
        for tid, btn in refs.get("nav_tabs", {}).items():
            if tid == tab_id:
                btn.classes(add="active")
            else:
                btn.classes(remove="active")
        if tab_id == "tickets":
            _load_tickets()
        if tab_id == "customers":
            _refresh_customers_tab()
        if tab_id == "overview":
            _refresh_overview()
        if tab_id == "reports":
            _refresh_reports()
        if tab_id == "data":
            _refresh_data_freshness()

    # Priority label → CB values (query uses UPPER(priority) IN [...] so these must be uppercase)
    _PRI_MAP = {
        "P1": ["URGENT", "CRITICAL", "P1"],
        "P2": ["HIGH", "P2"],
        "P3": ["NORMAL", "LOW", "P3"],
    }

    def _load_tickets():
        if not _CB_AVAILABLE:
            return
        filters = {}
        if ps["customer"]:
            filters["organization"] = ps["customer"]
        pri = ps["t_filter"].get("priority")
        if pri:
            filters["priorities"] = _PRI_MAP.get(pri.upper(), [pri.upper()])
        st = ps["t_filter"].get("status")
        if st:
            filters["statuses"] = [st.lower()]
        try:
            rows = tool_query_tickets(
                filters, cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                cfg["use_tls"], cfg["scope"], cfg["collection"], limit=200,
            )
            ps["tickets"] = rows
            ps["tickets_ts"] = time.time()
        except Exception:
            ps["tickets"] = []
        _refresh_tickets_table()

    def _refresh_customer_pill():
        el = refs.get("cust_label")
        if el:
            el.set_content(ps["customer"] or "All Customers")
        cust = ps["customer"] or ""
        safe = cust.replace('"', '\\"')
        ui.run_javascript(f'document.getElementById("cursus-ai")?.setAttribute("customer", "{safe}")')
        lbl = refs.get("tickets_scope_label")
        if lbl:
            lbl.set_content(
                f'<span class="cu-section-sub">Scoped to {cust}</span>'
                if cust else '<span class="cu-section-sub">All accounts</span>'
            )
        acct = refs.get("tickets_acct_label")
        if acct:
            acct.set_content(
                f'<div class="cu-summary-text">{cust}</div>'
                if cust else '<div class="cu-summary-text">All customers — select from top bar to scope</div>'
            )
        # Show/hide the "× All Customers" clear button
        clear_btn = refs.get("clear_scope_btn")
        if clear_btn:
            if cust:
                clear_btn.classes(remove="cu-tab-hidden")
            else:
                clear_btn.classes(add="cu-tab-hidden")

    def _refresh_overview():
        overview_kpi = refs.get("overview_kpi")
        if not overview_kpi:
            return
        overview_kpi.clear()

        fleet_rows = []
        p1_total = 0
        open_total = 0
        at_risk_count = 0
        pinned = _load_pinned_accounts()

        try:
            fleet_rows = _query_fleet_tickets(
                cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                cfg["use_tls"], cfg["scope"], cfg["collection"],
                group_by="organization", status_filter="open", limit=500,
            )
            open_total = sum(r.get("ticket_count", 0) for r in fleet_rows)
            p1_total = sum(r.get("p1_count", 0) for r in fleet_rows)
        except Exception:
            pass

        try:
            at_risk_rows = _list_at_risk_clusters(
                cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                cfg["use_tls"], cfg["scope"],
            )
            at_risk_count = len(at_risk_rows)
        except Exception:
            pass

        # Counts populated by _load_customers_bg (runs in a thread, safe for blocking CB calls).
        opp_count  = _SERVER_STATE["sfdc_opp_count"]
        acct_count = _SERVER_STATE["sfdc_acct_count"] or len(_SERVER_STATE["customers"])

        with overview_kpi:
            kpi_defs = [
                ("Open Tickets", open_total, "#ea2328",
                 "Call query_tickets with status=open and no organization filter. "
                 "Return the tool result table directly — do not reformat, rename columns, or summarize. "
                 "Then group the rows under bold ### Organization headers and add a one-line count per org."),
                ("Open P1s", p1_total, "#e53935",
                 "Call query_tickets with status=open and priority=urgent. "
                 "Return the full tool result table without reformatting or renaming columns. "
                 "Group rows under bold ### Organization headers."),
                ("At-Risk Clusters", at_risk_count, "#c98a12", "Which clusters are at risk?"),
                ("Accounts", acct_count or len(fleet_rows), "#3c4046", "Give me a portfolio status overview"),
                ("Opportunities", opp_count, "#0050a0", "Show me my open Salesforce opportunities"),
            ]
            for label, value, color, prompt in kpi_defs:
                def _make_kpi_click(p=prompt):
                    return lambda: _prime_assistant(p)
                with ui.element("div").classes("cu-kpi").style(
                    f"border-top:3px solid {color};"
                ).on("click", _make_kpi_click()):
                    ui.html(f'<div class="cu-kpi-label">{label}</div>')
                    ui.html(f'<div class="cu-kpi-value" style="color:{color};">{value}</div>')

        bar_container = refs.get("bar_chart_container")
        if bar_container:
            bar_container.clear()
            sfdc_idx_ov = _SERVER_STATE.get("sfdc_accounts") or {}
            _se_name_ov = (_load_sfdc_creds().get("user_name") or "").strip().lower()
            with bar_container:
                # Use the already-deduplicated customers list as the single source of truth.
                # Enrich with live ticket counts from fleet_rows.
                all_custs_ov = _SERVER_STATE["customers"]  # (name, ticket_count, p1_count)
                active_orgs = {(r.get("label") or "").lower(): r for r in fleet_rows}

                def _ov_role(name: str, adoc: dict) -> str:
                    se_n  = (adoc.get("se_name") or "").strip().lower()
                    sup_n = (adoc.get("supporting_se_name") or "").strip().lower()
                    if _se_name_ov and se_n == _se_name_ov:
                        return "primary"
                    if _se_name_ov and sup_n == _se_name_ov:
                        return "supporting"
                    if name in pinned:
                        return "pinned"
                    return "other"

                # Sort: primary first, supporting second, then by ticket count
                def _ov_sort(item):
                    name, cnt, p1 = item
                    adoc = sfdc_idx_ov.get(name.lower()) or {}
                    role = _ov_role(name, adoc)
                    order = {"primary": 0, "supporting": 1, "pinned": 2, "other": 3}
                    return (order.get(role, 3), -(cnt or 0))

                sorted_custs = sorted(all_custs_ov, key=_ov_sort)

                max_count = max((cnt for _, cnt, _ in sorted_custs), default=1) or 1
                for name, _base_cnt, _base_p1 in sorted_custs:
                    key = name.lower()
                    tr = active_orgs.get(key)
                    cnt = tr.get("ticket_count", _base_cnt) if tr else _base_cnt
                    p1c = tr.get("p1_count", _base_p1) if tr else _base_p1
                    pct = int((cnt / max_count) * 100) if max_count and cnt else 0
                    adoc = sfdc_idx_ov.get(key) or {}
                    role = _ov_role(name, adoc)

                    # Bar fill: P1 = red, primary = green, supporting = blue, else gray
                    if p1c > 0:
                        bar_color = "#ea2328"
                    elif role == "primary":
                        bar_color = "#4ec27f"
                    elif role == "supporting":
                        bar_color = "#4a90e2"
                    elif cnt == 0:
                        bar_color = "#5f6776"
                    else:
                        bar_color = "#9a9ea6"

                    row_style = ""
                    if role == "primary":
                        row_style = "border-left:3px solid #4ec27f;padding-left:9px;"
                    elif role == "supporting":
                        row_style = "border-left:3px solid #4a90e2;padding-left:9px;"

                    def _make_bar_click(n=name):
                        return lambda: _pick_customer(n)

                    with ui.element("div").classes("cu-bar-row").style(row_style).on("click", _make_bar_click()):
                        with ui.element("div").classes("cu-bar-label").props(f'title="{name}"'):
                            if role == "primary":
                                prefix = '<span class="cu-bar-role-dot" style="background:#4ec27f;"></span>'
                            elif role == "supporting":
                                prefix = '<span class="cu-bar-role-dot" style="background:#4a90e2;"></span>'
                            elif role == "pinned":
                                prefix = '<span style="color:var(--cbr);margin-right:4px;font-size:10px;">●</span>'
                            else:
                                prefix = ""
                            ui.html(f'{prefix}{name}')
                        with ui.element("div").classes("cu-bar-track"):
                            if cnt > 0:
                                ui.element("div").classes("cu-bar-fill").style(f"width:{pct}%;background:{bar_color};")
                        with ui.element("div").classes("cu-bar-count"):
                            ui.html(str(cnt) if cnt > 0 else "—")

                # Legend as DOM elements, not inline HTML text
                with ui.element("div").style(
                    "display:flex;align-items:center;gap:10px;margin-top:8px;flex-wrap:wrap;"
                ):
                    ui.html(
                        '<span style="font-family:\'IBM Plex Mono\',monospace;font-size:11px;color:var(--t4);">'
                        'Click a bar to scope'
                        + (f' · {len(pinned)} pinned' if pinned else '')
                        + '</span>'
                    )
                    if _se_name_ov:
                        for dot_color, dot_label in [("#4ec27f", "Primary"), ("#4a90e2", "Supporting")]:
                            with ui.element("div").style(
                                "display:flex;align-items:center;gap:4px;"
                            ):
                                ui.element("span").style(
                                    f"display:inline-block;width:9px;height:9px;"
                                    f"border-radius:50%;background:{dot_color};flex-shrink:0;"
                                )
                                ui.html(
                                    f'<span style="font-family:\'IBM Plex Mono\',monospace;'
                                    f'font-size:11px;color:{dot_color};">{dot_label}</span>'
                                )

        donut_el = refs.get("donut_container")
        if donut_el:
            donut_el.clear()
            with donut_el:
                with ui.element("div").classes("cu-card-title"):
                    ui.html("Version Distribution")
                version_data = []
                try:
                    version_data = _fleet_version_distribution(
                        cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                        cfg["use_tls"], cfg["scope"],
                    )
                except Exception:
                    pass
                palette = ["#ea2328", "#fb8c00", "#4ec27f", "#bcd4f0", "#c9ccd2", "#9a9ea6"]
                ver_total = sum(r.get("cluster_count", 0) for r in version_data) or 1
                conic_parts = []
                running = 0.0
                for i, vr in enumerate(version_data[:6]):
                    pct_v = (vr.get("cluster_count", 0) / ver_total) * 100
                    color = palette[i % len(palette)]
                    conic_parts.append(f"{color} {running:.1f}% {running + pct_v:.1f}%")
                    running += pct_v
                conic = ", ".join(conic_parts) if conic_parts else "#e0dbd0 0% 100%"
                ui.element("div").style(
                    f"width:100px;height:100px;border-radius:50%;"
                    f"background:conic-gradient({conic});"
                    f"margin:16px auto 12px;"
                )
                for i, vr in enumerate(version_data[:6]):
                    color = palette[i % len(palette)]
                    cnt = vr.get("cluster_count", 0)
                    ver = vr.get("version") or "unknown"
                    with ui.element("div").classes("cu-ver-row"):
                        ui.element("div").classes("cu-ver-dot").style(f"background:{color};")
                        with ui.element("div").classes("cu-ver-name"):
                            ui.html(ver)
                        with ui.element("div").classes("cu-ver-count"):
                            ui.html(str(cnt))
                if not version_data:
                    with ui.element("div").classes("cu-hint"):
                        ui.html("No snapshot data")

    def _refresh_customers_tab():
        org_list_el = refs.get("org_list_container")
        if not org_list_el:
            return
        org_list_el.clear()
        sfdc_idx = _SERVER_STATE.get("sfdc_accounts") or {}
        all_custs = _SERVER_STATE["customers"]
        page_size = ps["cust_page_size"]
        page      = ps["cust_page"]
        total_pages = max(1, (len(all_custs) + page_size - 1) // page_size)
        # Clamp page in case customers list shrank
        ps["cust_page"] = page = min(page, total_pages - 1)
        page_slice = all_custs[page * page_size : (page + 1) * page_size]

        # Resolve current SE identity for role highlighting
        _se_name = (_load_sfdc_creds().get("user_name") or "").strip().lower()
        _pinned_set = set(n.lower() for n in (_load_pinned_accounts() or []))

        # Classify all customers by role
        def _get_role(name: str) -> str:
            key = name.lower()
            doc = sfdc_idx.get(key) or {}
            if _se_name and (doc.get("se_name") or "").strip().lower() == _se_name:
                return "primary"
            if _se_name and (doc.get("supporting_se_name") or "").strip().lower() == _se_name:
                return "supporting"
            if key in _pinned_set:
                return "pinned"
            return "other"

        # Apply role filter before pagination
        role_filter = ps["cust_role_filter"]
        filtered_custs = all_custs
        if role_filter:
            filtered_custs = [c for c in all_custs if _get_role(c[0]) == role_filter]

        total_pages = max(1, (len(filtered_custs) + page_size - 1) // page_size)
        ps["cust_page"] = page = min(page, total_pages - 1)
        page_slice = filtered_custs[page * page_size : (page + 1) * page_size]

        with org_list_el:
            # Show loading state while background thread is still running
            if not all_custs:
                ui.html(
                    '<div style="padding:20px 10px;font-size:13px;color:var(--t4);">'
                    'Loading accounts… this may take a few seconds after page load.'
                    '</div>'
                )
                # Schedule a retry in 3 seconds
                async def _retry_cust():
                    import asyncio as _aio
                    await _aio.sleep(3)
                    _refresh_customers_tab()
                asyncio.ensure_future(_retry_cust())
                _refresh_customers_detail()
                return

            # Role filter chips
            def _set_role_filter(role: str):
                ps["cust_role_filter"] = role
                ps["cust_page"] = 0
                _refresh_customers_tab()

            with ui.element("div").style(
                "display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:8px;"
            ):
                for chip_role, chip_label in [
                    ("", "All"), ("primary", "Primary"),
                    ("supporting", "Supporting"), ("other", "Other / Pinned"),
                ]:
                    chip_on = "on" if ps["cust_role_filter"] == chip_role else ""
                    chip_css = chip_role or "all"
                    def _make_role_chip(r=chip_role):
                        return lambda: _set_role_filter(r)
                    with ui.element("button").classes(f"cu-role-chip {chip_css} {chip_on}").on(
                        "click", _make_role_chip()
                    ).style("font-family:inherit;"):
                        ui.html(chip_label)

            # "All Customers" deselect item
            all_selected = "active" if not ps["customer"] else ""
            with ui.element("div").classes(f"cu-all-custs-item {all_selected}").on(
                "click", lambda: _pick_customer("")
            ):
                ui.html(
                    '<span style="font-size:11px;color:var(--t3);">⊘</span>'
                    '<span style="font-size:12px;color:var(--t2);font-weight:500;">All Customers</span>'
                )

            # Pagination controls
            with ui.element("div").style(
                "display:flex;align-items:center;justify-content:space-between;"
                "padding:8px 0 4px;gap:6px;flex-wrap:wrap;"
            ):
                with ui.element("div").style("display:flex;align-items:center;gap:4px;"):
                    ui.button("‹", on_click=lambda: _cust_page(-1)).props("flat dense").style(
                        "min-width:28px;height:26px;padding:0;font-size:14px;"
                    )
                    ui.html(
                        f'<span style="font-size:11px;color:var(--t3);">'
                        f'Page {page+1}/{total_pages}'
                        f'</span>'
                    )
                    ui.button("›", on_click=lambda: _cust_page(1)).props("flat dense").style(
                        "min-width:28px;height:26px;padding:0;font-size:14px;"
                    )
                # Page-size selector
                ui.select(
                    options=[10, 25, 50, 100],
                    value=page_size,
                    on_change=lambda e: _cust_set_page_size(e.value),
                ).props("dense outlined").style("width:70px;font-size:11px;")
            count_label = f"{len(filtered_custs)} accounts" + (f" (of {len(all_custs)})" if role_filter else "")
            ui.html(
                f'<div style="font-size:10px;color:var(--t4);padding-bottom:4px;">{count_label}</div>'
            )
            for name, total, p1 in page_slice:
                selected = "selected" if name == ps["customer"] else ""
                key = name.lower()
                has_sfdc = key in sfdc_idx
                role = _get_role(name)
                role_class = role if role in ("primary", "supporting") else ""

                def _make_click(n=name):
                    return lambda: _pick_customer(n)
                with ui.element("div").classes(f"cu-org-item {selected} {role_class}").on("click", _make_click()):
                    sfdc_badge = ' <span style="font-size:9px;background:#0050a0;color:#fff;border-radius:3px;padding:1px 4px;vertical-align:middle;">SF</span>' if has_sfdc else ""
                    role_badge = ""
                    if role == "primary":
                        role_badge = ' <span class="cu-role-badge cu-role-primary">Primary</span>'
                    elif role == "supporting":
                        role_badge = ' <span class="cu-role-badge cu-role-supporting">Supporting</span>'
                    elif key in _pinned_set:
                        role_badge = ' <span class="cu-role-badge" style="background:#f4f2ec;color:var(--t3);border:1px solid var(--b1);">Pinned</span>'
                    ui.html(f'<div class="cu-org-name">{name}{sfdc_badge}{role_badge}</div>')
                    if total:
                        ui.html(f'<div class="cu-org-meta">{total} tickets &bull; {p1} P1</div>')
                    else:
                        ui.html('<div class="cu-org-meta" style="color:var(--t4);">SFDC · not yet scraped</div>')

        _refresh_customers_detail()

    def _cust_page(delta: int):
        all_custs = _SERVER_STATE["customers"]
        page_size = ps["cust_page_size"]
        role_filter = ps.get("cust_role_filter", "")
        if role_filter:
            sfdc_idx = _SERVER_STATE.get("sfdc_accounts") or {}
            pinned_set = set(n.lower() for n in (_load_pinned_accounts() or []))
            se_name = (_load_sfdc_creds().get("user_name") or "").strip().lower()
            def _role(name):
                doc = sfdc_idx.get(name.lower()) or {}
                if se_name and (doc.get("se_name") or "").strip().lower() == se_name:
                    return "primary"
                if se_name and (doc.get("supporting_se_name") or "").strip().lower() == se_name:
                    return "supporting"
                if name.lower() in pinned_set:
                    return "pinned"
                return "other"
            count = len([c for c in all_custs if _role(c[0]) == role_filter])
        else:
            count = len(all_custs)
        total_pages = max(1, (count + page_size - 1) // page_size)
        ps["cust_page"] = max(0, min(ps["cust_page"] + delta, total_pages - 1))
        _refresh_customers_tab()

    def _cust_set_page_size(size: int):
        ps["cust_page_size"] = int(size)
        ps["cust_page"] = 0
        _refresh_customers_tab()

    def _refresh_customers_detail(force: bool = False):
        detail = refs.get("cust_detail")
        if detail:
            # Skip expensive CB re-query when the selected customer hasn't changed
            if not force and ps.get("_last_detail_customer") == ps["customer"]:
                return
            ps["_last_detail_customer"] = ps["customer"]
            detail.clear()
            with detail:
                if not ps["customer"]:
                    ui.html('<div class="cu-card"><p style="color:var(--t4);font-size:13px;">Select an account on the left.</p></div>')
                else:
                    try:
                        h = _compute_health_score(
                            ps["customer"], cfg["cb_url"], cfg["bucket"],
                            cfg["username"], cfg["password"],
                            cfg["use_tls"], cfg["scope"], cfg["collection"],
                        )
                        open_c = h.get("open_tickets", 0)
                        p1_c = h.get("open_p1", 0)
                        score = h.get("health_score", 0)
                        score_col = "#ea2328" if score < 40 else "#c98a12" if score < 70 else "#2f8f5b"
                        detail_kpis = [
                            ("Health Score", f"{score:.0f}", score_col),
                            ("Open Tickets", open_c, "#ea2328"),
                            ("Open P1",      p1_c,    "#e53935"),
                        ]
                        with ui.element("div").classes("cu-kpi-grid").style("grid-template-columns:repeat(3,1fr);"):
                            for lbl, val, col in detail_kpis:
                                with ui.element("div").classes("cu-kpi").style(
                                    f"border-top:3px solid {col};"
                                ):
                                    ui.html(f'<div class="cu-kpi-label">{lbl}</div>')
                                    ui.html(f'<div class="cu-kpi-value" style="color:{col};font-size:24px;">{val}</div>')
                    except Exception:
                        ui.html('<div class="cu-card"><p style="font-size:13px;color:var(--t4);">Could not load health data.</p></div>')

                    # SFDC account metadata card
                    _sfdc_idx = _SERVER_STATE.get("sfdc_accounts") or {}
                    _acct = _sfdc_idx.get((ps["customer"] or "").lower())
                    if _acct:
                        with ui.element("div").classes("cu-card").style(
                            "margin-top:12px;border-top:3px solid #0050a0;"
                        ):
                            ui.html(
                                '<div class="cu-card-title" style="display:flex;align-items:center;gap:6px;">'
                                'Salesforce&nbsp;'
                                '<span style="font-size:9px;background:#0050a0;color:#fff;'
                                'border-radius:3px;padding:1px 5px;font-family:\'IBM Plex Mono\',monospace;'
                                'letter-spacing:.02em;">SF</span>'
                                '</div>'
                            )
                            _sf_fields = [
                                ("AE",           _acct.get("ae_name") or "—"),
                                ("SE",           _acct.get("se_name") or "—"),
                                ("CSM",          _acct.get("csm_name") or "—"),
                                ("ARR",          f"${_acct.get('arr') or 0:,.0f}" if _acct.get("arr") else "—"),
                                ("Contract End", (_acct.get("contract_end_date") or "—")[:10]),
                                ("Account Type", _acct.get("account_type") or "—"),
                                ("PS Projects",  str(_acct.get("active_ps_projects") or 0)),
                            ]
                            # CSS grid ensures label and value never run together
                            ui.html(
                                '<div style="display:grid;grid-template-columns:100px 1fr;'
                                'row-gap:5px;column-gap:10px;font-size:12px;margin-top:4px;">'
                                + "".join(
                                    f'<span style="color:var(--t4);font-family:\'IBM Plex Mono\',monospace;'
                                    f'font-size:10px;text-transform:uppercase;letter-spacing:.04em;'
                                    f'padding-top:2px;">{_lbl}</span>'
                                    f'<span style="font-weight:500;color:var(--t1);">{_val}</span>'
                                    for _lbl, _val in _sf_fields
                                )
                                + '</div>'
                            )

                    try:
                        rows = tool_query_tickets(
                            {"organization": ps["customer"]},
                            cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                            cfg["use_tls"], cfg["scope"], cfg["collection"], limit=5,
                        )
                        if rows:
                            with ui.element("div").classes("cu-card").style(
                                "margin-top:12px;border-top:3px solid var(--cbr);"
                            ):
                                ui.html('<div class="cu-card-title">Recent Open Tickets</div>')
                                for r in rows:
                                    tid = r.get("ticket_id", "")
                                    subj = (r.get("subject") or "")[:60]
                                    pri = r.get("priority", "")
                                    pri_cls = _pri_css(pri)
                                    with ui.element("div").classes("cu-recent-row").on(
                                        "click", lambda t=tid, s=subj: _prime_assistant(f"Tell me about ticket #{t}: {s}")
                                    ):
                                        with ui.element("span").classes("cu-ticket-id"):
                                            ui.html(f"#{tid}")
                                        with ui.element("span").classes("cu-recent-subj"):
                                            ui.html(subj)
                                        with ui.element("span").classes(f"cu-pri-pill {pri_cls}"):
                                            ui.html(pri)
                        else:
                            # No local tickets — offer to scrape
                            with ui.element("div").classes("cu-card").style("margin-top:12px;"):
                                ui.html('<div class="cu-card-title">Tickets</div>')
                                ui.html('<div style="font-size:12px;color:var(--t4);padding:4px 0 8px;">No local tickets yet.</div>')
                                def _scrape_this():
                                    _pick_customer(ps["customer"])
                                    _prime_assistant(f"Scrape support tickets for {ps['customer']} from Supportal, then give me a summary of the open issues.")
                                ui.button("Scrape from Supportal", on_click=_scrape_this).style(
                                    "background:#ea2328;color:#fff;font-size:11px;font-family:inherit;padding:4px 10px;"
                                )
                    except Exception:
                        pass

    def _open_ticket_flyout(row: dict):
        """Populate and open the ticket flyout drawer."""
        ps["flyout_ticket"] = row
        flyout = refs.get("ticket_flyout")
        if not flyout:
            return
        flyout.clear()
        tid   = row.get("ticket_id", "")
        subj  = row.get("subject") or ""
        org   = row.get("organization") or row.get("org") or ""
        pri   = row.get("priority") or "—"
        st    = row.get("status") or "—"
        created  = (row.get("created") or "")[:10]
        updated  = (row.get("updated") or row.get("last_comment_at") or "")[:10]
        assignee = row.get("assignee") or row.get("assignee_name") or "—"
        cb_ver   = row.get("cb_version") or row.get("couchbase_version") or "—"
        tags     = ", ".join(row.get("tags") or []) or "—"
        raw_score = row.get("score") or row.get("complexity_score")
        if isinstance(raw_score, dict):
            score = raw_score.get("overall") or raw_score.get("complexity")
        elif isinstance(raw_score, (int, float)):
            score = raw_score
        else:
            score = None
        score_str = f"{score:.1f}" if isinstance(score, (int, float)) else "—"
        pri_cls = _pri_css(pri)
        supportal_url = f"https://supportal.couchbase.com/agent/tickets/{tid}"

        with flyout:
            # Header
            with ui.element("div").classes("cu-flyout-header"):
                with ui.element("div").classes("cu-flyout-tid"):
                    ui.html(f"#{tid}")
                with ui.element("div").classes("cu-flyout-subj"):
                    ui.html(subj)
                with ui.element("button").classes("cu-flyout-close").on(
                    "click", _close_ticket_flyout
                ):
                    ui.html("✕")

            # Body
            with ui.element("div").classes("cu-flyout-body"):
                ui.html('<div class="cu-flyout-section">Details</div>')
                # Key-value metadata grid
                meta_pairs = [
                    ("Account", f'<span class="cu-acct-chip" style="cursor:pointer;">{org or "—"}</span>'),
                    ("Priority", f'<span class="cu-pri-pill {pri_cls}" style="display:inline-block;">{pri}</span>'),
                    ("Status", st.capitalize()),
                    ("Created", created or "—"),
                    ("Updated", updated or "—"),
                    ("Assignee", assignee),
                    ("CB Version", cb_ver),
                    ("Score", score_str),
                    ("Tags", f'<span style="font-size:11px;color:var(--t3);">{tags}</span>'),
                ]
                grid_html = '<div class="cu-flyout-meta">'
                for k, v in meta_pairs:
                    grid_html += (
                        f'<span class="cu-flyout-key">{k}</span>'
                        f'<span class="cu-flyout-val">{v}</span>'
                    )
                grid_html += '</div>'
                ui.html(grid_html)

                # Description / comments snippet
                desc = (
                    row.get("description")
                    or row.get("latest_comment")
                    or row.get("comments_summary")
                    or ""
                )
                if desc:
                    ui.html('<div class="cu-flyout-section">Description / Latest Comment</div>')
                    ui.html(
                        f'<div class="cu-flyout-desc">{desc[:600]}'
                        + ('…' if len(desc) > 600 else '')
                        + '</div>'
                    )

            # Action buttons
            with ui.element("div").classes("cu-flyout-actions"):
                with ui.element("button").classes("cu-flyout-btn primary").on(
                    "click", lambda: _prime_assistant(
                        f"Tell me about ticket #{tid}: {subj}. "
                        f"Summarize the issue, current status, and recommended next steps."
                    )
                ):
                    ui.html("Ask Assistant")
                if org:
                    def _scope_to_org(o=org):
                        _close_ticket_flyout()
                        _pick_customer(o)
                        _set_tab("customers")
                    with ui.element("button").classes("cu-flyout-btn scope").on(
                        "click", _scope_to_org
                    ):
                        ui.html(f"Scope to {org[:20]}")
                ui.html(
                    f'<a href="{supportal_url}" target="_blank" '
                    f'style="text-decoration:none;">'
                    f'<button class="cu-flyout-btn outline">Open in Supportal ↗</button>'
                    f'</a>'
                )

        # Show backdrop + flyout
        backdrop = refs.get("flyout_backdrop")
        if backdrop:
            backdrop.classes(remove="cu-tab-hidden")
        flyout.classes(add="open")
        _refresh_tickets_table()  # re-render to highlight selected row

    def _close_ticket_flyout():
        ps["flyout_ticket"] = None
        flyout = refs.get("ticket_flyout")
        if flyout:
            flyout.classes(remove="open")
        backdrop = refs.get("flyout_backdrop")
        if backdrop:
            backdrop.classes(add="cu-tab-hidden")
        _refresh_tickets_table()  # remove highlight

    # ── Column sort helpers ────────────────────────────────────────────────────
    _PRI_ORDER = {"urgent": 0, "p1": 0, "high": 1, "p2": 1, "normal": 2, "p3": 2,
                  "low": 3, "p4": 3, "": 9}

    def _ticket_sort_key(row, col):
        if col == "account":
            return (row.get("organization") or row.get("org") or "").lower()
        if col == "priority":
            return _PRI_ORDER.get((row.get("priority") or "").lower(), 9)
        if col == "status":
            return (row.get("status") or "").lower()
        if col == "created":
            return row.get("created") or ""
        if col == "score":
            raw = row.get("score") or row.get("complexity_score")
            if isinstance(raw, dict):
                v = raw.get("overall") or raw.get("complexity") or raw.get("stars")
                if v is None:
                    nums = [raw[k] for k in ("stars", "complexity", "communication_clarity",
                                             "resolution_quality", "response_timeliness")
                            if isinstance(raw.get(k), (int, float))]
                    v = sum(nums) / len(nums) if nums else -1
                return v if isinstance(v, (int, float)) else -1
            return float(raw) if isinstance(raw, (int, float)) else -1
        return row.get("ticket_id", 0)

    def _refresh_tickets_header():
        head = refs.get("tickets_head_el")
        if not head:
            return
        head.clear()
        col_defs = [
            ("id",       "ID",       False),
            ("account",  "Account",  True),
            ("subject",  "Subject",  False),
            ("priority", "Priority", True),
            ("status",   "Status",   True),
            ("created",  "Created",  True),
            ("score",    "Score",    True),
        ]
        cur_col = ps["tickets_sort_col"]
        cur_dir = ps["tickets_sort_dir"]
        with head:
            for col_key, col_label, sortable in col_defs:
                is_active = sortable and col_key == cur_col
                cell_cls = "cu-table-head-cell"
                if sortable:
                    cell_cls += " sortable"
                if is_active:
                    cell_cls += " sort-active"
                arrow = ""
                if sortable:
                    if is_active:
                        arrow = ("▲" if cur_dir == "asc" else "▼")
                    else:
                        arrow = "⇅"

                def _col_click(ck=col_key):
                    if ps["tickets_sort_col"] == ck:
                        ps["tickets_sort_dir"] = "asc" if ps["tickets_sort_dir"] == "desc" else "desc"
                    else:
                        ps["tickets_sort_col"] = ck
                        ps["tickets_sort_dir"] = "desc" if ck in ("created", "score") else "asc"
                    _refresh_tickets_table()

                if sortable:
                    with ui.element("div").classes(cell_cls).on("click", _col_click):
                        ui.html(col_label)
                        arr_cls = f"cu-sort-arrow{' active' if is_active else ''}"
                        with ui.element("span").classes(arr_cls):
                            ui.html(arrow)
                else:
                    with ui.element("div").classes(cell_cls):
                        ui.html(col_label)

    def _refresh_tickets_table():
        _refresh_tickets_header()
        wrap = refs.get("tickets_table_body")
        if not wrap:
            return
        wrap.clear()
        q = (ps["t_filter"].get("q") or "").lower()
        rows = ps["tickets"]
        if q:
            rows = [r for r in rows if q in (r.get("subject") or "").lower()
                    or q in str(r.get("ticket_id", ""))
                    or q in (r.get("organization") or "").lower()]
        # Apply sort
        sc = ps["tickets_sort_col"]
        sd = ps["tickets_sort_dir"]
        try:
            rows = sorted(rows, key=lambda r: _ticket_sort_key(r, sc),
                          reverse=(sd == "desc"))
        except Exception:
            pass
        open_tid = (ps["flyout_ticket"] or {}).get("ticket_id")
        with wrap:
            if not rows:
                ui.html('<div style="padding:20px;color:var(--t4);font-size:13px;">No tickets found.</div>')
                return
            for r in rows[:100]:
                tid = r.get("ticket_id", "")
                subj = (r.get("subject") or "")[:55]
                org  = (r.get("organization") or r.get("org") or "")[:20]
                pri = r.get("priority") or ""
                st = r.get("status") or ""
                created = (r.get("created") or "")[:10]
                raw_score = r.get("score") or r.get("complexity_score")
                if isinstance(raw_score, dict):
                    score = raw_score.get("overall") or raw_score.get("complexity")
                    if score is None:
                        _score_keys = ("communication_clarity", "resolution_quality",
                                       "response_timeliness", "stars", "complexity")
                        nums = [raw_score[k] for k in _score_keys
                                if isinstance(raw_score.get(k), (int, float))]
                        score = sum(nums) / len(nums) if nums else None
                elif isinstance(raw_score, (int, float)):
                    score = raw_score
                else:
                    score = None
                pri_cls = _pri_css(pri)
                sc_cls = _score_css(score)
                score_str = f"{score:.0f}" if isinstance(score, (int, float)) else "—"
                is_open = (tid == open_tid)
                row_extra = "flyout-open" if is_open else ""

                def _row_click(full_row=r):
                    if ps["flyout_ticket"] and ps["flyout_ticket"].get("ticket_id") == full_row.get("ticket_id"):
                        _close_ticket_flyout()
                    else:
                        _open_ticket_flyout(full_row)

                def _acct_click(o=org, e=None):
                    if o:
                        _pick_customer(o)
                        _set_tab("customers")

                with ui.element("div").classes(f"cu-table-row {row_extra}").on("click", _row_click):
                    with ui.element("div").classes("cu-ticket-id"):
                        ui.html(f"#{tid}")
                    # Account column — clickable chip
                    with ui.element("div").classes("cu-acct-chip").on("click.stop", _acct_click):
                        ui.html(org or "—")
                    with ui.element("div").classes("cu-subj-cell"):
                        ui.html(subj)
                    with ui.element("div"):
                        with ui.element("span").classes(f"cu-pri-pill {pri_cls}"):
                            ui.html(pri or "—")
                    with ui.element("div").classes("cu-status-cell"):
                        ui.html(st)
                    with ui.element("div").classes("cu-date-cell"):
                        ui.html(created)
                    with ui.element("div").classes(sc_cls):
                        ui.html(score_str)

    def _refresh_reports():
        container = refs.get("reports_container")
        if not container:
            return
        container.clear()
        saved_assets = []
        try:
            if _CB_AVAILABLE:
                saved_assets = _fleet_query(
                    cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                    cfg["use_tls"], cfg["scope"],
                    f"SELECT a.title, a.org, a.created_at, META(a).id AS doc_id "
                    f"FROM `{cfg['bucket']}`.`{cfg['scope']}`.`assets` a "
                    f"WHERE a.type='asset' ORDER BY a.created_at DESC LIMIT 20"
                )
        except Exception:
            pass

        with container:
            if not saved_assets:
                with ui.element("div").classes("cu-card").style("max-width:600px;"):
                    ui.html(
                        '<div class="cu-card-title">No saved reports</div>'
                        '<div style="font-size:13px;color:var(--t4);">'
                        'Ask the assistant to generate a customer report to create a saved asset.'
                        '</div>'
                    )
            else:
                with ui.element("div").classes("cu-card").style("max-width:700px;"):
                    ui.html('<div class="cu-card-title">Saved Reports</div>')
                    for asset in saved_assets:
                        title = asset.get("title") or "Untitled"
                        org = asset.get("org") or ""
                        _ca = asset.get("created_at") or ""
                        if isinstance(_ca, (int, float)):
                            import datetime as _dt
                            _ca = _dt.datetime.fromtimestamp(_ca).strftime("%Y-%m-%d")
                        created = str(_ca)[:10]

                        def _make_report_click(t=title, o=org):
                            return lambda: _prime_assistant(f"Show me the report for {o}: {t}")

                        with ui.element("div").style(
                            "display:flex;align-items:center;gap:10px;padding:8px 0;"
                            "border-bottom:1px solid var(--b2);"
                        ).on("click", _make_report_click()):
                            ui.html(
                                f'<div style="flex:1;">'
                                f'<div style="font-size:13px;font-weight:600;color:var(--t1);">{title}</div>'
                                f'<div style="font-size:11px;color:var(--t4);">{org} &bull; {created}</div>'
                                f'</div>'
                            )

    def _refresh_data_freshness():
        container = refs.get("freshness_container")
        if not container:
            return
        container.clear()
        rows = []
        err = None
        try:
            if _CB_AVAILABLE:
                rows = _fleet_ticket_freshness(
                    cfg["cb_url"], cfg["bucket"], cfg["username"], cfg["password"],
                    cfg["use_tls"], cfg["scope"], cfg["collection"],
                )
        except Exception as exc:
            err = str(exc)

        now = datetime.datetime.utcnow()

        def _parse_days(ts):
            if not ts:
                return None
            try:
                dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
                delta = now - dt.replace(tzinfo=None)
                return max(0, delta.days)
            except Exception:
                return None

        def _badge(days):
            if days is None:
                return "cu-badge-unknown", "Unknown"
            if days < 3:
                return "cu-badge-fresh", "Fresh"
            if days < 14:
                return "cu-badge-aging", "Aging"
            return "cu-badge-stale", "Stale"

        with container:
            if err:
                with ui.element("div").classes("cu-hint"):
                    ui.html(f"Could not load freshness data: {err}")
                return

            if not rows:
                with ui.element("div").classes("cu-hint"):
                    ui.html("No ticket data available.")
                return

            # Header row
            with ui.element("div").classes("cu-fresh-head"):
                for col in ["Account", "Last Activity", "Age", "Status", ""]:
                    ui.html(col)

            with ui.element("div").classes("cu-fresh-table"):
                for r in rows:
                    org = r.get("organization") or "—"
                    last_act = r.get("last_activity")
                    days = _parse_days(last_act)
                    badge_cls, badge_txt = _badge(days)

                    if last_act:
                        try:
                            dt = datetime.datetime.fromisoformat(last_act.replace("Z", "+00:00"))
                            date_str = dt.strftime("%Y-%m-%d")
                        except Exception:
                            date_str = last_act[:10] if len(last_act) >= 10 else last_act
                    else:
                        date_str = "—"

                    age_str = f"{days}d ago" if days is not None else "—"

                    def _make_ask(o=org, d=days, la=last_act):
                        age_hint = f"{d} days ago ({la[:10]})" if d is not None and la else "unknown date"
                        prompt = (
                            f"Check ticket activity for {o}. Last recorded update was {age_hint}. "
                            f"Summarise open tickets, highlight any P1s, and tell me if a rescrape is needed."
                        )
                        return lambda: _prime_assistant(prompt)

                    with ui.element("div").classes("cu-fresh-row"):
                        with ui.element("div").classes("cu-fresh-org").props(f'title="{org}"'):
                            ui.html(org)
                        with ui.element("div").classes("cu-fresh-date"):
                            ui.html(date_str)
                        with ui.element("div").classes("cu-fresh-age"):
                            ui.html(age_str)
                        with ui.element("div").classes(f"cu-fresh-badge {badge_cls}"):
                            ui.html(badge_txt)
                        with ui.element("button").classes("cu-fresh-ask").on("click", _make_ask()):
                            ui.html("Ask ↗")

            with ui.element("div").classes("cu-hint").style("margin-top:10px;"):
                ui.html(
                    "Age = days since the most recent ticket update in Zendesk. "
                    "Click <strong>Ask ↗</strong> to get a full summary, spot P1s, or trigger a rescrape. "
                    "You can also type: <em>'Show me ticket activity for [org] this week'</em> or "
                    "<em>'Rescrape [org] and report what's changed.'</em>"
                )

    # ── Top bar ────────────────────────────────────────────────────────────────
    with ui.element("div").classes("cu-header"):
        ui.html('<div class="cu-logo-mark">C</div>')
        ui.html('<div class="cu-wordmark">Cursus</div>')

        with ui.element("div").classes("cu-nav"):
            nav_tabs = {}
            for tab_id, label in [
                ("overview", "Overview"),
                ("customers", "Customers"),
                ("tickets", "Tickets"),
                ("data", "Data"),
                ("reports", "Reports"),
            ]:
                def _make_tab_click(t=tab_id):
                    return lambda: _set_tab(t)
                is_active = "active" if tab_id == "overview" else ""
                btn = ui.element("button").classes(f"cu-nav-tab {is_active}").on("click", _make_tab_click())
                with btn:
                    ui.html(label)
                nav_tabs[tab_id] = btn
            refs["nav_tabs"] = nav_tabs

        ui.element("div").classes("cu-spacer")

        # Customer pill + dropdown
        with ui.element("div").classes("cu-cust-pill").style("position:relative"):
            ui.html('<div class="cu-cust-dot"></div>')
            cust_label = ui.html("All Customers")
            refs["cust_label"] = cust_label
            ui.html('<span style="color:var(--ton-d2);font-size:11px;margin-left:2px;">&#9660;</span>')

            with ui.element("div").classes("cu-cust-menu cu-tab-hidden") as cust_menu_el:
                refs["cust_menu"] = cust_menu_el

            def _rebuild_cust_menu():
                """Repopulate the dropdown from current pinned + active accounts."""
                cust_menu_el.clear()
                pinned_now = _load_pinned_accounts()
                active = {(r[0] or "").lower(): r for r in (_SERVER_STATE["customers"] or [])}

                def _item_click(n):
                    def _do():
                        cust_menu_el.classes(add="cu-tab-hidden")
                        ps["_menu_open"] = False
                        _pick_customer(n)
                    return _do

                with cust_menu_el:
                    # Search box (JS-filtered)
                    ui.element("input").classes("cu-cust-menu-search").props(
                        'placeholder="Search accounts…" id="cu-org-search"'
                    )
                    ui.run_javascript("""
                      (function(){
                        var inp = document.getElementById('cu-org-search');
                        if (!inp) return;
                        inp.addEventListener('input', function(){
                          var q = inp.value.toLowerCase();
                          document.querySelectorAll('.cu-cust-menu-item[data-org]').forEach(function(el){
                            el.style.display = (!q || el.dataset.org.includes(q)) ? '' : 'none';
                          });
                          document.querySelectorAll('.cu-cust-menu-hdr[data-section]').forEach(function(hdr){
                            var sec = hdr.dataset.section;
                            var items = document.querySelectorAll('.cu-cust-menu-item[data-section="'+sec+'"]');
                            var anyVisible = Array.from(items).some(function(i){ return i.style.display !== 'none'; });
                            hdr.style.display = anyVisible ? '' : 'none';
                          });
                        });
                        inp.addEventListener('click', function(e){ e.stopPropagation(); });
                      })();
                    """)

                    # Pinned section
                    if pinned_now:
                        with ui.element("div").classes("cu-cust-menu-hdr").props('data-section="pinned"'):
                            ui.html("PINNED")
                        for pname in pinned_now:
                            arow = active.get(pname.lower())
                            p1c = arow[2] if arow else 0
                            cnt = arow[1] if arow else 0
                            with ui.element("div").classes("cu-cust-menu-item").props(
                                f'data-org="{pname.lower()}" data-section="pinned"'
                            ).on("click", _item_click(pname)):
                                ui.html('<span class="cu-pin-dot"></span>')
                                with ui.element("span"):
                                    ui.html(pname)
                                with ui.element("span").classes("cu-cust-menu-p1"):
                                    ui.html(f"{p1c} P1" if p1c else (f"{cnt} open" if cnt else "quiet"))

                    # Active accounts section
                    active_names = [r[0] for r in (_SERVER_STATE["customers"] or [])
                                    if r[0] and r[0] not in pinned_now]
                    if active_names:
                        with ui.element("div").classes("cu-cust-menu-hdr").props('data-section="active"'):
                            ui.html("ACTIVE")
                        for aname in active_names[:12]:
                            arow = active.get(aname.lower())
                            p1c = arow[2] if arow else 0
                            cnt = arow[1] if arow else 0
                            with ui.element("div").classes("cu-cust-menu-item").props(
                                f'data-org="{aname.lower()}" data-section="active"'
                            ).on("click", _item_click(aname)):
                                with ui.element("span"):
                                    ui.html(aname)
                                with ui.element("span").classes("cu-cust-menu-p1"):
                                    ui.html(f"{p1c} P1" if p1c else f"{cnt} open")

                    # All customers section (from full org cache)
                    all_orgs = _SERVER_STATE.get("all_orgs") or []
                    shown = {n.lower() for n in pinned_now} | {n.lower() for n in active_names}
                    rest = [o for o in all_orgs if o.lower() not in shown]
                    if rest:
                        with ui.element("div").classes("cu-cust-menu-hdr").props('data-section="all"'):
                            ui.html("ALL ACCOUNTS")
                        for oname in rest:
                            with ui.element("div").classes("cu-cust-menu-item").props(
                                f'data-org="{oname.lower()}" data-section="all"'
                            ).on("click", _item_click(oname)):
                                with ui.element("span"):
                                    ui.html(oname)

            refs["rebuild_cust_menu"] = _rebuild_cust_menu

            def _toggle_menu():
                ps["_menu_open"] = not ps["_menu_open"]
                if ps["_menu_open"]:
                    _rebuild_cust_menu()
                    cust_menu_el.classes(remove="cu-tab-hidden")
                    ui.run_javascript("setTimeout(()=>{ var s=document.getElementById('cu-org-search'); if(s) s.focus(); }, 60);")
                else:
                    cust_menu_el.classes(add="cu-tab-hidden")

            ui.element("div").classes("cu-cust-pill").style("position:absolute;inset:0;").on("click", _toggle_menu)

        # "× All Customers" clear button — only visible when an account is scoped
        with ui.element("div").classes("cu-clear-scope cu-tab-hidden") as clear_scope_btn:
            ui.html("× All Customers")
        refs["clear_scope_btn"] = clear_scope_btn
        clear_scope_btn.on("click", lambda: _pick_customer(""))

        # Gear / settings button
        with ui.element("button").classes("cu-gear-btn").on("click", lambda: refs["settings_dialog"].open()):
            ui.html("⚙")

        with ui.element("div").classes("cu-cb-status"):
            ui.html('<div class="cu-cb-dot"></div>')
            ui.html('<div class="cu-cb-label">Couchbase</div>')

    # ── Body ───────────────────────────────────────────────────────────────────
    with ui.element("div").classes("cu-body"):

        # ── Main canvas ────────────────────────────────────────────────────────
        with ui.element("div").classes("cu-canvas"):

            tab_panels = {}

            # ── Overview tab ──────────────────────────────────────────────────
            with ui.element("div") as overview_panel:
                tab_panels["overview"] = overview_panel
                ui.html('<div class="cu-section-title">Account Overview</div>')
                ui.html('<div class="cu-section-sub">Account health intelligence across your portfolio</div>')

                # KPI grid — populated by _refresh_overview()
                with ui.element("div").classes("cu-kpi-grid") as overview_kpi_el:
                    refs["overview_kpi"] = overview_kpi_el
                    # Placeholder KPIs shown immediately
                    for label, color in [
                        ("Open Tickets", "#ea2328"),
                        ("Open P1s", "#e53935"),
                        ("At-Risk Clusters", "#c98a12"),
                        ("Accounts", "#3c4046"),
                    ]:
                        with ui.element("div").classes("cu-kpi"):
                            ui.html(f'<div class="cu-kpi-label">{label}</div>')
                            ui.html(f'<div class="cu-kpi-value" style="color:{color};">…</div>')

                with ui.element("div").classes("cu-charts-row"):
                    # Org bar chart
                    with ui.element("div").classes("cu-card"):
                        ui.html('<div class="cu-card-title">Critical &amp; Open Issues by Account</div>')
                        with ui.element("div") as bar_chart_container:
                            refs["bar_chart_container"] = bar_chart_container
                            ui.html('<div style="font-size:12px;color:var(--t4);padding:8px 0;">Loading…</div>')

                    # Version donut — populated by _refresh_overview()
                    with ui.element("div").classes("cu-card") as donut_container:
                        refs["donut_container"] = donut_container

            # ── Customers tab ─────────────────────────────────────────────────
            with ui.element("div").classes("cu-tab-hidden") as customers_panel:
                tab_panels["customers"] = customers_panel
                ui.html('<div class="cu-section-title">Customers</div>')
                ui.html('<div class="cu-section-sub" style="margin-bottom:16px;">Select an account to drill in</div>')

                with ui.element("div").classes("cu-two-col"):
                    with ui.element("div"):
                        with ui.element("div").classes("cu-org-list") as org_list_container:
                            refs["org_list_container"] = org_list_container
                            for name, total, p1 in (_SERVER_STATE["customers"] or []):
                                selected = "selected" if name == ps["customer"] else ""
                                def _make_org_click(n=name):
                                    return lambda: _pick_customer(n)
                                with ui.element("div").classes(f"cu-org-item {selected}").on("click", _make_org_click()):
                                    ui.html(f'<div class="cu-org-name">{name}</div>')
                                    ui.html(f'<div class="cu-org-meta">{total} tickets &bull; {p1} P1</div>')

                    with ui.element("div") as cust_detail:
                        refs["cust_detail"] = cust_detail
                        ui.html('<div class="cu-card"><p style="font-size:13px;color:var(--t4);">Select an account on the left.</p></div>')

            # ── Tickets tab ───────────────────────────────────────────────────
            with ui.element("div").classes("cu-tab-hidden") as tickets_panel:
                tab_panels["tickets"] = tickets_panel
                with ui.element("div").classes("cu-section-hdr"):
                    with ui.element("div").classes("cu-section-title"):
                        ui.html("Tickets")
                    tickets_scope_label = ui.html(
                        f'<span class="cu-section-sub">'
                        + (f"Scoped to {ps['customer']}" if ps["customer"] else "All accounts")
                        + "</span>"
                    )
                    refs["tickets_scope_label"] = tickets_scope_label

                with ui.element("div").classes("cu-filter-bar"):
                    for pri_label, pri_key in [("P1", "P1"), ("P2", "P2"), ("P3", "P3")]:
                        def _make_pri_toggle(pk=pri_key):
                            def _do():
                                if ps["t_filter"]["priority"] == pk:
                                    ps["t_filter"]["priority"] = None
                                else:
                                    ps["t_filter"]["priority"] = pk
                                _load_tickets()
                            return _do
                        btn = ui.element("button").classes(f"cu-filter-chip {pri_label.lower()}").on("click", _make_pri_toggle())
                        with btn:
                            ui.html(pri_label)

                    ui.element("div").classes("cu-divider")

                    for st_label, st_key in [("Open", "open"), ("Pending", "pending"), ("Solved", "solved")]:
                        def _make_st_toggle(sk=st_key):
                            def _do():
                                if ps["t_filter"]["status"] == sk:
                                    ps["t_filter"]["status"] = None
                                else:
                                    ps["t_filter"]["status"] = sk
                                _load_tickets()
                            return _do
                        btn = ui.element("button").classes(f"cu-filter-chip {st_label.lower()}").on("click", _make_st_toggle())
                        with btn:
                            ui.html(st_label)

                    ui.element("div").classes("cu-divider")

                    search_input = ui.input(placeholder="Search…").classes("cu-search").style("border:1px solid var(--b1);border-radius:8px;padding:5px 10px;font-size:12px;")

                    def _on_search(e):
                        ps["t_filter"]["q"] = e.value
                        _refresh_tickets_table()

                    search_input.on("input", _on_search)

                with ui.element("div").classes("cu-summary-banner"):
                    ui.html('<div class="cu-summary-tag">ACCOUNT</div>')
                    tickets_acct_label = ui.html(
                        f'<div class="cu-summary-text">'
                        + (ps["customer"] or "All customers — select from top bar to scope")
                        + "</div>"
                    )
                    refs["tickets_acct_label"] = tickets_acct_label

                with ui.element("div").classes("cu-table-wrap"):
                    with ui.element("div").classes("cu-table-head") as tickets_head_el:
                        refs["tickets_head_el"] = tickets_head_el
                    with ui.element("div") as tickets_table_body:
                        refs["tickets_table_body"] = tickets_table_body
                        ui.html('<div style="padding:20px;color:var(--t4);font-size:13px;">Select a tab or filter to load tickets.</div>')
                ui.timer(0.05, _refresh_tickets_header, once=True)

            # ── Data tab ──────────────────────────────────────────────────────
            with ui.element("div").classes("cu-tab-hidden") as data_panel:
                tab_panels["data"] = data_panel
                ui.html('<div class="cu-section-title">Data</div>')
                ui.html('<div class="cu-section-sub" style="margin-bottom:16px;">Scrape management and freshness</div>')

                # Active configuration card
                with ui.element("div").classes("cu-card").style("max-width:600px;margin-bottom:16px;"):
                    ui.html('<div class="cu-card-title">Active Configuration</div>')
                    config_rows = [
                        ("CB URL", cfg.get("cb_url", "—")),
                        ("Bucket", cfg.get("bucket", "—")),
                        ("Scope", cfg.get("scope", "—")),
                        ("Collection", cfg.get("collection", "—")),
                        ("LLM Provider", cfg.get("provider", "—")),
                        ("Model", cfg.get("model", "—")),
                    ]
                    for k, v in config_rows:
                        with ui.element("div").classes("cu-config-row"):
                            with ui.element("span").classes("cu-config-key"):
                                ui.html(k)
                            with ui.element("span").classes("cu-config-val"):
                                ui.html(str(v))

                with ui.element("div").classes("cu-card").style("max-width:520px;"):
                    ui.html('<div class="cu-card-title">Rescrape Customer</div>')
                    scrape_input = ui.input(
                        label="Customer name",
                        placeholder=ps["customer"] or "e.g. Western Union",
                    ).style("width:100%;margin-bottom:12px;")
                    if ps["customer"]:
                        scrape_input.value = ps["customer"]

                    # Limit selector row
                    with ui.element("div").style("display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap;"):
                        ui.html('<span style="font-size:12px;color:var(--t3);white-space:nowrap;">Ticket limit:</span>')
                        _limit_opts = [
                            ("Freshness only (new since last scrape)", "new"),
                            ("Last 25", "25"),
                            ("Last 50", "50"),
                            ("Last 100", "100"),
                            ("All tickets", "all"),
                        ]
                        scrape_limit_sel = ui.select(
                            options={v: l for l, v in _limit_opts},
                            value="new",
                        ).style("flex:1;min-width:200px;font-size:13px;")

                    scrape_status = ui.html('<div style="font-size:12px;color:var(--t4);">Select a limit and click Rescrape.</div>')

                    def _do_rescrape():
                        name = (scrape_input.value or "").strip()
                        if not name:
                            scrape_status.set_content('<div style="font-size:12px;color:var(--danger);">Enter a customer name first.</div>')
                            return
                        limit_val = scrape_limit_sel.value or "new"
                        if limit_val == "new":
                            limit_desc = "new tickets only (freshness check)"
                            prompt_suffix = "Use smart_refresh to find and pull only new tickets not yet in the local database, then report what changed."
                        elif limit_val == "all":
                            limit_desc = "all tickets"
                            prompt_suffix = "Pull all tickets (full rescrape)."
                        else:
                            limit_desc = f"last {limit_val} tickets"
                            prompt_suffix = f"Limit to the most recent {limit_val} tickets."
                        scrape_status.set_content(f'<div style="font-size:12px;color:var(--amber);">Scrape queued for {name} ({limit_desc})…</div>')
                        _prime_assistant(f"Rescrape and refresh data for {name}. {prompt_suffix}")

                    ui.button("Rescrape", on_click=_do_rescrape).style(
                        "background:var(--cbr);color:white;border:none;border-radius:8px;"
                        "padding:8px 18px;font-size:13px;cursor:pointer;"
                    )

                with ui.element("div").classes("cu-card").style("max-width:800px;margin-top:12px;"):
                    ui.html('<div class="cu-card-title">Data Freshness</div>')
                    with ui.element("div") as freshness_container:
                        refs["freshness_container"] = freshness_container
                        with ui.element("div").classes("cu-hint"):
                            ui.html("Loading freshness data…")

            # ── Reports tab ───────────────────────────────────────────────────
            with ui.element("div").classes("cu-tab-hidden") as reports_panel:
                tab_panels["reports"] = reports_panel
                ui.html('<div class="cu-section-title">Reports</div>')
                ui.html('<div class="cu-section-sub" style="margin-bottom:16px;">Saved assets and published reports</div>')

                with ui.element("div") as reports_container:
                    refs["reports_container"] = reports_container
                    ui.html('<div style="font-size:12px;color:var(--t4);">Loading…</div>')

            refs["tab_panels"] = tab_panels

        # ── Ticket flyout drawer (overlays whole page, outside canvas) ───────
        # Backdrop — click to close
        with ui.element("div").classes("cu-flyout-backdrop cu-tab-hidden") as flyout_backdrop:
            refs["flyout_backdrop"] = flyout_backdrop
        flyout_backdrop.on("click", _close_ticket_flyout)

        # Flyout panel
        with ui.element("div").classes("cu-flyout") as ticket_flyout:
            refs["ticket_flyout"] = ticket_flyout

        # ── Assistant panel (Web Component) ──────────────────────────────────
        # NiceGUI's ui.html() sanitizer strips unknown tags, so we inject the
        # custom element via JS into a plain placeholder div.
        ui.element("div").props('id="cursus-ai-mount"').style(
            "display:contents;"  # transparent in layout — component becomes the flex child
        )

    # Inject <cursus-assistant>, wire .endpoint, and listen for cross-boundary events
    ui.run_javascript("""
(function init() {
  var mount = document.getElementById('cursus-ai-mount');
  if (!mount || !customElements.get('cursus-assistant')) {
    setTimeout(init, 100);
    return;
  }

  // Create and insert the custom element
  var el = document.createElement('cursus-assistant');
  el.id = 'cursus-ai';
  mount.parentNode.insertBefore(el, mount.nextSibling);
  mount.remove();

  // Wire .endpoint to the Python agent API
  el.endpoint = async function(payload) {
    try {
      var resp = await fetch('/api/agent', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });
      if (!resp.ok) return {text: 'Agent unavailable (' + resp.status + ')', tool: null};
      return await resp.json();
    } catch (e) {
      return {text: 'Network error: ' + e.message, tool: null};
    }
  };

  // assistant:report → navigate to Reports tab
  document.addEventListener('assistant:report', function() {
    var btn = Array.from(document.querySelectorAll('[class*="cu-nav-tab"]'))
      .find(function(b) { return b.textContent.trim() === 'Reports'; });
    if (btn) btn.click();
  });
})();
""")

    if not _SERVER_STATE["customers"] or (time.time() - _SERVER_STATE["customers_ts"]) > 300:
        threading.Thread(target=_load_customers_bg, args=(cfg,), daemon=True).start()

    # Load full org list (24h cache) for pinned-account selector
    threading.Thread(target=_load_all_orgs_bg, args=(cfg,), daemon=True).start()

    # Auto-sync SFDC data if credentials are configured and data is >24h stale
    def _sfdc_auto_sync_bg():
        time.sleep(3.0)  # Let page render fully before SFDC sync starts CB connections
        try:
            from supportal.sfdc_sync import sfdc_sync_age_seconds, sync_all
            _creds = _load_sfdc_creds()
            if not (_creds.get("consumer_key") and _creds.get("consumer_secret")):
                return
            age = sfdc_sync_age_seconds()
            if age is None or age > 86_400:
                sync_all()
        except Exception:
            pass
    threading.Thread(target=_sfdc_auto_sync_bg, daemon=True).start()

    # ── Settings dialog ────────────────────────────────────────────────────────
    # White background so Quasar's light-mode input/select/tab defaults work without
    # needing dark-prop overrides on every component.
    with ui.dialog().props("persistent") as settings_dialog:
        refs["settings_dialog"] = settings_dialog
        with ui.element("div").style(
            "background:#ffffff;border-radius:14px;padding:28px 32px;min-width:520px;max-width:640px;"
            "font-family:'IBM Plex Sans',sans-serif;color:#111827;"
        ):
            ui.html('<div style="font-size:16px;font-weight:600;margin-bottom:16px;color:#111827;">Settings</div>')

            with ui.tabs().props("dense indicator-color=red align=left dark=false").style(
                "margin-bottom:20px;border-bottom:1px solid #e5e7eb;color:#374151;"
            ) as _stabs:
                _tab_accts = ui.tab("Accounts").props("dark=false").style(
                    "color:#374151 !important;font-family:'IBM Plex Sans',sans-serif;font-size:13px;"
                )
                _tab_sfdc = ui.tab("Salesforce").props("dark=false").style(
                    "color:#374151 !important;font-family:'IBM Plex Sans',sans-serif;font-size:13px;"
                )

            with ui.tab_panels(_stabs, value=_tab_accts).props("dark=false").style(
                "background:#ffffff;color:#111827;min-height:220px;"
            ):
                with ui.tab_panel(_tab_accts).props("dark=false").style("background:#ffffff;color:#111827;padding:0;"):
                    ui.html('<div style="font-size:12px;color:#6b7280;margin-bottom:18px;">Always visible in the overview bar and at the top of the account selector, even when quiet.</div>')
                    _org_opts = sorted(
                        _SERVER_STATE["all_orgs"] or _load_org_cache()[0] or
                        [r[0] for r in (_SERVER_STATE["customers"] or [])]
                    )
                    pinned_select = ui.select(
                        options=_org_opts,
                        value=_load_pinned_accounts(),
                        multiple=True,
                        label="Pinned accounts",
                    ).style("width:100%;").props('use-chips outlined clearable')

                    ui.html('<div style="font-size:12px;color:#6b7280;margin:18px 0 6px;">Extra accounts — accounts you want to track that aren\'t in your SFDC primary SE scope (e.g. helping a colleague). These are additive: removing an entry here never removes an SFDC-synced or ticket-based account from the Customers tab.</div>')
                    extra_select = ui.select(
                        options=_org_opts,
                        value=_load_extra_accounts(),
                        multiple=True,
                        label="Extra accounts",
                        new_value_mode="add-unique",
                    ).style("width:100%;").props('use-chips outlined clearable')

                with ui.tab_panel(_tab_sfdc).props("dark=false").style("background:#ffffff;color:#111827;padding:0;"):
                    _sfdc_c = _load_sfdc_creds()
                    ui.html('<div style="font-size:12px;color:#6b7280;margin-bottom:18px;">OAuth credentials for Salesforce data sync. Saved to your active profile and mirrored to Couchbase.</div>')

                    sfdc_flow = ui.select(
                        options=["client_credentials", "password"],
                        value=_sfdc_c.get("auth_flow", "client_credentials"),
                        label="Auth flow",
                    ).style("width:100%;margin-bottom:12px;").props("outlined dense")

                    sfdc_host = ui.input(
                        label="Token host",
                        value=_sfdc_c.get("token_host", "https://couchbase.my.salesforce.com"),
                        placeholder="https://couchbase.my.salesforce.com",
                    ).style("width:100%;margin-bottom:12px;").props("outlined dense")

                    sfdc_key = ui.input(
                        label="Consumer key",
                        value=_sfdc_c.get("consumer_key", ""),
                    ).style("width:100%;margin-bottom:12px;").props("outlined dense")

                    sfdc_secret = ui.input(
                        label="Consumer secret",
                        value=_sfdc_c.get("consumer_secret", ""),
                        password=True,
                        password_toggle_button=True,
                    ).style("width:100%;margin-bottom:16px;").props("outlined dense")

                    sfdc_conn_status = ui.html("").style(
                        "font-size:12px;min-height:60px;margin-bottom:8px;"
                        "font-family:'IBM Plex Mono',monospace;line-height:1.8;"
                    )

                    def _test_sfdc():
                        def _step(icon: str, label: str, ok: bool | None = None) -> str:
                            if ok is True:
                                color, tick = "#15803d", "✓"
                            elif ok is False:
                                color, tick = "#dc2626", "✗"
                            else:
                                color, tick = "#374151", icon
                            return f'<span style="color:{color};">{tick} {label}</span><br>'

                        def _run():
                            import socket
                            import urllib.parse
                            import requests

                            host_val   = sfdc_host.value or ""
                            key_val    = sfdc_key.value or ""
                            secret_val = sfdc_secret.value or ""
                            flow_val   = sfdc_flow.value or "client_credentials"
                            host_base  = ""  # set after URL parse

                            rows: list[str] = []

                            def _update(row: str):
                                sfdc_conn_status.set_content("".join(rows) + row)

                            # Step 1 — URL format
                            _update(_step("…", "Checking URL format…"))
                            try:
                                parsed = urllib.parse.urlparse(host_val)
                                if parsed.scheme not in ("https", "http") or not parsed.netloc:
                                    raise ValueError("Not a valid URL")
                                # Always reconstruct from scheme+host only — the user may have
                                # pasted the full endpoint URL; we append the path ourselves.
                                host_base = f"{parsed.scheme}://{parsed.netloc}"
                                rows.append(_step("", f"URL format ({host_base})", ok=True))
                            except Exception as exc:
                                rows.append(_step("", f"URL format — {exc}", ok=False))
                                sfdc_conn_status.set_content("".join(rows))
                                return

                            # Step 2 — Network reachability (TCP connect)
                            _update(_step("…", "Checking network reachability…"))
                            try:
                                hostname = parsed.netloc.split(":")[0]
                                port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
                                sock = socket.create_connection((hostname, port), timeout=5)
                                sock.close()
                                rows.append(_step("", f"Network ({hostname}:{port})", ok=True))
                            except Exception as exc:
                                rows.append(_step("", f"Network unreachable — {exc}", ok=False))
                                sfdc_conn_status.set_content("".join(rows))
                                return

                            # Step 3 — OAuth token
                            _update(_step("…", "Authenticating…"))
                            try:
                                data = {
                                    "grant_type":    flow_val,
                                    "client_id":     key_val,
                                    "client_secret": secret_val,
                                }
                                _token_url = f"{host_base}/services/oauth2/token"
                                r = requests.post(_token_url, data=data, timeout=15)
                                if r.status_code != 200:
                                    raise RuntimeError(
                                        f"HTTP {r.status_code} — POST {_token_url} — {r.text[:200]}"
                                    )
                                tok = r.json()
                                instance_url = tok.get("instance_url", "")
                                rows.append(_step("", f"OAuth accepted ({instance_url.split('//')[-1]})", ok=True))
                            except Exception as exc:
                                rows.append(_step("", f"Authentication failed — {exc}", ok=False))
                                sfdc_conn_status.set_content("".join(rows))
                                return

                            # Step 4 — SOQL smoke test
                            _update(_step("…", "Running test query…"))
                            try:
                                from simple_salesforce import Salesforce
                                sf = Salesforce(
                                    instance_url=tok["instance_url"],
                                    session_id=tok["access_token"],
                                )
                                result = sf.query("SELECT Id, Name FROM Account LIMIT 1")
                                found = result.get("totalSize", 0)
                                rows.append(_step("", f"Query OK — {found} account(s) returned", ok=True))
                            except Exception as exc:
                                rows.append(_step("", f"Query failed — {exc}", ok=False))

                            sfdc_conn_status.set_content("".join(rows))

                        sfdc_conn_status.set_content(
                            '<span style="color:#374151;font-size:12px;">Starting…</span>'
                        )
                        threading.Thread(target=_run, daemon=True).start()

                    ui.button("Test connection", on_click=_test_sfdc).props("flat").style(
                        "color:#2563eb;font-family:inherit;font-size:13px;padding:0;"
                    )

                    # ── Identity subsection ────────────────────────────────────────
                    ui.html(
                        '<div style="font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;'
                        'letter-spacing:.06em;margin:20px 0 10px;border-top:1px solid #e5e7eb;padding-top:16px;">'
                        'My Salesforce Identity</div>'
                    )
                    ui.html(
                        '<div style="font-size:12px;color:#6b7280;margin-bottom:12px;">'
                        'Type your name then click <b>Find</b> to confirm the exact format it appears in '
                        'Salesforce opportunity records. The agent uses this for "my accounts" and "my pipeline".'
                        '</div>'
                    )

                    with ui.element("div").style("display:flex;gap:8px;align-items:flex-start;margin-bottom:8px;"):
                        sfdc_name_field = ui.input(
                            label="My name in Salesforce",
                            value=_sfdc_c.get("user_name", ""),
                            placeholder="e.g. Austin Gonyou",
                        ).style("flex:1;").props("outlined dense")

                        def _find_identity():
                            sfdc_identity_status.set_content('<span style="color:#374151;">Searching…</span>')
                            def _run_find():
                                try:
                                    import requests as _req
                                    import urllib.parse as _up
                                    _p = _up.urlparse(sfdc_host.value or "")
                                    _hb = f"{_p.scheme}://{_p.netloc}"
                                    _r = _req.post(
                                        f"{_hb}/services/oauth2/token",
                                        data={
                                            "grant_type":    sfdc_flow.value or "client_credentials",
                                            "client_id":     sfdc_key.value or "",
                                            "client_secret": sfdc_secret.value or "",
                                        },
                                        timeout=15,
                                    )
                                    if _r.status_code != 200:
                                        raise RuntimeError(f"OAuth {_r.status_code}: {_r.text[:120]}")
                                    _tok = _r.json()
                                    from simple_salesforce import Salesforce
                                    _sf = Salesforce(
                                        instance_url=_tok["instance_url"],
                                        session_id=_tok["access_token"],
                                    )
                                    _term = sfdc_name_field.value.strip()
                                    if not _term:
                                        sfdc_identity_status.set_content(
                                            '<span style="color:#dc2626;">Enter a name to search.</span>'
                                        )
                                        return
                                    _res = _sf.query(
                                        f"SELECT Id, Name, Email, Title FROM User "
                                        f"WHERE Name LIKE '%{_term}%' AND IsActive = true LIMIT 5"
                                    )
                                    _recs = _res.get("records", [])
                                    if not _recs:
                                        sfdc_identity_status.set_content(
                                            f'<span style="color:#dc2626;">No active users matching "{_term}".</span>'
                                        )
                                        return
                                    parts = []
                                    for _u in _recs:
                                        _n  = _u.get("Name", "")
                                        _em = _u.get("Email", "")
                                        _ti = _u.get("Title", "") or ""
                                        parts.append(f"{_n} &lt;{_em}&gt;{(' · ' + _ti) if _ti else ''}")
                                    sfdc_identity_status.set_content(
                                        '<span style="color:#374151;">'
                                        + "<br>".join(parts)
                                        + "</span>"
                                    )
                                    # Auto-select if exactly one match
                                    if len(_recs) == 1:
                                        _u = _recs[0]
                                        sfdc_name_field.set_value(_u["Name"])
                                        _save_sfdc_identity(_u["Name"], _u.get("Email", ""), _u.get("Id", ""))
                                except Exception as _exc:
                                    sfdc_identity_status.set_content(
                                        f'<span style="color:#dc2626;">Search failed — {_exc}</span>'
                                    )
                            threading.Thread(target=_run_find, daemon=True).start()

                        ui.button("Find", on_click=_find_identity).style(
                            "background:#374151;color:#ffffff;font-family:inherit;font-size:12px;"
                            "font-weight:600;height:40px;margin-top:2px;"
                        )

                    sfdc_identity_status = ui.html(
                        f'<span style="color:#374151;">{_sfdc_c.get("user_email","")}</span>'
                        if _sfdc_c.get("user_email") else ""
                    ).style("font-size:12px;min-height:18px;margin-bottom:16px;font-family:'IBM Plex Mono',monospace;")

                    # ── Sync subsection ────────────────────────────────────────
                    ui.html(
                        '<div style="font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;'
                        'letter-spacing:.06em;margin-bottom:10px;border-top:1px solid #e5e7eb;padding-top:16px;">'
                        'Data Sync</div>'
                    )

                    def _fmt_sync_age() -> str:
                        try:
                            from supportal.sfdc_sync import sfdc_sync_age_seconds
                            age = sfdc_sync_age_seconds()
                            if age is None:
                                return '<span style="color:#9ca3af;">Never synced</span>'
                            if age < 60:
                                return f'<span style="color:#15803d;">Last synced: just now</span>'
                            if age < 3600:
                                return f'<span style="color:#15803d;">Last synced: {int(age/60)}m ago</span>'
                            if age < 86400:
                                return f'<span style="color:#374151;">Last synced: {int(age/3600)}h ago</span>'
                            return f'<span style="color:#dc2626;">Last synced: {int(age/86400)}d ago — consider refreshing</span>'
                        except Exception:
                            return ""

                    sfdc_sync_status = ui.html(_fmt_sync_age()).style(
                        "font-size:12px;min-height:18px;margin-bottom:10px;font-family:'IBM Plex Mono',monospace;"
                    )

                    def _safe_err_text(exc: Exception) -> str:
                        """Return a plain-text summary of an exception, stripping any HTML."""
                        import re as _re
                        s = str(exc)
                        s = _re.sub(r"<[^>]+>", "", s)   # strip all HTML tags
                        s = s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
                        return s[:200].strip()

                    def _run_sfdc_sync():
                        sfdc_sync_status.set_content('<span style="color:#374151;">Syncing…</span>')
                        def _do():
                            try:
                                from supportal.sfdc_sync import sync_all
                                result = sync_all()
                                accts = result.get("accounts_synced", "?")
                                opps  = result.get("opportunities_synced", "?")
                                sfdc_sync_status.set_content(
                                    f'<span style="color:#15803d;">Done — {accts} accounts, {opps} opportunities synced</span>'
                                )
                            except Exception as exc:
                                sfdc_sync_status.set_content(
                                    f'<span style="color:#dc2626;">Sync failed — {_safe_err_text(exc)}</span>'
                                )
                        threading.Thread(target=_do, daemon=True).start()

                    ui.button("Sync Salesforce data now", on_click=_run_sfdc_sync).style(
                        "background:#ea2328;color:#ffffff;font-family:inherit;font-size:12px;"
                        "font-weight:600;border-radius:6px;"
                    )

            with ui.element("div").style(
                "display:flex;gap:10px;margin-top:24px;padding-top:16px;"
                "border-top:1px solid #e5e7eb;justify-content:flex-end;"
            ):
                def _close_settings():
                    settings_dialog.close()
                ui.button("Cancel", on_click=_close_settings).props("flat").style(
                    "color:#374151;font-family:inherit;font-size:13px;"
                )

                def _save_settings():
                    _save_pinned_accounts(pinned_select.value or [])
                    _save_extra_accounts(extra_select.value or [])
                    _save_sfdc_creds(
                        token_host=sfdc_host.value or "",
                        consumer_key=sfdc_key.value or "",
                        consumer_secret=sfdc_secret.value or "",
                        auth_flow=sfdc_flow.value or "client_credentials",
                    )
                    _save_sfdc_identity(sfdc_name_field.value or "")
                    settings_dialog.close()
                    _refresh_overview()
                    # Auto-sync if creds look complete and data has never been synced
                    _key = sfdc_key.value or ""
                    _secret = sfdc_secret.value or ""
                    if _key and _secret:
                        try:
                            from supportal.sfdc_sync import sfdc_sync_age_seconds, sync_all, _SYNC_LOCK
                            if sfdc_sync_age_seconds() is None and not _SYNC_LOCK.locked():
                                threading.Thread(target=sync_all, daemon=True).start()
                        except Exception:
                            pass
                ui.button("Save", on_click=_save_settings).style(
                    "background:#ea2328;color:#ffffff;font-family:inherit;font-size:13px;font-weight:600;"
                )

    # Schedule overview refresh on the event loop after page render completes.
    # Using ensure_future (not a background thread) so all UI mutations run on
    # the event loop and cannot race with build_response's client.elements iteration.
    async def _deferred_overview():
        # Wait for _load_customers_bg (1.5s sleep + CB queries ≈ 4-5s total).
        await asyncio.sleep(6.0)
        try:
            _refresh_overview()
        except Exception:
            pass

    asyncio.ensure_future(_deferred_overview())


@ng_app.on_event("startup")
async def _startup_daily_sync():
    """Fire SFDC sync on startup and every 6 hours while the server is up."""
    async def _loop():
        while True:
            await asyncio.sleep(6 * 3600)  # 6-hour cadence
            try:
                from supportal.sfdc_sync import sfdc_sync_age_seconds, sync_all, _SYNC_LOCK
                age = sfdc_sync_age_seconds()
                if (age is None or age > 82_800) and not _SYNC_LOCK.locked():
                    from nicegui import run as _ng_run
                    await _ng_run.io_bound(sync_all)
            except Exception as _e:
                print(f"[daily_sync] {_e}")
    asyncio.ensure_future(_loop())


if __name__ in {"__main__", "__mp_main__"}:
    port = int(os.environ.get("UNIFIED_PORT", 8767))
    ui.run(
        port=port,
        title="Cursus",
        favicon="\U0001f534",
        dark=False,
        storage_secret=os.environ.get("NICEGUI_SECRET", "cursus-unified"),
        show=False,
    )
