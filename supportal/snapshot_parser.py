"""
Snapshot parsing, topology extraction, and snapshot scraping pipeline.

Exports
-------
Constants / regexes
  _UUID_RE, _SNAP_ID_RE, _SNAP_HREF_RE, _SNAP_DEBUG_DIR

Helpers
  _topo_str, _highest_snap_id
  _normalize_checker_name
  extract_cluster_snapshot_info

Text parsers
  _parse_snapshot_checker_text
  _parse_structured_api_json

Debug
  _write_snap_debug

API topology
  fetch_snapshot_topology_api   — REST nutshell endpoints
  fetch_snapshot_topology       — full fetch strategy (requests → API → Playwright fallback)

Enrichment
  enrich_tickets_with_snapshots

Listing scrapers
  _find_snapshots_tab_url
  _extract_snapshot_rows
  scrape_snapshots_from_stubs
  scrape_snapshots_for_customer
"""

from __future__ import annotations

import concurrent.futures
import datetime
import json
import os
import re
import threading
import urllib.parse
from pathlib import Path
from typing import Callable, Optional

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup

from supportal.constants import BASE_URL, UA
from supportal.ticket_parser import _normalize_field_key
from supportal.api_client import _make_api_session


# ---------------------------------------------------------------------------
# Regex constants
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(
    r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b",
    re.IGNORECASE,
)
# Couchbase snapshot IDs: 32 hex chars followed by ::N
_SNAP_ID_RE = re.compile(r"\b([0-9a-f]{32}::\d+)\b", re.IGNORECASE)
_SNAP_HREF_RE = re.compile(r"/snapshot/([0-9a-f]{32}::\d+)", re.IGNORECASE)

# Cluster name patterns found in comments/description
_CLUSTERS_AFFECTED_RE = re.compile(
    r"[Cc]lusters?\s+(?:affected|involved|impacted)[:\s]+(.+?)(?:\.|$|\n)",
    re.IGNORECASE,
)
_CLUSTER_NAME_RE = re.compile(
    r"\b([A-Za-z0-9][A-Za-z0-9_\-\.]{3,})\s+cluster\b",
    re.IGNORECASE,
)

_SNAP_DEBUG_DIR = Path(os.path.expanduser("~")) / "Downloads" / "Apps" / "Scraper" / "snap_debug"


# ---------------------------------------------------------------------------
# Topology string helper
# ---------------------------------------------------------------------------

def _topo_str(val) -> str:
    """Safely coerce a topology field value to a stripped string.

    Topology dicts are stored in CB and occasionally have list values (e.g.
    cluster_name) due to raw checker data or legacy format differences.
    This helper handles str, list, int, None — callers don't need to guard.
    """
    if not val:
        return ""
    if isinstance(val, list):
        val = val[0] if val else ""
    return str(val).strip()


def _highest_snap_id(snap_ids: list[str]) -> str:
    """Return the snapshot ID with the highest ::N sequence number (most recent)."""
    def _seq(s: str) -> int:
        m = re.search(r"::(\d+)$", s)
        return int(m.group(1)) if m else -1
    return max(snap_ids, key=_seq)


# ---------------------------------------------------------------------------
# Cluster info extraction from ticket text
# ---------------------------------------------------------------------------

def extract_cluster_snapshot_info(ticket: dict) -> dict:
    """
    Parse cluster names/IDs and snapshot metadata from a ticket without calling
    the LLM.  Looks in:
      - ticket['snapshots']    — free-text snapshot block from the detail page
      - ticket['ticket_fields']— structured key/value table (JSON string or dict)
      - ticket['description'] / ticket['comments'] — fallback text scan

    Returns a dict with:
      cluster_names     list[str]  — unique cluster name strings found
      cluster_ids       list[str]  — unique cluster UUIDs found (standard UUID format)
      snapshot_count    int        — number of discrete snapshot entries
      last_snapshot_id  str|None   — last snapshot ID ({32hex}::N format)
    """
    _sv0 = ticket.get("snapshots")
    snapshots_raw   = _sv0 if isinstance(_sv0, str) else ""
    fields_raw      = ticket.get("ticket_fields") or {}
    if isinstance(fields_raw, str):
        try:
            fields_raw = json.loads(fields_raw)
        except Exception:
            fields_raw = {}

    cluster_names: list[str] = []
    cluster_ids:   list[str] = []

    # ── ticket_fields: look for cluster-related keys ───────────────────────────
    _cluster_key_re = re.compile(r"cluster|cb_cluster|cloud.*cluster", re.IGNORECASE)
    _guid_key_re    = re.compile(r"cluster.*id|id.*cluster|cluster.*guid", re.IGNORECASE)
    for k, v in fields_raw.items():
        if not v:
            continue
        v_str = str(v).strip()
        k_norm = _normalize_field_key(k)
        if _guid_key_re.search(k_norm):
            for m in _UUID_RE.findall(v_str):
                if m not in cluster_ids:
                    cluster_ids.append(m)
        elif _cluster_key_re.search(k_norm):
            for m in _UUID_RE.findall(v_str):
                if m not in cluster_ids:
                    cluster_ids.append(m)
            name = _UUID_RE.sub("", v_str).strip(" ()-,")
            if name and name not in cluster_names:
                cluster_names.append(name)

    # ── snapshots block ────────────────────────────────────────────────────────
    snap_lines = [ln.strip() for ln in snapshots_raw.splitlines() if ln.strip()]
    snapshot_count = len(snap_lines)

    all_snap_ids = _SNAP_ID_RE.findall(snapshots_raw)
    if not all_snap_ids:
        all_snap_ids = _UUID_RE.findall(snapshots_raw)
    last_snapshot_id = _highest_snap_id(all_snap_ids) if all_snap_ids else None

    # ── scan description + comments for cluster names ──────────────────────────
    desc = ticket.get("description") or ""
    comments_raw = ticket.get("comments") or "[]"
    try:
        comments_list = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
        full_text = desc + "\n" + "\n".join(c.get("body", "") for c in comments_list)
    except Exception:
        full_text = desc

    # Pattern 1: "Clusters affected: A, B, C and D"
    for m in _CLUSTERS_AFFECTED_RE.finditer(full_text):
        raw_list = m.group(1)
        parts = re.split(r"[,;]\s*|\s+and\s+", raw_list, flags=re.IGNORECASE)
        for part in parts:
            name = re.sub(r'[^a-zA-Z0-9\-_]+$', '', part.strip())
            if not name or len(name) < 5:
                continue
            if name not in cluster_names:
                cluster_names.append(name)

    # Pattern 2: "<name> cluster" (e.g. "p-csmohsm09-cb52 cluster logs")
    _CLUSTER_NAME_STOPWORDS = {
        "the", "a", "an", "this", "that", "these", "those",
        "each", "every", "any", "all", "both", "some", "such",
        "our", "your", "my", "their", "its", "his", "her",
        "from", "into", "onto", "upon", "with", "within",
        "about", "above", "after", "along", "among", "around",
        "before", "between", "during", "since", "through",
        "same", "another", "other", "different", "similar",
        "remote", "local", "current", "existing", "original",
        "second", "third", "fourth", "first", "last", "next",
        "new", "old", "fresh", "single", "entire", "whole",
        "main", "additional", "separate", "standalone",
        "active", "passive", "healthy", "unhealthy", "affected",
        "target", "source", "destination", "backup", "replica",
        "primary", "secondary", "tertiary", "master", "slave",
        "couchbase", "server", "test", "dev", "prod", "stage",
        "production", "development", "staging", "testing",
        "cloud", "data", "index", "query", "node", "virtual",
        "physical", "external", "internal", "custom", "example",
    }
    for m in _CLUSTER_NAME_RE.finditer(full_text):
        name = re.sub(r'[^a-zA-Z0-9\-_]+$', '', m.group(1).strip())
        if not name or len(name) < 7:
            continue
        if not re.match(r'^[a-zA-Z]', name):
            continue
        if name.lower() in _CLUSTER_NAME_STOPWORDS:
            continue
        if not re.search(r"[\d\-_]", name):
            continue
        if name not in cluster_names:
            cluster_names.append(name)

    if not cluster_ids:
        for m in _UUID_RE.findall(full_text):
            if m not in cluster_ids:
                cluster_ids.append(m)

    # ── snapshot_topology: authoritative cluster info from fetched snapshot ────
    topo = ticket.get("snapshot_topology") or {}
    topo_name = topo.get("cluster_name")
    topo_uuid = topo.get("cluster_uuid")
    if topo_name:
        if topo_name in cluster_names:
            cluster_names.remove(topo_name)
        cluster_names.insert(0, topo_name)
    if topo_uuid:
        if topo_uuid in cluster_ids:
            cluster_ids.remove(topo_uuid)
        cluster_ids.insert(0, topo_uuid)

    return {
        "cluster_names":    cluster_names,
        "cluster_ids":      cluster_ids,
        "snapshot_count":   snapshot_count,
        "last_snapshot_id": last_snapshot_id,
    }


# ---------------------------------------------------------------------------
# Checker name normalizer
# ---------------------------------------------------------------------------

def _normalize_checker_name(name: str, full_line: str = "") -> str:
    """
    Normalize a checker name to remove node-specific details so identical
    issues on different nodes/interfaces collapse to a single entry.

    Examples:
      "Interface 'eth0' (10.1.2.3) failures"   → "Interface 'eth0' RX failures"
      "Slow Operations (Total)"                 → "Slow Operations"
      "Resident Items 'my_bucket'"              → "Resident Items"
    """
    name = re.sub(r"\s*\([0-9a-f:./%]+\)", "", name).strip()
    if re.match(r"interface\s+'[^']+'\s+failures$", name, re.I) and full_line:
        _rx = re.search(r"RX\s*:\s*(\d+)", full_line, re.I)
        _tx = re.search(r"TX\s*:\s*(\d+)", full_line, re.I)
        _rx_n = int(_rx.group(1)) if _rx else 0
        _tx_n = int(_tx.group(1)) if _tx else 0
        if _rx_n > 0 and _tx_n == 0:
            name = re.sub(r"\s+failures$", " RX failures", name, flags=re.I)
        elif _tx_n > 0 and _rx_n == 0:
            name = re.sub(r"\s+failures$", " TX failures", name, flags=re.I)
        elif _rx_n > 0 and _tx_n > 0:
            name = re.sub(r"\s+failures$", " RX+TX failures", name, flags=re.I)
    name = re.sub(r"^(Slow Operations)\s*\([^)]*\)\s*$", r"\1", name, flags=re.I).strip()
    name = re.sub(r"\s*:\s*ns_\d+@\S+.*$", "", name).strip()
    name = re.sub(r"\s+'[^']+'$", "", name).strip()
    return name


# ---------------------------------------------------------------------------
# Text (legacy checker) parser
# ---------------------------------------------------------------------------

def _parse_snapshot_checker_text(text: str) -> dict:
    """
    Parse cbcollect checker output from a Supportal snapshot page.

    Accepts raw HTML (ANSI→HTML span format) or plain text.
    Uses direct line-by-line field extraction — no section-header regex dependency.
    """
    topo: dict = {
        "cluster_name":          None,
        "cluster_uuid":          None,
        "total_nodes":           None,
        "data_nodes":            0,
        "query_nodes":           0,
        "index_nodes":           0,
        "fts_nodes":             0,
        "eventing_nodes":        0,
        "analytics_nodes":       0,
        "backup_nodes":          0,
        "cb_version":            None,
        "ram_per_node_mib":      None,
        "cpus_per_node":         None,
        "os_name":               None,
        "bucket_count":          0,
        "bucket_names":          [],
        "total_bucket_quota_mb": None,
        "server_groups":         [],
        "auto_failover_seconds": None,
        "orchestrator":          None,
        "ldap_enabled":          None,
        "bad_count":             0,
        "warn_count":            0,
    }
    if not text:
        return topo

    # ── Unwrap Supportal JSON envelope ────────────────────────────────────────
    _stripped = text.lstrip()
    if _stripped.startswith("{"):
        try:
            _env = json.loads(text)
            if isinstance(_env, dict):
                text = (_env.get("nutshell_output") or _env.get("results")
                        or _env.get("html") or _env.get("content") or text)
        except Exception:
            pass

    # ── Strip HTML → clean plain text ─────────────────────────────────────────
    if "<" in text and ">" in text:
        _soup = BeautifulSoup(text, "html.parser")
        _pre = next(
            (p for p in _soup.find_all("pre")
             if "[info]" in p.get_text() or "===Checker" in p.get_text()),
            None,
        )
        if _pre:
            text = _pre.get_text("\n")
        else:
            for tag in _soup.find_all(["div", "p", "br", "li", "tr"]):
                tag.insert_before("\n")
            text = _soup.get_text("\n")

    lines = text.splitlines()

    # ── Severity counts + checker names ───────────────────────────────────────
    _bad_names:  set[str] = set()
    _warn_names: set[str] = set()
    for _line in lines:
        _m = re.match(r"\s*\[(BAD|warn|WARN|WARNING|ALERT)\]\s*([^:]+)", _line, re.I)
        if _m:
            _sev, _name = _m.group(1).upper(), _normalize_checker_name(_m.group(2).strip(), _line)
            if _sev in ("BAD", "ALERT"):
                _bad_names.add(_name)
            else:
                _warn_names.add(_name)
    topo["bad_count"]  = sum(1 for l in lines if re.match(r"\s*\[BAD\]",  l, re.I))
    topo["warn_count"] = sum(1 for l in lines if re.match(r"\s*\[warn\]", l, re.I))
    topo["bad_items"]  = sorted(_bad_names)
    topo["warn_items"] = sorted(_warn_names)

    # ── Section tracking ──────────────────────────────────────────────────────
    _SVC_MAP = {
        "data": "data_nodes", "kv": "data_nodes",
        "query": "query_nodes", "n1ql": "query_nodes",
        "index": "index_nodes",
        "fts": "fts_nodes", "search": "fts_nodes",
        "eventing": "eventing_nodes",
        "analytics": "analytics_nodes", "cbas": "analytics_nodes",
        "backup": "backup_nodes",
    }
    _section = None
    _in_checker_block = False
    _cluster_name_set = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("==="):
            _in_checker_block = True
            _section = None
            m = re.search(r"===Checker results for\s+['\"]?([^'\"=\n]+?)['\"]?===", stripped)
            if m and not topo["cluster_name"]:
                topo["cluster_name"] = m.group(1).strip()
                _cluster_name_set = True
            continue

        if not _in_checker_block:
            if re.match(r"\[(info|ok|BAD|warn)\]", stripped):
                _in_checker_block = True
            elif stripped.startswith("* "):
                _in_checker_block = True
            else:
                continue

        if stripped.startswith("* "):
            header_lc = stripped[2:].lower()
            if "multi-dimensional" in header_lc or "mds" in header_lc:
                _section = "mds"
            elif "rack zone" in header_lc or "server group" in header_lc:
                _section = "rack"
            elif "bucket details" in header_lc:
                _section = "buckets"
            elif "cluster users" in header_lc:
                _section = "users"
            else:
                _section = "other"
            continue

        if _section == "mds":
            parts = stripped.split()
            if len(parts) >= 2 and not stripped.startswith("-") and not stripped.lower().startswith("service"):
                svc = parts[0].lower()
                field = _SVC_MAP.get(svc)
                if field:
                    try:
                        topo[field] = int(parts[1])
                    except ValueError:
                        pass
            continue

        if _section == "rack":
            if stripped and not stripped.startswith("-") and not stripped.lower().startswith("server group") and not stripped.lower().startswith("nodes"):
                parts = stripped.split()
                if parts:
                    grp = parts[0]
                    if grp not in ("Group", "us-", "eu-", "ap-") and len(grp) > 2:
                        if grp not in topo["server_groups"]:
                            topo["server_groups"].append(grp)
            continue

        if _section == "buckets":
            m = re.match(r"Total\s+\((\d+)\s+buckets?\)", stripped)
            if m:
                topo["bucket_count"] = int(m.group(1))
                continue
            parts = stripped.split()
            if len(parts) >= 3 and parts[1] in ("CB", "Eph", "Mem"):
                if parts[0] not in topo["bucket_names"]:
                    topo["bucket_names"].append(parts[0])
                try:
                    quota = int(parts[2])
                    topo["total_bucket_quota_mb"] = (topo["total_bucket_quota_mb"] or 0) + quota
                except (ValueError, IndexError):
                    pass
            continue

        m = re.match(r"\[(info|ok|BAD|warn)\]\s+(.+?)\s{2,}:\s*(.+)", stripped)
        if not m:
            m = re.match(r"\[(info|ok|BAD|warn)\]\s+(.+?)\s*:\s*(.+)", stripped)
        if m:
            field_raw = m.group(2).strip().lower()
            value     = m.group(3).strip()

            if field_raw in ("ui cluster name", "cluster name") and value:
                topo["cluster_name"] = value
            elif field_raw == "uuid":
                uuid_m = re.search(r"[0-9a-f]{32}", value, re.I)
                if uuid_m and not topo["cluster_uuid"]:
                    topo["cluster_uuid"] = uuid_m.group(0)
            elif field_raw == "node count":
                try:
                    topo["total_nodes"] = int(value.split()[0])
                except (ValueError, IndexError):
                    pass
            elif field_raw in ("auto-failover", "auto failover"):
                af_m = re.search(r"(\d+)", value)
                if af_m and topo["auto_failover_seconds"] is None:
                    topo["auto_failover_seconds"] = int(af_m.group(1))
            elif field_raw == "orchestrator":
                if not topo["orchestrator"]:
                    topo["orchestrator"] = value
            elif field_raw in ("external authentication", "ldap", "ldap enabled"):
                topo["ldap_enabled"] = value.lower() in ("enabled", "true", "yes", "1")
            elif field_raw in ("cb version", "couchbase version", "version"):
                if not topo["cb_version"]:
                    topo["cb_version"] = value
            elif field_raw == "installed ram":
                ram_m = re.search(r"(\d+)", value)
                if ram_m and topo["ram_per_node_mib"] is None:
                    topo["ram_per_node_mib"] = int(ram_m.group(1))
            elif field_raw == "installed cpus":
                cpu_m = re.search(r"(\d+)", value)
                if cpu_m and topo["cpus_per_node"] is None:
                    topo["cpus_per_node"] = int(cpu_m.group(1))
            elif field_raw in ("os version", "os name"):
                if not topo["os_name"]:
                    topo["os_name"] = value
            continue

        if not _cluster_name_set and not topo["cluster_name"] and _section is None:
            if stripped and not stripped.startswith(("[", "*", "=", "-", "Service", "Bucket")):
                if len(stripped) < 128 and not stripped.startswith("http"):
                    topo["cluster_name"] = stripped
                    _cluster_name_set = True

    return topo


# ---------------------------------------------------------------------------
# Debug writer
# ---------------------------------------------------------------------------

def _write_snap_debug(snap_id: str, stage: str, content: str) -> None:
    """Write debug HTML/text for a snapshot fetch stage to snap_debug/."""
    try:
        _SNAP_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_id = re.sub(r"[^\w\-]", "_", snap_id)[:60]
        ext = "html" if "<" in content else "txt"
        path = _SNAP_DEBUG_DIR / f"{safe_id}__{stage}.{ext}"
        path.write_text(content, encoding="utf-8", errors="replace")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Structured API JSON parser
# ---------------------------------------------------------------------------

def _parse_structured_api_json(data: dict) -> dict:
    """
    Parse Supportal API response with structured nutshell_beta_output / nutshell keys.

    The Supportal API returns a JSON object with these keys:
      - nutshell_output       : ANSI-HTML string (Original tab)
      - nutshell_beta_output  : dict  {node: {...}, cluster: {...}}
      - nutshell              : dict  {results: {node: [...], cluster: [...]}, ...}

    Both nutshell_beta_output and nutshell.results have the same data — we prefer
    nutshell.results (list format) as it is consistent.

    Returns a topology dict with all canonical fields populated.
    """
    topo: dict = {
        "cluster_name": None, "cluster_uuid": None,
        "capella_cluster_id": None,
        "cluster_hostname": None,
        "total_nodes": None,
        "data_nodes": 0, "query_nodes": 0, "index_nodes": 0,
        "fts_nodes": 0, "eventing_nodes": 0, "analytics_nodes": 0, "backup_nodes": 0,
        "cb_version": None, "ram_per_node_mib": None, "cpus_per_node": None,
        "ram_used_per_node_mib": None,
        "os_name": None, "bucket_count": 0, "bucket_names": [], "bucket_details": [],
        "total_bucket_quota_mb": None, "server_groups": [],
        "auto_failover_seconds": None, "orchestrator": None,
        "ldap_enabled": None, "n2n_encryption": None, "data_quota_mib": None,
        "bad_count": 0, "warn_count": 0,
        "disk_total_per_node_mib": None, "disk_used_per_node_mib": None,
        "swap_used_per_node_mib": None, "data_size_mib": None, "total_items": None,
        "global_index_count": None, "global_index_names": [],
        "fts_index_count": None, "fts_index_names": [],
        "eventing_function_count": None, "eventing_function_names": [],
        "scopes_collections": [],
        "raw_fields": {},
    }

    results: dict | None = None
    nutshell = data.get("nutshell")
    beta = data.get("nutshell_beta_output")
    if isinstance(nutshell, dict) and isinstance(nutshell.get("results"), dict):
        results = nutshell["results"]
    elif isinstance(beta, dict):
        results = beta

    if not results:
        return topo

    # ── Cluster-level checkers ────────────────────────────────────────────────
    cluster_list = results.get("cluster", [])
    cluster_checkers: dict = {}
    cluster_analysers: dict = {}

    if isinstance(cluster_list, list) and cluster_list:
        cr = cluster_list[0].get("results", {}) if isinstance(cluster_list[0], dict) else {}
        cluster_checkers = cr.get("checkers", {})
        cluster_analysers = cr.get("analysers", {})
    elif isinstance(cluster_list, dict):
        first = next(iter(cluster_list.values()), {})
        cluster_checkers = first.get("checkers", {})
        cluster_analysers = first.get("analysers", {})

    _cci = {k.lower(): v for k, v in cluster_checkers.items()}

    def _raw(d: dict, key: str):
        entry = d.get(key)
        if entry is None:
            entry = _cci.get(key.lower())
        return entry.get("raw") if isinstance(entry, dict) else None

    _cci_keys_str = ", ".join(sorted(_cci.keys())[:30])
    topo["raw_fields"]["_checker_keys"] = _cci_keys_str

    topo["cluster_name"] = _raw(cluster_checkers, "UI Cluster Name")
    topo["cluster_uuid"] = _raw(cluster_checkers, "UUID")

    _nc = _raw(cluster_checkers, "Node Count")
    if _nc is not None:
        try:
            topo["total_nodes"] = int(_nc)
        except (ValueError, TypeError):
            pass

    _af = _raw(cluster_checkers, "Auto-failover")
    if _af is not None:
        try:
            topo["auto_failover_seconds"] = int(_af)
        except (ValueError, TypeError):
            m = re.search(r"(\d+)", str(_af))
            if m:
                topo["auto_failover_seconds"] = int(m.group(1))

    _ldap = _raw(cluster_checkers, "External Authentication")
    if _ldap is not None:
        topo["ldap_enabled"] = str(_ldap).lower() not in ("disabled", "off", "false", "0", "none", "")

    _orch = _raw(cluster_checkers, "Orchestrator")
    if _orch:
        topo["orchestrator"] = str(_orch)

    _n2n = _raw(cluster_checkers, "N2N Encryption")
    if _n2n is not None:
        topo["n2n_encryption"] = str(_n2n)

    _dq = _raw(cluster_checkers, "Data Quota")
    if _dq is not None:
        try:
            topo["data_quota_mib"] = int(float(str(_dq).replace(",", "").split()[0]))
        except Exception:
            pass

    # ── MDS from analysers ────────────────────────────────────────────────────
    mds = cluster_analysers.get("Multi-Dimensional Scaling", {})
    mds_raw = mds.get("raw", {}) if isinstance(mds, dict) else {}
    if isinstance(mds_raw, dict):
        _SVC_MAP = {
            "kv": "data_nodes", "data": "data_nodes",
            "n1ql": "query_nodes", "query": "query_nodes",
            "index": "index_nodes", "idx": "index_nodes",
            "fts": "fts_nodes", "search": "fts_nodes",
            "eventing": "eventing_nodes",
            "cbas": "analytics_nodes", "analytics": "analytics_nodes",
            "backup": "backup_nodes",
        }
        for svc, svc_nodes in mds_raw.items():
            field = _SVC_MAP.get(svc.lower())
            if field:
                topo[field] = len(svc_nodes) if isinstance(svc_nodes, list) else int(svc_nodes or 0)

    # ── Bucket count + details ────────────────────────────────────────────────
    bd = cluster_analysers.get("Bucket Details", {})
    bd_rows = bd.get("rows", []) if isinstance(bd, dict) else []
    _bd_head_src = bd.get("headings") or bd.get("header") or []
    bd_headers = []
    for c in _bd_head_src:
        if isinstance(c, dict):
            bd_headers.append((c.get("content") or c.get("name") or "").lower())
        elif isinstance(c, str):
            bd_headers.append(c.lower())
        else:
            bd_headers.append("")
    if isinstance(bd_rows, list):
        topo["bucket_count"] = len(bd_rows)
        topo["bucket_names"] = []
        topo["bucket_details"] = []
        for row in bd_rows:
            if not (isinstance(row, list) and row):
                continue
            def _cell(i, _r=row):
                c = _r[i] if i < len(_r) else {}
                return (c.get("content") or c.get("raw") or "") if isinstance(c, dict) else str(c) if c else ""
            name = _cell(0)
            if name:
                topo["bucket_names"].append(name)
            bdet: dict = {"name": name}
            for idx, hdr in enumerate(bd_headers):
                v = _cell(idx)
                if not v or idx == 0:
                    continue
                if "type" in hdr:
                    bdet["type"] = v
                elif "quota" in hdr:
                    try:
                        bdet["quota_mb"] = int(v.replace(",", "").split()[0])
                    except Exception:
                        bdet["quota_mb"] = v
                elif "replica" in hdr:
                    try:
                        bdet["replicas"] = int(v)
                    except Exception:
                        bdet["replicas"] = v
                elif "mem used" in hdr or "mem_used" in hdr:
                    bdet["mem_used"] = v
                elif "compression" in hdr:
                    bdet["compression"] = v
                elif "eviction" in hdr:
                    bdet["eviction"] = v
                elif "storage" in hdr:
                    bdet["storage_mode"] = v
                elif "item" in hdr or "count" in hdr:
                    bdet["items"] = v
                elif "resident" in hdr:
                    bdet["resident_ratio"] = v
                elif "ttl" in hdr:
                    bdet["max_ttl"] = v
            if "type" not in bdet and len(row) >= 2:
                bdet["type"] = _cell(1)
            if "quota_mb" not in bdet and len(row) >= 3:
                try:
                    bdet["quota_mb"] = int(_cell(2).replace(",", "").split()[0])
                except Exception:
                    pass
            if "replicas" not in bdet and len(row) >= 4:
                try:
                    bdet["replicas"] = int(_cell(3))
                except Exception:
                    pass
            if name:
                topo["bucket_details"].append(bdet)

    # ── Server groups from analysers ──────────────────────────────────────────
    sg = cluster_analysers.get("Server Groups", {})
    sg_rows = sg.get("rows", []) if isinstance(sg, dict) else []
    if isinstance(sg_rows, list) and len(sg_rows) > 1:
        groups = []
        for row in sg_rows:
            if isinstance(row, list) and row and isinstance(row[0], dict):
                grp_name = row[0].get("content", "")
                if grp_name:
                    groups.append(grp_name)
        if groups:
            topo["server_groups"] = groups

    def _analyser_rows_headings(a_dict: dict, key: str) -> tuple[list, list]:
        """Return (rows, headings_lower) for a named analyser."""
        a = a_dict.get(key, {})
        if not isinstance(a, dict):
            return [], []
        rows = a.get("rows", [])
        head_src = a.get("headings") or a.get("header") or []
        hdrs = []
        for c in head_src:
            if isinstance(c, dict):
                hdrs.append((c.get("content") or c.get("name") or "").lower())
            elif isinstance(c, str):
                hdrs.append(c.lower())
            else:
                hdrs.append("")
        return rows if isinstance(rows, list) else [], hdrs

    def _row_cell(row: list, idx: int) -> str:
        c = row[idx] if idx < len(row) else {}
        return (c.get("content") or c.get("raw") or "") if isinstance(c, dict) else str(c) if c else ""

    # ── Global Indexes ────────────────────────────────────────────────────────
    gi_rows, gi_hdrs = _analyser_rows_headings(cluster_analysers, "Global Indexes")
    if gi_rows:
        topo["global_index_count"] = len(gi_rows)
        name_col = next((i for i, h in enumerate(gi_hdrs) if "name" in h), 2)
        topo["global_index_names"] = [_row_cell(r, name_col) for r in gi_rows if isinstance(r, list) and _row_cell(r, name_col)]

    # ── FTS Indexes ───────────────────────────────────────────────────────────
    fi_rows, fi_hdrs = _analyser_rows_headings(cluster_analysers, "FTS Indexes")
    if fi_rows:
        topo["fts_index_count"] = len(fi_rows)
        name_col = next((i for i, h in enumerate(fi_hdrs) if "name" in h), 1)
        topo["fts_index_names"] = [_row_cell(r, name_col) for r in fi_rows if isinstance(r, list) and _row_cell(r, name_col)]

    # ── Eventing Functions ────────────────────────────────────────────────────
    ev_rows, ev_hdrs = _analyser_rows_headings(cluster_analysers, "Eventing Functions")
    if ev_rows:
        topo["eventing_function_count"] = len(ev_rows)
        name_col = next((i for i, h in enumerate(ev_hdrs) if "function" in h or "name" in h), 1)
        topo["eventing_function_names"] = [_row_cell(r, name_col) for r in ev_rows if isinstance(r, list) and _row_cell(r, name_col)]

    # ── User Defined Scopes and Collections ───────────────────────────────────
    sc_rows, sc_hdrs = _analyser_rows_headings(cluster_analysers, "User Defined Scopes and Collections")
    if sc_rows:
        bucket_col = next((i for i, h in enumerate(sc_hdrs) if "bucket" in h), 0)
        scopes_col = next((i for i, h in enumerate(sc_hdrs) if "scope" in h and "count" in h), 1)
        colls_col  = next((i for i, h in enumerate(sc_hdrs) if "collection" in h and "count" in h), 2)
        for r in sc_rows:
            if not isinstance(r, list):
                continue
            b  = _row_cell(r, bucket_col)
            sc = _row_cell(r, scopes_col)
            cc = _row_cell(r, colls_col)
            if b:
                topo["scopes_collections"].append({"bucket": b, "scopes": sc, "collections": cc})

    # ── Node-level checkers ───────────────────────────────────────────────────
    node_list = results.get("node", [])
    cb_versions:  list[str] = []
    ram_values:   list[int] = []
    cpu_values:   list[int] = []
    disk_total_values: list[int] = []
    disk_used_values:  list[int] = []
    swap_used_values:  list[int] = []
    ram_used_values:   list[float] = []
    data_size_values:  list[int] = []
    item_count_values: list[int] = []
    bad_count = warn_count = 0
    bad_names:  set[str] = set()
    warn_names: set[str] = set()

    def _to_bytes(raw) -> int | None:
        """Convert a raw value (int bytes, or dict with 'bytes'/'value') to int bytes."""
        if isinstance(raw, (int, float)):
            return int(raw)
        if isinstance(raw, dict):
            for k in ("bytes", "value", "total", "raw"):
                if isinstance(raw.get(k), (int, float)):
                    return int(raw[k])
        return None

    def _process_node_checkers(nc: dict) -> None:
        nonlocal bad_count, warn_count
        _v = nc.get("CB Version", {})
        if isinstance(_v, dict):
            raw_ver = _v.get("raw")
            if raw_ver and isinstance(raw_ver, str):
                cb_versions.append(raw_ver)
        _r = nc.get("Installed RAM", {})
        if isinstance(_r, dict):
            raw_ram = _r.get("raw")
            if isinstance(raw_ram, (int, float)):
                ram_values.append(int(raw_ram))
            elif isinstance(raw_ram, dict):
                tot = raw_ram.get("total")
                if tot:
                    ram_values.append(int(tot))
        _ur = nc.get("Used RAM", {})
        if isinstance(_ur, dict):
            _ur_raw = _ur.get("raw")
            if isinstance(_ur_raw, dict):
                _used_mib = _ur_raw.get("used")
                if isinstance(_used_mib, (int, float)):
                    ram_used_values.append(float(_used_mib))
        _c = nc.get("Installed CPUs", {})
        if isinstance(_c, dict):
            raw_cpu = _c.get("raw")
            if isinstance(raw_cpu, (int, float)):
                cpu_values.append(int(raw_cpu))
        _on = nc.get("OS Name", {})
        if isinstance(_on, dict) and not topo["os_name"]:
            raw_os = _on.get("raw")
            if raw_os:
                topo["os_name"] = str(raw_os)
        for _dk in ("Disk Total", "Total Disk", "Disk Size"):
            _dv = nc.get(_dk, {})
            if isinstance(_dv, dict):
                b = _to_bytes(_dv.get("raw"))
                if b:
                    disk_total_values.append(b)
                    break
        for _dk in ("Disk Used", "Disk Usage"):
            _dv = nc.get(_dk, {})
            if isinstance(_dv, dict):
                b = _to_bytes(_dv.get("raw"))
                if b:
                    disk_used_values.append(b)
                    break
        for _sk in ("Swap Used", "Swap Usage"):
            _sv = nc.get(_sk, {})
            if isinstance(_sv, dict):
                _sw_raw = _sv.get("raw")
                _sw_kb = None
                if isinstance(_sw_raw, dict):
                    _sw_kb = _sw_raw.get("used")
                elif isinstance(_sw_raw, (int, float)):
                    _sw_kb = int(_sw_raw)
                if _sw_kb is not None:
                    swap_used_values.append(int(_sw_kb))
                break
        for _sk in ("Data Size", "Data Used", "Couch Docs Actual Disk Size"):
            _sv = nc.get(_sk, {})
            if isinstance(_sv, dict):
                b = _to_bytes(_sv.get("raw"))
                if b:
                    data_size_values.append(b)
                    break
        for _ik in ("Items", "Item Count", "Curr Items"):
            _iv = nc.get(_ik, {})
            if isinstance(_iv, dict):
                raw_items = _iv.get("raw")
                if isinstance(raw_items, (int, float)):
                    item_count_values.append(int(raw_items))
                    break
        for _ck_name, _ck_val in nc.items():
            if not isinstance(_ck_val, dict):
                continue
            st = _ck_val.get("status", "").upper()
            if st in ("ALERT", "BAD"):
                bad_count += 1
                bad_names.add(_normalize_checker_name(_ck_name))
            elif st in ("WARN", "WARNING"):
                warn_count += 1
                warn_names.add(_normalize_checker_name(_ck_name))

    _node_checker_keys_logged = False
    if isinstance(node_list, list):
        for ne in node_list:
            if isinstance(ne, dict):
                _nc = ne.get("results", {}).get("checkers", {})
                if not _node_checker_keys_logged and _nc:
                    topo["raw_fields"]["_node_checker_keys"] = ", ".join(sorted(_nc.keys())[:30])
                    _node_checker_keys_logged = True
                _process_node_checkers(_nc)
    elif isinstance(node_list, dict):
        for nd in node_list.values():
            if isinstance(nd, dict):
                _nc = nd.get("checkers", {})
                if not _node_checker_keys_logged and _nc:
                    topo["raw_fields"]["_node_checker_keys"] = ", ".join(sorted(_nc.keys())[:30])
                    _node_checker_keys_logged = True
                _process_node_checkers(_nc)

    for _ck_name, _ck_val in cluster_checkers.items():
        if not isinstance(_ck_val, dict):
            continue
        st = _ck_val.get("status", "").upper()
        if st in ("ALERT", "BAD"):
            bad_count += 1
            bad_names.add(_normalize_checker_name(_ck_name))
        elif st in ("WARN", "WARNING"):
            warn_count += 1
            warn_names.add(_normalize_checker_name(_ck_name))

    if cb_versions:
        from collections import Counter as _Counter
        topo["cb_version"] = _Counter(cb_versions).most_common(1)[0][0]
    if ram_values:
        topo["ram_per_node_mib"] = int(sorted(ram_values)[len(ram_values) // 2])
    if cpu_values:
        topo["cpus_per_node"] = int(sorted(cpu_values)[len(cpu_values) // 2])
    if ram_used_values:
        topo["ram_used_per_node_mib"] = round(sorted(ram_used_values)[len(ram_used_values) // 2])

    def _median_mb(byte_list: list[int]) -> int | None:
        if not byte_list:
            return None
        return round(sorted(byte_list)[len(byte_list) // 2] / (1024 * 1024))

    if disk_total_values:
        topo["disk_total_per_node_mib"] = _median_mb(disk_total_values)
    if disk_used_values:
        topo["disk_used_per_node_mib"] = _median_mb(disk_used_values)
    if swap_used_values:
        # swap_used_values are in kibibytes — convert to MiB (÷ 1024, not ÷ 1M)
        _sw_median_kb = sorted(swap_used_values)[len(swap_used_values) // 2]
        topo["swap_used_per_node_mib"] = round(_sw_median_kb / 1024)
    if data_size_values:
        topo["data_size_mib"] = round(sum(data_size_values) / (1024 * 1024))
    if item_count_values:
        topo["total_items"] = sum(item_count_values)

    topo["bad_count"]  = bad_count
    topo["warn_count"] = warn_count
    topo["bad_items"]  = sorted(bad_names)
    topo["warn_items"] = sorted(warn_names)

    return topo


# ---------------------------------------------------------------------------
# REST API topology fetch
# ---------------------------------------------------------------------------

def fetch_snapshot_topology_api(
    snap_uid_enc: str,
    session: requests.Session,
) -> dict:
    """
    Fetch full cluster topology via the snapshot nutshell endpoints.

    Primary:  GET /snapshot/{enc}/nutshell
    Fallback: GET /api/snapshots/{enc}/nutshell/summary  (cbs.* format)
              + GET /api/snapshots/{enc}/nutshell/results?scopeType=cluster
    """
    snap_base = f"{BASE_URL}/snapshot/{snap_uid_enc}"
    api_base  = f"{BASE_URL}/api/snapshots/{snap_uid_enc}"

    # ── Primary: /snapshot/{enc}/nutshell ────────────────────────────────────
    try:
        r = session.get(f"{snap_base}/nutshell", timeout=30)
        if r.status_code == 200:
            body = r.json()
            if isinstance(body, dict) and ("nutshell_beta_output" in body or "nutshell" in body):
                parsed = _parse_structured_api_json(body)
                if parsed.get("total_nodes") or parsed.get("cluster_uuid") or parsed.get("cluster_name"):
                    if parsed.get("cpus_per_node") is None or parsed.get("ram_per_node_mib") is None:
                        try:
                            _sr = session.get(f"{api_base}/nutshell/summary", timeout=15)
                            if _sr.status_code == 200:
                                _summ = _sr.json()
                                _cbs = _summ.get("cbs") if isinstance(_summ.get("cbs"), dict) else None
                                if _cbs:
                                    _nodes = _cbs.get("nodes") or {}
                                    _first = next(iter(_nodes.values()), {})
                                    _hw = _first.get("hardwareStats") or {}
                                    if parsed.get("cpus_per_node") is None and _hw.get("cpuCores"):
                                        parsed["cpus_per_node"] = _hw["cpuCores"]
                                    if parsed.get("ram_per_node_mib") is None:
                                        _mem = _hw.get("memLimit")
                                        if _mem:
                                            parsed["ram_per_node_mib"] = round(_mem / (1024 * 1024))
                                    if not parsed.get("os_name"):
                                        _osd = _first.get("osDetails") or {}
                                        parsed["os_name"] = _osd.get("name") or _osd.get("type")
                        except Exception:
                            pass
                    return parsed
    except Exception:
        pass

    # ── Fallback: /api/snapshots/{enc}/nutshell/summary (cbs.* format) ───────
    topo: dict = {
        "cluster_name": None, "cluster_uuid": None, "capella_cluster_id": None,
        "cluster_hostname": None, "total_nodes": None,
        "data_nodes": 0, "query_nodes": 0, "index_nodes": 0,
        "fts_nodes": 0, "eventing_nodes": 0, "analytics_nodes": 0, "backup_nodes": 0,
        "cb_version": None, "ram_per_node_mib": None, "cpus_per_node": None,
        "os_name": None, "bucket_count": 0, "bucket_names": [],
        "total_bucket_quota_mb": None, "server_groups": [],
        "auto_failover_seconds": None, "orchestrator": None,
        "ldap_enabled": None, "bad_count": 0, "warn_count": 0,
        "bad_items": [], "warn_items": [],
        "raw_fields": {},
    }
    try:
        r = session.get(f"{api_base}/nutshell/summary", timeout=20)
        if r.status_code == 200:
            summ = r.json()
            if isinstance(summ, dict):
                cbs = summ.get("cbs") if isinstance(summ.get("cbs"), dict) else None
                if cbs:
                    nodes = cbs.get("nodes") or {}
                    topo["cluster_uuid"] = cbs.get("clusterUuid")
                    topo["cluster_name"] = cbs.get("clusterUiName")
                    topo["total_nodes"]  = cbs.get("clusterSize") or len(nodes)
                    first = next(iter(nodes.values()), {})
                    topo["cb_version"]   = first.get("serverVersion")
                    hw = first.get("hardwareStats") or {}
                    mem = hw.get("memLimit")
                    if mem:
                        topo["ram_per_node_mib"] = round(mem / (1024 * 1024))
                    topo["cpus_per_node"] = hw.get("cpuCores")
                    os_d = first.get("osDetails") or {}
                    topo["os_name"] = os_d.get("name") or os_d.get("type")
                    _SVC_MAP = {"kv": "data", "n1ql": "query", "index": "index",
                                "fts": "fts", "eventing": "eventing",
                                "cbas": "analytics", "backup": "backup"}
                    svc_counts: dict = {}
                    for node in nodes.values():
                        for svc in (node.get("services") or []):
                            svc_counts[svc] = svc_counts.get(svc, 0) + 1
                    for raw_svc, canon in _SVC_MAP.items():
                        if svc_counts.get(raw_svc):
                            topo[f"{canon}_nodes"] = svc_counts[raw_svc]
    except Exception as exc:
        topo["raw_fields"]["summary_error"] = str(exc)

    try:
        r = session.get(f"{api_base}/nutshell/results", params={"scopeType": "cluster"}, timeout=20)
        if r.status_code == 200:
            res = r.json()
            if isinstance(res, dict):
                results = res.get("results") or {}
                bad_items, warn_items = [], []
                for rule_id, entry in (results.items() if isinstance(results, dict) else []):
                    if not isinstance(entry, dict):
                        continue
                    sev = (entry.get("severity") or "").upper()
                    if sev in ("BAD", "ERROR"):
                        bad_items.append(rule_id)
                    elif sev == "WARN":
                        warn_items.append(rule_id)
                topo["bad_count"]  = len(bad_items)
                topo["warn_count"] = len(warn_items)
                topo["bad_items"]  = bad_items
                topo["warn_items"] = warn_items
    except Exception as exc:
        topo["raw_fields"]["results_error"] = str(exc)

    return topo


# ---------------------------------------------------------------------------
# Full topology fetch (multi-strategy)
# ---------------------------------------------------------------------------

def fetch_snapshot_topology(snap_id: str, cookie: str | None = None) -> dict:
    """
    Fetch a Supportal snapshot page and extract Couchbase cluster topology.

    Strategy (in order):
      1. requests + cookie → check for plain-text checker markers
      2. REST API nutshell endpoints
      3. Returns empty dict on complete failure (Playwright removed as fallback)

    Returns an empty dict on complete failure.
    """
    url = f"{BASE_URL}/snapshot/{urllib.parse.quote(snap_id, safe='')}"
    _snap_log: list[str] = []

    # ── Strategy 1: requests fast-path (plain-text checker format only) ────────
    if cookie:
        try:
            sess = requests.Session()
            sess.headers.update({"User-Agent": UA, "Accept": "text/html,*/*", "Cookie": cookie})
            resp = sess.get(url, timeout=15, allow_redirects=True, verify=False)
            resp.raise_for_status()
            raw_html = resp.text
            _snap_log.append(f"s1: status={resp.status_code} len={len(raw_html)}")

            if "===Checker results" in raw_html or "===Cluster" in raw_html:
                _snap_log.append("s1: plain-text checker detected in raw HTML → text parser")
                _write_snap_debug(snap_id, "s1_raw", raw_html)
                return _parse_snapshot_checker_text(raw_html)
            soup_fast = BeautifulSoup(raw_html, "html.parser")
            body_text = soup_fast.get_text("\n")
            if "===Checker results" in body_text or "===Cluster" in body_text:
                _snap_log.append("s1: plain-text checker detected in body text → text parser")
                _write_snap_debug(snap_id, "s1_body", body_text)
                return _parse_snapshot_checker_text(body_text)
            _snap_log.append("s1: no text-format checker markers — skipping to Playwright")
        except Exception as _e:
            _snap_log.append(f"s1: exception {_e}")

    # ── Strategy 2: REST API nutshell endpoints ─────────────────────────────────
    if cookie:
        try:
            snap_uid_enc = urllib.parse.quote(snap_id, safe="")
            sess = _make_api_session(cookie)
            api_topo = fetch_snapshot_topology_api(snap_uid_enc, sess)
            if api_topo.get("total_nodes") or api_topo.get("cluster_uuid") or api_topo.get("cluster_name"):
                _snap_log.append(
                    f"s2: API topo: cluster_name={api_topo.get('cluster_name')} "
                    f"nodes={api_topo.get('total_nodes')} cb_ver={api_topo.get('cb_version')}"
                )
                _write_snap_debug(snap_id, "summary", "\n".join(_snap_log))
                return api_topo
            _snap_log.append("s2: API returned no usable topology")
        except Exception as _e:
            _snap_log.append(f"s2: exception {type(_e).__name__}: {_e}")

    _write_snap_debug(snap_id, "summary", "\n".join(_snap_log))
    return {}


# ---------------------------------------------------------------------------
# Ticket snapshot enrichment
# ---------------------------------------------------------------------------

def enrich_tickets_with_snapshots(
    tickets: list[dict],
    cookie: str | None,
    progress_cb: Callable[[str, float], None],
    cancel_event,
    max_workers: int = 4,
    snap_upsert_fn: "Callable[[dict], None] | None" = None,
) -> tuple[int, int]:
    """
    Pipeline step: for each ticket with snapshot IDs, fetch the most-recent snapshot,
    parse the cluster topology, and merge it into the ticket dict in-place.

    Also sets ticket["snap_ids"] (all IDs found in the ticket text) and
    ticket["snapshot_summary"] (lightweight flattened fields for display).

    If snap_upsert_fn is provided it is called with a complete snapshot dict so the
    caller can persist snapshots to Couchbase during the same pass.

    Returns (enriched_count, error_count).
    """
    with_snaps = [t for t in tickets if t.get("snapshots")]
    total = len(with_snaps)
    if total == 0:
        progress_cb("No tickets with snapshots — skipping enrichment.", 1.0)
        return 0, 0

    for ticket in with_snaps:
        _sv = ticket.get("snapshots")
        all_found = _SNAP_ID_RE.findall(_sv if isinstance(_sv, str) else "")
        if all_found:
            deduped: list[str] = []
            for sid in all_found:
                if sid not in deduped:
                    deduped.append(sid)
            ticket["snap_ids"] = deduped

    snap_to_tickets: dict[str, list[dict]] = {}
    for ticket in with_snaps:
        _sv2 = ticket.get("snapshots")
        snap_ids = _SNAP_ID_RE.findall(_sv2 if isinstance(_sv2, str) else "")
        if not snap_ids:
            continue
        snap_id = _highest_snap_id(snap_ids)
        snap_to_tickets.setdefault(snap_id, []).append(ticket)

    unique_snaps = list(snap_to_tickets.keys())
    total_unique = len(unique_snaps)
    if total_unique == 0:
        progress_cb("No parseable snapshot IDs found — skipping enrichment.", 1.0)
        return 0, 0

    progress_cb(
        f"Fetching {total_unique} unique snapshots across {total} tickets "
        f"({max_workers} parallel workers)…", 0.0
    )

    enriched = errors = 0
    completed = 0
    lock = threading.Lock()

    def _fetch_one(snap_id: str):
        nonlocal enriched, errors, completed
        if cancel_event and cancel_event.is_set():
            return
        try:
            topo = fetch_snapshot_topology(snap_id, cookie=cookie)
        except Exception as exc:
            with lock:
                errors += 1
                completed += 1
                pct = completed / total_unique
                progress_cb(f"Snapshot {snap_id[:16]}… error: {exc}", pct)
            return

        with lock:
            completed += 1
            pct = completed / total_unique
            if topo:
                summary = {
                    "snap_id":      snap_id,
                    "cluster_name": topo.get("cluster_name") or "",
                    "cb_version":   topo.get("cb_version") or "",
                    "bad_count":    topo.get("bad_count", 0),
                    "warn_count":   topo.get("warn_count", 0),
                    "node_count":   topo.get("total_nodes", 0),
                }
                ticket_ids_for_snap: list[str] = []
                for ticket in snap_to_tickets[snap_id]:
                    ticket["snapshot_topology"] = topo
                    ticket["snapshot_summary"]  = summary
                    tid = ticket.get("ticket_id") or ticket.get("id", "")
                    if tid and tid not in ticket_ids_for_snap:
                        ticket_ids_for_snap.append(str(tid))
                enriched += len(snap_to_tickets[snap_id])
                if snap_upsert_fn is not None:
                    first_ticket = snap_to_tickets[snap_id][0]
                    now_iso = (
                        datetime.datetime.now(datetime.timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                    snap_doc = {
                        "snap_id":            snap_id,
                        "cluster_id":         snap_id.split("::")[0],
                        "url":                f"{BASE_URL}/snapshot/{snap_id}",
                        "date":               None,
                        "organization":       first_ticket.get("organization") or "",
                        "customer_url":       first_ticket.get("customer_url") or "",
                        "cluster_name":       topo.get("cluster_name") or "",
                        "cluster_uuid":       topo.get("cluster_uuid") or "",
                        "capella_cluster_id": topo.get("capella_cluster_id") or "",
                        "topology":           topo,
                        "ticket_ids":         ticket_ids_for_snap,
                        "scraped_at":         now_iso,
                        "bad_count":          topo.get("bad_count", 0),
                        "warn_count":         topo.get("warn_count", 0),
                        "bad_items":          topo.get("bad_items") or [],
                        "warn_items":         topo.get("warn_items") or [],
                        "cb_version":         topo.get("cb_version") or "",
                        "node_count":         topo.get("total_nodes", 0),
                        "bucket_names":       topo.get("bucket_names") or [],
                        "auto_failover_seconds": topo.get("auto_failover_seconds"),
                        "ram_per_node_mib":   topo.get("ram_per_node_mib"),
                        "cpus_per_node":      topo.get("cpus_per_node"),
                        "os_name":            topo.get("os_name"),
                        "bucket_count":       topo.get("bucket_count", 0),
                        "server_groups":      topo.get("server_groups") or [],
                    }
                    try:
                        snap_upsert_fn(snap_doc)
                    except Exception:
                        pass
                progress_cb(
                    f"[{completed}/{total_unique}] Snapshot {snap_id[:16]}… OK"
                    f" ({len(snap_to_tickets[snap_id])} ticket(s))", pct
                )
            else:
                progress_cb(
                    f"[{completed}/{total_unique}] Snapshot {snap_id[:16]}… no data", pct
                )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, sid): sid for sid in unique_snaps}
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception:
                pass
            if cancel_event and cancel_event.is_set():
                break

    progress_cb(
        f"Enriched {enriched} ticket(s) from {total_unique} unique snapshots"
        + (f" ({errors} errors)" if errors else "") + ".",
        1.0,
    )
    return enriched, errors


# ---------------------------------------------------------------------------
# Snapshot listing scraper
# ---------------------------------------------------------------------------

def _find_snapshots_tab_url(html: str, current_url: str) -> Optional[str]:
    """Find the Snapshots tab link on a customer page."""
    soup = BeautifulSoup(html, "html.parser")
    customer_base = current_url.rstrip("/")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        abs_href = href if href.startswith("http") else urllib.parse.urljoin(current_url, href)
        if not abs_href.startswith(customer_base):
            continue
        if text == "snapshots" or re.search(r"/snapshots(?:/|\?|$|#)", abs_href, re.I):
            return abs_href
    return None


def _extract_snapshot_rows(html: str) -> list[dict]:
    """
    Extract snapshot summary rows from a Supportal snapshot listing page.
    Returns list of dicts with snap_id, url, date, cluster_name, cluster_id, ticket_ids.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        m = _SNAP_HREF_RE.search(a["href"])
        if not m:
            continue
        snap_id = m.group(1)
        if snap_id in seen:
            continue
        seen.add(snap_id)
        row: dict = {
            "snap_id":      snap_id,
            "url":          f"{BASE_URL}/snapshot/{snap_id}",
            "date":         None,
            "cluster_name": None,
            "cluster_id":   snap_id.split("::")[0],
            "ticket_ids":   [],
        }
        tr = a
        for _ in range(6):
            if tr is None or tr.name == "tr":
                break
            tr = tr.parent
        if tr and tr.name == "tr":
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            for cell in cells:
                if not cell:
                    continue
                if re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", cell) and not row["date"]:
                    row["date"] = cell
                elif (
                    not re.match(r"^[0-9a-f]{32}::\d+$", cell, re.I)
                    and not cell.isdigit()
                    and len(cell) > 2
                    and cell.lower() not in ("open", "pending", "solved", "closed", "snapshots")
                    and not row["cluster_name"]
                ):
                    row["cluster_name"] = cell
            for a_link in tr.find_all("a", href=True):
                href = a_link.get("href", "")
                m_zd = re.search(r"/zendesk/ticket/(\d+)", href)
                if m_zd:
                    zd_id = f"ZD-{m_zd.group(1)}"
                    if zd_id not in row["ticket_ids"]:
                        row["ticket_ids"].append(zd_id)
                else:
                    txt = a_link.get_text(strip=True)
                    if re.match(r"^ZD-\d+$", txt) and txt not in row["ticket_ids"]:
                        row["ticket_ids"].append(txt)
        out.append(row)
    return out


def scrape_snapshots_from_stubs(
    stubs: list[dict],
    cookie: str | None,
    max_detail_workers: int,
    progress_cb: Callable[[str, float], None],
) -> list[dict]:
    """Fetch full topology for analytics-fetched stubs via REST API (no Playwright)."""
    def log(msg: str, pct: float):
        print(f"[SNAP-DIRECT] {msg}")
        progress_cb(msg, pct)

    if not stubs:
        log("No stubs to scrape.", 1.0)
        return []

    total = len(stubs)
    log(f"Fetching topology for {total} snapshots via REST API…", 0.0)

    session = _make_api_session(cookie) if cookie else None
    results: list[dict] = []
    done_n = [0]
    errors_n = [0]
    lock = threading.Lock()

    def _fetch_one(stub: dict) -> None:
        snap_id = stub.get("snap_id", "")
        if not snap_id:
            return
        try:
            enc = urllib.parse.quote(snap_id, safe="")
            topo = fetch_snapshot_topology_api(enc, session) if session else {}
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            doc = {
                "snap_id":            snap_id,
                "cluster_id":         snap_id.split("::")[0],
                "url":                f"{BASE_URL}/snapshot/{enc}",
                "date":               stub.get("date"),
                "organization":       stub.get("organization", ""),
                "customer_url":       stub.get("customer_url", ""),
                "cluster_name":       topo.get("cluster_name") or stub.get("cluster_name") or "",
                "cluster_uuid":       topo.get("cluster_uuid") or stub.get("cluster_uuid") or "",
                "capella_cluster_id": topo.get("capella_cluster_id") or "",
                "topology":           topo,
                "ticket_ids":         stub.get("ticket_ids") or [],
                "scraped_at":         now_iso,
                "bad_count":          topo.get("bad_count", 0),
                "warn_count":         topo.get("warn_count", 0),
                "bad_items":          topo.get("bad_items") or [],
                "warn_items":         topo.get("warn_items") or [],
                "cb_version":         topo.get("cb_version") or "",
                "node_count":         topo.get("total_nodes") or 0,
                "bucket_names":       topo.get("bucket_names") or [],
                "auto_failover_seconds": topo.get("auto_failover_seconds"),
                "ram_per_node_mib":   topo.get("ram_per_node_mib"),
                "bucket_count":       topo.get("bucket_count", 0),
                "server_groups":      topo.get("server_groups") or [],
            }
            with lock:
                done_n[0] += 1
                results.append(doc)
                log(f"[{done_n[0]}/{total}] {snap_id[:16]}… bad={doc['bad_count']} warn={doc['warn_count']}",
                    done_n[0] / total)
        except Exception as exc:
            with lock:
                done_n[0] += 1
                errors_n[0] += 1
                log(f"[{done_n[0]}/{total}] Error {snap_id[:16]}…: {exc}", done_n[0] / total)

    workers = max(1, min(max_detail_workers, total))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(_fetch_one, stubs))

    log(f"Done — {len(results)} scraped, {errors_n[0]} errors.", 1.0)
    return results


def scrape_snapshots_for_customer(
    customer: str,
    cookie: Optional[str],
    max_listing_pages: int,
    max_detail_workers: int,
    progress_cb: Callable[[str, float], None],
    skip_ids: Optional[set] = None,
    max_snapshots: int = 0,
) -> list[dict]:
    """
    Full snapshot scrape for a customer:
      1. Enumerate snapshot listing (paginated) → snap summaries
      2. Fetch full topology for each new snapshot in parallel
      3. Return list of snapshot dicts ready for storage
    """
    skip_ids = skip_ids or set()
    customer = customer.strip().strip('"\'')
    customer_url = (
        customer if customer.startswith("http")
        else f"{BASE_URL}/customer/{urllib.parse.quote(customer, safe='')}"
    )

    def log(msg: str, pct: float):
        print(f"[SNAP] {msg}")
        progress_cb(msg, pct)

    # ── Phase 1: enumerate listing via REST API ────────────────────────────
    log("Enumerating snapshot listing…", 0.0)
    org_name = customer
    if customer.startswith("http"):
        _path_parts = urllib.parse.urlparse(customer).path.strip("/").split("/")
        if len(_path_parts) >= 2 and _path_parts[0] == "customer":
            org_name = urllib.parse.unquote(_path_parts[1])
    from supportal.api_client import _get_customer_snapshot_listing_api  # noqa: PLC0415
    raw_snaps = _get_customer_snapshot_listing_api(org_name, _make_api_session(cookie)) if cookie else []
    summaries: list[dict] = []
    for _snap in raw_snaps:
        _sid = _snap.get("snap_id") or _snap.get("id") or _snap.get("uid")
        if not _sid:
            _enc = _snap.get("encoded_uid", "")
            if _enc:
                _sid = urllib.parse.unquote(_enc)
        if _sid:
            summaries.append({
                "snap_id":      _sid,
                "url":          _snap.get("url") or f"{BASE_URL}/snapshot/{urllib.parse.quote(_sid, safe='')}",
                "date":         _snap.get("date") or _snap.get("created_at"),
                "cluster_name": _snap.get("cluster_name") or _snap.get("name") or "",
                "ticket_ids":   _snap.get("ticket_ids") or [],
            })

    new_summaries = [s for s in summaries if s["snap_id"] not in skip_ids]
    log(f"Listing: {len(summaries)} snapshots found, {len(new_summaries)} new", 0.40)
    if max_snapshots > 0 and len(new_summaries) > max_snapshots:
        log(f"Capping at {max_snapshots} snapshots (of {len(new_summaries)} new).", 0.40)
        new_summaries = new_summaries[:max_snapshots]
    if not new_summaries:
        log("No new snapshots to fetch.", 1.0)
        return []

    # ── Phase 2: fetch topology for each snapshot ──────────────────────────
    results: list[dict] = []
    total = len(new_summaries)
    done_n = [0]
    errors_n = [0]
    lock = threading.Lock()

    def _fetch_one(summary: dict) -> None:
        snap_id = summary["snap_id"]
        try:
            topo = fetch_snapshot_topology(snap_id, cookie)
            now_iso = (
                datetime.datetime.now(datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            doc = {
                "snap_id":           snap_id,
                "cluster_id":        snap_id.split("::")[0],
                "url":               summary["url"],
                "date":              summary.get("date"),
                "organization":      customer,
                "customer_url":      customer_url,
                "cluster_name":      topo.get("cluster_name") or summary.get("cluster_name") or "",
                "cluster_uuid":      topo.get("cluster_uuid") or "",
                "capella_cluster_id": topo.get("capella_cluster_id") or "",
                "topology":          topo,
                "ticket_ids":        summary.get("ticket_ids") or [],
                "scraped_at":        now_iso,
                "bad_count":         topo.get("bad_count", 0),
                "warn_count":        topo.get("warn_count", 0),
                "bad_items":         topo.get("bad_items") or [],
                "warn_items":        topo.get("warn_items") or [],
                "cb_version":        topo.get("cb_version") or "",
                "node_count":        topo.get("total_nodes") or 0,
                "bucket_names":      topo.get("bucket_names") or [],
                "auto_failover_seconds": topo.get("auto_failover_seconds"),
                "ram_per_node_mib":  topo.get("ram_per_node_mib"),
                "bucket_count":      topo.get("bucket_count", 0),
                "server_groups":     topo.get("server_groups") or [],
            }
            with lock:
                done_n[0] += 1
                results.append(doc)
                pct = 0.40 + 0.58 * (done_n[0] / total)
                log(f"[{done_n[0]}/{total}] {snap_id[:16]}… bad={doc['bad_count']} warn={doc['warn_count']}", pct)
        except Exception as exc:
            with lock:
                done_n[0] += 1
                errors_n[0] += 1
                log(f"[{done_n[0]}/{total}] Error {snap_id[:16]}…: {exc}",
                    0.40 + 0.58 * (done_n[0] / total))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_detail_workers) as pool:
        list(pool.map(_fetch_one, new_summaries))

    log(f"Done: {len(results)} snapshots fetched, {errors_n[0]} errors.", 1.0)
    return results
