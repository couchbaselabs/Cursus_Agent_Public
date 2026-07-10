"""
Couchbase SDK helpers, embedding utilities, and retrieval pipeline.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import json
import re
import threading
import time
from typing import Callable

import requests

# ── Couchbase SDK — optional ─────────────────────────────────────────────────
try:
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.exceptions import CouchbaseException
    from couchbase.options import ClusterOptions, QueryOptions, SearchOptions
    from couchbase.search import SearchRequest, MatchQuery, DisjunctionQuery
    from couchbase.vector_search import VectorQuery, VectorSearch
    from datetime import timedelta
    _CB_AVAILABLE = True
except ImportError:
    _CB_AVAILABLE = False

# ── MLX embeddings — optional ────────────────────────────────────────────────
try:
    from mlx_embeddings import load as _mlx_emb_load
    import mlx.core as _mx
    try:
        from mlx_embeddings import pool_embeddings as _mlx_pool
    except ImportError:
        _mlx_pool = None
    _MLX_EMB_AVAILABLE = True
    _MLX_EMB_IMPORT_ERROR = None
except Exception as _mlx_err:
    _mlx_emb_load = None
    _mx = None
    _mlx_pool = None
    _MLX_EMB_AVAILABLE = False
    _MLX_EMB_IMPORT_ERROR = str(_mlx_err)

# ── OpenAI / Gemini — optional (used by embed_text) ─────────────────────────
try:
    import openai as _openai_mod
    _OPENAI_AVAILABLE = True
except ImportError:
    _openai_mod = None
    _OPENAI_AVAILABLE = False

try:
    from google import genai as _genai_mod
    _GEMINI_AVAILABLE = True
except ImportError:
    _genai_mod = None
    _GEMINI_AVAILABLE = False

# ── Inter-module imports ──────────────────────────────────────────────────────
from supportal.scoring import (
    _openai_base_url,
    _get_openai_client,
    _get_cluster_to_app,
    _get_app_cluster_aliases,
    _cluster_app_dynamic,
    _app_cluster_dynamic,
    _ticket_date,
    _ticket_cluster_ids,
    prefilter_for_query,
)
from supportal.snapshot_parser import _topo_str
from supportal.ticket_parser import (
    _normalize_field_key,
    _parse_ticket_fields,
    _extract_ticket_ids,
)

# ── MLX model cache (one loaded model reused across calls) ───────────────────
_mlx_emb_cache: dict = {"model": None, "tokenizer": None, "model_id": None}


# ── Connection helpers ────────────────────────────────────────────────────────

def _cb_conn_str(cb_url: str, use_tls: bool) -> str:
    """Build a couchbase[s]:// connection string from whatever the user typed."""
    host = re.sub(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", "", cb_url).strip().rstrip("/")
    scheme = "couchbases" if use_tls else "couchbase"
    return f"{scheme}://{host}"


def _cb_kv_get_multi(col, doc_ids: list[str], batch_size: int = 100) -> list[dict]:
    """Batch-fetch full documents from a Collection by key using KV get_multi.

    Returns documents in the same order as doc_ids; silently skips missing keys.
    """
    from couchbase.options import GetMultiOptions  # type: ignore
    docs_by_key: dict[str, dict] = {}
    for i in range(0, len(doc_ids), batch_size):
        batch = doc_ids[i : i + batch_size]
        try:
            result = col.get_multi(batch, GetMultiOptions(return_exceptions=True))
            for key, res in result.results.items():
                if not isinstance(res, Exception):
                    try:
                        docs_by_key[key] = res.content_as[dict]
                    except Exception:
                        pass
        except Exception as exc:
            print(f"[_cb_kv_get_multi] batch {i//batch_size} failed: {exc}")
    return [docs_by_key[k] for k in doc_ids if k in docs_by_key]


# ── Org search ───────────────────────────────────────────────────────────────

def search_orgs_from_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    query_str: str, limit: int = 50,
) -> list[str]:
    """Return org names containing query_str (case-insensitive), capped at limit."""
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed")
    conn_str = _cb_conn_str(cb_url, use_tls)
    cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    keyspace = f"`{bucket}`.`{scope}`.`{collection}`"
    rows = list(cluster.query(
        f"SELECT DISTINCT RAW t.organization FROM {keyspace} AS t "
        f"WHERE META(t).id LIKE 'ticket::%' "
        f"AND t.organization IS NOT MISSING "
        f"AND LOWER(t.organization) LIKE $1 "
        f"LIMIT {limit}",
        QueryOptions(
            positional_parameters=[f"%{query_str.lower()}%"],
            timeout=timedelta(seconds=30),
        ),
    ))
    cluster.close()
    return sorted({str(r).strip() for r in rows if r and str(r).strip()})


def load_tickets_for_orgs_from_cb(
    orgs: list[str],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    progress_cb: Callable[[str, float], None],
) -> list[dict]:
    """Load tickets for a specific set of org names using ID-first + KV fetch."""
    if not _CB_AVAILABLE or not orgs:
        return []
    conn_str = _cb_conn_str(cb_url, use_tls)
    cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    keyspace     = f"`{bucket}`.`{scope}`.`{collection}`"
    placeholders = ", ".join(f"${i + 1}" for i in range(len(orgs)))
    progress_cb(f"Querying {len(orgs)} customer(s) …", 0.1)
    id_rows = list(cluster.query(
        f"SELECT META(t).id AS id FROM {keyspace} AS t "
        f"WHERE t.organization IN [{placeholders}]",
        QueryOptions(positional_parameters=orgs, timeout=timedelta(seconds=60)),
    ))
    doc_ids = [r["id"] for r in id_rows if r.get("id")]
    progress_cb(f"Fetching {len(doc_ids)} ticket(s) via KV …", 0.5)
    col  = cluster.bucket(bucket).scope(scope).collection(collection)
    rows = _cb_kv_get_multi(col, doc_ids)
    cluster.close()
    progress_cb(f"Loaded {len(rows)} tickets.", 1.0)
    return rows


# ── Ticket loading ────────────────────────────────────────────────────────────

def load_tickets_from_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    customer_filter: str,
    progress_cb: Callable[[str, float], None],
    summary_collection: str = "summary",
) -> list[dict]:
    """Query tickets from Couchbase via SQL++ and return them as a list of dicts."""
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed")

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb(f"Connecting to {conn_str} …", 0.0)
    cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))

    _order = (
        "CASE WHEN LOWER(t.status) IN [\"closed\",\"solved\"] THEN 1 ELSE 0 END ASC, "
        "CASE LOWER(t.priority) "
        "WHEN 'urgent' THEN 0 WHEN 'p1' THEN 0 "
        "WHEN 'high'   THEN 1 WHEN 'p2' THEN 1 "
        "WHEN 'normal' THEN 2 WHEN 'p3' THEN 2 WHEN 'medium' THEN 2 "
        "WHEN 'low'    THEN 3 WHEN 'p4' THEN 3 "
        "ELSE 4 END ASC, "
        "t.created DESC"
    )
    keyspace = f"`{bucket}`.`{scope}`.`{collection}`"
    _terms = [t.strip() for t in customer_filter.split(",") if t.strip()]
    _not_deleted = "(t.`_deleted` IS MISSING OR t.`_deleted` = false)"
    if _terms:
        _clauses = " OR ".join(
            f"LOWER(t.organization) LIKE ${i+1}" for i in range(len(_terms))
        )
        id_query = (f"SELECT META(t).id AS id FROM {keyspace} AS t "
                    f"WHERE t.ticket_id IS NOT MISSING "
                    f"AND {_not_deleted} "
                    f"AND ({_clauses}) "
                    f"ORDER BY {_order}")
        opts = QueryOptions(positional_parameters=[f"%{t.lower()}%" for t in _terms])
    else:
        id_query = (f"SELECT META(t).id AS id FROM {keyspace} AS t "
                    f"WHERE t.ticket_id IS NOT MISSING "
                    f"AND {_not_deleted} "
                    f"ORDER BY {_order}")
        opts = QueryOptions()

    progress_cb("Running query …", 0.1)
    id_rows = list(cluster.query(id_query, opts))
    doc_ids = [r["id"] for r in id_rows if r.get("id")]

    progress_cb(f"Fetching {len(doc_ids)} ticket(s) via KV …", 0.5)
    col     = cluster.bucket(bucket).scope(scope).collection(collection)
    tickets = _cb_kv_get_multi(col, doc_ids)

    if summary_collection and tickets:
        progress_cb("Loading ticket summaries …", 0.95)
        try:
            sum_ks = f"`{bucket}`.`{scope}`.`{summary_collection}`"
            sum_rows = list(cluster.query(
                f"SELECT ticket_id, summary_text, health, resolution, cluster_name, cb_version "
                f"FROM {sum_ks} WHERE type = 'ticket_summary' "
                f"AND summary_text IS NOT NULL AND summary_text != ''",
                QueryOptions(timeout=timedelta(seconds=60)),
            ))
            sum_map = {str(r["ticket_id"]): r for r in sum_rows if r.get("ticket_id")}
            for t in tickets:
                s = sum_map.get(str(t.get("ticket_id", "")))
                if s:
                    t["summary_text"]       = s.get("summary_text", "")
                    t["summary_health"]     = s.get("health")
                    t["summary_resolution"] = s.get("resolution")
        except Exception as _sum_exc:
            print(f"[load_tickets_from_cb] summary enrich skipped: {_sum_exc}")

    cluster.close()
    progress_cb(f"Loaded {len(tickets)} tickets.", 1.0)
    return tickets


def fetch_tickets_by_keys(
    doc_keys: list[str],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
) -> list[dict]:
    """Fetch ticket documents from Couchbase by their document keys via KV get_multi."""
    if not _CB_AVAILABLE or not doc_keys:
        return []
    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cluster.wait_until_ready(timedelta(seconds=15))
        col = cluster.bucket(bucket).scope(scope).collection(collection)
        tickets = _cb_kv_get_multi(col, list(doc_keys))
        cluster.close()
        return tickets
    except Exception as exc:
        print(f"[fetch_tickets_by_keys] CB fetch failed ({len(doc_keys)} keys): {exc}")
        return []


def _make_snap_col(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, snap_collection: str,
):
    """Open and return a Couchbase Collection for snapshots, or None on failure."""
    if not _CB_AVAILABLE:
        return None
    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cluster.wait_until_ready(timedelta(seconds=10))
        return cluster.bucket(bucket).scope(scope).collection(snap_collection)
    except Exception as exc:
        print(f"[_make_snap_col] Could not open snapshots collection: {exc}")
        return None


def load_to_couchbase(
    tickets: list[dict],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    progress_cb: Callable[[str, float], None],
    cancel_event=None,
) -> tuple[int, int]:
    """Upsert each ticket into Couchbase. Returns (upserted_count, error_count)."""
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed — run: venv/bin/pip install couchbase")

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb(f"Connecting to {conn_str} …", 0.0)
    cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    col = cluster.bucket(bucket).scope(scope).collection(collection)

    total = len(tickets)
    upserted = errors = 0
    _now = int(time.time())
    for i, ticket in enumerate(tickets, start=1):
        if cancel_event is not None and cancel_event.is_set():
            progress_cb("Cancelled.", i / total)
            break
        tid = ticket.get("ticket_id") or f"unknown_{i}"
        doc_key = f"ticket::{tid}"
        try:
            doc = ticket.copy()
            doc["last_scraped_at"] = _now
            doc["type"] = "ticket"
            col.upsert(doc_key, doc)
            upserted += 1
        except CouchbaseException as exc:
            errors += 1
            progress_cb(f"Error on {doc_key}: {exc}", i / total)
            continue
        if i % 25 == 0 or i == total:
            progress_cb(f"Upserted {i}/{total} …", i / total)

    cluster.close()
    return upserted, errors


# ── Embed text builders ───────────────────────────────────────────────────────

def build_embed_text(ticket: dict) -> str:
    """Concatenate all meaningful ticket fields into a text blob for embedding."""
    parts: list[str] = []

    if ticket.get("ticket_id"):
        parts.append(f"Ticket ID: {ticket['ticket_id']}")
    if ticket.get("organization"):
        parts.append(f"Customer: {ticket['organization']}")
    if ticket.get("subject"):
        parts.append(f"Subject: {ticket['subject']}")
        parts.append(f"Topic: {ticket['subject']}")

    _topo_for_alias = ticket.get("snapshot_topology") or {}
    if isinstance(_topo_for_alias, str):
        try:
            _topo_for_alias = json.loads(_topo_for_alias)
        except Exception:
            _topo_for_alias = {}
    _bn_for_alias = _topo_for_alias.get("bucket_names") or []
    _bn_str = " ".join(_bn_for_alias) if isinstance(_bn_for_alias, list) else str(_bn_for_alias)
    _text_for_alias = " ".join([
        ticket.get("subject") or "", ticket.get("description") or "", _bn_str,
    ]).lower()
    _injected_apps: set[str] = set()
    for _host, _app in _get_cluster_to_app().items():
        if _host in _text_for_alias and _app not in _injected_apps:
            if _app.lower() not in _text_for_alias:
                parts.append(f"Application: {_app.upper()}")
                _injected_apps.add(_app)

    _summary_text = (ticket.get("summary_text") or "").strip()
    if _summary_text:
        parts.append(f"Summary: {_summary_text}")

    _t_score   = ticket.get("score") or {}
    _t_summary = (ticket.get("interaction_summary") or _t_score.get("interaction_summary") or "").strip()
    if _t_summary and not _summary_text:
        parts.append(f"Summary: {_t_summary}")
    if ticket.get("status"):
        parts.append(f"Status: {ticket['status']}")
    if ticket.get("priority"):
        parts.append(f"Priority: {ticket['priority']}")
    if ticket.get("created"):
        parts.append(f"Date Created: {ticket['created']}")
    if ticket.get("solved"):
        parts.append(f"Date Solved: {ticket['solved']}")
    if ticket.get("requester"):
        parts.append(f"Requester: {ticket['requester']}")
    if ticket.get("assignee"):
        parts.append(f"Assignee: {ticket['assignee']}")

    ticket_ver = (ticket.get("cb_version") or "").strip()
    if ticket_ver and ticket_ver != "Unknown":
        parts.append(f"Couchbase Version (reported): {ticket_ver}")

    if ticket.get("feature_area"):
        parts.append(f"Feature Area: {ticket['feature_area']}")
    if ticket.get("ticket_origin"):
        parts.append(f"Ticket Origin: {ticket['ticket_origin']}")

    tags = (ticket.get("tags") or "").strip()
    if tags:
        parts.append(f"Tags: {tags}")

    _cbses_e = ticket.get("cbses") or []
    if _cbses_e:
        _cbses_str_e = ", ".join(_cbses_e) if isinstance(_cbses_e, list) else str(_cbses_e)
        parts.append(f"CBSEs: {_cbses_str_e}")
    _jiras_e = ticket.get("jira_issues") or []
    if _jiras_e:
        _jiras_str_e = ", ".join(_jiras_e) if isinstance(_jiras_e, list) else str(_jiras_e)
        parts.append(f"Jira Issues: {_jiras_str_e}")

    tf = _parse_ticket_fields(ticket)
    tf_lines = []
    for key, val in tf.items():
        val_str = (str(val) or "").strip()
        if val_str:
            label = key.replace("_", " ").title()
            tf_lines.append(f"  {label}: {val_str}")
    if tf_lines:
        parts.append("Ticket Fields:\n" + "\n".join(tf_lines))

    _esc = ticket.get("escalations")
    if _esc:
        parts.append(f"Escalations: {str(_esc)[:500]}")

    _score_e = ticket.get("score") or {}
    _cluster_names_e = _score_e.get("cluster_names") or []
    if _cluster_names_e:
        _cn_str = ", ".join(_cluster_names_e) if isinstance(_cluster_names_e, list) else str(_cluster_names_e)
        parts.append(f"Cluster Names: {_cn_str}")
    _app_labels_e = _score_e.get("analytics_app_labels") or []
    if _app_labels_e:
        _al_str = ", ".join(_app_labels_e) if isinstance(_app_labels_e, list) else str(_app_labels_e)
        parts.append(f"Application Labels: {_al_str}")

    topo = ticket.get("snapshot_topology")
    if topo:
        if isinstance(topo, str):
            try:
                topo = json.loads(topo)
            except Exception:
                topo = {}
    if isinstance(topo, dict) and topo:
        topo_lines = []
        if topo.get("cluster_name"):
            topo_lines.append(f"  Cluster Name: {topo['cluster_name']}")
        snap_ver = _topo_str(topo.get("cb_version"))
        if snap_ver:
            topo_lines.append(f"  Cluster CB Version (at snapshot): {snap_ver}")
        if topo.get("total_nodes"):
            topo_lines.append(f"  Nodes: {topo['total_nodes']}")
        if topo.get("bucket_count"):
            topo_lines.append(f"  Buckets: {topo['bucket_count']}")
        _bn = topo.get("bucket_names") or []
        if isinstance(_bn, list) and _bn:
            topo_lines.append(f"  Bucket Names: {', '.join(_bn[:20])}")
        elif isinstance(_bn, str) and _bn:
            topo_lines.append(f"  Bucket Names: {_bn}")
        if topo.get("ram_per_node_mib"):
            topo_lines.append(f"  RAM per Node: {topo['ram_per_node_mib']} MiB")
        if topo.get("auto_failover_seconds") is not None:
            topo_lines.append(f"  Auto-failover: {topo['auto_failover_seconds']}s")
        svc_parts = []
        for svc, key in [("KV/Data", "data_nodes"), ("Index", "index_nodes"),
                         ("Query", "query_nodes"), ("Search", "fts_nodes"),
                         ("Eventing", "eventing_nodes"), ("Analytics", "analytics_nodes")]:
            n = topo.get(key)
            if n:
                svc_parts.append(f"{svc}×{n}")
        if svc_parts:
            topo_lines.append(f"  Services: {', '.join(svc_parts)}")
        if topo.get("cpus_per_node"):
            topo_lines.append(f"  CPUs per Node: {topo['cpus_per_node']}")
        if topo.get("ram_used_per_node_mib") and topo.get("ram_per_node_mib"):
            topo_lines.append(f"  RAM Used per Node: {topo['ram_used_per_node_mib']} / {topo['ram_per_node_mib']} MiB")
        elif topo.get("ram_used_per_node_mib"):
            topo_lines.append(f"  RAM Used per Node: {topo['ram_used_per_node_mib']} MiB")
        if topo.get("os_name"):
            topo_lines.append(f"  OS: {topo['os_name']}")
        if topo.get("n2n_encryption") is not None:
            topo_lines.append(f"  N2N Encryption: {topo['n2n_encryption']}")
        if topo.get("data_quota_mib"):
            topo_lines.append(f"  Data Quota: {topo['data_quota_mib']} MiB")
        if topo.get("disk_total_per_node_mib"):
            _du = topo.get("disk_used_per_node_mib")
            _disk_str = f"  Disk per Node: {topo['disk_total_per_node_mib']} MiB total"
            if _du:
                _disk_str += f", {_du} MiB used"
            topo_lines.append(_disk_str)
        if topo.get("swap_used_per_node_mib"):
            topo_lines.append(f"  Swap Used per Node: {topo['swap_used_per_node_mib']} MiB")
        if topo.get("data_size_mib"):
            topo_lines.append(f"  Data Size (cluster): {topo['data_size_mib']} MiB")
        if topo.get("total_items"):
            topo_lines.append(f"  Total Items: {topo['total_items']:,}")
        _bdets = topo.get("bucket_details") or []
        if _bdets:
            _blines = []
            for _b in _bdets[:10]:
                _bp = _b.get("name", "")
                if _b.get("type"):
                    _bp += f" ({_b['type']})"
                if _b.get("quota_mb"):
                    _bp += f" {_b['quota_mb']}MB quota"
                if _b.get("replicas") is not None:
                    _bp += f" {_b['replicas']}r"
                if _b.get("storage_mode"):
                    _bp += f" {_b['storage_mode']}"
                if _b.get("compression"):
                    _bp += f" compress={_b['compression']}"
                if _b.get("items"):
                    _bp += f" {_b['items']} items"
                _blines.append(_bp)
            topo_lines.append(f"  Buckets: {'; '.join(_blines)}")
        if topo.get("global_index_count") is not None:
            topo_lines.append(f"  Global Indexes: {topo['global_index_count']}")
        if topo.get("fts_index_count") is not None:
            topo_lines.append(f"  FTS Indexes: {topo['fts_index_count']}")
        if topo.get("eventing_function_count") is not None:
            topo_lines.append(f"  Eventing Functions: {topo['eventing_function_count']}")
        _sc = topo.get("scopes_collections") or []
        if _sc:
            _sc_str = "; ".join(f"{s['bucket']}:{s['scopes']}s/{s['collections']}c" for s in _sc[:6])
            topo_lines.append(f"  Scopes/Collections: {_sc_str}")
        _bc = topo.get("bad_count") or len(topo.get("bad_items") or [])
        _wc = topo.get("warn_count") or len(topo.get("warn_items") or [])
        if _bc or _wc:
            _health_str = f"  Health checks: {_bc} bad, {_wc} warn"
            _bi = topo.get("bad_items") or []
            if _bi:
                _health_str += f" (bad: {', '.join(_bi[:8])})"
            topo_lines.append(_health_str)
        if topo_lines:
            parts.append("Cluster Topology (snapshot):\n" + "\n".join(topo_lines))

    BUDGET = 24_000
    header_text = "\n\n".join(parts)

    desc_text = ""
    if ticket.get("description"):
        desc_text = f"Description:\n{ticket['description'][:4_000]}"

    comment_parts: list[str] = []
    comments_raw = ticket.get("comments")
    if comments_raw:
        try:
            comments = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
            comments = sorted(comments, key=lambda c: c.get("timestamp") or "")
            def _fmt(c: dict) -> str:
                body = (c.get("body") or "").strip()[:800]
                return f"[{c.get('timestamp','')}] {c.get('author','')}: {body}" if body else ""
            early = [_fmt(c) for c in comments[:2] if _fmt(c)]
            late  = [_fmt(c) for c in comments[-4:] if _fmt(c)]
            seen: set[str] = set()
            for s in early + late:
                if s and s not in seen:
                    comment_parts.append(s)
                    seen.add(s)
        except Exception:
            pass

    comment_text = "\n\n".join(comment_parts)
    remaining = BUDGET - len(header_text)
    assembled = header_text
    if desc_text and remaining > 200:
        assembled += "\n\n" + desc_text[:remaining - 200]
        remaining -= len(desc_text[:remaining - 200]) + 2
    if comment_text and remaining > 100:
        assembled += "\n\n" + comment_text[:remaining - 100]
    return assembled


def build_snapshot_embed_text(snap: dict) -> str:
    """Build embedding text for a snapshot document."""
    parts: list[str] = []
    if snap.get("organization"):
        parts.append(f"Customer: {snap['organization']}")
    if snap.get("cluster_name"):
        parts.append(f"Cluster Name: {snap['cluster_name']}")
        parts.append(f"Cluster: {snap['cluster_name']}")
    if snap.get("cluster_uuid"):
        parts.append(f"Cluster UUID: {snap['cluster_uuid']}")
    if snap.get("cb_version"):
        parts.append(f"Couchbase Version: {snap['cb_version']}")
    if snap.get("date"):
        parts.append(f"Snapshot Date: {snap['date']}")
    _nodes = snap.get("total_nodes") or snap.get("node_count")
    if _nodes:
        parts.append(f"Total Nodes: {_nodes}")
    if snap.get("ram_per_node_mib"):
        parts.append(f"RAM per Node: {snap['ram_per_node_mib']} MiB")
    svc_parts = []
    for svc, key in [("KV/Data", "data_nodes"), ("Index", "index_nodes"),
                     ("Query", "query_nodes"), ("Search", "fts_nodes"),
                     ("Eventing", "eventing_nodes"), ("Analytics", "analytics_nodes")]:
        n = snap.get(key)
        if n:
            svc_parts.append(f"{svc}×{n}")
    if svc_parts:
        parts.append(f"Services: {', '.join(svc_parts)}")
    _bn = snap.get("bucket_names") or []
    if isinstance(_bn, list) and _bn:
        parts.append(f"Bucket Names: {', '.join(_bn[:30])}")
    elif isinstance(_bn, str) and _bn:
        parts.append(f"Bucket Names: {_bn}")
    _bad = snap.get("bad_items") or []
    if isinstance(_bad, list) and _bad:
        parts.append(f"Bad Items: {', '.join(str(b) for b in _bad[:20])}")
    elif isinstance(_bad, str) and _bad:
        parts.append(f"Bad Items: {_bad}")
    _warn = snap.get("warn_items") or []
    if isinstance(_warn, list) and _warn:
        parts.append(f"Warning Items: {', '.join(str(w) for w in _warn[:20])}")
    elif isinstance(_warn, str) and _warn:
        parts.append(f"Warning Items: {_warn}")
    if snap.get("bad_count"):
        parts.append(f"Bad Count: {snap['bad_count']}")
    if snap.get("warn_count"):
        parts.append(f"Warning Count: {snap['warn_count']}")
    _tids = snap.get("ticket_ids") or []
    if _tids:
        parts.append(f"Associated Tickets: {', '.join(str(t) for t in _tids[:10])}")
    _alias_text = " ".join([
        snap.get("cluster_name") or "",
        " ".join(_bn[:30]) if isinstance(_bn, list) else (_bn or ""),
    ]).lower()
    for _host, _app in _get_cluster_to_app().items():
        if _host in _alias_text and _app.lower() not in _alias_text:
            parts.append(f"Application: {_app.upper()}")
    return "\n".join(parts)


# ── Embedding providers ───────────────────────────────────────────────────────

def embed_text_ollama(text: str, model: str, base_url: str,
                      num_ctx: int | None = None) -> list[float]:
    """POST to Ollama and return the embedding vector."""
    base = base_url.rstrip("/")
    options = {"num_ctx": num_ctx} if num_ctx else {}

    payload: dict = {"model": model, "input": text}
    if options:
        payload["options"] = options
    resp = requests.post(f"{base}/api/embed", json=payload, timeout=120)
    if resp.status_code == 200:
        data = resp.json()
        vecs = data.get("embeddings")
        if vecs and isinstance(vecs, list) and len(vecs) > 0:
            return vecs[0]

    payload2: dict = {"model": model, "prompt": text}
    if options:
        payload2["options"] = options
    resp = requests.post(f"{base}/api/embeddings", json=payload2, timeout=120)
    resp.raise_for_status()
    return resp.json()["embedding"]


def save_feedback(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str,
    source: str,             # mcp | chat | ui | scheduled
    kind: str,               # correction | rating | preference
    subject_kind: str,       # score | report | answer | tool_call | data
    subject_ref: str,        # e.g. "ticket:78964", "asset:<id>", "org:Western Union"
    verdict: str,            # positive | negative | corrected
    details: str = "",
    correction: dict | None = None,   # {"field":..., "old":..., "new":...}
    organization: str = "",
    session_id: str = "",
) -> str:
    """Shared writer for the human-feedback knowledge base (`feedback`
    collection). Every capture surface (MCP tool, chat agent tool, future UI
    affordances) MUST write through this one function so the schema stays
    uniform — the data's long-term value (few-shot examples, eval sets,
    DPO/fine-tune pairs for local models) depends on consistency.

    Returns the doc key, or raises on failure (callers surface the error —
    silently dropped feedback is worse than an error the user can see).
    """
    import datetime as _dt
    import uuid as _uuid

    if not _CB_AVAILABLE:
        raise RuntimeError("Couchbase SDK not available")
    cluster = Cluster(
        _cb_conn_str(cb_url, use_tls),
        ClusterOptions(PasswordAuthenticator(username, password)),
    )
    cluster.wait_until_ready(timedelta(seconds=10))
    try:
        cm = cluster.bucket(bucket).collections()
        existing = {s.name: {c.name for c in s.collections} for s in cm.get_all_scopes()}
        if "feedback" not in existing.get(scope, set()):
            from couchbase.management.collections import CollectionSpec
            cm.create_collection(CollectionSpec("feedback", scope_name=scope))
            import time as _t
            _t.sleep(1)
    except Exception:
        pass

    key = f"feedback::{_uuid.uuid4().hex[:12]}"
    doc = {
        "type":         "feedback",
        "source":       (source or "").lower().strip(),
        "kind":         (kind or "").lower().strip(),
        "subject_kind": (subject_kind or "").lower().strip(),
        "subject_ref":  subject_ref or "",
        "verdict":      (verdict or "").lower().strip(),
        "details":      (details or "")[:2000],
        "correction":   correction or None,
        "organization": organization or "",
        "session_id":   session_id or "",
        "at":           _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    cluster.bucket(bucket).scope(scope).collection("feedback").upsert(key, doc)
    cluster.close()
    return key


def classify_error(err) -> dict:
    """Classify a failure into an abridged, aggregatable form for the
    failure-knowledge base: {error_type, error_code, abridged}.

    error_type: the exception class name (or "str" for plain messages).
    error_code: coarse bucket usable in GROUP BY queries — the point is
                insight into recurring failure modes, not full fidelity.
    abridged:   first line of the message, trimmed.
    """
    etype = type(err).__name__ if isinstance(err, BaseException) else "str"
    msg = str(err)
    first = msg.splitlines()[0][:160] if msg else ""
    # Classify on the FIRST LINE only — error strings often carry a full
    # traceback, and matching keywords against source lines in the traceback
    # misclassifies (e.g. `dims=vector_dims` in a code line made every embed
    # connection failure look like EMBED_DIMS on the framework's first day).
    low = first.lower()

    if "unknown embedding provider" in low or "unknown llm provider" in low or "unknown provider" in low:
        code = "PROVIDER_CONFIG"
    elif ("model" in low and ("not loaded" in low or "not found" in low or "no model" in low)) \
            or "failed to load model" in low or "error loading model" in low:
        code = "MODEL_MISSING"
    elif "timed out" in low or "timeout" in low:
        code = "TIMEOUT"
    elif "connection closed" in low or "connection reset" in low or "broken pipe" in low or "server disconnected" in low:
        code = "CONN_CLOSED"
    elif "connection refused" in low or "connect call failed" in low or "failed to establish" in low or "connection error" in low:
        code = "CONN_REFUSED"
    elif "name or service not known" in low or "nodename nor servname" in low or "getaddrinfo" in low:
        code = "DNS"
    elif "401" in low or "403" in low or "unauthorized" in low or "forbidden" in low or "authentication" in low:
        code = "AUTH"
    elif "document_not_found" in low or "documentnotfound" in low or "key_enoent" in low:
        code = "DOC_NOT_FOUND"
    elif "404" in low or "not found" in low:
        code = "NOT_FOUND"
    elif "500" in low or "502" in low or "503" in low or "internal server error" in low:
        code = "HTTP_5XX"
    elif "400" in low or "bad request" in low:
        code = "HTTP_4XX"
    elif "jsondecode" in low or "expecting value" in low or "parse" in low:
        code = "PARSE"
    elif "dims" in low or "dimension" in low:
        code = "EMBED_DIMS"
    else:
        code = "UNKNOWN"
    return {"error_type": etype, "error_code": code, "abridged": first}


def embed_text(
    text: str,
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
    dims: int = 0,
    num_ctx: int | None = None,
) -> list[float]:
    """Dispatch to the correct embedding provider and return a float vector."""
    provider = (provider or "").lower().strip()
    if provider == "ollama":
        return embed_text_ollama(text, model, base_url or "http://localhost:11434", num_ctx=num_ctx)

    elif provider == "lmstudio":
        client = _get_openai_client("lmstudio", _openai_base_url(base_url, "http://localhost:1234"))
        resp = client.embeddings.create(model=model, input=text, encoding_format="float")
        return resp.data[0].embedding

    elif provider == "gemini":
        if not _GEMINI_AVAILABLE:
            raise RuntimeError("google-genai not installed: venv/bin/pip install google-genai")
        _gem_ver = "v1beta" if any(k in model for k in ("preview", "exp", "latest")) else "v1"
        client = _genai_mod.Client(api_key=api_key, http_options={"api_version": _gem_ver})
        extra = {}
        if dims and dims > 0:
            extra["config"] = {"output_dimensionality": dims}
        result = client.models.embed_content(model=model, contents=text, **extra)
        return list(result.embeddings[0].values)

    elif provider == "openai":
        if not _OPENAI_AVAILABLE:
            raise RuntimeError("openai package not installed: venv/bin/pip install openai")
        kwargs: dict = {"model": model, "input": text, "encoding_format": "float"}
        if dims and dims > 0:
            kwargs["dimensions"] = dims
        client = _get_openai_client(api_key, "")
        resp = client.embeddings.create(**kwargs)
        return resp.data[0].embedding

    elif provider == "mlx":
        if not _MLX_EMB_AVAILABLE:
            raise RuntimeError(
                f"mlx-embeddings import failed: {_MLX_EMB_IMPORT_ERROR}\n"
                "Run: venv/bin/pip install mlx-embeddings"
            )
        if _mlx_emb_cache["model_id"] != model:
            m, tok = _mlx_emb_load(model)
            _mlx_emb_cache["model"]     = m
            _mlx_emb_cache["tokenizer"] = tok
            _mlx_emb_cache["model_id"]  = model
        m   = _mlx_emb_cache["model"]
        tok = _mlx_emb_cache["tokenizer"]
        encoded = tok.encode([text])
        if isinstance(encoded, (list, tuple)) and len(encoded) == 2:
            input_ids, attention_mask = encoded
        elif isinstance(encoded, dict):
            input_ids      = encoded["input_ids"]
            attention_mask = encoded.get("attention_mask")
        else:
            input_ids      = encoded
            attention_mask = None
        if not hasattr(input_ids, "shape"):
            input_ids = _mx.array(input_ids)
        if attention_mask is not None and not hasattr(attention_mask, "shape"):
            attention_mask = _mx.array(attention_mask)
        if attention_mask is not None:
            out = m(input_ids, attention_mask)
        else:
            out = m(input_ids)
        hidden = out.last_hidden_state if hasattr(out, "last_hidden_state") else out
        if _mlx_pool is not None and attention_mask is not None:
            vec = _mlx_pool(hidden, attention_mask)
        elif attention_mask is not None:
            mask_exp = attention_mask[:, :, None].astype(_mx.float32)
            vec = (hidden * mask_exp).sum(axis=1) / mask_exp.sum(axis=1).clip(1e-9)
        else:
            vec = _mx.mean(hidden, axis=1)
        vec = vec / _mx.linalg.norm(vec, axis=-1, keepdims=True)
        _mx.eval(vec)
        return vec[0].tolist()

    else:
        raise ValueError(f"Unknown embedding provider: {provider!r}")


# ── Bulk embedding ────────────────────────────────────────────────────────────

def embed_all_snapshots(
    snapshots: list[dict],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, snap_collection: str,
    embed_provider: str, embed_model: str, embed_api_key: str,
    embed_base_url: str, vector_dims: int,
    progress_cb: Callable[[str, float], None],
    cancel_event: threading.Event | None = None,
    max_workers: int = 1,
) -> tuple[int, int]:
    """For each snapshot: build embed text → embed → upsert back. Returns (done, errors)."""
    from couchbase.subdocument import upsert as _SD_upsert

    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed")

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb("Connecting to Couchbase for snapshot embedding …", 0.0)
    cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    col = cluster.bucket(bucket).scope(scope).collection(snap_collection)

    total = len(snapshots)
    done_count = error_count = 0
    lock = threading.Lock()

    def _embed_one(snap: dict) -> tuple[str, list[float] | None, str | None]:
        sid = snap.get("snap_id") or "unknown"
        doc_key = f"snapshot::{sid}"
        try:
            text = build_snapshot_embed_text(snap)
            vec  = embed_text(text, embed_provider, embed_model, embed_api_key,
                              embed_base_url, vector_dims)
            if vector_dims and len(vec) > vector_dims:
                vec = vec[:vector_dims]
                norm = sum(x * x for x in vec) ** 0.5
                if norm > 0:
                    vec = [x / norm for x in vec]
            return doc_key, vec, None
        except Exception as exc:
            return doc_key, None, str(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        futs = {pool.submit(_embed_one, s): s for s in snapshots}
        for i, fut in enumerate(concurrent.futures.as_completed(futs)):
            if cancel_event and cancel_event.is_set():
                break
            doc_key, vec, err = fut.result()
            if vec:
                try:
                    col.mutate_in(doc_key, [_SD_upsert("embedding", vec)])
                    with lock:
                        done_count += 1
                        if done_count % 10 == 0 or done_count == total:
                            progress_cb(f"Embedded {done_count}/{total} snapshots …",
                                        done_count / total)
                except Exception as exc:
                    with lock:
                        error_count += 1
                    print(f"[embed_snapshots] upsert error {doc_key}: {exc}")
            else:
                with lock:
                    error_count += 1
                print(f"[embed_snapshots] embed error {doc_key}: {err}")

    cluster.close()
    return done_count, error_count


def embed_all_tickets(
    tickets: list[dict],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    embed_provider: str, embed_model: str, embed_api_key: str,
    embed_base_url: str, vector_dims: int,
    progress_cb: Callable[[str, float], None],
    cancel_event: threading.Event | None = None,
    embed_num_ctx: int | None = None,
    max_workers: int = 1,
    error_sink: list | None = None,
) -> tuple[int, int]:
    """For each ticket: build embed text → embed → upsert. Returns (done, errors).

    error_sink: optional list — per-ticket failure detail dicts are appended
    ({ticket_id, stage, error}) so callers can persist failure knowledge
    instead of only receiving an opaque error count.
    """
    import traceback as _tb
    from couchbase.subdocument import upsert as _SD_upsert

    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed — run: venv/bin/pip install couchbase")

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb(f"Connecting to {conn_str} …", 0.0)
    cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    col = cluster.bucket(bucket).scope(scope).collection(collection)

    if (embed_provider or "").lower().strip() == "mlx":
        max_workers = 1
        if not _MLX_EMB_AVAILABLE:
            raise RuntimeError(
                f"mlx-embeddings import failed: {_MLX_EMB_IMPORT_ERROR}\n"
                "Run: venv/bin/pip install mlx-embeddings"
            )
        if _mlx_emb_cache["model_id"] != embed_model:
            progress_cb(f"Loading MLX model {embed_model} …", 0.0)
            m, tok = _mlx_emb_load(embed_model)
            _mlx_emb_cache["model"]     = m
            _mlx_emb_cache["tokenizer"] = tok
            _mlx_emb_cache["model_id"]  = embed_model
            progress_cb(f"MLX model loaded — starting embeddings …", 0.0)

    effective_workers = max(1, max_workers)
    total = len(tickets)
    progress_cb(
        f"Embedding {total} tickets"
        + (f" (parallel={effective_workers})" if effective_workers > 1 else "") + " …",
        0.0,
    )

    done_count = error_count = 0
    lock = threading.Lock()
    first_error: list[str] = []

    def _embed_one(ticket: dict) -> tuple[str, list[float] | None, str | None]:
        tid     = ticket.get("ticket_id") or "unknown"
        doc_key = f"ticket::{tid}"
        try:
            text = build_embed_text(ticket)
            vec  = embed_text(text, embed_provider, embed_model, embed_api_key,
                              embed_base_url, dims=vector_dims, num_ctx=embed_num_ctx)
            if len(vec) > vector_dims:
                vec = vec[:vector_dims]
                norm = sum(x * x for x in vec) ** 0.5
                if norm > 0:
                    vec = [x / norm for x in vec]
            elif len(vec) < vector_dims:
                raise ValueError(
                    f"Model returned {len(vec)} dims but expected {vector_dims}. "
                    "Lower the Vector Dims field to match or below the model's native output."
                )
            return doc_key, vec, None
        except Exception as exc:
            return doc_key, None, f"{type(exc).__name__}: {exc}\n{_tb.format_exc()}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {pool.submit(_embed_one, t): t for t in tickets}
        for fut in concurrent.futures.as_completed(futures):
            if cancel_event and cancel_event.is_set():
                pool.shutdown(wait=False, cancel_futures=True)
                progress_cb(f"Cancelled — {done_count}/{total} embedded.", done_count / total)
                break

            ticket     = futures[fut]
            doc_key, vec, err = fut.result()
            tid = ticket.get("ticket_id") or "unknown"

            if err:
                with lock:
                    error_count += 1
                    if not first_error:
                        first_error.append(err)
                    if error_sink is not None and len(error_sink) < 200:
                        error_sink.append({"ticket_id": str(tid), "stage": "embed",
                                           **classify_error(err)})
                    progress_cb(f"Skipped ticket {tid}: {err.splitlines()[0]}", done_count / total)
                continue

            col.mutate_in(doc_key, [_SD_upsert("embedding", vec)])
            with lock:
                done_count += 1
                if done_count % 10 == 0 or done_count == total:
                    progress_cb(f"Embedded {done_count}/{total} …", done_count / total)

    cluster.close()
    return done_count, error_count


# ── CB data migration ─────────────────────────────────────────────────────────

def migrate_ticket_fields_in_cb(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    progress_cb=None,
) -> tuple[int, int]:
    """Normalize ticket_fields and comments from JSON strings to native dicts/lists."""
    import couchbase.subdocument as _SD
    if not _CB_AVAILABLE:
        raise RuntimeError("Couchbase SDK not available")

    cluster = Cluster(
        _cb_conn_str(cb_url, use_tls),
        ClusterOptions(PasswordAuthenticator(username, password)),
    )
    col = cluster.bucket(bucket).scope(scope).collection(collection)
    q = f"SELECT META().id AS doc_key FROM `{bucket}`.`{scope}`.`{collection}`"
    rows = list(cluster.query(q))
    total = len(rows)
    migrated = skipped = 0

    for i, row in enumerate(rows, 1):
        key = row["doc_key"]
        try:
            result = col.get(key)
            doc = result.content_as[dict]
            mutations = []

            raw_tf = doc.get("ticket_fields")
            if raw_tf is not None:
                if isinstance(raw_tf, str):
                    try:
                        tf_dict = json.loads(raw_tf)
                    except Exception:
                        tf_dict = None
                elif isinstance(raw_tf, dict):
                    tf_dict = raw_tf
                else:
                    tf_dict = None
                if isinstance(tf_dict, dict):
                    if any(" " in k or "(" in k for k in tf_dict):
                        normalized_tf = {_normalize_field_key(k): v for k, v in tf_dict.items()}
                        mutations.append(_SD.upsert("ticket_fields", normalized_tf))
                    elif isinstance(raw_tf, str):
                        mutations.append(_SD.upsert("ticket_fields", tf_dict))

            raw_cm = doc.get("comments")
            if isinstance(raw_cm, str):
                try:
                    cm_list = json.loads(raw_cm)
                    if isinstance(cm_list, list):
                        mutations.append(_SD.upsert("comments", cm_list))
                except Exception:
                    pass

            if mutations:
                col.mutate_in(key, mutations)
                migrated += 1
            else:
                skipped += 1

        except Exception:
            skipped += 1

        if progress_cb and i % 10 == 0:
            progress_cb(f"Migrating documents {i}/{total}…", i / total)

    cluster.close()
    return migrated, skipped


# ── FTS vector index ──────────────────────────────────────────────────────────

def create_vector_index(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str, vector_dims: int,
) -> None:
    """PUT a vector FTS index definition via the Couchbase FTS REST API (port 8094)."""
    index_name = f"{collection}_vector_idx"
    port       = 18094 if use_tls else 8094
    api_scheme = "https" if use_tls else "http"
    host       = re.sub(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", "", cb_url).strip().rstrip("/")
    api_url    = f"{api_scheme}://{host}:{port}/api/bucket/{bucket}/scope/{scope}/index/{index_name}"

    def _text_field(name: str) -> dict:
        return {
            "dynamic": False, "enabled": True,
            "fields": [{"analyzer": "standard", "index": True, "name": name, "store": True, "type": "text"}],
        }

    snap_collection  = "snapshots"
    ticket_type_key  = f"{scope}.{collection}"
    snap_type_key    = f"{scope}.{snap_collection}"

    def _vec_field() -> dict:
        return {
            "dynamic": False, "enabled": True,
            "fields": [{"dims": vector_dims, "index": True, "name": "embedding",
                        "similarity": "dot_product", "type": "vector"}],
        }

    def _nested(children: dict) -> dict:
        return {"dynamic": False, "enabled": True, "properties": children}

    index_def = {
        "type": "fulltext-index", "name": f"{bucket}.{scope}.{index_name}",
        "sourceType": "gocbcore", "sourceName": bucket, "sourceParams": {},
        "planParams": {"maxPartitionsPerPIndex": 1024, "indexPartitions": 1},
        "params": {
            "doc_config": {
                "docid_prefix_delim": "", "docid_regexp": "",
                "mode": "scope.collection.type_field", "type_field": "type",
            },
            "mapping": {
                "analysis": {}, "default_analyzer": "standard",
                "default_datetime_parser": "dateTimeOptional", "default_field": "_all",
                "default_mapping": {"dynamic": False, "enabled": False},
                "default_type": "_default", "docvalues_dynamic": False,
                "index_dynamic": False, "store_dynamic": False, "type_field": "_type",
                "types": {
                    ticket_type_key: {
                        "dynamic": False, "enabled": True,
                        "properties": {
                            "embedding":   _vec_field(),
                            "subject":     _text_field("subject"),
                            "description": _text_field("description"),
                            "comments":    _nested({"body": _text_field("body")}),
                            "tags":        _text_field("tags"),
                            "status":      _text_field("status"),
                            "priority":    _text_field("priority"),
                            "requester":   _text_field("requester"),
                            "assignee":    _text_field("assignee"),
                            "created":     _text_field("created"),
                            "snapshot_topology": _nested({
                                "cluster_name": _text_field("cluster_name"),
                                "bucket_names": _text_field("bucket_names"),
                            }),
                            "score": _nested({"cluster_names": _text_field("cluster_names")}),
                            "cbses":       _text_field("cbses"),
                            "jira_issues": _text_field("jira_issues"),
                        },
                    },
                    snap_type_key: {
                        "dynamic": False, "enabled": True,
                        "properties": {
                            "embedding":    _vec_field(),
                            "cluster_name": _text_field("cluster_name"),
                            "organization": _text_field("organization"),
                            "cb_version":   _text_field("cb_version"),
                            "bad_items":    _text_field("bad_items"),
                            "warn_items":   _text_field("warn_items"),
                        },
                    },
                },
            },
            "store": {"indexType": "scorch", "segmentVersion": 16},
        },
    }

    _auth = (username, password)
    _base = f"{api_scheme}://{host}:{port}"
    for _del_url in [
        f"{_base}/api/index/{index_name}",
        f"{_base}/api/bucket/{bucket}/index/{index_name}",
        f"{_base}/api/bucket/{bucket}/scope/{scope}/index/{index_name}",
    ]:
        requests.delete(_del_url, auth=_auth, verify=False, timeout=10)

    resp = requests.put(api_url, json=index_def, auth=(username, password), verify=False, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"FTS index PUT failed {resp.status_code}: {resp.text}")


# ── Vector + keyword search ───────────────────────────────────────────────────

def vector_search_cb(
    query_vec: list[float],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    top_k: int = 10,
) -> list[str]:
    """Run a CB vector search; returns document keys sorted by relevance."""
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed")

    index_name = f"{collection}_vector_idx"
    conn_str   = _cb_conn_str(cb_url, use_tls)
    cluster    = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    scope_obj  = cluster.bucket(bucket).scope(scope)

    _num_candidates = min(top_k * 3, 200)
    search_req = SearchRequest.create(
        VectorSearch.from_vector_query(
            VectorQuery("embedding", query_vec, num_candidates=_num_candidates)
        )
    )

    last_exc: Exception = RuntimeError("vector search did not run")
    for attempt in range(1, 6):
        try:
            result = scope_obj.search(index_name, search_req, SearchOptions(limit=top_k))
            ids = [row.id for row in result.rows()]
            cluster.close()
            return ids
        except Exception as exc:
            last_exc = exc
            err_str = str(exc)
            if "429" in err_str or "query request rejected" in err_str or "internal_server_failure" in err_str.lower():
                if attempt < 5:
                    time.sleep(3)
                    continue
            cluster.close()
            raise

    cluster.close()
    raise RuntimeError(
        f"Vector index not ready after 5 attempts (last error: {last_exc}). "
        "The index may still be building — check its status in the Couchbase UI "
        "under Search → your index, then try again."
    ) from last_exc


def _snap_keys_to_ticket_keys(
    snap_keys: list[str],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, snap_collection: str = "snapshots",
) -> list[str]:
    """Resolve snapshot doc keys → ticket doc keys via snapshot.ticket_ids."""
    if not _CB_AVAILABLE or not snap_keys:
        return []
    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cluster.wait_until_ready(timedelta(seconds=10))
        ks           = f"`{bucket}`.`{scope}`.`{snap_collection}`"
        placeholders = ", ".join(f"${i+1}" for i in range(len(snap_keys)))
        q    = f"SELECT s.ticket_ids FROM {ks} s WHERE META(s).id IN [{placeholders}]"
        rows = list(cluster.query(q, QueryOptions(positional_parameters=snap_keys)))
        cluster.close()
        seen: set[str] = set()
        result: list[str] = []
        for row in rows:
            for tid in (row.get("ticket_ids") or []):
                key = f"ticket::{tid}"
                if key not in seen:
                    seen.add(key)
                    result.append(key)
        return result
    except Exception as exc:
        print(f"[_snap_keys_to_ticket_keys] {exc}")
        return []


def fts_keyword_search_cb(
    keywords: list[str],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    top_k: int = 50,
) -> list[str]:
    """FTS text search (BM25) against the hybrid FTS index. Returns doc keys."""
    if not _CB_AVAILABLE or not keywords:
        return []
    try:
        conn_str   = _cb_conn_str(cb_url, use_tls)
        cluster    = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cluster.wait_until_ready(timedelta(seconds=10))
        scope_obj  = cluster.bucket(bucket).scope(scope)
        index_name = f"{collection}_vector_idx"

        kw_list = keywords[:8]
        if len(kw_list) == 1:
            fts_q = MatchQuery(kw_list[0])
        else:
            fts_q = DisjunctionQuery(*[MatchQuery(kw) for kw in kw_list])

        result = scope_obj.search(
            index_name, SearchRequest.create(fts_q), SearchOptions(limit=top_k),
        )
        ids = [row.id for row in result.rows()]
        cluster.close()
        return ids
    except Exception as exc:
        print(f"[fts_keyword_search_cb] {exc}")
        return []


# ── Cluster app map population ────────────────────────────────────────────────

def _load_cluster_app_map(
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, snap_collection: str,
    ticket_collection: str = "tickets",
) -> tuple[int, int]:
    """Query CB to build dynamic cluster→app maps; updates scoring module dicts in place."""
    if not _CB_AVAILABLE:
        return (0, 0)
    try:
        scheme = "couchbases" if use_tls else "couchbase"
        cluster = Cluster(
            f"{scheme}://{cb_url}",
            ClusterOptions(PasswordAuthenticator(username, password)),
        )
        cluster.wait_until_ready(datetime.timedelta(seconds=10))
        ks_snaps   = f"`{bucket}`.`{scope}`.`{snap_collection}`"
        ks_tickets = f"`{bucket}`.`{scope}`.`{ticket_collection}`"

        snap_n = 0
        try:
            q = (
                f"SELECT DISTINCT cluster_name, organization "
                f"FROM {ks_snaps} "
                f"WHERE cluster_name IS NOT MISSING AND organization IS NOT MISSING "
                f"LIMIT 2000"
            )
            rows = list(cluster.query(q, QueryOptions(scan_consistency=0)))
            for row in rows:
                cname = (row.get("cluster_name") or "").strip().lower()
                org   = (row.get("organization") or "").strip().lower()
                if not cname or not org:
                    continue
                _cluster_app_dynamic[cname] = org
                if org not in _app_cluster_dynamic:
                    _app_cluster_dynamic[org] = []
                if cname not in _app_cluster_dynamic[org]:
                    _app_cluster_dynamic[org].append(cname)
                snap_n += 1
        except Exception:
            pass

        ticket_n = 0
        try:
            q = (
                f"SELECT organization, score.cluster_names AS cnames "
                f"FROM {ks_tickets} "
                f"WHERE organization IS NOT MISSING "
                f"  AND score.cluster_names IS NOT MISSING "
                f"LIMIT 5000"
            )
            rows = list(cluster.query(q, QueryOptions(scan_consistency=0)))
            for row in rows:
                org    = (row.get("organization") or "").strip().lower()
                cnames = row.get("cnames") or []
                if not org or not isinstance(cnames, list):
                    continue
                for cname in cnames:
                    cname = (cname or "").strip().lower()
                    if not cname:
                        continue
                    if cname not in _cluster_app_dynamic:
                        _cluster_app_dynamic[cname] = org
                        ticket_n += 1
                    if org not in _app_cluster_dynamic:
                        _app_cluster_dynamic[org] = []
                    if cname not in _app_cluster_dynamic[org]:
                        _app_cluster_dynamic[org].append(cname)
        except Exception:
            pass

        cluster.close()
        return (snap_n, ticket_n)
    except Exception:
        return (0, 0)


# ── Query parsing and retrieval ───────────────────────────────────────────────

_KEYWORD_STOPWORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "must", "can", "could", "i", "me", "my",
    "we", "our", "you", "your", "he", "she", "it", "they", "them",
    "this", "that", "these", "those", "what", "which", "who", "how",
    "when", "where", "why", "all", "any", "both", "each", "few", "more",
    "most", "other", "some", "such", "no", "nor", "not", "only", "own",
    "same", "so", "than", "too", "very", "just", "but", "and", "or",
    "if", "in", "on", "at", "to", "for", "of", "with", "about", "as",
    "into", "through", "during", "before", "after", "above", "below",
    "from", "up", "down", "out", "off", "over", "under", "again", "then",
    "once", "here", "there",
    "tell", "get", "show", "give", "find", "list", "please", "hi", "hello",
    "can", "could", "would", "also", "now", "like", "want", "need",
    "help", "look", "using", "used", "use",
    "summary", "summarize", "summarise", "table", "timeline", "review",
    "happened", "able", "create", "sort", "impact", "details", "detail",
    "info", "information", "explain", "description", "describe", "reason",
    "note", "notes", "result", "results", "output", "format", "report",
    "chart", "graph", "compare", "comparison", "analysis", "analyze",
    "cluster", "clusters", "name", "status", "id", "date", "time",
    "customer", "org", "organization", "account",
    "ticket", "tickets", "issue", "issues", "problem", "problems", "case",
    "cases", "related", "recent", "latest", "last", "first", "top", "next",
    "new", "old", "many", "number", "count", "see", "per", "each",
    "month", "months", "week", "weeks", "day", "days", "year", "years",
    "past", "quarter", "today", "yesterday", "ago",
    "open", "closed", "solved", "pending", "hold", "high", "low",
    "priority", "p1", "p2", "p3", "p4",
    "involved", "involving", "involve", "involves",
    "affected", "affecting", "affects", "affect",
    "occurred", "occurring", "occur", "occurs",
    "failed", "failing", "fails", "fail",
    "caused", "causing", "causes", "cause",
    "seen", "see", "far", "across", "along",
    "means", "mean", "take", "taken", "took",
    "since", "until", "while", "whereby",
})


def build_structured_query(question: str) -> dict:
    """Parse a free-text question into structured CB filter constraints."""
    q = question.lower()
    today = datetime.datetime.now()
    result: dict = {
        "ticket_ids": [], "priorities": [], "date_from": None, "date_to": None,
        "cluster_ids": [], "statuses": [], "keywords": [], "limit": 0,
        "topology_min_nodes": None, "topology_max_nodes": None,
        "topology_min_data": None, "topology_services": [],
    }

    result["ticket_ids"] = list(_extract_ticket_ids(question))

    prio_map = {"p1": "P1", "p2": "P2", "p3": "P3", "p4": "P4",
                "priority 1": "P1", "priority 2": "P2",
                "priority 3": "P3", "priority 4": "P4"}
    result["priorities"] = list({v for k, v in prio_map.items() if k in q})

    _WORD_TO_NUM = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "eleven": 11, "twelve": 12,
    }
    def _n_unit(unit: str) -> int | None:
        dm = re.search(rf"\b(?:last|past)\s+(\d+)\s+{unit}s?\b", q)
        if dm:
            return int(dm.group(1))
        wm = re.search(rf"\b(?:last|past)\s+({'|'.join(_WORD_TO_NUM)})\s+{unit}s?\b", q)
        if wm:
            return _WORD_TO_NUM[wm.group(1)]
        return None

    _n_d = _n_unit("day")
    _n_w = _n_unit("week")
    _n_m = _n_unit("month")
    _n_y = _n_unit("year")

    if _n_d is not None:
        result["date_from"] = (today - datetime.timedelta(days=_n_d)).strftime("%Y-%m-%d")
    elif _n_w is not None:
        result["date_from"] = (today - datetime.timedelta(weeks=_n_w)).strftime("%Y-%m-%d")
    elif _n_m is not None:
        result["date_from"] = (today - datetime.timedelta(days=_n_m * 30)).strftime("%Y-%m-%d")
    elif _n_y is not None:
        result["date_from"] = (today - datetime.timedelta(days=_n_y * 365)).strftime("%Y-%m-%d")
    elif any(k in q for k in ("last week", "past week", "7 day", "past 7")):
        result["date_from"] = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    elif any(k in q for k in ("last month", "past month", "30 day", "this month", "past 30")):
        result["date_from"] = (today - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    elif any(k in q for k in ("last quarter", "last three month", "last 3 month",
                               "3 month", "past 3 month", "90 day", "past quarter")):
        result["date_from"] = (today - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    elif any(k in q for k in ("last year", "past year", "12 month", "365 day")):
        result["date_from"] = (today - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    elif any(k in q for k in ("this year", "year to date", "ytd", "so far this year", "current year")):
        result["date_from"] = f"{today.year}-01-01"
    elif re.search(r"\brecent\b", q):
        result["date_from"] = (today - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    else:
        iso_dates = re.findall(r"\b(20\d{2}-\d{2}-\d{2})\b", q)
        if iso_dates:
            iso_dates_sorted = sorted(iso_dates)
            result["date_from"] = iso_dates_sorted[0]
            if len(iso_dates_sorted) > 1:
                result["date_to"] = iso_dates_sorted[-1]
        else:
            ym = re.search(r"\b(20\d{2})\b", q)
            if ym:
                yr = int(ym.group(1))
                result["date_from"] = f"{yr}-01-01"
                result["date_to"]   = f"{yr}-12-31"
            month_names = {"january": 1, "february": 2, "march": 3, "april": 4,
                           "may": 5, "june": 6, "july": 7, "august": 8,
                           "september": 9, "october": 10, "november": 11, "december": 12}
            for name, num in month_names.items():
                if name in q:
                    yr_m = re.search(r"\b(20\d{2})\b", q)
                    if yr_m:
                        yr = int(yr_m.group(1))
                        last_day = (datetime.datetime(yr, num, 1)
                                    + datetime.timedelta(days=32)).replace(day=1)
                        result["date_from"] = f"{yr}-{num:02d}-01"
                        result["date_to"]   = (last_day - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    break

    status_map = {"open": "open", "closed": "closed", "solved": "solved",
                  "pending": "pending", "new": "new", "hold": "hold"}
    result["statuses"] = [v for k, v in status_map.items() if re.search(rf"\b{k}\b", q)]

    m_lim = re.search(r"\b(?:last|top|first|show|recent)\s+(\d+)\b", q)
    if m_lim:
        result["limit"] = int(m_lim.group(1))

    _aca = _get_app_cluster_aliases()
    _alias_key_set = set(_aca.keys())
    _TECH_ACRONYMS = frozenset({
        "mle", "fts", "kv", "xdcr", "cbas", "n1ql", "sdk", "ssl",
        "tls", "ldap", "rbac", "cbse", "cbm", "dcp", "gsi", "eventing",
    })
    _tokens = re.sub(r"[^\w\s]", " ", q).split()
    _used = set(result["ticket_ids"]) | {p.lower() for p in result["priorities"]} | \
            {s.lower() for s in result["statuses"]}
    _keywords = [
        t for t in _tokens
        if t not in _KEYWORD_STOPWORDS
        and t not in _used
        and not t.isdigit()
        and (
            t in _alias_key_set
            or t in _TECH_ACRONYMS
            or len(t) >= 4
        )
    ]
    _seen: set[str] = set()
    _deduped = [k for k in _keywords if not (k in _seen or _seen.add(k))]  # type: ignore[func-returns-value]

    _alias_hosts: list[str] = []
    for kw in _deduped:
        for alias, hosts in _aca.items():
            if kw == alias or alias.startswith(kw) or kw in alias:
                for h in hosts:
                    if h not in _alias_hosts and h not in _deduped:
                        _alias_hosts.append(h)
    result["keywords"] = _deduped + _alias_hosts

    _known_hostnames = set(_get_cluster_to_app().keys())
    _HOSTNAME_RE = re.compile(r"^[a-z][a-z0-9]{11,}$")
    def _base_tech(tok: str) -> str:
        if tok in _TECH_ACRONYMS:
            return tok
        stripped = tok.rstrip("s")
        if stripped in _TECH_ACRONYMS:
            return stripped
        return tok

    _struct_raw = [
        _base_tech(k) for k in _deduped
        if k in _alias_key_set
        or k in _TECH_ACRONYMS
        or k.rstrip("s") in _TECH_ACRONYMS
        or k in _known_hostnames
        or _HOSTNAME_RE.match(k)
    ]
    _sk_seen: set[str] = set()
    result["struct_keywords"] = [
        k for k in _struct_raw + _alias_hosts
        if not (k in _sk_seen or _sk_seen.add(k))  # type: ignore[func-returns-value]
    ]

    _topo_gt = re.search(r"\b(?:more than|greater than|over|>\s*)(\d+)\s+(?:total\s+)?nodes?\b", q)
    _topo_ge = re.search(r"\b(?:at least|minimum of?|min\s+)(\d+)\s+(?:total\s+)?nodes?\b", q)
    _topo_plus = re.search(r"\b(\d+)\+\s*nodes?\b", q)
    _topo_lt = re.search(r"\b(?:fewer than|less than|under|<\s*)(\d+)\s+(?:total\s+)?nodes?\b", q)
    _topo_le = re.search(r"\b(?:at most|maximum of?|max\s+)(\d+)\s+(?:total\s+)?nodes?\b", q)
    if _topo_gt:
        result["topology_min_nodes"] = int(_topo_gt.group(1)) + 1
    elif _topo_ge:
        result["topology_min_nodes"] = int(_topo_ge.group(1))
    elif _topo_plus:
        result["topology_min_nodes"] = int(_topo_plus.group(1))
    if _topo_lt:
        result["topology_max_nodes"] = int(_topo_lt.group(1)) - 1
    elif _topo_le:
        result["topology_max_nodes"] = int(_topo_le.group(1))
    _data_gt = re.search(r"\b(?:more than|greater than|over|>\s*)(\d+)\s+data\s+nodes?\b", q)
    if _data_gt:
        result["topology_min_data"] = int(_data_gt.group(1)) + 1
    _SVC_MAP = {
        "fts": "fts", "full text search": "fts", "full-text": "fts",
        "eventing": "eventing", "analytics": "analytics",
        "index": "index", "query": "query",
    }
    result["topology_services"] = list({v for k, v in _SVC_MAP.items() if k in q})

    return result


def structured_search_cb(
    filters: dict,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    default_limit: int = 50,
) -> list[str]:
    """Run a N1QL query built from structured filters; return doc key list."""
    if not _CB_AVAILABLE:
        return []

    ticket_ids = filters.get("ticket_ids") or []
    priorities = filters.get("priorities") or []
    date_from  = filters.get("date_from")
    date_to    = filters.get("date_to")
    # Accept singular "status" (string) as well as "statuses" (list) — several
    # agent-tool branches pass the singular form.
    statuses   = filters.get("statuses") or (
        [str(filters["status"]).strip().lower()] if filters.get("status") else []
    )
    limit      = filters.get("limit") or default_limit

    if not any([ticket_ids, priorities, date_from, statuses]):
        return []

    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cluster.wait_until_ready(timedelta(seconds=10))
        keyspace = f"`{bucket}`.`{scope}`.`{collection}`"

        where_parts: list[str] = ["t.ticket_id IS NOT MISSING"]
        params: list = []

        if ticket_ids:
            placeholders = ", ".join(f"${ i+1}" for i, _ in enumerate(ticket_ids))
            where_parts.append(f"t.ticket_id IN [{placeholders}]")
            params.extend(ticket_ids)
        if priorities:
            p_idx = len(params) + 1
            placeholders = ", ".join(f"${p_idx + i}" for i, _ in enumerate(priorities))
            where_parts.append(f"UPPER(TOSTRING(t.priority)) IN [{placeholders}]")
            params.extend(priorities)
        if date_from:
            params.append(date_from)
            where_parts.append(f"t.created >= ${len(params)}")
        if date_to:
            params.append(date_to)
            where_parts.append(f"t.created <= ${len(params)}")
        if statuses:
            s_idx = len(params) + 1
            placeholders = ", ".join(f"${s_idx + i}" for i, _ in enumerate(statuses))
            where_parts.append(f"LOWER(TOSTRING(t.status)) IN [{placeholders}]")
            params.extend(statuses)

        where_clause = " AND ".join(where_parts)
        n1ql = (
            f"SELECT META(t).id AS doc_key FROM {keyspace} AS t "
            f"WHERE {where_clause} "
            f"ORDER BY t.created DESC "
            f"LIMIT {min(limit * 3, 200)}"
        )
        rows = list(cluster.query(
            n1ql,
            QueryOptions(positional_parameters=params, timeout=timedelta(seconds=20)),
        ))
        cluster.close()
        return [r["doc_key"] for r in rows if r.get("doc_key")]
    except Exception as exc:
        print(f"[structured_search_cb] {exc}")
        return []


def tool_query_tickets(
    filters: dict,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    limit: int = 500,
) -> list[dict]:
    """Stage-1 structured retrieval; returns full ticket docs, no top_k cap."""
    if not _CB_AVAILABLE:
        return []
    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cluster  = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cluster.wait_until_ready(timedelta(seconds=10))
        keyspace = f"`{bucket}`.`{scope}`.`{collection}`"

        where_parts: list[str] = [
            "t.ticket_id IS NOT MISSING",
            "(t.`_deleted` IS MISSING OR t.`_deleted` = false)",
        ]
        params: list = []

        organization = (filters.get("organization") or "").strip()
        if organization:
            params.append(f"%{organization.lower()}%")
            where_parts.append(f"LOWER(TOSTRING(t.organization)) LIKE ${len(params)}")

        ticket_ids = filters.get("ticket_ids") or []
        if ticket_ids:
            phs = ", ".join(f"${i + 1}" for i, _ in enumerate(ticket_ids))
            where_parts.append(f"t.ticket_id IN [{phs}]")
            params.extend(ticket_ids)

        date_from = filters.get("date_from")
        date_to   = filters.get("date_to")
        if date_from:
            params.append(date_from)
            _df_idx = len(params)
            where_parts.append(
                f"(t.created >= ${_df_idx}"
                f" OR (t.created IS NULL"
                f" AND MILLIS_TO_STR(t.last_scraped_at * 1000) >= ${_df_idx}))"
            )
        if date_to:
            params.append(date_to + "T23:59:59Z")
            _dt_idx = len(params)
            where_parts.append(
                f"(t.created <= ${_dt_idx}"
                f" OR (t.created IS NULL"
                f" AND MILLIS_TO_STR(t.last_scraped_at * 1000) <= ${_dt_idx}))"
            )

        priorities = filters.get("priorities") or []
        if priorities:
            p_idx = len(params) + 1
            phs   = ", ".join(f"${p_idx + i}" for i, _ in enumerate(priorities))
            where_parts.append(f"UPPER(TOSTRING(t.priority)) IN [{phs}]")
            params.extend(priorities)

        # Accept singular "status" (string) as well as "statuses" (list) — several
        # agent-tool branches pass the singular form.
        statuses = filters.get("statuses") or (
            [str(filters["status"]).strip().lower()] if filters.get("status") else []
        )
        if statuses:
            s_idx = len(params) + 1
            phs   = ", ".join(f"${s_idx + i}" for i, _ in enumerate(statuses))
            where_parts.append(f"LOWER(TOSTRING(t.status)) IN [{phs}]")
            params.extend(statuses)

        struct_kws = filters.get("struct_keywords") or []
        _array_kws: set[str] = set()
        if "cbse" in struct_kws:
            where_parts.append("ARRAY_LENGTH(t.`cbses`) > 0")
            _array_kws.add("cbse")
        if "jira" in struct_kws:
            where_parts.append("ARRAY_LENGTH(t.`jira_issues`) > 0")
            _array_kws.add("jira")
        _text_kws = [kw for kw in struct_kws if kw not in _array_kws]

        if _text_kws:
            kw_ors: list[str] = []
            for kw in _text_kws:
                params.append(f"%{kw.lower()}%")
                idx = len(params)
                kw_ors.append(
                    f"(LOWER(t.subject) LIKE ${idx}"
                    f" OR LOWER(t.description) LIKE ${idx}"
                    f" OR ANY c IN t.`score`.`cluster_names`"
                    f"   SATISFIES LOWER(c) LIKE ${idx} END"
                    f" OR ANY cb IN t.`cbses`"
                    f"   SATISFIES LOWER(cb) LIKE ${idx} END"
                    f" OR ANY ji IN t.`jira_issues`"
                    f"   SATISFIES LOWER(ji) LIKE ${idx} END)"
                )
            where_parts.append(f"({' OR '.join(kw_ors)})")

        where_clause = " AND ".join(where_parts)
        n1ql = (
            f"SELECT t.* FROM {keyspace} AS t "
            f"WHERE {where_clause} "
            f"ORDER BY IFNULL(t.created, MILLIS_TO_STR(t.last_scraped_at * 1000)) DESC "
            f"LIMIT {int(limit)}"
        )
        rows = list(cluster.query(
            n1ql,
            QueryOptions(positional_parameters=params, timeout=timedelta(seconds=30)),
        ))
        cluster.close()
        return [dict(r) for r in rows if r.get("ticket_id")]
    except Exception as exc:
        print(f"[tool_query_tickets] {exc}")
        return []


def search_tickets_retrieve_rerank(
    question: str,
    original_question: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    embed_fn,
    in_memory_tickets: list[dict],
    top_k_vec: int = 60,
    query_limit: int = 500,
    customer_name: str = "",
) -> tuple[list[dict], str]:
    """Two-stage retrieval: structured (Stage 1a) + vector supplement (Stage 1b)."""
    notes: list[str] = []
    filters = build_structured_query(original_question or question)

    _cust_for_filter = customer_name.strip()
    if _cust_for_filter and _cust_for_filter.lower() != "all customers":
        filters["organization"] = _cust_for_filter

    struct_tickets: list[dict] = []
    if _CB_AVAILABLE and cb_url:
        struct_tickets = tool_query_tickets(
            filters, cb_url, bucket, username, password,
            use_tls, scope, collection, limit=query_limit,
        )
        notes.append(f"struct:{len(struct_tickets)}")
    struct_ids = {str(t.get("ticket_id", "")) for t in struct_tickets}

    vec_tickets: list[dict] = []
    if embed_fn and _CB_AVAILABLE and cb_url:
        try:
            query_vec = embed_fn(question)
            if query_vec:
                all_vec_keys = vector_search_cb(
                    query_vec, cb_url, bucket, username, password,
                    use_tls, scope, collection, top_k_vec,
                )
                new_vec_keys = [k for k in all_vec_keys if k.split("::")[-1] not in struct_ids]
                if new_vec_keys:
                    vec_tickets = fetch_tickets_by_keys(
                        new_vec_keys, cb_url, bucket, username,
                        password, use_tls, scope, collection,
                    )
                    if _cust_for_filter and _cust_for_filter.lower() != "all customers":
                        _org_lc = _cust_for_filter.lower()
                        vec_tickets = [
                            t for t in vec_tickets
                            if _org_lc in (t.get("organization") or "").lower()
                            or (t.get("organization") or "").lower() in _org_lc
                        ]
                notes.append(f"vec_new:{len(vec_tickets)}")
        except Exception as exc:
            notes.append(f"vec_err:{exc}")

    all_tickets = struct_tickets + [
        t for t in vec_tickets if str(t.get("ticket_id", "")) not in struct_ids
    ]

    if not all_tickets and in_memory_tickets:
        all_tickets, pf_note = prefilter_for_query(question, in_memory_tickets)
        notes.append(f"mem:{pf_note}")

    _date_from = filters.get("date_from")
    _date_to   = filters.get("date_to")
    if _date_from or _date_to:
        _pre_date = len(all_tickets)
        def _in_date_range(t: dict) -> bool:
            d = _ticket_date(t)[:10]
            if not d:
                return True
            if _date_from and d < _date_from:
                return False
            if _date_to and d > _date_to:
                return False
            return True
        all_tickets = [t for t in all_tickets if _in_date_range(t)]
        if len(all_tickets) < _pre_date:
            notes.append(f"date_filter:{_pre_date}→{len(all_tickets)}")

    _skws = filters.get("struct_keywords") or []
    _known_apps = set(_get_app_cluster_aliases().keys())
    _pure_tech_kws = [k for k in _skws if k not in _known_apps]
    if _pure_tech_kws and not struct_tickets:
        def _ticket_mentions_kw(t: dict) -> bool:
            _comments_raw = t.get("comments") or []
            _comments_str = " ".join(
                str(c.get("body") or c.get("content") or c)
                for c in (_comments_raw if isinstance(_comments_raw, list) else [])
            )[:800]
            _score = t.get("score") or {}
            _summary = (
                str(t.get("interaction_summary") or "")
                or str(_score.get("interaction_summary") or "")
            )
            haystack = " ".join([
                str(t.get("subject") or ""), str(t.get("description") or ""),
                str(t.get("tags") or ""), _comments_str, _summary,
            ]).lower()
            return any(kw.lower() in haystack for kw in _pure_tech_kws)
        grounded = [t for t in all_tickets if _ticket_mentions_kw(t)]
        if grounded:
            all_tickets = grounded
            notes.append(f"grounded:{len(all_tickets)}")
        elif all_tickets:
            notes.append(f"grounded:0,candidates:{len(all_tickets)}")
        else:
            notes.append("grounded:0→empty")
            return [], " | ".join(notes)

    _MAX_CANDIDATES = 150
    if len(all_tickets) > _MAX_CANDIDATES:
        notes.append(f"capped:{len(all_tickets)}→{_MAX_CANDIDATES}")
        all_tickets = all_tickets[:_MAX_CANDIDATES]

    notes.append(f"total:{len(all_tickets)}")
    return all_tickets, " | ".join(notes)


def snapshot_topology_search(
    topology_filters: dict,
    date_from: str | None, date_to: str | None,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, ticket_collection: str,
    snap_collection: str = "snapshots",
    default_limit: int = 200,
) -> list[str]:
    """Two-step topology-aware ticket retrieval via snapshots collection."""
    if not _CB_AVAILABLE:
        return []
    min_nodes = topology_filters.get("topology_min_nodes")
    max_nodes = topology_filters.get("topology_max_nodes")
    min_data  = topology_filters.get("topology_min_data")
    services  = topology_filters.get("topology_services") or []
    if not any([min_nodes, max_nodes, min_data, services]):
        return []
    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cl       = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cl.wait_until_ready(timedelta(seconds=10))
        ks_snap = f"`{bucket}`.`{scope}`.`{snap_collection}`"
        ks_tick = f"`{bucket}`.`{scope}`.`{ticket_collection}`"

        snap_where = ["ticket_ids IS NOT MISSING AND ARRAY_LENGTH(ticket_ids) > 0"]
        if min_nodes is not None:
            snap_where.append(f"node_count >= {min_nodes}")
        if max_nodes is not None:
            snap_where.append(f"node_count <= {max_nodes}")
        if min_data is not None:
            snap_where.append(f"topology.data_nodes >= {min_data}")
        for svc in services:
            snap_where.append(f"topology.{svc}_nodes > 0")

        snap_q = (
            f"SELECT cluster_uuid, cluster_name, node_count, ticket_ids "
            f"FROM {ks_snap} WHERE {' AND '.join(snap_where)} LIMIT 500"
        )
        snap_rows = list(cl.query(snap_q, QueryOptions(timeout=timedelta(seconds=20))))
        if not snap_rows:
            cl.close()
            return []

        all_tids: list[str] = []
        seen_tids: set[str] = set()
        for row in snap_rows:
            for tid in (row.get("ticket_ids") or []):
                s = str(tid)
                if s not in seen_tids:
                    all_tids.append(s)
                    seen_tids.add(s)
        if not all_tids:
            cl.close()
            return []

        if date_from or date_to:
            placeholders = ", ".join(f'"{t}"' for t in all_tids[:500])
            tick_where   = [f"t.ticket_id IN [{placeholders}]"]
            if date_from:
                tick_where.append(f"t.created >= '{date_from}'")
            if date_to:
                tick_where.append(f"t.created <= '{date_to}'")
            tick_q = (
                f"SELECT META(t).id AS doc_key FROM {ks_tick} AS t "
                f"WHERE {' AND '.join(tick_where)} "
                f"ORDER BY t.created DESC LIMIT {default_limit}"
            )
            tick_rows = list(cl.query(tick_q, QueryOptions(timeout=timedelta(seconds=20))))
            cl.close()
            return [r["doc_key"] for r in tick_rows if r.get("doc_key")]

        cl.close()
        return [f"ticket::{tid}" for tid in all_tids[:default_limit]]
    except Exception as exc:
        print(f"[snapshot_topology_search] {exc}")
        return []


def fetch_snapshots_for_clusters(
    cluster_uuids: list[str],
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str,
    snap_collection: str = "snapshots",
) -> dict[str, dict]:
    """Fetch the latest snapshot document for each cluster UUID."""
    if not cluster_uuids or not _CB_AVAILABLE:
        return {}
    try:
        conn_str = _cb_conn_str(cb_url, use_tls)
        cl       = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
        cl.wait_until_ready(timedelta(seconds=10))
        ks   = f"`{bucket}`.`{scope}`.`{snap_collection}`"
        uids = ", ".join(f'"{u}"' for u in cluster_uuids[:50])
        q    = (
            f"SELECT s.cluster_uuid, s.cluster_name, s.node_count, s.cb_version, s.date, "
            f"s.topology.data_nodes, s.topology.index_nodes, s.topology.query_nodes, "
            f"s.topology.fts_nodes, s.topology.eventing_nodes, s.topology.analytics_nodes, "
            f"s.topology.warn_items, s.topology.bad_items, s.topology.cluster_hostname "
            f"FROM {ks} s WHERE s.cluster_uuid IN [{uids}] ORDER BY s.date DESC"
        )
        rows = list(cl.query(q, QueryOptions(timeout=timedelta(seconds=15))))
        cl.close()
        result: dict[str, dict] = {}
        for row in rows:
            uid = row.get("cluster_uuid")
            if uid and uid not in result:
                result[uid] = row
        return result
    except Exception as exc:
        print(f"[fetch_snapshots_for_clusters] {exc}")
        return {}


def reciprocal_rank_fusion(*ranked_lists: list[str], k: int = 60) -> list[str]:
    """Merge ranked doc-key lists using Reciprocal Rank Fusion (k=60)."""
    scores: dict[str, float] = {}
    for ranked in ranked_lists:
        for rank, doc_id in enumerate(ranked):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores, key=lambda d: scores[d], reverse=True)


def _rrf_elbow_k(
    ordered_ids: list[str],
    all_ranked: list[list[str]],
    k_max: int,
    k_min: int = 3,
    rrf_k: int = 60,
) -> int:
    """
    Find the natural cutoff in RRF scores using the largest relative score drop
    (elbow detection).  Returns a value between k_min and k_max inclusive.

    Works by recomputing the RRF score for each candidate in ordered_ids[:k_max]
    and finding the index where the score drops most sharply relative to the
    previous score — the "elbow" in the score curve.  Everything above the elbow
    is signal; everything below is noise.
    """
    candidates = ordered_ids[:k_max]
    if len(candidates) <= k_min:
        return len(candidates)

    score_map: dict[str, float] = {}
    for ranked in all_ranked:
        for rank, doc_id in enumerate(ranked):
            score_map[doc_id] = score_map.get(doc_id, 0.0) + 1.0 / (rrf_k + rank + 1)

    scores = [score_map.get(doc_id, 0.0) for doc_id in candidates]

    best_drop, cut_at = 0.0, len(scores)
    for i in range(k_min - 1, len(scores) - 1):
        if scores[i] == 0:
            cut_at = i
            break
        drop = (scores[i] - scores[i + 1]) / scores[i]
        if drop > best_drop:
            best_drop, cut_at = drop, i + 1

    return max(k_min, min(cut_at, k_max))


def hybrid_retrieval(
    question: str,
    query_vec: list[float] | None,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    top_k: int = 10,
    in_memory_tickets: list[dict] | None = None,
    embed_fn: "Callable[[str], list[float]] | None" = None,
    original_question: str | None = None,
) -> tuple[list[dict], str]:
    """Dense vector + structured N1QL in parallel, merged with RRF."""
    filters = build_structured_query(original_question or question)
    cb_args = (cb_url, bucket, username, password, use_tls, scope, collection)
    vector_ids:    list[str] = []
    struct_ids:    list[str] = []
    expansion_ids: list[str] = []
    notes:         list[str] = []

    if not _CB_AVAILABLE:
        if in_memory_tickets:
            tickets, pf_note = prefilter_for_query(question, in_memory_tickets)
            kws = filters.get("struct_keywords") or filters.get("keywords") or []
            if kws:
                def _kw_match(t: dict) -> bool:
                    haystack = " ".join([
                        str(t.get("subject") or ""),
                        str(t.get("description") or ""),
                        str(t.get("tags") or ""),
                    ]).lower()
                    return any(kw.lower() in haystack for kw in kws)
                tickets = [t for t in tickets if _kw_match(t)] or tickets
            return tickets[:top_k], f"in-memory fallback ({pf_note})"
        return [], "CB unavailable, no in-memory tickets"

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_struct = pool.submit(structured_search_cb, filters, *cb_args, top_k)
        f_vec    = pool.submit(vector_search_cb, query_vec, *cb_args, top_k * 3) \
                   if query_vec else None

        try:
            struct_ids = f_struct.result(timeout=20)
            if struct_ids:
                notes.append(f"{len(struct_ids)} structured")
        except Exception as e:
            notes.append(f"structured err: {e}")

        if f_vec:
            try:
                _raw_vec = f_vec.result(timeout=30)
                _snap_from_vec   = [k for k in _raw_vec if k.startswith("snapshot::")]
                vector_ids       = [k for k in _raw_vec if not k.startswith("snapshot::")]
                notes.append(f"{len(vector_ids)} vector")
                if _snap_from_vec:
                    _xref = _snap_keys_to_ticket_keys(_snap_from_vec, *cb_args)
                    expansion_ids.extend(k for k in _xref if k not in expansion_ids)
                    notes.append(f"{len(_xref)} snap-vec→ticket")
            except Exception as e:
                notes.append(f"vector err: {e}")

    keyword_ids: list[str] = []
    _skw = filters.get("struct_keywords") or []
    if _skw and _CB_AVAILABLE:
        try:
            _raw_kw = fts_keyword_search_cb(_skw, *cb_args, top_k * 3)
            _snap_from_kw = [k for k in _raw_kw if k.startswith("snapshot::")]
            keyword_ids   = [k for k in _raw_kw if not k.startswith("snapshot::")]
            notes.append(f"{len(keyword_ids)} fts-keyword")
            if _snap_from_kw:
                _xref = _snap_keys_to_ticket_keys(_snap_from_kw, *cb_args)
                expansion_ids.extend(k for k in _xref if k not in expansion_ids)
                notes.append(f"{len(_xref)} snap-kw→ticket")
        except Exception as e:
            notes.append(f"fts-keyword err: {e}")

    topology_ids: list[str] = []
    _snap_col = "snapshots"
    if any([filters.get("topology_min_nodes"), filters.get("topology_max_nodes"),
            filters.get("topology_min_data"), filters.get("topology_services")]):
        try:
            topology_ids = snapshot_topology_search(
                filters,
                filters.get("date_from"), filters.get("date_to"),
                cb_url, bucket, username, password, use_tls, scope, collection,
                snap_collection=_snap_col,
            )
            if topology_ids:
                notes.append(f"{len(topology_ids)} topology-snapshot")
        except Exception as e:
            notes.append(f"topology err: {e}")

    if filters.get("ticket_ids") and embed_fn and _CB_AVAILABLE:
        _mem_map = {str(t.get("ticket_id", "")): t for t in (in_memory_tickets or [])}
        _target_tickets = [_mem_map[tid] for tid in filters["ticket_ids"] if tid in _mem_map]
        if not _target_tickets:
            _target_keys = [f"ticket::{tid}" for tid in filters["ticket_ids"]]
            _target_tickets = fetch_tickets_by_keys(_target_keys, *cb_args)

        if _target_tickets:
            _enriched_parts: list[str] = []
            for t in _target_tickets[:3]:
                parts = []
                if t.get("subject"):
                    parts.append(t["subject"])
                for cid in _ticket_cluster_ids(t)[:2]:
                    parts.append(f"cluster {cid}")
                if t.get("priority"):
                    parts.append(f"priority {t['priority']}")
                if t.get("description"):
                    parts.append((t["description"] or "")[:200])
                _enriched_parts.append(" ".join(parts))

            _enriched_query = question + "\n" + "\n".join(_enriched_parts)
            try:
                _exp_vec = embed_fn(_enriched_query)
                expansion_ids = vector_search_cb(_exp_vec, *cb_args, top_k * 2)
                notes.append(f"{len(expansion_ids)} expansion")
            except Exception as e:
                notes.append(f"expansion err: {e}")

    all_lists = [lst for lst in (struct_ids, vector_ids, keyword_ids, topology_ids, expansion_ids) if lst]
    if not all_lists:
        if in_memory_tickets:
            tickets, pf_note = prefilter_for_query(question, in_memory_tickets)
            return tickets[:top_k], f"in-memory fallback ({pf_note})"
        return [], "no results"

    _rrf_ordered = reciprocal_rank_fusion(*all_lists)
    _rrf_cap     = top_k * 2
    _rrf_set     = set(_rrf_ordered[:_rrf_cap])
    _struct_forced = [k for k in struct_ids if k not in _rrf_set][:top_k]
    _sf_set = set(_struct_forced)
    _kw_forced = [k for k in keyword_ids if k not in _rrf_set and k not in _sf_set][:top_k]
    _kf_set = set(_kw_forced)
    merged_ids = (
        _struct_forced
        + _kw_forced
        + [k for k in _rrf_ordered[:_rrf_cap] if k not in _sf_set and k not in _kf_set]
    )
    notes.append(f"{len(merged_ids)} after RRF (sf={len(_struct_forced)} kf={len(_kw_forced)})")

    resolved: list[dict] = []
    if _CB_AVAILABLE:
        resolved = fetch_tickets_by_keys(merged_ids, *cb_args)
        if resolved:
            notes.append(f"cb:{len(resolved)}")
        else:
            notes.append("cb:0(failed?)")
        if in_memory_tickets:
            _cb_ids = {str(t.get("ticket_id", "")) for t in resolved}
            mem_map = {str(t.get("ticket_id", "")): t for t in in_memory_tickets}
            _mem_filled = 0
            for k in merged_ids:
                tid = k.split("::")[-1]
                if tid not in _cb_ids and tid in mem_map:
                    resolved.append(mem_map[tid])
                    _mem_filled += 1
            if _mem_filled:
                notes.append(f"mem-fill:{_mem_filled}")
    elif in_memory_tickets:
        mem_map = {str(t.get("ticket_id", "")): t for t in in_memory_tickets}
        resolved = [mem_map[k.split("::")[-1]] for k in merged_ids
                    if k.split("::")[-1] in mem_map]
        notes.append(f"mem-only:{len(resolved)}")

    order = {k.split("::")[-1]: i for i, k in enumerate(merged_ids)}
    resolved.sort(key=lambda t: order.get(str(t.get("ticket_id", "")), 999))

    kws = [k for k in (filters.get("struct_keywords") or filters.get("keywords") or []) if len(k) >= 3]
    _kw_trusted = set(keyword_ids)
    if kws and not filters.get("ticket_ids"):
        _c2a_s6 = _get_cluster_to_app()
        _known_apps_s6 = set(_get_app_cluster_aliases().keys())
        _queried_apps_s6 = {kw.lower() for kw in kws if kw.lower() in _known_apps_s6}
        def _kw_match(t: dict) -> bool:
            if _queried_apps_s6:
                _t_cids = _ticket_cluster_ids(t)
                _t_apps = {_c2a_s6.get(cid, "") for cid in _t_cids} - {""}
                if _t_apps and not (_t_apps & _queried_apps_s6):
                    return False
            _key = f"ticket::{t.get('ticket_id', '')}"
            if _key in _kw_trusted:
                return True
            _comments_raw = t.get("comments") or []
            if isinstance(_comments_raw, list):
                _comments_str = " ".join(
                    str(c.get("body") or c.get("content") or c) for c in _comments_raw
                )[:1000]
            else:
                _comments_str = str(_comments_raw)[:1000]
            haystack = " ".join([
                str(t.get("subject") or ""), str(t.get("description") or ""),
                str(t.get("tags") or ""), _comments_str,
            ]).lower()
            if any(kw.lower() in haystack for kw in kws):
                return True
            _ticket_apps = {_c2a_s6.get(cid, "") for cid in _ticket_cluster_ids(t)} - {""}
            if any(kw.lower() in _ticket_apps for kw in kws):
                return True
            for host, app in _c2a_s6.items():
                if host in haystack and any(kw.lower() == app for kw in kws):
                    return True
            return False
        filtered = [t for t in resolved if _kw_match(t)]
        if filtered:
            resolved = filtered
            notes.append(f"keyword-filtered to {len(resolved)}")

    _date_from = filters.get("date_from")
    _date_to   = filters.get("date_to")
    if _date_from or _date_to:
        def _in_date_range(t: dict) -> bool:
            _cd = str(t.get("created") or "")[:10]
            if not _cd:
                return True
            if _date_from and _cd < _date_from:
                return False
            if _date_to and _cd > _date_to:
                return False
            return True
        date_filtered = [t for t in resolved if _in_date_range(t)]
        if date_filtered:
            resolved = date_filtered
            notes.append(f"date-filtered to {len(resolved)}")

    _auto_k = _rrf_elbow_k(_rrf_ordered, all_lists, k_max=min(_rrf_cap, len(resolved)))
    _final_k = min(_auto_k, top_k, len(resolved))
    if _final_k < top_k:
        notes.append(f"elbow@{_final_k}")
    return resolved[:_final_k], " | ".join(notes)


def compute_and_mark_freshness(
    organization: str,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    cookie: str = "",
    verified_by: str = "",
) -> dict:
    """Live-vs-local ticket-ID reconciliation; writes the freshness::<org> marker.

    Single shared writer for the freshness lifecycle — used by the MCP
    check_data_freshness tool AND the pipeline's post-job completion hook, so
    a rescrape always concludes with a VERIFIED freshness state rather than an
    assumed one. verified_by records what produced this check (e.g. a job id).

    One-directional by design: live-referenced IDs missing locally = drift;
    local-not-in-live is normal. Missing candidates are validated against the
    org's own live ticket listing so foreign/deleted snapshot refs surface as
    unresolvable_snapshot_refs instead of a perpetual stale status. Raises on
    live-lookup or CB failure — callers decide how to log.
    """
    import datetime as _dt
    from supportal.api_client import fetch_snapshots_via_analytics

    live_rows = fetch_snapshots_via_analytics(organization, cookie, limit=5000)
    live_ids: set[str] = set()
    for r in live_rows:
        for tid in r.get("ticket_ids") or []:
            if tid:
                live_ids.add(str(tid))
    resolved_org = next(
        (r.get("organization") for r in live_rows if r.get("organization")),
        organization,
    )

    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions, QueryOptions
    conn = cb_url if "://" in cb_url else ("couchbases://" if use_tls else "couchbase://") + cb_url
    cl = Cluster(conn, ClusterOptions(PasswordAuthenticator(username, password)))
    ks = f"`{bucket}`.`{scope}`.`{collection}`"
    rows = list(cl.query(
        f"SELECT RAW TO_STRING(t.ticket_id) FROM {ks} t WHERE LOWER(t.organization) IN $orgs",
        QueryOptions(named_parameters={"orgs": sorted(
            {organization.lower().strip(), resolved_org.lower().strip()}
        )}),
    ))
    local_ids = {r for r in rows if r}

    missing = sorted(live_ids - local_ids, key=lambda x: int(x) if x.isdigit() else 0)

    # Old snapshots can carry zendesk[] IDs that belong to OTHER customers or
    # deleted tickets (cross-era Supportal data). Those would flag the org
    # stale forever and trigger a pointless rescrape on every monitor run, so
    # validate each candidate against the org's own live ticket listing:
    # only IDs the org actually owns count as missing; the rest are reported
    # separately as unresolvable_snapshot_refs and don't affect status.
    unresolvable: list[str] = []
    listing_validated = False
    listing_error = ""
    if missing:
        try:
            from supportal.api_client import (
                _get_customer_ticket_listing_api, _make_api_session,
            )
            listing = _get_customer_ticket_listing_api(
                resolved_org, _make_api_session(cookie)
            )
            listing_ids = {str(r.get("id") or "").strip() for r in listing if r.get("id")}
            if listing_ids:
                unresolvable = [t for t in missing if t not in listing_ids]
                missing = [t for t in missing if t in listing_ids]
                listing_validated = True
            else:
                listing_error = "org ticket listing came back empty; skipped validation"
        except Exception as exc:
            listing_error = f"listing validation unavailable: {exc}"

    status = "fresh" if not missing else "stale"
    now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
    marker = {
        "type":            "freshness",
        "organization":    resolved_org,
        "requested_as":    organization,
        "checked_at":      now_iso,
        "live_ticket_ids": len(live_ids),
        "local_tickets":   len(local_ids),
        "missing_count":   len(missing),
        "missing_ids":     missing[:50],
        "unresolvable_snapshot_refs":       unresolvable[:50],
        "unresolvable_snapshot_ref_count":  len(unresolvable),
        "listing_validated": listing_validated,
        "status":          status,
        "source":          "snapshot.zendesk[] via Supportal Analytics API",
    }
    if listing_error:
        marker["listing_validation_note"] = listing_error
    if verified_by:
        marker["verified_by"] = verified_by
    try:
        _ensure_collection(cl, bucket, scope, "markers")
        key = f"freshness::{resolved_org.lower().replace(' ', '_')}"
        cl.bucket(bucket).scope(scope).collection("markers").upsert(key, marker)
        marker["marker_key"] = key
        marker["saved"] = True
    except Exception as exc:
        marker["saved"] = False
        marker["save_error"] = str(exc)
    if status == "stale":
        marker["recommendation"] = (
            f"{len(missing)} live-referenced ticket(s) missing locally — run "
            f"rescrape_customer_tickets('{resolved_org}') then re-check."
        )
    return marker


def _ensure_collection(cluster, bucket_name: str, scope_name: str, coll_name: str) -> None:
    """Create a collection if missing. Never raises."""
    try:
        from couchbase.management.collections import CollectionSpec
        cm = cluster.bucket(bucket_name).collections()
        existing = {s.name: {c.name for c in s.collections} for s in cm.get_all_scopes()}
        if coll_name not in existing.get(scope_name, set()):
            cm.create_collection(CollectionSpec(coll_name, scope_name=scope_name))
    except Exception:
        pass
