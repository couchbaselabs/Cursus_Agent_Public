"""
Supportal REST API client helpers.

All functions that make outbound HTTP requests to Supportal endpoints:
  - _make_api_session          — authenticated requests.Session factory
  - query_supportal_analytics  — POST /api/support360/query
  - fetch_snapshots_via_analytics — snapshot listing via SQL++
  - fetch_ticket_api           — GET /zendesk/ticket/{id}/status
  - _get_customer_ticket_listing_api
  - _get_customer_snapshot_listing_api
  - search_customers_on_supportal — /search/{q}/data endpoint
  - search_customers_via_analytics — SQL++ LIKE customer search
  - resolve_customer_name      — chains Analytics + local CB + UI search
  - _find_tickets_tab_url      — extract Tickets-tab href from customer page HTML

Note: fetch_snapshot_topology_api lives in snapshot_parser.py because it
calls _parse_structured_api_json which owns the topology parsing logic.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from typing import Callable, Optional

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup

from supportal.constants import BASE_URL, UA
from supportal.ticket_parser import parse_ticket_from_api


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def _make_api_session(cookie: str) -> requests.Session:
    """Create a requests.Session authenticated for Supportal REST APIs."""
    sess = requests.Session()
    sess.verify = False
    sess.headers.update({
        "User-Agent": UA,
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": cookie,
    })
    return sess


# ---------------------------------------------------------------------------
# Analytics SQL++ endpoint
# ---------------------------------------------------------------------------

def _extract_xsrf(cookie: str) -> str:
    """Pull the _xsrf token value out of a raw Cookie header string, URL-decoded."""
    for part in cookie.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == "_xsrf":
            return urllib.parse.unquote(v.strip())
    return ""


def query_supportal_analytics(statement: str, cookie: str = "") -> list[dict]:
    """POST /api/support360/query with a SQL++ statement; returns result rows as dicts.

    NOTE (v2.6.2): As of 2026-06-22 the analytics endpoint is open — no auth required.
    The cookie parameter is retained so callers don't break and can be re-enabled if
    Supportal adds authentication in the future.
    """
    url = f"{BASE_URL}/api/support360/query"
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/v2/analytics/query",
    }
    # Re-enable if Supportal adds auth: uncomment and pass cookie from callers.
    # if cookie:
    #     headers["Cookie"] = cookie
    #     xsrf = _extract_xsrf(cookie)
    #     if xsrf:
    #         headers["X-Xsrftoken"] = xsrf
    try:
        resp = requests.post(
            url,
            json={"statement": statement, "scope": "v1"},
            headers=headers,
            timeout=60,
            verify=False,
        )
    except Exception as exc:
        raise RuntimeError(f"Analytics request failed: {exc}") from exc
    if resp.status_code in (401, 403):
        raise RuntimeError(
            f"Analytics returned HTTP {resp.status_code} — endpoint may now require authentication. "
            "Re-enable cookie auth in api_client.py query_supportal_analytics."
        )
    resp.raise_for_status()
    body = resp.text.strip()
    if not body:
        raise ValueError(
            f"Analytics endpoint returned HTTP {resp.status_code} with empty body."
        )
    if body.lstrip().startswith("<"):
        snippet = body[:200].replace("\n", " ")
        raise ValueError(
            f"Analytics endpoint returned HTML (HTTP {resp.status_code}). "
            f"Response: {snippet!r}"
        )
    try:
        payload = resp.json()
    except Exception:
        raise ValueError(f"Analytics non-JSON response: {body[:300]}")
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    rows = payload.get("results") or payload.get("rows") or []
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected analytics response shape: {list(payload.keys())}")
    return rows


# ---------------------------------------------------------------------------
# Snapshot listing
# ---------------------------------------------------------------------------

def _resolve_customer_name(customer_name: str, _log=None) -> str:
    """Resolve a possibly-imprecise customer name to the canonical live name.

    All lookups hit only the small `customer` collection (~1s) — never the
    snapshot JOIN. Order: exact → case-insensitive → LIKE prefix → LIKE
    contains → difflib fuzzy suggestion. Raises ValueError with suggestions
    when nothing matches.
    """
    def log(msg: str):
        if _log:
            _log(msg, 0.1)

    name = customer_name.strip().strip("\"'")
    esc = name.lower().replace("%", "\\%").replace("_", "\\_")
    attempts = [
        f"cu.`name` = {json.dumps(name)}",
        f"LOWER(cu.`name`) = {json.dumps(name.lower())}",
        f"LOWER(cu.`name`) LIKE {json.dumps(esc + '%')}",
        f"LOWER(cu.`name`) LIKE {json.dumps('%' + esc + '%')}",
    ]
    for expr in attempts:
        rows = query_supportal_analytics(
            "SELECT cu.`name` AS n, ARRAY_LENGTH(cu.`clusters`) AS nc "
            f"FROM customer cu WHERE {expr} LIMIT 10"
        )
        cands = {r["n"]: (r.get("nc") or 0) for r in rows if r.get("n")}
        if cands:
            # Duplicate/stub docs exist live (e.g. empty 'amex' and
            # 'american express az' beside the real 'American Express AZ') —
            # the doc that actually owns clusters wins, then name as tiebreak.
            resolved = max(sorted(cands), key=lambda n: cands[n])
            if len(cands) > 1:
                log(f"Ambiguous name {name!r} → {cands}; using {resolved!r}")
            elif resolved.lower() != name.lower():
                log(f"Resolved {name!r} → {resolved!r}")
            if cands[resolved] == 0:
                log(f"Warning: {resolved!r} matched but owns no clusters — continuing search")
                continue
            return resolved

    # Fuzzy fallback: suggest close matches, still customer-collection only.
    _close: list[str] = []
    try:
        import difflib as _dl
        _all_rows = query_supportal_analytics(
            "SELECT DISTINCT cu.`name` AS n FROM customer cu WHERE LENGTH(cu.`name`) > 2 LIMIT 15000",
        )
        _all_names = [r["n"] for r in _all_rows if r.get("n")]
        _close = _dl.get_close_matches(name, _all_names, n=3, cutoff=0.5)
    except Exception:
        pass
    if _close:
        _names = ", ".join(f"'{n}'" for n in _close)
        raise ValueError(
            f"Customer {name!r} not found. Did you mean: {_names}? "
            "Use the exact name and try again."
        )
    raise ValueError(
        f"Customer {name!r} not found in the analytics database. "
        "Check the spelling or use list_supportal_customers to browse all customers."
    )


def fetch_snapshots_via_analytics(
    customer_name: str,
    cookie: str | None = None,
    limit: int = 200,
    progress_cb: Callable[[str, float], None] | None = None,
) -> list[dict]:
    """Fetch snapshot listing + Zendesk ticket IDs via the Supportal analytics API.

    Tries exact match, then case-insensitive, then LIKE prefix — so partial or
    differently-cased customer names still resolve.
    cookie is unused as of v2.6.2 (endpoint is open); retained for future re-enablement.
    """
    def _log(msg: str, pct: float = 0.0):
        print(f"[SNAP-ANALYTICS] {msg}")
        if progress_cb:
            progress_cb(msg, pct)

    customer_name = customer_name.strip().strip('"\'')
    _log(f"Querying analytics for customer: {customer_name!r}", 0.05)

    def _make_statement(name_expr: str) -> str:
        return (
            "SELECT "
            "REPLACE(META(sn).id, \"Snapshot::\", \"\") AS snap_id, "
            "sn.`timestamp` AS date, "
            "sn.`uuid` AS cluster_uuid, "
            "cl.`ui_name` AS cluster_name, "
            "cu.`name` AS organization, "
            "sn.`zendesk` AS ticket_ids "
            "FROM snapshot sn "
            "JOIN cluster cl ON META(cl).id = (\"Cluster::\" || sn.`uuid`) "
            "JOIN customer cu ON META(cu).id = (\"Customer::\" || cl.`customer`) "
            f"WHERE {name_expr} "
            "ORDER BY sn.`timestamp` DESC "
            f"LIMIT {int(limit)}"
        )

    # Resolve the canonical name FIRST via cheap queries against the small
    # `customer` collection. A non-matching predicate on the 3-way snapshot
    # JOIN forces a full scan that times out (>60s) and 500s server-side, so
    # the heavy query must only ever run once, with a name known to exist.
    resolved = _resolve_customer_name(customer_name, _log)
    rows = query_supportal_analytics(
        _make_statement(f"LOWER(cu.`name`) = {json.dumps(resolved.lower())}"), cookie
    )
    if not rows:
        raise ValueError(
            f"No snapshots found for customer {resolved!r} in the analytics database."
        )
    _log(f"Analytics returned {len(rows)} snapshot records.", 0.5)

    import datetime as _dt
    now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
    results: list[dict] = []
    for row in rows:
        snap_id = row.get("snap_id") or ""
        cluster_uuid = row.get("cluster_uuid") or (snap_id.split("::")[0] if "::" in snap_id else "")
        ticket_ids = [str(t) for t in (row.get("ticket_ids") or []) if t]
        enc = urllib.parse.quote(snap_id, safe="")
        results.append({
            "snap_id":            snap_id,
            "cluster_id":         cluster_uuid,
            "url":                f"{BASE_URL}/snapshot/{enc}",
            "date":               row.get("date") or "",
            "organization":       row.get("organization") or customer_name,
            "customer_url":       f"{BASE_URL}/customer/{urllib.parse.quote(customer_name, safe='')}",
            "cluster_name":       row.get("cluster_name") or "",
            "cluster_uuid":       cluster_uuid,
            "capella_cluster_id": "",
            "topology":           {},
            "ticket_ids":         ticket_ids,
            "scraped_at":         now_iso,
            "bad_count": 0, "warn_count": 0, "bad_items": [], "warn_items": [],
            "cb_version": "", "node_count": 0, "bucket_names": [],
            "auto_failover_seconds": None, "ram_per_node_mib": None,
            "bucket_count": 0, "server_groups": [],
        })

    _log(f"Done — {len(results)} snapshots with ticket IDs.", 1.0)
    return results


# ---------------------------------------------------------------------------
# Ticket fetch
# ---------------------------------------------------------------------------

def fetch_ticket_api(ticket_id: str, session: requests.Session) -> dict:
    """GET /zendesk/ticket/{id}/status → parsed ticket dict."""
    url = f"{BASE_URL}/zendesk/ticket/{ticket_id}/status"
    ticket_url = f"{BASE_URL}/zendesk/ticket/{ticket_id}"
    try:
        resp = session.get(url, timeout=30, allow_redirects=True)
        if resp.status_code == 200:
            body = resp.json()
            return parse_ticket_from_api(body, ticket_id)
        if resp.status_code == 404:
            return {"ticket_id": ticket_id, "_deleted": True, "url": ticket_url}
        return {"ticket_id": ticket_id, "error": f"HTTP {resp.status_code}", "url": ticket_url}
    except Exception as exc:
        return {"ticket_id": ticket_id, "error": str(exc), "url": ticket_url}


# ---------------------------------------------------------------------------
# Customer listing endpoints
# ---------------------------------------------------------------------------

def _get_customer_ticket_listing_api(
    org_name: str,
    session: requests.Session,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    """GET /customer/{org}/status/tickets → list of ticket summary dicts.

    Supportal slugs are case-sensitive ('NetDocuments' ≠ 'netdocuments').
    We first hit the customer page with allow_redirects to discover the
    canonical casing, then call /status/tickets on that resolved slug.
    """
    encoded = urllib.parse.quote(org_name.strip(), safe="")
    base_url = f"{BASE_URL}/customer/{encoded}"

    # Resolve the canonical slug via a HEAD-like redirect on the customer page
    canonical_base = base_url
    try:
        head = session.get(base_url, timeout=15, allow_redirects=True)
        m = re.search(r"/customer/([^/?#]+)", head.url)
        if m:
            canonical_base = f"{BASE_URL}/customer/{m.group(1)}"
    except Exception:
        pass

    url = f"{canonical_base}/status/tickets"
    print(f"[LISTING-API] GET {url}")
    try:
        resp = session.get(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("tickets") or data.get("data") or []
    except Exception as exc:
        if progress_cb:
            progress_cb(f"Listing API error: {exc}", 0.01)
        print(f"[LISTING-API] error: {exc}")
    return []


def _get_customer_snapshot_listing_api(
    org_name: str,
    session: requests.Session,
) -> list[dict]:
    """GET /customer/{org}/status/snapshots → list of snapshot summary dicts.
    Returns empty list if the endpoint is unavailable."""
    encoded = urllib.parse.quote(org_name.strip(), safe="")
    url = f"{BASE_URL}/customer/{encoded}/status/snapshots"
    try:
        resp = session.get(url, timeout=30, allow_redirects=True)
        if resp.status_code in (404, 500):
            return []
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("snapshots") or data.get("data") or []
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# Customer search
# ---------------------------------------------------------------------------

def search_customers_on_supportal(
    query: str,
    cookie: Optional[str],
    max_results: int = 30,
) -> list[dict]:
    """
    Search Supportal /search/{query}/data and return Customer-type results.
    Returns list of {slug, display_name, url} sorted by relevance.
    """
    if not query.strip():
        return []
    if not cookie:
        return []

    session = _make_api_session(cookie)
    api_url = (
        f"{BASE_URL}/search/{urllib.parse.quote(query.strip(), safe='')}/data"
        f"?page=0&index=Customers&resultsPerPage={max_results}&sortOrder=-_score"
    )

    try:
        resp = session.get(api_url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"[CUST-SEARCH] API error: {exc}")
        return []

    rows = data if isinstance(data, list) else (
        data.get("results") or data.get("data") or data.get("hits") or []
    )

    results: list[dict] = []
    seen_slugs: set[str] = set()

    for item in rows:
        if not isinstance(item, dict):
            continue
        src = item.get("_source", {}) or {}
        link_url = item.get("link_url") or item.get("url") or src.get("link_url") or ""
        item_type = (item.get("type") or src.get("type") or "").lower()
        if item_type and item_type not in ("customer", "customers"):
            continue

        m = re.search(r"/customer/([^/?#]+)", link_url, re.I)
        if not m:
            continue
        slug = urllib.parse.unquote(m.group(1))
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        display_name = (
            item.get("name") or item.get("display_name") or item.get("title") or
            src.get("name") or slug
        )
        url = f"{BASE_URL}/customer/{urllib.parse.quote(slug, safe='')}"
        results.append({"slug": slug, "display_name": display_name, "url": url})
        if len(results) >= max_results:
            break

    return results


def search_customers_via_analytics(
    query: str,
    cookie: str | None,
    limit: int = 20,
) -> list[dict]:
    """
    Search Supportal customer names via the Analytics SQL++ endpoint using
    a LIKE '%query%' pattern. This catches partial / imprecise names that the
    UI search API (/search/.../data) misses.
    Returns list of {slug, display_name, url, source}.
    """
    if not query.strip():
        return []
    like_val = f"%{query.strip().lower().replace('%', '\\%').replace('_', '\\_')}%"
    try:
        rows = query_supportal_analytics(
            f"SELECT DISTINCT cu.`name` AS display_name "
            f"FROM customer cu "
            f"WHERE LOWER(cu.`name`) LIKE {json.dumps(like_val)} "
            f"ORDER BY cu.`name` LIMIT {int(limit)}",
            cookie,
        )
    except Exception as exc:
        print(f"[CUST-SEARCH-ANALYTICS] {exc}")
        return []
    results: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        name = (row.get("display_name") or "").strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        url = f"{BASE_URL}/customer/{urllib.parse.quote(name, safe='')}"
        results.append({"slug": name, "display_name": name, "url": url, "source": "Analytics"})
    return results


def resolve_customer_name(
    query: str,
    cookie: str | None = None,
    cb_url: str = "",
    cb_bucket: str = "supportal",
    cb_user: str = "",
    cb_pass: str = "",
    cb_tls: bool = False,
    cb_scope: str = "_default",
    cb_collection: str = "tickets",
    limit: int = 10,
) -> list[dict]:
    """
    Find Supportal customer names matching a partial/fuzzy query.
    Chains three sources and deduplicates by normalized name:
      1. Analytics SQL++ LIKE '%query%'   — Supportal's live customer list
      2. Local Couchbase LIKE '%query%'   — customers already scraped
      3. Supportal UI search API          — keyword-search fallback
    Returns list of {display_name, url, source} sorted by source quality.
    """
    all_hits: list[dict] = []
    seen: set[str] = set()

    def _add(name: str, source: str) -> None:
        key = name.strip().lower()
        if not key or key in seen:
            return
        seen.add(key)
        url = f"{BASE_URL}/customer/{urllib.parse.quote(name.strip(), safe='')}"
        all_hits.append({"display_name": name.strip(), "url": url, "source": source})

    # 1 — Analytics LIKE (most complete, authoritative; open endpoint as of v2.6.2)
    try:
        for h in search_customers_via_analytics(query, cookie, limit):
            _add(h.get("display_name") or h.get("slug") or "", "Analytics")
    except Exception:
        pass

    # 2 — Local CB LIKE (works offline)
    if cb_url and cb_user:
        try:
            from supportal.cb_helpers import search_orgs_from_cb as _search_orgs_fn  # noqa: PLC0415
            for org in _search_orgs_fn(cb_url, cb_bucket, cb_user, cb_pass, cb_tls, cb_scope, cb_collection, query, limit):
                _add(org, "Local DB")
        except ImportError:
            pass
        except Exception:
            pass

    # 3 — Supportal UI search (good for short-name lookups)
    if len(all_hits) < 3:
        try:
            for h in search_customers_on_supportal(query, cookie, limit):
                _add(h.get("display_name") or h.get("slug") or "", "Supportal")
        except Exception:
            pass

    # 4 — Per-word LIKE: split on whitespace, try each word >= 4 chars individually.
    # Catches cases where one word is garbled but another is recognisable
    # (e.g. "Azerixan Express" → word "Express" → matches "American Express AZ").
    if not all_hits:
        _words = [w for w in query.split() if len(w) >= 4]
        for _w in _words:
            try:
                for h in search_customers_via_analytics(_w, cookie, limit):
                    _add(h.get("display_name") or h.get("slug") or "", "Partial match")
            except Exception:
                pass

    # 5 — difflib fuzzy fallback when all LIKE/FTS searches come up empty
    # Handles transpositions and garbled names that LIKE can't catch.
    if not all_hits:
        try:
            import difflib as _dl
            _all_rows = query_supportal_analytics(
                "SELECT DISTINCT cu.`name` AS n FROM customer cu WHERE LENGTH(cu.`name`) > 2 LIMIT 15000",
            )
            _all_names = [r["n"] for r in _all_rows if r.get("n")]
            for name in _dl.get_close_matches(query, _all_names, n=3, cutoff=0.4):
                _add(name, "Fuzzy match")
        except Exception:
            pass

    return all_hits[:limit]


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def _find_tickets_tab_url(html: str, current_url: str) -> Optional[str]:
    """
    Find the Tickets tab link on a customer page.
    Only considers links that are sub-paths of the current customer URL so that
    global nav links (e.g. dashboard#tickets) are never mistakenly returned.
    Returns the absolute URL or None.
    """
    soup = BeautifulSoup(html, "html.parser")
    customer_base = current_url.rstrip("/")  # e.g. ".../customer/convera"

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()

        # Resolve to absolute URL so we can compare paths properly
        abs_href = href if href.startswith("http") else urllib.parse.urljoin(current_url, href)

        # Skip any link that doesn't sit under the customer's own path
        if not abs_href.startswith(customer_base):
            continue

        # Match /tickets sub-path or link text "tickets"
        if text == "tickets" or re.search(r"/tickets(?:/|\?|$|#)", abs_href, re.I):
            return abs_href

    return None
