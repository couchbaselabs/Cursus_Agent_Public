#!/usr/bin/env python3
"""
Couchbase Supportal Scraper — NiceGUI App
==========================================
Single script replacing all previous scraper versions.

Auth modes:
  1. Cookie paste  — copy the Cookie header from browser DevTools, paste here.
                     Uses requests (no browser required).
  2. Browser login — opens a real Chromium window so you can complete SSO,
                     then scrapes headlessly with the saved session.

Usage:
  ./venv/bin/python supportal_nicegui_app.py
  # then open http://localhost:8765 in your browser
"""

import asyncio
import csv
import io
import json
import os
import re
import time
import urllib.parse
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup
from nicegui import run, ui
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Optional — Couchbase SDK (Phase 1 + 2).  Import lazily so the app starts
# even if the package is not installed.
try:
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions, UpsertOptions, SearchOptions, QueryOptions
    from couchbase.exceptions import CouchbaseException
    from couchbase.search import SearchRequest
    from couchbase.vector_search import VectorQuery, VectorSearch
    from datetime import timedelta
    _CB_AVAILABLE = True
except ImportError:
    _CB_AVAILABLE = False

# Optional — LLM providers (Phase 2).
try:
    import anthropic as _anthropic_mod
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False

try:
    from google import genai as _genai_mod
    _GEMINI_AVAILABLE = True
except ImportError:
    _GEMINI_AVAILABLE = False

try:
    import openai as _openai_mod   # also covers Ollama chat + LMStudio
    _OPENAI_AVAILABLE = True
except ImportError:
    _OPENAI_AVAILABLE = False

try:
    from mlx_embeddings import load as _mlx_emb_load
    import mlx.core as _mx
    _MLX_EMB_AVAILABLE = True
except ImportError:
    _MLX_EMB_AVAILABLE = False

# Cached MLX embedding model — loaded once, reused for every ticket
_mlx_emb_cache: dict = {"model": None, "tokenizer": None, "model_id": None}

# ─────────────────────────── Constants ────────────────────────────────────────

BASE_URL = "https://supportal.couchbase.com"
PROFILE_DIR = os.path.join(os.path.dirname(__file__), ".playwright_supportal")
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TICKET_HREF_RE = re.compile(r"/zendesk/ticket/(\d+)(?:\?.*)?$")

# Module-level state — safe because NiceGUI runs as a single process.
# The headful browser is kept alive between "Open Browser" and "Confirm Login"
# by holding a reference here.
_browser_state: dict = {
    "pw": None,       # sync_playwright() handle
    "ctx": None,      # launch_persistent_context() handle
    "logged_in": False,
}

# Shared results — written by worker thread, read by UI download handlers.
_results: list[dict] = []


# ─────────────────────────── Extraction helpers ───────────────────────────────

def _find_label_value(soup: BeautifulSoup, *labels: str) -> Optional[str]:
    """
    Search for a field by label using multiple strategies:
      1. dt/dd pairs
      2. th/td pairs in table rows
      3. Any element whose text matches the label, then look for a sibling value
    """
    for label in labels:
        pat = re.compile(rf"^\s*{re.escape(label)}\s*:?\s*$", re.I)

        # Strategy 1: dl / dt+dd
        el = soup.find(string=pat)
        if el:
            parent = el.parent
            if parent:
                if parent.name in ("dt", "th", "label", "strong", "b"):
                    sib = parent.find_next_sibling(["dd", "td", "span", "div", "p"])
                    if sib:
                        txt = sib.get_text(" ", strip=True)
                        if txt:
                            return txt
                # Embedded in a table row
                tr = parent.find_parent("tr")
                if tr:
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        txt = tds[1].get_text(" ", strip=True)
                        if txt:
                            return txt

        # Strategy 2: explicit dl structure
        for dl in soup.find_all("dl"):
            for dt in dl.find_all("dt"):
                if re.search(rf"\b{re.escape(label)}\b", dt.get_text(), re.I):
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        txt = dd.get_text(" ", strip=True)
                        if txt:
                            return txt

        # Strategy 3: th:td in any table
        for tr in soup.find_all("tr"):
            th = tr.find(["th", "td"])
            if th and re.search(rf"^\s*{re.escape(label)}\s*:?\s*$", th.get_text(), re.I):
                rest = tr.find_all("td")
                if rest:
                    txt = rest[-1].get_text(" ", strip=True)
                    if txt:
                        return txt

    return None


_DATE_RE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}"
    r"|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}"
    r"|\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?",
    re.I,
)


def _guess_author_from_text(text: str) -> Optional[str]:
    """Try to extract an author name from the tail of a comment (e.g. 'Regards,\nPiyush')."""
    for pat in [
        re.compile(r"(?:Regards|Thanks|Best|Cheers|Sincerely)[,.]?\s*\n+\s*([A-Z][A-Za-z ]+)", re.I),
        re.compile(r"(?:^|\n)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\s*$"),
    ]:
        m = pat.search(text)
        if m:
            candidate = m.group(1).strip()
            # Reject lines that look like sentences rather than names
            if len(candidate.split()) <= 4 and "." not in candidate:
                return candidate
    return None


def _extract_comments(soup: BeautifulSoup) -> list[dict]:
    """
    Attempt to find comment / conversation thread entries.
    Uses broad selector coverage and regex fallbacks for author/timestamp.
    Returns list of {author, timestamp, body}.
    """
    # Ordered from most-specific to least-specific
    comment_selectors = [
        "[data-comment-id]", "[data-test-id*='comment']",
        ".comment", ".ticket-comment", ".zd-comment", "article.comment",
        ".event-container .event", ".activity-item",
        ".message", ".note", ".post", ".reply",
        # Broad fallback: any article or section with meaningful text
        "article", "section.comment",
    ]

    author_selectors = [
        ".author", ".comment-author", ".user-name", ".display-name",
        "[class*='author']", "[class*='user']", "[class*='name']",
        ".requester", ".from", ".sender",
        "strong", "b",
        # Zendesk-specific
        ".zd-requester", ".ticket-author",
    ]

    timestamp_selectors = [
        "time", "[datetime]",
        "[data-timestamp]", "[data-time]",
        "[class*='time']", "[class*='date']", "[class*='timestamp']",
        "abbr[title]", "span[title]",
    ]

    body_selectors = [
        ".comment-body", ".body", ".content", ".description",
        ".zd-comment", ".rich-text", ".markdown",
        "p", "pre",
    ]

    for sel in comment_selectors:
        els = soup.select(sel)
        if not els:
            continue

        comments = []
        for el in els:
            author, ts, body = None, None, None

            # Author
            for a_sel in author_selectors:
                a = el.select_one(a_sel)
                if a:
                    txt = a.get_text(strip=True)
                    if txt and len(txt) < 80:   # sanity-check: names are short
                        author = txt
                        break

            # Timestamp — try attribute first, then text
            for t_sel in timestamp_selectors:
                t = el.select_one(t_sel)
                if t:
                    ts = (t.get("datetime") or t.get("data-timestamp")
                          or t.get("title") or t.get_text(strip=True))
                    if ts:
                        break
            # Regex fallback for timestamp embedded in element text
            if not ts:
                m = _DATE_RE.search(el.get_text(" ", strip=True))
                if m:
                    ts = m.group(0)

            # Body — prefer dedicated body element, fall back to whole element text
            for b_sel in body_selectors:
                b = el.select_one(b_sel)
                if b:
                    txt = b.get_text("\n", strip=True)
                    if txt:
                        body = txt
                        break
            if not body:
                body = el.get_text("\n", strip=True)

            if not body or not body.strip():
                continue

            body = body.strip()

            # Author fallback: try to guess from the body text itself
            if not author:
                author = _guess_author_from_text(body)

            comments.append({"author": author, "timestamp": ts, "body": body})

        if comments:
            return comments

    return []


def _extract_all_dl_fields(soup: BeautifulSoup) -> dict:
    """Harvest every dt/dd pair and every th/td row into a flat dict."""
    fields: dict = {}
    for dl in soup.find_all("dl"):
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                key = dt.get_text(strip=True).rstrip(":").strip()
                val = dd.get_text(" ", strip=True)
                if key and val:
                    fields[key] = val
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) == 2:
            key = cells[0].get_text(strip=True).rstrip(":")
            val = cells[1].get_text(" ", strip=True)
            if key and val and key not in fields:
                fields[key] = val
    return fields


def _extract_named_section(soup: BeautifulSoup, *heading_patterns: str) -> Optional[str]:
    """
    Find a section whose heading text matches any of the given patterns (case-insensitive),
    then return all text content inside that section container.
    Looks for headings (h1–h4) then walks to the next sibling container.
    """
    for pat in heading_patterns:
        regex = re.compile(pat, re.I)
        for tag in ["h1", "h2", "h3", "h4", "h5", "strong", "b", "th", "dt", "label"]:
            for el in soup.find_all(tag):
                if regex.search(el.get_text(strip=True)):
                    # Collect content from the parent container or next siblings
                    container = el.find_parent(["section", "div", "article", "td", "dd"])
                    if container:
                        # Exclude the heading itself
                        parts = []
                        for child in container.children:
                            if child == el:
                                continue
                            if hasattr(child, "get_text"):
                                t = child.get_text("\n", strip=True)
                                if t:
                                    parts.append(t)
                        text = "\n".join(parts).strip()
                        if text:
                            return text
                    # Fall back: grab all following siblings until next heading
                    parts = []
                    for sib in el.find_next_siblings():
                        if sib.name and sib.name in ["h1","h2","h3","h4","h5"]:
                            break
                        t = sib.get_text("\n", strip=True) if hasattr(sib, "get_text") else ""
                        if t:
                            parts.append(t)
                    text = "\n".join(parts).strip()
                    if text:
                        return text
    return None


def parse_ticket_detail(html: str, url: str) -> dict:
    """
    Parse a Supportal ticket detail page rendered by ticket.js (Vue SPA).

    Page structure (each section is a div.box with h3.box-title):
      - Ticket Information : strong→p label/value pairs
      - Ticket Fields      : table.table-striped with td/td rows
      - Ticket Tags        : plain text
      - Escalations        : ESC-NNNN links
      - Snapshots          : timestamp list
      - Ticket Timeline    : ul.timeline with comment li items
    """
    soup = BeautifulSoup(html, "html.parser")
    m = TICKET_HREF_RE.search(urllib.parse.urlparse(url).path)
    ticket_id = m.group(1) if m else url.rsplit("/", 1)[-1]

    # ── Subject ───────────────────────────────────────────────────────────────
    subject = None
    h1 = soup.select_one("section.content-header h1")
    if h1:
        subject = h1.get_text(" ", strip=True)
    if not subject:
        t = soup.find("title")
        if t:
            subject = re.sub(r"^Supportal\s*-\s*", "", t.get_text(strip=True))
    # Strip the "<Customer Name> ZD-NNNNN " prefix that Supportal prepends to h1
    if subject:
        subject = re.sub(r"^.*?\bZD-\d+\s+", "", subject, count=1).strip() or subject

    # ── Per-box extraction ────────────────────────────────────────────────────
    status = priority = created = assignee = requester = organization = None
    ticket_group = tags_text = escalations_text = snapshots_text = None
    ticket_fields: dict = {}
    comments: list[dict] = []

    for box in soup.select("div.box"):
        title_el = box.select_one("h3.box-title")
        if not title_el:
            continue
        box_title = title_el.get_text(strip=True).lower()
        body = box.select_one("div.box-body")
        if not body:
            continue

        # ── Ticket Information ────────────────────────────────────────────────
        if "ticket information" in box_title:
            # Fields are <strong>Label</strong> followed by <p>Value</p>
            # Walk direct children looking for strong→p pairs
            nodes = [n for n in body.children if getattr(n, "name", None) or str(n).strip()]
            i = 0
            while i < len(nodes):
                node = nodes[i]
                tag = getattr(node, "name", None)
                # Handle <span> wrapper around <strong>
                strong = None
                if tag == "strong":
                    strong = node
                elif tag == "span":
                    strong = node.find("strong")
                if strong:
                    label = strong.get_text(" ", strip=True).lower()
                    # Find the next <p>
                    for j in range(i + 1, min(i + 6, len(nodes))):
                        sib = nodes[j]
                        if getattr(sib, "name", None) == "p":
                            if "ticket status" in label or "status" in label:
                                spans = sib.select("span.ticket-status")
                                if len(spans) >= 2:
                                    priority = spans[0].get_text(strip=True)
                                    status   = spans[1].get_text(strip=True)
                                elif spans:
                                    status = spans[0].get_text(strip=True)
                            elif "date created" in label or "created" in label:
                                created = sib.get_text(strip=True)
                            elif "assignee" in label:
                                assignee = sib.get_text(" ", strip=True).strip()
                            elif "requester" in label:
                                org_a = sib.select_one("a[href*='/customer/']")
                                if org_a:
                                    organization = org_a.get_text(strip=True)
                                full = sib.get_text(" ", strip=True)
                                m2 = re.match(r"^(.+?)\s+at\s+", full)
                                requester = m2.group(1).strip() if m2 else full.split("\n")[0].strip()
                            elif "group" in label:
                                ticket_group = sib.get_text(strip=True)
                            break
                i += 1

        # ── Ticket Fields ─────────────────────────────────────────────────────
        elif "ticket fields" in box_title:
            for row in body.select("tr"):
                cells = row.select("td")
                if len(cells) >= 2:
                    k = cells[0].get_text(strip=True)
                    v = cells[1].get_text(strip=True)
                    if k:
                        ticket_fields[k] = v

        # ── Ticket Tags ───────────────────────────────────────────────────────
        elif "ticket tags" in box_title:
            # Tag spans each have a "Follow" button — strip those words out
            raw = body.get_text(" ", strip=True)
            tags_text = re.sub(r"\s*Follow\s*", " ", raw).strip() or None

        # ── Escalations ───────────────────────────────────────────────────────
        elif "escalation" in box_title:
            escs = [a.get_text(strip=True) for a in body.select("a")
                    if re.match(r"ESC-\d+", a.get_text(strip=True))]
            escalations_text = ", ".join(escs) if escs else None

        # ── Snapshots ─────────────────────────────────────────────────────────
        elif "snapshot" in box_title:
            lines = [t.strip() for t in body.get_text("\n").splitlines() if t.strip()
                     and not t.strip().lower().startswith("link") and t.strip() != "No snapshots"]
            snapshots_text = "\n".join(lines) if lines else None

        # ── Ticket Timeline ───────────────────────────────────────────────────
        elif "ticket timeline" in box_title:
            for li in body.select("ul.timeline li"):
                # Date label rows
                label_span = li.select_one("li.time-label span, span.bg-red, span.bg-green, span.bg-blue")
                # Comment rows have div.timeline-item
                item = li.select_one("div.timeline-item")
                if not item:
                    continue
                time_el = item.select_one("span.time")
                author_el = item.select_one("h3.timeline-header a")
                body_el = item.select_one("div.timeline-body")
                timestamp = time_el.get_text(strip=True) if time_el else None
                author    = author_el.get_text(strip=True) if author_el else None
                body_text = body_el.get_text("\n", strip=True) if body_el else None
                if author or body_text:
                    comments.append({
                        "timestamp": timestamp,
                        "author":    author,
                        "body":      body_text,
                    })

    # ── First comment as description ──────────────────────────────────────────
    description = comments[0]["body"] if comments else None

    return {
        "ticket_id":     ticket_id,
        "url":           url,
        "subject":       subject,
        "status":        status,
        "priority":      priority,
        "requester":     requester,
        "assignee":      assignee,
        "organization":  organization,
        "ticket_group":  ticket_group,
        "created":       created,
        "tags":          tags_text,
        "escalations":   escalations_text,
        "snapshots":     snapshots_text,
        "ticket_fields": json.dumps(ticket_fields) if ticket_fields else None,
        "description":   description,
        "comment_count": len(comments),
        "comments":      json.dumps(comments) if comments else None,
    }


# ─────────────────────────── Listing / navigation helpers ─────────────────────

_STATUS_MAP = {"O": "Open", "P": "Pending", "S": "Solved", "C": "Closed", "H": "Hold"}


def _extract_ticket_links(html: str) -> list[tuple[str, str]]:
    """Return unique (ticket_id, canonical_url) pairs found on a listing page."""
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        m = TICKET_HREF_RE.search(a["href"])
        if m:
            tid = m.group(1)
            canonical = f"{BASE_URL}/zendesk/ticket/{tid}"
            if canonical not in seen:
                out.append((tid, canonical))
                seen.add(canonical)
    return out


def _extract_ticket_rows(html: str) -> dict[str, dict]:
    """
    Extract summary row data from the ticket listing table.
    Returns a dict keyed by ticket_id with listing-level fields:
    status, priority, subject, created, solved.
    Handles the Supportal listing format:
      Ticket | Status | Priority | Subject | Created | Solved
      76866  |   O    |    P2    | ...     | date    | N/A
    """
    soup = BeautifulSoup(html, "html.parser")
    summaries: dict[str, dict] = {}

    # Find the main ticket table — look for a table with these header keywords
    target_table = None
    for table in soup.find_all("table"):
        header_text = table.get_text(" ", strip=True).lower()
        if "ticket" in header_text and ("status" in header_text or "priority" in header_text):
            target_table = table
            break

    # Also try a generic approach: any <tr> containing a zendesk ticket link
    if not target_table:
        # Find rows that have ticket links directly
        rows_with_links = [
            tr for tr in soup.find_all("tr")
            if any(TICKET_HREF_RE.search(a.get("href", "")) for a in tr.find_all("a", href=True))
        ]
    else:
        rows_with_links = target_table.find_all("tr")

    # Parse column headers to map positions
    header_map: dict[str, int] = {}
    for tr in (rows_with_links[:1] if not target_table else (target_table.find_all("tr")[:1])):
        cells = tr.find_all(["th", "td"])
        for i, cell in enumerate(cells):
            label = cell.get_text(strip=True).lower().rstrip(":")
            if label:
                header_map[label] = i

    # Fallback column positions if no headers found
    # Default: Ticket=0, Status=1, Priority=2, Subject=3, Created=4, Solved=5
    col = {
        "ticket":   header_map.get("ticket", 0),
        "status":   header_map.get("status", 1),
        "priority": header_map.get("priority", 2),
        "subject":  header_map.get("subject", 3),
        "created":  header_map.get("created", 4),
        "solved":   header_map.get("solved", 5),
    }

    for tr in rows_with_links:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        # Find ticket ID via link in the row
        ticket_link = None
        ticket_id = None
        for a in tr.find_all("a", href=True):
            m = TICKET_HREF_RE.search(a["href"])
            if m:
                ticket_id = m.group(1)
                ticket_link = a
                break
        if not ticket_id:
            continue

        def cell_text(idx: int) -> Optional[str]:
            if 0 <= idx < len(cells):
                t = cells[idx].get_text(strip=True)
                return t if t else None
            return None

        raw_status = cell_text(col["status"]) or ""
        status = _STATUS_MAP.get(raw_status.upper(), raw_status) or None
        solved_raw = cell_text(col["solved"])
        solved = None if solved_raw in ("N/A", "n/a", "-", "") else solved_raw

        summaries[ticket_id] = {
            "ticket_id": ticket_id,
            "url":       f"{BASE_URL}/zendesk/ticket/{ticket_id}",
            "status":    status,
            "priority":  cell_text(col["priority"]),
            "subject":   cell_text(col["subject"]),
            "created":   cell_text(col["created"]),
            "solved":    solved,
        }

    return summaries


def _resolve_customer_input(raw: str) -> tuple[str, str]:
    """
    Accept either a full Supportal customer URL or a plain name.
    Returns (customer_name, customer_url) with correct encoding.

    Examples:
      "https://supportal.couchbase.com/customer/American%20Express%20AZ"
        → ("American Express AZ", "https://.../customer/American%20Express%20AZ")
      "American Express AZ"
        → ("American Express AZ", "https://.../customer/American%20Express%20AZ")
    """
    raw = raw.strip().strip('"\'')
    # If it looks like a URL, extract the /customer/<name> path segment
    m = re.search(r"/customer/([^/?#]+)", raw, re.I)
    if m:
        # Decode percent-encoding to get the display name, then re-encode cleanly
        name = urllib.parse.unquote(m.group(1))
        url  = f"{BASE_URL}/customer/{urllib.parse.quote(name, safe='')}"
        return name, url
    # Plain name — encode directly
    name = urllib.parse.unquote(raw)   # handle if someone pastes partial %20 etc.
    url  = f"{BASE_URL}/customer/{urllib.parse.quote(name, safe='')}"
    return name, url


def _normalize_customer_url(href: str) -> str:
    """
    Normalize Supportal customer hrefs to a clean, navigable URL.

    The search page emits malformed absolute hrefs like:
        https://supportal.couchbase.com:/customer/american express az
    (extra colon after domain, unencoded spaces in path).

    This function returns a proper URL:
        https://supportal.couchbase.com/customer/American%20Express%20AZ
    """
    if not href:
        return ""
    # Strip the rogue colon that appears after the hostname
    # e.g. "https://supportal.couchbase.com:/customer/foo" → proper URL
    href = re.sub(r"(https?://[^/:]+):/", r"\1/", href)
    # Split into scheme+host and path
    if href.startswith("http"):
        # Reconstruct with properly encoded path
        m = re.match(r"(https?://[^/]+)(/.*)$", href)
        if m:
            scheme_host = m.group(1)
            path = m.group(2)
            # Encode any unencoded spaces/special chars in the path
            path = urllib.parse.quote(urllib.parse.unquote(path), safe="/-_.")
            return scheme_host + path
        return href
    # Relative path
    path = urllib.parse.quote(urllib.parse.unquote(href), safe="/-_.")
    return BASE_URL + path


def _find_customer_url_in_search(html: str, query: str) -> Optional[str]:
    """
    Parse Supportal search results to find the best Customer match.
    Search hrefs use lowercase names which the server may reject; we only use
    this function to CONFIRM a match exists, then return None so the caller
    builds the URL from the original query string (preserving capitalisation).
    Returns None always — caller uses the query-based canonical URL.
    """
    soup = BeautifulSoup(html, "html.parser")
    query_words = [w.lower() for w in query.split() if w]

    def _matches(text: str) -> bool:
        t = text.lower()
        return all(w in t for w in query_words)

    # Build list of (score, url) — higher score = better match
    candidates: list[tuple[int, str]] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/customer/" not in href.lower():
            continue
        url = _normalize_customer_url(href)

        # Walk up to the result container (up to 6 levels) to get all visible text
        container = a.parent
        for _ in range(6):
            if container is None:
                break
            text = container.get_text(" ", strip=True)
            # Must contain the query words somewhere in the result block
            if _matches(text):
                # Prefer alias matches (the alias label appears near the match)
                score = 2 if "alias" in text.lower() else 1
                candidates.append((score, url))
                break
            container = container.parent

    # Always return None — search hrefs are lowercase and case-sensitive server
    # will reject them. The caller uses the user-provided name to build the URL.
    return None


def _find_tickets_tab_url(html: str, current_url: str) -> Optional[str]:
    """
    Find the Tickets tab link on a customer page.
    Tries href-based detection first, then link-text matching.
    Returns the absolute URL or None.
    """
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        # Exact tab text or href segment
        if text == "tickets" or re.search(r"/tickets(?:\?|$|#)", href, re.I):
            return href if href.startswith("http") else urllib.parse.urljoin(current_url, href)
    return None


def _parse_all_page_urls(html: str, base_url: str) -> list[str]:
    """
    Parse the numbered page list at the bottom of the ticket listing.
    Returns a sorted list of all distinct page URLs found (including any
    not yet visited), derived from page-number links.
    Handles both ?page=N query-param style and path-based /page/N style.
    """
    soup = BeautifulSoup(html, "html.parser")
    page_map: dict[int, str] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)

        # Query-param style: ?page=N or &page=N
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            n = int(m.group(1))
            url = href if href.startswith("http") else urllib.parse.urljoin(base_url, href)
            page_map[n] = url
            continue

        # Path style: /page/N
        m = re.search(r"/page/(\d+)", href)
        if m:
            n = int(m.group(1))
            url = href if href.startswith("http") else urllib.parse.urljoin(base_url, href)
            page_map[n] = url
            continue

        # Link whose visible text is a plain number (e.g. "2", "3", ... "581")
        if text.isdigit():
            n = int(text)
            url = href if href.startswith("http") else urllib.parse.urljoin(base_url, href)
            page_map.setdefault(n, url)

    return [url for _, url in sorted(page_map.items())]


def _find_next_page(html: str, current_url: str) -> Optional[str]:
    """
    Fallback single-step next-page detector used when no full page list is found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # rel="next"
    a = soup.find("a", attrs={"rel": "next"})
    if a and a.get("href"):
        return urllib.parse.urljoin(current_url, a["href"])

    # Text-based
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").strip().lower()
        if txt in {"next", "›", "»", "→", "older", "more", "next »", "next ›"}:
            return urllib.parse.urljoin(current_url, a["href"])

    # Increment page= param
    parsed = urllib.parse.urlparse(current_url)
    qs = urllib.parse.parse_qs(parsed.query)
    if "page" in qs:
        try:
            page = int(qs["page"][0])
            candidate = re.sub(r"([?&]page=)\d+", rf"\g<1>{page + 1}", current_url)
            if candidate != current_url:
                return candidate
        except (ValueError, IndexError):
            pass

    return None


# ─────────────────────────── Scraper workers (blocking — run in thread) ───────

DEBUG_DIR = os.path.join(os.path.dirname(__file__), "debug_html")


def _save_debug(name: str, url: str, html: str) -> str:
    """Save HTML to debug_html/<name>.html and return the file path."""
    os.makedirs(DEBUG_DIR, exist_ok=True)
    safe = re.sub(r"[^\w\-]", "_", name)
    path = os.path.join(DEBUG_DIR, f"{safe}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"<!-- URL: {url} -->\n{html}")
    return path


def _scrape_listing(get_html: Callable[[str], str], customer: str, max_pages: int,
                    progress_cb: Callable[[str, float], None],
                    debug: bool = False) -> list[tuple[str, str]]:
    """
    Full navigation flow:
      1. Search Supportal for the customer name
      2. Find the correct Customer result (by alias / name match)
      3. Navigate to the customer page
      4. Find and follow the Tickets tab
      5. Paginate through the full numbered page list
      6. Collect every ticket link

    Set debug=True to save HTML at every step to ./debug_html/ for inspection.
    """
    all_tickets: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    visited_pages: set[str] = set()

    def log(msg: str, pct: float):
        print(f"[SCRAPE] {msg}")
        progress_cb(msg, pct)

    # ── Step 1: search ────────────────────────────────────────────────────────
    _cust_dec = urllib.parse.unquote(customer)
    search_url = f"{BASE_URL}/search/{urllib.parse.quote(_cust_dec)}"
    log(f"GET {search_url}", 0.01)
    search_html = get_html(search_url)
    if debug:
        p = _save_debug("01_search", search_url, search_html)
        log(f"Saved search HTML → {p}", 0.01)

    # Report what the page looks like
    soup_s = BeautifulSoup(search_html, "html.parser")
    page_title = (soup_s.find("title") or soup_s.find("h1") or "")
    page_title = page_title.get_text(strip=True) if hasattr(page_title, "get_text") else str(page_title)
    customer_links = [a["href"] for a in soup_s.find_all("a", href=True) if "/customer/" in a["href"]]
    log(f"Search page title: '{page_title}' | /customer/ links found: {len(customer_links)}", 0.015)
    if customer_links:
        log(f"  First few: {customer_links[:5]}", 0.015)

    # ── Step 2: find customer URL ─────────────────────────────────────────────
    customer_url = _find_customer_url_in_search(search_html, customer)
    if customer_url:
        log(f"Matched customer URL: {customer_url}", 0.02)
    else:
        slug = customer.lower().replace(" ", "-")
        customer_url = f"{BASE_URL}/customer/{urllib.parse.quote(slug, safe='')}"
        log(f"No search match — falling back to slug URL: {customer_url}", 0.02)

    # ── Step 3: load customer page ────────────────────────────────────────────
    log(f"GET customer page: {customer_url}", 0.03)
    customer_html = get_html(customer_url)
    if debug:
        p = _save_debug("02_customer", customer_url, customer_html)
        log(f"Saved customer HTML → {p}", 0.03)

    soup_c = BeautifulSoup(customer_html, "html.parser")
    ctitle = (soup_c.find("title") or soup_c.find("h1") or "")
    ctitle = ctitle.get_text(strip=True) if hasattr(ctitle, "get_text") else ""
    all_links = [(a.get_text(strip=True), a["href"]) for a in soup_c.find_all("a", href=True)]
    log(f"Customer page title: '{ctitle}' | total links: {len(all_links)}", 0.035)
    tab_like = [(t, h) for t, h in all_links if t.lower() in
                {"tickets", "entitlements", "snapshots", "clusters", "users", "analytics"}]
    log(f"  Tab-like links: {tab_like}", 0.035)

    # ── Step 4: find Tickets tab ──────────────────────────────────────────────
    tickets_url = _find_tickets_tab_url(customer_html, customer_url)
    if tickets_url:
        log(f"Tickets tab URL: {tickets_url}", 0.04)
    else:
        tickets_url = customer_url.rstrip("/") + "/tickets"
        log(f"Tickets tab not found — trying suffix: {tickets_url}", 0.04)

    # ── Step 5: load first tickets page ──────────────────────────────────────
    log(f"GET tickets page 1: {tickets_url}", 0.05)
    first_html = get_html(tickets_url)
    visited_pages.add(tickets_url)
    if debug:
        p = _save_debug("03_tickets_page1", tickets_url, first_html)
        log(f"Saved tickets page 1 HTML → {p}", 0.05)

    soup_t = BeautifulSoup(first_html, "html.parser")
    ttitle = (soup_t.find("title") or soup_t.find("h1") or "")
    ttitle = ttitle.get_text(strip=True) if hasattr(ttitle, "get_text") else ""
    ticket_links_found = _extract_ticket_links(first_html)
    all_page_hrefs = [(a.get_text(strip=True), a["href"])
                      for a in soup_t.find_all("a", href=True)
                      if re.search(r"[?&]page=\d+|/page/\d+", a["href"])]
    log(f"Tickets page title: '{ttitle}' | ticket links: {len(ticket_links_found)} | page links: {len(all_page_hrefs)}", 0.06)
    if all_page_hrefs:
        log(f"  Pagination sample: {all_page_hrefs[:6]}", 0.06)

    for item in ticket_links_found:
        if item[1] not in seen_urls:
            all_tickets.append(item)
            seen_urls.add(item[1])

    log(f"Page 1 collected: {len(all_tickets)} tickets", 0.07)

    # Parse full page list
    all_page_urls = _parse_all_page_urls(first_html, tickets_url)
    total_pages = len(all_page_urls) + 1

    if total_pages > 1:
        log(f"Pagination: {total_pages} total pages", 0.08)
    else:
        all_page_urls = []
        log("No numbered pagination found — will use next-page fallback", 0.08)

    # ── Step 6: iterate remaining pages ──────────────────────────────────────
    if all_page_urls:
        for i, page_url in enumerate(all_page_urls, start=2):
            if max_pages and i > max_pages:
                break
            if page_url in visited_pages:
                continue
            visited_pages.add(page_url)

            pct = 0.08 + 0.87 * (i / max(total_pages, 1))
            log(f"Listing page {i}/{total_pages}…", pct)
            html = get_html(page_url)

            for item in _extract_ticket_links(html):
                if item[1] not in seen_urls:
                    all_tickets.append(item)
                    seen_urls.add(item[1])

            log(f"Page {i}/{total_pages}: {len(all_tickets)} tickets total", pct)
            time.sleep(0.3)

    else:
        current_url = tickets_url
        current_html = first_html
        page_num = 1
        while True:
            if max_pages and page_num >= max_pages:
                break
            nxt = _find_next_page(current_html, current_url)
            if not nxt or nxt in visited_pages:
                break
            visited_pages.add(nxt)
            page_num += 1
            log(f"Next-page {page_num}: {nxt}", 0.08 + 0.02 * page_num)
            current_html = get_html(nxt)
            for item in _extract_ticket_links(current_html):
                if item[1] not in seen_urls:
                    all_tickets.append(item)
                    seen_urls.add(item[1])
            current_url = nxt
            time.sleep(0.3)

    return all_tickets


def diagnose(get_html: Callable[[str], str], customer: str,
             progress_cb: Callable[[str, float], None]) -> str:
    """
    Run just the discovery steps (no ticket detail scraping) with debug=True.
    Returns a text report summarising what was found at each step.
    Saves HTML files to ./debug_html/ for manual inspection.
    """
    report_lines: list[str] = []

    def log_cb(msg: str, pct: float):
        report_lines.append(msg)
        progress_cb(msg, pct)

    _scrape_listing(get_html, customer, max_pages=1, progress_cb=log_cb, debug=True)
    report_lines.append(f"\nDebug HTML saved to: {DEBUG_DIR}")
    return "\n".join(report_lines)


def scrape_with_cookie(
    cookie: str,
    customer: str,
    max_pages: int,
    progress_cb: Callable[[str, float], None],
) -> list[dict]:
    """Auth mode A: plain HTTP requests with a Cookie header."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Cookie": cookie,
    })

    def get_html(url: str) -> str:
        resp = session.get(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        return resp.text

    all_tickets = _scrape_listing(get_html, customer, max_pages, progress_cb)
    results: list[dict] = []

    for i, (tid, turl) in enumerate(all_tickets):
        pct = 0.1 + 0.9 * (i / max(len(all_tickets), 1))
        progress_cb(f"Ticket {tid}  ({i + 1}/{len(all_tickets)})", pct)
        try:
            html = get_html(turl)
            rec = parse_ticket_detail(html, turl)
        except Exception as exc:
            rec = {"ticket_id": tid, "url": turl, "error": str(exc)}
        results.append(rec)
        time.sleep(0.25)

    progress_cb("Done", 1.0)
    return results


def _scrape_listing_playwright(
    page,
    customer: str,
    max_pages: int,
    progress_cb: Callable[[str, float], None],
    debug: bool = False,
) -> list[dict]:
    """
    Playwright-native listing: navigates with proper wait_for_selector calls at
    each step so JS-rendered content is available before we parse.
    """
    # Strip any accidental surrounding quotes from the input
    customer = customer.strip().strip('"\'')

    all_tickets: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    visited_pages: set[str] = set()

    def log(msg: str, pct: float):
        print(f"[SCRAPE] {msg}")
        progress_cb(msg, pct)

    # ── Step 1: search ────────────────────────────────────────────────────────
    # The SPA makes continuous background XHR requests so "networkidle" never
    # fires.  Use "domcontentloaded" + explicit wait for Vue to render results.
    _customer_decoded = urllib.parse.unquote(customer)
    search_url = f"{BASE_URL}/search/{urllib.parse.quote(_customer_decoded)}"
    log(f"Search: {search_url}", 0.01)
    page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
    # Wait for Vue to inject search results (customer links appear after JS runs)
    try:
        page.wait_for_selector("a[href*='/customer/']", timeout=15_000)
    except PWTimeoutError:
        log("Timeout waiting for /customer/ links — waiting 4s for Vue render", 0.01)
        page.wait_for_timeout(4000)
    search_html = page.content()
    if debug:
        _save_debug("01_search", search_url, search_html)

    soup_s = BeautifulSoup(search_html, "html.parser")
    clinks = [a["href"] for a in soup_s.find_all("a", href=True) if "/customer/" in a["href"]]
    log(f"Search: {len(clinks)} /customer/ links found. First 5: {clinks[:5]}", 0.015)

    # ── Step 2: find and navigate to customer page ────────────────────────────
    customer_url = _find_customer_url_in_search(search_html, customer)

    if not customer_url and clinks:
        # Search hrefs use lowercase names (e.g. "american express az") which the
        # server may not accept. Confirm a match exists, then build the URL from
        # the user-supplied name (which preserves original capitalisation).
        try:
            loc = page.locator("a[href*='/customer/']").filter(
                has_text=re.compile(re.escape(_customer_decoded), re.I)
            )
            if loc.count() > 0:
                customer_url = f"{BASE_URL}/customer/{urllib.parse.quote(_customer_decoded, safe='')}"
                log(f"Search confirmed customer exists → using canonical URL: {customer_url}", 0.02)
        except Exception as exc:
            log(f"Locator match failed: {exc}", 0.02)

    if not customer_url:
        customer_url = f"{BASE_URL}/customer/{urllib.parse.quote(_customer_decoded, safe='')}"
        log(f"No search match — using direct URL: {customer_url}", 0.02)

    log(f"Navigating to customer page: {customer_url}", 0.03)
    page.goto(customer_url, wait_until="domcontentloaded", timeout=30_000)

    # Vue (customer.js) renders ALL content into the page after load.
    # Vue (customer.js) loads ticket data via XHR after domcontentloaded.
    # For large customers (1650 tickets) this can take >30s.
    # Wait for ul.pagination to appear — it renders when the data is ready.
    log("Waiting for pagination widget (Vue data load)…", 0.03)
    try:
        page.wait_for_selector("ul.pagination", timeout=90_000)
        log("Pagination widget visible — data loaded", 0.035)
    except PWTimeoutError:
        log("Timeout waiting for pagination — proceeding with whatever rendered", 0.035)

    if debug:
        _save_debug("02_customer", customer_url, page.content())

    # The customer URL base — used to detect when pagination navigates away
    customer_base = customer_url.split("?")[0].split("#")[0]

    def _collect_current_page() -> list[dict]:
        """Extract ticket summaries from the currently rendered page."""
        current = page.url.split("?")[0].split("#")[0]
        if customer_base and customer_base not in current and "/zendesk/ticket/" not in current:
            log(f"Context guard: URL drifted to {page.url} — skipping", 0.0)
            return []
        html = page.content()
        rows = _extract_ticket_rows(html)
        new = [r for r in rows.values() if r["url"] not in seen_urls]
        if not new:
            for tid, turl in _extract_ticket_links(html):
                if turl not in seen_urls:
                    new.append({"ticket_id": tid, "url": turl})
        return new

    # ── Collect page 1 ────────────────────────────────────────────────────────
    new_items = _collect_current_page()
    for item in new_items:
        all_tickets.append(item)
        seen_urls.add(item["url"])
    log(f"Page 1: {len(new_items)} tickets ({len(all_tickets)} total)", 0.06)

    if debug:
        _save_debug("03_tickets_page1", page.url, page.content())

    # ── Step 4: paginate ──────────────────────────────────────────────────────
    # Pagination: <ul class="pagination pagination-sm no-margin pull-right">
    #   <li><a>First</a></li> <li class="active"><a>1</a></li>
    #   <li class=""><a>2</a></li> … <li><a>Last</a></li>
    # NOTE: <a> tags have NO href — Vue uses click handlers.
    # Page change detected by waiting for li.active a text to equal new page num.

    def _parse_total_from_page() -> int | None:
        # "Showing 15 of 1650 matching items (out of total 1650)"
        m = re.search(r"Showing\s+\d+\s+of\s+(\d+)\s+matching", page.content(), re.I)
        return int(m.group(1)) if m else None

    def _tickets_per_page() -> int:
        rows = _extract_ticket_rows(page.content())
        return len(rows) if rows else 15

    import math
    total_tickets = _parse_total_from_page()
    per_page = _tickets_per_page() or 15
    if total_tickets:
        total_pages = math.ceil(total_tickets / per_page)
        log(f"Total: {total_tickets} tickets → {total_pages} pages ({per_page}/page)", 0.07)
    else:
        total_pages = 9999
        log("Could not parse total — paginating until no more buttons", 0.07)

    page_num = 1
    while page_num < total_pages:
        if max_pages and page_num >= max_pages:
            break

        next_page_num = page_num + 1
        pct = min(0.07 + 0.88 * (page_num / max(total_pages, 1)), 0.94)

        # Find the page button inside ul.pagination — <a> with exact text
        btn = page.locator("ul.pagination li a").filter(
            has_text=re.compile(rf"^\s*{next_page_num}\s*$")
        )
        if btn.count() == 0:
            log(f"No button for page {next_page_num} — done at page {page_num}", 0.95)
            break

        log(f"Clicking page {next_page_num}…", pct)
        try:
            btn.first.scroll_into_view_if_needed()
            btn.first.click()

            # Wait for ul.pagination li.active to show the new page number
            try:
                page.wait_for_function(
                    """(n) => {
                        const active = document.querySelector('ul.pagination li.active a');
                        return active && active.innerText.trim() === String(n);
                    }""",
                    arg=next_page_num,
                    timeout=15_000,
                )
            except Exception:
                page.wait_for_timeout(2000)

            new_items = _collect_current_page()
            for item in new_items:
                all_tickets.append(item)
                seen_urls.add(item["url"])
            log(f"Page {next_page_num}: +{len(new_items)} → {len(all_tickets)} total", pct)
            page_num = next_page_num
            time.sleep(0.3)
        except Exception as exc:
            log(f"Page {next_page_num} error: {exc}", pct)
            break

    return all_tickets


def scrape_with_playwright(
    customer: str,
    max_pages: int,
    progress_cb: Callable[[str, float], None],
) -> list[dict]:
    """Auth mode B: headless Playwright using the saved session profile."""
    os.makedirs(PROFILE_DIR, exist_ok=True)
    results: list[dict] = []

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=True,
            user_agent=UA,
            ignore_https_errors=True,
        )
        page = ctx.new_page()
        page.set_default_timeout(60_000)

        listing_summaries = _scrape_listing_playwright(page, customer, max_pages, progress_cb)
        progress_cb(f"Listing complete — {len(listing_summaries)} tickets found. Fetching details…", 0.15)

        for i, summary in enumerate(listing_summaries):
            tid  = summary.get("ticket_id", "")
            turl = summary.get("url", "")
            pct = 0.15 + 0.84 * (i / max(len(listing_summaries), 1))
            progress_cb(f"Detail {i + 1}/{len(listing_summaries)}  ticket #{tid}", pct)
            try:
                # "commit" fires on first bytes — more reliable than domcontentloaded
                # for pages that go through auth redirects before settling.
                page.goto(turl, wait_until="commit", timeout=60_000)
                # ticket.js (Vue) renders ticket fields into section.content via XHR.
                # Wait until that section has meaningful text before parsing.
                try:
                    page.wait_for_function(
                        """() => {
                            const sec = document.querySelector('section.content');
                            return sec && sec.innerText && sec.innerText.trim().length > 50;
                        }""",
                        timeout=60_000,
                    )
                except PWTimeoutError:
                    pass
                html = page.content()
                rec = parse_ticket_detail(html, turl)
                # Merge listing summary — fill gaps if detail parsing missed them
                for field in ("status", "priority", "subject", "created", "solved"):
                    if not rec.get(field) and summary.get(field):
                        rec[field] = summary[field]
            except PWTimeoutError:
                rec = {**summary, "error": "timeout"}
            except Exception as exc:
                rec = {**summary, "error": str(exc)}
            results.append(rec)
            time.sleep(0.15)

        ctx.close()

    progress_cb("Done", 1.0)
    return results


def open_browser_thread() -> None:
    """
    Launch a headful Chromium window so the user can complete interactive login.
    Returns immediately after navigating to BASE_URL — the browser stays open
    because _browser_state holds the references.
    """
    os.makedirs(PROFILE_DIR, exist_ok=True)
    pw = sync_playwright().start()
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=PROFILE_DIR,
        headless=False,
        viewport={"width": 1280, "height": 900},
        user_agent=UA,
        ignore_https_errors=True,
    )
    page = ctx.new_page()
    # Use domcontentloaded here — SSO chains may prevent networkidle from firing
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=120_000)
    _browser_state["pw"] = pw
    _browser_state["ctx"] = ctx
    # Return — browser window stays open; references in _browser_state keep it alive


def confirm_login_thread() -> None:
    """
    Close the headful browser, flushing all cookies to PROFILE_DIR.
    After this, scrape_with_playwright() can reuse the saved session headlessly.
    """
    ctx = _browser_state.get("ctx")
    pw = _browser_state.get("pw")
    try:
        if ctx:
            ctx.close()
    finally:
        if pw:
            pw.stop()
    _browser_state["ctx"] = None
    _browser_state["pw"] = None
    _browser_state["logged_in"] = True


# ─────────────────────────── Export helpers ───────────────────────────────────

_FLAT_FIELDS = [
    "ticket_id", "url", "subject", "status", "priority",
    "requester", "assignee", "organization",
    "created", "updated", "solved", "tags",
    "description", "ticket_information", "ticket_timeline",
    "escalations", "snapshots", "comment_count", "error",
]


def to_csv_bytes(data: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_FLAT_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in data:
        writer.writerow({k: row.get(k, "") for k in _FLAT_FIELDS})
    return buf.getvalue().encode()


def to_json_bytes(data: list[dict]) -> bytes:
    return json.dumps(data, ensure_ascii=False, indent=2).encode()


# ─────────────────────────── UI ───────────────────────────────────────────────

@ui.page("/")
def main_page():
    # Per-page state (NiceGUI re-creates this for each browser tab)
    state = {
        "results":      [],
        "auth_mode":    "cookie",   # "cookie" | "browser"
        "chat_history": [],         # list of {"role": "user"|"assistant", "content": str}
        "scores":       {},         # ticket_id -> score dict from Phase 3 LLM scoring
    }

    # ── Header ──────────────────────────────────────────────────────────────
    with ui.header().classes("bg-blue-900 text-white items-center px-6 py-3 shadow-md"):
        ui.label("Couchbase Supportal Scraper").classes("text-xl font-bold")

    with ui.column().classes("w-full max-w-5xl mx-auto px-4 pt-2 gap-0"):
        with ui.tabs().classes("w-full") as main_tabs:
            tab_auth    = ui.tab("Authentication", icon="lock")
            tab_scrape  = ui.tab("Scraping",       icon="search")
            tab_results = ui.tab("Results",        icon="table_view")
            tab_config  = ui.tab("Configuration",  icon="settings")
            tab_chat    = ui.tab("Chat",           icon="chat")
            tab_scoring = ui.tab("Scoring & Analysis", icon="analytics")
        with ui.tab_panels(main_tabs, value=tab_auth).classes("w-full pt-4"):

            with ui.tab_panel(tab_auth):
                with ui.column().classes("w-full gap-6"):
                    # ── Auth card ───────────────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        ui.label("Authentication").classes("text-base font-semibold mb-1")

                        with ui.tabs().classes("w-full") as auth_tabs:
                            tab_cookie  = ui.tab("Cookie / Header")
                            tab_browser = ui.tab("Browser Login (SSO)")

                        with ui.tab_panels(auth_tabs, value=tab_cookie).classes("w-full border-t"):

                            # ── Cookie tab ──────────────────────────────────────────────
                            with ui.tab_panel(tab_cookie):
                                ui.label(
                                    "Open DevTools → Network → any request → Headers → Cookie. "
                                    "Copy the full Cookie header value and paste it below. "
                                    "Alternatively set the SUPPORTAL_COOKIE environment variable before launching."
                                ).classes("text-sm text-gray-500 mb-2")
                                cookie_input = (
                                    ui.textarea(label="Cookie string", placeholder="session=abc123; other=xyz …")
                                    .classes("w-full font-mono text-sm")
                                    .props('rows=3 outlined clearable')
                                )
                                # Pre-fill from env if available
                                env_cookie = os.environ.get("SUPPORTAL_COOKIE", "")
                                if env_cookie:
                                    cookie_input.value = env_cookie
                                    ui.label("(pre-filled from SUPPORTAL_COOKIE env var)").classes("text-xs text-green-600")

                            # ── Browser login tab ───────────────────────────────────────
                            with ui.tab_panel(tab_browser):
                                ui.label(
                                    "Click Open Browser. A Chromium window will appear — log in through SSO "
                                    "as normal and navigate to at least one ticket to confirm access. "
                                    "Then click Confirm Login to save the session for headless scraping."
                                ).classes("text-sm text-gray-500 mb-3")

                                with ui.row().classes("items-center gap-2 mb-2"):
                                    browser_dot    = ui.icon("circle").classes("text-red-500 text-sm")
                                    browser_status = ui.label("Not logged in").classes("text-sm font-semibold text-gray-600")

                                async def do_open_browser():
                                    btn_open.props("loading disabled")
                                    browser_dot.classes(replace="text-orange-500 text-sm")
                                    browser_status.set_text("Browser opening…")
                                    try:
                                        await run.io_bound(open_browser_thread)
                                        browser_dot.classes(replace="text-blue-500 text-sm")
                                        browser_status.set_text("Browser open — log in then click Confirm Login")
                                        btn_confirm.props(remove="disabled")
                                    except Exception as exc:
                                        browser_dot.classes(replace="text-red-600 text-sm")
                                        browser_status.set_text(f"Error: {exc}")
                                        btn_open.props(remove="loading disabled")

                                async def do_confirm_login():
                                    btn_confirm.props("loading disabled")
                                    browser_status.set_text("Closing browser and saving session…")
                                    try:
                                        await run.io_bound(confirm_login_thread)
                                        browser_dot.classes(replace="text-green-500 text-sm")
                                        browser_status.set_text("Logged in ✓")
                                        ui.notify("Session saved. Ready to scrape.", type="positive")
                                    except Exception as exc:
                                        browser_dot.classes(replace="text-red-600 text-sm")
                                        browser_status.set_text(f"Error: {exc}")
                                        ui.notify(f"Error saving session: {exc}", type="negative")
                                        btn_confirm.props(remove="loading disabled")

                                with ui.row().classes("gap-3"):
                                    btn_open = ui.button("Open Browser & Login", on_click=do_open_browser, icon="open_in_browser")
                                    btn_confirm = ui.button("Confirm Login", on_click=do_confirm_login, icon="check_circle")
                                    btn_confirm.props("disabled color=positive")

            with ui.tab_panel(tab_scrape):
                with ui.column().classes("w-full gap-6"):
                    # ── Settings card ────────────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        ui.label("Scraper Settings").classes("text-base font-semibold mb-1")
                        with ui.row().classes("gap-4 w-full flex-wrap items-start"):
                            customer_input = (
                                ui.input(
                                    label="Customer URL or name",
                                    placeholder="https://supportal.couchbase.com/customer/American%20Express%20AZ",
                                )
                                .classes("flex-1 min-w-64")
                                .props("outlined clearable")
                            )
                            ui.label(
                                "Tip: navigate to the customer page in your browser and paste the full URL. "
                                "The name is extracted from the URL path to ensure correct capitalisation."
                            ).classes("text-xs text-gray-400 w-full -mt-2")
                            max_pages_input = (
                                ui.number(label="Max listing pages  (0 = all)", value=0, min=0, step=1)
                                .classes("w-56")
                                .props("outlined")
                            )

                    # ── Run card ─────────────────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        progress_bar   = ui.linear_progress(value=0).props("stripe color=blue-9 rounded")
                        progress_label = ui.label("").classes("text-sm text-gray-500 mt-1 min-h-5")

                        async def do_scrape():
                            if not customer_input.value.strip():
                                ui.notify("Enter a customer URL or name first.", type="warning")
                                return
                            customer, _resolved_url = _resolve_customer_input(customer_input.value)
                            progress_label.set_text(f"Resolved → {_resolved_url}")
                            if not customer:
                                ui.notify("Could not resolve a customer name from the input.", type="warning")
                                return
                            max_pages = int(max_pages_input.value or 0)

                            # Resolve auth
                            active_tab = auth_tabs.value          # NiceGUI tab label text
                            using_browser = (active_tab == tab_browser.name if hasattr(tab_browser, "name")
                                             else str(active_tab) == "Browser Login (SSO)")
                            cookie = (cookie_input.value or "").strip() or os.environ.get("SUPPORTAL_COOKIE", "")

                            if using_browser and not _browser_state.get("logged_in"):
                                ui.notify("Complete browser login first.", type="warning")
                                return
                            if not using_browser and not cookie:
                                ui.notify("Paste a cookie string or set SUPPORTAL_COOKIE env var.", type="warning")
                                return

                            loop = asyncio.get_event_loop()

                            def progress_cb(msg: str, pct: float):
                                async def _update():
                                    progress_bar.set_value(pct)
                                    progress_label.set_text(msg)
                                asyncio.run_coroutine_threadsafe(_update(), loop)

                            btn_scrape.props("loading disabled")
                            progress_bar.set_value(0)
                            progress_label.set_text("Starting…")

                            try:
                                if using_browser:
                                    data = await run.io_bound(
                                        scrape_with_playwright, customer, max_pages, progress_cb
                                    )
                                else:
                                    data = await run.io_bound(
                                        scrape_with_cookie, cookie, customer, max_pages, progress_cb
                                    )

                                state["results"] = data
                                _results.clear()
                                _results.extend(data)

                                _refresh_table(data)
                                btn_dl_json.set_enabled(True)
                                btn_dl_csv.set_enabled(True)
                                btn_cb_load.set_enabled(_CB_AVAILABLE)
                                btn_embed.set_enabled(_CB_AVAILABLE)
                                btn_score.set_enabled(True)
                                btn_render_charts.set_enabled(True)
                                progress_label.set_text(f"Done — {len(data)} tickets scraped.")
                                try:
                                    ui.notify(f"Done — {len(data)} tickets scraped.", type="positive")
                                except RuntimeError:
                                    pass  # client context gone after long scrape; element updates above are sufficient

                            except Exception as exc:
                                progress_label.set_text(f"Error: {exc}")
                                try:
                                    ui.notify(f"Scrape error: {exc}", type="negative", timeout=10000)
                                except RuntimeError:
                                    pass
                            finally:
                                btn_scrape.props(remove="loading disabled")

                        async def do_diagnose():
                            if not customer_input.value.strip():
                                ui.notify("Enter a customer URL or name first.", type="warning")
                                return
                            customer, _resolved_url = _resolve_customer_input(customer_input.value)
                            if not customer:
                                ui.notify("Could not resolve a customer name from the input.", type="warning")
                                return

                            if not _browser_state.get("logged_in"):
                                ui.notify(
                                    "Diagnose uses the browser session — complete Browser Login first.",
                                    type="warning",
                                )
                                return

                            loop = asyncio.get_event_loop()

                            def progress_cb(msg: str, pct: float):
                                async def _update():
                                    progress_bar.set_value(pct)
                                    progress_label.set_text(msg)
                                asyncio.run_coroutine_threadsafe(_update(), loop)

                            btn_diagnose.props("loading disabled")
                            progress_bar.set_value(0)
                            progress_label.set_text("Running diagnostics (Playwright)…")

                            def run_diag():
                                os.makedirs(PROFILE_DIR, exist_ok=True)
                                report_lines: list[str] = []

                                def log_cb(msg: str, pct: float):
                                    report_lines.append(msg)
                                    progress_cb(msg, pct)

                                with sync_playwright() as pw:
                                    ctx = pw.chromium.launch_persistent_context(
                                        user_data_dir=PROFILE_DIR,
                                        headless=True,
                                        user_agent=UA,
                                        ignore_https_errors=True,
                                    )
                                    pg = ctx.new_page()
                                    pg.set_default_timeout(60_000)
                                    _scrape_listing_playwright(pg, customer, max_pages=1,
                                                               progress_cb=log_cb, debug=True)
                                    ctx.close()

                                report_lines.append(f"\nDebug HTML saved to: {DEBUG_DIR}")
                                return "\n".join(report_lines)

                            try:
                                report = await run.io_bound(run_diag)
                                diag_output.set_text(report)
                                diag_output.set_visibility(True)
                                progress_label.set_text("Diagnostics complete — check output and debug_html/ folder")
                            except Exception as exc:
                                diag_output.set_text(str(exc))
                                diag_output.set_visibility(True)
                                progress_label.set_text(f"Diagnostics error: {exc}")
                            finally:
                                btn_diagnose.props(remove="loading disabled")

                        with ui.row().classes("gap-3 items-center"):
                            btn_scrape = ui.button("Scrape Tickets", on_click=do_scrape, icon="search").props(
                                "color=primary size=lg"
                            )
                            btn_diagnose = ui.button("Diagnose", on_click=do_diagnose, icon="bug_report").props(
                                "color=orange outline size=lg"
                            )
                            ui.label("(Diagnose runs only the navigation steps and saves HTML to debug_html/)").classes(
                                "text-xs text-gray-400"
                            )

                    # ── Diagnostics output ────────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        ui.label("Diagnostics Output").classes("text-base font-semibold mb-1")
                        diag_output = ui.label("").classes("font-mono text-xs whitespace-pre-wrap text-gray-700")
                        diag_output.set_visibility(False)

            with ui.tab_panel(tab_results):
                with ui.column().classes("w-full gap-6"):
                    # ── Results card ──────────────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        ui.label("Results").classes("text-base font-semibold mb-1")

                        columns = [
                            {"name": "ticket_id",    "label": "Ticket",       "field": "ticket_id",    "sortable": True, "align": "left"},
                            {"name": "status",       "label": "Status",       "field": "status",       "sortable": True},
                            {"name": "priority",     "label": "Priority",     "field": "priority",     "sortable": True},
                            {"name": "subject",      "label": "Subject",      "field": "subject",      "sortable": True, "align": "left"},
                            {"name": "requester",    "label": "Requester",    "field": "requester",    "sortable": True, "align": "left"},
                            {"name": "assignee",     "label": "Assignee",     "field": "assignee",     "sortable": True, "align": "left"},
                            {"name": "organization", "label": "Org",          "field": "organization", "sortable": True, "align": "left"},
                            {"name": "created",      "label": "Created",      "field": "created",      "sortable": True},
                            {"name": "updated",      "label": "Updated",      "field": "updated",      "sortable": True},
                            {"name": "solved",       "label": "Solved",       "field": "solved",       "sortable": True},
                            {"name": "comment_count","label": "Comments",     "field": "comment_count","sortable": True},
                            {"name": "tags",         "label": "Tags",         "field": "tags",         "sortable": True, "align": "left"},
                        ]

                        # Pagination state
                        _page_state = {"current": 1, "per_page": 25, "all_rows": []}

                        with ui.row().classes("items-center gap-4 mb-1"):
                            page_info_label = ui.label("").classes("text-xs text-gray-500 flex-1")
                            ui.label("Per page:").classes("text-xs text-gray-500")
                            per_page_select = ui.select(
                                options=[10, 25, 50, 100],
                                value=25,
                            ).classes("w-20 text-xs")

                        result_table = ui.table(columns=columns, rows=[], row_key="ticket_id").classes("w-full")
                        result_table.props("flat dense")

                        pager = ui.pagination(1, 1, direction_links=True).classes("mt-1 self-center")

                        # Row click → detail dialog
                        detail_dialog = ui.dialog().props("maximized")
                        with detail_dialog:
                            with ui.card().classes("w-full h-full overflow-auto p-6"):
                                dialog_title   = ui.label("").classes("text-lg font-bold mb-2")
                                dialog_content = ui.column().classes("w-full gap-2")

                        def _show_detail(ticket: dict):
                            dialog_title.set_text(f"Ticket #{ticket.get('ticket_id')} — {ticket.get('subject', '')}")
                            dialog_content.clear()
                            with dialog_content:
                                # Well-known fields
                                meta_fields = [
                                    ("Status",       ticket.get("status")),
                                    ("Priority",     ticket.get("priority")),
                                    ("Requester",    ticket.get("requester")),
                                    ("Assignee",     ticket.get("assignee")),
                                    ("Organization", ticket.get("organization")),
                                    ("Created",      ticket.get("created")),
                                    ("Updated",      ticket.get("updated")),
                                    ("Solved",       ticket.get("solved")),
                                    ("Tags",         ticket.get("tags")),
                                ]
                                with ui.grid(columns=2).classes("w-full gap-1 text-sm"):
                                    for label, val in meta_fields:
                                        if val:
                                            ui.label(label).classes("font-semibold text-gray-500")
                                            ui.label(val).classes("text-gray-800")

                                # Extra fields found on the page
                                all_fields = ticket.get("all_fields", {})
                                known_keys = {
                                    "status", "priority", "requester", "assignee",
                                    "organization", "created", "updated", "solved", "tags",
                                }
                                extra = {k: v for k, v in all_fields.items()
                                         if k.lower() not in known_keys and v}
                                if extra:
                                    ui.separator()
                                    ui.label("Additional fields").classes("font-semibold mt-1")
                                    with ui.grid(columns=2).classes("w-full gap-1 text-sm"):
                                        for k, v in extra.items():
                                            ui.label(k).classes("font-semibold text-gray-500")
                                            ui.label(str(v)).classes("text-gray-800")

                                # Named sections
                                def _section(label: str, content: Optional[str]):
                                    if not content:
                                        return
                                    ui.separator()
                                    ui.label(label).classes("font-semibold mt-1 text-blue-800")
                                    ui.label(content).classes("text-sm whitespace-pre-wrap text-gray-700 bg-gray-50 p-2 rounded")

                                _section("Description",  ticket.get("description"))
                                _section("Ticket Fields", ticket.get("ticket_fields"))
                                _section("Tags",          ticket.get("tags"))
                                _section("Escalations",   ticket.get("escalations"))
                                _section("Snapshots",     ticket.get("snapshots"))

                                # Comments / conversation (stored as JSON string or list)
                                comments = ticket.get("comments", [])
                                if isinstance(comments, str):
                                    try:
                                        comments = json.loads(comments)
                                    except Exception:
                                        comments = []
                                if comments:
                                    ui.separator()
                                    ui.label(f"Conversation  ({len(comments)} entries)").classes("font-semibold mt-1 text-blue-800")
                                    for c in comments:
                                        with ui.card().classes("w-full bg-gray-50"):
                                            with ui.row().classes("justify-between text-xs text-gray-400"):
                                                ui.label(c.get("author") or "—")
                                                ui.label(c.get("timestamp") or "")
                                            ui.label(c.get("body", "")).classes("text-sm whitespace-pre-wrap mt-1")

                                ui.button("Close", on_click=detail_dialog.close).classes("mt-4").props("flat color=grey")
                            detail_dialog.open()

                        def _render_page():
                            pp   = _page_state["per_page"]
                            pg   = _page_state["current"]
                            all_rows = _page_state["all_rows"]
                            total = len(all_rows)
                            start = (pg - 1) * pp
                            end   = start + pp
                            result_table.rows = all_rows[start:end]
                            result_table.update()
                            pages = max(1, -(-total // pp))  # ceiling division
                            pager.max = pages
                            if pager.value > pages:
                                pager.value = pages
                            showing_end = min(end, total)
                            page_info_label.set_text(
                                f"Showing {start + 1}–{showing_end} of {total} tickets" if total else "No tickets"
                            )

                        def _refresh_table(data: list[dict]):
                            _page_state["all_rows"] = [
                                {c["field"]: (rec.get(c["field"]) or "") for c in columns}
                                for rec in data
                            ]
                            _page_state["current"] = 1
                            pager.value = 1
                            _render_page()

                        def _on_page_change(e):
                            _page_state["current"] = int(pager.value)
                            _render_page()

                        def _on_per_page_change(e):
                            _page_state["per_page"] = int(per_page_select.value)
                            _page_state["current"] = 1
                            pager.value = 1
                            _render_page()

                        pager.on("update:model-value", _on_page_change)
                        per_page_select.on("update:model-value", _on_per_page_change)

                        def _on_row_click(e):
                            # NiceGUI 3.x / Quasar QTable fires row-click as (evt, row, index)
                            # so e.args is a list: [quasar_evt_dict, row_dict, row_index]
                            args = e.args
                            row_data = args[1] if isinstance(args, list) and len(args) > 1 else {}
                            if isinstance(row_data, dict):
                                ticket_id = str(row_data.get("ticket_id", ""))
                            else:
                                return
                            ticket = next(
                                (r for r in state["results"] if str(r.get("ticket_id")) == ticket_id), {}
                            )
                            _show_detail(ticket)

                        result_table.on("rowClick", _on_row_click)

                        # Downloads
                        with ui.row().classes("gap-3 mt-3"):
                            def dl_json():
                                if state["results"]:
                                    ui.download(src=to_json_bytes(state["results"]), filename="tickets.json")

                            def dl_csv():
                                if state["results"]:
                                    ui.download(src=to_csv_bytes(state["results"]), filename="tickets.csv")

                            btn_dl_json = ui.button("Download JSON", on_click=dl_json, icon="download").props("outline color=primary")
                            btn_dl_csv  = ui.button("Download CSV",  on_click=dl_csv,  icon="download").props("outline color=secondary")
                            btn_dl_json.set_enabled(False)
                            btn_dl_csv.set_enabled(False)

            with ui.tab_panel(tab_config):
                with ui.column().classes("w-full gap-6"):
                    # ── Phase 1: Load to Couchbase ───────────────────────────────────────
                    with ui.card().classes("w-full"):
                        with ui.row().classes("items-center justify-between w-full"):
                            ui.label("Load to Couchbase").classes("text-base font-semibold")
                            if not _CB_AVAILABLE:
                                ui.badge("SDK not installed", color="red").props("outline")

                        with ui.grid(columns=2).classes("w-full gap-x-4 gap-y-2 mt-2"):
                            cb_url_input      = ui.input("Cluster URL", placeholder="127.0.0.1").classes("w-full")
                            cb_bucket_input   = ui.input("Bucket",      placeholder="supportal").classes("w-full")
                            cb_user_input     = ui.input("Username",    placeholder="Administrator").classes("w-full")
                            cb_pass_input     = ui.input("Password").props("type=password").classes("w-full")
                            cb_scope_input      = ui.input("Scope",      placeholder="_default").classes("w-full")
                            cb_collection_input = ui.input("Collection", placeholder="tickets").classes("w-full")

                        with ui.row().classes("items-center gap-4 mt-2"):
                            cb_tls_toggle = ui.switch("TLS (couchbases://)", value=False)

                        cb_status = ui.label("").classes("text-sm text-gray-500 mt-1")
                        cb_progress = ui.linear_progress(value=0).classes("w-full mt-1")
                        cb_progress.set_visibility(False)

                        async def _do_cb_load():
                            if not state["results"]:
                                ui.notify("Run a scrape first — no tickets loaded.", type="warning")
                                return
                            if not _CB_AVAILABLE:
                                ui.notify("couchbase SDK not installed. Run: venv/bin/pip install couchbase", type="negative")
                                return

                            btn_cb_load.set_enabled(False)
                            cb_progress.set_visibility(True)
                            cb_progress.set_value(0)
                            cb_status.set_text("Starting…")

                            loop = asyncio.get_event_loop()

                            def _cb_progress(msg: str, pct: float):
                                asyncio.run_coroutine_threadsafe(
                                    _update_cb_progress(msg, pct), loop
                                )

                            async def _update_cb_progress(msg: str, pct: float):
                                cb_status.set_text(msg)
                                cb_progress.set_value(pct)

                            try:
                                upserted, errors = await run.io_bound(
                                    load_to_couchbase,
                                    state["results"],
                                    cb_url_input.value.strip(),
                                    cb_bucket_input.value.strip(),
                                    cb_user_input.value.strip(),
                                    cb_pass_input.value,
                                    cb_tls_toggle.value,
                                    cb_scope_input.value.strip() or "_default",
                                    cb_collection_input.value.strip() or "tickets",
                                    _cb_progress,
                                )
                                msg = f"Done — {upserted} upserted, {errors} errors."
                                cb_status.set_text(msg)
                                cb_progress.set_value(1.0)
                                ui.notify(msg, type="positive" if errors == 0 else "warning")
                            except Exception as exc:
                                cb_status.set_text(f"Error: {exc}")
                                ui.notify(str(exc), type="negative")
                            finally:
                                btn_cb_load.set_enabled(True)

                        btn_cb_load = ui.button(
                            "Load to Couchbase",
                            on_click=_do_cb_load,
                            icon="upload",
                        ).props("color=red-8").classes("mt-2")
                        btn_cb_load.set_enabled(False)

                        # ── Load FROM Couchbase (skip re-scrape for re-embedding) ─────────
                        ui.separator().classes("mt-4")
                        ui.label("Load tickets from Couchbase").classes("text-sm font-semibold text-gray-600 mt-1")
                        ui.label(
                            "Reload previously stored tickets into the session — useful for re-embedding "
                            "without re-scraping the site."
                        ).classes("text-xs text-gray-400")

                        with ui.row().classes("w-full gap-3 mt-2 items-center"):
                            cb_load_filter = ui.input(
                                "Filter by organization (optional)",
                                placeholder="e.g. Maccabi — leave blank for all",
                            ).classes("flex-1")
                            btn_load_from_cb = ui.button("Load from Couchbase", icon="download_for_offline").props("outline color=primary")

                        cb_load_status   = ui.label("").classes("text-sm text-gray-500 mt-1")
                        cb_load_progress = ui.linear_progress(value=0).classes("w-full mt-1")
                        cb_load_progress.set_visibility(False)

                        async def _do_load_from_cb():
                            btn_load_from_cb.set_enabled(False)
                            cb_load_progress.set_visibility(True)
                            cb_load_progress.set_value(0)
                            cb_load_status.set_text("Connecting …")
                            loop = asyncio.get_event_loop()

                            def _prog(msg: str, pct: float):
                                asyncio.run_coroutine_threadsafe(
                                    _upd_load(msg, pct), loop
                                )

                            async def _upd_load(msg: str, pct: float):
                                cb_load_status.set_text(msg)
                                cb_load_progress.set_value(pct)

                            try:
                                tickets = await run.io_bound(
                                    load_tickets_from_cb,
                                    cb_url_input.value.strip(),
                                    cb_bucket_input.value.strip(),
                                    cb_user_input.value.strip(),
                                    cb_pass_input.value,
                                    cb_tls_toggle.value,
                                    cb_scope_input.value.strip() or "_default",
                                    cb_collection_input.value.strip() or "tickets",
                                    cb_load_filter.value,
                                    _prog,
                                )
                                state["results"] = tickets
                                _refresh_table(tickets)
                                btn_embed.set_enabled(_CB_AVAILABLE)
                                btn_dl_json.set_enabled(True)
                                btn_dl_csv.set_enabled(True)
                                btn_score.set_enabled(True)
                                btn_render_charts.set_enabled(True)
                                msg = f"Loaded {len(tickets)} tickets from Couchbase."
                                cb_load_status.set_text(msg)
                                cb_load_progress.set_value(1.0)
                                ui.notify(msg, type="positive")
                            except Exception as exc:
                                cb_load_status.set_text(f"Error: {exc}")
                                ui.notify(str(exc), type="negative")
                            finally:
                                btn_load_from_cb.set_enabled(True)

                        btn_load_from_cb.on("click", _do_load_from_cb)

                        # Enable load button when results are available (mirrored alongside download buttons)
                        # Done by patching enable logic at the scrape-done site — see below.

                    # ── Phase 2: Embed Tickets ───────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        with ui.row().classes("items-center justify-between w-full"):
                            ui.label("Embed Tickets (Vector Search)").classes("text-base font-semibold")
                            ui.label("Run after 'Load to Couchbase'").classes("text-xs text-gray-400")

                        # Embedding provider tabs
                        with ui.tabs().classes("w-full mt-2") as emb_tabs:
                            emb_tab_ollama   = ui.tab("Ollama")
                            emb_tab_lmstudio = ui.tab("LMStudio")
                            emb_tab_gemini   = ui.tab("Gemini")
                            emb_tab_mlx      = ui.tab("MLX")

                        with ui.tab_panels(emb_tabs, value=emb_tab_ollama).classes("w-full border-t"):
                            with ui.tab_panel(emb_tab_ollama):
                                with ui.grid(columns=3).classes("w-full gap-x-4 gap-y-2"):
                                    emb_ollama_url_input   = ui.input("Ollama URL", placeholder="http://localhost:11434").classes("w-full")
                                    emb_ollama_model_input = ui.input("Embedding Model", placeholder="nomic-embed-text").classes("w-full")
                                    emb_dims_input         = ui.number("Vector Dims", value=768, min=64, max=8192).classes("w-full")
                            with ui.tab_panel(emb_tab_lmstudio):
                                with ui.grid(columns=3).classes("w-full gap-x-4 gap-y-2"):
                                    emb_lms_url_input   = ui.input("LMStudio URL", placeholder="http://localhost:1234").classes("w-full")
                                    emb_lms_model_input = ui.input("Embedding Model", placeholder="text-embedding-nomic-embed-text-v1.5").classes("w-full")
                                    emb_lms_dims_input  = ui.number("Vector Dims", value=768, min=64, max=8192).classes("w-full")
                            with ui.tab_panel(emb_tab_gemini):
                                with ui.grid(columns=3).classes("w-full gap-x-4 gap-y-2"):
                                    emb_gemini_key_input   = ui.input("API Key").props("type=password").classes("w-full")
                                    emb_gemini_model_input = ui.input("Embedding Model", value="text-embedding-004").classes("w-full")
                                    emb_gemini_dims_input  = ui.number("Vector Dims", value=768, min=64, max=8192).classes("w-full")
                            with ui.tab_panel(emb_tab_mlx):
                                ui.label(
                                    "Runs locally via mlx-embeddings — no server needed. "
                                    "Use any mlx-community embedding model from HuggingFace. "
                                    "Model is cached in memory after first load."
                                ).classes("text-xs text-gray-500 mb-2")
                                with ui.grid(columns=2).classes("w-full gap-x-4 gap-y-2"):
                                    emb_mlx_model_input = ui.input(
                                        "HuggingFace model ID",
                                        value="mlx-community/nomic-embed-text-v1.5",
                                    ).classes("w-full")
                                    emb_mlx_dims_input  = ui.number("Vector Dims", value=768, min=64, max=8192).classes("w-full")

                        def _get_embed_config() -> tuple[str, str, str, str, int]:
                            """Returns (provider, model, api_key, base_url, dims)."""
                            active = emb_tabs.value
                            if active == "LMStudio":
                                return (
                                    "lmstudio",
                                    emb_lms_model_input.value.strip(),
                                    "",
                                    emb_lms_url_input.value.strip() or "http://localhost:1234",
                                    int(emb_lms_dims_input.value or 768),
                                )
                            elif active == "Gemini":
                                return (
                                    "gemini",
                                    emb_gemini_model_input.value.strip() or "text-embedding-004",
                                    emb_gemini_key_input.value,
                                    "",
                                    int(emb_gemini_dims_input.value or 768),
                                )
                            elif active == "MLX":
                                return (
                                    "mlx",
                                    emb_mlx_model_input.value.strip() or "mlx-community/nomic-embed-text-v1.5",
                                    "",
                                    "",
                                    int(emb_mlx_dims_input.value or 768),
                                )
                            else:  # Ollama
                                return (
                                    "ollama",
                                    emb_ollama_model_input.value.strip(),
                                    "",
                                    emb_ollama_url_input.value.strip() or "http://localhost:11434",
                                    int(emb_dims_input.value or 768),
                                )

                        emb_status   = ui.label("").classes("text-sm text-gray-500 mt-1")
                        emb_progress = ui.linear_progress(value=0).classes("w-full mt-1")
                        emb_progress.set_visibility(False)

                        async def _do_embed():
                            if not state["results"]:
                                ui.notify("Scrape tickets first.", type="warning")
                                return
                            btn_embed.set_enabled(False)
                            btn_create_idx.set_enabled(False)
                            emb_progress.set_visibility(True)
                            emb_progress.set_value(0)
                            emb_status.set_text("Starting …")
                            loop = asyncio.get_event_loop()

                            def _prog(msg: str, pct: float):
                                asyncio.run_coroutine_threadsafe(
                                    _upd_emb(msg, pct), loop
                                )

                            async def _upd_emb(msg: str, pct: float):
                                emb_status.set_text(msg)
                                emb_progress.set_value(pct)

                            try:
                                emb_provider, emb_model, emb_api_key, emb_base_url, emb_dims = _get_embed_config()
                                done, errs = await run.io_bound(
                                    embed_all_tickets,
                                    state["results"],
                                    cb_url_input.value.strip(),
                                    cb_bucket_input.value.strip(),
                                    cb_user_input.value.strip(),
                                    cb_pass_input.value,
                                    cb_tls_toggle.value,
                                    cb_scope_input.value.strip() or "_default",
                                    cb_collection_input.value.strip() or "tickets",
                                    emb_provider,
                                    emb_model,
                                    emb_api_key,
                                    emb_base_url,
                                    emb_dims,
                                    _prog,
                                )
                                msg = f"Done — {done} embedded, {errs} errors."
                                emb_status.set_text(msg)
                                emb_progress.set_value(1.0)
                                ui.notify(msg, type="positive" if errs == 0 else "warning")
                                btn_create_idx.set_enabled(True)
                            except Exception as exc:
                                emb_status.set_text(f"Error: {exc}")
                                ui.notify(str(exc), type="negative")
                            finally:
                                btn_embed.set_enabled(True)

                        async def _do_create_index():
                            btn_create_idx.set_enabled(False)
                            emb_status.set_text("Creating vector index …")
                            try:
                                await run.io_bound(
                                    create_vector_index,
                                    cb_url_input.value.strip(),
                                    cb_bucket_input.value.strip(),
                                    cb_user_input.value.strip(),
                                    cb_pass_input.value,
                                    cb_tls_toggle.value,
                                    cb_scope_input.value.strip() or "_default",
                                    cb_collection_input.value.strip() or "tickets",
                                    _get_embed_config()[4],
                                )
                                emb_status.set_text("Vector index created — ready for chat.")
                                ui.notify("Vector index created!", type="positive")
                            except Exception as exc:
                                emb_status.set_text(f"Index error: {exc}")
                                ui.notify(str(exc), type="negative")
                            finally:
                                btn_create_idx.set_enabled(True)

                        with ui.row().classes("gap-3 mt-2"):
                            btn_embed      = ui.button("Embed Tickets",       on_click=_do_embed,        icon="model_training").props("outline color=primary")
                            btn_create_idx = ui.button("Create Vector Index", on_click=_do_create_index, icon="manage_search").props("outline color=secondary")
                        btn_embed.set_enabled(False)
                        btn_create_idx.set_enabled(False)

            with ui.tab_panel(tab_chat):
                with ui.column().classes("w-full gap-6"):
                    # ── Phase 2: Chat / RAG ──────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        ui.label("Chat with Tickets (RAG)").classes("text-base font-semibold")

                        # LLM provider tabs
                        with ui.tabs().classes("w-full mt-2") as llm_tabs:
                            tab_claude   = ui.tab("Claude")
                            tab_gemini   = ui.tab("Gemini")
                            tab_ollama   = ui.tab("Ollama")
                            tab_lmstudio = ui.tab("LMStudio")

                        with ui.tab_panels(llm_tabs, value=tab_claude).classes("w-full border-t"):
                            # Claude
                            with ui.tab_panel(tab_claude):
                                with ui.grid(columns=2).classes("w-full gap-4 mt-2"):
                                    claude_key_input   = ui.input("API Key").props("type=password").classes("w-full")
                                    claude_model_input = ui.input("Model", value="claude-sonnet-4-6").classes("w-full")

                            # Gemini
                            with ui.tab_panel(tab_gemini):
                                with ui.grid(columns=2).classes("w-full gap-4 mt-2"):
                                    gemini_key_input   = ui.input("API Key").props("type=password").classes("w-full")
                                    gemini_model_input = ui.input("Model", value="gemini-2.0-flash").classes("w-full")

                            # Ollama chat
                            with ui.tab_panel(tab_ollama):
                                with ui.grid(columns=2).classes("w-full gap-4 mt-2"):
                                    ollama_chat_url_input   = ui.input("Ollama URL", placeholder="http://localhost:11434").classes("w-full")
                                    ollama_chat_model_input = ui.input("Model",      placeholder="llama3.3").classes("w-full")

                            # LMStudio
                            with ui.tab_panel(tab_lmstudio):
                                with ui.grid(columns=2).classes("w-full gap-4 mt-2"):
                                    lms_url_input   = ui.input("LMStudio URL", placeholder="http://localhost:1234").classes("w-full")
                                    lms_model_input = ui.input("Model",        placeholder="local-model").classes("w-full")

                        top_k_input = ui.number("Results to retrieve (top-K)", value=10, min=1, max=50).classes("w-48 mt-2")

                        # Conversation display
                        ui.separator().classes("mt-3")
                        chat_log = ui.column().classes("w-full gap-2 mt-2 max-h-96 overflow-y-auto")
                        chat_status = ui.label("").classes("text-xs text-gray-400 mt-1")

                        def _render_chat():
                            chat_log.clear()
                            with chat_log:
                                for msg in state["chat_history"]:
                                    if msg["role"] == "user":
                                        with ui.row().classes("justify-end w-full"):
                                            ui.label(msg["content"]).classes(
                                                "bg-blue-600 text-white rounded-xl px-4 py-2 max-w-xl text-sm whitespace-pre-wrap"
                                            )
                                    elif msg["role"] == "assistant":
                                        with ui.row().classes("justify-start w-full"):
                                            ui.label(msg["content"]).classes(
                                                "bg-gray-100 text-gray-900 rounded-xl px-4 py-2 max-w-xl text-sm whitespace-pre-wrap"
                                            )

                        def _get_llm_config() -> tuple[str, str, str, str]:
                            """Return (provider, model, api_key, base_url) from active tab."""
                            active = llm_tabs.value
                            if active == "Claude":
                                return "claude", claude_model_input.value.strip(), claude_key_input.value, ""
                            elif active == "Gemini":
                                return "gemini", gemini_model_input.value.strip(), gemini_key_input.value, ""
                            elif active == "Ollama":
                                return "ollama", ollama_chat_model_input.value.strip(), "", ollama_chat_url_input.value.strip() or "http://localhost:11434"
                            else:  # LMStudio
                                return "lmstudio", lms_model_input.value.strip(), "", lms_url_input.value.strip() or "http://localhost:1234"

                        async def _send_chat():
                            question = chat_input.value.strip()
                            if not question:
                                return
                            if not state["results"]:
                                ui.notify("No tickets loaded — run a scrape first.", type="warning")
                                return

                            chat_input.value = ""
                            state["chat_history"].append({"role": "user", "content": question})
                            _render_chat()
                            chat_status.set_text("Retrieving relevant tickets …")
                            btn_send.set_enabled(False)

                            provider, model, api_key, base_url = _get_llm_config()
                            loop = asyncio.get_event_loop()

                            try:
                                # 1. Embed the question
                                _ep, _em, _eak, _ebu, _ = _get_embed_config()
                                query_vec = await run.io_bound(
                                    embed_text,
                                    question,
                                    _ep,
                                    _em,
                                    _eak,
                                    _ebu,
                                )

                                # 2. Vector search
                                chat_status.set_text("Searching Couchbase …")
                                doc_keys = await run.io_bound(
                                    vector_search_cb,
                                    query_vec,
                                    cb_url_input.value.strip(),
                                    cb_bucket_input.value.strip(),
                                    cb_user_input.value.strip(),
                                    cb_pass_input.value,
                                    cb_tls_toggle.value,
                                    cb_scope_input.value.strip() or "_default",
                                    cb_collection_input.value.strip() or "tickets",
                                    int(top_k_input.value or 10),
                                )

                                # 3. Look up full ticket dicts from scraped results
                                id_set = {k.split("::")[-1] for k in doc_keys}
                                context_tickets = [
                                    t for t in state["results"]
                                    if str(t.get("ticket_id")) in id_set
                                ][:int(top_k_input.value or 10)]

                                # 4. Build messages with RAG context in system prompt
                                context_block = build_rag_context(context_tickets)
                                system_msg    = SYSTEM_PROMPT_TEMPLATE.format(context=context_block)
                                messages      = [{"role": "system", "content": system_msg}]
                                # Include prior conversation (skip previous system messages)
                                for h in state["chat_history"][:-1]:
                                    if h["role"] in ("user", "assistant"):
                                        messages.append(h)
                                messages.append({"role": "user", "content": question})

                                # 5. Call LLM
                                chat_status.set_text(f"Asking {provider} ({model}) …")
                                answer = await run.io_bound(call_llm, messages, provider, model, api_key, base_url)

                                state["chat_history"].append({"role": "assistant", "content": answer})
                                _render_chat()
                                chat_status.set_text(f"{len(context_tickets)} tickets used as context.")

                            except Exception as exc:
                                chat_status.set_text(f"Error: {exc}")
                                ui.notify(str(exc), type="negative")
                                # Remove the user message if we failed completely
                                if state["chat_history"] and state["chat_history"][-1]["role"] == "user":
                                    state["chat_history"].pop()
                                _render_chat()
                            finally:
                                btn_send.set_enabled(True)

                        def _clear_chat():
                            state["chat_history"].clear()
                            _render_chat()
                            chat_status.set_text("")

                        with ui.row().classes("w-full gap-2 mt-3 items-center"):
                            chat_input = ui.input(placeholder="Ask a question about the tickets …").classes("flex-1")
                            chat_input.on("keydown.enter", _send_chat)
                            btn_send  = ui.button("Send",  on_click=_send_chat,  icon="send").props("color=primary")
                            btn_clear = ui.button("Clear", on_click=_clear_chat, icon="delete").props("outline color=grey")

            with ui.tab_panel(tab_scoring):
                with ui.column().classes("w-full gap-6"):
                    # ── Phase 3: Score Tickets ───────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        with ui.row().classes("items-center justify-between w-full"):
                            ui.label("Score Tickets (Sentiment & Complexity)").classes("text-base font-semibold")
                            ui.label("Uses the LLM provider configured above").classes("text-xs text-gray-400")

                        ui.label(
                            "Scores each ticket for stars (1-5), temperature (cold/warm/hot), "
                            "resolution quality, timeliness, communication clarity, and complexity "
                            "using few-shot prompting."
                        ).classes("text-xs text-gray-500 mt-1")

                        with ui.row().classes("items-center gap-4 mt-2"):
                            score_batch_input = ui.number("Tickets per batch", value=20, min=5, max=50).classes("w-40")

                        score_status   = ui.label("").classes("text-sm text-gray-500 mt-1")
                        score_progress = ui.linear_progress(value=0).classes("w-full mt-1")
                        score_progress.set_visibility(False)

                        async def _do_score():
                            if not state["results"]:
                                ui.notify("No tickets loaded.", type="warning")
                                return
                            btn_score.set_enabled(False)
                            score_progress.set_visibility(True)
                            score_progress.set_value(0)
                            score_status.set_text("Starting …")
                            loop = asyncio.get_event_loop()

                            def _prog(msg: str, pct: float):
                                asyncio.run_coroutine_threadsafe(_upd_score(msg, pct), loop)

                            async def _upd_score(msg: str, pct: float):
                                score_status.set_text(msg)
                                score_progress.set_value(pct)

                            provider, model, api_key, base_url = _get_llm_config()
                            try:
                                scores = await run.io_bound(
                                    score_all_tickets,
                                    state["results"],
                                    provider,
                                    model,
                                    api_key,
                                    base_url,
                                    int(score_batch_input.value or 20),
                                    _prog,
                                )
                                state["scores"] = scores
                                msg = f"Scored {len(scores)}/{len(state['results'])} tickets."
                                score_status.set_text(msg)
                                score_progress.set_value(1.0)
                                ui.notify(msg, type="positive")
                                btn_render_charts.set_enabled(True)
                            except Exception as exc:
                                score_status.set_text(f"Error: {exc}")
                                ui.notify(str(exc), type="negative")
                            finally:
                                btn_score.set_enabled(True)

                        btn_score = ui.button(
                            "Score Tickets", on_click=_do_score, icon="psychology"
                        ).props("color=deep-purple").classes("mt-2")
                        btn_score.set_enabled(False)

                    # ── Phase 3: Analytics ───────────────────────────────────────────────
                    with ui.card().classes("w-full"):
                        with ui.row().classes("items-center justify-between w-full"):
                            ui.label("Analytics").classes("text-base font-semibold")
                            ui.label("Score tickets first to unlock sentiment & complexity charts").classes("text-xs text-gray-400")

                        chart_status = ui.label("").classes("text-sm text-gray-500 mt-1")
                        charts_area  = ui.column().classes("w-full gap-4 mt-3")

                        def _make_chart(container, cfg: dict):
                            with container:
                                ui.chart(cfg).classes("w-full")

                        async def _render_charts():
                            if not state["results"]:
                                ui.notify("No tickets loaded.", type="warning")
                                return
                            btn_render_charts.set_enabled(False)
                            chart_status.set_text("Building charts …")
                            charts_area.clear()

                            data = build_analytics_data(state["results"], state["scores"])

                            with charts_area:
                                # ── Row 1: Frequency over time ────────────────────────────
                                if data["month_keys"]:
                                    ui.chart({
                                        "chart":  {"type": "bar", "height": 280},
                                        "title":  {"text": "Ticket Volume Over Time"},
                                        "series": [{"name": "Tickets", "data": data["month_values"]}],
                                        "xaxis":  {"categories": data["month_keys"], "labels": {"rotate": -45}},
                                        "colors": ["#1565C0"],
                                    }).classes("w-full")
                                else:
                                    ui.label("No parseable dates for frequency chart.").classes("text-sm text-gray-400")

                                # ── Row 2: Priority + Status side by side ─────────────────
                                with ui.row().classes("w-full gap-4"):
                                    with ui.card().classes("flex-1"):
                                        ui.chart({
                                            "chart":  {"type": "pie", "height": 280},
                                            "title":  {"text": "Priority Distribution"},
                                            "labels": data["priority_labels"],
                                            "series": data["priority_values"],
                                            "colors": ["#43A047","#FB8C00","#E53935","#8E24AA"],
                                        }).classes("w-full")
                                    with ui.card().classes("flex-1"):
                                        ui.chart({
                                            "chart":  {"type": "donut", "height": 280},
                                            "title":  {"text": "Status Breakdown"},
                                            "labels": data["status_labels"],
                                            "series": data["status_values"],
                                        }).classes("w-full")

                                # ── Row 3: Comment distribution + Escalation rate ─────────
                                with ui.row().classes("w-full gap-4"):
                                    with ui.card().classes("flex-1"):
                                        ui.chart({
                                            "chart":  {"type": "bar", "height": 260},
                                            "title":  {"text": "Comment Count Distribution"},
                                            "series": [{"name": "Tickets", "data": data["comment_values"]}],
                                            "xaxis":  {"categories": data["comment_labels"]},
                                            "colors": ["#00ACC1"],
                                        }).classes("w-full")
                                    with ui.card().classes("flex-1"):
                                        ui.chart({
                                            "chart":  {"type": "donut", "height": 260},
                                            "title":  {"text": "Escalation Rate"},
                                            "labels": data["esc_labels"],
                                            "series": data["esc_values"],
                                            "colors": ["#E53935","#43A047"],
                                        }).classes("w-full")

                                # ── Scored metrics (only if scores available) ─────────────
                                if state["scores"]:
                                    ui.label("— Scored Metrics —").classes("text-sm font-semibold text-gray-500 text-center w-full")

                                    # Row 4: Stars + Temperature
                                    with ui.row().classes("w-full gap-4"):
                                        with ui.card().classes("flex-1"):
                                            ui.chart({
                                                "chart":  {"type": "bar", "height": 260},
                                                "title":  {"text": "Experience Stars Distribution"},
                                                "series": [{"name": "Tickets", "data": data["stars_values"]}],
                                                "xaxis":  {"categories": ["★1","★2","★3","★4","★5"]},
                                                "colors": ["#FDD835"],
                                            }).classes("w-full")
                                        with ui.card().classes("flex-1"):
                                            ui.chart({
                                                "chart":  {"type": "donut", "height": 260},
                                                "title":  {"text": "Temperature Distribution"},
                                                "labels": data["temp_labels"],
                                                "series": data["temp_values"],
                                                "colors": ["#42A5F5","#FFA726","#EF5350"],
                                            }).classes("w-full")

                                    # Row 5: Complexity + Dimension averages
                                    with ui.row().classes("w-full gap-4"):
                                        with ui.card().classes("flex-1"):
                                            ui.chart({
                                                "chart":  {"type": "bar", "height": 260},
                                                "title":  {"text": "Complexity Score Distribution"},
                                                "series": [{"name": "Tickets", "data": data["complexity_values"]}],
                                                "xaxis":  {"categories": ["1","2","3","4","5"]},
                                                "colors": ["#8E24AA"],
                                            }).classes("w-full")
                                        with ui.card().classes("flex-1"):
                                            ui.chart({
                                                "chart":  {"type": "bar", "height": 260, "horizontal": True},
                                                "title":  {"text": "Avg Dimension Scores (1-5)"},
                                                "series": [{"name": "Avg Score", "data": data["dim_avg"]}],
                                                "xaxis":  {"categories": data["dim_categories"]},
                                                "yaxis":  {"max": 5},
                                                "colors": ["#26A69A"],
                                            }).classes("w-full")

                            chart_status.set_text(
                                f"{len(state['results'])} tickets · "
                                f"{len(state['scores'])} scored · charts updated."
                            )
                            btn_render_charts.set_enabled(True)

                        btn_render_charts = ui.button(
                            "Generate Charts", on_click=_render_charts, icon="bar_chart"
                        ).props("color=teal").classes("mt-2")
                        btn_render_charts.set_enabled(False)


# ─────────────────────────── Couchbase connection helper ─────────────────────

def _cb_conn_str(cb_url: str, use_tls: bool) -> str:
    """
    Build a couchbase[s]:// connection string from whatever the user typed.
    Strips any existing scheme so we never produce couchbase://couchbase://...
    """
    # Remove any existing scheme the user may have included
    host = re.sub(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", "", cb_url).strip().rstrip("/")
    scheme = "couchbases" if use_tls else "couchbase"
    return f"{scheme}://{host}"


# ─────────────────────────── Load tickets from Couchbase ─────────────────────

def load_tickets_from_cb(
    cb_url: str,
    bucket: str,
    username: str,
    password: str,
    use_tls: bool,
    scope: str,
    collection: str,
    customer_filter: str,
    progress_cb: Callable[[str, float], None],
) -> list[dict]:
    """
    Query tickets from Couchbase via SQL++ and return them as a list of dicts,
    ready to populate state["results"].  Optionally filter by organization field.
    """
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed")

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb(f"Connecting to {conn_str} …", 0.0)
    cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))

    keyspace = f"`{bucket}`.`{scope}`.`{collection}`"
    if customer_filter.strip():
        query  = f"SELECT t.* FROM {keyspace} AS t WHERE LOWER(t.organization) LIKE $1"
        opts   = QueryOptions(positional_parameters=[f"%{customer_filter.strip().lower()}%"])
    else:
        query  = f"SELECT t.* FROM {keyspace} AS t"
        opts   = QueryOptions()

    progress_cb("Running query …", 0.1)
    result  = cluster.query(query, opts)
    tickets = [row for row in result.rows()]
    cluster.close()
    progress_cb(f"Loaded {len(tickets)} tickets.", 1.0)
    return tickets


# ─────────────────────────── Phase 1: Couchbase loader ───────────────────────

def load_to_couchbase(
    tickets: list[dict],
    cb_url: str,
    bucket: str,
    username: str,
    password: str,
    use_tls: bool,
    scope: str,
    collection: str,
    progress_cb: Callable[[str, float], None],
) -> tuple[int, int]:
    """
    Upsert each ticket into Couchbase using the Python SDK.

    Returns (upserted_count, error_count).
    """
    if not _CB_AVAILABLE:
        raise RuntimeError(
            "couchbase SDK not installed — run: venv/bin/pip install couchbase"
        )

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb(f"Connecting to {conn_str} …", 0.0)
    auth = PasswordAuthenticator(username, password)
    opts = ClusterOptions(auth)
    cluster = Cluster(conn_str, opts)
    cluster.wait_until_ready(timedelta(seconds=15))

    progress_cb(f"Connected — opening {bucket}.{scope}.{collection} …", 0.02)
    bkt = cluster.bucket(bucket)
    col = bkt.scope(scope).collection(collection)

    total = len(tickets)
    upserted = 0
    errors = 0

    for i, ticket in enumerate(tickets, start=1):
        tid = ticket.get("ticket_id") or f"unknown_{i}"
        doc_key = f"ticket::{tid}"
        try:
            col.upsert(doc_key, ticket)
            upserted += 1
        except CouchbaseException as exc:
            errors += 1
            progress_cb(f"Error on {doc_key}: {exc}", i / total)
            continue

        if i % 25 == 0 or i == total:
            pct = i / total
            progress_cb(f"Upserted {i}/{total} …", pct)

    cluster.close()
    return upserted, errors


# ─────────────────────────── Phase 2: Embedding & RAG ────────────────────────

def build_embed_text(ticket: dict) -> str:
    """
    Concatenate subject, date, description and all conversation entries
    into a single text blob for embedding.  Capped at ~8 000 chars so
    embedding models with shorter prompt limits don't choke.
    """
    parts: list[str] = []
    if ticket.get("subject"):
        parts.append(f"Subject: {ticket['subject']}")
    if ticket.get("created"):
        parts.append(f"Date: {ticket['created']}")
    if ticket.get("description"):
        parts.append(f"Description:\n{ticket['description']}")
    comments_raw = ticket.get("comments")
    if comments_raw:
        try:
            comments = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
            for c in comments:
                body = (c.get("body") or "").strip()
                if body:
                    ts     = c.get("timestamp", "")
                    author = c.get("author", "")
                    parts.append(f"[{ts}] {author}: {body}")
        except Exception:
            pass
    return "\n\n".join(parts)[:8_000]


def embed_text_ollama(text: str, model: str, base_url: str) -> list[float]:
    """
    POST to Ollama and return the embedding vector.

    Tries the new /api/embed endpoint (Ollama >= 0.1.26) first, then falls
    back to the legacy /api/embeddings endpoint for older installs.
    """
    base = base_url.rstrip("/")

    # New API (>= 0.1.26): POST /api/embed, field "input", response "embeddings": [[...]]
    resp = requests.post(
        f"{base}/api/embed",
        json={"model": model, "input": text},
        timeout=120,
    )
    if resp.status_code == 200:
        data = resp.json()
        vecs = data.get("embeddings")
        if vecs and isinstance(vecs, list) and len(vecs) > 0:
            return vecs[0]

    # Legacy API (< 0.1.26): POST /api/embeddings, field "prompt", response "embedding": [...]
    resp = requests.post(
        f"{base}/api/embeddings",
        json={"model": model, "prompt": text},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


def _openai_base_url(raw: str, default: str) -> str:
    """Normalise a user-supplied URL so it ends with exactly one /v1."""
    url = (raw or default).rstrip("/")
    if url.endswith("/v1"):
        return url          # already correct
    return url + "/v1"


def embed_text(
    text: str,
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
) -> list[float]:
    """Dispatch to the correct embedding provider and return a float vector."""
    if provider == "ollama":
        return embed_text_ollama(text, model, base_url or "http://localhost:11434")

    elif provider == "lmstudio":
        if not _OPENAI_AVAILABLE:
            raise RuntimeError("openai package not installed: venv/bin/pip install openai")
        client = _openai_mod.OpenAI(
            api_key="lmstudio",
            base_url=_openai_base_url(base_url, "http://localhost:1234"),
        )
        resp = client.embeddings.create(model=model, input=text, encoding_format="float")
        return resp.data[0].embedding

    elif provider == "gemini":
        if not _GEMINI_AVAILABLE:
            raise RuntimeError("google-genai not installed: venv/bin/pip install google-genai")
        client = _genai_mod.Client(api_key=api_key)
        result = client.models.embed_content(model=model, content=text)
        return list(result.embeddings[0].values)

    elif provider == "mlx":
        if not _MLX_EMB_AVAILABLE:
            raise RuntimeError(
                "mlx-embeddings not installed: venv/bin/pip install mlx-embeddings"
            )
        # Load (or reuse cached) model
        if _mlx_emb_cache["model_id"] != model:
            m, tok = _mlx_emb_load(model)
            _mlx_emb_cache["model"]     = m
            _mlx_emb_cache["tokenizer"] = tok
            _mlx_emb_cache["model_id"]  = model
        m   = _mlx_emb_cache["model"]
        tok = _mlx_emb_cache["tokenizer"]
        tokens = tok(text, return_tensors="mlx", padding=True,
                     truncation=True, max_length=512)
        out = m(**tokens)
        # Mean-pool over token dimension, then L2-normalise
        vec = _mx.mean(out.last_hidden_state, axis=1)
        vec = vec / _mx.linalg.norm(vec, axis=-1, keepdims=True)
        return vec[0].tolist()

    else:
        raise ValueError(f"Unknown embedding provider: {provider!r}")


def embed_all_tickets(
    tickets: list[dict],
    cb_url: str,
    bucket: str,
    username: str,
    password: str,
    use_tls: bool,
    scope: str,
    collection: str,
    embed_provider: str,
    embed_model: str,
    embed_api_key: str,
    embed_base_url: str,
    vector_dims: int,
    progress_cb: Callable[[str, float], None],
) -> tuple[int, int]:
    """
    For each ticket: build embed text → call embedding provider → upsert the doc
    back to Couchbase with an added `embedding` field.  Returns (done, errors).
    """
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed — run: venv/bin/pip install couchbase")

    conn_str = _cb_conn_str(cb_url, use_tls)
    progress_cb(f"Connecting to {conn_str} …", 0.0)
    cluster = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    col = cluster.bucket(bucket).scope(scope).collection(collection)

    total = len(tickets)
    done = errors = 0

    for i, ticket in enumerate(tickets, 1):
        tid     = ticket.get("ticket_id") or f"unknown_{i}"
        doc_key = f"ticket::{tid}"
        try:
            text = build_embed_text(ticket)
            vec  = embed_text(text, embed_provider, embed_model, embed_api_key, embed_base_url)
            if len(vec) != vector_dims:
                raise ValueError(
                    f"Model returned {len(vec)} dims but expected {vector_dims}. "
                    "Update the Vector Dims field to match your model."
                )
            doc = ticket.copy()
            doc["embedding"] = vec
            col.upsert(doc_key, doc)
            done += 1
        except Exception as exc:
            errors += 1
            progress_cb(f"Error on ticket {tid}: {exc}", i / total)
            continue
        if i % 10 == 0 or i == total:
            progress_cb(f"Embedded {i}/{total} …", i / total)

    cluster.close()
    return done, errors


def create_vector_index(
    cb_url: str,
    bucket: str,
    username: str,
    password: str,
    use_tls: bool,
    scope: str,
    collection: str,
    vector_dims: int,
) -> None:
    """
    PUT a vector FTS index definition via the Couchbase FTS REST API (port 8094).

    Uses the scope-aware endpoint:
      PUT /api/bucket/{bucket}/scope/{scope}/index/{indexName}

    The index definition follows the structure from the official CB curl example:
    - sourceType = couchbase, sourceParams with bucket/scope/collections
    - doc_config.mode = scope.collection.type_field
    - mapping type key = collection name only (scoping is done by the URL path)
    """
    index_name = f"{collection}_vector_idx"
    port       = 18094 if use_tls else 8094
    api_scheme = "https" if use_tls else "http"
    host       = re.sub(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", "", cb_url).strip().rstrip("/")
    api_url    = f"{api_scheme}://{host}:{port}/api/bucket/{bucket}/scope/{scope}/index/{index_name}"

    def _text_field(name: str) -> dict:
        return {
            "dynamic": False,
            "enabled": True,
            "fields": [{"analyzer": "standard", "index": True, "name": name, "store": True, "type": "text"}],
        }

    type_key  = f"{scope}.{collection}"

    index_def = {
        "type":       "fulltext-index",
        "name":       f"{bucket}.{scope}.{index_name}",
        "sourceType": "gocbcore",
        "sourceName": bucket,
        "sourceUUID": "",
        "sourceParams": {},
        "planParams": {"maxPartitionsPerPIndex": 512, "indexPartitions": 1},
        "params": {
            "doc_config": {
                "docid_prefix_delim": "",
                "docid_regexp": "",
                "mode": "scope.collection.type_field",
                "type_field": "type",
            },
            "mapping": {
                "analysis": {},
                "default_analyzer": "standard",
                "default_datetime_parser": "dateTimeOptional",
                "default_field": "_all",
                "default_mapping": {"dynamic": False, "enabled": False},
                "default_type": "_default",
                "docvalues_dynamic": False,
                "index_dynamic": False,
                "store_dynamic": False,
                "type_field": "_type",
                "types": {
                    type_key: {
                        "dynamic": False,
                        "enabled": True,
                        "properties": {
                            "embedding": {
                                "dynamic": False,
                                "enabled": True,
                                "fields": [{
                                    "dims":       vector_dims,
                                    "index":      True,
                                    "name":       "embedding",
                                    "similarity": "dot_product",
                                    "type":       "vector",
                                }],
                            },
                            "subject":     _text_field("subject"),
                            "status":      _text_field("status"),
                            "priority":    _text_field("priority"),
                            "requester":   _text_field("requester"),
                            "assignee":    _text_field("assignee"),
                            "created":     _text_field("created"),
                            "description": _text_field("description"),
                            "comments":    _text_field("comments"),
                        },
                    }
                },
            },
            "store": {"indexType": "scorch", "segmentVersion": 16},
        },
    }

    resp = requests.put(
        api_url,
        json=index_def,
        auth=(username, password),
        verify=False,
        timeout=30,
    )
    resp.raise_for_status()


def vector_search_cb(
    query_vec: list[float],
    cb_url: str,
    bucket: str,
    username: str,
    password: str,
    use_tls: bool,
    scope: str,
    collection: str,
    top_k: int = 10,
) -> list[str]:
    """
    Run a CB vector search; returns document keys sorted by relevance.

    Retries up to 5 times with a 3-second pause on 429 / InternalServerFailure,
    which CB FTS returns while a newly created index is still building.
    """
    if not _CB_AVAILABLE:
        raise RuntimeError("couchbase SDK not installed")

    index_name = f"{collection}_vector_idx"
    conn_str   = _cb_conn_str(cb_url, use_tls)
    cluster    = Cluster(conn_str, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    scope_obj  = cluster.bucket(bucket).scope(scope)

    search_req = SearchRequest.create(
        VectorSearch.from_vector_query(
            VectorQuery("embedding", query_vec, num_candidates=top_k * 3)
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
            # CB FTS returns 429 while the index is still building
            if "429" in err_str or "query request rejected" in err_str or "internal_server_failure" in err_str.lower():
                if attempt < 5:
                    time.sleep(3)
                    continue
            # Any other error — fail immediately
            cluster.close()
            raise

    cluster.close()
    raise RuntimeError(
        f"Vector index not ready after 5 attempts (last error: {last_exc}). "
        "The index may still be building — check its status in the Couchbase UI "
        "under Search → your index, then try again."
    ) from last_exc


def build_rag_context(tickets: list[dict]) -> str:
    """Format a list of ticket dicts as a context block for the LLM system prompt."""
    lines = ["### Retrieved Ticket Context\n"]
    for i, t in enumerate(tickets, 1):
        lines.append(
            f"**Ticket {i}** (ID: {t.get('ticket_id', '?')}) — {t.get('subject', 'N/A')}"
        )
        lines.append(
            f"Status: {t.get('status','?')} | Priority: {t.get('priority','?')} "
            f"| Created: {t.get('created','?')}"
        )
        lines.append(
            f"Requester: {t.get('requester','?')} | Assignee: {t.get('assignee','?')}"
        )
        if t.get("description"):
            lines.append(f"Description: {t['description'][:1_000]}")
        comments_raw = t.get("comments")
        if comments_raw:
            try:
                comments = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
                for c in comments[:10]:
                    body = (c.get("body") or "").strip()[:500]
                    if body:
                        lines.append(
                            f"  [{c.get('timestamp','')}] {c.get('author','')}: {body}"
                        )
            except Exception:
                pass
        lines.append("")
    lines.append("--- END CONTEXT ---")
    return "\n".join(lines)


SYSTEM_PROMPT_TEMPLATE = """\
You are a Couchbase support analyst.  Answer the user's question using ONLY the \
ticket context provided below.  If the answer cannot be determined from the context, \
say so clearly.  Be concise but thorough — cite ticket IDs when relevant.

{context}
"""


def call_llm(
    messages: list[dict],
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
) -> str:
    """Send a messages list to the selected provider and return the response text."""
    if provider == "claude":
        if not _ANTHROPIC_AVAILABLE:
            raise RuntimeError("anthropic package not installed: venv/bin/pip install anthropic")
        client = _anthropic_mod.Anthropic(api_key=api_key or None)
        system   = next((m["content"] for m in messages if m["role"] == "system"), None)
        user_msgs = [m for m in messages if m["role"] != "system"]
        kwargs: dict = {"model": model, "max_tokens": 4096, "messages": user_msgs}
        if system:
            kwargs["system"] = system
        resp = client.messages.create(**kwargs)
        return resp.content[0].text

    elif provider == "gemini":
        if not _GEMINI_AVAILABLE:
            raise RuntimeError(
                "google-genai not installed: venv/bin/pip install google-genai"
            )
        client  = _genai_mod.Client(api_key=api_key)
        system  = next((m["content"] for m in messages if m["role"] == "system"), None)
        non_sys = [m for m in messages if m["role"] != "system"]
        # google-genai expects role "model" not "assistant"
        contents = [
            {"role": "user" if m["role"] == "user" else "model", "parts": [{"text": m["content"]}]}
            for m in non_sys
        ]
        config = {"max_output_tokens": 4096}
        if system:
            config["system_instruction"] = system
        resp = client.models.generate_content(model=model, contents=contents, config=config)
        return resp.text

    elif provider in ("ollama", "lmstudio"):
        if not _OPENAI_AVAILABLE:
            raise RuntimeError("openai package not installed: venv/bin/pip install openai")
        default = "http://localhost:1234" if provider == "lmstudio" else "http://localhost:11434"
        client = _openai_mod.OpenAI(
            api_key=api_key or "ollama",
            base_url=_openai_base_url(base_url, default),
        )
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=4096,
        )
        return resp.choices[0].message.content

    else:
        raise ValueError(f"Unknown LLM provider: {provider!r}")


# ─────────────────────────── Phase 3: Scoring & Analytics ────────────────────

# ── Few-shot scoring prompt ───────────────────────────────────────────────────
SCORING_SYSTEM_PROMPT = """\
You are a Couchbase support quality analyst. For each ticket you will assess the \
CUSTOMER'S OVERALL SUPPORT EXPERIENCE — not just tone, but the quality of resolution, \
responsiveness, communication, and whether the issue appears to be part of a pattern \
of recurring problems.

Return ONLY a valid JSON array — no prose, no markdown fences. One object per ticket.

Schema per object:
{
  "ticket_id": "<id>",
  "stars": <1-5>,
  "temperature": "<cold|warm|hot>",
  "resolution_quality": <1-5>,
  "response_timeliness": <1-5>,
  "communication_clarity": <1-5>,
  "complexity": <1-5>,
  "complexity_reason": "<one sentence>",
  "sentiment_summary": "<one sentence customer experience summary>"
}

Definitions:
  stars               — overall experience (1=very poor, 5=excellent)
  temperature         — cold: resolved cleanly in one pass; warm: moderate back-and-forth;
                        hot: repeated contacts, escalations, or unresolved recurring frustration
  resolution_quality  — how completely and correctly the issue was resolved
  response_timeliness — how quickly support engaged and progressed
  communication_clarity — clarity, professionalism, and usefulness of responses
  complexity          — technical difficulty and scope (1=trivial how-to, 5=multi-team production incident)
  complexity_reason   — brief justification for complexity score
  sentiment_summary   — one-sentence description of the customer's experience

--- FEW-SHOT EXAMPLES ---

Input ticket:
  ID: 10001 | Priority: normal | Status: solved | Comments: 2 | Escalations: none
  Subject: How do I enable SSL for Python SDK connection to Capella
  Description: Customer asking for SSL configuration steps for the Python SDK.
  Last comment: "Thank you, the certificate configuration worked perfectly."

Output:
[{"ticket_id":"10001","stars":5,"temperature":"cold","resolution_quality":5,
  "response_timeliness":5,"communication_clarity":5,"complexity":1,
  "complexity_reason":"Simple how-to answered in a single exchange.",
  "sentiment_summary":"Customer received a clear, immediate answer and confirmed success."}]

---

Input ticket:
  ID: 10002 | Priority: urgent | Status: solved | Comments: 18 | Escalations: ESC-441, ESC-442
  Subject: Production cluster completely unresponsive — possible data loss after failover
  Description: Customer reports all nodes showing as failed, application fully down since 2am.
  Last comment: "We finally recovered but this took 3 weeks and we lost confidence in the product."

Output:
[{"ticket_id":"10002","stars":2,"temperature":"hot","resolution_quality":2,
  "response_timeliness":1,"communication_clarity":3,"complexity":5,
  "complexity_reason":"Multi-node production failure with data loss risk requiring two escalations and three weeks to resolve.",
  "sentiment_summary":"Customer experienced a prolonged critical outage and left the engagement with significantly damaged confidence."}]

---

Input ticket:
  ID: 10003 | Priority: high | Status: solved | Comments: 7 | Escalations: none
  Subject: N1QL index not being selected by query optimizer after 7.2 upgrade
  Description: After upgrading to 7.2, the query planner ignores a covering index, causing full scans.
  Last comment: "The USE INDEX hint resolved it for now but we'd like a permanent fix in the next release."

Output:
[{"ticket_id":"10003","stars":3,"temperature":"warm","resolution_quality":3,
  "response_timeliness":3,"communication_clarity":4,"complexity":3,
  "complexity_reason":"Post-upgrade query planner regression requiring multi-step investigation with a workaround rather than a root-cause fix.",
  "sentiment_summary":"Issue was mitigated but not fully resolved, leaving the customer with a workaround and lingering concern about the next release."}]

---

Input ticket:
  ID: 10004 | Priority: normal | Status: solved | Comments: 4 | Escalations: none
  Subject: RBAC permission error after upgrading to 7.2 — breaking change not in release notes
  Description: New RBAC behavior in 7.2 broke customer's application; they found no mention in docs.
  Last comment: "Got it working after your guidance. Would have been nice to have this in the upgrade notes."

Output:
[{"ticket_id":"10004","stars":4,"temperature":"cold","resolution_quality":4,
  "response_timeliness":4,"communication_clarity":5,"complexity":2,
  "complexity_reason":"Undocumented breaking change in RBAC resolved quickly with clear guidance.",
  "sentiment_summary":"Customer resolved the issue efficiently but noted a documentation gap that could affect other users."}]

---

Input ticket:
  ID: 10005 | Priority: high | Status: open | Comments: 12 | Escalations: ESC-389
  Subject: Memory usage climbing indefinitely on analytics nodes — 4th report this year
  Description: Customer has opened tickets about this exact issue in January, April, and July.
  Last comment: "This is the same problem again. We keep reporting it and nothing changes permanently."

Output:
[{"ticket_id":"10005","stars":1,"temperature":"hot","resolution_quality":1,
  "response_timeliness":2,"communication_clarity":2,"complexity":4,
  "complexity_reason":"Recurring memory leak affecting analytics nodes with no permanent fix across four separate tickets.",
  "sentiment_summary":"Customer is visibly frustrated by the same unresolved issue recurring repeatedly with no lasting resolution."}]

--- END EXAMPLES ---

Now score the following tickets. Return ONLY the JSON array.
"""


def build_scoring_input(ticket: dict) -> str:
    """Build a compact ticket representation for the scoring prompt."""
    escs  = ticket.get("escalations") or "none"
    parts = [
        f"ID: {ticket.get('ticket_id','?')} | "
        f"Priority: {ticket.get('priority','?')} | "
        f"Status: {ticket.get('status','?')} | "
        f"Comments: {ticket.get('comment_count', 0)} | "
        f"Escalations: {escs}",
        f"Subject: {ticket.get('subject','(no subject)')}",
    ]
    if ticket.get("description"):
        parts.append(f"Description: {ticket['description'][:400]}")
    # Add last comment for tone signal
    comments_raw = ticket.get("comments")
    if comments_raw:
        try:
            comments = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
            if comments:
                last = comments[-1]
                body = (last.get("body") or "").strip()[:300]
                if body:
                    parts.append(f"Last comment ({last.get('author','')}): {body}")
        except Exception:
            pass
    return "\n  ".join(parts)


def _extract_json_array(text: str) -> list:
    """Extract a JSON array from an LLM response that may contain prose or fences."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
    start = text.find("[")
    end   = text.rfind("]")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON array found in response:\n{text[:500]}")
    return json.loads(text[start : end + 1])


def score_tickets_batch(
    batch: list[dict],
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
) -> list[dict]:
    """Send one batch to the LLM and return parsed score objects."""
    ticket_block = "\n\n".join(
        f"Ticket {i + 1}:\n  {build_scoring_input(t)}"
        for i, t in enumerate(batch)
    )
    messages = [
        {"role": "system",  "content": SCORING_SYSTEM_PROMPT},
        {"role": "user",    "content": ticket_block},
    ]
    raw = call_llm(messages, provider, model, api_key, base_url)
    return _extract_json_array(raw)


def score_all_tickets(
    tickets: list[dict],
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
    batch_size: int,
    progress_cb: Callable[[str, float], None],
) -> dict[str, dict]:
    """
    Score all tickets in batches.  Returns a dict keyed by ticket_id.
    Errors on individual batches are logged but do not abort the run.
    """
    total   = len(tickets)
    results: dict[str, dict] = {}
    batches = [tickets[i : i + batch_size] for i in range(0, total, batch_size)]

    for b_idx, batch in enumerate(batches):
        pct = b_idx / len(batches)
        progress_cb(
            f"Scoring batch {b_idx + 1}/{len(batches)} ({len(results)}/{total} done)…",
            pct,
        )
        try:
            scored = score_tickets_batch(batch, provider, model, api_key, base_url)
            for s in scored:
                tid = str(s.get("ticket_id", ""))
                if tid:
                    results[tid] = s
        except Exception as exc:
            progress_cb(f"Batch {b_idx + 1} error: {exc}", pct)

    progress_cb(f"Scoring complete — {len(results)}/{total} tickets scored.", 1.0)
    return results


# ── Analytics helpers ─────────────────────────────────────────────────────────

def _parse_created(tickets: list[dict]) -> list[str]:
    """Return YYYY-MM strings for each ticket that has a parseable created date."""
    months = []
    for t in tickets:
        raw = (t.get("created") or "").strip()
        # Try common formats: "2024-03-15", "03/15/2024", "March 15, 2024"
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y",
                    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                import datetime
                dt = datetime.datetime.strptime(raw[:len(fmt) + 2], fmt)
                months.append(f"{dt.year}-{dt.month:02d}")
                break
            except ValueError:
                continue
    return months


def build_analytics_data(tickets: list[dict], scores: dict[str, dict]) -> dict:
    """
    Compute all chart series from tickets + scores dict.
    Returns a nested dict consumed by the chart-rendering functions.
    """
    from collections import Counter
    import datetime

    # Frequency over time
    months     = sorted(_parse_created(tickets))
    month_freq = Counter(months)
    month_keys = sorted(month_freq.keys())

    # Priority
    priority_counts = Counter(
        (t.get("priority") or "unknown").capitalize() for t in tickets
    )

    # Status
    status_counts = Counter(
        (t.get("status") or "unknown").capitalize() for t in tickets
    )

    # Comment count buckets
    buckets    = {"1": 0, "2-5": 0, "6-10": 0, "11-20": 0, "21+": 0}
    for t in tickets:
        c = int(t.get("comment_count") or 0)
        if c <= 1:   buckets["1"]     += 1
        elif c <= 5:  buckets["2-5"]   += 1
        elif c <= 10: buckets["6-10"]  += 1
        elif c <= 20: buckets["11-20"] += 1
        else:         buckets["21+"]   += 1

    # Escalation rate
    with_esc    = sum(1 for t in tickets if t.get("escalations"))
    without_esc = len(tickets) - with_esc

    # Scored metrics (only if scores available)
    stars_counts      = Counter()
    temp_counts       = Counter()
    complexity_counts = Counter()
    rq_counts         = Counter()
    rt_counts         = Counter()
    cc_counts         = Counter()

    for s in scores.values():
        if s.get("stars"):        stars_counts[str(s["stars"])]                 += 1
        if s.get("temperature"):  temp_counts[s["temperature"].capitalize()]     += 1
        if s.get("complexity"):   complexity_counts[str(s["complexity"])]        += 1
        if s.get("resolution_quality"):   rq_counts[str(s["resolution_quality"])] += 1
        if s.get("response_timeliness"):  rt_counts[str(s["response_timeliness"])] += 1
        if s.get("communication_clarity"): cc_counts[str(s["communication_clarity"])] += 1

    return {
        "month_keys":        month_keys,
        "month_values":      [month_freq[k] for k in month_keys],
        "priority_labels":   list(priority_counts.keys()),
        "priority_values":   list(priority_counts.values()),
        "status_labels":     list(status_counts.keys()),
        "status_values":     list(status_counts.values()),
        "comment_labels":    list(buckets.keys()),
        "comment_values":    list(buckets.values()),
        "esc_labels":        ["With escalation", "No escalation"],
        "esc_values":        [with_esc, without_esc],
        "stars_labels":      [str(i) for i in range(1, 6)],
        "stars_values":      [stars_counts[str(i)] for i in range(1, 6)],
        "temp_labels":       ["Cold", "Warm", "Hot"],
        "temp_values":       [temp_counts["Cold"], temp_counts["Warm"], temp_counts["Hot"]],
        "complexity_labels": [str(i) for i in range(1, 6)],
        "complexity_values": [complexity_counts[str(i)] for i in range(1, 6)],
        "dim_categories":    ["Resolution Quality", "Response Timeliness", "Communication Clarity"],
        "dim_avg":           [
            round(sum(int(k)*v for k,v in rq_counts.items()) / max(sum(rq_counts.values()),1), 2),
            round(sum(int(k)*v for k,v in rt_counts.items()) / max(sum(rt_counts.values()),1), 2),
            round(sum(int(k)*v for k,v in cc_counts.items()) / max(sum(cc_counts.values()),1), 2),
        ],
    }


# ─────────────────────────── Entry point ──────────────────────────────────────

if __name__ == "__main__":
    ui.run(
        title="Supportal Scraper",
        port=8765,
        reload=False,   # reload=True would destroy _browser_state mid-session
        show=True,
        favicon="🔍",
    )
