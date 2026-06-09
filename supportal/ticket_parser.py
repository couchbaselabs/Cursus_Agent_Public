"""
Ticket HTML and API parsers for Supportal.

Covers:
  - HTML extraction helpers (_find_label_value, _extract_comments, etc.)
  - parse_ticket_detail (Playwright/cookie HTML scrape result)
  - parse_ticket_from_api (REST /zendesk/ticket/{id}/status response)
  - _is_deleted_api_ticket (detect empty/deleted API responses)
  - Listing/navigation helpers (_extract_ticket_rows, _resolve_customer_input, etc.)

Import with:
    from supportal.ticket_parser import parse_ticket_detail, parse_ticket_from_api
"""

import datetime
import json
import re
import urllib.parse
from typing import Optional

from bs4 import BeautifulSoup

from supportal.constants import BASE_URL, UA, TICKET_HREF_RE

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


def _normalize_field_key(k: str) -> str:
    """
    Normalize a ticket_fields key to a clean, dot-navigable identifier.

    Rules (applied in order):
      1. ' (EOL)' suffix → '_EOL'   (before general replacement so parens vanish cleanly)
      2. Any non-alphanumeric/underscore character → '_'
      3. Collapse runs of underscores to one
      4. Strip leading/trailing underscores

    Examples:
      'Bug ID'                              → 'Bug_ID'
      'Node Count'                          → 'Node_Count'
      'Couchbase Server (EOL)'              → 'Couchbase_Server_EOL'
      'Environment / Current Impact'        → 'Environment_Current_Impact'
      'Mitigated (do not set directly)'     → 'Mitigated_do_not_set_directly'
      'CBSE'                                → 'CBSE'
    """
    k = k.replace(" (EOL)", "_EOL")
    k = re.sub(r"[^A-Za-z0-9_]", "_", k)
    k = re.sub(r"_+", "_", k)
    return k.strip("_")


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
    cbses: list[str] = []
    jira_issues: list[str] = []
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
                        ticket_fields[_normalize_field_key(k)] = v

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

        # ── CBSEs ──────────────────────────────────────────────────────────────
        elif box_title.strip().lower() in ("cbses", "cbse"):
            raw_body = body.get_text(" ", strip=True)
            # Collect from links first, fall back to text scan
            found = [a.get_text(strip=True) for a in body.select("a")
                     if re.search(r"CBSE-\d+", a.get_text(strip=True), re.IGNORECASE)]
            if not found:
                found = re.findall(r"CBSE-\d+", raw_body, re.IGNORECASE)
            cbses = [c.upper() for c in found]

        # ── JIRA Issues ────────────────────────────────────────────────────────
        elif "jira" in box_title:
            raw_body = body.get_text(" ", strip=True)
            # Jira keys: PROJECT-NUMBER (e.g. MB-12345, CB-12345, JMSE-456)
            found = [a.get_text(strip=True) for a in body.select("a")
                     if re.match(r"[A-Z]+-\d+", a.get_text(strip=True))]
            if not found:
                found = re.findall(r"\b[A-Z]{2,}-\d+\b", raw_body)
            # Exclude CBSEs that may appear in the Jira box by accident
            jira_issues = [j for j in found
                           if not j.upper().startswith("CBSE-")
                           and not j.upper().startswith("ESC-")]

        # ── Snapshots ─────────────────────────────────────────────────────────
        elif "snapshot" in box_title:
            lines = [t.strip() for t in body.get_text("\n").splitlines() if t.strip()
                     and not t.strip().lower().startswith("link")
                     and not t.strip().lower().startswith("no snapshot")
                     and t.strip() != "No snapshots"]
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

    # ── Last comment timestamp → last_comment_at ──────────────────────────────
    # Most recent comment is the most meaningful "last activity" indicator.
    # Also try ticket_fields for an explicit "Updated" or "Last Updated" entry.
    last_comment_at: str | None = None
    for c in reversed(comments):
        ts = c.get("timestamp")
        if ts:
            last_comment_at = ts
            break
    if not last_comment_at:
        for _k in ("Updated", "Last_Updated", "Last_Comment", "Last_Reply", "updated"):
            if ticket_fields.get(_k):
                last_comment_at = ticket_fields[_k]
                break

    return {
        "ticket_id":       ticket_id,
        "url":             url,
        "subject":         subject,
        "status":          status,
        "priority":        priority,
        "requester":       requester,
        "assignee":        assignee,
        "organization":    organization,
        "ticket_group":    ticket_group,
        "created":         created,
        "last_comment_at": last_comment_at,
        "tags":            tags_text,
        "escalations":     escalations_text,
        "cbses":           cbses if cbses else None,
        "jira_issues":     jira_issues if jira_issues else None,
        "snapshots":       snapshots_text,
        "ticket_fields":   ticket_fields if ticket_fields else None,
        "description":     description,
        "comment_count":   len(comments),
        "comments":        comments if comments else None,
    }


def _is_deleted_api_ticket(body: dict, ticket: dict) -> bool:
    """Return True if the API response represents a deleted/empty ticket."""
    status = str(ticket.get("status") or body.get("status") or "").lower()
    if status == "deleted":
        return True
    # Deleted tickets sometimes return 200 with a null/empty subject and no description
    subject = ticket.get("subject") or body.get("subject") or ""
    description = ticket.get("description") or body.get("description") or ""
    if not subject and not description and not (ticket.get("comments") or []):
        return True
    return False


def parse_ticket_from_api(body: dict, ticket_id: str) -> dict:
    """Map a /zendesk/ticket/{id}/status JSON response to the same schema as parse_ticket_detail."""
    ticket = body.get("ticket") or {}

    if _is_deleted_api_ticket(body, ticket):
        return {"ticket_id": str(ticket_id), "_deleted": True, "url": f"{BASE_URL}/zendesk/ticket/{ticket_id}"}

    # Build author_id -> name map from known user objects in the response
    id_to_name: dict[int, str] = {}
    for user_key in ("assignee", "requester", "current_user"):
        u = body.get(user_key) or {}
        if u.get("id") and u.get("name"):
            id_to_name[u["id"]] = u["name"]

    fields = ticket.get("fields") or {}

    # Normalize ticket_fields keys — skip None/empty-string values
    ticket_fields = {
        _normalize_field_key(k): v
        for k, v in fields.items()
        if v is not None and v != ""
    }

    # cbses — API pre-parses at body level; fall back to fields.CBSE string
    cbses: list[str] = list(body.get("cbses") or [])
    if not cbses and fields.get("CBSE"):
        cbses = [
            c.upper()
            for c in re.split(r"[,\s]+", str(fields["CBSE"]))
            if re.match(r"CBSE-\d+", c, re.IGNORECASE)
        ]

    # escalations — pre-parsed list; join to string for schema compatibility
    esc_list: list[str] = list(body.get("escalations") or [])
    escalations_text = ", ".join(esc_list) if esc_list else None

    # jira_issues — pre-parsed list
    jira_issues: list[str] = list(body.get("jira_issues") or [])

    # snapshots — structured list; convert to text for schema compatibility.
    # Include decoded snap IDs so _SNAP_ID_RE (hex32::N) can match them for enrichment.
    snap_list = body.get("snapshots") or []
    snap_lines = []
    for s in snap_list:
        parts = []
        enc = s.get("encoded_uid", "")
        if enc:
            parts.append(urllib.parse.unquote(enc))   # e.g. "b9fc95c4...::1"
        if s.get("timestamp"):
            parts.append(s["timestamp"])
        if parts:
            snap_lines.append("  ".join(parts))
    snapshots_text = "\n".join(snap_lines) if snap_lines else None

    # tags — list to space-separated string
    tags_list = ticket.get("tags") or []
    tags_text = " ".join(tags_list) if tags_list else None

    priority = (fields.get("Priority") or ticket.get("priority")) or None

    # Map comments
    comments: list[dict] = []
    for c in ticket.get("comments") or []:
        author_id = c.get("author_id")
        author_name = id_to_name.get(author_id, str(author_id) if author_id else None)
        comments.append({
            "timestamp": c.get("created_at"),
            "author":    author_name,
            "body":      c.get("body"),
        })

    # ── Last comment timestamp → last_comment_at ──────────────────────────────
    last_comment_at: str | None = None
    for c in reversed(comments):
        ts = c.get("timestamp")
        if ts:
            last_comment_at = ts
            break
    if not last_comment_at:
        for _k in ("Updated", "Last_Updated", "Last_Comment", "Last_Reply", "updated"):
            if ticket_fields.get(_k):
                last_comment_at = ticket_fields[_k]
                break

    url = f"{BASE_URL}/zendesk/ticket/{ticket_id}"
    return {
        "ticket_id":       str(ticket.get("id", ticket_id)),
        "url":             url,
        "subject":         ticket.get("subject"),
        "status":          ticket.get("status"),
        "priority":        priority,
        "requester":       ticket.get("requester"),
        "assignee":        ticket.get("assignee"),
        "organization":    ticket.get("organization"),
        "ticket_group":    ticket.get("group"),
        "created":         ticket.get("created_at"),
        "updated":         ticket.get("updated_at"),
        "last_comment_at": last_comment_at,
        "tags":            tags_text,
        "escalations":     escalations_text,
        "cbses":           cbses if cbses else None,
        "jira_issues":     jira_issues if jira_issues else None,
        "snapshots":       snapshots_text,
        "ticket_fields":   ticket_fields if ticket_fields else None,
        "description":     ticket.get("description"),
        "comment_count":   len(comments),
        "comments":        comments if comments else None,
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


def _extract_ticket_ids(text: str) -> set[str]:
    """Extract ticket IDs from free text (hash-prefixed, 'ticket N', or standalone 5+ digit)."""
    ids: set[str] = set()
    for m in re.finditer(r"(?:ticket\s+#?|#)(\d{4,})", text, re.IGNORECASE):
        ids.add(m.group(1))
    for m in re.finditer(r"(?<!\d)(\d{5,})(?!\d)", text):
        ids.add(m.group(1))
    return ids


def _parse_ticket_fields(ticket: dict) -> dict:
    """Return ticket_fields custom-field dict with normalized keys, or {}.

    Handles three formats: already-normalized dict, JSON string with original
    spaced keys, or missing field.
    """
    import json as _json
    raw = ticket.get("ticket_fields") or ticket.get("fields") or {}
    if isinstance(raw, dict):
        if all(" " not in k and "(" not in k for k in raw):
            return raw
        return {_normalize_field_key(k): v for k, v in raw.items()}
    try:
        parsed = _json.loads(raw)
        if isinstance(parsed, dict):
            return {_normalize_field_key(k): v for k, v in parsed.items()}
        return {}
    except Exception:
        return {}

