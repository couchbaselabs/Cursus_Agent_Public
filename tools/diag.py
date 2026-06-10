#!/usr/bin/env python3
"""Quick diagnostic: launch headless Playwright with the saved session and
   navigate to a few key URLs, saving HTML and printing what was found."""
import os, re, sys, pathlib
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PROFILE_DIR = ".playwright_supportal"
BASE_URL     = "https://supportal.couchbase.com"
OUT_DIR      = pathlib.Path("diag_html")
OUT_DIR.mkdir(exist_ok=True)
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

CUSTOMER = sys.argv[1] if len(sys.argv) > 1 else "American Express"

def save(name, url, html):
    p = OUT_DIR / f"{name}.html"
    p.write_text(html, encoding="utf-8")
    print(f"  → saved {p}  ({len(html)} bytes)")
    return html

def check(html, label):
    lh = html.lower()
    if "sign in" in lh or "login" in lh and "logout" not in lh:
        print(f"  ⚠  {label}: looks like a LOGIN PAGE (not authenticated)")
    elif "american express" in lh or CUSTOMER.lower() in lh:
        print(f"  ✓  {label}: customer name found in page")
    else:
        print(f"  ?  {label}: page loaded but customer name not found")
    # Show title
    m = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
    if m:
        print(f"     title: {m.group(1).strip()}")

with sync_playwright() as pw:
    print(f"Launching headless Chromium with profile: {PROFILE_DIR}")
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=PROFILE_DIR,
        headless=True,
        user_agent=UA,
        ignore_https_errors=True,
    )
    page = ctx.new_page()
    page.set_default_timeout(30_000)

    # ── 1. Home page ──────────────────────────────────────────────────────────
    print("\n[1] Home page")
    try:
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
        html = page.content()
        save("01_home", BASE_URL, html)
        check(html, "home")
    except PWTimeout:
        print("  ✗ TIMEOUT on home page")

    # ── 2. Search (path-based) ────────────────────────────────────────────────
    import urllib.parse
    search_url = f"{BASE_URL}/search/{urllib.parse.quote(CUSTOMER)}"
    print(f"\n[2] Search: {search_url}")
    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
        # Wait for Vue to render customer links
        try:
            page.wait_for_selector("a[href*='/customer/']", timeout=10_000)
        except Exception:
            page.wait_for_timeout(4000)
        html = page.content()
        save("02_search", search_url, html)
        check(html, "search")
        # All hrefs containing /customer/ (absolute or relative)
        links = re.findall(r'href="([^"]*customer[^"]*)"', html)
        print(f"  /customer/ links: {links[:10]}")
    except PWTimeout:
        print("  ✗ TIMEOUT on search page")

    # ── 3. Direct customer URL ────────────────────────────────────────────────
    cust_url = f"{BASE_URL}/customer/{urllib.parse.quote(CUSTOMER, safe='')}"
    print(f"\n[3] Direct customer: {cust_url}")
    try:
        page.goto(cust_url, wait_until="domcontentloaded", timeout=30_000)

        # Wait for Vue to render ticket links into the live DOM
        print("  Waiting up to 30s for ticket links to appear in live DOM…")
        try:
            page.wait_for_selector("a[href*='/zendesk/ticket/']", timeout=30_000)
            print("  ✓ Ticket links visible in live DOM")
        except Exception:
            print("  ⚠ Timed out — capturing whatever is rendered")

        # Capture live DOM HTML (this is what Playwright sees, not view-source)
        html = page.content()
        save("03_customer", cust_url, html)
        check(html, "customer direct")

        ticket_links = re.findall(r'href="[^"]*zendesk/ticket/(\d+)[^"]*"', html)
        print(f"  Ticket links found: {len(ticket_links)}  first few IDs: {ticket_links[:5]}")

        # ── Pagination: query live DOM via JS (BS4 on static source won't work) ──
        print("\n  --- Live DOM pagination ---")
        pag_html = page.evaluate("""() => {
            // Bootstrap pagination is a <ul class="pagination">
            const ul = document.querySelector('ul.pagination');
            if (ul) return ul.outerHTML;
            // Fallback: find any element whose text contains "First" and "Last"
            for (const el of document.querySelectorAll('*')) {
                const t = el.innerText || '';
                if (t.includes('First') && t.includes('Last') && /\\d/.test(t)) {
                    // Return the smallest matching container
                    return el.outerHTML.substring(0, 2000);
                }
            }
            return 'NOT FOUND';
        }""")
        print(f"  Pagination outerHTML:\n{pag_html}\n")

        # ── "Showing N of T" text ──
        showing = page.evaluate("""() => {
            for (const el of document.querySelectorAll('*')) {
                const t = (el.innerText || '').trim();
                if (/Showing \\d+ of \\d+/.test(t) && t.length < 200) return t;
            }
            return 'NOT FOUND';
        }""")
        print(f"  Showing text: {showing!r}")

        # ── Tab links from live DOM ──
        tab_links = page.evaluate("""() => {
            return Array.from(document.querySelectorAll('a[href^="#"]'))
                .map(a => ({text: a.innerText.trim(), href: a.getAttribute('href')}))
                .filter(x => x.text && !x.href.includes(' '));
        }""")
        print(f"  Hash-href tab links: {tab_links}")

    except PWTimeout:
        print("  ✗ TIMEOUT on customer page")

    # ── 4. Single ticket detail ──────────────────────────────────────────────
    ticket_url = "https://supportal.couchbase.com/zendesk/ticket/76866"
    print(f"\n[4] Ticket detail: {ticket_url}")
    try:
        # Use "commit" — fires on first bytes received, doesn't wait for DOM.
        # Ticket pages may be slow or go through an auth redirect.
        page.goto(ticket_url, wait_until="commit", timeout=60_000)
        # ticket.js (Vue) renders content into <section class="content"> after load.
        print("  Waiting up to 60s for ticket.js to render content…")
        try:
            page.wait_for_function(
                """() => {
                    const sec = document.querySelector('section.content');
                    return sec && sec.innerText && sec.innerText.trim().length > 50;
                }""",
                timeout=60_000,
            )
            print("  ✓ Content section populated")
        except Exception as e:
            print(f"  ⚠ Timed out waiting for content: {e}")

        html = page.content()
        save("04_ticket_detail", ticket_url, html)

        structure = page.evaluate("""() => {
            const sec = document.querySelector('section.content');
            const headings = Array.from(document.querySelectorAll('h1,h2,h3,h4'))
                .map(h => h.innerText.trim()).filter(Boolean).slice(0, 15);
            const dts = Array.from(document.querySelectorAll('dt,th,.field-label,.label'))
                .map(el => el.innerText.trim()).filter(Boolean).slice(0, 30);
            const contentText = sec ? sec.innerText.substring(0, 2000) : '(section.content not found)';
            return {headings, dts, contentText};
        }""")
        print(f"\n  Headings: {structure['headings']}")
        print(f"  Field labels (dt/th): {structure['dts']}")
        print(f"\n  section.content text (first 2000 chars):\n{structure['contentText']}")

    except PWTimeout:
        print("  ✗ TIMEOUT on ticket detail page")

    ctx.close()

print(f"\nDone. HTML files saved to {OUT_DIR}/")
