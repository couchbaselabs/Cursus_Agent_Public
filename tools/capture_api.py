#!/usr/bin/env python3
"""
Probe the Supportal REST API using the saved Playwright session.
Fetches GET /api/tickets/{id} and saves the raw JSON response.

Usage:
    venv/bin/python capture_api.py [ticket_id]

Default ticket_id: 76866
"""
import json, sys, pathlib
from playwright.sync_api import sync_playwright

PROFILE_DIR = pathlib.Path(__file__).parent / ".playwright_supportal"
BASE_URL     = "https://supportal.couchbase.com"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

ticket_id = sys.argv[1] if len(sys.argv) > 1 else "76866"

results: dict = {}

with sync_playwright() as pw:
    print(f"Launching with profile: {PROFILE_DIR}")
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=True,
        user_agent=UA,
        ignore_https_errors=True,
    )
    page = ctx.new_page()
    page.set_default_timeout(30_000)

    # Use route.fetch() to intercept the SPA's /zendesk/ticket/{id}/status call
    # and read the response body synchronously — avoids the race condition where
    # the browser closes before response.body() can be called.
    def intercept_status(route):
        response = route.fetch()
        url = route.request.url
        try:
            body = response.body()
            parsed = json.loads(body) if body else None
            results[url] = {"status": response.status, "body": parsed}
            print(f"  intercepted {response.status}  {url}  ({len(body)} bytes)")
        except Exception as exc:
            results[url] = {"status": response.status, "error": str(exc)}
            print(f"  intercept error {url}: {exc}")
        route.fulfill(response=response)

    page.route("**/zendesk/ticket/*/status**", intercept_status)
    page.route("**/zendesk/ticket/*/comments**", intercept_status)
    page.route("**/zendesk/ticket/*/escalations**", intercept_status)

    # Navigate to the SPA ticket page and let Vue make its XHR calls
    spa_url = f"{BASE_URL}/zendesk/ticket/{ticket_id}"
    print(f"\nNavigating to SPA ticket page: {spa_url}")
    try:
        page.goto(spa_url, wait_until="domcontentloaded", timeout=30_000)
        print("  Waiting 10s for Vue XHR calls...")
        page.wait_for_timeout(10_000)
        print(f"  Intercepted {len(results)} API calls")
    except Exception as exc:
        print(f"  error: {exc}")

    # Also try the list API (no sortBy — sortBy triggers a CB timeout in their backend)
    list_url = f"{BASE_URL}/api/tickets?page=1&perPage=3"
    print(f"\nFetching ticket list: {list_url}")
    try:
        resp = page.goto(list_url, wait_until="domcontentloaded", timeout=30_000)
        text = page.evaluate("() => document.body.innerText")
        print(f"  status: {resp.status if resp else '?'}")
        try:
            list_body = json.loads(text)
            results[list_url] = {"status": resp.status, "body": list_body}
            tickets = list_body.get("data", [])
            print(f"  total: {list_body.get('cursor',{}).get('totalItems')}  fields: {list(tickets[0].keys()) if tickets else 'none'}")
            for t in tickets:
                print(f"    id={t.get('id')}  status={t.get('status')}  cbse={t.get('cbse')!r}  esc_ent={t.get('esc_ent')!r}  bug={t.get('bug')!r}")
        except json.JSONDecodeError:
            print(f"  not JSON: {text[:300]}")
    except Exception as exc:
        print(f"  error: {exc}")

    ctx.close()

# Save all results
out = pathlib.Path(__file__).parent / f"api_response_{ticket_id}.json"
out.write_text(json.dumps(results, indent=2, default=str))
print(f"\nSaved to {out}")

# Print any intercepted /status responses
for url, data in results.items():
    if "/status" in url and data.get("body"):
        print(f"\n=== {url} ===")
        print(json.dumps(data["body"], indent=2, default=str)[:4000])
