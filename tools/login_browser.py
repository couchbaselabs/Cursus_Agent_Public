#!/usr/bin/env python3
"""
Open a headed Chromium window with the saved Supportal session profile.
Complete the Okta SSO login; cookies are auto-saved when login is detected.

Usage:
    venv/bin/python login_browser.py
"""
import json
import pathlib
from playwright.sync_api import sync_playwright

PROFILE_DIR = pathlib.Path(__file__).parent / ".playwright_supportal"
BASE_URL = "https://supportal.couchbase.com"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

with sync_playwright() as pw:
    print(f"Opening browser with profile: {PROFILE_DIR}")
    print("Complete the Okta SSO login in the browser window.")
    print("Once you are logged in and can see Supportal content, come back here and press Enter.")
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=False,
        user_agent=UA,
        ignore_https_errors=True,
        args=["--start-maximized"],
    )
    page = ctx.new_page()
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)

    print("Waiting for successful login (URL must return to supportal.couchbase.com)...")
    print("Complete the Okta SSO flow in the browser window.")

    # Poll until we're back on Supportal and NOT on an auth/login page
    import time
    while True:
        try:
            url = page.url
            if (
                "supportal.couchbase.com" in url
                and "okta.com" not in url
                and "/login" not in url
                and "/auth" not in url
            ):
                # Verify there's actual content — look for the nav or any ticket link
                logged_in = page.evaluate("""() => {
                    return document.querySelector('nav') !== null ||
                           document.querySelector('a[href*="/customer/"]') !== null ||
                           document.querySelector('.navbar') !== null;
                }""")
                if logged_in:
                    print(f"  Logged in! Current URL: {url}")
                    break
        except Exception:
            pass
        time.sleep(2)

    # Save cookies to ~/.supportal_cookies.json for the main app to read
    try:
        raw_cookies = ctx.cookies(urls=[BASE_URL])
        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in raw_cookies)
        cookies_file = pathlib.Path.home() / ".supportal_cookies.json"
        cookies_file.write_text(json.dumps({"cookie": cookie_str}, indent=2))
        print(f"Cookies saved to {cookies_file}")
    except Exception as exc:
        print(f"Warning: could not save cookies: {exc}")

    print("Session saved. Closing browser...")
    ctx.close()

print("Done. Cookies saved to ~/.supportal_cookies.json")
