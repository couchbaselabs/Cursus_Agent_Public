"""
Shared constants for Supportal modules.

Import with:
    from supportal.constants import BASE_URL, UA, TICKET_HREF_RE, SETTINGS_FILE
"""

import os
import re
from pathlib import Path

BASE_URL = "https://supportal.couchbase.com"
PROFILE_DIR = os.path.join(os.path.dirname(__file__), ".playwright_supportal")  # kept for login_browser.py reference
COOKIES_FILE = Path.home() / ".supportal_cookies.json"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TICKET_HREF_RE  = re.compile(r"/zendesk/ticket/(\d+)(?:\?.*)?$")
SETTINGS_FILE   = Path.home() / ".supportal_settings.json"

