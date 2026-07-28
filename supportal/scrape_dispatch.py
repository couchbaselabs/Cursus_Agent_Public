"""
supportal/scrape_dispatch.py — Scraping tool dispatch for the unified app.

Imports apps.strabo.app in library mode (STRABO_LIBRARY_MODE=1) so that
strabo's NiceGUI page decorators are skipped and only its tool logic is used.
The shared _SCRAPE_JOBS dict lives in strabo; both strabo and unified share it
when strabo is imported here.
"""
import os

# Must be set before strabo is imported so its @ui.page("/") decorator is skipped.
os.environ.setdefault("STRABO_LIBRARY_MODE", "1")


def _strabo():
    import importlib
    return importlib.import_module("apps.strabo.app")


def execute_scrape_tool(
    name: str, args: dict,
    cb_url: str, bucket: str, username: str, password: str,
    use_tls: bool, scope: str, collection: str,
    default_customer: str = "", ctx=None,
) -> str:
    return _strabo()._execute_agent_tool(
        name, args, cb_url, bucket, username, password,
        use_tls, scope, collection,
        default_customer=default_customer, ctx=ctx,
    )
