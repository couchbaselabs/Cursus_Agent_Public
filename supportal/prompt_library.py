"""
supportal/prompt_library.py — Curated prompt library for chat users.

Prompts are organised by category. Templates may contain {customer},
which is substituted at runtime with the active customer name.
customer_required=True means the prompt is hidden/greyed when no
customer is set; False means it works fleet-wide.
"""

PROMPT_LIBRARY: list[dict] = [
    {
        "category": "Morning Routine",
        "icon": "wb_sunny",
        "prompts": [
            {
                "label": "Morning briefing",
                "prompt": "Run get_portfolio_status to show me a ranked portfolio overview — my morning briefing across all customers.",
                "customer_required": False,
            },
            {
                "label": "What's new today for {customer}?",
                "prompt": "What's changed or updated for {customer} in the last 24 hours?",
                "customer_required": True,
            },
            {
                "label": "At-risk clusters (no open ticket)",
                "prompt": "List all clusters with elevated bad or warn items and no open support ticket — leading indicators across the fleet.",
                "customer_required": False,
            },
            {
                "label": "Fleet version distribution",
                "prompt": "Show the Couchbase version distribution across all clusters in the fleet.",
                "customer_required": False,
            },
        ],
    },
    {
        "category": "Health & SLA",
        "icon": "monitor_heart",
        "prompts": [
            {
                "label": "Health score for {customer}",
                "prompt": "What is the health score for {customer}? Include open P1s, escalation rate, and data freshness.",
                "customer_required": True,
            },
            {
                "label": "SLA compliance for {customer}",
                "prompt": "Show SLA compliance breakdown by priority for {customer}.",
                "customer_required": True,
            },
            {
                "label": "Open P1 and P2 tickets for {customer}",
                "prompt": "Show all open P1 and P2 tickets for {customer}, sorted by age.",
                "customer_required": True,
            },
            {
                "label": "Escalated tickets for {customer}",
                "prompt": "How many tickets for {customer} have been escalated? Show me the list.",
                "customer_required": True,
            },
            {
                "label": "Portfolio health — all customers",
                "prompt": "Show me the health score and open P1 count for every customer, ranked worst to best.",
                "customer_required": False,
            },
        ],
    },
    {
        "category": "Fleet Analysis",
        "icon": "public",
        "prompts": [
            {
                "label": "Customers with most open P1s",
                "prompt": "Which customers have the most open P1 tickets right now? Show as a ranked chart.",
                "customer_required": False,
            },
            {
                "label": "CBSEs affecting most customers",
                "prompt": "Which CBSEs are affecting the most customers? Rank by blast radius.",
                "customer_required": False,
            },
            {
                "label": "Fleet ticket breakdown by priority",
                "prompt": "Show a breakdown of open tickets across the fleet grouped by priority.",
                "customer_required": False,
            },
            {
                "label": "Clusters with bad/warn items",
                "prompt": "List all clusters with bad or warn health items, including the customer and item details.",
                "customer_required": False,
            },
        ],
    },
    {
        "category": "Ticket Investigation",
        "icon": "manage_search",
        "prompts": [
            {
                "label": "All open tickets for {customer}",
                "prompt": "Show all open tickets for {customer} as a table, sorted by priority then age.",
                "customer_required": True,
            },
            {
                "label": "Priority breakdown chart for {customer}",
                "prompt": "Show a priority breakdown chart for all tickets belonging to {customer}.",
                "customer_required": True,
            },
            {
                "label": "Tickets older than 30 days for {customer}",
                "prompt": "Show all open tickets for {customer} that have been open for more than 30 days.",
                "customer_required": True,
            },
            {
                "label": "Recent ticket activity for {customer}",
                "prompt": "Show the 10 most recently updated tickets for {customer}.",
                "customer_required": True,
            },
            {
                "label": "Tickets with no recent update for {customer}",
                "prompt": "Which open tickets for {customer} have had no update in the last 7 days?",
                "customer_required": True,
            },
        ],
    },
    {
        "category": "Cluster & Snapshot",
        "icon": "device_hub",
        "prompts": [
            {
                "label": "Cluster health for {customer}",
                "prompt": "Show the cluster health summary for {customer} — versions, node counts, bad/warn items, RAM and CPU per node.",
                "customer_required": True,
            },
            {
                "label": "Sync snapshots for {customer}",
                "prompt": "Sync the latest snapshots for {customer} — fetch stubs and backfill topology in one step.",
                "customer_required": True,
            },
            {
                "label": "Clusters running old CB versions",
                "prompt": "Which clusters across the fleet are running Couchbase versions older than 7.2? Show a chart.",
                "customer_required": False,
            },
        ],
    },
    {
        "category": "Data Freshness",
        "icon": "update",
        "prompts": [
            {
                "label": "Check data freshness for {customer}",
                "prompt": "How fresh is our data for {customer}? Check when tickets were last scraped and flag anything stale.",
                "customer_required": True,
            },
            {
                "label": "Rescrape stale tickets for {customer}",
                "prompt": "Rescrape all stale tickets for {customer} — refresh anything older than 4 hours.",
                "customer_required": True,
            },
            {
                "label": "Force-refresh all tickets for {customer}",
                "prompt": "Force-refresh all tickets for {customer} regardless of age — use stale_hours=0.",
                "customer_required": True,
            },
        ],
    },
    {
        "category": "Reporting",
        "icon": "summarize",
        "prompts": [
            {
                "label": "Full customer report for {customer}",
                "prompt": "Generate a full customer report for {customer} including health score, SLA compliance, open tickets, and 24h digest.",
                "customer_required": True,
            },
            {
                "label": "SLA compliance chart for {customer}",
                "prompt": "Create an SLA compliance chart for {customer} broken down by priority.",
                "customer_required": True,
            },
            {
                "label": "Export all open tickets for {customer}",
                "prompt": "Export all open tickets for {customer} as a downloadable table.",
                "customer_required": True,
            },
            {
                "label": "Monthly ticket trend for {customer}",
                "prompt": "Show a monthly ticket volume trend for {customer} over the last 6 months as a line chart.",
                "customer_required": True,
            },
        ],
    },
]


def inject_customer(prompt: str, customer: str) -> str:
    """Substitute {customer} placeholder with the active customer name."""
    if "{customer}" in prompt:
        return prompt.replace("{customer}", customer) if customer else prompt
    return prompt


def get_categories() -> list[str]:
    return [c["category"] for c in PROMPT_LIBRARY]


def get_prompts_for_category(category: str) -> list[dict]:
    for c in PROMPT_LIBRARY:
        if c["category"] == category:
            return c["prompts"]
    return []


def all_prompts_flat() -> list[dict]:
    """Flat list with category and icon fields added to each prompt."""
    result = []
    for cat in PROMPT_LIBRARY:
        for p in cat["prompts"]:
            result.append({**p, "category": cat["category"], "icon": cat["icon"]})
    return result
