"""
Cursus MCP server runner.

Usage:
  # stdio — for Claude Desktop / Cursor (default)
  venv/bin/python run_mcp.py

  # SSE — for Gemini, remote clients, or testing via curl
  venv/bin/python run_mcp.py --transport sse --port 8768

Claude Desktop config  (~/.claude/claude_desktop_config.json):
  {
    "mcpServers": {
      "cursus": {
        "command": "/Users/austin.gonyou/Downloads/Apps/Scraper/venv/bin/python",
        "args": ["/Users/austin.gonyou/Downloads/Apps/Scraper/run_mcp.py"]
      }
    }
  }
"""
import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path so `apps.*` and `supportal.*` imports resolve.
_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from apps.mcp.server import mcp  # noqa: E402  (path must be set first)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cursus MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport to use (default: stdio)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8768,
        help="Port for SSE transport (default: 8768)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host for SSE transport (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    if args.transport == "sse":
        print(f"[run_mcp] Starting Cursus MCP server (SSE) → http://{args.host}:{args.port}")
        mcp.run(transport="sse", host=args.host, port=args.port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
