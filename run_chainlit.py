"""
Direct Chainlit launcher — bypasses nest_asyncio for Python 3.14 compatibility.

Chainlit's CLI applies nest_asyncio.apply() at import time, which breaks
asyncio.current_task() under Python 3.14.  This launcher replicates the
same server setup without patching asyncio, so anyio/sniffio work correctly.

Usage (instead of 'chainlit run apps/chainlit/app.py --port 8766'):
    venv/bin/python run_chainlit.py [--port 8766]
"""
import asyncio
import os
import sys
from pathlib import Path

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

_PORT = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8766
_HOST = os.environ.get("CHAINLIT_HOST", "0.0.0.0")

# ── JWT secret — required when password auth is enabled ──────────────────────
# Auto-generate and persist to .env so re-runs keep sessions valid.
_ENV_FILE = _HERE / ".env"

def _ensure_jwt_secret() -> None:
    import secrets as _sec
    if os.environ.get("CHAINLIT_AUTH_SECRET"):
        return
    # Try loading from .env
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            if line.startswith("CHAINLIT_AUTH_SECRET="):
                os.environ["CHAINLIT_AUTH_SECRET"] = line.split("=", 1)[1].strip()
                return
    # Generate a new secret and persist it
    secret = _sec.token_hex(32)
    with _ENV_FILE.open("a") as f:
        f.write(f"\nCHAINLIT_AUTH_SECRET={secret}\n")
    os.environ["CHAINLIT_AUTH_SECRET"] = secret
    print(f"[run_chainlit] Generated new JWT secret → {_ENV_FILE}")

_ensure_jwt_secret()

# ── Chainlit setup (mirrors run_chainlit() without nest_asyncio) ─────────────
# Import config modules BEFORE chainlit.cli so nest_asyncio is never applied.
from chainlit.auth import ensure_jwt_secret          # noqa: E402
from chainlit.cache import init_lc_cache             # noqa: E402
from chainlit.config import (                        # noqa: E402
    DEFAULT_ROOT_PATH, config, init_config, load_module,
)
from chainlit.markdown import init_markdown          # noqa: E402

config.run.host = _HOST
config.run.port = _PORT
config.run.root_path = os.environ.get("CHAINLIT_ROOT_PATH", DEFAULT_ROOT_PATH)

# Import the ASGI app after config is populated
from chainlit.server import app                      # noqa: E402

# Load our handler module — this registers @cl.on_chat_start / @cl.on_message
_target = str(_HERE / "apps" / "chainlit" / "app.py")
config.run.module_name = _target
load_module(_target)

ensure_jwt_secret()
init_markdown(config.root)
init_lc_cache()


async def _serve() -> None:
    import uvicorn
    cfg = uvicorn.Config(
        app,
        host=_HOST,
        port=_PORT,
        log_level="warning",
        ws="auto",
    )
    server = uvicorn.Server(cfg)
    print(f"Supportal Chainlit Chat → http://localhost:{_PORT}")
    await server.serve()


if __name__ == "__main__":
    asyncio.run(_serve())
