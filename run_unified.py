"""
Cursus Unified — single-process dashboard + assistant shell.

Usage:
    venv/bin/python run_unified.py
    venv/bin/python run_unified.py --port 8767
"""
import os
import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).parent
_PORT = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8767

env = os.environ.copy()
env["UNIFIED_PORT"] = str(_PORT)
env["PYTHONPATH"] = str(_HERE) + os.pathsep + env.get("PYTHONPATH", "")

subprocess.run(
    [sys.executable, str(_HERE / "apps" / "unified" / "app.py")],
    env=env,
    cwd=str(_HERE),
)
