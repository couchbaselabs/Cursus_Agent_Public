"""
Strabo launcher.

Usage:
    venv/bin/python run_strabo.py [--port 8765]
"""
import os
import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).parent

_PORT = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8765

env = os.environ.copy()
env["STRABO_PORT"] = str(_PORT)
env["PYTHONPATH"] = str(_HERE) + os.pathsep + env.get("PYTHONPATH", "")

subprocess.run(
    [sys.executable, str(_HERE / "apps" / "strabo" / "app.py")],
    env=env,
    cwd=str(_HERE),
)
