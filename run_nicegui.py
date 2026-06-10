"""
NiceGUI launcher.

Usage:
    venv/bin/python run_nicegui.py [--port 8080]
"""
import sys
import os
from pathlib import Path

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

_PORT = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8080
os.environ.setdefault("NICEGUI_PORT", str(_PORT))

import apps.nicegui.app  # noqa: F401, E402 — triggers ui.run() at bottom of module
