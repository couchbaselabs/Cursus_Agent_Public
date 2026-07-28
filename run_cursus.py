"""
Cursus — starts Strabo and Corax together with auto-restart on file changes.

Named after the cursus publicus, Rome's imperial relay network that kept all
communications moving across the empire.

Hot reload:
    Cursus watches all .py files under supportal/, apps/strabo/, and apps/corax/.
    Saving any file triggers a targeted restart of the affected app(s):
      - supportal/*.py  → restarts both Strabo and Corax (shared library)
      - apps/strabo/**  → restarts Strabo only
      - apps/corax/**   → restarts Corax only
    A 15-second debounce + post-restart cooldown prevents startup-event loops.
    Corax session history is safe — threads are persisted in Couchbase.

Usage:
    venv/bin/python run_cursus.py
    venv/bin/python run_cursus.py --strabo-port 8765 --corax-port 8766
    venv/bin/python run_cursus.py --no-watch          # disable file watcher
    venv/bin/python run_cursus.py --unified --mcp     # also serve Unified + MCP

Optional extra surfaces (off by default; used by the Docker image):
    --unified          also start Cursus Unified (:8767).  Env: RUN_UNIFIED=1
    --mcp              also start the MCP server via SSE (:8768).
                       Env: MCP_TRANSPORT=sse
    --unified-port N   Unified port (default 8767)
    --mcp-port N       MCP SSE port (default 8768)
    Unified and MCP are supervised (restarted on crash) but NOT hot-reloaded —
    restart Cursus to pick up code changes in apps/unified or apps/mcp.
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path


def _free_port(port: int) -> None:
    """Kill any process currently bound to *port* so a fresh start never races."""
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f"tcp:{port}"], stderr=subprocess.DEVNULL
        ).decode().split()
        for pid_str in out:
            try:
                os.kill(int(pid_str), signal.SIGKILL)
            except (ProcessLookupError, ValueError):
                pass
        if out:
            time.sleep(0.4)  # give the kernel a moment to release the socket
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass  # lsof exits non-zero when no process holds the port — that's fine
              # FileNotFoundError: lsof not present in slim container images

_HERE = Path(__file__).parent

_BOLD  = "\033[1m"
_RESET = "\033[0m"
_BLUE  = "\033[34m"
_TEAL  = "\033[36m"
_GREY  = "\033[90m"
_YELL  = "\033[33m"

# Debounce: ignore further changes for this many seconds after a restart.
# Must be longer than app startup time — Corax imports are heavy.
_DEBOUNCE = 15.0

# Files/dirs that trigger a hot-reload restart. Only Strabo and Corax are
# hot-reloaded; Unified and MCP are supervised (start/stream/restart-on-crash)
# but not reloaded on save — restart Cursus to pick up changes to those.
_STRABO_PATHS = {"apps/strabo", "run_strabo.py"}
_CORAX_PATHS  = {"apps/corax",  "run_corax.py"}
_SHARED_PATHS = {"supportal"}   # changes here reload both Strabo and Corax


def _stream(proc_holder: list, label: str, color: str) -> None:
    """Stream stdout from whichever process is currently in proc_holder[0]."""
    while True:
        proc = proc_holder[0]
        if proc is None:
            time.sleep(0.1)
            continue
        for raw in proc.stdout:
            sys.stdout.write(f"{color}{_BOLD}{label}{_RESET} {raw.decode(errors='replace')}")
            sys.stdout.flush()
        # stdout closed — process ended; loop back and wait for next proc
        time.sleep(0.1)


def _parse_port(args: list[str], flag: str, default: int) -> int:
    try:
        return int(args[args.index(flag) + 1])
    except (ValueError, IndexError):
        return default


def _start_strabo(py: str, port: int) -> subprocess.Popen:
    _free_port(port)
    return subprocess.Popen(
        [py, str(_HERE / "run_strabo.py"), "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )


def _start_corax(py: str, port: int) -> subprocess.Popen:
    _free_port(port)
    return subprocess.Popen(
        [py, str(_HERE / "run_corax.py"), "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )


def _start_mcp(py: str, port: int) -> subprocess.Popen:
    _free_port(port)
    return subprocess.Popen(
        [py, str(_HERE / "run_mcp.py"), "--transport", "sse", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )


def _start_unified(py: str, port: int) -> subprocess.Popen:
    _free_port(port)
    return subprocess.Popen(
        [py, str(_HERE / "run_unified.py"), "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )


def _restart(proc_holder: list, name: str, start_fn, label: str, color: str) -> None:
    old = proc_holder[0]
    if old and old.poll() is None:
        print(f"\n{_YELL}{_BOLD}[cursus]{_RESET} restarting {name}…")
        try:
            old.terminate()
            old.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                old.kill()
                old.wait(timeout=2)
            except Exception:
                pass
        except Exception:
            pass
    # start_fn calls _free_port internally — port is guaranteed clear
    new = start_fn()
    proc_holder[0] = new
    print(f"{_YELL}{_BOLD}[cursus]{_RESET} {name} restarted (pid {new.pid})")


def _watch_loop(
    strabo_holder: list,
    corax_holder: list,
    strabo_fn,
    corax_fn,
) -> None:
    """Use watchfiles to watch for source changes and restart the affected app."""
    try:
        from watchfiles import watch as wf_watch
    except ImportError:
        print(f"{_GREY}[cursus] watchfiles not available — file watching disabled.{_RESET}")
        return

    watch_root = str(_HERE)
    last_restart: dict[str, float] = {"strabo": 0.0, "corax": 0.0}

    print(f"{_GREY}[cursus] watching {watch_root} for changes…{_RESET}")

    try:
        for changes in wf_watch(watch_root, watch_filter=_py_filter):
            now = time.monotonic()
            restart_strabo = False
            restart_corax  = False
            trigger_files: list[str] = []

            for _change_type, path in changes:
                rel = Path(path).relative_to(_HERE).as_posix()
                trigger_files.append(rel)
                if any(rel.startswith(p) for p in _SHARED_PATHS):
                    restart_strabo = True
                    restart_corax  = True
                elif any(rel.startswith(p) for p in _STRABO_PATHS):
                    restart_strabo = True
                elif any(rel.startswith(p) for p in _CORAX_PATHS):
                    restart_corax  = True

            if restart_strabo and now - last_restart["strabo"] > _DEBOUNCE:
                print(f"{_GREY}[cursus] change detected: {', '.join(trigger_files)}{_RESET}")
                _restart(strabo_holder, "strabo", strabo_fn, "[strabo]", _BLUE)
                last_restart["strabo"] = now

            if restart_corax and now - last_restart["corax"] > _DEBOUNCE:
                if not restart_strabo:  # avoid printing twice when both restart
                    print(f"{_GREY}[cursus] change detected: {', '.join(trigger_files)}{_RESET}")
                _restart(corax_holder, "corax", corax_fn, "[corax] ", _TEAL)
                last_restart["corax"] = now

    except Exception as exc:
        print(f"{_GREY}[cursus] watcher stopped: {exc}{_RESET}")


def _py_filter(change, path: str) -> bool:
    """Only watch .py files; ignore __pycache__, .pyc, venv, .git."""
    p = Path(path)
    if p.suffix != ".py":
        return False
    parts = p.parts
    return not any(seg in parts for seg in ("__pycache__", "venv", ".git", ".tox"))


def main() -> None:
    args = sys.argv[1:]
    strabo_port  = _parse_port(args, "--strabo-port",  8765)
    corax_port   = _parse_port(args, "--corax-port",   8766)
    mcp_port     = _parse_port(args, "--mcp-port",     8768)
    unified_port = _parse_port(args, "--unified-port", 8767)
    no_watch     = "--no-watch" in args
    # MCP and Unified are opt-in: enabled by CLI flag or env toggle. In Docker,
    # docker-compose sets both env vars so the container serves all four surfaces.
    run_mcp      = os.environ.get("MCP_TRANSPORT", "").lower() == "sse" or "--mcp" in args
    run_unified  = os.environ.get("RUN_UNIFIED", "").lower() in ("1", "true", "yes") or "--unified" in args

    py = sys.executable

    unified_label = f"  {_YELL}Unified :{unified_port}{_RESET}" if run_unified else ""
    mcp_label     = f"  {_GREY}MCP :{mcp_port}{_RESET}" if run_mcp else ""
    print(
        f"\n{_BOLD}Cursus{_RESET}  "
        f"{_BLUE}Strabo :{strabo_port}{_RESET}  "
        f"{_TEAL}Corax :{corax_port}{_RESET}"
        + unified_label
        + mcp_label
        + (f"  {_GREY}(watching for changes){_RESET}" if not no_watch else "")
        + f"\n{_GREY}Press Ctrl+C to stop all.{_RESET}\n"
    )

    strabo_fn  = lambda: _start_strabo(py, strabo_port)
    corax_fn   = lambda: _start_corax(py, corax_port)
    mcp_fn     = lambda: _start_mcp(py, mcp_port)
    unified_fn = lambda: _start_unified(py, unified_port)

    strabo_holder:  list = [strabo_fn()]
    corax_holder:   list = [corax_fn()]
    mcp_holder:     list = [mcp_fn() if run_mcp else None]
    unified_holder: list = [unified_fn() if run_unified else None]

    def _shutdown(sig=None, _frame=None) -> None:
        print(f"\n{_BOLD}[cursus]{_RESET} shutting down…")
        procs = [h[0] for h in (strabo_holder, corax_holder, mcp_holder, unified_holder) if h[0]]
        # Graceful SIGTERM first
        for p in procs:
            try:
                p.terminate()
            except OSError:
                pass
        # Wait up to 6s for clean exit, then SIGKILL anything still running
        deadline = time.monotonic() + 6.0
        for p in procs:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    p.kill()
                    p.wait(timeout=2)
                except Exception:
                    pass
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    threading.Thread(
        target=_stream, args=(strabo_holder, "[strabo]", _BLUE), daemon=True
    ).start()
    threading.Thread(
        target=_stream, args=(corax_holder, "[corax] ", _TEAL), daemon=True
    ).start()
    if run_unified:
        threading.Thread(
            target=_stream, args=(unified_holder, "[unified]", _YELL), daemon=True
        ).start()
    if run_mcp:
        threading.Thread(
            target=_stream, args=(mcp_holder, "[mcp]   ", _GREY), daemon=True
        ).start()

    # If either process exits unexpectedly, bring down everything.
    # Only shutdown if the process that died is still the *current* one in the
    # holder — a restarted process has already been replaced before it's killed.
    def _watch_exit(holder: list, name: str) -> None:
        while True:
            p = holder[0]
            if p:
                p.wait()
                # Check it's still the active process (not an old one from pre-restart)
                if holder[0] is p and p.returncode not in (0, -15):  # -15 = SIGTERM
                    print(
                        f"\n{_BOLD}[cursus]{_RESET} {name} exited "
                        f"(code {p.returncode}) — stopping all."
                    )
                    _shutdown()
            time.sleep(0.5)

    threading.Thread(target=_watch_exit, args=(strabo_holder, "strabo"), daemon=True).start()
    threading.Thread(target=_watch_exit, args=(corax_holder,  "corax"),  daemon=True).start()
    if run_unified:
        threading.Thread(target=_watch_exit, args=(unified_holder, "unified"), daemon=True).start()
    if run_mcp:
        threading.Thread(target=_watch_exit, args=(mcp_holder, "mcp"), daemon=True).start()

    if not no_watch:
        threading.Thread(
            target=_watch_loop,
            args=(strabo_holder, corax_holder, strabo_fn, corax_fn),
            daemon=True,
        ).start()

    # Keep the main thread alive. Do NOT use holder[0].wait() here — after a
    # hot-reload restart the initial process is already dead, so .wait() returns
    # immediately and the main thread exits, killing Cursus entirely.
    # The _watch_exit daemon threads handle unexpected exits.
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    main()
