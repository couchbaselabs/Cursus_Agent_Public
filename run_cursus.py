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
    A 2-second debounce prevents burst-write double-restarts.
    Corax session history is safe — threads are persisted in Couchbase.

Usage:
    venv/bin/python run_cursus.py
    venv/bin/python run_cursus.py --strabo-port 8765 --corax-port 8766
    venv/bin/python run_cursus.py --no-watch   # disable file watcher
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

_HERE = Path(__file__).parent

_BOLD  = "\033[1m"
_RESET = "\033[0m"
_BLUE  = "\033[34m"
_TEAL  = "\033[36m"
_GREY  = "\033[90m"
_YELL  = "\033[33m"

# Debounce window — ignore further changes for this many seconds after a restart
_DEBOUNCE = 2.0

# Files/dirs that trigger a restart of each app
_STRABO_PATHS = {"apps/strabo", "run_strabo.py"}
_CORAX_PATHS  = {"apps/corax",  "run_corax.py"}
_SHARED_PATHS = {"supportal"}   # changes here restart both


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
    return subprocess.Popen(
        [py, str(_HERE / "run_strabo.py"), "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )


def _start_corax(py: str, port: int) -> subprocess.Popen:
    return subprocess.Popen(
        [py, str(_HERE / "run_corax.py"), "--port", str(port)],
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
        except Exception:
            try:
                old.kill()
            except Exception:
                pass
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

            for _change_type, path in changes:
                rel = Path(path).relative_to(_HERE).as_posix()
                if any(rel.startswith(p) for p in _SHARED_PATHS):
                    restart_strabo = True
                    restart_corax  = True
                elif any(rel.startswith(p) for p in _STRABO_PATHS):
                    restart_strabo = True
                elif any(rel.startswith(p) for p in _CORAX_PATHS):
                    restart_corax  = True

            if restart_strabo and now - last_restart["strabo"] > _DEBOUNCE:
                _restart(strabo_holder, "strabo", strabo_fn, "[strabo]", _BLUE)
                last_restart["strabo"] = now

            if restart_corax and now - last_restart["corax"] > _DEBOUNCE:
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
    strabo_port = _parse_port(args, "--strabo-port", 8765)
    corax_port  = _parse_port(args, "--corax-port",  8766)
    no_watch    = "--no-watch" in args

    py = sys.executable

    print(
        f"\n{_BOLD}Cursus{_RESET}  "
        f"{_BLUE}Strabo :{strabo_port}{_RESET}  "
        f"{_TEAL}Corax :{corax_port}{_RESET}"
        + (f"  {_GREY}(watching for changes){_RESET}" if not no_watch else "")
        + f"\n{_GREY}Press Ctrl+C to stop both.{_RESET}\n"
    )

    strabo_fn = lambda: _start_strabo(py, strabo_port)
    corax_fn  = lambda: _start_corax(py, corax_port)

    strabo_holder: list = [strabo_fn()]
    corax_holder:  list = [corax_fn()]

    def _shutdown(sig=None, _frame=None) -> None:
        print(f"\n{_BOLD}[cursus]{_RESET} shutting down…")
        for h in (strabo_holder, corax_holder):
            p = h[0]
            if p:
                try:
                    p.terminate()
                except OSError:
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

    # If either process exits unexpectedly, bring down everything
    def _watch_exit(holder: list, name: str) -> None:
        while True:
            p = holder[0]
            if p:
                p.wait()
                if p.returncode not in (0, -15):  # -15 = SIGTERM (our own shutdown)
                    print(
                        f"\n{_BOLD}[cursus]{_RESET} {name} exited "
                        f"(code {p.returncode}) — stopping all."
                    )
                    _shutdown()
            time.sleep(0.5)

    threading.Thread(target=_watch_exit, args=(strabo_holder, "strabo"), daemon=True).start()
    threading.Thread(target=_watch_exit, args=(corax_holder,  "corax"),  daemon=True).start()

    if not no_watch:
        threading.Thread(
            target=_watch_loop,
            args=(strabo_holder, corax_holder, strabo_fn, corax_fn),
            daemon=True,
        ).start()

    strabo_holder[0].wait()
    corax_holder[0].wait()


if __name__ == "__main__":
    main()
