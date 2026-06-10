"""
Cursus — starts Strabo and Corax together.

Named after the cursus publicus, Rome's imperial relay network that kept all
communications moving across the empire.

Usage:
    venv/bin/python run_cursus.py
    venv/bin/python run_cursus.py --strabo-port 8765 --corax-port 8766
"""
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path

_HERE = Path(__file__).parent

_BOLD  = "\033[1m"
_RESET = "\033[0m"
_BLUE  = "\033[34m"
_TEAL  = "\033[36m"
_GREY  = "\033[90m"


def _stream(proc: subprocess.Popen, label: str, color: str) -> None:
    for raw in proc.stdout:
        sys.stdout.write(f"{color}{_BOLD}{label}{_RESET} {raw.decode(errors='replace')}")
        sys.stdout.flush()


def _parse_port(args: list[str], flag: str, default: int) -> int:
    try:
        return int(args[args.index(flag) + 1])
    except (ValueError, IndexError):
        return default


def main() -> None:
    args = sys.argv[1:]
    strabo_port = _parse_port(args, "--strabo-port", 8765)
    corax_port  = _parse_port(args, "--corax-port",  8766)

    py = sys.executable

    print(
        f"\n{_BOLD}Cursus{_RESET}  "
        f"{_BLUE}Strabo :{strabo_port}{_RESET}  "
        f"{_TEAL}Corax :{corax_port}{_RESET}\n"
        f"{_GREY}Press Ctrl+C to stop both.{_RESET}\n"
    )

    strabo = subprocess.Popen(
        [py, str(_HERE / "run_strabo.py"), "--port", str(strabo_port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )
    corax = subprocess.Popen(
        [py, str(_HERE / "run_corax.py"), "--port", str(corax_port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_HERE),
    )

    procs = [strabo, corax]

    def _shutdown(sig=None, _frame=None) -> None:
        print(f"\n{_BOLD}[cursus]{_RESET} shutting down…")
        for p in procs:
            try:
                p.terminate()
            except OSError:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    threading.Thread(
        target=_stream, args=(strabo, "[strabo]", _BLUE), daemon=True
    ).start()
    threading.Thread(
        target=_stream, args=(corax,  "[corax] ", _TEAL), daemon=True
    ).start()

    # If either process exits unexpectedly, bring down the other
    def _watch(proc: subprocess.Popen, name: str) -> None:
        proc.wait()
        if proc.returncode not in (0, -15):  # -15 = SIGTERM (our own shutdown)
            print(
                f"\n{_BOLD}[cursus]{_RESET} {name} exited "
                f"(code {proc.returncode}) — stopping all."
            )
            _shutdown()

    threading.Thread(target=_watch, args=(strabo, "strabo"), daemon=True).start()
    threading.Thread(target=_watch, args=(corax,  "corax"),  daemon=True).start()

    strabo.wait()
    corax.wait()


if __name__ == "__main__":
    main()
