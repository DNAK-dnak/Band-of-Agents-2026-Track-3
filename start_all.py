"""
Start All Agents + Coordinator
================================
Launches all 4 compliance pipeline agents and the intake
coordinator as background processes. Stops everything on Ctrl+C.

Usage:
  python start_all.py

This replaces opening 5 separate terminals.
"""

import asyncio
import subprocess
import signal
import sys
import os
import time
import logging
import csv
import shutil

LOCK_FILE = ".pipeline.lock"

CSV_PATH = os.getenv("CSV_PATH", "transactions.csv")
RESULTS_PATH = os.getenv("RESULTS_PATH", "results.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

AGENTS = [
    {"name": "Policy Agent",   "script": "policy_agent.py",       "color": "\033[96m"},   # cyan
    {"name": "Risk Agent",     "script": "risk_agent.py",         "color": "\033[93m"},   # yellow
    {"name": "Legal Agent",    "script": "legal_agent.py",        "color": "\033[95m"},   # magenta
    {"name": "Decision Agent", "script": "decision_agent.py",     "color": "\033[92m"},   # green
    {"name": "Coordinator",    "script": "pipeline_ros2.py",      "color": "\033[97m"},   # white
    {"name": "Web Dashboard",  "script": "app.py",                "color": "\033[94m"},   # blue
]

RESET = "\033[0m"


def check_lock():
    """Abort if another instance is already running."""
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE) as f:
            old_pid = f.read().strip()
        # Check if that PID is still alive
        try:
            os.kill(int(old_pid), 0)
            print(f"\n[ERROR] Pipeline already running (PID {old_pid}).")
            print("        Run: kill " + old_pid + " or: pkill -f start_all.py")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            # Stale lock — old process is dead
            os.remove(LOCK_FILE)


def write_lock():
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))


def clear_lock():
    try:
        os.remove(LOCK_FILE)
    except FileNotFoundError:
        pass


def reset_csvs():
    """Wipe transactions and results CSVs back to headers only."""
    specs = [
        (CSV_PATH, ["id","user_id","status","description","room_id","verdict","submitted_at","completed_at"]),
        (RESULTS_PATH,      ["id","description","verdict","room_id","completed_at"]),
    ]
    for path, fields in specs:
        tmp = path + ".tmp"
        with open(tmp, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()
        shutil.move(tmp, path)
    print("  ✓ CSVs reset (transactions + results)")


def start_all():
    processes = []

    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║  Financial Compliance Pipeline — Launcher    ║")
    logger.info("╠══════════════════════════════════════════════╣")

    for agent in AGENTS:
        script = agent["script"]
        name = agent["name"]
        color = agent["color"]

        if not os.path.exists(script):
            logger.error(f"  ✗ {name}: {script} not found!")
            continue

        proc = subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
        )
        processes.append({"proc": proc, **agent})
        logger.info(f"  ✓ {color}{name}{RESET} started (PID {proc.pid})")

    logger.info("╠══════════════════════════════════════════════╣")
    logger.info("║  All processes running. Press Ctrl+C to stop ║")
    logger.info("╚══════════════════════════════════════════════╝")
    logger.info("")

    # Stream output from all processes with color-coded prefixes
    def stream_output():
        import selectors
        sel = selectors.DefaultSelector()

        for p in processes:
            if p["proc"].stdout:
                sel.register(p["proc"].stdout, selectors.EVENT_READ, p)

        try:
            while processes:
                # Check if any process has died
                for p in list(processes):
                    ret = p["proc"].poll()
                    if ret is not None:
                        logger.warning(
                            f"{p['color']}[{p['name']}]{RESET} exited with code {ret}"
                        )
                        processes.remove(p)
                        if p["proc"].stdout:
                            try:
                                sel.unregister(p["proc"].stdout)
                            except KeyError:
                                pass

                events = sel.select(timeout=1)
                for key, _ in events:
                    p = key.data
                    line = key.fileobj.readline()
                    if line:
                        prefix = f"{p['color']}[{p['name']:<16}]{RESET}"
                        print(f"{prefix} {line.rstrip()}")

        except KeyboardInterrupt:
            pass
        finally:
            sel.close()

    try:
        stream_output()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("")
        logger.info("Shutting down all processes...")
        for p in processes:
            try:
                p["proc"].terminate()
                p["proc"].wait(timeout=5)
                logger.info(f"  ✓ {p['name']} stopped")
            except subprocess.TimeoutExpired:
                p["proc"].kill()
                logger.info(f"  ✓ {p['name']} killed")
            except Exception:
                pass
        logger.info("All processes stopped.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Financial Compliance Pipeline Launcher")
    parser.add_argument("--reset", action="store_true",
                        help="Clear transactions.csv and results.csv before starting")
    args = parser.parse_args()

    check_lock()   # Abort if already running

    if args.reset:
        print("\n  [--reset] Clearing CSVs...")
        reset_csvs()

    write_lock()   # Claim the lock
    try:
        start_all()
    finally:
        clear_lock()