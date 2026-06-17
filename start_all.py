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
]

RESET = "\033[0m"


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
    start_all()