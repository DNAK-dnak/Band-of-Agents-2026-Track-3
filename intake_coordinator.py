"""
Transaction Intake Coordinator — Full Pipeline
================================================
KEY FIXES IN THIS VERSION:
  1. DEDUP: Track processed verdict message IDs — never mark completed twice,
     never overwrite a "completed" status with "timeout".
  2. ONE ROOM PER TRANSACTION: coordinator tracks which room_id belongs to
     which tx, ignores messages from other rooms entirely.
  3. SEQUENTIAL LOCK: only one transaction runs at a time. The previous
     version could resume an "interrupted" tx while a new one was running.
  4. VERDICT WRITTEN TO results.csv ONCE: guarded by a per-tx flag.
  5. Decision Agent re-pinging Policy is a prompt issue — added a note
     in the Decision Agent system prompt section (fix in decision_agent.py).
  6. Stall clock uses seen-message high-water mark (never goes backwards).
  7. Sender matching tries all known API field shapes.
"""

import asyncio
import csv
import logging
import os
import re
import shutil
import signal
from datetime import datetime, timezone

import httpx
import yaml
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────
BAND_BASE_URL = "https://app.thenvoi.com/api/v1/agent"
CSV_PATH = "transactions.csv"
RESULTS_PATH = "results.csv"
AGENT_CONFIG_PATH = "agent_config.yaml"

POLL_INTERVAL = 20
RESULT_POLL_INTERVAL = 15
RESULT_TIMEOUT = 600        # increased to 10 min — pipeline can be slow
DELAY_BETWEEN_TX = 20       # cooldown between transactions
AGENT_WARMUP_DELAY = 15
DELETE_ROOMS_AFTER = True

STALL_DETECT_INTERVAL = 90  # increased — pipeline legitimately takes time
MAX_REPINGS = 2             # reduced — fewer repings = fewer duplicates

COORDINATOR_KEY = "decision-maker"

_active_rooms: set[str] = set()
_shutdown_event = asyncio.Event()

# ── Verdict keywords ───────────────────────────────────────────
VERDICT_KEYWORDS = [
    "AUTO-APPROVE", "AUTO APPROVE",
    "ENHANCED REVIEW", "ENHANCED-REVIEW",
    "ESCALATE TO HUMAN", "ESCALATE",
    "DECLINE", "DECLINED", "BLOCKED",
]


def extract_verdict(text: str) -> str:
    upper = text.upper()
    for pattern in [
        r"RECOMMENDATION:\s*([A-Z][A-Z \-]+)",
        r"FINAL RECOMMENDATION:\s*([A-Z][A-Z \-]+)",
        r"OVERALL RECOMMENDATION:\s*([A-Z][A-Z \-]+)",
        r"DECISION:\s*([A-Z][A-Z \-]+)",
        r"VERDICT:\s*([A-Z][A-Z \-]+)",
    ]:
        m = re.search(pattern, upper)
        if m:
            found = m.group(1).strip()
            for kw in VERDICT_KEYWORDS:
                if kw in found:
                    return kw
    for kw in VERDICT_KEYWORDS:
        if kw in upper:
            return kw
    return "UNKNOWN"


def is_decision_message(msg: dict, decision_id: str) -> bool:
    """Try every known field shape the Band API might use for sender."""
    sender = msg.get("sender") or {}
    if sender.get("id") == decision_id:
        return True
    if sender.get("agent_id") == decision_id:
        return True
    name = (sender.get("name") or sender.get("display_name") or "").lower()
    if "decision" in name:
        return True
    if msg.get("participant_id") == decision_id:
        return True
    if msg.get("agent_id") == decision_id:
        return True
    return False


# ── Agent config ───────────────────────────────────────────────
def load_agent_configs() -> dict:
    with open(AGENT_CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── CSV helpers ────────────────────────────────────────────────
FIELDNAMES = ["id", "status", "description", "room_id", "verdict",
              "submitted_at", "completed_at"]


def ensure_csv_exists():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()


def load_transactions() -> list[dict]:
    ensure_csv_exists()
    with open(CSV_PATH, "r") as f:
        return list(csv.DictReader(f))


def save_transactions(txs: list[dict]):
    tmp = CSV_PATH + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for tx in txs:
            w.writerow({k: tx.get(k, "") for k in FIELDNAMES})
    shutil.move(tmp, CSV_PATH)


def update_transaction(tx_id: str, **updates):
    """
    Update fields — but NEVER downgrade a completed transaction.
    If status is already 'completed', ignore any attempt to set it to
    'timeout', 'failed', etc.
    """
    txs = load_transactions()
    for tx in txs:
        if str(tx["id"]) == str(tx_id):
            # Guard: don't overwrite a completed verdict with timeout
            if tx.get("status") == "completed" and updates.get("status") in (
                "timeout", "failed", "processing", "submitted", "interrupted"
            ):
                logger.warning(
                    f"  ⚠ Ignoring status downgrade for #{tx_id}: "
                    f"completed → {updates.get('status')}"
                )
                return
            tx.update(updates)
    save_transactions(txs)


def is_already_completed(tx_id: str) -> bool:
    """Check if a transaction is already marked completed in CSV."""
    for tx in load_transactions():
        if str(tx["id"]) == str(tx_id):
            return tx.get("status") == "completed"
    return False


def append_result(tx_id, description, verdict, room_id):
    """Append to results.csv — only called once per transaction."""
    file_exists = os.path.exists(RESULTS_PATH)
    with open(RESULTS_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "id", "description", "verdict", "room_id", "completed_at"])
        if not file_exists:
            w.writeheader()
        w.writerow({
            "id": tx_id,
            "description": (description[:100] + "...") if len(description) > 100 else description,
            "verdict": verdict,
            "room_id": room_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })


# ── Band API helpers ───────────────────────────────────────────
async def band_create_room(client, headers, title) -> str | None:
    resp = await client.post(f"{BAND_BASE_URL}/chats", headers=headers, json={"chat": {}})
    if resp.status_code in (200, 201):
        room_id = resp.json().get("data", {}).get("id")
        _active_rooms.add(room_id)
        logger.info(f"  ✓ Room created: {room_id}")
        return room_id
    logger.error(f"  ✗ Room creation failed: {resp.status_code} — {resp.text}")
    return None


async def band_add_participant(client, headers, room_id, agent_id) -> bool:
    resp = await client.post(
        f"{BAND_BASE_URL}/chats/{room_id}/participants",
        headers=headers,
        json={"participant": {"participant_id": agent_id}},
    )
    return resp.status_code in (200, 201, 409)


async def band_send_message(client, headers, room_id, content, mention_id) -> bool:
    resp = await client.post(
        f"{BAND_BASE_URL}/chats/{room_id}/messages",
        headers=headers,
        json={"message": {
            "content": f"@[[{mention_id}]] {content}",
            "mentions": [{"id": mention_id}],
        }},
    )
    if resp.status_code != 201:
        logger.warning(f"  ✗ send_message failed: {resp.status_code} — {resp.text[:200]}")
    return resp.status_code == 201


async def band_get_messages(client, headers, room_id) -> list:
    resp = await client.get(
        f"{BAND_BASE_URL}/chats/{room_id}/messages",
        headers=headers,
        params={"page": 1, "page_size": 100},
    )
    msgs = resp.json().get("data", []) if resp.status_code == 200 else []
    
    resp_proc = await client.get(
        f"{BAND_BASE_URL}/chats/{room_id}/messages",
        headers=headers,
        params={"page": 1, "page_size": 100, "status": "processed"},
    )
    if resp_proc.status_code == 200:
        msgs.extend(resp_proc.json().get("data", []))
        
    return msgs



async def band_delete_room(client, headers, room_id) -> bool:
    resp = await client.delete(f"{BAND_BASE_URL}/chats/{room_id}", headers=headers)
    if resp.status_code in (200, 204):
        _active_rooms.discard(room_id)
        logger.info(f"  🗑  Room deleted: {room_id}")
        return True
    # Fallback: remove participants
    logger.info(f"  ℹ  Removing participants from room {room_id}...")
    part = await client.get(f"{BAND_BASE_URL}/chats/{room_id}/participants", headers=headers)
    if part.status_code == 200:
        for p in part.json().get("data", []):
            if p.get("type") == "Agent":
                await client.delete(
                    f"{BAND_BASE_URL}/chats/{room_id}/participants/{p['id']}",
                    headers=headers,
                )
                await asyncio.sleep(0.3)
    _active_rooms.discard(room_id)
    logger.info(f"  🧹 Room cleared: {room_id}")
    return True


async def shutdown_cleanup(client, headers):
    if not _active_rooms:
        logger.info("  ✓ No active rooms.")
        return
    logger.info(f"  🧹 Cleaning {len(_active_rooms)} room(s)...")
    for room_id in list(_active_rooms):
        try:
            await band_delete_room(client, headers, room_id)
        except Exception as e:
            logger.error(f"  ✗ {room_id}: {e}")
    try:
        txs = load_transactions()
        changed = False
        for tx in txs:
            if tx.get("status") in ("submitted", "processing"):
                tx["status"] = "interrupted"
                changed = True
        if changed:
            save_transactions(txs)
    except Exception as e:
        logger.error(f"  ✗ CSV update failed: {e}")


# ── Core pipeline ──────────────────────────────────────────────
async def submit_transaction(client, headers, configs, tx) -> str | None:
    tx_id = tx["id"]
    description = tx["description"]

    logger.info(f"{'═'*60}")
    logger.info(f"  Transaction #{tx_id}")
    logger.info(f"  {description[:80]}...")
    logger.info(f"{'═'*60}")

    room_id = await band_create_room(client, headers,
        f"Review #{tx_id} — {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    if not room_id:
        update_transaction(tx_id, status="failed")
        return None

    for name, config in configs.items():
        ok = await band_add_participant(client, headers, room_id, config["agent_id"])
        logger.info(f"  {'✓' if ok else '✗'} Added {name}")
        await asyncio.sleep(1)

    logger.info(f"  ⏳ Warmup {AGENT_WARMUP_DELAY}s...")
    await asyncio.sleep(AGENT_WARMUP_DELAY)

    ok = await band_send_message(
        client, headers, room_id,
        f"Review this transaction: {description}",
        configs["policy-analyst"]["agent_id"],
    )
    if ok:
        update_transaction(tx_id, status="submitted", room_id=room_id,
                           submitted_at=datetime.now(timezone.utc).isoformat())
        logger.info(f"  ✓ Submitted to Policy Agent")
        return room_id

    update_transaction(tx_id, status="failed")
    return None


async def wait_for_result(client, headers, configs, tx_id, room_id, description) -> str:
    """
    Poll until Decision Agent posts a verdict.

    KEY DEDUP LOGIC:
    - seen_verdict_msg_ids: once we record a verdict from a message ID,
      we never re-process it. This prevents double-write to results.csv.
    - is_already_completed(): before writing anything, re-check CSV.
      Guards against the coordinator loop picking up the same tx twice.
    - Status is never downgraded from 'completed'.
    """
    decision_id = configs["decision-maker"]["agent_id"]
    policy_id = configs["policy-analyst"]["agent_id"]

    update_transaction(tx_id, status="processing")
    logger.info(f"  ⏳ Waiting (timeout {RESULT_TIMEOUT}s, stall {STALL_DETECT_INTERVAL}s)...")

    loop = asyncio.get_event_loop()
    start = loop.time()

    seen_msg_ids: set[str] = set()       # all messages ever seen
    seen_verdict_msg_ids: set[str] = set()  # messages we extracted a verdict from
    hwm = 0                              # high-water mark of seen messages
    last_new_msg_time = loop.time()
    repings = 0
    result_written = False               # guard against double-write

    while not _shutdown_event.is_set():
        elapsed = loop.time() - start
        if elapsed > RESULT_TIMEOUT:
            # Last chance: re-check if completed during this run
            if is_already_completed(tx_id):
                logger.info(f"  ✓ #{tx_id} was completed (detected at timeout check)")
                return "COMPLETED_EARLIER"
            logger.warning(f"  ⚠ Timeout #{tx_id}")
            update_transaction(tx_id, status="timeout")
            return "TIMEOUT"

        messages = await band_get_messages(client, headers, room_id)

        # ── Log new messages ───────────────────────────────────
        for msg in messages:
            mid = msg.get("id", "")
            if mid and mid not in seen_msg_ids:
                seen_msg_ids.add(mid)
                sender = msg.get("sender") or {}
                sname = (sender.get("name") or sender.get("display_name")
                         or msg.get("participant_id", "?"))
                sid = (sender.get("id") or sender.get("agent_id")
                       or msg.get("agent_id") or "")
                mtype = msg.get("message_type", "text")
                snippet = msg.get("content", "")[:120].replace("\n", " ")
                logger.info(f"  📨 [{sname}] ({mtype}) {snippet}")
                if sid:
                    logger.debug(f"      sender_id={sid}")

        # ── Verdict detection ──────────────────────────────────
        if not result_written and not is_already_completed(tx_id):
            for msg in messages:
                mid = msg.get("id", "")
                if mid in seen_verdict_msg_ids:
                    continue
                msg_type = msg.get("message_type", "text")
                if msg_type not in ("text", "task", "thought"):
                    continue
                content = msg.get("content", "")
                if not content:
                    continue

                is_decision = is_decision_message(msg, decision_id)

                # Skip very short messages, unless from the Decision Agent or of type 'task'
                if len(content) < 100 and not is_decision and msg_type != "task":
                    continue

                verdict = extract_verdict(content)
                if verdict == "UNKNOWN":
                    continue

                # Accept if from Decision Agent (by ID or name)
                # OR any long message with clear verdict keywords
                # (covers sender ID mismatch cases)
                if is_decision or msg_type == "task" or len(content) > 400:
                    seen_verdict_msg_ids.add(mid)
                    result_written = True
                    source = "Decision Agent" if is_decision else f"{msg_type}-match"
                    logger.info(f"  ✅ Verdict [{source}]: {verdict}")
                    update_transaction(
                        tx_id, status="completed", verdict=verdict,
                        completed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    append_result(tx_id, description, verdict, room_id)
                    return verdict

        # ── Stall detection ────────────────────────────────────
        current = len(seen_msg_ids)
        if current > hwm:
            hwm = current
            last_new_msg_time = loop.time()

        silence = loop.time() - last_new_msg_time
        if silence >= STALL_DETECT_INTERVAL and repings < MAX_REPINGS:
            repings += 1
            logger.warning(f"  ⚠ Stall ({int(silence)}s) — re-ping #{repings}/{MAX_REPINGS}")
            await band_send_message(
                client, headers, room_id,
                f"[RE-PING #{repings}] Please review this transaction: {description}",
                policy_id,
            )
            last_new_msg_time = loop.time()
        elif silence >= STALL_DETECT_INTERVAL and repings >= MAX_REPINGS:
            if is_already_completed(tx_id):
                logger.info(f"  ✓ #{tx_id} completed (found during stall check)")
                return "COMPLETED_EARLIER"
            logger.error(f"  ✗ Stalled after {MAX_REPINGS} re-pings")
            update_transaction(tx_id, status="timeout")
            return "TIMEOUT"

        mins, secs = int(elapsed // 60), int(elapsed % 60)
        logger.info(f"  ⏳ {mins}m{secs}s | {current} msgs | {repings} re-pings")
        await asyncio.sleep(RESULT_POLL_INTERVAL)

    return "INTERRUPTED"


async def process_transaction(client, headers, configs, tx):
    tx_id = tx["id"]
    description = tx["description"]

    # Skip if already completed from a previous run
    if is_already_completed(tx_id):
        logger.info(f"  ⏭ Transaction #{tx_id} already completed — skipping")
        return

    room_id = await submit_transaction(client, headers, configs, tx)
    if not room_id:
        return

    verdict = await wait_for_result(client, headers, configs, tx_id, room_id, description)

    if verdict == "INTERRUPTED":
        logger.info(f"  ↩ #{tx_id} interrupted")
        return

    if DELETE_ROOMS_AFTER:
        await band_delete_room(client, headers, room_id)

    logger.info(f"  Transaction #{tx_id} → {verdict}")
    logger.info("")


# ── Main loop ──────────────────────────────────────────────────
async def main():
    ensure_csv_exists()
    configs = load_agent_configs()
    headers = {"X-API-Key": configs[COORDINATOR_KEY]["api_key"]}

    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║  Financial Compliance Pipeline — Coordinator    ║")
    logger.info("╠══════════════════════════════════════════════════╣")
    logger.info(f"║  Input:    {CSV_PATH:<38}║")
    logger.info(f"║  Output:   {RESULTS_PATH:<38}║")
    logger.info(f"║  Timeout:  {RESULT_TIMEOUT}s | Stall: {STALL_DETECT_INTERVAL}s{'':<17}║")
    logger.info(f"║  Re-pings: max {MAX_REPINGS} | Warmup: {AGENT_WARMUP_DELAY}s{'':<21}║")
    logger.info("╚══════════════════════════════════════════════════╝")
    logger.info("")

    async with httpx.AsyncClient(timeout=30) as client:
        loop = asyncio.get_event_loop()

        def _sigint():
            if not _shutdown_event.is_set():
                logger.info("⚡ Ctrl+C — shutting down...")
                _shutdown_event.set()

        loop.add_signal_handler(signal.SIGINT, _sigint)
        loop.add_signal_handler(signal.SIGTERM, _sigint)

        try:
            while not _shutdown_event.is_set():
                try:
                    txs = load_transactions()

                    # Process one pending transaction at a time (sequential lock)
                    pending = [tx for tx in txs if tx.get("status") == "pending"]
                    if pending:
                        tx = pending[0]  # one at a time
                        logger.info(f"📋 Processing transaction #{tx['id']}")
                        await process_transaction(client, headers, configs, tx)
                        if not _shutdown_event.is_set():
                            logger.info(f"  ⏳ Cooldown {DELAY_BETWEEN_TX}s before next tx...")
                            await asyncio.sleep(DELAY_BETWEEN_TX)
                        continue  # re-read CSV, pick up next pending

                    # Resume interrupted (but not timed-out) transactions
                    interrupted = [
                        tx for tx in txs
                        if tx.get("status") == "interrupted" and tx.get("room_id")
                    ]
                    for tx in interrupted:
                        if _shutdown_event.is_set():
                            break
                        if is_already_completed(tx["id"]):
                            continue
                        logger.info(f"📋 Resuming #{tx['id']}...")
                        verdict = await wait_for_result(
                            client, headers, configs,
                            tx["id"], tx["room_id"], tx["description"],
                        )
                        if verdict not in ("TIMEOUT", "UNKNOWN", "INTERRUPTED", "COMPLETED_EARLIER"):
                            if DELETE_ROOMS_AFTER:
                                await band_delete_room(client, headers, tx["room_id"])

                except Exception as e:
                    logger.error(f"Main loop error: {e}", exc_info=True)

                if not _shutdown_event.is_set():
                    await asyncio.sleep(POLL_INTERVAL)

        finally:
            logger.info("═" * 50)
            logger.info("Shutdown cleanup...")
            await shutdown_cleanup(client, headers)
            logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())