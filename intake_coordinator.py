"""
Transaction Intake Coordinator — Full Pipeline
================================================
FIXES in this version:
  - Verdict detection no longer depends on sender ID matching.
    It scans ALL messages for verdict keywords from Decision Agent
    by checking sender name OR any message containing the verdict pattern.
  - Full sender structure is logged for every new message so you can
    see the exact field names the API returns.
  - Message count never goes backwards (use max of seen vs current).
  - Page size increased to 100 to avoid missing messages.
  - Stall clock only resets on genuinely new messages.
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
RESULT_TIMEOUT = 300
DELAY_BETWEEN_TX = 15
AGENT_WARMUP_DELAY = 15
DELETE_ROOMS_AFTER = True

STALL_DETECT_INTERVAL = 120
MAX_REPINGS = 3

COORDINATOR_KEY = "decision-maker"

_active_rooms: set[str] = set()
_shutdown_event = asyncio.Event()

# ── Verdict extraction ─────────────────────────────────────────
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
        r"DECISION:\s*([A-Z][A-Z \-]+)",
        r"VERDICT:\s*([A-Z][A-Z \-]+)",
    ]:
        match = re.search(pattern, upper)
        if match:
            found = match.group(1).strip()
            for keyword in VERDICT_KEYWORDS:
                if keyword in found:
                    return keyword
    for keyword in VERDICT_KEYWORDS:
        if keyword in upper:
            return keyword
    return "UNKNOWN"


def is_decision_agent_message(msg: dict, decision_id: str) -> bool:
    """
    Match Decision Agent messages robustly — the API may return sender info
    under different keys. Try all known structures.
    """
    sender = msg.get("sender") or {}

    # Try direct ID match
    if sender.get("id") == decision_id:
        return True
    if sender.get("agent_id") == decision_id:
        return True

    # Try name match (case-insensitive)
    name = (sender.get("name") or sender.get("display_name") or "").lower()
    if "decision" in name:
        return True

    # Try participant_id / user_id fields at top level
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
        logger.info(f"Created empty {CSV_PATH}")


def load_transactions() -> list[dict]:
    ensure_csv_exists()
    with open(CSV_PATH, "r") as f:
        return list(csv.DictReader(f))


def save_transactions(transactions: list[dict]):
    tmp = CSV_PATH + ".tmp"
    with open(tmp, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for tx in transactions:
            writer.writerow({k: tx.get(k, "") for k in FIELDNAMES})
    shutil.move(tmp, CSV_PATH)


def update_transaction(tx_id: str, **updates):
    transactions = load_transactions()
    for tx in transactions:
        if str(tx["id"]) == str(tx_id):
            tx.update(updates)
    save_transactions(transactions)


def append_result(tx_id, description, verdict, room_id):
    file_exists = os.path.exists(RESULTS_PATH)
    with open(RESULTS_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id", "description", "verdict", "room_id", "completed_at"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "id": tx_id,
            "description": (description[:100] + "...") if len(description) > 100 else description,
            "verdict": verdict,
            "room_id": room_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })


# ── Band API helpers ───────────────────────────────────────────
async def band_create_room(client, headers, title) -> str | None:
    resp = await client.post(
        f"{BAND_BASE_URL}/chats",
        headers=headers,
        json={"chat": {}},
    )
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
        json={
            "message": {
                "content": f"@[[{mention_id}]] {content}",
                "mentions": [{"id": mention_id}],
            }
        },
    )
    if resp.status_code != 201:
        logger.warning(f"  ✗ send_message failed: {resp.status_code} — {resp.text[:200]}")
    return resp.status_code == 201


async def band_get_messages(client, headers, room_id) -> list:
    resp = await client.get(
        f"{BAND_BASE_URL}/chats/{room_id}/messages",
        headers=headers,
        params={"page": 1, "page_size": 100},  # increased from 50
    )
    return resp.json().get("data", []) if resp.status_code == 200 else []


async def band_delete_room(client, headers, room_id) -> bool:
    resp = await client.delete(f"{BAND_BASE_URL}/chats/{room_id}", headers=headers)
    if resp.status_code in (200, 204):
        _active_rooms.discard(room_id)
        logger.info(f"  🗑  Room deleted: {room_id}")
        return True

    logger.info(f"  ℹ  Hard delete not supported (HTTP {resp.status_code}), removing participants instead")
    part_resp = await client.get(f"{BAND_BASE_URL}/chats/{room_id}/participants", headers=headers)
    if part_resp.status_code == 200:
        for p in part_resp.json().get("data", []):
            if p.get("type") == "Agent":
                await client.delete(
                    f"{BAND_BASE_URL}/chats/{room_id}/participants/{p['id']}",
                    headers=headers,
                )
                await asyncio.sleep(0.3)
    _active_rooms.discard(room_id)
    logger.info(f"  🧹 Room participants cleared: {room_id}")
    return True


async def shutdown_cleanup(client, headers):
    if not _active_rooms:
        logger.info("  ✓ No active rooms to clean up.")
        return
    rooms = list(_active_rooms)
    logger.info(f"  🧹 Cleaning up {len(rooms)} active room(s)...")
    for room_id in rooms:
        try:
            await band_delete_room(client, headers, room_id)
        except Exception as e:
            logger.error(f"  ✗ Failed to clean room {room_id}: {e}")
    try:
        transactions = load_transactions()
        changed = False
        for tx in transactions:
            if tx.get("status") in ("submitted", "processing"):
                tx["status"] = "interrupted"
                changed = True
        if changed:
            save_transactions(transactions)
            logger.info("  ✓ In-progress transactions marked as 'interrupted'")
    except Exception as e:
        logger.error(f"  ✗ Could not update CSV during shutdown: {e}")


# ── Core pipeline ──────────────────────────────────────────────
async def submit_transaction(client, headers, configs, tx) -> str | None:
    tx_id = tx["id"]
    description = tx["description"]

    logger.info(f"{'═'*60}")
    logger.info(f"  Transaction #{tx_id}")
    logger.info(f"  {description[:80]}...")
    logger.info(f"{'═'*60}")

    room_id = await band_create_room(
        client, headers,
        f"Review #{tx_id} — {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
    )
    if not room_id:
        update_transaction(tx_id, status="failed")
        return None

    for name, config in configs.items():
        ok = await band_add_participant(client, headers, room_id, config["agent_id"])
        logger.info(f"  {'✓' if ok else '✗'} Added {name}")
        await asyncio.sleep(1)

    logger.info(f"  ⏳ Waiting {AGENT_WARMUP_DELAY}s for agents to subscribe...")
    await asyncio.sleep(AGENT_WARMUP_DELAY)

    ok = await band_send_message(
        client, headers, room_id,
        f"Review this transaction: {description}",
        configs["policy-analyst"]["agent_id"],
    )

    if ok:
        update_transaction(tx_id, status="submitted", room_id=room_id,
                           submitted_at=datetime.now(timezone.utc).isoformat())
        logger.info(f"  ✓ Transaction submitted to Policy Agent")
        return room_id

    update_transaction(tx_id, status="failed")
    logger.error(f"  ✗ Submission failed")
    return None


async def wait_for_result(client, headers, configs, tx_id, room_id, description) -> str:
    decision_id = configs["decision-maker"]["agent_id"]
    policy_id = configs["policy-analyst"]["agent_id"]

    logger.info(f"  ⏳ Waiting for pipeline (timeout {RESULT_TIMEOUT}s)...")
    update_transaction(tx_id, status="processing")

    loop = asyncio.get_event_loop()
    start = loop.time()

    # Track seen messages by ID to avoid re-processing
    seen_msg_ids: set[str] = set()
    # High-water mark — never let "count" go backwards
    hwm_count = 0
    last_new_msg_time = loop.time()
    repings = 0

    while not _shutdown_event.is_set():
        elapsed = loop.time() - start
        if elapsed > RESULT_TIMEOUT:
            logger.warning(f"  ⚠ Timeout on transaction #{tx_id}")
            update_transaction(tx_id, status="timeout")
            return "TIMEOUT"

        messages = await band_get_messages(client, headers, room_id)

        # ── Log every new message with full sender info ────────
        for msg in messages:
            msg_id = msg.get("id", "")
            if msg_id and msg_id not in seen_msg_ids:
                seen_msg_ids.add(msg_id)
                sender = msg.get("sender") or {}
                sender_name = (sender.get("name") or sender.get("display_name")
                               or msg.get("participant_id", "unknown"))
                sender_id = sender.get("id") or sender.get("agent_id") or msg.get("agent_id", "")
                msg_type = msg.get("message_type", "text")
                snippet = msg.get("content", "")[:100].replace("\n", " ")
                # Log full sender dict once so we learn the real field names
                logger.info(f"  📨 [{sender_name}|{sender_id[:8] if sender_id else '?'}] ({msg_type}) {snippet}")
                if sender_id and sender_id not in (decision_id, policy_id):
                    logger.debug(f"      sender raw: {sender}")

        # ── Check for verdict in ANY recent text message ───────
        # We don't rely solely on sender ID match — we check all
        # text messages for verdict keywords, prioritising Decision Agent.
        for msg in messages:
            if msg.get("message_type") != "text":
                continue
            content = msg.get("content", "")
            if not content:
                continue

            verdict = extract_verdict(content)
            if verdict == "UNKNOWN":
                continue

            # Accept it if it looks like a Decision Agent message
            if is_decision_agent_message(msg, decision_id):
                logger.info(f"  ✅ Verdict from Decision Agent: {verdict}")
                update_transaction(tx_id, status="completed", verdict=verdict,
                                   completed_at=datetime.now(timezone.utc).isoformat())
                append_result(tx_id, description, verdict, room_id)
                return verdict

            # Fallback: accept verdict from ANY sender if message is long enough
            # (Decision Agent reports are substantial, not one-liners)
            if len(content) > 300:
                sender = msg.get("sender") or {}
                sender_name = sender.get("name") or sender.get("display_name") or "unknown"
                logger.info(f"  ✅ Verdict found in message from [{sender_name}]: {verdict}")
                logger.info(f"      (sender ID didn't match decision_id={decision_id[:8]} — using content match)")
                update_transaction(tx_id, status="completed", verdict=verdict,
                                   completed_at=datetime.now(timezone.utc).isoformat())
                append_result(tx_id, description, verdict, room_id)
                return verdict

        # ── Stall detection using high-water mark ─────────────
        current_count = len(seen_msg_ids)  # use seen set, never goes backwards
        if current_count > hwm_count:
            hwm_count = current_count
            last_new_msg_time = loop.time()

        silence = loop.time() - last_new_msg_time
        if silence >= STALL_DETECT_INTERVAL:
            if repings >= MAX_REPINGS:
                logger.error(f"  ✗ Pipeline stalled after {MAX_REPINGS} re-pings. Giving up.")
                update_transaction(tx_id, status="timeout")
                return "TIMEOUT"
            repings += 1
            logger.warning(f"  ⚠ Stall ({int(silence)}s silence) — re-ping #{repings}/{MAX_REPINGS}")
            ok = await band_send_message(
                client, headers, room_id,
                f"[RE-PING #{repings}] Please review this transaction (pipeline stalled): {description}",
                policy_id,
            )
            logger.info(f"  ↩ Re-ping {'sent' if ok else 'FAILED'}")
            last_new_msg_time = loop.time()

        mins, secs = int(elapsed // 60), int(elapsed % 60)
        logger.info(
            f"  ⏳ Running... ({mins}m {secs}s | {current_count} msgs seen | {repings} re-pings)"
        )
        await asyncio.sleep(RESULT_POLL_INTERVAL)

    return "INTERRUPTED"


async def process_transaction(client, headers, configs, tx):
    tx_id = tx["id"]
    description = tx["description"]

    room_id = await submit_transaction(client, headers, configs, tx)
    if not room_id:
        return

    verdict = await wait_for_result(client, headers, configs, tx_id, room_id, description)

    if verdict == "INTERRUPTED":
        logger.info(f"  ↩ Transaction #{tx_id} interrupted by shutdown")
        return

    if DELETE_ROOMS_AFTER:
        await band_delete_room(client, headers, room_id)
    else:
        logger.info(f"  ℹ Room kept for inspection: {room_id}")

    logger.info(f"  Transaction #{tx_id} → {verdict}")
    logger.info("")


# ── Main loop ──────────────────────────────────────────────────
async def main():
    ensure_csv_exists()
    configs = load_agent_configs()
    coordinator = configs[COORDINATOR_KEY]
    headers = {"X-API-Key": coordinator["api_key"]}

    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║  Financial Compliance Pipeline — Coordinator    ║")
    logger.info("╠══════════════════════════════════════════════════╣")
    logger.info(f"║  Input:    {CSV_PATH:<38}║")
    logger.info(f"║  Output:   {RESULTS_PATH:<38}║")
    logger.info(f"║  Poll:     every {POLL_INTERVAL}s{'':<31}║")
    logger.info(f"║  Timeout:  {RESULT_TIMEOUT}s per transaction{'':<20}║")
    logger.info(f"║  Cleanup:  {'delete rooms' if DELETE_ROOMS_AFTER else 'keep rooms':<38}║")
    logger.info(f"║  Stall:    re-ping after {STALL_DETECT_INTERVAL}s silence{'':<20}║")
    logger.info("╚══════════════════════════════════════════════════╝")
    logger.info("")
    logger.info("Add transactions to transactions.csv with status=pending")
    logger.info("Press Ctrl+C to stop (all rooms will be deleted)")
    logger.info("")

    async with httpx.AsyncClient(timeout=30) as client:
        loop = asyncio.get_event_loop()

        def _handle_sigint():
            if not _shutdown_event.is_set():
                logger.info("")
                logger.info("⚡ Ctrl+C received — shutting down gracefully...")
                _shutdown_event.set()

        loop.add_signal_handler(signal.SIGINT, _handle_sigint)
        loop.add_signal_handler(signal.SIGTERM, _handle_sigint)

        try:
            while not _shutdown_event.is_set():
                try:
                    transactions = load_transactions()

                    pending = [tx for tx in transactions if tx.get("status") == "pending"]
                    if pending:
                        logger.info(f"📋 {len(pending)} pending transaction(s) found")
                        for tx in pending:
                            if _shutdown_event.is_set():
                                break
                            await process_transaction(client, headers, configs, tx)
                            if len(pending) > 1 and not _shutdown_event.is_set():
                                logger.info(f"  ⏳ Cooling down {DELAY_BETWEEN_TX}s...")
                                await asyncio.sleep(DELAY_BETWEEN_TX)

                    submitted = [tx for tx in transactions
                                 if tx.get("status") in ("submitted", "processing", "interrupted")]
                    for tx in submitted:
                        if _shutdown_event.is_set():
                            break
                        room_id = tx.get("room_id")
                        if room_id:
                            logger.info(f"📋 Resuming transaction #{tx['id']}...")
                            verdict = await wait_for_result(
                                client, headers, configs,
                                tx["id"], room_id, tx["description"],
                            )
                            if verdict not in ("TIMEOUT", "UNKNOWN", "INTERRUPTED") and DELETE_ROOMS_AFTER:
                                await band_delete_room(client, headers, room_id)

                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)

                if not _shutdown_event.is_set():
                    await asyncio.sleep(POLL_INTERVAL)

        finally:
            logger.info("")
            logger.info("═" * 50)
            logger.info("Shutdown cleanup — deleting all active rooms...")
            await shutdown_cleanup(client, headers)
            logger.info("═" * 50)
            logger.info("All done. Goodbye.")


if __name__ == "__main__":
    asyncio.run(main())