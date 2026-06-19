"""
Financial Compliance Pipeline — Coordinator with Band Native Features
=====================================================================
Band features used:
  PEER DISCOVERY    — Discovers agents by name at startup (list_agent_peers)
  PARTICIPANT MGMT  — Adds/removes agents per room lifecycle
  ADAPTIVE POLLING  — Fast poll when active, slow when idle
  REST API          — Room create/destroy, message send, message poll

Flow:
  CSV → Coordinator → [Band room] → Policy→Risk→Legal→Decision → results.csv
  One transaction at a time. Room created fresh, destroyed when done.
"""

import asyncio
import csv
import logging
import os
import re
import shutil
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

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

# ══════════════════════════════════════════════════════════════
# Band API
# ══════════════════════════════════════════════════════════════
BAND_BASE_URL = "https://app.thenvoi.com/api/v1"
AGENT_API     = f"{BAND_BASE_URL}/agent"

# ══════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════
CSV_PATH           = os.getenv("CSV_PATH", "transactions.csv")
RESULTS_PATH       = os.getenv("RESULTS_PATH", "results.csv")
AGENT_CONFIG_PATH  = os.getenv("AGENT_CONFIG_PATH", "agent_config.yaml")

AGENT_WARMUP_DELAY     = 15   # initial warmup only (once at startup)
RESULT_TIMEOUT         = 600
STALL_DETECT_INTERVAL  = 180
MAX_REPINGS            = 1
DELAY_BETWEEN_TX       = 5    # short cooldown — no room teardown needed
CSV_POLL_INTERVAL      = 8

POLL_FAST   = 8
POLL_SLOW   = 20
RETRY_DELAY = 30   # seconds to wait before retrying a failed send

COORDINATOR_KEY = "decision-maker"

AGENT_ROLES = {
    "policy-analyst":  ["policy", "policy agent", "policy-agent"],
    "risk-analyst":    ["risk", "risk agent",   "risk-agent"],
    "legal-reviewer":  ["legal", "legal agent", "legal-agent"],
    "decision-maker":  ["decision", "decision agent", "decision-agent"],
}

VERDICT_KEYWORDS = [
    "AUTO-APPROVE", "AUTO APPROVE",
    "ENHANCED REVIEW", "ENHANCED-REVIEW",
    "ESCALATE TO HUMAN", "ESCALATE",
    "DECLINE", "DECLINED", "BLOCKED",
]

_shutdown_event  = asyncio.Event()
_active_room_id: Optional[str] = None


# ══════════════════════════════════════════════════════════════
# Message types
# ══════════════════════════════════════════════════════════════
@dataclass
class TransactionMsg:
    tx_id: str
    description: str
    room_id: str
    published_at: float = field(default_factory=time.monotonic)

@dataclass
class VerdictMsg:
    tx_id: str
    verdict: str
    room_id: str
    raw_content: str

class Topic:
    def __init__(self, name: str):
        self.name = name
        self._q: asyncio.Queue = asyncio.Queue(maxsize=1)

    async def publish(self, msg):
        if self._q.full():
            try: self._q.get_nowait()
            except asyncio.QueueEmpty: pass
        await self._q.put(msg)

    async def subscribe(self, timeout: float = None):
        try:
            return await asyncio.wait_for(self._q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


# ══════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════
def load_configs() -> dict:
    with open(AGENT_CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ══════════════════════════════════════════════════════════════
# BAND: Peer Discovery
# ══════════════════════════════════════════════════════════════
async def discover_agents(client, headers, room_id: str = None) -> dict[str, str]:
    params = {"page": 1, "page_size": 100}
    if room_id:
        params["not_in_chat"] = room_id

    resp = await client.get(f"{AGENT_API}/peers", headers=headers, params=params)
    if resp.status_code != 200:
        logger.warning(f"  ⚠ Peer discovery failed ({resp.status_code}), using config fallback")
        return {}

    peers = resp.json().get("data", [])
    discovered: dict[str, str] = {}

    for peer in peers:
        peer_id   = peer.get("id", "")
        peer_name = (peer.get("name") or peer.get("display_name") or "").lower()
        peer_type = peer.get("type", "")

        if peer_type != "Agent":
            continue

        for config_key, aliases in AGENT_ROLES.items():
            if any(alias in peer_name for alias in aliases):
                if config_key not in discovered:
                    discovered[config_key] = peer_id
                    logger.info(f"  🔍 Discovered [{config_key}] → {peer_id[:8]}… ({peer.get('name')})")
                break

    return discovered


async def resolve_agent_ids(client, headers, configs: dict, room_id: str = None) -> dict[str, str]:
    discovered = await discover_agents(client, headers, room_id)
    resolved = {}
    for key, config in configs.items():
        if key in discovered:
            resolved[key] = discovered[key]
        else:
            resolved[key] = config["agent_id"]
            logger.info(f"  📋 Config fallback [{key}] → {config['agent_id'][:8]}…")
    return resolved


# ══════════════════════════════════════════════════════════════
# BAND: Participant Management
# ══════════════════════════════════════════════════════════════
async def add_participant(client, headers, room_id: str, agent_id: str, name: str) -> bool:
    resp = await client.post(
        f"{AGENT_API}/chats/{room_id}/participants",
        headers=headers,
        json={"participant": {"participant_id": agent_id}},
    )
    ok = resp.status_code in (200, 201, 409)
    logger.info(f"  {'✓' if ok else '✗'} Added [{name}] ({agent_id[:8]}…)")
    return ok


async def remove_all_participants(client, headers, room_id: str):
    resp = await client.get(f"{AGENT_API}/chats/{room_id}/participants", headers=headers)
    if resp.status_code != 200:
        return
    removed = 0
    for p in resp.json().get("data", []):
        if p.get("type") == "Agent":
            pid = p.get("id") or p.get("participant_id", "")
            if not pid:
                continue
            r = await client.delete(
                f"{AGENT_API}/chats/{room_id}/participants/{pid}",
                headers=headers,
            )
            if r.status_code in (200, 204):
                removed += 1
            await asyncio.sleep(0.3)
    logger.info(f"  🚪 Removed {removed} participant(s)")


# ══════════════════════════════════════════════════════════════
# Room lifecycle
# ══════════════════════════════════════════════════════════════
async def create_room(client, headers) -> Optional[str]:
    global _active_room_id
    resp = await client.post(f"{AGENT_API}/chats", headers=headers, json={"chat": {}})
    if resp.status_code in (200, 201):
        room_id = resp.json().get("data", {}).get("id")
        _active_room_id = room_id
        logger.info(f"  ✓ Room created: {room_id}")
        return room_id
    logger.error(f"  ✗ Room creation failed: {resp.status_code} — {resp.text[:200]}")
    return None


async def destroy_room(client, headers, room_id: str):
    global _active_room_id
    resp = await client.delete(f"{AGENT_API}/chats/{room_id}", headers=headers)
    if resp.status_code in (200, 204):
        logger.info(f"  🗑  Room deleted: {room_id}")
        _active_room_id = None
        return
    logger.info(f"  ℹ  Hard delete returned {resp.status_code} — removing participants")
    await remove_all_participants(client, headers, room_id)
    _active_room_id = None


# ══════════════════════════════════════════════════════════════
# Messaging
# ══════════════════════════════════════════════════════════════
async def send_message(client, headers, room_id: str, content: str, mention_id: str) -> bool:
    resp = await client.post(
        f"{AGENT_API}/chats/{room_id}/messages",
        headers=headers,
        json={"message": {
            "content": f"@[[{mention_id}]] {content}",
            "mentions": [{"id": mention_id}],
        }},
    )
    if resp.status_code != 201:
        logger.warning(f"  ✗ send_message failed: {resp.status_code} — {resp.text[:150]}")
    return resp.status_code == 201


async def get_messages(client, headers, room_id: str) -> list:
    resp = await client.get(
        f"{AGENT_API}/chats/{room_id}/messages",
        headers=headers,
        params={"page": 1, "page_size": 100},
    )
    msgs = resp.json().get("data", []) if resp.status_code == 200 else []
    
    resp_proc = await client.get(
        f"{AGENT_API}/chats/{room_id}/messages",
        headers=headers,
        params={"page": 1, "page_size": 100, "status": "processed"},
    )
    if resp_proc.status_code == 200:
        msgs.extend(resp_proc.json().get("data", []))
        
    return msgs



# ══════════════════════════════════════════════════════════════
# Verdict extraction
# ══════════════════════════════════════════════════════════════
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


def is_decision_msg(msg: dict, decision_id: str) -> bool:
    sender = msg.get("sender") or {}
    if sender.get("id") == decision_id: return True
    if sender.get("agent_id") == decision_id: return True
    name = (sender.get("name") or sender.get("display_name") or "").lower()
    if "decision" in name: return True
    if msg.get("participant_id") == decision_id: return True
    if msg.get("agent_id") == decision_id: return True
    return False


# ══════════════════════════════════════════════════════════════
# CSV helpers
# ══════════════════════════════════════════════════════════════
FIELDNAMES = ["id", "status", "description", "room_id", "verdict",
              "submitted_at", "completed_at"]

def ensure_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

def load_txs() -> list[dict]:
    ensure_csv()
    with open(CSV_PATH) as f:
        return list(csv.DictReader(f))

def save_txs(txs: list[dict]):
    tmp = CSV_PATH + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for tx in txs: w.writerow({k: tx.get(k, "") for k in FIELDNAMES})
    shutil.move(tmp, CSV_PATH)

def update_tx(tx_id: str, **updates):
    txs = load_txs()
    for tx in txs:
        if str(tx["id"]) == str(tx_id):
            if tx.get("status") == "completed" and updates.get("status") not in ("completed", None):
                logger.warning(f"  ⚠ Blocked downgrade #{tx_id}: completed→{updates.get('status')}")
                return
            tx.update(updates)
    save_txs(txs)

def is_completed(tx_id: str) -> bool:
    for tx in load_txs():
        if str(tx["id"]) == str(tx_id):
            return tx.get("status") == "completed"
    return False

def write_result(tx_id, description, verdict, room_id):
    exists = os.path.exists(RESULTS_PATH)
    with open(RESULTS_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id","description","verdict","room_id","completed_at"])
        if not exists: w.writeheader()
        w.writerow({
            "id": tx_id,
            "description": description[:100] + ("..." if len(description) > 100 else ""),
            "verdict": verdict,
            "room_id": room_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
    logger.info(f"  📝 results.csv ← #{tx_id} → {verdict}")


# ══════════════════════════════════════════════════════════════
# Decision Subscriber — polls room, extracts verdict
# ══════════════════════════════════════════════════════════════
async def decision_subscriber_node(
    client, headers, agent_ids: dict,
    decision_outbox: Topic, tx_msg: TransactionMsg,
    seen_ids: set = None,   # unused — kept for API compat
):
    decision_id = agent_ids["decision-maker"]
    room_id     = tx_msg.room_id
    tx_id       = tx_msg.tx_id

    # Load legal agent config to use its API key for message polling.
    # This ensures we can see messages sent by the decision agent which are delivered to the legal agent.
    with open(AGENT_CONFIG_PATH) as f:
        configs = yaml.safe_load(f)
    legal_api_key = configs["legal-reviewer"]["api_key"]
    legal_headers = {"X-API-Key": legal_api_key}

    # WATERMARK: snapshot the room using legal_headers (same key used for polling)
    # so we only see messages posted AFTER this transaction was submitted
    msgs_before = await get_messages(client, legal_headers, room_id)
    watermark   = frozenset(m.get("id") for m in msgs_before if m.get("id"))
    logger.info(f"  [Subscriber] Watermark={len(watermark)} existing msgs — will ignore these")

    start        = asyncio.get_event_loop().time()
    seen_ids     = set(watermark)   # start seen from watermark so loop skips old msgs
    last_new     = asyncio.get_event_loop().time()
    repings      = 0
    poll_interval = POLL_SLOW

    logger.info(f"  [Subscriber] Listening — room {room_id[:8]}… | tx_id={tx_id}")

    while not _shutdown_event.is_set():
        elapsed = asyncio.get_event_loop().time() - start

        if elapsed > RESULT_TIMEOUT:
            logger.warning(f"  ⚠ Timeout #{tx_id}")
            await decision_outbox.publish(VerdictMsg(tx_id, "TIMEOUT", room_id, ""))
            return

        msgs = await get_messages(client, legal_headers, room_id)
        new_this_poll = 0

        for msg in msgs:
            mid = msg.get("id", "")
            if not mid or mid in seen_ids:
                continue
            seen_ids.add(mid)
            new_this_poll += 1
            sender = msg.get("sender") or {}
            sname  = sender.get("name") or sender.get("display_name") or "?"
            mtype  = msg.get("message_type", "text")
            snippet = msg.get("content", "")[:120].replace("\n", " ")
            logger.info(f"  📨 [{sname}] ({mtype}) {snippet}")

        if new_this_poll > 0:
            poll_interval = POLL_FAST
            last_new = asyncio.get_event_loop().time()
        else:
            poll_interval = POLL_SLOW

        # ── Verdict scan: ONLY new messages (NOT in the pre-existing watermark) ──
        new_msgs = [m for m in msgs if m.get("id") not in watermark]
        for msg in new_msgs:
            msg_type = msg.get("message_type", "")
            content  = msg.get("content", "")

            # Skip irrelevant message types
            if msg_type not in ("text", "task", "thought"):
                continue

            is_dec = is_decision_msg(msg, decision_id)

            # Skip very short messages, unless from the Decision Agent or of type 'task'
            if len(content) < 100 and not is_dec and msg_type != "task":
                continue

            verdict = extract_verdict(content)
            if verdict == "UNKNOWN":
                continue

            # Accept verdict from Decision Agent, or from any long message with a verdict
            if is_dec or msg_type == "task" or len(content) > 400:
                src = "Decision Agent" if is_dec else f"{msg_type}-match"
                logger.info(f"  ✅ Verdict [{src}]: {verdict}")
                await decision_outbox.publish(VerdictMsg(tx_id, verdict, room_id, content))
                return

        # ── Stall detection: re-ping the NEXT agent in chain ──
        silence = asyncio.get_event_loop().time() - last_new

        if silence >= STALL_DETECT_INTERVAL:
            if repings >= MAX_REPINGS:
                logger.error(f"  ✗ Stalled after {MAX_REPINGS} re-pings")
                await decision_outbox.publish(VerdictMsg(tx_id, "TIMEOUT", room_id, ""))
                return
            repings += 1

            # Find last agent that responded → ping the NEXT one (only from new messages)
            last_agent = None
            for msg in reversed(new_msgs):
                sender_name = (msg.get("sender", {}).get("name", "") or "").lower()
                if "policy" in sender_name:
                    last_agent = "policy-analyst"
                    break
                elif "risk" in sender_name:
                    last_agent = "risk-analyst"
                    break
                elif "legal" in sender_name:
                    last_agent = "legal-reviewer"
                    break
                elif "decision" in sender_name:
                    last_agent = "decision-maker"
                    break

            chain = ["policy-analyst", "risk-analyst", "legal-reviewer", "decision-maker"]
            if last_agent and last_agent in chain:
                idx = chain.index(last_agent)
                next_agent = chain[min(idx + 1, len(chain) - 1)]
            else:
                next_agent = "policy-analyst"

            # Don't re-ping decision-maker (it uses events, not messages)
            if next_agent == "decision-maker":
                logger.warning(f"  ⚠ Stall {int(silence)}s — Decision Agent may be processing, waiting...")
                last_new = asyncio.get_event_loop().time()
            else:
                target_id = agent_ids[next_agent]
                logger.warning(f"  ⚠ Stall {int(silence)}s — re-ping #{repings} → {next_agent}")
                await send_message(client, headers, room_id,
                    f"[TX#{tx_id}][RE-PING] Continue the compliance review for: {tx_msg.description[:200]}", target_id)
                last_new = asyncio.get_event_loop().time()
                poll_interval = POLL_FAST

        current = len(seen_ids)
        mins, secs = int(elapsed // 60), int(elapsed % 60)
        logger.info(f"  ⏳ {mins}m{secs}s | {current} msgs seen | {len(new_msgs)} new | poll={poll_interval}s | {repings} re-pings")
        await asyncio.sleep(poll_interval)


# ══════════════════════════════════════════════════════════════
# Single-room coordinator — one room, all transactions
# ══════════════════════════════════════════════════════════════
async def setup_persistent_room(client, headers, configs: dict) -> tuple[str, dict]:
    """Create ONE room and add all agents. Called once at startup."""
    global _active_room_id

    while not _shutdown_event.is_set():
        room_id = await create_room(client, headers)
        if not room_id:
            logger.warning(f"  ⚠ Room creation failed — retrying in {RETRY_DELAY}s")
            await asyncio.sleep(RETRY_DELAY)
            continue

        _active_room_id = room_id
        agent_ids = await resolve_agent_ids(client, headers, configs, room_id)

        for role_key, agent_id in agent_ids.items():
            await add_participant(client, headers, room_id, agent_id, role_key)
            await asyncio.sleep(1)

        logger.info(f"  ⏳ Warmup {AGENT_WARMUP_DELAY}s… (one-time)")
        await asyncio.sleep(AGENT_WARMUP_DELAY)
        logger.info(f"  ✅ Persistent room ready: {room_id}")
        return room_id, agent_ids

    return "", {}


async def transaction_publisher_node(client, headers, configs: dict, policy_inbox: Topic):
    decision_outbox = Topic("decision_outbox")

    # ── One-time room setup ────────────────────────────────────
    logger.info("  Setting up persistent room…")
    room_id, agent_ids = await setup_persistent_room(client, headers, configs)
    if not room_id:
        return

    logger.info("  Watching transactions.csv…")

    while not _shutdown_event.is_set():
        txs     = load_txs()
        pending = [tx for tx in txs if tx.get("status") == "pending"]

        if not pending:
            await asyncio.sleep(CSV_POLL_INTERVAL)
            continue

        tx          = pending[0]
        tx_id       = str(tx["id"])
        description = tx["description"]

        if is_completed(tx_id):
            logger.info(f"  ⏭ #{tx_id} already completed — skipping")
            update_tx(tx_id, status="completed")
            await asyncio.sleep(1)
            continue

        logger.info(f"{'═'*60}")
        logger.info(f"  📋 Transaction #{tx_id}")
        logger.info(f"  {description[:80]}…")
        logger.info(f"{'═'*60}")

        # Submit to Policy Agent (no room creation — reuse persistent room)
        tx_msg = TransactionMsg(tx_id=tx_id, description=description, room_id=room_id)
        update_tx(tx_id, status="submitted", room_id=room_id,
                  submitted_at=datetime.now(timezone.utc).isoformat())

        ok = await send_message(
            client, headers, room_id,
            f"[TX#{tx_id}] Review this transaction: {description}",
            agent_ids["policy-analyst"],
        )
        if not ok:
            logger.error(f"  ✗ Failed to send #{tx_id} — will retry in {RETRY_DELAY}s")
            update_tx(tx_id, status="pending", room_id="", submitted_at="")
            await asyncio.sleep(RETRY_DELAY)
            continue

        await policy_inbox.publish(tx_msg)
        update_tx(tx_id, status="processing")
        logger.info(f"  ✓ #{tx_id} submitted to pipeline (room {room_id[:8]}…)")

        # Wait for verdict (watermark taken inside subscriber with legal_headers)
        await decision_subscriber_node(client, headers, agent_ids, decision_outbox, tx_msg)

        # Collect verdict
        verdict_msg: VerdictMsg = await decision_outbox.subscribe(timeout=30)

        if verdict_msg is None:
            logger.error(f"  ✗ No verdict for #{tx_id}")
            update_tx(tx_id, status="timeout")
        elif verdict_msg.verdict == "TIMEOUT":
            update_tx(tx_id, status="timeout")
            logger.warning(f"  ⚠ #{tx_id} timed out")
        else:
            if not is_completed(tx_id):
                update_tx(tx_id, status="completed", verdict=verdict_msg.verdict,
                          completed_at=datetime.now(timezone.utc).isoformat())
                write_result(tx_id, description, verdict_msg.verdict, room_id)

        v = verdict_msg.verdict if verdict_msg else "UNKNOWN"
        logger.info(f"  ✅ Transaction #{tx_id} → {v}")
        logger.info("")

        # Short cooldown — no room teardown needed
        if not _shutdown_event.is_set() and pending[1:]:
            logger.info(f"  ⏳ Next tx in {DELAY_BETWEEN_TX}s…")
            await asyncio.sleep(DELAY_BETWEEN_TX)


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
async def main():
    ensure_csv()
    configs = load_configs()
    coord   = configs[COORDINATOR_KEY]
    headers = {"X-API-Key": coord["api_key"]}

    policy_inbox = Topic("policy_inbox")

    logger.info("╔═══════════════════════════════════════════════════════╗")
    logger.info("║  Financial Compliance Pipeline — Coordinator          ║")
    logger.info("╠═══════════════════════════════════════════════════════╣")
    logger.info("║  Mode: SINGLE PERSISTENT ROOM (all transactions)     ║")
    logger.info(f"║  Timeout: {RESULT_TIMEOUT}s | Stall: {STALL_DETECT_INTERVAL}s | Warmup: {AGENT_WARMUP_DELAY}s (once)    ║")
    logger.info(f"║  Poll: fast={POLL_FAST}s / slow={POLL_SLOW}s | Re-pings: max {MAX_REPINGS}        ║")
    logger.info("╚═══════════════════════════════════════════════════════╝\n")

    async with httpx.AsyncClient(timeout=30) as client:
        loop = asyncio.get_event_loop()

        def _sig():
            if not _shutdown_event.is_set():
                logger.info("⚡ Shutting down…")
                _shutdown_event.set()

        loop.add_signal_handler(signal.SIGINT,  _sig)
        loop.add_signal_handler(signal.SIGTERM, _sig)

        try:
            await transaction_publisher_node(client, headers, configs, policy_inbox)
        finally:
            if _active_room_id:
                logger.info(f"  🧹 Tearing down persistent room {_active_room_id[:8]}…")
                await destroy_room(client, headers, _active_room_id)

            try:
                txs = load_txs()
                changed = False
                for tx in txs:
                    if tx.get("status") in ("submitted", "processing"):
                        tx["status"] = "interrupted"
                        changed = True
                if changed:
                    save_txs(txs)
                    logger.info("  ✓ In-progress transactions marked 'interrupted'")
            except Exception as e:
                logger.error(f"  ✗ CSV cleanup failed: {e}")

            logger.info("Pipeline stopped.")


if __name__ == "__main__":
    asyncio.run(main())