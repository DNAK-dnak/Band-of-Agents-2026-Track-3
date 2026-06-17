"""
Fix stuck messages AND delete stale empty rooms.

Run this before starting agents to ensure a clean state.
It will:
  1. Find all rooms for each agent
  2. Clear any messages stuck in "processing" state
  3. DELETE rooms that have no useful messages (empty / leftover sessions)
     so agents don't bind to stale rooms on startup

Usage:
  python fix_stuck_messages.py [agent_config.yaml]
"""

import asyncio
import httpx
import yaml
import sys


def load_agent_configs(config_path="agent_config.yaml"):
    with open(config_path) as f:
        return yaml.safe_load(f)


async def clean_agent(agent_name: str, agent_id: str, api_key: str):
    base_url = "https://app.thenvoi.com/api/v1/agent"
    headers = {"X-API-Key": api_key}

    async with httpx.AsyncClient(timeout=20) as client:
        # ── Get all rooms ──────────────────────────────────────
        resp = await client.get(
            f"{base_url}/chats",
            headers=headers,
            params={"page": 1, "page_size": 100},
        )
        if resp.status_code != 200:
            print(f"  ERROR fetching chats: {resp.status_code}")
            return

        rooms = resp.json().get("data", [])
        print(f"  Found {len(rooms)} chat room(s)")

        for room in rooms:
            room_id = room["id"]
            room_title = room.get("title", "Untitled")

            # ── Clear stuck messages ───────────────────────────
            stuck_resp = await client.get(
                f"{base_url}/chats/{room_id}/messages",
                headers=headers,
                params={"status": "processing", "page": 1},
            )
            stuck_msgs = stuck_resp.json().get("data", []) if stuck_resp.status_code == 200 else []

            for msg in stuck_msgs:
                msg_id = msg["id"]
                r = await client.post(
                    f"{base_url}/chats/{room_id}/messages/{msg_id}/failed",
                    headers=headers,
                    json={"error": "Cleared by cleanup script"},
                )
                if r.status_code == 200:
                    print(f"    CLEARED stuck message: {msg_id}")
                else:
                    # try marking processed
                    r2 = await client.post(
                        f"{base_url}/chats/{room_id}/messages/{msg_id}/processed",
                        headers=headers,
                    )
                    status = "CLEARED (processed)" if r2.status_code == 200 else f"FAILED ({r.status_code}/{r2.status_code})"
                    print(f"    {status}: {msg_id}")

            # ── Check total message count in room ──────────────
            all_resp = await client.get(
                f"{base_url}/chats/{room_id}/messages",
                headers=headers,
                params={"page": 1, "page_size": 5},
            )
            all_msgs = all_resp.json().get("data", []) if all_resp.status_code == 200 else []
            msg_count = len(all_msgs)

            # ── Delete stale/empty rooms ───────────────────────
            # A room is stale if it has 0 real messages (just a leftover session).
            # These cause agents to bind at startup and miss real work.
            should_delete = msg_count == 0

            if should_delete:
                del_resp = await client.delete(
                    f"{base_url}/chats/{room_id}",
                    headers=headers,
                )
                if del_resp.status_code in (200, 204):
                    print(f"    🗑  DELETED stale room '{room_title}' ({room_id}) — {msg_count} messages")
                else:
                    # Fallback: remove all participants so agents won't re-join it
                    print(f"    ℹ  Hard delete not supported (HTTP {del_resp.status_code}), removing participants...")
                    part_resp = await client.get(
                        f"{base_url}/chats/{room_id}/participants",
                        headers=headers,
                    )
                    if part_resp.status_code == 200:
                        for p in part_resp.json().get("data", []):
                            if p.get("type") == "Agent":
                                await client.delete(
                                    f"{base_url}/chats/{room_id}/participants/{p['id']}",
                                    headers=headers,
                                )
                                await asyncio.sleep(0.3)
                    print(f"    🧹 Removed participants from stale room '{room_title}'")
            else:
                if stuck_msgs:
                    print(f"    Room '{room_title}': {len(stuck_msgs)} stuck message(s) cleared, {msg_count} total — KEEPING")
                else:
                    print(f"    Room '{room_title}': {msg_count} message(s), no stuck — KEEPING")


async def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "agent_config.yaml"
    print(f"Loading configs from {config_path}...")
    configs = load_agent_configs(config_path)

    for agent_name, config in configs.items():
        print(f"\n{'='*50}")
        print(f"Agent: {agent_name} ({config['agent_id']})")
        print(f"{'='*50}")
        await clean_agent(agent_name, config["agent_id"], config["api_key"])

    print(f"\n{'='*50}")
    print("DONE. All stale rooms deleted, stuck messages cleared.")
    print("Agents will start fresh with no pre-existing room subscriptions.")
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())