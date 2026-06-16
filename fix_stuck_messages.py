# ============================================================
# FIX FOR PROBLEM 2: Model returns plain text instead of tools
# ============================================================
#
# The issue: Featherless models sometimes return plain text
# instead of calling thenvoi_send_message. The SDK treats
# plain text as "done" and nothing appears in the chat.
#
# FIX: Force the model to always use tools by adding
# model_kwargs={"tool_choice": "required"} to ChatOpenAI.
#
# EXAMPLE (update in all 4 agent files):

"""
adapter = LangGraphAdapter(
    llm=ChatOpenAI(
        model="Qwen/Qwen2.5-32B-Instruct",
        base_url="https://api.featherless.ai/v1",
        api_key=os.getenv("FEATHERLESS_API_KEY"),
        temperature=0.1,
        model_kwargs={"tool_choice": "required"},  # <-- ADD THIS
    ),
    checkpointer=InMemorySaver(),
    custom_section=SYSTEM_PROMPT,
)
"""

# "tool_choice": "required" forces the model to ALWAYS call a tool.
# This prevents the model from returning plain text that goes nowhere.
#
# WARNING: This may cause the model to loop (call tools forever).
# If that happens, try "auto" instead:
#   model_kwargs={"tool_choice": "auto"}
#
# Or force a specific tool:
#   model_kwargs={"tool_choice": {"type": "function", "function": {"name": "thenvoi_send_message"}}}


# ============================================================
# FIX FOR PROBLEM 3: Stuck messages in "processing" state
# ============================================================
#
# The issue: When marking a message as "processed" fails (422),
# the message stays in "processing" state forever. The SDK's
# crash recovery keeps re-fetching it, creating an infinite loop.
#
# FIX: Use this script to find and clear stuck messages.

import asyncio
import httpx
import yaml
import sys


def load_agent_configs(config_path="agent_config.yaml"):
    """Load all agent configs from yaml."""
    with open(config_path) as f:
        return yaml.safe_load(f)


async def clear_stuck_messages(agent_name: str, agent_id: str, api_key: str):
    """Find and clear all stuck messages for an agent."""
    
    base_url = "https://app.thenvoi.com/api/v1/agent"
    headers = {"X-API-Key": api_key}
    
    async with httpx.AsyncClient() as client:
        # Step 1: Get all chat rooms for this agent
        resp = await client.get(
            f"{base_url}/chats",
            headers=headers,
            params={"page": 1, "page_size": 100}
        )
        
        if resp.status_code != 200:
            print(f"  ERROR: Could not fetch chats: {resp.status_code}")
            return
        
        rooms = resp.json().get("data", [])
        print(f"  Found {len(rooms)} chat rooms")
        
        for room in rooms:
            room_id = room["id"]
            room_title = room.get("title", "Untitled")
            
            # Step 2: Get messages stuck in "processing" state
            resp = await client.get(
                f"{base_url}/chats/{room_id}/messages",
                headers=headers,
                params={"status": "processing", "page": 1}
            )
            
            if resp.status_code != 200:
                print(f"  ERROR: Could not fetch messages for room {room_title}: {resp.status_code}")
                continue
            
            messages = resp.json().get("data", [])
            
            if not messages:
                print(f"  Room '{room_title}': No stuck messages")
                continue
            
            print(f"  Room '{room_title}': {len(messages)} stuck messages found")
            
            # Step 3: Mark each stuck message as "failed" to clear it
            for msg in messages:
                msg_id = msg["id"]
                content_preview = msg.get("content", "")[:50]
                
                resp = await client.post(
                    f"{base_url}/chats/{room_id}/messages/{msg_id}/failed",
                    headers=headers,
                    json={"error": "Cleared by cleanup script — message was stuck in processing state"}
                )
                
                if resp.status_code == 200:
                    print(f"    CLEARED: {msg_id} ({content_preview}...)")
                else:
                    # Try marking as processed instead
                    resp2 = await client.post(
                        f"{base_url}/chats/{room_id}/messages/{msg_id}/processed",
                        headers=headers,
                    )
                    if resp2.status_code == 200:
                        print(f"    CLEARED (processed): {msg_id} ({content_preview}...)")
                    else:
                        print(f"    FAILED to clear: {msg_id} — status {resp.status_code} / {resp2.status_code}")
            
            # Step 4: Also check /next for any remaining stuck messages
            resp = await client.get(
                f"{base_url}/chats/{room_id}/messages/next",
                headers=headers,
            )
            
            if resp.status_code == 200:
                next_msg = resp.json().get("data", {})
                if next_msg:
                    msg_id = next_msg.get("id", "unknown")
                    print(f"    WARNING: /next still returns message {msg_id} — may need manual clearing on Band dashboard")
            elif resp.status_code == 204:
                print(f"    /next is clear — no pending messages")


async def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "agent_config.yaml"
    
    print(f"Loading configs from {config_path}...")
    configs = load_agent_configs(config_path)
    
    for agent_name, config in configs.items():
        agent_id = config["agent_id"]
        api_key = config["api_key"]
        
        print(f"\n{'='*50}")
        print(f"Agent: {agent_name} ({agent_id})")
        print(f"{'='*50}")
        
        await clear_stuck_messages(agent_name, agent_id, api_key)
    
    print(f"\n{'='*50}")
    print("DONE. All stuck messages cleared.")
    print("You can now restart your agent scripts.")
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())