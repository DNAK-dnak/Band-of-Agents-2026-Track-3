import asyncio
import os
import random
import time
import logging
import warnings
from datetime import datetime, timezone
from typing import Any, Optional
import httpx
import yaml
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.outputs import ChatResult
from langchain_core.tools import StructuredTool

# Load env variables at module import
load_dotenv()

# Logger setup helper
def get_agent_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(name)

# ── Retry wrapper ──────────────────────────────────────────────
BUSY_PHRASES = ("model is busy", "please try again later", "503", "overloaded",
                "rate limit", "too many requests", "concurrency", "overloaded")

def _is_busy(exc: Exception) -> bool:
    return any(p in str(exc).lower() for p in BUSY_PHRASES)

class RetryingChatOpenAI(ChatOpenAI):
    max_busy_retries: int = 6
    base_delay: float = 15.0
    max_delay: float = 120.0

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        for attempt in range(self.max_busy_retries + 1):
            try:
                return await super()._agenerate(messages, stop=stop, run_manager=run_manager, **kwargs)
            except Exception as exc:
                if _is_busy(exc) and attempt < self.max_busy_retries:
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    delay += random.uniform(-2, 2)
                    print(f"  [Model Warning] Busy (attempt {attempt+1}/{self.max_busy_retries}), retrying in {delay:.1f}s")
                    await asyncio.sleep(max(delay, 1.0))
                else:
                    raise

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        for attempt in range(self.max_busy_retries + 1):
            try:
                return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)
            except Exception as exc:
                if _is_busy(exc) and attempt < self.max_busy_retries:
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    print(f"  [Model Warning] Busy (attempt {attempt+1}/{self.max_busy_retries}), retrying in {delay:.1f}s")
                    time.sleep(max(delay, 1.0))
                else:
                    raise

# ── REST API Wrappers ──────────────────────────────────────────

async def get_next_message(client: httpx.AsyncClient, headers: dict, room_id: str) -> Optional[dict]:
    url = f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/messages/next"
    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code == 204:
            return None
        if resp.status_code == 200:
            return resp.json().get("data")
        return None
    except Exception as e:
        print(f"Error fetching next message: {e}")
        return None

async def mark_processing(client: httpx.AsyncClient, headers: dict, room_id: str, message_id: str) -> bool:
    url = f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/messages/{message_id}/processing"
    try:
        resp = await client.post(url, headers=headers, json={})
        return resp.status_code == 200
    except Exception as e:
        print(f"Error marking processing: {e}")
        return False

async def mark_processed(client: httpx.AsyncClient, headers: dict, room_id: str, message_id: str) -> bool:
    url = f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/messages/{message_id}/processed"
    try:
        resp = await client.post(url, headers=headers, json={})
        return resp.status_code == 200
    except Exception as e:
        print(f"Error marking processed: {e}")
        return False

async def mark_failed(client: httpx.AsyncClient, headers: dict, room_id: str, message_id: str, error: str) -> bool:
    url = f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/messages/{message_id}/failed"
    try:
        resp = await client.post(url, headers=headers, json={"error": error})
        return resp.status_code == 200
    except Exception as e:
        print(f"Error marking failed: {e}")
        return False

async def resolve_mentions(client: httpx.AsyncClient, headers: dict, room_id: str, mentions: list[str]) -> list[dict]:
    cleaned_input_handles = []
    for m in mentions:
        h = m.strip()
        if h.startswith("@"):
            h = h[1:]
        if h.startswith("[[") and h.endswith("]]"):
            h = h[2:-2]
        cleaned_input_handles.append(h.lower())

    resolved = []
    try:
        resp = await client.get(f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/participants", headers=headers)
        if resp.status_code == 200:
            participants = resp.json().get("data", [])
            for ch in cleaned_input_handles:
                found = False
                for p in participants:
                    phandle = (p.get("handle") or "").lower()
                    pname = (p.get("name") or "").lower()
                    pid = (p.get("id") or "").lower()
                    if ch == phandle or ch == pname or ch == pid:
                        resolved.append({"id": p["id"], "handle": p.get("handle") or p["name"]})
                        found = True
                        break
                if not found:
                    print(f"Warning: Could not resolve mention handle: {ch}")
    except Exception as e:
        print(f"Error resolving mentions: {e}")
    return resolved

async def send_message(client: httpx.AsyncClient, headers: dict, room_id: str, content: str, mentions: list[str], self_agent_id: str = None) -> bool:
    # Resolve mention handles to UUIDs + handles
    resolved_mentions = await resolve_mentions(client, headers, room_id, mentions)
    
    # Filter out self-agent from resolved mentions to avoid 422 self-mention error
    if self_agent_id:
        resolved_mentions = [m for m in resolved_mentions if m["id"] != self_agent_id]

    # Validation: API requires at least 1 mention
    if not resolved_mentions:
        # Fallback: find any other agent in participants to mention
        try:
            resp = await client.get(f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/participants", headers=headers)
            if resp.status_code == 200:
                participants = resp.json().get("data", [])
                # Try to find a member other than the self-agent to mention
                for p in participants:
                    if self_agent_id and p["id"] == self_agent_id:
                        continue
                    resolved_mentions.append({"id": p["id"], "handle": p.get("handle") or p["name"]})
                    break
        except Exception:
            pass

    if not resolved_mentions:
        print("Error: Message sending failed because no participants could be resolved for mentions.")
        return False

    url = f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/messages"
    payload = {
        "message": {
            "content": content,
            "mentions": resolved_mentions
        }
    }
    try:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code != 201:
            print(f"Failed to send message: {resp.status_code} - {resp.text}")
        return resp.status_code == 201
    except Exception as e:
        print(f"Error sending message: {e}")
        return False

async def send_event(client: httpx.AsyncClient, headers: dict, room_id: str, content: str, message_type: str) -> bool:
    url = f"https://app.thenvoi.com/api/v1/agent/chats/{room_id}/events"
    payload = {
        "event": {
            "content": content,
            "message_type": message_type,
            "metadata": {}
        }
    }
    try:
        resp = await client.post(url, headers=headers, json=payload)
        return resp.status_code == 201
    except Exception as e:
        print(f"Error sending event: {e}")
        return False

# ── Agent Context & Tools for LangChain ────────────────────────

class AgentContext:
    def __init__(self, client: httpx.AsyncClient, headers: dict, room_id: str, logger: logging.Logger, agent_id: str):
        self.client = client
        self.headers = headers
        self.room_id = room_id
        self.logger = logger
        self.agent_id = agent_id
        self.sent_message = False

    async def send_message_tool(self, content: str, mentions: list[str]) -> str:
        """Send a message to the room to hand off to the next agent or submit the report."""
        self.logger.info(f"  [Tool] send_message (mentions: {mentions})")
        success = await send_message(self.client, self.headers, self.room_id, content, mentions, self.agent_id)
        if success:
            self.sent_message = True
            return "Message sent successfully."
        else:
            raise Exception("Failed to send message via Band API.")

    async def send_event_tool(self, content: str, message_type: str) -> str:
        """Send an event (e.g. thought, task) to the room."""
        self.logger.info(f"  [Tool] send_event (type: {message_type})")
        success = await send_event(self.client, self.headers, self.room_id, content, message_type)
        if success:
            return "Event sent successfully."
        else:
            raise Exception("Failed to send event via Band API.")

def get_tools(context: AgentContext) -> list:
    send_msg_tool = StructuredTool.from_function(
        coroutine=context.send_message_tool,
        name="thenvoi_send_message",
        description="Send a text message to the current room. Must specify content and mentions (e.g. ['@doannguyenanhkhoa84/risk-agent'])."
    )
    send_evt_tool = StructuredTool.from_function(
        coroutine=context.send_event_tool,
        name="thenvoi_send_event",
        description="Send an event to the room. message_type must be thought, task, or error."
    )
    return [send_msg_tool, send_evt_tool]

# ── Message Processing ─────────────────────────────────────────

async def process_message(
    client: httpx.AsyncClient,
    headers: dict,
    room_id: str,
    message: dict,
    llm: RetryingChatOpenAI,
    system_prompt: str,
    logger: logging.Logger,
    agent_id: str,
    allowed_sender_ids: list[str] = None
):
    mid = message["id"]
    content = message["content"]
    sender_id = message.get("sender_id") or message.get("sender", {}).get("id") or ""
    sender_name = message.get("sender_name") or ""
    
    # Self-message skip to prevent loops when an agent mentions itself
    if sender_id == agent_id:
        logger.info(f"Skipping self-sent message {mid} to prevent loops")
        await mark_processing(client, headers, room_id, mid)
        await mark_processed(client, headers, room_id, mid)
        return

    # Allowed sender check
    if allowed_sender_ids is not None and sender_id not in allowed_sender_ids:
        logger.info(f"Skipping message {mid} from unauthorized sender {sender_name} ({sender_id})")
        await mark_processing(client, headers, room_id, mid)
        await mark_processed(client, headers, room_id, mid)
        return

    logger.info(f"📨 Processing message {mid} from {sender_name}")
    
    # 1. Claim message
    await mark_processing(client, headers, room_id, mid)
    
    # 2. Setup context & tools
    context = AgentContext(client, headers, room_id, logger, agent_id)
    tools = get_tools(context)
    llm_with_tools = llm.bind_tools(tools)
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": content}
    ]
    
    # 3. LLM / Tool Execution Loop
    try:
        for turn in range(10):
            response = await llm_with_tools.ainvoke(messages)
            messages.append(response)
            
            if not response.tool_calls:
                break
                
            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                args = tool_call["args"]
                call_id = tool_call["id"]
                
                if tool_name == "thenvoi_send_message":
                    res = await context.send_message_tool(
                        content=args.get("content", ""),
                        mentions=args.get("mentions", [])
                    )
                elif tool_name == "thenvoi_send_event":
                    res = await context.send_event_tool(
                        content=args.get("content", ""),
                        message_type=args.get("message_type", "")
                    )
                else:
                    res = f"Unknown tool: {tool_name}"
                    
                messages.append({
                    "role": "tool",
                    "content": res,
                    "tool_call_id": call_id
                })
                
        # 4. Success — mark processed
        await mark_processed(client, headers, room_id, mid)
        logger.info(f"✓ Message {mid} processed successfully")
        
    except Exception as e:
        logger.exception(f"✗ Failed to process message {mid}")
        await mark_failed(client, headers, room_id, mid, str(e))
        # Mark processed to prevent infinite retry loops in get_next_message
        await mark_processed(client, headers, room_id, mid)

# ── Main Run Loop ──────────────────────────────────────────────

async def run_agent(agent_key: str, system_prompt: str, logger: logging.Logger, allowed_senders: list[str] = None):
    with open("agent_config.yaml") as f:
        configs = yaml.safe_load(f)
        
    agent_id = configs[agent_key]["agent_id"]
    api_key = configs[agent_key]["api_key"]
    headers = {"X-API-Key": api_key}
    
    allowed_sender_ids = []
    if allowed_senders:
        for s in allowed_senders:
            if s in configs:
                allowed_sender_ids.append(configs[s]["agent_id"])
    
    llm = RetryingChatOpenAI(
        model="Qwen/Qwen2.5-14B-Instruct",
        base_url="https://api.featherless.ai/v1",
        api_key=os.getenv("FEATHERLESS_API_KEY"),
        temperature=0.3,
        model_kwargs={"tool_choice": "required"}
    )
    
    logger.info(f"Agent Loop Started: {agent_key} ({agent_id})")
    
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                # 1. Fetch active chats
                chats_resp = await client.get("https://app.thenvoi.com/api/v1/agent/chats?page=1&page_size=100", headers=headers)
                if chats_resp.status_code != 200:
                    logger.error(f"Failed to list chats: {chats_resp.status_code}")
                    await asyncio.sleep(8)
                    continue
                    
                rooms = chats_resp.json().get("data", [])
                for room in rooms:
                    room_id = room["id"]
                    # 2. Get next message
                    msg = await get_next_message(client, headers, room_id)
                    if msg:
                        await process_message(client, headers, room_id, msg, llm, system_prompt, logger, agent_id, allowed_sender_ids)
                        
                await asyncio.sleep(8)
                
            except asyncio.CancelledError:
                logger.info("Agent shutting down...")
                break
            except Exception as e:
                logger.exception("Error in agent main loop")
                await asyncio.sleep(8)
