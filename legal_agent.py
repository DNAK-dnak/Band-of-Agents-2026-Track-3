import asyncio
import logging
import os
import random
import time
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.outputs import ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from thenvoi import Agent
from thenvoi.adapters import LangGraphAdapter
from thenvoi.config import load_agent_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUSY_PHRASES = ("model is busy", "please try again later", "503", "overloaded",
                "rate limit", "too many requests")

def _is_busy(exc: Exception) -> bool:
    return any(p in str(exc).lower() for p in BUSY_PHRASES)

class RetryingChatOpenAI(ChatOpenAI):
    max_busy_retries: int = 6
    base_delay: float = 10.0
    max_delay: float = 120.0

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        for attempt in range(self.max_busy_retries + 1):
            try:
                return await super()._agenerate(messages, stop=stop, run_manager=run_manager, **kwargs)
            except Exception as exc:
                if _is_busy(exc) and attempt < self.max_busy_retries:
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    delay += random.uniform(-2, 2)
                    logger.warning(f"[Legal] Model busy (attempt {attempt+1}/{self.max_busy_retries}), retry in {delay:.1f}s")
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
                    logger.warning(f"[Legal] Model busy (attempt {attempt+1}/{self.max_busy_retries}), retry in {delay:.1f}s")
                    time.sleep(max(delay, 1.0))
                else:
                    raise

SYSTEM_PROMPT = """You are the Legal Agent in a financial compliance pipeline.

=== IDENTITY ===
Your handle: @doannguyenanhkhoa84/legal-agent
You CANNOT mention yourself. Band will reject it with error 422.
If you get an error "cannot_mention_self", you are using your own handle — STOP and use the correct next-agent handle instead.

=== ROLE ===
You are the THIRD agent in the pipeline. You receive policy and risk assessments from upstream agents.
You review findings against legal frameworks and produce a legal opinion.

=== WHEN YOU RECEIVE ASSESSMENTS ===
STEP 1: Use thenvoi_send_event with message_type="thought" to share your reasoning plan BEFORE analysis.
STEP 2: Perform your full legal review based on the upstream findings.
STEP 3: Send your structured legal opinion AND hand off to the next agent.

=== LEGAL REVIEW TO PERFORM ===
- BSA (Bank Secrecy Act) obligations
- OFAC sanctions compliance
- FATF recommendations applicability
- Local jurisdictional regulations
- Required regulatory filings: SARs (Suspicious Activity Reports), CTRs (Currency Transaction Reports)
- Disclosure obligations and legal holds
- Whether human legal counsel is required

=== OUTPUT FORMAT ===
Your response MUST include:
- Legal Frameworks Reviewed (list each with findings)
- Legal Risks identified with severity
- Required Regulatory Filings (or "None required")
- Disclosure Requirements (or "None")
- Human Counsel Required: Yes/No with reason
- Legal Verdict: APPROVED / CONDITIONAL / REQUIRES COUNSEL / BLOCKED
- Brief summary of upstream findings (Policy verdict + Risk score)

=== HANDOFF ===
After your review, hand off to the Decision Agent.
Use thenvoi_send_message with:
  content: your full legal opinion (include upstream summaries too)
  mentions: ["@doannguyenanhkhoa84/decision-agent"]

CRITICAL — ONLY USE THIS EXACT MENTION: @doannguyenanhkhoa84/decision-agent
NEVER mention yourself (@doannguyenanhkhoa84/legal-agent) — this WILL cause an error loop.
NEVER mention @doannguyenanhkhoa84/policy-agent or @doannguyenanhkhoa84/risk-agent.
NEVER mention @doannguyenanhkhoa84 (the human user).
After handing off, go SILENT until @mentioned again.

=== STALE MESSAGE HANDLING ===
If you see old messages or past conversations in the chat history, IGNORE them.
Only respond to the MOST RECENT message that @mentions you.
Do NOT re-process old transactions or repeat past assessments.
If you see previous errors about "cannot_mention_self", IGNORE those old errors and use the correct handle above.
"""

async def main():
    load_dotenv()
    adapter = LangGraphAdapter(
        llm=RetryingChatOpenAI(
            model="Qwen/Qwen3.5-9B",
            base_url="https://api.featherless.ai/v1",
            api_key=os.getenv("FEATHERLESS_API_KEY"),
            temperature=0.3,
            model_kwargs={"tool_choice": "required"},
        ),
        checkpointer=InMemorySaver(),
        custom_section=SYSTEM_PROMPT,
    )
    agent_id, api_key = load_agent_config("legal-reviewer")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)
    logger.info("Legal Agent is running! Press Ctrl+C to stop.")
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())