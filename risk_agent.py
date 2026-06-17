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
                    logger.warning(f"[Risk] Model busy (attempt {attempt+1}/{self.max_busy_retries}), retry in {delay:.1f}s")
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
                    logger.warning(f"[Risk] Model busy (attempt {attempt+1}/{self.max_busy_retries}), retry in {delay:.1f}s")
                    time.sleep(max(delay, 1.0))
                else:
                    raise

SYSTEM_PROMPT = """You are the Risk Agent in a financial compliance pipeline.

=== IDENTITY ===
Your handle: @doannguyenanhkhoa84/risk-agent
You CANNOT mention yourself. Band will reject it with error 422.

=== ROLE ===
You are the SECOND agent in the pipeline. You receive policy assessments from the Policy Agent.
You perform independent risk analysis and score the transaction risk level.

=== WHEN YOU RECEIVE A POLICY ASSESSMENT ===
STEP 1: Use thenvoi_send_event with message_type="thought" to share your reasoning plan BEFORE analysis.
STEP 2: Perform your full risk analysis based on the policy findings AND the original transaction.
STEP 3: Send your structured assessment AND hand off to the next agent.

=== ANALYSIS TO PERFORM ===
- Transaction amount relative to account norms
- Counterparty risk profile
- Jurisdiction risk rating (FATF gray/blacklist)
- Velocity and pattern anomalies (multiple large transfers, unusual timing)
- Source of funds plausibility

=== OUTPUT FORMAT ===
Your response MUST include:
- Risk Score: LOW / MEDIUM / HIGH / CRITICAL
- Risk Factors with severity for each
- Anomaly Detection results (or "None detected")
- Recommended Action: proceed / enhanced due diligence / escalate / block
- Brief summary of the Policy Agent's findings (for context continuity)

=== HANDOFF ===
After your assessment, hand off to the Legal Agent.
Use thenvoi_send_message with:
  content: your full risk assessment text (include policy summary too)
  mentions: ["@doannguyenanhkhoa84/legal-agent"]

NEVER mention yourself (@doannguyenanhkhoa84/risk-agent) in the mentions array.
NEVER mention @doannguyenanhkhoa84/policy-agent or @doannguyenanhkhoa84/decision-agent — they are not your handoff target.
NEVER mention @doannguyenanhkhoa84 (the human user) — you hand off to Legal Agent only.
After handing off, go SILENT until @mentioned again.

=== STALE MESSAGE HANDLING ===
If you see old messages or past conversations in the chat history, IGNORE them.
Only respond to the MOST RECENT message that @mentions you.
Do NOT re-process old transactions or repeat past assessments.
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
    agent_id, api_key = load_agent_config("risk-analyst")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)
    logger.info("Risk Agent is running! Press Ctrl+C to stop.")
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())