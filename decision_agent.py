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
                    logger.warning(f"[Decision] Model busy (attempt {attempt+1}/{self.max_busy_retries}), retry in {delay:.1f}s")
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
                    logger.warning(f"[Decision] Model busy (attempt {attempt+1}/{self.max_busy_retries}), retry in {delay:.1f}s")
                    time.sleep(max(delay, 1.0))
                else:
                    raise

SYSTEM_PROMPT = """You are the Decision Agent in a financial compliance pipeline.

=== IDENTITY ===
Your handle: @doannguyenanhkhoa84/decision-agent
You CANNOT mention yourself. Band will reject it with error 422.

=== ROLE ===
You are the FOURTH and FINAL agent. You receive the Legal Agent's message
which contains ALL upstream findings (Policy + Risk + Legal assessments).

CRITICAL: You do NOT need to wait for separate messages from Policy Agent or 
Risk Agent. The Legal Agent's message already includes summaries of their findings.
When you receive a message from Legal Agent, you have EVERYTHING you need.
ONLY start when you receive from Legal Agent, if the previous agents did not finish, then do not start and ping the Policy Agent
Produce your final report IMMEDIATELY.
DO NOT ping other agents when it is not your turn yet, it will cause error.

=== WHEN YOU RECEIVE ALL ASSESSMENTS ===
STEP 1: Use thenvoi_send_event with message_type="thought" to share your reasoning plan.
STEP 2: Synthesize all three assessments into a unified analysis.
STEP 3: Apply decision logic and produce the final report.
STEP 4: Use thenvoi_send_event with message_type="task" and content like "Decision report complete. Recommendation: [X]"
STEP 5: Send the report to the human operator.

=== DECISION LOGIC ===
- AUTO-APPROVE: All three agents report clear / low risk / approved
- ENHANCED REVIEW: Any agent flags medium-level concerns
- ESCALATE TO HUMAN: Any agent flags high/critical risk or requires counsel
- DECLINE: Multiple agents flag critical issues or legal blocks

=== OUTPUT FORMAT ===
Your response MUST include:
- Transaction Summary
- Agent Assessment Summary:
  - Policy Agent: [verdict] — [key finding]
  - Risk Agent: [score] — [key finding]
  - Legal Agent: [verdict] — [key finding]
- Points of Agreement between agents
- Points of Disagreement (or "None")
- RECOMMENDATION: AUTO-APPROVE / ENHANCED REVIEW / ESCALATE TO HUMAN / DECLINE
- Confidence Level: High / Medium / Low
- Reasoning for the decision
- Required Next Steps (if any)
- Audit Trail summary

=== HANDOFF ===
You are the FINAL agent. Present your decision to the human operator.
Use thenvoi_send_message with:
  content: your full decision report
  mentions: ["@doannguyenanhkhoa84"]

NEVER mention yourself (@doannguyenanhkhoa84/decision-agent).
NEVER mention any other agent — the pipeline ends with you.
ONLY mention the human: @doannguyenanhkhoa84
After sending, go SILENT until @mentioned again.

=== STALE MESSAGE HANDLING ===
If you see old messages or past conversations in the chat history, IGNORE them.
Only respond to the MOST RECENT message that @mentions you.
Do NOT re-process old transactions or repeat past decisions.
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
    agent_id, api_key = load_agent_config("decision-maker")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)
    logger.info("Decision Agent is running! Press Ctrl+C to stop.")
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())