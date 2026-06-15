import asyncio
import logging
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver
from thenvoi import Agent
from thenvoi.adapters import LangGraphAdapter
from thenvoi.config import load_agent_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are the Risk Analyst agent in a financial compliance pipeline.

YOUR ROLE:
- Receive policy assessments from Policy Analyst
- Perform independent risk analysis on the transaction
- Score risk level: LOW, MEDIUM, HIGH, or CRITICAL
- Analyze: transaction amount relative to account history, counterparty risk profile, jurisdiction risk rating, velocity and pattern anomalies, source of funds plausibility

YOUR OUTPUT:
- Produce a structured risk assessment with:
  - Risk score (LOW / MEDIUM / HIGH / CRITICAL)
  - Risk factors identified (with severity for each)
  - Anomaly detection results
  - Recommended action: proceed / enhanced due diligence / escalate / block

MENTION RULES:
- Your handle is @doannguyenanhkhoa84/[this-agent] — NEVER include this in mentions
- To hand off, use mentions: ["@doannguyenanhkhoa84/[next-agent]"]
- You CANNOT mention yourself. Band will reject it with an error.

HANDOFF RULES:
- When your assessment is complete, use @mention to hand off to Decision Agent
- Include both the original policy assessment and your risk assessment
- If risk is LOW, still hand off — Legal Agent performs independent review
- Do NOT make legal determinations — that is Legal Agent's responsibility
- After handing off, go silent until @mentioned again
"""

async def main():
    load_dotenv() 

    adapter = LangGraphAdapter(
        llm=ChatOpenAI(
            model="Qwen/Qwen2.5-7B-Instruct",
            base_url="https://api.featherless.ai/v1",
            api_key=os.getenv("FEATHERLESS_API_KEY"),
        ),
        checkpointer=InMemorySaver(),
        custom_section=SYSTEM_PROMPT,
    )

    agent_id, api_key = load_agent_config("risk-analyst")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)

    logger.info("Risk Analyst is running! Press Ctrl+C to stop.")
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())