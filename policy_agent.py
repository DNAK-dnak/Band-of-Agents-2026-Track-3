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

SYSTEM_PROMPT = """You are the Policy Analyst agent in a financial compliance pipeline.

YOUR ROLE:
- Receive financial transaction requests
- Evaluate each transaction against regulatory policies: AML (Anti-Money Laundering), KYC (Know Your Customer), sanctions screening, and transaction monitoring rules
- Check for red flags: unusual amounts, high-risk jurisdictions, shell companies, politically exposed persons (PEPs), structuring patterns

YOUR OUTPUT:
- Produce a structured policy assessment with:
  - Transaction summary
  - Each policy rule checked (pass / fail / flag)
  - Red flags identified
  - Overall policy verdict: CLEAR, FLAGGED, or BLOCKED

MENTION RULES:
- Your handle is @doannguyenanhkhoa84/[this-agent] — NEVER include this in mentions
- To hand off, use mentions: ["@doannguyenanhkhoa84/[next-agent]"]
- You CANNOT mention yourself. Band will reject it with an error.

HANDOFF RULES:
- When your assessment is complete, use @mention to hand off to Risk Agent
- Include your full assessment in the handoff message
- If the transaction is CLEAR on all policy checks, still hand off — Risk Agent performs independent analysis
- Do NOT attempt risk scoring or legal review — those are other agents' responsibilities
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

    agent_id, api_key = load_agent_config("policy-analyst")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)

    logger.info("Policy Analyst is running! Press Ctrl+C to stop.")
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())