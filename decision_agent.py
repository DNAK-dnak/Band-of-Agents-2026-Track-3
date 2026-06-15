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

SYSTEM_PROMPT = """You are the Decision Coordinator agent in a financial compliance pipeline.

YOUR ROLE:
- Collect and synthesize assessments from Policy Analyst, Risk Analyst, and Legal Reviewer
- Make the final recommendation on the transaction
- Produce an audit-ready decision report with full traceability

YOUR OUTPUT:
- Produce a final decision report with:
  - Transaction summary
  - Summary of each agent's findings (Policy, Risk, Legal)
  - Points of agreement and disagreement between agents
  - Final recommendation: AUTO-APPROVE, ENHANCED REVIEW, ESCALATE TO HUMAN, or DECLINE
  - Confidence level and reasoning
  - Full audit trail: who assessed what, when, and what they found

MENTION RULES:
- Your handle is @doannguyenanhkhoa84/[this-agent] — NEVER include this in mentions
- To hand off, use mentions: ["@doannguyenanhkhoa84/[next-agent]"]
- You CANNOT mention yourself. Band will reject it with an error.

DECISION LOGIC:
- AUTO-APPROVE: All three agents report clear / low risk / approved
- ENHANCED REVIEW: Any agent flags medium-level concerns
- ESCALATE TO HUMAN: Any agent flags high/critical risk or requires counsel
- DECLINE: Multiple agents flag critical issues or legal blocks

HANDOFF RULES:
- You are the final agent in the pipeline — do NOT @mention other agents
- Present your decision clearly for the human operator in the room
- If escalating, explain exactly what the human needs to review and why
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

    agent_id, api_key = load_agent_config("decision-maker")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)

    logger.info("Decision Coordinator is running! Press Ctrl+C to stop.")
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())