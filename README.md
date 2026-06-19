# Financial Compliance Multi-Agent Pipeline

**Band of Agents Hackathon 2026 — Track 3: Regulated & High-Stakes Workflows**

A multi-agent system that automates financial transaction compliance review using 4 specialized AI agents coordinated through [Band](https://www.band.ai/).

🚀 **[Live Demo Web Dashboard](https://band-of-agents-2026-track-3-production.up.railway.app/)**

## How It Works

```
Transaction CSV → Coordinator → Band Chatroom → Pipeline → Results CSV

                    ┌──────────┐
                    │  Policy  │  AML, KYC, sanctions screening
                    │  Agent   │
                    └────┬─────┘
                         ↓
                    ┌──────────┐
                    │   Risk   │  Risk scoring, anomaly detection
                    │  Agent   │
                    └────┬─────┘
                         ↓
                    ┌──────────┐
                    │  Legal   │  BSA, OFAC, FATF compliance
                    │  Agent   │
                    └────┬─────┘
                         ↓
                    ┌──────────┐
                    │ Decision │  Final verdict + audit trail
                    │  Agent   │
                    └──────────┘
```

Each agent performs an independent assessment, then hands off to the next via @mention in a Band chatroom. The Decision Agent synthesizes all findings into a final recommendation: **AUTO-APPROVE**, **ENHANCED REVIEW**, **ESCALATE TO HUMAN**, or **DECLINE**.

## Tech Stack

- **[Band](https://www.band.ai/)** — Multi-agent coordination platform
- **Band Native REST API** — Direct HTTP/JSON integration for agent communication (using adaptive async polling loops for maximum stability)
- **[Featherless AI](https://featherless.ai/)** — Serverless LLM inference (Qwen models via LangChain)
- **Python 3.11+**

## Project Structure

```
├── policy_agent.py          # Agent 1: Regulatory policy checks
├── risk_agent.py            # Agent 2: Risk analysis and scoring
├── legal_agent.py           # Agent 3: Legal framework review
├── decision_agent.py        # Agent 4: Final decision and audit trail
├── agent_helper.py          # Core runner helper (defines REST tools & polling loop)
├── pipeline_ros2.py         # Coordinator: Orchestrates room lifecycle and CSV flow
├── start_all.py             # Launches all 4 agents + coordinator
├── fix_stuck_messages.py    # Utility: clears stuck message queues
├── transactions.csv         # Input: pending transactions
├── results.csv              # Output: verdicts and audit records
├── agent_config.yaml        # Band agent credentials (not committed)
├── .env                     # API keys (not committed)
└── requirements.txt         # Python dependencies
```

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Add your FEATHERLESS_API_KEY to .env

# Configure agents
cp agent_config.example.yaml agent_config.yaml
# Add your Band agent UUIDs and API keys

# Run everything
python start_all.py
```

## Agent Roles

| Agent | Role | Output |
|-------|------|--------|
| **Policy Agent** | Checks AML, KYC, sanctions, transaction monitoring | Verdict: CLEAR / FLAGGED / BLOCKED |
| **Risk Agent** | Scores risk level, detects anomalies | Score: LOW / MEDIUM / HIGH / CRITICAL |
| **Legal Agent** | Reviews BSA, OFAC, FATF compliance, filing requirements | Verdict: APPROVED / CONDITIONAL / REQUIRES COUNSEL / BLOCKED |
| **Decision Agent** | Synthesizes all assessments, produces final recommendation | Decision: AUTO-APPROVE / ENHANCED REVIEW / ESCALATE / DECLINE |

## Status

🚧 **Work in progress** — Hackathon project (June 12–19, 2026)

- [x] 4 agents connected to Band via Native REST API
- [x] Linear pipeline with @mention handoffs
- [x] Thought events for audit trail transparency
- [x] Automated transaction intake from CSV
- [x] One-command launcher (start_all.py)
- [x] Multi-run stability in same room (achieved by native REST transition)
- [ ] Dynamic specialist recruitment (Level 4)
- [ ] Sub-room delegation for sensitive findings (Level 5)

## Team

- Khoa Đoàn Nguyễn Anh ([@DNAK-dnak](https://github.com/DNAK-dnak))
- Khải Phan Văn
- Vy Lê Tường

## License

MIT
Apache 2.0
