# Event-Sourced AI Agent with Kafka

**Advanced Data Engineering — Final Project**

An event-driven, multi-step AI agent built on Apache Kafka 3.8.0. Every state change is an immutable event. No service calls another service directly — all coordination happens through Kafka topics.

---

## System Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            SYSTEM FLOW                                      │
│                                                                             │
│   USER                                                                      │
│    │  types question                                                        │
│    ▼                                                                        │
│  userInterface.ts / webServer.ts                                            │
│    │  produces:  UserQueryReceived { conversationId, userInput }            │
│    │  topic:     user-commands                                              │
│    ▼                                                                        │
│  routerService.ts                                                           │
│    │  reads:     UserQueryReceived from user-commands                       │
│    │  action:    calls OpenAI gpt-4o-mini → generates tool plan             │
│    │  produces:  PlanGenerated { conversationId, steps[] }                  │
│    │  topic:     conversation-events                                        │
│    ▼                                                                        │
│  orchestrator.ts  ◄──── LevelDB (.plan-store/)                              │
│    │  reads:     PlanGenerated from conversation-events                     │
│    │  action:    saves plan to LevelDB, dispatches step 1                   │
│    │  produces:  ToolInvocationRequested { conversationId, toolName, args } │
│    │  topic:     tool-invocation-requests                                   │
│    ▼                                                                        │
│  [Tool Workers — each is an independent consumer group]                     │
│    ├── mathApp.ts          → handles toolName="math"                        │
│    ├── weatherApp.ts       → handles toolName="weather"                     │
│    ├── exchangeApp.ts      → handles toolName="exchange"                    │
│    ├── generalChatApp.ts   → handles toolName="chat"                        │
│    └── rag_retriever.py    → handles toolName="getProductInformation"       │
│    │                                                                        │
│    │  each worker produces: ToolInvocationResulted { result }               │
│    │  topic:                conversation-events                             │
│    ▼                                                                        │
│  orchestrator.ts  (continues)                                               │
│    │  reads:     ToolInvocationResulted from conversation-events            │
│    │  action:    updates LevelDB state, resolves {{step_N.result}}          │
│    │             if more steps → dispatch next ToolInvocationRequested      │
│    │             if all steps done:                                         │
│    │  produces:  PlanCompleted { conversationId, results[] }                │
│    │  topic:     conversation-events                                        │
│    │             on error:                                                  │
│    │  produces:  PlanFailed + dead-letter-queue entry                       │
│    ▼                                                                        │
│  aggregator.ts                                                              │
│    │  reads:     PlanCompleted from conversation-events                     │
│    │  produces:  SynthesizeFinalAnswerRequested { results[] }               │
│    │  topic:     user-commands                                              │
│    ▼                                                                        │
│  answerSynthesizer.ts                                                       │
│    │  reads:     SynthesizeFinalAnswerRequested from user-commands          │
│    │  action:    calls OpenAI gpt-4o-mini → synthesizes final answer        │
│    │  produces:  FinalAnswerSynthesized { answer }                          │
│    │  topic:     conversation-events                                        │
│    ▼                                                                        │
│  userInterface.ts / webServer.ts  (consumer loop)                           │
│    │  reads:     FinalAnswerSynthesized matching conversationId             │
│    ▼                                                                        │
│   USER                                                                      │
│    receives answer                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key rule:** every message is keyed by `conversationId` — guarantees ordering per conversation and enables parallel conversations across 3 partitions.

---

## Kafka Topics

| Topic | Type | Events |
|---|---|---|
| `user-commands` | Commands | `UserQueryReceived`, `SynthesizeFinalAnswerRequested` |
| `conversation-events` | Events (immutable log) | `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `PlanFailed`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | Commands | `ToolInvocationRequested` |
| `dead-letter-queue` | Errors | Unrecoverable failures |

4 topics × 3 partitions each.

---

## Services

| Service | File | Consumer Group | Produces to |
|---|---|---|---|
| UserInterface | `src/node/core/userInterface.ts` | `ui-final-answer` | `user-commands` |
| WebServer | `src/node/core/webServer.ts` | `ui-web-final-answer` | `user-commands` |
| RouterService | `src/node/core/routerService.ts` | `router-plan-service` | `conversation-events` |
| Orchestrator | `src/node/orchestration/orchestrator.ts` | `orchestrator-service` | `tool-invocation-requests`, `conversation-events` |
| Aggregator | `src/node/orchestration/aggregator.ts` | `aggregator-service` | `user-commands` |
| AnswerSynthesizer | `src/node/orchestration/answerSynthesizer.ts` | `answer-synthesizer` | `conversation-events` |
| mathApp | `src/node/apps/mathApp.ts` | `math-tool-worker` | `conversation-events` |
| weatherApp | `src/node/apps/weatherApp.ts` | `weather-tool-worker` | `conversation-events` |
| exchangeApp | `src/node/apps/exchangeApp.ts` | `exchange-tool-worker` | `conversation-events` |
| generalChatApp | `src/node/apps/generalChatApp.ts` | `chat-tool-worker` | `conversation-events` |
| RAG Retriever | `src/python/rag/rag_retriever.py` | `rag-tool-worker` | `conversation-events` |

---

## Setup & Run

### Prerequisites
- Docker + docker-compose
- Bun 1.0+
- `OPENAI_API_KEY` in `.env`

### One-time setup

```bash
cp .env.example .env          # add OPENAI_API_KEY
docker-compose -f infra/docker-compose.yml up -d
bash infra/topics-final.sh    # create 4 topics × 3 partitions
bun install

# Python RAG (one-time)
cd src/python
python3 -m venv venv && source venv/bin/activate
pip install -r rag/requirements.txt
python rag/index_kb.py        # index data/products/ into ChromaDB
cd ../..
```

### Run

```bash
bun run start    # start 10 background services incl. webServer (logs → scripts/logs/final-project-services/)

# Option A — Terminal UI
bun run ui

# Option B — Web UI (webServer already running, just start Vite)
bun run web:dev    # http://localhost:5173 (hot reload, proxies /ws → port 3001)

bun run stop     # stop everything
```

---

## Architecture Concepts

### Event Sourcing
`conversation-events` is the only source of truth — an immutable, append-only log. The current state of any conversation is always derivable by replaying its events from offset 0. This gives:
- **Resilience** — no events are lost on crash; consumer group offset marks where replay resumes
- **Recoverability** — orchestrator rebuilds plan state on restart by replaying events
- **Auditability** — every tool call, result, and answer is permanently recorded with `conversationId` + `timestamp`

### Stateful Stream Processing — Orchestrator
The only stateful service. Uses **LevelDB** (`.plan-store/`) keyed by `conversationId`:

```
PlanGenerated  → save plan (stepIndex=0, results=[]) → dispatch step[0]
ToolInvocationResulted → load state → append result → resolve {{step_N.result}}
  → if more steps: dispatch next step
  → if done: emit PlanCompleted, delete plan
  → if error: emit PlanFailed, publish to dead-letter-queue
```

LevelDB survives restarts. Kafka consumer group offset replays unprocessed triggers.

### CQRS

```
COMMANDS (write-side)                 EVENTS (read-side facts)
─────────────────────                 ────────────────────────
user-commands                         conversation-events
  UserQueryReceived                     PlanGenerated
  SynthesizeFinalAnswerRequested        ToolInvocationResulted
tool-invocation-requests                PlanCompleted / PlanFailed
  ToolInvocationRequested               FinalAnswerSynthesized

Aggregator = CQRS bridge: PlanCompleted (event) → SynthesizeFinalAnswerRequested (command)
```

### Idempotency
Workers filter by `toolName` — process only their own events. Orchestrator silently drops `ToolInvocationResulted` for unknown `conversationId` (already completed/deleted). Handles Kafka's at-least-once delivery safely.

---

## Resilience

| Scenario | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka retains messages at last committed offset; worker rejoins consumer group on restart | Automatic |
| Orchestrator crash | LevelDB persists plan state; resumes from `stepIndex` on restart | Automatic |
| Duplicate events | Workers filter by `toolName`; orchestrator drops unknown `conversationId` | Silent drop |
| Unrecoverable error | Router + orchestrator publish to `dead-letter-queue` | Manual inspection |

Full reproduction steps: [`docs/resilience-tests/resilience-demo.md`](docs/resilience-tests/resilience-demo.md)

---

## Benchmark

Measured across 25 live queries (2026-04-05):

| Component | Avg latency | Notes |
|---|---|---|
| RouterService | ~1135 ms | LLM plan generation (gpt-4o-mini) |
| Tool workers (simple) | ~14 ms | weather, exchange, math, chat |
| Tool workers (RAG) | ~120 ms | ChromaDB retrieval + embedding |
| AnswerSynthesizer | ~1687 ms | LLM synthesis (gpt-4o-mini) |
| **End-to-end** | **~2868 ms** | **~2.9 s average** |

Kafka pipeline overhead: < 50 ms. 97% of latency is LLM inference.

Full report: [`docs/benchmark.md`](docs/benchmark.md)

---

## Conclusions

**Why Kafka as event store:** Event Sourcing on Kafka provides ordering guarantees, consumer group offset management, and a durable event history in one primitive — impossible to replicate with a mutable DB row.

**Stateful processing:** The Orchestrator accumulates state incrementally from the event stream rather than polling a DB. `{{step_N.result}}` placeholder resolution enables chained tool data flow without any shared memory.

**Trade-offs:**

| Trade-off | Detail |
|---|---|
| Eventual consistency | UI polls `conversation-events` — no synchronous request/response |
| Debugging complexity | A single query produces 8–12 events across 4 topics — correlate by `conversationId` |
| Operational overhead | 9 processes + Kafka + ChromaDB vs a single REST API |
| Sequential execution | Orchestrator dispatches one step at a time — independent steps are not parallelised |

**Future improvements:** Kafka Streams DSL, Confluent Schema Registry, parallel tool dispatch, Grafana + Prometheus observability, multi-broker cluster.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Broker | Apache Kafka 3.8.0 (KRaft — no ZooKeeper) |
| Runtime | Bun 1.0+ (TypeScript) |
| Frontend | React 19 + Vite + Tailwind CSS v4 |
| LLM | OpenAI gpt-4o-mini |
| Vector DB | ChromaDB + sentence-transformers/all-MiniLM-L6-v2 |
| State store | LevelDB (via `level` npm package) |
| Infrastructure | Docker + docker-compose |

---

## File Structure

```
├── infra/
│   ├── docker-compose.yml             # Kafka (KRaft) + ChromaDB
│   └── topics-final.sh                # create 4 topics × 3 partitions
├── scripts/
│   ├── start-final.sh                 # launch 9 background services
│   ├── stop-all.sh
│   └── logs/final-project-services/   # per-service log files
├── src/
│   ├── frontend/                      # React 19 + Vite web UI
│   ├── node/
│   │   ├── core/                      # userInterface.ts, routerService.ts, webServer.ts
│   │   ├── orchestration/             # orchestrator.ts, aggregator.ts, answerSynthesizer.ts
│   │   └── apps/                      # mathApp, weatherApp, exchangeApp, generalChatApp
│   └── python/
│       └── rag/                       # rag_retriever.py, index_kb.py
├── shared/
│   ├── kafka/client.ts                # KafkaJS wrapper
│   ├── schemas/                       # TypeScript event interfaces
│   ├── prompts/                       # LLM prompt functions
│   ├── state/planStore.ts             # LevelDB plan persistence
│   └── topics.ts                      # topic name constants
├── src/schemas/                       # JSON Schema files (8 event types)
└── data/products/                     # iphone.txt, macbook.txt, tesla.txt (RAG knowledge base)
```

---

## Docs

| Document | Content |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture + event schemas |
| [`docs/benchmark.md`](docs/benchmark.md) | Latency measurements across 25 queries |
| [`docs/execution-log.txt`](docs/execution-log.txt) | Full execution log — 14 scenarios |
| [`docs/demo-scenarios/demo-scenarios.md`](docs/demo-scenarios/demo-scenarios.md) | Step-by-step demo with real log output |
| [`docs/resilience-tests/resilience-demo.md`](docs/resilience-tests/resilience-demo.md) | Crash + recovery reproduction steps |
