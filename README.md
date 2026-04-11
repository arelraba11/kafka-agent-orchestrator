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
│  webServer.ts                                                               │
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
│  webServer.ts  (consumer loop)                                              │
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
bun run start    # start background services incl. webServer (logs → scripts/logs/final-project-services/)

# Open the Web UI at http://localhost:3001
# Or for hot-reload dev mode:
bun run web:dev    # http://localhost:5173 (proxies /ws → port 3001)

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

Separating commands from events means each service has a single, narrow responsibility: producers never need to know who consumes their output, and consumers never mutate shared state. This makes each microservice independently deployable, testable, and replaceable without breaking the rest of the pipeline.

### Idempotency
Workers filter by `toolName` — process only their own events. Orchestrator silently drops `ToolInvocationResulted` for unknown `conversationId` (already completed/deleted). Handles Kafka's at-least-once delivery safely. Combined with CQRS, this means any event can be replayed without side effects — a critical property for crash recovery and debugging.

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

Measured across 27 live queries (2026-04-05), with session history injection enabled:

| Component | Model | Avg latency | Throughput | Accuracy (1–5) | Cost |
|---|---|---|---|---|---|
| RouterService | gpt-4o-mini | ~1,113 ms | ~0.9 events/s | 5 — correct plan on all 27 queries | ~$0.001/query |
| AnswerSynthesizer | gpt-4o-mini | ~2,189 ms | ~0.5 events/s | 5 — coherent, history-aware | ~$0.002/query |
| RAG Retriever | sentence-transformers (local) | ~65 ms | ~15 events/s | 4 — top-3 semantic search | $0 |
| Tool workers (math/weather/exchange/chat) | — | ~1 ms | ~1000 events/s | 3–5 | $0 |
| Orchestrator (state machine) | — | ~1 ms/step | ~1000 events/s | 5 — correct sequencing | $0 |
| **End-to-end** | | **~3,341 ms** | | **5** | **~$0.003/query** |

Kafka pipeline overhead (all orchestration + tool workers + Kafka round-trips): **< 50 ms**. LLM inference accounts for **~99% of end-to-end latency**.

Full report: [`docs/benchmark.md`](docs/benchmark.md)

---

## Conclusions

**Why Kafka as event store:** Event Sourcing on Kafka provides ordering guarantees, consumer group offset management, and a durable event history in one primitive — impossible to replicate with a mutable DB row. Every action is permanently auditable: replay any `conversationId` from offset 0 to reconstruct exactly what happened, when, and in what order.

**Stateful processing:** The Orchestrator accumulates state incrementally from the event stream rather than polling a DB. `{{step_N.result}}` placeholder resolution enables chained tool data flow without any shared memory. The Aggregator is fully stateless — by the time `PlanCompleted` arrives, all results are embedded in its payload.

**CQRS + Idempotency advantages:** Separating commands (`user-commands`, `tool-invocation-requests`) from facts (`conversation-events`) means each service is independently deployable and testable. Idempotent workers (toolName filter + conversationId guard) make the pipeline safe under Kafka's at-least-once delivery — duplicate events are silently dropped, never double-processed.

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
│   ├── start.ts                       # launch 9 background services
│   ├── stop.ts
│   └── logs/final-project-services/   # per-service log files
├── src/
│   ├── frontend/                      # React 19 + Vite web UI
│   ├── node/
│   │   ├── core/                      # routerService.ts, webServer.ts
│   │   ├── orchestration/             # orchestrator.ts, aggregator.ts, answerSynthesizer.ts
│   │   └── apps/                      # mathApp, weatherApp, exchangeApp, generalChatApp
│   └── python/
│       └── rag/                       # rag_retriever.py, index_kb.py
├── shared/
│   ├── kafka/client.ts                # KafkaJS wrapper
│   ├── schemas/                       # TypeScript event interfaces + JSON Schema files (8 event types)
│   ├── prompts/                       # LLM prompt functions
│   ├── state/planStore.ts             # LevelDB plan persistence
│   └── topics.ts                      # topic name constants
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
