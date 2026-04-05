# Event-Sourced AI Agent with Kafka

**Advanced Data Engineering — Final Project**

An event-driven, multi-step AI agent built on Apache Kafka 3.8.0. Every state change is an immutable event. No service calls another service directly — all coordination happens through Kafka topics.

---

## System Flow

This is the full end-to-end event flow for a single user query:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            SYSTEM FLOW                                      │
│                                                                             │
│   USER                                                                      │
│    │  types question                                                        │
│    ▼                                                                        │
│  userInterface.ts                                                           │
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
│  userInterface.ts  (consumer loop)                                          │
│    │  reads:     FinalAnswerSynthesized matching conversationId             │
│    ▼                                                                        │
│   USER                                                                      │
│    receives answer                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key rule:** every message is keyed by `conversationId` — this guarantees ordering per conversation and enables parallel conversations across 3 partitions.

---

## Kafka Topics

| Topic | Type | Events |
|---|---|---|
| `user-commands` | Commands | `UserQueryReceived`, `SynthesizeFinalAnswerRequested` |
| `conversation-events` | Events (immutable log) | `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `PlanFailed`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | Commands | `ToolInvocationRequested` |
| `dead-letter-queue` | Errors | Unrecoverable failures from orchestrator and router |

4 topics × 3 partitions each.

---

## Services

| Service | File | Consumer Group | Produces to |
|---|---|---|---|
| UserInterface | `src/node/core/userInterface.ts` | `ui-final-answer` | `user-commands` |
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

## Event Schemas

All events share the same envelope:

```typescript
{
  conversationId: string   // UUID — ties all events in one query together
  timestamp:      number   // Unix ms — used for latency measurement
  eventType:      string   // discriminator
  payload:        object   // event-specific data
}
```

JSON Schema files: `src/schemas/` (8 files).  
TypeScript interfaces: `shared/schemas/` (barrel export with discriminated unions).

---

## Orchestrator State Machine

The Orchestrator is the only stateful service. It uses **LevelDB** (`.plan-store/`) keyed by `conversationId`:

```
PlanGenerated received
  → savePlan({ plan, stepIndex: 0, results: [], status: "pending" })
  → dispatch step[0] → ToolInvocationRequested

ToolInvocationResulted received
  → updatePlan({ results: [...prev, result], stepIndex: stepIndex + 1 })
  → if stepIndex < plan.length:
      resolve {{step_N.result}} placeholders in next step args
      dispatch next step → ToolInvocationRequested
  → if stepIndex === plan.length:
      emit PlanCompleted
      deletePlan()
  → if error:
      emit PlanFailed + dead-letter-queue
      deletePlan()
```

On crash + restart: LevelDB state survives. Unprocessed `PlanGenerated` events are replayed from Kafka consumer group offset.

---

## Setup & Run

### Prerequisites
- Docker + docker-compose
- Bun 1.0+
- `OPENAI_API_KEY` in `.env`

### One-time setup

```bash
cp .env.example .env          # add OPENAI_API_KEY

# Start Kafka (KRaft — no ZooKeeper required), ChromaDB, Ollama
# KRaft is Kafka's built-in consensus mechanism (Kafka 3.3+), replacing ZooKeeper
docker-compose -f infra/docker-compose.yml up -d

bash infra/topics-final.sh    # create 4 topics with 3 partitions each
bun install

# Python RAG setup (one-time)
cd src/python
python3 -m venv venv && source venv/bin/activate
pip install -r rag/requirements.txt
python rag/index_kb.py        # index data/products/ into ChromaDB (53 chunks)
cd ../..
```

### Run

```bash
bun run start    # start 9 background services (logs → scripts/logs/final-project-services/)
bun run ui       # start interactive terminal (separate window)
bun run stop     # stop everything
```

Individual services: `bun run router` / `bun run orchestrator` / `bun run math` / etc.

---

## CQRS Pattern

```
COMMANDS (write-side intent)          EVENTS (read-side facts)
────────────────────────────          ────────────────────────
topic: user-commands                  topic: conversation-events
  UserQueryReceived                     PlanGenerated
  SynthesizeFinalAnswerRequested        ToolInvocationResulted
                                        PlanCompleted / PlanFailed
topic: tool-invocation-requests         FinalAnswerSynthesized
  ToolInvocationRequested

Aggregator = CQRS bridge
  reads:    PlanCompleted    (read-side, conversation-events)
  emits:    SynthesizeFinalAnswerRequested  (write-side, user-commands)
```

---

## Event Sourcing & Stateful Stream Processing

### Event Sourcing in This System

Traditional systems store the *current state* — a single row in a database that gets overwritten on every update. This system uses **Event Sourcing**: the Kafka `conversation-events` topic is the only source of truth. Every state change is an immutable, append-only fact:

```
UserQueryReceived  →  PlanGenerated  →  ToolInvocationResulted (×N)  →  PlanCompleted  →  FinalAnswerSynthesized
```

No event is ever deleted or modified. The current state of any conversation is always derivable by replaying its events from offset 0. Three properties follow directly:

- **Resilience** — no event is lost on service crash; the consumer group offset marks where replay resumes.
- **Recoverability** — an orchestrator that starts from scratch reconstructs full plan state by replaying `PlanGenerated` + all `ToolInvocationResulted` events for the in-flight conversation.
- **Auditability** — every tool call, every result, and the final answer are permanently recorded with `conversationId` and `timestamp`. The full history of any query is queryable at any time.

### Stateful Stream Processing — Orchestrator

The Orchestrator is the canonical example of stateful stream processing in this system. It does not query an external database to decide what to do next — it *builds its state incrementally from the event stream*:

```
Event arrives: PlanGenerated
  → extract plan steps and stepIndex=0
  → persist to LevelDB (fault-tolerant state store)
  → dispatch step[0] as ToolInvocationRequested

Event arrives: ToolInvocationResulted (step N)
  → load plan from LevelDB by conversationId
  → append result, increment stepIndex
  → resolve {{step_N.result}} placeholders in next step's args (chained data flow)
  → if more steps: dispatch next ToolInvocationRequested
  → if done: emit PlanCompleted, delete plan from LevelDB
  → if error: emit PlanFailed, publish to dead-letter-queue
```

**Why LevelDB and not in-memory?** An in-memory `Map` would lose state on any process restart. LevelDB is an embedded key-value store that persists to disk. On restart, the orchestrator reads all in-progress plans from LevelDB and picks up exactly where it left off — without replaying the entire Kafka topic from the beginning. The Kafka consumer group offset handles replaying the *trigger* (`PlanGenerated`), while LevelDB provides fast state access per `conversationId`.

### Stateful Stream Processing — Aggregator

The Aggregator is a lighter stateful component. It listens for `PlanCompleted` events on `conversation-events`, then reads all `ToolInvocationResulted` events embedded in the `PlanCompleted` payload (the orchestrator includes the full results array) and emits a `SynthesizeFinalAnswerRequested` command to `user-commands`. It acts as the **CQRS bridge**: converting a read-side event (plan is done) into a write-side command (synthesize the answer).

### Schema Registry (Conceptual)

JSON Schema files in `src/schemas/` define the contract for each event type. Every producer validates its output against the schema before publishing. This prevents malformed events from entering `conversation-events` and corrupting downstream state. The 8 schema files map to the 8 event/command types:

| Schema file | Event/Command |
|---|---|
| `UserQueryReceived.json` | User query command |
| `PlanGenerated.json` | Router output |
| `ToolInvocationRequested.json` | Orchestrator dispatch |
| `ToolInvocationResulted.json` | Worker result |
| `PlanCompleted.json` | All steps done |
| `PlanFailed.json` | Unrecoverable plan error |
| `SynthesizeFinalAnswerRequested.json` | Aggregator bridge command |
| `FinalAnswerSynthesized.json` | Final answer event |

---

## Resilience

| Scenario | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka retains unacknowledged messages at last committed offset. Worker rejoins consumer group on restart and replays from offset. | Automatic |
| Orchestrator crash | LevelDB persists plan state. On restart, orchestrator reads LevelDB for in-progress plans and resumes from `stepIndex`. Unprocessed `PlanGenerated` events are replayed via Kafka offset. | Automatic |
| Duplicate events | Workers filter by `toolName` (process only their own events). Orchestrator drops `ToolInvocationResulted` for unknown `conversationId` (plan already deleted from LevelDB). | Silent drop |
| Unrecoverable error | Router and orchestrator publish to `dead-letter-queue` on failures that cannot be retried. | Manual inspection |

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

The Kafka pipeline adds under 50 ms. 97% of latency is LLM inference.

Full report: [`docs/benchmark.md`](docs/benchmark.md)

---

## Execution Scenarios

Full log: [`docs/execution-log.txt`](docs/execution-log.txt)

| Scenario | Plan | Result |
|---|---|---|
| Weather + Exchange | `[weather, exchange]` | 28°C Tel Aviv + 100 USD = 370 ILS |
| Math + Weather | `[math, weather]` | 25×8=200, London 12°C |
| iPhone cost in Germany | `[getProductInformation, exchange]` | 3700 ILS → 925 EUR (chained) |
| iPhones per Tesla | `[getProductInformation, getProductInformation]` | 100 iPhones |
| São Paulo Tesla decision | `[getProductInformation, weather, chat]` | 27°C < 30°C → go out |
| RAG — Tesla Model 3 | `[getProductInformation]` | ChromaDB score 0.69 |
| RAG — MacBook Pro | `[getProductInformation]` | ChromaDB score 0.61 |
| Guardrail — bomb | `[chat]` | Refused correctly |

---

## Conclusions

### Why Kafka as Event Store

Every state change is an immutable event appended to a topic. In a conventional REST/RPC system, state lives in a single mutable record — a crash between writes can leave it inconsistent. With Event Sourcing on Kafka, state is derived from an ordered log of facts that can never be overwritten:

- **Resilience** — crashed services resume from their last committed Kafka offset. No events are lost because Kafka retains messages on disk independent of consumer availability.
- **Recoverability** — any conversation can be reconstructed by replaying `conversation-events` from offset 0. The orchestrator rebuilds its plan state on restart without an external backup.
- **Auditability** — every tool call, result, and answer is permanently recorded with `conversationId` and `timestamp`. The full lifecycle of any query — from receipt to final answer — is queryable at any time for debugging, analytics, or compliance.

This is why Kafka is preferable to a traditional DB as the primary state store here: it provides ordering guarantees, consumer group offset management, and a durable event history in one primitive.

### Stateful Processing Power

The Orchestrator demonstrates event-stream-driven state accumulation. Rather than polling a database or receiving synchronous callbacks, it reacts to events:

- `PlanGenerated` → initialize state in LevelDB (plan steps, `stepIndex=0`, empty results)
- `ToolInvocationResulted` → load state from LevelDB, append result, increment `stepIndex`, resolve `{{step_N.result}}` placeholders, dispatch next step or emit `PlanCompleted`

State grows incrementally as events arrive. `planReceivedAt` enables latency measurement. All state is derived from event data — no external metrics system or secondary database is needed. The LevelDB persistence layer means state survives process restarts; the Kafka consumer group offset means the trigger event (`PlanGenerated`) is replayed on restart even if LevelDB already has the state, providing a double safety net.

The Aggregator applies the same pattern at a smaller scale: it reacts to `PlanCompleted` and bridges the read-side event log to a write-side command, emitting `SynthesizeFinalAnswerRequested` with all intermediate results.

### CQRS and Idempotency

Separating commands (`user-commands`, `tool-invocation-requests`) from events (`conversation-events`) means producers and consumers are fully decoupled. A new consumer can subscribe to `conversation-events` and reconstruct system history without touching any command topic.

Workers are idempotent by design: the same `ToolInvocationRequested` input always produces the same `ToolInvocationResulted` output. This is essential in a distributed system where Kafka guarantees *at-least-once* delivery — a message may be redelivered after a crash. The Orchestrator's `conversationId` guard provides the second layer: `ToolInvocationResulted` events for an unknown (already-completed or deleted) conversation are silently dropped, preventing double answers or corrupted state.

### Trade-offs

| Trade-off | Detail |
|---|---|
| Eventual consistency | The UI polls `conversation-events` for the answer — no synchronous request/response |
| Debugging complexity | A single query produces 8–12 events across 4 topics — correlate by `conversationId` |
| Operational overhead | 9 processes + Kafka + ChromaDB vs a single REST API |
| Latency floor | ~50 ms Kafka round-trip per hop (negligible vs LLM inference) |
| Sequential execution | Orchestrator dispatches one step at a time — independent steps are not parallelised |

### Future Improvements

- **Kafka Streams DSL** — replace manual consumer loops with stateful topology
- **Confluent Schema Registry** — runtime contract enforcement + backward compatibility
- **Parallel tool dispatch** — dispatch independent plan steps concurrently
- **Grafana + Prometheus** — replace `[Benchmark]` log lines with real metrics
- **Durable synthesizer cache** — back the `userInput` cache with LevelDB
- **Multi-broker cluster** — replication factor ≥ 2 for production resilience

---

## Tech Stack

| Layer | Technology |
|---|---|
| Broker | Apache Kafka 3.8.0 (KRaft) |
| Runtime | Bun 1.0+ (TypeScript) |
| LLM | OpenAI gpt-4o-mini |
| Vector DB | ChromaDB + sentence-transformers/all-MiniLM-L6-v2 |
| State store | LevelDB (via `level` npm package) |
| Infrastructure | Docker + docker-compose |

---

## File Structure

```
├── infra/
│   ├── docker-compose.yml        # Kafka (KRaft) + ChromaDB + Ollama
│   └── topics-final.sh           # create 4 topics × 3 partitions
├── scripts/
│   ├── start-final.sh            # launch 9 background services
│   ├── stop-all.sh               # stop everything
│   └── logs/final-project-services/   # per-service log files
├── src/
│   ├── node/
│   │   ├── core/                 # userInterface.ts, routerService.ts
│   │   ├── orchestration/        # orchestrator.ts, aggregator.ts, answerSynthesizer.ts
│   │   └── apps/                 # mathApp, weatherApp, exchangeApp, generalChatApp
│   └── python/
│       └── rag/                  # rag_retriever.py, index_kb.py
├── shared/
│   ├── kafka/client.ts           # KafkaJS wrapper
│   ├── schemas/                  # TypeScript event interfaces
│   ├── prompts/                  # routerPlanPrompt.ts, synthesisPrompt.ts
│   ├── state/planStore.ts        # LevelDB plan persistence
│   └── topics.ts                 # topic name constants
├── src/schemas/                  # JSON Schema files (8 event types)
└── data/products/                # iphone.txt, macbook.txt, tesla.txt (RAG knowledge base)
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
