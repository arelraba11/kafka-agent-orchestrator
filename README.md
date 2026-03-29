# Final Project: Event-Sourced Hybrid AI Agent with Kafka


**Advanced Data Engineering — Final Submission**

---

## Project Overview

This project implements an **event-sourced, multi-step AI agent** built on Apache Kafka 3.8.0 (KRaft). Every component is a stateless microservice that communicates exclusively through Kafka topics — no service calls another service directly. Coordination is achieved entirely through immutable, ordered events.

The system accepts a natural-language query from a user, plans a sequence of tool calls, executes each tool in order, and synthesizes a single coherent answer — entirely through asynchronous Kafka event flows.

**Goal:** Demonstrate that Kafka is a viable, low-latency backbone for AI agent orchestration, replacing fragile point-to-point RPC with durable, replayable, loosely coupled event streams. The architecture supports horizontal scaling, fault isolation, and plan recovery across process restarts.

The project also includes four progressive exercises (Ex1–Ex4) that build foundational Kafka skills culminating in the Final Project architecture.

---

## How This Implementation Satisfies the Course Requirements

### Event Sourcing

Every state change in the pipeline is an immutable event published to a Kafka topic. The `conversation-events` topic is the append-only log that captures the full lifecycle of a conversation: `PlanGenerated → ToolInvocationResulted (×N) → PlanCompleted → FinalAnswerSynthesized`. Any service can replay this topic from offset 0 to reconstruct what happened to any `conversationId`.

Tool workers are purely functional: each consumes one event, computes a result, and emits one event. They hold no state and are trivially replayable.

### Stateful Stream Processing

The Orchestrator is the only stateful component. It uses **LevelDB** as a local embedded key-value store (keyed by `conversationId`) to persist plan state across process restarts. The state machine transitions are: `savePlan()` on `PlanGenerated`, `updatePlan()` on each `ToolInvocationResulted`, and `deletePlan()` after `PlanCompleted` is published. If the Orchestrator crashes mid-plan, `ToolInvocationResulted` events are already durable in Kafka; on restart, the plan is restored from LevelDB and execution resumes from `stepIndex`.

### Distributed Orchestration

The Orchestrator dispatches tool steps sequentially — one `ToolInvocationRequested` at a time — and collects results from independent worker processes. Each worker runs as a separate process with its own consumer group. The `conversationId` is used as the Kafka message key, guaranteeing partition-level ordering per conversation and enabling concurrent execution of multiple conversations across the three-partition topics.

### CQRS

Commands (write-side intent) and events (read-side facts) flow on separate topics:

- **Commands:** `user-commands` carries `UserQueryReceived` (user intent) and `SynthesizeFinalAnswerRequested` (synthesis trigger).
- **Events:** `conversation-events` carries all domain facts (`PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `FinalAnswerSynthesized`).
- **Tool dispatch:** `tool-invocation-requests` carries `ToolInvocationRequested` — write-side commands to workers.

The Aggregator acts as the CQRS bridge: it reads the read-side (`PlanCompleted` on `conversation-events`) and emits a write-side command (`SynthesizeFinalAnswerRequested` on `user-commands`).

### Fault Tolerance / Resilience

- **Worker crash recovery:** When a worker process is killed and restarted, its consumer group rejoins Kafka, picks up from its last committed offset, and processes any pending `ToolInvocationRequested` events. Demonstrated in the resilience scenario (see Execution Scenarios below).
- **Orchestrator restart recovery:** LevelDB persists `in_progress` plans. On restart, the Orchestrator can resume dispatching from `stepIndex`.
- **Idempotency guards:** `ToolInvocationResulted` events for unknown `conversationId`s (plan already completed or state deleted) are silently dropped rather than causing errors.
- **Dead-letter queue:** Any service that encounters an unrecoverable error publishes to `dead-letter-queue` for manual inspection.

### Microservices Architecture

Eight independent services, each in its own file, each with its own consumer group, each deployable and restartable independently:

| Service | Binary | Responsibility |
|---|---|---|
| UserInterface | `src/node/core/userInterface.ts` | Stdin/stdout bridge |
| RouterService | `src/node/core/routerService.ts` | Plan generation |
| Orchestrator | `src/node/orchestration/orchestrator.ts` | State machine + tool dispatch |
| Aggregator | `src/node/orchestration/aggregator.ts` | Orchestration / synthesis bridge |
| AnswerSynthesizer | `src/node/orchestration/answerSynthesizer.ts` | LLM synthesis |
| mathApp | `src/node/apps/mathApp.ts` | Math tool worker |
| weatherApp | `src/node/apps/weatherApp.ts` | Weather tool worker |
| exchangeApp | `src/node/apps/exchangeApp.ts` | Currency exchange worker |
| generalChatApp | `src/node/apps/generalChatApp.ts` | Chat / RAG worker |
| RAG Retriever | `src/python/rag/rag_retriever.py` | Product knowledge retrieval |

---

## Architecture

Full architecture diagram: [`docs/architecture.md`](docs/architecture.md)

### System Flow

```
stdin
  │
UserInterface ──────────────────────────── UserQueryReceived ──────────────┐
                           │                                               │
                     [user-commands]                                       │
                           │                                               │
                    RouterService                                          │
                           │  PlanGenerated                                │
                  [conversation-events]                                    │
                           │                                               │
                   Orchestrator ◄─── LevelDB (.plan-store/)                │
                           │  ToolInvocationRequested (one per step)       │
               [tool-invocation-requests]                                  │
                           │                                               │
       ┌───────────────────┼──────────────────┬────────────────────┐       │
    mathApp          weatherApp          exchangeApp        generalChatApp  │
    (+ RAG via Python rag_retriever.py)                                    │
       └───────────────────┴──────────────────┴────────────────────┘       │
                           │  ToolInvocationResulted                       │
                  [conversation-events]                                    │
                           │                                               │
                   Orchestrator ──── PlanCompleted ────────────────────────┤
                           │                                               │
                  [conversation-events]                                    │
                           │                                               │
                    Aggregator                                             │
                           │  SynthesizeFinalAnswerRequested               │
                     [user-commands]                                       │
                           │                                               │
                  AnswerSynthesizer ──── FinalAnswerSynthesized ───────────┘
                           │
                  [conversation-events]
                           │
                   UserInterface → stdout
```

### Kafka Topics

All topic name constants are defined in `shared/topics.ts`. Topic strings are never hardcoded in service code.

| Topic | Partitions | Purpose |
|---|---|---|
| `user-commands` | 3 | Commands from UserInterface (`UserQueryReceived`) and Aggregator (`SynthesizeFinalAnswerRequested`) |
| `conversation-events` | 3 | All domain events: `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | 3 | Tool dispatch requests from the Orchestrator, one per step |
| `dead-letter-queue` | 3 | Unrecoverable errors from any service — for manual inspection |

`conversationId` is used as the Kafka message key, guaranteeing ordering per conversation and enabling up to three concurrent conversations across the three-partition topics without head-of-line blocking.

---

## Repository Structure

```
kafka-beginners-course-main/
├── infra/
│   ├── docker-compose.yml          # Kafka (KRaft) + Ollama + ChromaDB
│   ├── topics.sh                   # Creates all 21 Kafka topics (1 partition)
│   └── topics-final.sh             # Recreates final-project topics (3 partitions)
├── scripts/
│   ├── start-final.sh              # Final Project launcher (8 background services)
│   ├── start-ex{1-4}.sh            # Per-exercise background service launchers
│   ├── stop-all.sh                 # pkill -f bun
│   └── logs/                       # Per-service log files (gitignored)
│       ├── final-project-services/ # router.log, orchestrator.log, answer.log, ...
│       └── ex{1-4}-services/
├── src/
│   ├── node/
│   │   ├── core/                   # userInterface, routerService, memoryService, responseAggregator
│   │   ├── apps/                   # mathApp, weatherApp, exchangeApp, generalChatApp (dual-mode)
│   │   ├── llm/                    # guardrailService, llmRouterService, cotMathService (Ex2)
│   │   ├── orchestration/          # orchestrator, aggregator, answerSynthesizer (Final)
│   │   ├── reviews/                # reviewProducer, reviewProcessor, reviewAnalytics (Ex3)
│   │   └── customer-support/       # customerSupportProducer, sanitizer, sentiment, urgency, insightAggregator (Ex4)
│   ├── python/
│   │   └── rag/                    # rag_retriever.py, index_kb.py, requirements.txt
│   └── schemas/                    # JSON Schema (draft-07) for all Final Project events
├── shared/
│   ├── kafka/client.ts             # KafkaJS factory: createProducer, createConsumer, sendMessage, subscribeAndRun
│   ├── llm/openai.ts               # callLLM(prompt) → gpt-4o-mini, strips markdown fences
│   ├── topics.ts                   # All topic name constants — import here, never hardcode
│   ├── state/planStore.ts          # LevelDB-backed orchestrator plan store
│   ├── schemas/                    # TypeScript interfaces for Final Project events
│   ├── types/                      # TypeScript interfaces for Ex1–4 events and domain types
│   ├── prompts/                    # All LLM prompt functions — never inline prompt strings
│   └── customerSupport/            # benchmark helpers (Ex4)
├── data/
│   └── products/                   # RAG knowledge base: iphone.txt, macbook.txt, tesla.txt
├── docs/
│   ├── architecture.md             # Full system architecture (this project's source of truth)
│   ├── benchmark.md                # Measured latency results
│   ├── execution-log.txt           # Three annotated end-to-end execution traces
│   ├── demo-scenarios/             # Demo scripts
│   └── resilience-tests/           # Resilience test procedures and results
```

---

## How to Run

### Prerequisites

- Docker + docker-compose
- Bun 1.0+
- `OPENAI_API_KEY` (exercises 2, 3, 4, and Final Project)
- Ollama with `llama3` pulled (Exercise 4 sanitizer only)

### Infrastructure Setup (once)

```bash
# Copy and configure environment variables
cp .env.example .env
# Set OPENAI_API_KEY and ROUTER_MODE in .env

# Start Kafka (KRaft), Ollama, and ChromaDB
docker-compose -f infra/docker-compose.yml up -d

# Create all 21 topics (1 partition each)
bash infra/topics.sh

# Upgrade the 4 Final Project topics to 3 partitions
bash infra/topics-final.sh

# Install TypeScript dependencies
bun install
```

| Variable | Required for | Value |
|---|---|---|
| `OPENAI_API_KEY` | Ex2, Ex3, Ex4, Final | `sk-...` |
| `ROUTER_MODE` | Ex1, Ex2 | `regex` or `llm` |

### Final Project

```bash
# Start all 8 background services (router, orchestrator, aggregator, synthesizer, 4 workers)
bash scripts/start-final.sh

# In a separate terminal — start last
bun run src/node/core/userInterface.ts
```

Logs: `scripts/logs/final-project-services/`

To stop all services: `bash scripts/stop-all.sh`

### Exercise 1 — Regex Chatbot

```bash
# .env: ROUTER_MODE=regex
bash scripts/start-ex1.sh
bun run src/node/core/userInterface.ts   # separate terminal
```

### Exercise 2 — LLM Routing

```bash
# .env: ROUTER_MODE=llm, OPENAI_API_KEY=sk-...
bash scripts/start-ex2.sh
bun run src/node/core/userInterface.ts   # separate terminal
```

### Exercise 3 — Review Analysis Pipeline

```bash
# .env: OPENAI_API_KEY=sk-...
bash scripts/start-ex3.sh
bun run src/node/reviews/reviewProducer.ts   # separate terminal
```

### Exercise 4 — Customer Support Pipeline

```bash
# .env: OPENAI_API_KEY=sk-...
# Requires: ollama serve && ollama pull llama3
bash scripts/start-ex4.sh
bun run src/node/customer-support/customerSupportProducer.ts   # separate terminal
```

---

## Implemented Services

### UserInterface
**File:** `src/node/core/userInterface.ts` | **Groups:** `ui-service`, `ui-service-final-answer`

Reads from `stdin`, generates a `conversationId` (UUID v4), and publishes `UserQueryReceived` to `user-commands`. Concurrently listens on `conversation-events` for `FinalAnswerSynthesized` (prints the answer) and `PlanFailed` (prints the failure reason and the tool that failed). The only component that bridges the human operator to the Kafka cluster. Started manually in a separate terminal after all background services are running. Dual-mode: also handles Ex1/2 chatbot responses from `bot-responses`.

### RouterService
**File:** `src/node/core/routerService.ts` | **Group:** `router-plan-service`

Receives `UserQueryReceived` from `user-commands` and generates an ordered list of tool steps using an **LLM-based planner** (`generatePlanLLM`). The planner calls `gpt-4o-mini` with `ROUTER_SYSTEM_PROMPT` — a few-shot prompt that lists available tools, their argument shapes, and the `{{step_N.result}}` placeholder syntax for inter-step dependencies. If the LLM call fails or returns invalid JSON, it falls back to the keyword/regex planner (`generatePlanRegex`) and logs `[router] mode=regex-fallback`.

Available tools: `weather`, `exchange`, `math`, `chat`, `getProductInformation`.

Emits `PlanGenerated` to `conversation-events`. Logs `[router] mode=llm|regex-fallback` and `[Benchmark] routerLatency=Xms`.

### Orchestrator
**File:** `src/node/orchestration/orchestrator.ts` | **Group:** `orchestrator-service`

The central state machine. On `PlanGenerated`: saves the full plan to LevelDB (`stepIndex=0`, `status=pending`), dispatches the first `ToolInvocationRequested`, then updates status to `running`. On each `ToolInvocationResulted`: resolves any `{{step_N.result}}` placeholders in the next step's args before dispatching, appends the result, and increments `stepIndex`. When all steps complete, updates status to `completed`, emits `PlanCompleted`, and deletes the plan from LevelDB. If a tool result contains an `error` field, or if the Kafka send itself fails, emits `PlanFailed` to `conversation-events`, updates status to `failed`, and cleans up. Also subscribes to `tool-invocation-requests` (consumer group `orchestrator-dedup`) to detect and log duplicate dispatches. Logs `[Benchmark] workerLatency=Xms`.

#### LevelDB Plan Store
**File:** `shared/state/planStore.ts` | **Location:** `.plan-store/` (gitignored)

```typescript
interface PlanState {
  plan: { tool: string; args: Record<string, unknown> }[];
  stepIndex: number;                      // index of the next step to dispatch
  results: Record<string, unknown>[];     // accumulated ToolInvocationResulted payloads
  status: "pending" | "running" | "completed" | "failed";
  planReceivedAt: number;                 // epoch ms — used for workerLatency
}
```

State is keyed by `conversationId`. Survives process restarts. `deletePlan` is called only after `PlanCompleted` is successfully published.

### Aggregator
**File:** `src/node/orchestration/aggregator.ts` | **Group:** `aggregator-service`

A lightweight decoupling bridge. Listens for `PlanCompleted` on `conversation-events` and re-publishes the accumulated results as `SynthesizeFinalAnswerRequested` on `user-commands`. This means the Orchestrator has no knowledge of the synthesizer — the Aggregator is the CQRS seam between orchestration and synthesis.

### AnswerSynthesizer
**File:** `src/node/orchestration/answerSynthesizer.ts` | **Group:** `answer-synthesizer`

Maintains an in-memory cache of `userInput` per `conversationId` (populated from `UserQueryReceived` events). On `SynthesizeFinalAnswerRequested`, builds a prompt containing the original question and all tool results, makes a single `gpt-4o-mini` call, and publishes `FinalAnswerSynthesized`. Synthesis latency is constant regardless of the number of plan steps. Logs `[Benchmark] synthesizerLatency=Xms`.

### Tool Workers

Each worker runs two consumer groups simultaneously — one for the Ex1/2 chatbot mode (`intent-*` topics) and one for the Final Project (`tool-invocation-requests`, filtered by `toolName`). Workers are stateless.

| Worker | File | Final Group | Logic |
|---|---|---|---|
| **mathApp** | `src/node/apps/mathApp.ts` | `math-tool-worker` | Recursive descent parser — no `eval()`. Supports `+ − × ÷` and parentheses. |
| **weatherApp** | `src/node/apps/weatherApp.ts` | `weather-tool-worker` | Mock data for 10 cities (e.g. Tel Aviv: 28°C Sunny, Paris: 17°C Overcast). Unknown cities default to 20°C clear. |
| **exchangeApp** | `src/node/apps/exchangeApp.ts` | `exchange-tool-worker` | Static ILS-based cross-rates for 7 currencies (USD, EUR, GBP, JPY, CHF, CAD, AUD). No external API calls. |
| **generalChatApp** | `src/node/apps/generalChatApp.ts` | `chat-tool-worker` | 9 named-entity/intent patterns + 5 random fallbacks. History-aware for name and memory lookups. |

### RAG Retriever
**File:** `src/python/rag/rag_retriever.py` | **Tool name:** `getProductInformation`

Indexes three product knowledge files (`data/products/iphone.txt`, `macbook.txt`, `tesla.txt`) into ChromaDB using sentence-transformer embeddings. On receiving a `ToolInvocationRequested` with `toolName=getProductInformation`, performs a top-K vector similarity search and returns the retrieved context for downstream synthesis.

---

## Schemas / Event Model

JSON Schema definitions (draft-07) for all Final Project events are in `src/schemas/`. TypeScript interfaces are in `shared/schemas/`.

All events share a common envelope:

```typescript
{
  conversationId: string;   // shared across all events in one request
  timestamp: number;        // Unix epoch ms — used for latency benchmarking
  eventType: string;        // discriminator
  payload: object;          // event-specific fields
}
```

| Event / Command | Discriminator | Topic | Payload |
|---|---|---|---|
| `UserQueryReceived` | `eventType: "UserQueryReceived"` | `user-commands` | `{ userInput: string }` |
| `SynthesizeFinalAnswerRequested` | `commandType: "SynthesizeFinalAnswerRequested"` | `user-commands` | `{ results: Record<string, unknown>[] }` |
| `PlanGenerated` | `eventType: "PlanGenerated"` | `conversation-events` | `{ steps: { tool: string, args: Record<string, unknown> }[] }` |
| `ToolInvocationRequested` | `eventType: "ToolInvocationRequested"` | `tool-invocation-requests` | `{ toolName: string, input: Record<string, unknown> }` |
| `ToolInvocationResulted` | `eventType: "ToolInvocationResulted"` | `conversation-events` | `{ toolName: string, result: Record<string, unknown> }` |
| `PlanCompleted` | `eventType: "PlanCompleted"` | `conversation-events` | `{ results: Record<string, unknown>[] }` |
| `PlanFailed` | `eventType: "PlanFailed"` | `conversation-events` | `{ reason: string, failedTool: string, completedResults: Record<string, unknown>[] }` |
| `FinalAnswerSynthesized` | `eventType: "FinalAnswerSynthesized"` | `conversation-events` | `{ answer: string }` |

Schema files (JSON Schema draft-07): `src/schemas/UserQueryReceived.json`, `PlanGenerated.json`, `ToolInvocationRequested.json`, `ToolInvocationResulted.json`, `PlanCompleted.json`, `PlanFailed.json`, `FinalAnswerSynthesized.json`, `SynthesizeFinalAnswerRequested.json`.

---

## Benchmark Summary

Latency is measured via `timestamp` fields embedded in events. Each service computes the delta and emits a `[Benchmark]` log line. Three requests were executed against the Final Project pipeline.

```bash
grep "\[Benchmark\]" scripts/logs/final-project-services/*.log
```

| Component / Scenario | Model (Provider) | Avg Processing Time / Event (ms) | Max Event Rate (events/sec) | Quality / Accuracy (1–5) | Estimated Cost (per 1k events) |
|---|---|---|---|---|---|
| **Router** (LLM plan generation) | gpt-4o-mini (OpenAI) | ~800 ms | ~1.25 | 5 — deterministic JSON plan | ~$0.02 |
| **Router** (regex fallback) | — (no model) | ~5 ms | ~200 | 4 — rule-based, no ambiguity resolution | $0 |
| **Orchestrator** (state machine) | — (no model) | ~2 ms | ~500 | 5 — deterministic step dispatch | $0 |
| **mathApp** (tool worker) | — (recursive descent parser) | ~1 ms | ~1000 | 5 — exact arithmetic | $0 |
| **weatherApp** (tool worker) | — (mock lookup) | ~1 ms | ~1000 | 3 — mock data only | $0 |
| **exchangeApp** (tool worker) | — (static rates) | ~1 ms | ~1000 | 3 — static rates, no live feed | $0 |
| **generalChatApp** (tool worker) | — (rule-based) | ~1 ms | ~1000 | 3 — pattern matching, no LLM | $0 |
| **RAG Retriever** | sentence-transformers (local) | ~120 ms | ~8 | 4 — semantic search, 3 chunks | $0 |
| **Aggregator** (bridge) | — (no model) | ~1 ms | ~1000 | 5 — pass-through | $0 |
| **AnswerSynthesizer** | gpt-4o-mini (OpenAI) | ~2458 ms | ~0.4 | 5 — coherent multi-tool synthesis | ~$0.04 |
| **End-to-End** (regex router) | gpt-4o-mini (OpenAI) | ~2477 ms | ~0.4 | 5 | ~$0.04 |
| **End-to-End** (LLM router) | gpt-4o-mini × 2 (OpenAI) | ~3277 ms | ~0.3 | 5 | ~$0.06 |

**Key finding:** The Kafka pipeline — routing (regex), orchestration, and all tool workers — contributes under **20 ms** of overhead. The sole bottleneck is the `gpt-4o-mini` LLM call in the AnswerSynthesizer (~2.46 s average). Switching to the LLM router adds one additional OpenAI call (~800 ms) in exchange for handling complex, ambiguous, or multi-tool queries that regex cannot express.

---

## Resilience Summary

Source: [`docs/execution-log.txt`](docs/execution-log.txt)

Three resilience scenarios are documented in [`docs/execution-log.txt`](docs/execution-log.txt):

**Scenario 1 — Worker crash and recovery:**
1. `mathApp` is killed with `pkill -f mathApp`.
2. Worker is restarted: `bun run src/node/apps/mathApp.ts`.
3. Consumer group `math-tool-worker` rejoins Kafka: `Consumer has joined the group`.
4. Query `"what is 12 * 9"` is submitted; Orchestrator dispatches `ToolInvocationRequested`.
5. Restarted worker processes the event from its last committed offset; returns `12 * 9 = 108`.
6. Full pipeline completes. `synthesizerLatency=1508ms`.

**Scenario 2 — Orchestrator crash and plan recovery:**
1. A multi-step query is submitted; Orchestrator saves plan to LevelDB (`status=pending`).
2. After the first tool dispatches, the Orchestrator is killed with `pkill -f orchestrator`.
3. Orchestrator is restarted: `bun run src/node/orchestration/orchestrator.ts`.
4. `initializeStore()` reloads the `in-progress` plan from LevelDB.
5. Orchestrator resumes from `stepIndex`, dispatches the next tool, and the pipeline completes normally.

**Scenario 3 — Duplicate event handling:**
1. A `ToolInvocationRequested` event is replayed to `tool-invocation-requests` for a `conversationId` whose plan is already `completed` or deleted.
2. The `orchestrator-dedup` consumer detects the duplicate and logs a warning.
3. If the worker re-emits `ToolInvocationResulted`, the Orchestrator finds no plan state for that `conversationId` and silently drops it — no double-answer, no crash.

---

## Execution Scenarios

Three orchestration scenarios and two RAG scenarios were executed and logged in [`docs/execution-log.txt`](docs/execution-log.txt).

### Orchestration Scenarios

| Scenario | Query | Plan | Outcome |
|---|---|---|---|
| Weather + Exchange | `what's the weather in tel aviv and convert 100 usd to ils` | `[weather, exchange]` | Weather: 28°C sunny · 100 USD = 370 ILS · synthesizerLatency=2204ms |
| Math + Weather | `what is 25 * 8 and what is the weather in paris` | `[weather, math]` | 25 × 8 = 200 · Paris 17°C overcast · synthesizerLatency=1035ms |
| Chat + Exchange | `hello, and also convert 50 eur to usd` | `[chat, exchange]` | 50 EUR = 54.0541 USD · synthesizerLatency=1270ms |

### RAG Scenarios

| Scenario | Query | Outcome |
|---|---|---|
| Tesla Model 3 | `tell me about the tesla model 3` | Plan: `[getProductInformation]` · ChromaDB top-3 from `tesla.txt` · range, perf, Autopilot · synthesizerLatency=3466ms |
| MacBook | `what can you tell me about the macbook` | Plan: `[getProductInformation]` · ChromaDB top-3 from `macbook.txt` · Air vs Pro, M-series · synthesizerLatency=3085ms |

### Resilience Scenarios

Three scenarios are fully logged in [`docs/execution-log.txt`](docs/execution-log.txt) and summarised in the [Resilience Summary](#resilience-summary) section above:

| Scenario | Method | Result |
|---|---|---|
| Worker crash + recovery | `pkill -f mathApp` → restart | Consumer group rejoins; pending event processed from last offset |
| Orchestrator crash + recovery | `pkill -f orchestrator` → restart | LevelDB reloads in-progress plan; execution resumes from `stepIndex` |
| Duplicate event handling | Manual re-produce to `tool-invocation-requests` | `orchestrator-dedup` warns; `ToolInvocationResulted` silently dropped (plan deleted) |

---

## Conclusions and Trade-offs

### 1. Kafka as Event Store — Justification

Kafka is not just a message bus here — it is the **system of record**. Every state transition in the agent pipeline is an immutable, ordered event appended to a topic. This provides three properties that a traditional REST/RPC architecture cannot:

- **Resilience:** If any service crashes, Kafka retains all unacknowledged events. Consumer groups resume from their last committed offset. No events are lost across restarts.
- **Recoverability:** The full history of any conversation can be reconstructed by replaying `conversation-events` from offset 0. An Orchestrator restart reads `in-progress` plans from LevelDB and can resume execution without the user noticing.
- **Auditability:** Every `ToolInvocationRequested`, `ToolInvocationResulted`, `PlanCompleted`, and `FinalAnswerSynthesized` event is permanently stored with its `conversationId` and `timestamp`. This creates a full audit trail of every AI agent decision at zero additional cost.

### 2. Stateful Processing Power

The Orchestrator demonstrates how event streams enable **stateful, multi-step coordination** without shared databases or synchronous locks:

- State is accumulated incrementally as `ToolInvocationResulted` events arrive on `conversation-events`.
- LevelDB provides local durability so the state machine survives process restarts.
- The `stepIndex` pointer in `PlanState` makes it trivial to resume from exactly the right step after a crash — no duplicate tool calls, no missed steps.
- The `planReceivedAt` timestamp enables end-to-end latency measurement (`workerLatency`) as a derived metric, computed purely from event data with no external metrics system.

### 3. CQRS and Idempotency

The architecture applies **Command-Query Responsibility Segregation** cleanly:

- **Commands** (write-side intent) flow on `user-commands`: `UserQueryReceived` (from the user) and `SynthesizeFinalAnswerRequested` (from the Aggregator).
- **Events** (read-side facts) flow on `conversation-events` and `tool-invocation-requests`.
- The **Aggregator** is the explicit CQRS seam: it reads `PlanCompleted` (read-side) and emits `SynthesizeFinalAnswerRequested` (write-side), decoupling the Orchestrator from the AnswerSynthesizer entirely.

**Idempotency guards** prevent double-processing:
- Workers filter by `toolName` — every consumer reads every message but only processes its own.
- The Orchestrator checks `conversationId` on `ToolInvocationResulted`; unknown IDs (plan already deleted) are silently dropped.
- The `orchestrator-dedup` consumer group monitors `tool-invocation-requests` and logs any re-dispatched `conversationId`.

### 4. Event Sourcing Trade-offs

**Benefits realised in this project:**
- Full replayability — any scenario in `docs/execution-log.txt` can be reproduced from Kafka offsets.
- Temporal decoupling — the AnswerSynthesizer can start after the Orchestrator finishes; there is no timeout to manage.
- Zero-downtime worker restarts — Kafka consumer group rebalancing is transparent to the rest of the pipeline.

**Real costs:**
- **Eventual consistency:** The answer appears asynchronously. There is no request/response primitive — the UI must poll `conversation-events` for `FinalAnswerSynthesized` or `PlanFailed`.
- **Debugging complexity:** A single user query produces 8–12 Kafka events across 4 topics. Tracing a bug requires correlating events by `conversationId` across multiple log files.
- **Operational overhead:** Running Kafka, ZooKeeper-less KRaft, ChromaDB, Ollama, and 8 Bun processes locally requires more setup than a monolithic REST API.
- **Latency floor:** Even with zero LLM calls, the Kafka round-trip (produce → consume → produce → consume) adds ~5–20 ms per hop. For ultra-low-latency use cases, this overhead matters.

### Strengths

- **Loose coupling:** No service knows about any other service. Adding a new tool worker requires zero changes to the orchestrator or synthesizer.
- **Durability:** Every event is persisted in Kafka. Plan state is persisted in LevelDB. The system is resilient to individual process crashes.
- **Observability:** Latency is measurable at every pipeline segment using `timestamp` fields embedded in events. The `[Benchmark]` pattern makes performance visible without additional tooling.
- **Horizontal scale:** `conversationId`-keyed partitioning allows multiple concurrent conversations to run in parallel without interference.
- **Testability:** Stateless workers are purely functional (event in → event out) and straightforward to test in isolation.

### Real Trade-offs

- **Sequential tool execution:** The Orchestrator dispatches tools one at a time. For independent parallel steps (e.g. weather and exchange simultaneously), this adds unnecessary round-trip latency.
- **LLM dominance:** The Kafka pipeline contributes under 20 ms; the synthesis LLM call contributes ~2.46 s. Latency improvements to the Kafka layer have negligible end-to-end impact.
- **Single-broker limitation:** Running a single Kafka broker with a replication factor of 1 means the broker itself is a single point of failure. Appropriate for development; not for production.
- **In-memory synthesizer cache:** The AnswerSynthesizer's `userInput` cache is in-memory only. An orchestrator restart would lose cached queries for in-flight conversations.

### 5. Future Improvements

- **Kafka Streams DSL** (`kafka-streams` npm package or the Java native API): Replace the manual consumer-loop pattern in the Orchestrator and Aggregator with a stateful Streams topology. Kafka Streams provides built-in join semantics, state stores, and windowed aggregations without managing offsets manually.
- **Confluent Schema Registry**: Replace the ad-hoc JSON Schema files in `src/schemas/` with a centralised Schema Registry. Producers register schemas on publish; consumers validate on consume. This enforces contract compatibility across service versions and enables schema evolution with backward/forward compatibility checks.
- **KSQL / ksqlDB**: Express the routing and aggregation logic as SQL queries over Kafka topics rather than imperative TypeScript. For example, the Aggregator's `PlanCompleted → SynthesizeFinalAnswerRequested` bridge could be a two-line KSQL stream-to-stream join.
- **Grafana + Prometheus for observability**: The current `[Benchmark]` log-line approach requires manual `grep`. A production system would export `routerLatency`, `workerLatency`, and `synthesizerLatency` as Prometheus metrics (e.g. via `prom-client`) and visualise them on a Grafana dashboard with alerting thresholds.
- **Parallel tool dispatch**: Track which plan steps are independent (no `{{step_N.result}}` references) and dispatch them concurrently, joining results before the next dependent step.
- **Durable `userInput` cache** in the AnswerSynthesizer (backed by LevelDB or sourced from `conversation-events` replay).
- **Dynamic tool registration** via a `tool-registry` topic, removing the hardcoded tool list from the router prompt.
- **Multi-broker Kafka cluster** with replication factor ≥ 2 for production resilience.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Messaging | Apache Kafka 3.8.0 (KRaft, no ZooKeeper) |
| Runtime | Bun 1.0+ |
| Language | TypeScript (strict mode) |
| AI / LLM | OpenAI `gpt-4o-mini` |
| Local LLM | Ollama `llama3` (Exercise 4 only) |
| Vector DB | ChromaDB + sentence-transformers |
| State Store | LevelDB (Orchestrator) |
| Kafka Client | KafkaJS |
| Infrastructure | Docker, docker-compose |
