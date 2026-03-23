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

Reads from `stdin`, generates a `conversationId` (UUID v4), and publishes `UserQueryReceived` to `user-commands`. Concurrently listens on `conversation-events` for `FinalAnswerSynthesized` and prints the answer to stdout. The only component that bridges the human operator to the Kafka cluster. Started manually in a separate terminal after all background services are running. Dual-mode: also handles Ex1/2 chatbot responses from `bot-responses`.

### RouterService
**File:** `src/node/core/routerService.ts` | **Group:** `router-plan-service`

Receives `UserQueryReceived` from `user-commands` and runs a keyword/regex planner (`generatePlan`) that produces an ordered list of tool steps. Rules are non-exclusive — a single query can match multiple tools.

Planner rules (evaluated in order):
1. **weather** — matches weather/temperature/forecast keywords; extracts city
2. **exchange** — matches currency codes or convert/exchange keywords; extracts currencies and amount
3. **math** — matches arithmetic operators or word-form math with a digit; extracts expression
4. **chat** — fallback if no other rule matched

Emits `PlanGenerated` to `conversation-events`. Logs `[Benchmark] routerLatency=Xms`.

### Orchestrator
**File:** `src/node/orchestration/orchestrator.ts` | **Group:** `orchestrator-service`

The central state machine. On `PlanGenerated`: saves the full plan to LevelDB (`stepIndex=0`, `status=in_progress`) and dispatches `ToolInvocationRequested` for `steps[0]`. On `ToolInvocationResulted`: appends the result, increments `stepIndex`, then either dispatches the next step or emits `PlanCompleted` (and deletes the plan from LevelDB) when all steps are done. Logs `[Benchmark] workerLatency=Xms`.

#### LevelDB Plan Store
**File:** `shared/state/planStore.ts` | **Location:** `.plan-store/` (gitignored)

```typescript
interface PlanState {
  plan: { tool: string; args: Record<string, unknown> }[];
  stepIndex: number;                      // index of the next step to dispatch
  results: Record<string, unknown>[];     // accumulated ToolInvocationResulted payloads
  status: "in_progress" | "completed";
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

| Event | `eventType` | Topic | Payload |
|---|---|---|---|
| `UserQueryReceived` | `"UserQueryReceived"` | `user-commands` | `{ userInput: string }` |
| `PlanGenerated` | `"PlanGenerated"` | `conversation-events` | `{ steps: { tool: string, args: Record<string, unknown> }[] }` |
| `ToolInvocationRequested` | `"ToolInvocationRequested"` | `tool-invocation-requests` | `{ toolName: string, input: Record<string, unknown> }` |
| `ToolInvocationResulted` | `"ToolInvocationResulted"` | `conversation-events` | `{ toolName: string, result: Record<string, unknown> }` |
| `PlanCompleted` | `"PlanCompleted"` | `conversation-events` | `{ results: Record<string, unknown>[] }` |
| `FinalAnswerSynthesized` | `"FinalAnswerSynthesized"` | `conversation-events` | `{ answer: string }` |

Schema files: `src/schemas/UserQueryReceived.json`, `PlanGenerated.json`, `ToolInvocationRequested.json`, `ToolInvocationResulted.json`, `PlanCompleted.json`, `FinalAnswerSynthesized.json`.

---

## Benchmark Summary

Latency measured via `timestamp` fields in event payloads. Each service computes the delta and emits a `[Benchmark]` log line. Three requests were executed. Source: [`docs/benchmark.md`](docs/benchmark.md).

```bash
grep "[Benchmark]" scripts/logs/final-project-services/*.log
```

### Router Latency (`routerLatency`) — `router.log`

| Request | Latency |
|---|---|
| 1 | 5 ms |
| 2 | 4 ms |
| 3 | 5 ms |

**Average: ~4.7 ms**

### Worker Latency (`workerLatency`) — `orchestrator.log`

| Request | Latency |
|---|---|
| 1 | 25 ms |
| 2 | 7 ms |
| 3 | 11 ms |

**Average: ~14 ms**

### Synthesizer Latency (`synthesizerLatency`) — `answer.log`

| Request | Latency |
|---|---|
| 1 | 1369 ms |
| 2 | 3104 ms |
| 3 | 2901 ms |

**Average: ~2458 ms (~2.46 s)**

### End-to-End

```
  4.7 ms  (router)
+  14 ms  (workers)
+2458 ms  (synthesizer)
──────────────────────
≈ 2477 ms (~2.5 s)
```

**The Kafka pipeline — routing, orchestration, and tool execution — adds under 20 ms of overhead. The dominant cost is the single `gpt-4o-mini` LLM call in the AnswerSynthesizer (~2.46 s). Kafka is not the bottleneck.**

---

## Resilience Summary

Source: [`docs/execution-log.txt`](docs/execution-log.txt)

The resilience scenario demonstrates **worker crash and recovery**:

1. The `mathApp` process is terminated with `pkill -f mathApp`.
2. The worker is restarted: `bun run src/node/apps/mathApp.ts`.
3. The consumer group `math-tool-worker` rejoins Kafka and logs: `Consumer has joined the group`.
4. A new query (`"what is 12 * 9"`) is submitted.
5. The Orchestrator dispatches `ToolInvocationRequested` for the math tool.
6. The restarted worker picks up the event from its last committed offset and returns `12 * 9 = 108`.
7. The full pipeline completes successfully. `synthesizerLatency=1508ms`.

This demonstrates that Kafka's durable offset management ensures no event is lost across a worker restart. The Orchestrator's LevelDB state also allows plan recovery across an orchestrator restart.

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
| Tesla Model 3 | `tell me about the tesla model 3` | Product context retrieved from ChromaDB; summary covering performance, range, autopilot · synthesizerLatency=3466ms |
| MacBook | `what can you tell me about the macbook` | Product context retrieved; summary covering MacBook Air, MacBook Pro, macOS · synthesizerLatency=3085ms |

### Resilience Scenario

Worker crash (`pkill -f mathApp`) → restart → `what is 12 * 9` → `12 * 9 = 108` → successful recovery. synthesizerLatency=1508ms.

---

## Conclusions and Trade-offs

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
- **Static tool registry:** The RouterService uses hardcoded keyword rules and the worker set is fixed. Adding a new tool requires code changes to both the router and the worker list.

### Future Improvements

- Parallel tool dispatch for independent steps within a single plan.
- Durable `userInput` cache in the AnswerSynthesizer (e.g. backed by LevelDB or sourced from `conversation-events` replay).
- Dynamic tool registration via a `tool-registry` topic.
- Multi-broker Kafka cluster with replication for production resilience.
- LLM-based planner to replace keyword regex rules, enabling open-ended tool selection.

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
