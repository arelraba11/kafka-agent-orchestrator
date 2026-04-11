# Final Project — Architecture

## Overview

The Final Project implements an **event-sourced, multi-step AI agent** built on Apache Kafka. Every component is a stateless microservice that communicates exclusively through Kafka topics — no service calls another service directly. Coordination is achieved entirely through immutable events.

The system accepts a natural-language query, plans a sequence of tool calls, executes them one at a time, and synthesizes a single coherent answer — all through Kafka.

---

## Event Flow

```
WebSocket (browser)
  │
WebServer ─── UserQueryReceived ──────────────────────────────────────┐
                         │                                            │
                   [user-commands]                                    │
                         │                                            │
                  RouterService                                       │
                         │ PlanGenerated                              │
                 [conversation-events]                                │
                         │                                            │
                  Orchestrator ◄── LevelDB (.plan-store/)             │
                         │ ToolInvocationRequested (one per step)     │
             [tool-invocation-requests]                               │
                         │                                            │
       ┌─────────────────┼──────────────────┬──────────────────┐      │
    mathApp         weatherApp         exchangeApp       generalChatApp│
    (+ RAG via Python)                                                │
       └─────────────────┴──────────────────┴──────────────────┘      │
                         │ ToolInvocationResulted                     │
                 [conversation-events]                                │
                         │                                            │
                  Orchestrator ─── PlanCompleted ────────────────────┤│
                         │                                            │
                 [conversation-events]                                │
                         │                                            │
                   Aggregator                                         │
                         │ SynthesizeFinalAnswerRequested             │
                   [user-commands]                                    │
                         │                                            │
                AnswerSynthesizer ─── FinalAnswerSynthesized ─────────┘
                         │
                 [conversation-events]
                         │
                  WebServer → WebSocket → browser
```

---

## Kafka Topics

All topic constants are defined in `shared/topics.ts`. Topic strings are never hardcoded in service code.

| Topic | Partitions | Purpose |
|---|---|---|
| `user-commands` | 3 | Commands from the UI (`UserQueryReceived`) and Aggregator (`SynthesizeFinalAnswerRequested`) |
| `conversation-events` | 3 | All domain events: `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | 3 | Tool dispatch requests from the Orchestrator, one per step |
| `dead-letter-queue` | 3 | Unrecoverable errors from any service — for manual inspection |

`conversationId` is used as the Kafka message key, guaranteeing ordering per conversation and enabling parallel execution across conversations. Three partitions allow up to three concurrent conversations without head-of-line blocking.

---

## Event Types

All events share a common envelope:

```typescript
{
  conversationId: string;  // shared across all events in one request
  timestamp: number;       // Unix epoch ms — used for latency benchmarking
  eventType: string;       // discriminator
  payload: object;         // event-specific fields
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

---

## Components

### WebServer
**File:** `src/node/core/webServer.ts` | **Group:** `ui-web-final-answer`

Serves the React frontend and bridges WebSocket connections to Kafka. On each new WebSocket connection a `sessionId` is generated (once per tab/session). User messages arrive as WebSocket frames, are wrapped into `UserQueryReceived` events keyed by a fresh `conversationId`, and published to `user-commands`. The server concurrently consumes `conversation-events` and pushes `FinalAnswerSynthesized` (or `PlanFailed`) back to the originating WebSocket. Runs on port 3001.

---

### RouterService
**File:** `src/node/core/routerService.ts` | **Group:** `router-plan-service`
**Subscribes to:** `user-commands` | **Publishes to:** `conversation-events`

Receives `UserQueryReceived` and runs a keyword/regex planner (`generatePlan`) that produces an ordered list of tool steps. Rules are non-exclusive — a single query can produce multiple steps (e.g. `"weather in paris and convert 100 USD to EUR"` → `[weather, exchange]`).

Planner rules (evaluated in order):
1. **weather** — matches weather/temperature/forecast keywords; extracts city
2. **exchange** — matches currency codes or "convert"/"exchange"; extracts currencies and amount
3. **math** — matches arithmetic operators or word-form math with a digit; extracts expression
4. **chat** — fallback if no other rule matched

Logs `[Benchmark] routerLatency=Xms`.

---

### Orchestrator
**File:** `src/node/orchestration/orchestrator.ts` | **Group:** `orchestrator-service`
**Subscribes to:** `conversation-events` (`PlanGenerated`, `ToolInvocationResulted`)
**Publishes to:** `tool-invocation-requests`, `conversation-events`

The state machine at the centre of the pipeline:

1. On `PlanGenerated`: saves the full plan to LevelDB (`stepIndex=0`, `status=in_progress`) and dispatches `ToolInvocationRequested` for `steps[0]`.
2. On `ToolInvocationResulted`: appends the result, increments `stepIndex`, then either dispatches the next step or — if all steps are done — emits `PlanCompleted` and deletes the plan from LevelDB.

**Idempotency:** `ToolInvocationResulted` events for unknown `conversationId`s (plan already completed or state deleted) are silently dropped.

Logs `[Benchmark] workerLatency=Xms`.

#### LevelDB Plan Store
**File:** `shared/state/planStore.ts` | **Location:** `.plan-store/` (gitignored)

```typescript
interface PlanState {
  plan: { tool: string; args: Record<string, unknown> }[];
  stepIndex: number;                       // next step to dispatch
  results: Record<string, unknown>[];      // accumulated tool results
  status: "in_progress" | "completed";
  planReceivedAt: number;                  // epoch ms — for latency computation
}
```

Keyed by `conversationId`. State survives process restarts — any `in_progress` plan can be resumed from `stepIndex` after an orchestrator restart. `deletePlan` is called only after `PlanCompleted` is successfully published.

---

### Tool Workers

Each worker runs two consumer groups simultaneously:
- **Ex1/2 group** — consumes a dedicated `intent-*` topic (chatbot mode)
- **Final group** — consumes `tool-invocation-requests`, filters by `toolName`, publishes `ToolInvocationResulted` to `conversation-events`

Workers are **stateless**. Each handles exactly one tool name and ignores all others.

| Worker | File | Final Group | Logic |
|---|---|---|---|
| **mathApp** | `src/node/apps/mathApp.ts` | `math-tool-worker` | Recursive descent parser — no `eval()`. Accepts `/^[\d\s+\-*\/().]+$/`. Supports `+−×÷` and `()`. |
| **weatherApp** | `src/node/apps/weatherApp.ts` | `weather-tool-worker` | Mock data for 10 cities (e.g. Tel Aviv: 28°C Sunny, Paris: 17°C Overcast). Unknown cities default to 20°C clear. |
| **exchangeApp** | `src/node/apps/exchangeApp.ts` | `exchange-tool-worker` | Static ILS-based cross-rates for 7 currencies (USD, EUR, GBP, JPY, CHF, CAD, AUD). No external API call. |
| **generalChatApp** | `src/node/apps/generalChatApp.ts` | `chat-tool-worker` | 9 named-entity/intent patterns + 5 random fallbacks. History-aware for name/remember lookups. |

#### RAG Retriever (Python)
**File:** `src/python/rag/rag_retriever.py` | **Tool name:** `getProductInformation`

Indexes three product documents (`data/products/iphone.txt`, `macbook.txt`, `tesla.txt`) into ChromaDB using sentence-transformer embeddings. On request, performs a top-K similarity search and returns the retrieved context for downstream LLM answer generation.

---

### Aggregator
**File:** `src/node/orchestration/aggregator.ts` | **Group:** `aggregator-service`
**Subscribes to:** `conversation-events` (`PlanCompleted`) | **Publishes to:** `user-commands`

A lightweight bridge. Listens for `PlanCompleted` and re-publishes the accumulated results as `SynthesizeFinalAnswerRequested` on `user-commands`. This decouples orchestration from synthesis — the Orchestrator does not need to know a synthesizer exists. The Aggregator is also the natural extension point for result validation or multi-synthesizer routing.

---

### AnswerSynthesizer
**File:** `src/node/orchestration/answerSynthesizer.ts` | **Group:** `answer-synthesizer`
**Subscribes to:** `user-commands` (`UserQueryReceived`, `SynthesizeFinalAnswerRequested`)
**Publishes to:** `conversation-events`

Maintains an in-memory cache of `userInput` keyed by `conversationId` (populated from `UserQueryReceived` events on `user-commands`). On `SynthesizeFinalAnswerRequested`, builds a prompt containing the original question and all tool results, calls `gpt-4o-mini` via a single LLM call, and publishes `FinalAnswerSynthesized`. Synthesis latency is constant regardless of the number of plan steps.

Logs `[Benchmark] synthesizerLatency=Xms`.

---

## Event Sourcing

Every state change in the pipeline is an immutable event on a Kafka topic. `conversation-events` is the source of truth for a conversation's lifecycle — any service can reconstruct what happened to a given `conversationId` by replaying from offset 0.

Key properties:
- Tool workers are purely functional: input event → output event, no shared mutable state.
- The Orchestrator reads from and writes to its own LevelDB store; every transition is triggered by an event.
- If the Orchestrator crashes mid-plan, `ToolInvocationResulted` events are already durable in Kafka. On restart, the plan is restored from LevelDB and processing resumes from `stepIndex`.

---

## Stateful Stream Processing

The Orchestrator is the only service with durable state. LevelDB is used as a local embedded key-value store rather than a remote database — reads/writes are in-process (microsecond latency), on-disk (durable across restarts), and isolated (no shared mutable state across services).

State lifecycle per conversation:
```
savePlan()     ← PlanGenerated received
  updatePlan() ← each ToolInvocationResulted (stepIndex++)
  deletePlan() ← PlanCompleted published
```

The Aggregator is stateless — by the time `PlanCompleted` arrives, the full result set is embedded in the event payload. No stream join or buffering is required.

---

## Benchmark

Latency is measured via `timestamp` fields in event payloads. Services compute deltas and emit `[Benchmark]` log lines.

```bash
grep "\[Benchmark\]" scripts/logs/final-project-services/*.log
```

Measured across 27 live queries. Averages:

| Segment | Avg Latency | Source |
|---|---|---|
| Router (`routerLatency`) | ~1,113 ms | `router.log` |
| Workers (`workerLatency`) | ~39 ms | `orchestrator.log` |
| Synthesizer (`synthesizerLatency`) | ~2,189 ms | `answer.log` |

**End-to-end: ~3.3 s** — dominated by two `gpt-4o-mini` calls (router + synthesizer). The Kafka pipeline overhead (orchestration + all tool workers + Kafka round-trips) is **under 50 ms total**. Full report: [`docs/benchmark.md`](../benchmark.md)

---

## Infrastructure

Defined in `infra/docker-compose.yml`. Three containers, no ZooKeeper (KRaft mode):

| Container | Image | Port | Used by |
|---|---|---|---|
| `kafka` | `apache/kafka:3.8.0` | `9092` | All services |
| `ollama` | `ollama/ollama:latest` | `11434` | Exercise 4 sanitizer (llama3) |
| `chromadb` | `chromadb/chroma:latest` | `8000` | RAG retriever |

Topics are created by `infra/topics.sh` (1 partition, all 21 topics). Run `infra/topics-final.sh` afterward to upgrade the four Final Project topics to 3 partitions.
