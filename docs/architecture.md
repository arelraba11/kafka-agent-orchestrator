# Architecture

## Overview

This system is an **event-driven, multi-step AI agent** built on Apache Kafka. Services communicate exclusively through Kafka topics — no service calls another service directly. The orchestrator is the only stateful service (LevelDB); the others keep no durable state, apart from the synthesizer's in-memory query cache and the chat history file `history.json`.

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

Topic constants are defined in `shared/topics.ts`, and the Node services import them from there. The topic names are also hardcoded in `src/python/rag/rag_retriever.py:30-31` and `infra/create-topics.sh`; keep all three in sync.

| Topic | Partitions | Purpose |
|---|---|---|
| `user-commands` | 3 | Commands from the UI (`UserQueryReceived`) and Aggregator (`SynthesizeFinalAnswerRequested`) |
| `conversation-events` | 3 | All domain events: `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `PlanFailed`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | 3 | Tool dispatch requests from the Orchestrator, one per step |
| `dead-letter-queue` | 3 | Unrecoverable errors from any service — for manual inspection |

`conversationId` is used as the Kafka message key, guaranteeing ordering per conversation. Each topic has 3 partitions, but `consumer.run` sets no `partitionsConsumedConcurrently` and `bun run start` launches one instance per consumer group, so each service processes one message at a time. The partitions allow scale-out that is not used today.

---

## Event Types

Events share this envelope:

```typescript
{
  conversationId: string;  // shared across all events in one request
  timestamp: number;       // Unix epoch ms — used for latency benchmarking
  eventType: string;       // discriminator
  payload: object;         // event-specific fields
}
```

Differences from the envelope:
- `SynthesizeFinalAnswerRequested` is a command and uses `commandType` instead of `eventType`.
- `PlanGenerated`, `PlanCompleted` and `SynthesizeFinalAnswerRequested` also carry a top-level `sessionId`.

| Event | Discriminator | Topic | Payload |
|---|---|---|---|
| `UserQueryReceived` | `eventType` | `user-commands` | `{ userInput: string, sessionId: string }` |
| `PlanGenerated` | `eventType` | `conversation-events` | `{ steps: { tool: string, args: Record<string, unknown> }[] }` |
| `ToolInvocationRequested` | `eventType` | `tool-invocation-requests` | `{ toolName: string, input: Record<string, unknown> }` |
| `ToolInvocationResulted` | `eventType` | `conversation-events` | `{ toolName: string, result: Record<string, unknown> }` |
| `PlanCompleted` | `eventType` | `conversation-events` | `{ results: Record<string, unknown>[] }` |
| `PlanFailed` | `eventType` | `conversation-events` | `{ reason: string, failedTool: string, completedResults: Record<string, unknown>[] }` |
| `SynthesizeFinalAnswerRequested` | `commandType` | `user-commands` | `{ results: Record<string, unknown>[] }` |
| `FinalAnswerSynthesized` | `eventType` | `conversation-events` | `{ answer: string }` |

---

## Components

### WebServer
**File:** `src/node/core/webServer.ts` | **Group:** `ui-web-final-answer`

Serves the React frontend and bridges WebSocket connections to Kafka. On each new WebSocket connection a `sessionId` is generated (once per tab/session). User messages arrive as WebSocket frames, are wrapped into `UserQueryReceived` events keyed by a fresh `conversationId`, and published to `user-commands`. The server concurrently consumes `conversation-events` and keeps an in-memory `pending` map from `conversationId` to socket. It sends `{type:"ack"}` after publishing a query, `{type:"tool"}` for each `ToolInvocationResulted`, `{type:"answer"}` for `FinalAnswerSynthesized` and `{type:"error"}` for `PlanFailed`. Only `/ws` is upgraded to a WebSocket; all other paths serve the built frontend. Runs on port 3001.

---

### RouterService
**File:** `src/node/core/routerService.ts` | **Group:** `router-plan-service`
**Subscribes to:** `user-commands` | **Publishes to:** `conversation-events`

Receives `UserQueryReceived` and builds a plan, an ordered list of `{tool, args}` steps. The planner is LLM-first: `generatePlanLLM` asks `callLLM` (prompt in `shared/prompts/routerPlanPrompt.ts`, with the session history) for a JSON plan and validates it against the valid tools `weather`, `exchange`, `math`, `chat` and `getProductInformation`. If the call fails or the plan is invalid, it falls back to the regex planner `generatePlanRegex` (`[router] mode=regex-fallback` in the log). On an unrecoverable error it writes to `dead-letter-queue` only; the UI receives no error (see README, Known limitations).

Regex fallback rules (non-exclusive — a single query can produce multiple steps, e.g. `"weather in paris and convert 100 USD to EUR"` → `[weather, exchange]`):
1. **weather** — weather/temperature/forecast keywords; extracts the city (default Tel Aviv)
2. **exchange** — currency codes or "exchange"/"convert"; extracts currencies and amount
3. **math** — arithmetic operators or word-form math with a digit; extracts the expression
4. **getProductInformation** — product keywords (`PRODUCT_REGEX`: iphone, macbook, tesla, model 3/s/x/y, cybertruck, laptop, phone, ...). There is no Honda or iPad keyword, so the regex fallback does not route those to the RAG tool.
5. **chat** — fallback if no other rule matched

Logs `[Benchmark] routerLatency=Xms`.

---

### Orchestrator
**File:** `src/node/orchestration/orchestrator.ts` | **Group:** `orchestrator-service`
**Subscribes to:** `conversation-events` (`PlanGenerated`, `ToolInvocationResulted`)
**Publishes to:** `tool-invocation-requests`, `conversation-events`

The state machine at the centre of the pipeline:

1. On `PlanGenerated`: saves the full plan to LevelDB (`stepIndex=0`, `status=pending`), dispatches `ToolInvocationRequested` for `steps[0]` and sets `status=running`.
2. On `ToolInvocationResulted`: appends the result, increments `stepIndex`, then either dispatches the next step or — if all steps are done — emits `PlanCompleted` and deletes the plan from LevelDB. Step args may reference earlier results with `{{step_N.result}}`.
3. If a result carries an `error` field, or a dispatch fails, it emits `PlanFailed`, writes to `dead-letter-queue` and deletes the plan.

**Idempotency (partial):** only a `ToolInvocationResulted` for an *unknown* `conversationId` (plan already completed or deleted) is dropped. While a plan is live there is no check that a result matches `plan[stepIndex].tool`, so a duplicate result is appended and advances `stepIndex`. That can skip a step or emit `PlanCompleted` early. A redelivered `PlanGenerated` overwrites state and re-dispatches step 0. Workers do not deduplicate, so a duplicate `ToolInvocationRequested` re-runs the tool. This is fixed in phase 2.

Logs `[Benchmark] workerLatency=Xms`.

#### LevelDB Plan Store
**File:** `shared/state/planStore.ts` | **Location:** `.plan-store/` (gitignored)

```typescript
interface PlanState {
  plan: { tool: string; args: Record<string, unknown> }[];
  stepIndex: number;                       // next step to dispatch
  results: Record<string, unknown>[];      // accumulated tool results
  status: "pending" | "running" | "completed" | "failed";
  planReceivedAt: number;                  // epoch ms — for latency computation
  sessionId: string;                       // groups conversations into a session for history
}
```

Keyed by `conversationId`. State survives process restarts, but nothing scans the store on startup: a plan continues only when its next `ToolInvocationResulted` arrives (Kafka redelivers events whose offsets were not committed). `deletePlan` is called after `PlanCompleted` is published.

---

### Tool Workers

Each worker runs one consumer group on `tool-invocation-requests`, filters by `toolName` and publishes `ToolInvocationResulted` to `conversation-events`. (An earlier version also ran a second group per worker; it was removed in `ea78fa3`.)

Workers keep no state. Each handles exactly one tool name and ignores all others. A failing tool returns `{ error }`, which the orchestrator treats as a plan failure.

| Worker | File | Group | Logic |
|---|---|---|---|
| **mathApp** | `src/node/apps/mathApp.ts` | `math-tool-worker` | Recursive descent parser — no `eval()`. Accepts `/^[\d\s+\-*\/().]+$/`. Supports `+−×÷` and `()`. |
| **weatherApp** | `src/node/apps/weatherApp.ts` | `weather-tool-worker` | Stubbed data for 11 cities (e.g. Tel Aviv: 28°C sunny, Paris: 17°C overcast). Unknown cities default to 20°C clear. |
| **exchangeApp** | `src/node/apps/exchangeApp.ts` | `exchange-tool-worker` | Static ILS-based cross-rates for 7 currencies (USD, EUR, GBP, JPY, CHF, CAD, AUD). No external API call. |
| **generalChatApp** | `src/node/apps/generalChatApp.ts` | `chat-tool-worker` | 2 guardrail rules plus 11 keyword rules. The fallback is one of 5 canned replies, chosen deterministically by input length. It has no access to history. |

#### RAG Retriever (Python)
**File:** `src/python/rag/rag_retriever.py` | **Tool name:** `getProductInformation`

Indexes the product documents (`data/products/{honda,ipad,iphone,macbook,tesla}.txt`, 21 chunks from `index_kb.py`'s `split_into_chunks`) into an embedded ChromaDB `PersistentClient` using sentence-transformer embeddings. On request, performs a top-K similarity search and returns the retrieved text as `result.context` (standard tools return `result.value`; the orchestrator relies on this shape). The worker is a plain Kafka consumer loop in Python 3.12 using the `rag-tool-worker` group.

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

Maintains an in-memory cache of `userInput` keyed by `conversationId` (populated from `UserQueryReceived` events on `user-commands`). On `SynthesizeFinalAnswerRequested`, builds a prompt containing the original question and all tool results, calls `callLLM` once (prompt in `shared/prompts/synthesisPrompt.ts`, provider per `LLM_PROVIDER`: Ollama first, then OpenAI `gpt-4o-mini`, by default), appends the exchange to the session history (`history.json`), and publishes `FinalAnswerSynthesized`. If the LLM call fails, nothing is published (see README, Known limitations).

Logs `[Benchmark] synthesizerLatency=Xms`.

---

## Event Sourcing

Kafka carries every command and event in the pipeline, keyed by `conversationId`, so the history of a conversation can be read from the topics. This is event-driven rather than replay-based event sourcing:

- Nothing replays events: consumers subscribe with `fromBeginning: false` (`shared/kafka/client.ts`), and the orchestrator's state lives only in LevelDB.
- Compose sets no retention, so Kafka's default 7-day retention applies; events are not kept permanently.
- Tool workers are functional: input event → output event, no shared mutable state.
- If the orchestrator crashes mid-plan, Kafka redelivers the events whose offsets were not committed, and the orchestrator loads the plan from LevelDB when the next result arrives. There is no startup scan that resumes plans.

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
grep "\[Benchmark\]" scripts/logs/services/*.log
```

Single local run of 27 queries (2026-04-05, `LLM_PROVIDER=openai`, earlier 3-product KB). Averages:

| Segment | Avg Latency | Source |
|---|---|---|
| Router (`routerLatency`) | ~1,113 ms | `router.log` |
| Workers (`workerLatency`) | ~39 ms | `orchestrator.log` |
| Synthesizer (`synthesizerLatency`) | ~2,189 ms | `answer.log` |

**End-to-end: ~3.3 s** — dominated by the two LLM calls (router + synthesizer). The Kafka pipeline overhead (orchestration + all tool workers + Kafka round-trips) is **under 50 ms total**. Full report: [`docs/benchmark.md`](benchmark.md)

---

## Infrastructure

Defined in `infra/docker-compose.yml`. Two containers, no ZooKeeper (KRaft mode):

| Container | Image | Port | Used by |
|---|---|---|---|
| `kafka` | `apache/kafka:3.8.0` | `9092` | All services |
| `ollama` | `ollama/ollama:latest` | `11434` | Optional local LLM (`LLM_PROVIDER=auto\|ollama`) |

ChromaDB is embedded in the Python process (`PersistentClient`); it is not a container.

Topics are created by `infra/create-topics.sh` (4 topics × 3 partitions); `bun run start` runs it too.
