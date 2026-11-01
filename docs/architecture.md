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
| `ToolInvocationRequested` | `eventType` | `tool-invocation-requests` | `{ toolName: string, stepIndex: number, input: Record<string, unknown> }` |
| `ToolInvocationResulted` | `eventType` | `conversation-events` | `{ toolName: string, stepIndex: number, result: Record<string, unknown> }` |
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

The transition logic is a pure function, `transition(state, event, now)` in `src/node/orchestration/planTransition.ts`, which returns the new state, the messages to emit and a decision. `orchestrator.ts` is the shell around it: it loads the state, calls `transition`, emits the messages, persists the new state and returns, and kafkajs then commits the offset.

1. On `PlanGenerated`: saves the full plan to LevelDB (`stepIndex=0`, `status=running`) and dispatches `ToolInvocationRequested` (with `stepIndex: 0`) for `steps[0]`. An empty plan fails immediately with `failedTool: "none"`.
2. On `ToolInvocationResulted`: appends the result and increments `stepIndex`, then either dispatches the next step (with its `stepIndex`) or — if all steps are done — emits `PlanCompleted` and marks the plan `completed`. Step args may reference earlier results with `{{step_N.result}}`.
3. If a result carries an `error` field, it emits `PlanFailed`, writes to `dead-letter-queue` and marks the plan `failed`. A failed send is not turned into `PlanFailed`: the handler throws, kafkajs redelivers, and the plan stalls until the broker is back.

Terminal plans stay in LevelDB as tombstones; the orchestrator never deletes plan state.

**Idempotency:** a `ToolInvocationResulted` is applied only when the plan is `running`, `payload.stepIndex` is an integer equal to the step the plan is awaiting, and `toolName` equals that step's tool. Everything else is ignored, with no write and no emit, and logged as `ignored reason=...`: `unknown-plan`, `plan-terminal`, `missing-step-index`, `duplicate-step`, `future-step` (logged as an error) or `tool-mismatch`. A `PlanGenerated` for a conversation that already has a plan is ignored as `duplicate-plan`. There is no fallback for results without `stepIndex`.

**Delivery order:** emit, then persist (`sync: true`), then offset commit. A crash between any two of these replays the message against the unchanged or already-updated state, and the replay is either recomputed identically or ignored. Store read and write errors are rethrown, so kafkajs redelivers instead of the handler continuing on stale state. Outbound events are at-least-once; workers do not deduplicate, so a duplicate `ToolInvocationRequested` re-runs the tool and the orchestrator absorbs the extra result through `stepIndex`. The correctness argument depends on one partition per conversation being handled serially by a single orchestrator (the invariant comment above `handle()`).

Logs `[Benchmark] workerLatency=Xms`.

#### LevelDB Plan Store
**File:** `shared/state/planStore.ts` | **Location:** `.plan-store/` (gitignored)

```typescript
interface PlanState {
  plan: { tool: string; args: Record<string, unknown> }[];
  stepIndex: number;                       // step awaiting a result; equals plan.length once completed
  results: Record<string, unknown>[];      // accumulated tool results
  status: "running" | "completed" | "failed";
  planReceivedAt: number;                  // epoch ms — for latency computation
  sessionId: string;                       // groups conversations into a session for history
}
```

Keyed by `conversationId`. State survives process restarts, but nothing scans the store on startup: a plan continues only when its next `ToolInvocationResulted` arrives (Kafka redelivers events whose offsets were not committed). Completed and failed plans are kept as tombstones and never deleted, so late or replayed messages for them can be recognised and ignored. The store grows without bound. `getPlan` and `savePlan` rethrow I/O errors, and writes use `{ sync: true }`.

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

Indexes the product documents (`data/products/{honda,ipad,iphone,macbook,tesla}.txt`, 21 chunks from `index_kb.py`'s `split_into_chunks`) into an embedded ChromaDB `PersistentClient` using sentence-transformer embeddings. On request, performs a top-K similarity search and returns the retrieved text as `result.context` (standard tools return `result.value`; the orchestrator relies on this shape). The worker is a plain Kafka consumer loop in Python 3.12 using the `rag-tool-worker` group. It echoes `payload.stepIndex` into its result. Offsets are committed manually (`enable_auto_commit=False`): the result is sent with `send(...).get(timeout=10)`, which raises on a failed send, and only then is the record committed. A failed send therefore exits the process uncommitted and the request is redelivered on restart; nothing restarts the worker automatically. Records that are skipped (other tools or event types) are committed immediately. `auto_offset_reset` is `latest`, matching the TS consumers' `fromBeginning: false`.

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
savePlan() ← PlanGenerated received (status=running, stepIndex=0)
savePlan() ← each ToolInvocationResulted (stepIndex++), after the next request is emitted
savePlan() ← plan finished (status=completed, or failed); the entry is kept as a tombstone
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
