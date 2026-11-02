# Architecture

## Overview

This system is an **event-driven, multi-step AI agent** built on Apache Kafka. Services communicate exclusively through Kafka topics — no service calls another service directly. The orchestrator is the only stateful service (LevelDB); the others keep no durable state, apart from the synthesizer's small LevelDB (`.synth-store/`: the original question, the memoized outcome and a `done` marker per conversation) and the chat history file `history.json`, which only the synthesizer writes.

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

Topic constants are defined in `shared/topics.ts`, and the Node services import them from there. The topic names are also hardcoded in `src/python/rag/rag_retriever.py` (input, output and dead-letter topics) and `infra/create-topics.sh`; keep all three in sync.

| Topic | Partitions | Purpose |
|---|---|---|
| `user-commands` | 3 | Commands from the UI (`UserQueryReceived`) and Aggregator (`SynthesizeFinalAnswerRequested`) |
| `conversation-events` | 3 | All domain events: `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `PlanFailed`, `FinalAnswerSynthesized`, `ConversationFailed`, `StepTimedOut` |
| `tool-invocation-requests` | 3 | Tool dispatch requests from the Orchestrator, one per step |
| `dead-letter-queue` | 3 | Poison messages only (`MessageDeadLettered`), written by the consumer wrapper and the RAG worker — for manual inspection. Keyed by the original message's key. Failed plans are `PlanFailed` events, not dead letters |

`conversationId` is used as the Kafka message key, guaranteeing ordering per conversation. Each topic has 3 partitions, but `consumer.run` sets no `partitionsConsumedConcurrently` and `bun run start` launches (under the supervisor) one instance per consumer group, so each service processes one message at a time. The partitions allow scale-out that is not used today.

---

## Event Types

**Source of truth.** Every message has one contract, a zod schema in `shared/schemas/<Name>.ts`; the TypeScript type is `z.infer` of it. `bun run schemas:gen` (`scripts/gen-schemas.ts`) turns the schemas into JSON Schema (`shared/schemas/<Name>.json`, draft 2020-12, generated with `io: "input"` so objects stay open). The JSON files are generated: never edit them by hand. `bun run schemas:check` and `shared/schemas/generated.test.ts` fail when they are stale. The Python RAG worker validates against these files with `jsonschema` (`src/python/rag/contracts.py`). Shared fixtures in `shared/schemas/__fixtures__/<Name>/{valid,invalid}/` are run through zod (`bun test`) and through `jsonschema` (`bun run test:py`), so both languages must accept and reject the same messages.

Events share this envelope:

```typescript
{
  conversationId: string;  // UUID; shared across all events in one request, and the Kafka key
  timestamp: number;       // non-negative integer, Unix epoch ms — used for latency benchmarking
  eventType: string;       // discriminator
  payload: object;         // event-specific fields
}
```

Differences from the envelope:
- `SynthesizeFinalAnswerRequested` is a command and uses `commandType` instead of `eventType`. The registry reads `eventType ?? commandType`.
- `PlanGenerated`, `PlanCompleted` and `SynthesizeFinalAnswerRequested` also carry a top-level `sessionId`. `UserQueryReceived` carries it in `payload`.
- `MessageDeadLettered` has its own shape (below).

Readers are tolerant: unknown fields are accepted and stripped, so an additive change does not turn into a poison storm during a rolling restart. `ToolName` is a closed enum (`weather`, `exchange`, `math`, `chat`, `getProductInformation`). A new tool is one `ToolName` entry plus a worker.

| Event | Discriminator | Topic | Payload |
|---|---|---|---|
| `UserQueryReceived` | `eventType` | `user-commands` | `{ userInput: string (non-empty), sessionId: string }` |
| `PlanGenerated` | `eventType` | `conversation-events` | `{ steps: { tool: ToolName, args: Record<string, unknown> }[] }` (at least one step) |
| `ToolInvocationRequested` | `eventType` | `tool-invocation-requests` | `{ toolName: ToolName, stepIndex: number (required), input: Record<string, unknown> }` |
| `ToolInvocationResulted` | `eventType` | `conversation-events` | `{ toolName: ToolName, stepIndex: number (required), result: Record<string, unknown> }` |
| `PlanCompleted` | `eventType` | `conversation-events` | `{ results: { toolName: ToolName, result: Record<string, unknown> }[] }` |
| `PlanFailed` | `eventType` | `conversation-events` | `{ code: "tool-error" \| "step-timeout" \| "empty-plan", reason: string, failedTool: string, completedResults: { toolName, result }[] }` |
| `SynthesizeFinalAnswerRequested` | `commandType` | `user-commands` | `{ results: { toolName: ToolName, result: Record<string, unknown> }[] }` |
| `FinalAnswerSynthesized` | `eventType` | `conversation-events` | `{ answer: string }` (must contain a non-whitespace character; the contract is `regex(/\S/)`) |
| `ConversationFailed` | `eventType` | `conversation-events` | `{ stage: "routing" \| "orchestration" \| "aggregation" \| "synthesis", code: "llm-unavailable" \| "llm-misconfigured" \| "empty-answer" \| "missing-question" \| "internal-error", reason: string }` (`reason` is for operators; the UI text comes from `code`) |
| `StepTimedOut` | `eventType` | `conversation-events` | `{ stepIndex: number, deadline: number }`; produced and consumed by the orchestrator only |
| `MessageDeadLettered` | `eventType` | `dead-letter-queue` | see below |

**Validation at every boundary.** Consumers go through `subscribeAndRun` (`shared/kafka/client.ts`, core in `shared/kafka/consume.ts`): null check, `JSON.parse`, discriminator, schema, then a check that the Kafka key equals `conversationId`. A message type the topic does not know, or one the service does not handle, is skipped. Producers go through `publish()`, which validates before sending, rejects a type that does not belong to the topic, and derives the key; a violation throws `OutboundContractViolationError` before any send.

**`MessageDeadLettered`** (`dead-letter-queue`, keyed by the original key): `{ eventType, schemaVersion: 1, conversationId (null if unparseable), timestamp, payload: { source: { topic, partition, offset, key, timestamp }, consumer: { group, service }, error: { class, name, reason, issues?, outbound?, attempts }, originalValue: { encoding: "utf8" | "base64", data, truncated } | null } }`. `error.class` is one of `malformed-json`, `missing-value`, `contract-violation`, `outbound-contract-violation`, `key-mismatch`, `handler-non-retryable`, `unknown-exhausted`. For an `outbound-contract-violation`, `originalValue` is the valid input and `issues` and `outbound` describe the message the handler tried to produce. `originalValue` is cut at 512 KiB so a record always fits the broker's message limit.

**Failure handling.** Handlers signal what should happen by what they throw (`shared/kafka/errors.ts`): `RetryableError` subclasses (`StoreError`, `ProducerError`) are redelivered without limit and never dead-lettered; `NonRetryableError` subclasses are dead-lettered and the offset is committed; any other error is retried up to 3 times (counter in memory, reset by a restart) and then dead-lettered as `unknown-exhausted`. Pure code is wrapped in `nonRetryable(...)`. In the orchestrator, every side effect goes through `publish()` or the plan store, which throw retryable errors, and validation and pure code run before any emit or persist, so a dead-lettered input has produced no partial effects there. The synthesizer also writes `history.json` and calls the LLM. Its LLM failures are not exceptions: `callLLM` throws `LlmError` (a `NonRetryableError`, after its own bounded retries), and the synthesizer catches it and turns it into a `ConversationFailed` outcome, so Kafka never redelivers an LLM failure. Its other effects follow the publish → persist order described under AnswerSynthesizer, and every I/O failure there is retryable.

**Reporting poison messages to the user.** `subscribeAndRun` accepts `onDeadLettered: { owns, handle }`. After the DLQ record is published, in the same serialized handler path, the wrapper calls `handle(record)` only if the probed type of the original message (`messageTypeOf`; null for a missing value or non-JSON) is in `owns` and the record has a `conversationId`. A hook that throws is rethrown as retryable; the resulting duplicate DLQ record and duplicate hook call are tolerated. A service lists only the types whose failure it is responsible for, so one poison message yields one user-visible event:

| Service | Owns | Reports |
|---|---|---|
| router | `UserQueryReceived` (inbound violations, and an outbound `PlanGenerated` violation raised while handling it) | `ConversationFailed{stage:"routing", code:"internal-error"}` |
| orchestrator | `PlanGenerated`, only if `getPlan(cid)` is null | `ConversationFailed{stage:"orchestration"}` |
| aggregator | `PlanCompleted` | `ConversationFailed{stage:"aggregation"}` |
| synthesizer | `SynthesizeFinalAnswerRequested` only (its `UserQueryReceived` is a cache fill; the router reports that one) | `ConversationFailed{stage:"synthesis"}` |
| webServer | `FinalAnswerSynthesized`, `PlanFailed`, `ConversationFailed` | no event; a direct error frame if the conversation is pending |

A dead-lettered `ToolInvocationRequested`, `ToolInvocationResulted` or `StepTimedOut` is not reported by the hook: the step timeout produces that failure. Tool workers do not use the hook.

---

## Components

### WebServer
**File:** `src/node/core/webServer.ts` | **Group:** `ui-web-final-answer`

Serves the React frontend and bridges WebSocket connections to Kafka. On each new WebSocket connection a `sessionId` is generated (once per tab/session). User messages arrive as WebSocket frames, are wrapped into `UserQueryReceived` events keyed by a fresh `conversationId`, and published to `user-commands`. The server concurrently consumes `conversation-events` and keeps an in-memory `pending` map from `conversationId` to socket. It sends `{type:"ack"}` after publishing a query, `{type:"tool"}` for each `ToolInvocationResulted`, `{type:"answer"}` for `FinalAnswerSynthesized` and `{type:"error"}` for `PlanFailed` and `ConversationFailed`. **Every frame carries the `conversationId`.** The error frame keeps its shape (`reason`, `failedTool?`) and adds `conversationId` and `code`; the pure `toClientFrame` in `src/node/core/clientFrames.ts` derives `reason` from the code (for example "The weather tool didn't respond within 30 s.", "The language model is unavailable right now. Try again.", "Something went wrong while planning your request (ref `<first 8 characters of the conversationId>`)."). Only a tool's own error text is passed through, for `tool-error`. The `pending` entry is deleted on either failure event. If publishing the query fails (a contract violation or a send error), the server logs it, forgets the `pending` entry and sends `{type:"error", code:"unavailable", reason:"The service is unavailable right now. Try again.", conversationId}` (`unavailableFrame`): there is no input message to dead-letter, and no ack was sent. `pending` holds `{ws, at}`; a sweeper (`pendingSweep.ts`, every 30 s) drops entries older than 5 min. That is memory hygiene only and sends nothing, because the client's own idle timeout is what tells the user. Only `/ws` is upgraded to a WebSocket; all other paths serve the built frontend. Runs on port 3001.

---

### RouterService
**File:** `src/node/core/routerService.ts` | **Group:** `router-plan-service`
**Subscribes to:** `user-commands` | **Publishes to:** `conversation-events`

Receives `UserQueryReceived` and builds a plan, an ordered list of `{tool, args}` steps. The planner is LLM-first: `generatePlanLLM` asks `callLLM` (prompt in `shared/prompts/routerPlanPrompt.ts`, with the session history) for a JSON plan and validates it against the valid tools `weather`, `exchange`, `math`, `chat` and `getProductInformation`. The LLM call has a 15 s budget and one attempt. If it fails (`LlmError`: timeout, network, auth, an empty response, …) or the plan is invalid, the router falls back to the regex planner `generatePlanRegex` (`[router] mode=regex-fallback` in the log), so an LLM problem degrades the plan instead of failing the request. The router does not catch and dead-letter: a failed send throws `ProducerError` and is redelivered, and an unexpected handler error is retried 3 times and then dead-lettered by the consumer wrapper. A dead-lettered `UserQueryReceived` (or an outbound-violating `PlanGenerated`) is reported to the user through the `onDeadLettered` hook as `ConversationFailed{stage:"routing"}`.

Regex fallback rules (non-exclusive — a single query can produce multiple steps, e.g. `"weather in paris and convert 100 USD to EUR"` → `[weather, exchange]`):
1. **weather** — weather/temperature/forecast keywords; extracts the city (default Tel Aviv)
2. **exchange** — currency codes or "exchange"/"convert"; extracts currencies and amount
3. **math** — arithmetic operators or word-form math with a digit; extracts the expression
4. **getProductInformation** — product keywords (`PRODUCT_REGEX`: iphone, macbook, tesla, model 3/s/x/y, cybertruck, laptop, phone, ...). There is no keyword for iPad, Honda or the newer products (Galaxy S25, Sony WH-1000XM5, PlayStation 5 Pro, Nintendo Switch 2, Toyota Camry), so the regex fallback does not route those to the RAG tool; the LLM planner does, because `routerPlanPrompt.ts` lists them.
5. **chat** — fallback if no other rule matched

Logs `[Benchmark] routerLatency=Xms`.

---

### Orchestrator
**File:** `src/node/orchestration/orchestrator.ts` | **Group:** `orchestrator-service`
**Subscribes to:** `conversation-events` (`PlanGenerated`, `ToolInvocationResulted`, `StepTimedOut`)
**Publishes to:** `tool-invocation-requests`, `conversation-events`

The state machine at the centre of the pipeline:

The transition logic is a pure function, `transition(state, event, now, { stepTimeoutMs })` in `src/node/orchestration/planTransition.ts`, which returns the new state, the messages to emit and a decision. `orchestrator.ts` is the shell around it: it loads the state, calls `transition` (wrapped in `nonRetryable`), validates every outbound message before the first send, emits them, persists the new state and returns, and kafkajs then commits the offset.

1. On `PlanGenerated`: saves the full plan to LevelDB (`stepIndex=0`, `status=running`) and dispatches `ToolInvocationRequested` (with `stepIndex: 0`) for `steps[0]`. Every dispatch sets `stepDeadline = now + stepTimeoutMs`. An empty plan fails immediately with `PlanFailed{code:"empty-plan", failedTool:"none"}`.
2. On `ToolInvocationResulted`: appends the result and increments `stepIndex`, then either dispatches the next step (with its `stepIndex`) or — if all steps are done — emits `PlanCompleted` and marks the plan `completed`. Step args may reference earlier results with `{{step_N.result}}`.
3. If a result carries an `error` field, it emits `PlanFailed{code:"tool-error"}` and marks the plan `failed`. Nothing is written to `dead-letter-queue`: a failed plan is a domain event. A failed send is not turned into `PlanFailed`: `publish()` throws `ProducerError`, kafkajs redelivers, and the plan stalls until the broker is back.

4. On `StepTimedOut`: if the plan is `running`, the event's `stepIndex` is the awaited step and its `deadline` equals the stored `stepDeadline`, the plan fails with `PlanFailed{code:"step-timeout", reason:"step <k> (<tool>) did not respond within <s>s"}`. A different step or deadline is ignored as `stale-timeout`; a terminal plan as `plan-terminal`; no plan as `unknown-plan`. A `running` plan stored before deadlines existed has none and matches on `stepIndex` alone.

Terminal plans stay in LevelDB as tombstones; the orchestrator never deletes plan state.

**Step timeouts** (`stepTimers.ts`, `STEP_TIMEOUT_MS`, default 30 000). A timer only publishes `StepTimedOut{stepIndex, deadline}`; it never reads or mutates plan state. After each `savePlan` of a `running` plan the orchestrator schedules (replacing any earlier timer) at `stepDeadline`; after a terminal save it cancels. A failed timer publish is logged and re-armed after 5 s. Because the timeout is an ordinary event in the conversation's partition, whichever of the result and the timeout comes first in the log wins, and duplicates are harmless. On startup, after the store opens and before the consumer subscribes, the orchestrator scans `running` plans (`listRunningPlans()`), arms each at its stored deadline (overdue ones fire immediately, a plan without a deadline gets `now + stepTimeoutMs`) and logs `rescheduled N step timer(s)`. The result of an overdue plan that is already in the log has a lower offset than a timeout published on restart, so the real result still wins. Timers live in this one process: nothing times out while it is down.

**Idempotency:** a `ToolInvocationResulted` is applied only when the plan is `running`, `payload.stepIndex` is an integer equal to the step the plan is awaiting, and `toolName` equals that step's tool. Everything else is ignored, with no write and no emit, and logged as `ignored reason=...`: `unknown-plan`, `plan-terminal`, `missing-step-index`, `duplicate-step`, `future-step` (logged as an error) or `tool-mismatch`. A `PlanGenerated` for a conversation that already has a plan is ignored as `duplicate-plan`. There is no fallback for results without `stepIndex`.

**Delivery order:** emit, then persist (`sync: true`), then offset commit. A crash between any two of these replays the message against the unchanged or already-updated state, and the replay is either recomputed identically or ignored. Store read and write errors become `StoreError` (a missing plan is `null`, not an error) and are rethrown, so kafkajs redelivers instead of the handler continuing on stale state. Outbound events are at-least-once; workers do not deduplicate, so a duplicate `ToolInvocationRequested` re-runs the tool and the orchestrator absorbs the extra result through `stepIndex`. The correctness argument depends on one partition per conversation being handled serially by a single orchestrator (the invariant comment above `handle()`).

Logs `[Benchmark] workerLatency=Xms`.

#### LevelDB Plan Store
**File:** `shared/state/planStore.ts` | **Location:** `.plan-store/` (gitignored)

```typescript
interface PlanState {
  plan: { tool: string; args: Record<string, unknown> }[];
  stepIndex: number;                       // step awaiting a result; equals plan.length once completed
  results: Record<string, unknown>[];      // accumulated tool results
  status: "running" | "completed" | "failed";
  stepDeadline: number | null;             // epoch ms the awaited step must answer by; null once terminal
  planReceivedAt: number;                  // epoch ms — for latency computation
  sessionId: string;                       // groups conversations into a session for history
}
```

Keyed by `conversationId`. State survives process restarts. The only startup scan re-arms step timers (`listRunningPlans()`); a plan otherwise continues when its next `ToolInvocationResulted` (or `StepTimedOut`) arrives, and Kafka redelivers events whose offsets were not committed. Completed and failed plans are kept as tombstones and never deleted, so late or replayed messages for them can be recognised and ignored. The store grows without bound. `getPlan` and `savePlan` rethrow I/O errors, and writes use `{ sync: true }`.

---

### Tool Workers

Each worker runs one consumer group on `tool-invocation-requests`, filters by `toolName` and publishes `ToolInvocationResulted` to `conversation-events`. (An earlier version also ran a second group per worker; it was removed in `ea78fa3`.)

Workers keep no state. Each handles exactly one tool name and ignores all others. A failing tool returns `{ error }`, which the orchestrator treats as a plan failure. A worker computes its result inside the `try` and publishes outside it, so a failed send is a redelivery and never an error result.

| Worker | File | Group | Logic |
|---|---|---|---|
| **mathApp** | `src/node/apps/mathApp.ts` | `math-tool-worker` | Recursive descent parser — no `eval()`. Accepts `/^[\d\s+\-*\/().]+$/`. Supports `+−×÷` and `()`. |
| **weatherApp** | `src/node/apps/weatherApp.ts` | `weather-tool-worker` | Stubbed data for 11 cities (e.g. Tel Aviv: 28°C sunny, Paris: 17°C overcast). Unknown cities default to 20°C clear. |
| **exchangeApp** | `src/node/apps/exchangeApp.ts` | `exchange-tool-worker` | Static ILS-based cross-rates for 7 currencies (USD, EUR, GBP, JPY, CHF, CAD, AUD). No external API call. |
| **generalChatApp** | `src/node/apps/generalChatApp.ts` | `chat-tool-worker` | 2 guardrail rules plus 11 keyword rules. The fallback is one of 5 canned replies, chosen deterministically by input length. It has no access to history. |

#### RAG Retriever (Python)
**File:** `src/python/rag/rag_retriever.py` | **Tool name:** `getProductInformation`

Indexes the product documents (the 11 files in `data/products/`: honda, ipad, iphone, macbook, tesla, galaxy_s25, galaxy_s25_ultra, sony_wh1000xm5, ps5_pro, switch_2, toyota_camry; 56 chunks from `index_kb.py`'s `split_into_chunks`) into an embedded ChromaDB `PersistentClient` using sentence-transformer embeddings. On request, performs a top-K similarity search, moves the chunks of the source named in the query to the front (`source_match.py`: a source whose name tokens all appear in the query beats a longer sibling, so `Galaxy S25 price` selects `galaxy_s25` over `galaxy_s25_ultra`; with no name match the embedding order stands), and returns the retrieved text as `result.context` and, when it finds the product price, `result.value` (the price in USD, digits only, e.g. `"1199"` or `"699.99"`); standard tools return `result.value`, and the orchestrator relies on this shape. `{{step_N.result}}` resolves to `value`, then `context`, then JSON, so a following `exchange` step converts the KB price instead of the first number in the context text. The price comes from `price.py`: the first `Price:` / `Base Price:` / `Price: Starting at` amount that is not a `/year` or `/month` charge, taken from the top-ranked source's retrieved chunks in rank order, then, if none has one, from all of that source's chunks in numeric chunk order (`collection.get`). The pattern is not anchored to a line start, because `index_kb.py` flattens split paragraphs onto one line. Without a price the result has no `value`. The worker is a plain Kafka consumer loop in Python 3.12 using the `rag-tool-worker` group. It echoes `payload.stepIndex` into its result. Every request is validated against the generated contract before the tool filter: malformed JSON, a contract violation or a key mismatch is written to `dead-letter-queue` as a `MessageDeadLettered` record (sent with `send(...).get(timeout=10)`) and only then committed, and the loop continues; if that send fails, nothing is committed and the process exits. The result is validated before it is sent; an invalid result is dead-lettered as `outbound-contract-violation`. A retrieval exception stays a tool failure (an error result, then `PlanFailed`). There is no bounded retry in Python: a process restart is its only redelivery. Offsets are committed manually (`enable_auto_commit=False`): the result is sent with `send(...).get(timeout=10)`, which raises on a failed send, and only then is the record committed. A failed send therefore exits the process uncommitted and the request is redelivered on restart; the supervisor restarts the worker (until then the step timeout fails the plan). Records that are skipped (other tools or event types) are committed immediately. `auto_offset_reset` is `latest`, matching the TS consumers' `fromBeginning: false`.

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

`answerSynthesizer.ts` is a thin service shell. The logic is `synthesize()` in the side-effect-free `synthesize.ts`, which takes its store, LLM, publisher and history as dependencies. The store is a LevelDB at `.synth-store/` (`shared/state/synthStore.ts`; same pattern as the plan store: `sync: true`, I/O errors as `StoreError`, and a missing key is `null`, not an error) with three key kinds:

- `q:<cid>` → `{ userInput, sessionId }`, written when `UserQueryReceived` is consumed (skipped if `done:<cid>` already exists, so a late redelivery leaves no orphan). This replaces the old in-memory cache, so a restart no longer loses the question.
- `o:<cid>` → the memoized outcome, `{ kind: "answer", answer }` or `{ kind: "failed", code, reason }`.
- `done:<cid>` → `{ at }`.

On `SynthesizeFinalAnswerRequested`, every outcome follows **publish → persist**:

1. `done:<cid>` present → log `ignored duplicate`, return (no LLM call, no publish, no history).
2. Use `o:<cid>` if present; otherwise compute it and `put o:<cid>`. A missing `q:` is `{failed, missing-question}`. Otherwise `callLLM` (prompt in `shared/prompts/synthesisPrompt.ts`; 60 s budget, 3 attempts). An answer is `{answer}`; an `LlmError` becomes `{failed}` with `llm-unavailable` (timeout, network, 429, 5xx), `llm-misconfigured` (auth, config; also logged at ERROR) or `empty-answer`.
3. Publish `FinalAnswerSynthesized` or `ConversationFailed{stage:"synthesis"}`. A failed publish is retryable and leaves `done` unwritten.
4. For an answer, `appendToHistory(sessionId, conversationId, …)`, idempotent by `conversationId`.
5. One LevelDB batch: `put done:<cid>`, `del q:<cid>`, `del o:<cid>`.

`done:<cid>` is never written unless the publish succeeded, because writing it first and crashing would lose the user's notification. A crash after `o:` republishes from the memo with no second LLM call; a crash after the publish republishes (a duplicate frame, harmless because the web server forgets the conversation on the first) and then completes with one history entry. A failure while writing history is retryable and reuses `o:`, so a disk outage never triggers more LLM calls. Failed outcomes commit the offset normally and write no DLQ record.

**History** (`shared/state/historyStore.ts`): writes go to `history.json.<pid>.<rand>.tmp`, are fsynced, then renamed over `history.json`; reads are validated against a schema (legacy entries without `conversationId` are accepted). If the file does not parse or validate, the writer renames it to `history.json.corrupt-<ts>`, logs ERROR and starts fresh; readers (the router, the synthesizer's prompt) log ERROR and use no history. Appends are serialized by an in-process promise-chain mutex. **Only the synthesizer writes**; there is no cross-process lock. The path can be set with `HISTORY_PATH`.

The synthesizer owns only `SynthesizeFinalAnswerRequested` for dead-letter reporting (`ConversationFailed{stage:"synthesis", code:"internal-error"}`).

Logs `[Benchmark] synthesizerLatency=Xms`.

---

## Event Sourcing

Kafka carries every command and event in the pipeline, keyed by `conversationId`, so the history of a conversation can be read from the topics. This is event-driven rather than replay-based event sourcing:

- Nothing replays events: consumers subscribe with `fromBeginning: false` by default (`shared/kafka/client.ts`; `KAFKA_OFFSET_RESET=earliest` is used only by the integration suite), and the orchestrator's state lives only in LevelDB.
- Compose sets no retention, so Kafka's default 7-day retention applies; events are not kept permanently.
- Tool workers are functional: input event → output event, no shared mutable state.
- If the orchestrator crashes mid-plan, Kafka redelivers the events whose offsets were not committed, and the orchestrator loads the plan from LevelDB when the next result arrives. Its only startup scan re-arms the step timers of `running` plans; it does not resume them.

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

## LLM Calls

**Files:** `shared/llm/callLLM.ts`, `providers.ts`, `errors.ts`.

`callLLM(prompt, { budgetMs, maxAttempts })`:

- **Budget and timeouts.** Each attempt is aborted after `min(LLM_ATTEMPT_TIMEOUT_MS (default 20 000), remaining budget)`: `AbortSignal.timeout` for Ollama, the per-request `timeout` option for OpenAI. Callers: router `15 000 ms, 1 attempt`; synthesizer `60 000 ms, 3 attempts`.
- **One retry layer.** The OpenAI client is built with `maxRetries: 0`; retries happen only here, only for transient kinds, with backoff `500 ms · 2^n` plus jitter. `Retry-After` is honored; if it exceeds the remaining budget the call fails immediately instead of sleeping.
- **Providers.** `LLM_PROVIDER` is `auto` (default, also when unset), `ollama` or `openai`; any other value is a `config` error. `auto` makes one Ollama attempt (a refused connection fails fast), then OpenAI with the remaining budget. The Ollama default model is `DEFAULT_OLLAMA_MODEL` (`llama3.2:1b`), the only place that default lives (`scripts/start.ts` pulls the same one).
- **Lazy OpenAI client.** It is created on the first call that reaches OpenAI. With no `OPENAI_API_KEY` that call throws `LlmError{config}` (`OPENAI_API_KEY is not set (LLM_PROVIDER=<v>)`); services that never reach OpenAI start and run without a key.
- **Blank answers.** Content with no non-whitespace character is an `empty-response` error (transient: retried, then reported as `empty-answer`).

`LlmError { kind, transient, provider, status? }`:

| Kind | Source | Transient (retried) |
|---|---|---|
| `timeout` | abort / SDK timeout | yes |
| `network` | ECONNREFUSED / ECONNRESET / DNS | yes (in `auto`, falls back from Ollama) |
| `rate-limited` | 429 | yes, honoring `Retry-After` |
| `server` | 5xx | yes |
| `empty-response` | blank content | yes |
| `auth` | 401 / 403 | no |
| `config` | missing key, unknown `LLM_PROVIDER`, 400, 404 (model not found) | no |

`LlmError extends NonRetryableError`: in the consumer wrapper's terms, "Kafka redelivery will not help", because transient errors were already retried inside the budget. **Callers must catch it**: the router falls back to regex; the synthesizer maps `auth`/`config` → `llm-misconfigured`, `empty-response` → `empty-answer`, anything else → `llm-unavailable`. An uncaught `LlmError` is a bug and is dead-lettered as `handler-non-retryable` rather than redelivered (redelivering would re-run the whole budget and block the partition during an outage). Every attempt logs `[llm] provider=… attempt=n/m outcome=… ms=…`.

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

Topics are created by `infra/create-topics.sh` (4 topics × 3 partitions); `bun run start` runs it too and exits 1 with the script's output if it fails.

**Configuration.** Every address, path and timeout the services use is an environment variable whose default is the dev-stack value, so a plain `bun run start` is unchanged: `KAFKA_BROKERS` (comma-separated, default `localhost:9092`; the Python worker also accepts the legacy `KAFKA_BROKER` when `KAFKA_BROKERS` is unset), `KAFKA_NAMESPACE`, `KAFKA_OFFSET_RESET`, `KAFKA_SESSION_TIMEOUT_MS` / `KAFKA_HEARTBEAT_INTERVAL_MS` / `KAFKA_REBALANCE_TIMEOUT_MS`, `KAFKA_RETRIES`, `KAFKA_REQUEST_TIMEOUT_MS`, `RAG_SEND_TIMEOUT_S`, `PLAN_STORE_PATH`, `SYNTH_STORE_PATH`, `HISTORY_PATH`, `WEB_PORT`, `LLM_ROUTER_BUDGET_MS` and `LLM_SYNTH_BUDGET_MS` (see `.env.example`). `KAFKA_NAMESPACE` is a prefix for every topic and consumer group, applied only at the Kafka edge (`publish`, `subscribeAndRun`, `createConsumer` and the Python equivalents in `kafka_config.py`). Handlers and dead-letter records stay logical: `MessageDeadLettered.source.topic` and `consumer.group` never contain the prefix.

**Test broker.** The integration suite (`bun run test:int`) runs against its own broker, defined in `infra/docker-compose.test.yml`: compose project `kafka-agent-orchestrator-test`, no `container_name`, `apache/kafka:3.8.0` on port `19092` (`TEST_KAFKA_PORT` overrides it), instant group joins, a 1 s minimum session timeout and topic auto-creation off. It is started and removed (`down -v`) by the suite itself, and shares nothing with the dev `kafka` container, port `9092`, `.plan-store/`, `.synth-store/` or `history.json`. Each test file gets its own namespace and temp state directory.

**Fault points.** `shared/testing/faultPoint.ts` (TypeScript) and `src/python/rag/fault_point.py` give service code named places where the suite can count, crash (`SIGKILL`) or hold a process. They are no-ops unless `FAULT_DIR` is set, which only the suite does.

### Process management (`scripts/`)

- **`start.ts`** refuses to start if any pid file in `$RUN_DIR` (the supervisor's or a service's) points to a live process whose command line contains this repo's path, and tells you to run `bun run stop`. Checking the service pids too matters: a supervisor killed with `-9` leaves its children running, and a second set would fight them for ports and consumer groups. Then it waits for Kafka, creates the topics, pulls the Ollama model only for `LLM_PROVIDER` `auto` or `ollama` and only when `/api/tags` does not list it (`stream:false`, `res.ok` checked; a failed pull exits 1 in `ollama` mode and warns about the OpenAI fallback in `auto` mode), and spawns `supervisor.ts` detached.
- **`supervisor.ts`** spawns each service directly (`bun <abs path>`, `venv/bin/python -u <abs path>`; one process per service, and the absolute path is what `stop.ts` verifies). It writes `$RUN_DIR/supervisor.pid` and `$RUN_DIR/<service>.pid` (`web`, `router`, `orchestrator`, `aggregator`, `synthesizer`, `math`, `weather`, `exchange`, `chat`, `rag`), appends service output to `$LOG_DIR/<service log>`, and restarts a crashed service according to `scripts/lib/supervisorPolicy.ts`: backoff 1 s doubling to 30 s, reset after 60 s of uptime, `GAVE UP <service>` in `$LOG_DIR/supervisor.log` after 5 crashes within 2 min, and no restart once a stop was requested. On SIGTERM it forwards SIGTERM to the children, waits up to 5 s, sends SIGKILL to the rest, removes the pid files and exits.
- **`stop.ts`** sends SIGTERM to the supervisor (SIGKILL if it has not exited after 10 s), then, as a fallback for children left behind, signals each remaining pid file's process. A pid is signalled only if `ps -o command= -p <pid>` contains the repo's absolute path (`isOwnedPid`), which guards against pid reuse. It uses no `pkill` and never deletes `.plan-store/LOCK`.
- `RUN_DIR` (default `<repo>/.run`) and `LOG_DIR` (default `<repo>/scripts/logs/services`) are read by all three.

### Frontend failure handling

`src/frontend/src/components/chat/inFlight.ts` is a pure reducer (no React or DOM imports). A tab has at most one in-flight request. The bot bubble is created when the user sends; the `ack` binds it to its `conversationId`, and after that only frames with that id resolve it (frames for any other id are ignored). An `error` that arrives before any ack resolves the unbound request, which is how the `unavailable` frame reaches the user. The request fails after 90 s without a frame (the deadline is reset by `ack` and `tool`; `ChatBot.tsx` arms a timer and feeds a `tick` back to the reducer) with "No response after 90 s. The request may still finish; try again.", and a WebSocket close fails it with "Connection lost while waiting for the answer." Both leave the error bubble with its Try again button, and a later answer for the failed request is ignored. 90 s exceeds the longest silent server stage (synthesizer 60 s, step 30 s, router 15 s).
