# Event-Sourced AI Agent with Kafka

An event-driven, multi-step AI agent built on Apache Kafka 3.8.0. Services publish events and commands to Kafka topics and never call each other directly; the only service holding state is the orchestrator (LevelDB). The LLM is called via `callLLM` (Ollama or OpenAI, see `LLM_PROVIDER`).

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
│    │  produces:  UserQueryReceived { payload: { userInput, sessionId } }    │
│    │  topic:     user-commands                                              │
│    ▼                                                                        │
│  routerService.ts                                                           │
│    │  reads:     UserQueryReceived from user-commands                       │
│    │  action:    callLLM → tool plan (regex planner as fallback)            │
│    │  produces:  PlanGenerated { sessionId, payload: { steps[] } }          │
│    │  topic:     conversation-events                                        │
│    ▼                                                                        │
│  orchestrator.ts  ◄──── LevelDB (.plan-store/)                              │
│    │  reads:     PlanGenerated from conversation-events                     │
│    │  action:    saves plan to LevelDB, dispatches step 1                   │
│    │  produces:  ToolInvocationRequested { conversationId, toolName,        │
│    │             stepIndex, input }                                         │
│    │  topic:     tool-invocation-requests                                   │
│    ▼                                                                        │
│  [Tool Workers — each is an independent consumer group]                     │
│    ├── mathApp.ts          → handles toolName="math"                        │
│    ├── weatherApp.ts       → handles toolName="weather"                     │
│    ├── exchangeApp.ts      → handles toolName="exchange"                    │
│    ├── generalChatApp.ts   → handles toolName="chat"                        │
│    └── rag_retriever.py    → handles toolName="getProductInformation"       │
│    │                                                                        │
│    │  each worker produces: ToolInvocationResulted { toolName, stepIndex,   │
│    │                        result }                                        │
│    │  topic:                conversation-events                             │
│    ▼                                                                        │
│  orchestrator.ts  (continues)                                               │
│    │  reads:     ToolInvocationResulted from conversation-events            │
│    │  action:    updates LevelDB state, resolves {{step_N.result}}          │
│    │             if more steps → dispatch next ToolInvocationRequested      │
│    │             if all steps done:                                         │
│    │  produces:  PlanCompleted { sessionId, payload: { results[] } }        │
│    │  topic:     conversation-events                                        │
│    │             on error:                                                  │
│    │  produces:  PlanFailed (no dead letter)                                │
│    ▼                                                                        │
│  aggregator.ts                                                              │
│    │  reads:     PlanCompleted from conversation-events                     │
│    │  produces:  SynthesizeFinalAnswerRequested { payload: { results[] } }  │
│    │  topic:     user-commands                                              │
│    ▼                                                                        │
│  answerSynthesizer.ts                                                       │
│    │  reads:     SynthesizeFinalAnswerRequested from user-commands          │
│    │  action:    callLLM → synthesizes final answer                         │
│    │  produces:  FinalAnswerSynthesized { answer }                          │
│    │  topic:     conversation-events                                        │
│    ▼                                                                        │
│  webServer.ts  (consumer loop)                                              │
│    │  reads:     ToolInvocationResulted, FinalAnswerSynthesized, PlanFailed │
│    │  action:    pushes {tool} / {answer} / {error} over the WebSocket      │
│    ▼                                                                        │
│   USER                                                                      │
│    receives answer                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key rule:** every message is keyed by `conversationId` — guarantees ordering per conversation. Topics have 3 partitions, but each service runs a single instance that handles one message at a time (see Known limitations).

---

## Kafka Topics

| Topic | Type | Events |
|---|---|---|
| `user-commands` | Commands | `UserQueryReceived`, `SynthesizeFinalAnswerRequested` |
| `conversation-events` | Events | `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `PlanFailed`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | Commands | `ToolInvocationRequested` |
| `dead-letter-queue` | Poison messages | `MessageDeadLettered`: a message that can never be processed (malformed, contract-violating, key mismatch, or crashing a handler). Failed plans are `PlanFailed` events, not dead letters |

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
- Python 3.12 (for the RAG worker)
- `OPENAI_API_KEY` set in `.env`, even if you only use Ollama (see below)

### LLM provider

`callLLM` (`shared/llm/openai.ts`) picks the provider from `LLM_PROVIDER` in `.env`. The default is `auto`.

| `LLM_PROVIDER` | Behavior |
|---|---|
| `auto` (default) | Try Ollama (`OLLAMA_HOST`, `OLLAMA_MODEL`, default `llama3.2`) first; fall back to OpenAI `gpt-4o-mini` if Ollama fails |
| `ollama` | Ollama only |
| `openai` | OpenAI `gpt-4o-mini` only |

The OpenAI client is constructed when `shared/llm/openai.ts` is imported, and the `openai` library throws if the key is undefined. The router and synthesizer therefore crash at startup without `OPENAI_API_KEY` in **every** mode, including `LLM_PROVIDER=ollama`. A placeholder value is enough if you never call OpenAI. An *invalid* key behaves differently: the router falls back to its regex planner, while the synthesizer fails silently (see Known limitations).

### One-time setup

```bash
cp .env.example .env          # set OPENAI_API_KEY (and LLM_PROVIDER if you want)
docker-compose -f infra/docker-compose.yml up -d    # Kafka (and Ollama)
bash infra/create-topics.sh   # create 4 topics × 3 partitions
bun install
cd src/frontend && bun install && cd ../..          # only needed for `bun run web:dev`

# Python RAG (one-time, Python 3.12)
cd src/python
python3.12 -m venv venv && source venv/bin/activate
pip install -r rag/requirements.txt
python rag/index_kb.py        # index data/products/ into an embedded Chroma store
cd ../..
```

The knowledge base in `data/products/` is sample data: 11 products (56 chunks), one `.txt` file per product or product family, all prices in USD. The newer files (Galaxy S25 / S25 Ultra, Sony WH-1000XM5, PlayStation 5 Pro, Nintendo Switch 2, Toyota Camry) use launch prices (US MSRP at launch, not today's price). The five older files (iPhone, iPad, MacBook, Tesla, Honda) keep their original headline prices, which are illustrative: for example the MacBook Pro 16-inch line says "Starting at $2,000", which is not Apple's launch price. After editing a file, re-run `index_kb.py`.

### Run

```bash
bun run start    # start 10 background processes incl. webServer (logs → scripts/logs/services/)

# Open the Web UI at http://localhost:3001
# The WebSocket endpoint is ws://localhost:3001/ws: send the query as a plain-text frame.
# The server replies with JSON frames {type:"ack"}, {type:"tool"}, {type:"answer"} or {type:"error"}.

# Or for hot-reload dev mode:
bun run web:dev    # http://localhost:5173 (proxies /ws → port 3001)

bun run stop     # stop everything
```

`bun run start` serves the committed build in `src/frontend/dist/`. After changing frontend code, run `bun run build` in `src/frontend` so `dist/` is current.

---

## Architecture Concepts

### Events in Kafka
Kafka carries every command and event, keyed by `conversationId`, so you can inspect a conversation's history on the topics. This is not full event sourcing:
- **No replay** — consumers subscribe with `fromBeginning: false`, and nothing rebuilds state by replaying events. Plan state lives only in LevelDB (`.plan-store/`).
- **Retention** — compose sets no retention, so Kafka's default of 7 days applies; events are not permanent.
- **Recovery** — there is no startup scan that resumes plans. After a crash, a service's consumer group re-reads the events it had not committed, and the orchestrator picks up LevelDB state when the next `ToolInvocationResulted` for that conversation arrives.
- **Auditability** — each event carries `conversationId` and `timestamp`, so a conversation can be traced by key while the topic retains it.

### Stateful Stream Processing — Orchestrator
The only stateful service. Uses **LevelDB** (`.plan-store/`) keyed by `conversationId`:

```
PlanGenerated  → save plan (stepIndex=0, results=[]) → dispatch step[0]
ToolInvocationResulted → load state → append result → resolve {{step_N.result}}
  → if more steps: dispatch next step
  → if done: emit PlanCompleted, keep the plan as a `completed` tombstone
  → if error: emit PlanFailed, keep a `failed` tombstone (no dead letter: a failed plan is a domain event)
```

Every request and result carries `stepIndex`. The orchestrator applies a result only when it matches the step the plan is awaiting; anything else is ignored with a logged reason. The handler emits the outbound events first, then persists the new state (`sync: true`), and Kafka commits the offset after the handler returns. A crash anywhere in that sequence replays the message, and the replay is either recomputed identically or ignored. Terminal plans are never deleted, so a late or replayed message for a finished plan is recognised and ignored.

LevelDB survives restarts. Kafka redelivers events whose offsets were not committed.

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
At-least-once delivery plus an idempotent orchestrator gives effectively-once plan state transitions. A `ToolInvocationResulted` is applied only if the plan is `running`, `stepIndex` is an integer equal to the step the plan awaits, and `toolName` matches that step. Otherwise it is ignored and logged as `ignored reason=<unknown-plan | plan-terminal | missing-step-index | duplicate-step | future-step | tool-mismatch>`. A replayed `PlanGenerated` for an existing plan is ignored as `duplicate-plan`.

Outbound events (`ToolInvocationRequested`, `PlanCompleted`, `PlanFailed`, dead-letter records) are at-least-once: never lost, sometimes duplicated. Workers do not deduplicate, so a duplicate request re-runs the tool, and the orchestrator absorbs the extra result through `stepIndex`. The guarantee relies on each conversation mapping to one partition handled serially by a single orchestrator (see the invariant comment in `orchestrator.ts`). See Known limitations.

---

## Resilience

| Scenario | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka retains messages at the last committed offset; the worker rejoins its consumer group on restart | Automatic |
| Orchestrator crash | Kafka redelivers uncommitted events; plan state is read from LevelDB when the next result arrives (no startup scan) | Automatic |
| Duplicate events | The orchestrator applies a result only for the step the plan is awaiting (`stepIndex` + `toolName`). Duplicates, stale, mismatched and post-terminal results, and a replayed `PlanGenerated`, are ignored and logged with a reason. The orchestrator emits before it persists, so a crash replays the message instead of losing a step. | Automatic for plan state (duplicate downstream events remain, see Known limitations) |
| Poison message (malformed JSON, contract violation, key mismatch, a handler bug) | The consumer wrapper writes a `MessageDeadLettered` record to `dead-letter-queue` and the offset is committed, so the partition keeps flowing. An unknown error is retried 3 times first | Manual inspection of the dead letter |
| Transient infrastructure failure (LevelDB or producer I/O) | Handlers throw `StoreError` / `ProducerError`; the message is never dead-lettered and kafkajs redelivers it | Automatic |

Full reproduction steps: [`docs/resilience-demo.md`](docs/resilience-demo.md)

---

## Known limitations (current behavior)

| Limitation | Fixed in |
|---|---|
| A crash between the orchestrator publishing `PlanCompleted` and persisting `completed` replays the last result, so `PlanCompleted` goes out twice. The aggregator and synthesizer then run twice: two LLM calls, a duplicate answer and a doubled `history.json` entry (the web client shows only the first answer) | Phase 4 |
| A step nobody answers (worker down, unknown tool) leaves the plan `running` forever; there is no step timeout | Phase 4 |
| Crashed services are not restarted (`scripts/start.ts` spawns each process once). The RAG worker exits on a failed result send, so its plans stall until it is started again | Phase 4 (Part B) |
| Plan tombstones (`completed` and `failed`) are kept in `.plan-store/` forever; there is no pruning | Known design limit |
| The OpenAI client is built at import, so the router and synthesizer crash at startup without `OPENAI_API_KEY` in every mode | Phase 4 |
| Synthesizer failures are silent: the user gets no answer and no error | Phase 4 |
| A router failure never reaches the UI: a failed send is retried forever, and a handler bug is dead-lettered after 3 attempts; either way the UI keeps waiting | Phase 4 |
| A dead-lettered `ToolInvocationRequested` or `ToolInvocationResulted` leaves its plan `running`, because nothing turns the missing step into `PlanFailed` | Phase 4 (step timeout) |
| A poison message is dead-lettered once per consumer group that handles its type, so one bad message can produce several records (`consumer.group` tells them apart). There is no DLQ replay tool | Known design limit |
| The unknown-error retry budget (3 attempts) is held in memory and resets when the process restarts. The Python RAG worker has no bounded retry: a failure other than poison crashes it uncommitted, and it is redelivered when it is started again | Known design limit |
| A RAG result with no `value` (no price line found, even after the full-source fallback) makes `{{step_N.result}}` fall back to `context`, and `exchange` parses the first number in that text, which can be the wrong one (for example a model name). The fallback also takes the first price line in the top-ranked source, so an add-on line (MacBook AppleCare `$299`, Tesla FSD `$8,000`) wins if its chunk ranks above the headline chunk | Known design limit |
| `PlanState` in LevelDB and `history.json` are not schema-validated, and the Python worker hardcodes the topic names | Known design limit |
| `bun run stop` runs `pkill -f "bun run"` (`scripts/stop.ts`), which also kills unrelated `bun run` processes on the machine | Phase 4 |
| Each service handles one message at a time (`consumer.run` sets no `partitionsConsumedConcurrently`, one instance per group), so 3 partitions give no parallelism today | Known design limit |
| Orchestrator dispatches one step at a time; independent steps are not parallelised | Known design limit |
| Weather and exchange-rate data are stubbed (11 cities, static rates) | Known design limit |
| The regex fallback planner (`PRODUCT_REGEX`) only knows iphone, macbook, tesla, model 3/s/x/y, cybertruck and generic words (product, laptop, phone, ...). It has no rule for iPad, Honda or the newer products (Galaxy S25, Sony WH-1000XM5, PlayStation 5 Pro, Nintendo Switch 2, Toyota Camry); the LLM planner lists them in its prompt and does not depend on the regex | Known design limit |
| Retrieval is plain similarity search over chunks, with no product filter, so an ambiguous query can rank the wrong product first and return its price. On the live index, `Galaxy S25 price` ranks `galaxy_s25_ultra` first (value `1299.99`), while `Galaxy S25 Ultra price` ranks it first correctly. The price rule then reads the price of whichever source ranks first | Known design limit |

---

## Benchmark

Single local run, 2026-04-05, `LLM_PROVIDER=openai` (gpt-4o-mini), earlier 3-product KB, 27 queries, session history enabled. Latencies come from `[Benchmark]` log deltas; it has not been re-run since.

| Component | Avg latency |
|---|---|
| RouterService | ~1,113 ms |
| Tool execution (dispatch to final result, orchestrator view) | ~39 ms overall (simple tools ~14 ms, RAG ~65 ms) |
| AnswerSynthesizer | ~2,189 ms |
| **End-to-end** | **~3,341 ms** |

Everything except the two LLM calls accounts for under 50 ms; the LLM calls are ~99% of end-to-end latency.

Full report: [`docs/benchmark.md`](docs/benchmark.md)

---

## Conclusions

**Why Kafka as the backbone:** Topics keyed by `conversationId` give per-conversation ordering, and consumer group offsets give redelivery after a crash. Events stay on the topics for Kafka's default 7-day retention, so a conversation can be traced by key. There is no replay: state lives in LevelDB.

**Stateful processing:** The Orchestrator accumulates state incrementally from the event stream rather than polling a DB. `{{step_N.result}}` placeholder resolution enables chained tool data flow without any shared memory. The Aggregator is fully stateless — by the time `PlanCompleted` arrives, all results are embedded in its payload.

**CQRS:** Separating commands (`user-commands`, `tool-invocation-requests`) from facts (`conversation-events`) means each service is independently deployable and testable. Delivery is at-least-once, and the orchestrator makes its own state transitions idempotent (see Idempotency and Known limitations).

**Trade-offs:**

| Trade-off | Detail |
|---|---|
| Eventual consistency | The web server consumes `conversation-events` and pushes updates over WebSocket — no synchronous request/response |
| Debugging complexity | A single-step query produces 7 events (plus 2 per extra step) across 3 topics; the dead-letter queue is used only for poison messages. Correlate by `conversationId` |
| Operational overhead | 10 processes + Kafka (+ Ollama optionally) vs a single REST API |
| Sequential execution | Orchestrator dispatches one step at a time — independent steps are not parallelised |

**Future improvements:** Kafka Streams DSL, Confluent Schema Registry, parallel tool dispatch, Grafana + Prometheus observability, multi-broker cluster.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Broker | Apache Kafka 3.8.0 (KRaft — no ZooKeeper) |
| Runtime | Bun 1.0+ (TypeScript) |
| Frontend | React 19 + Vite + Tailwind CSS v4 |
| LLM | Ollama (default `llama3.2`) or OpenAI gpt-4o-mini, via `LLM_PROVIDER` |
| Vector DB | Embedded ChromaDB (`PersistentClient`) + sentence-transformers/all-MiniLM-L6-v2 |
| State store | LevelDB (via `level` npm package) |
| Infrastructure | Docker + docker-compose |

---

## File Structure

```
├── infra/
│   ├── docker-compose.yml             # Kafka (KRaft) + Ollama
│   └── create-topics.sh               # create 4 topics × 3 partitions
├── scripts/
│   ├── gen-schemas.ts                 # zod → shared/schemas/*.json (bun run schemas:gen / schemas:check)
│   ├── start.ts                       # launch 10 background processes
│   ├── stop.ts
│   └── logs/services/                 # per-service log files
├── src/
│   ├── frontend/                      # React 19 + Vite web UI
│   ├── node/
│   │   ├── core/                      # routerService.ts, webServer.ts
│   │   ├── orchestration/             # orchestrator.ts, aggregator.ts, answerSynthesizer.ts
│   │   └── apps/                      # mathApp, weatherApp, exchangeApp, generalChatApp
│   └── python/
│       └── rag/                       # rag_retriever.py, contracts.py, price.py, index_kb.py, tests
├── shared/
│   ├── kafka/                         # client.ts (KafkaJS wrapper, publish, subscribeAndRun), errors.ts, consume.ts (validate, classify, dead-letter), deadLetter.ts
│   ├── llm/openai.ts                  # callLLM (Ollama / OpenAI)
│   ├── schemas/                       # zod contracts (9 messages), generated JSON Schema, registry, test fixtures
│   ├── prompts/                       # LLM prompt functions
│   ├── state/                         # planStore.ts (LevelDB plans), historyStore.ts (history.json)
│   └── topics.ts                      # topic name constants
├── history.json                       # per-session chat history (runtime file, gitignored)
└── data/products/                     # 11 .txt files, one per product (RAG knowledge base, sample data)
```

---

## Docs

| Document | Content |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture + event schemas |
| [`docs/benchmark.md`](docs/benchmark.md) | Latency measurements from a single 27-query run |
| [`docs/demo-scenarios/demo-scenarios.md`](docs/demo-scenarios/demo-scenarios.md) | Step-by-step demo with real log output |
| [`docs/test-run-2026-04-11.md`](docs/test-run-2026-04-11.md) | Recorded test run |
| [`docs/resilience-demo.md`](docs/resilience-demo.md) | Crash + recovery reproduction steps |
