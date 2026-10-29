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
│    │  produces:  PlanCompleted { sessionId, payload: { results[] } }        │
│    │  topic:     conversation-events                                        │
│    │             on error:                                                  │
│    │  produces:  PlanFailed + dead-letter-queue entry                       │
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
| `dead-letter-queue` | Errors | Failures from the router and the orchestrator |

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
  → if done: emit PlanCompleted, delete plan
  → if error: emit PlanFailed, publish to dead-letter-queue
```

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

### Idempotency (partial)
Workers filter by `toolName` and process only their own requests. The orchestrator drops a `ToolInvocationResulted` whose `conversationId` has no stored plan (already completed or deleted). That is all the protection there is: a duplicate result for a *live* plan is not detected, and workers do not deduplicate. See Known limitations.

---

## Resilience

| Scenario | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka retains messages at the last committed offset; the worker rejoins its consumer group on restart | Automatic |
| Orchestrator crash | Kafka redelivers uncommitted events; plan state is read from LevelDB when the next result arrives (no startup scan) | Automatic |
| Duplicate events | A result for an unknown `conversationId` is dropped. A duplicate while the plan is live is **not** detected: it is appended and advances `stepIndex`, which can skip a step or complete the plan early. A redelivered `PlanGenerated` overwrites state and re-dispatches step 0. | Not handled (fixed in phase 2) |
| Unrecoverable error | Router + orchestrator publish to `dead-letter-queue` | Manual inspection |

Full reproduction steps: [`docs/resilience-tests/resilience-demo.md`](docs/resilience-tests/resilience-demo.md)

---

## Known limitations (current behavior)

| Limitation | Fixed in |
|---|---|
| Duplicate events are not handled for live plans (see Resilience): can skip a step, complete a plan early, or re-run a tool | Phase 2 |
| The RAG consumer uses `auto_offset_reset="earliest"` while the TS consumers use `fromBeginning: false`, so a fresh `rag-tool-worker` group replays old requests | Phase 2 |
| The OpenAI client is built at import, so the router and synthesizer crash at startup without `OPENAI_API_KEY` in every mode | Phase 4 |
| Synthesizer failures are silent: the user gets no answer and no error | Phase 4 |
| A router failure goes to the dead-letter queue only; the UI gets no error and keeps waiting | Phase 4 |
| `bun run stop` runs `pkill -f "bun run"` (`scripts/stop.ts`), which also kills unrelated `bun run` processes on the machine | Phase 4 |
| Each service handles one message at a time (`consumer.run` sets no `partitionsConsumedConcurrently`, one instance per group), so 3 partitions give no parallelism today | Known design limit |
| Orchestrator dispatches one step at a time; independent steps are not parallelised | Known design limit |
| Weather and exchange-rate data are stubbed (11 cities, static rates) | Known design limit |
| The regex fallback planner has no Honda/iPad product rule (the LLM planner does not depend on it) | Known design limit |

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

**CQRS:** Separating commands (`user-commands`, `tool-invocation-requests`) from facts (`conversation-events`) means each service is independently deployable and testable. Delivery is at-least-once, and duplicate handling is only partial (see Known limitations).

**Trade-offs:**

| Trade-off | Detail |
|---|---|
| Eventual consistency | The web server consumes `conversation-events` and pushes updates over WebSocket — no synchronous request/response |
| Debugging complexity | A single-step query produces 7 events (plus 2 per extra step) across 3 topics; the dead-letter queue is used only on failure. Correlate by `conversationId` |
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
│       └── rag/                       # rag_retriever.py, index_kb.py
├── shared/
│   ├── kafka/client.ts                # KafkaJS wrapper
│   ├── llm/openai.ts                  # callLLM (Ollama / OpenAI)
│   ├── schemas/                       # TypeScript event interfaces + JSON Schema files (8 event types)
│   ├── prompts/                       # LLM prompt functions
│   ├── state/                         # planStore.ts (LevelDB plans), historyStore.ts (history.json)
│   └── topics.ts                      # topic name constants
├── history.json                       # per-session chat history (runtime file, gitignored)
└── data/products/                     # honda, ipad, iphone, macbook, tesla .txt (RAG knowledge base)
```

---

## Docs

| Document | Content |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture + event schemas |
| [`docs/benchmark.md`](docs/benchmark.md) | Latency measurements from a single 27-query run |
| [`docs/demo-scenarios/demo-scenarios.md`](docs/demo-scenarios/demo-scenarios.md) | Step-by-step demo with real log output |
| [`docs/test-run-2026-04-11.md`](docs/test-run-2026-04-11.md) | Recorded test run |
| [`docs/resilience-tests/resilience-demo.md`](docs/resilience-tests/resilience-demo.md) | Crash + recovery reproduction steps |
