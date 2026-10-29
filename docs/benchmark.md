# Benchmark Report

> **Historical run.** Single local run on 2026-04-05 with `LLM_PROVIDER=openai` (gpt-4o-mini), not repeated since. Latencies are deltas computed from event `timestamp` fields and printed as `[Benchmark]` log lines (`grep "\[Benchmark\]" scripts/logs/services/*.log`). The knowledge base then held 3 products (iPhone, MacBook, Tesla). It now holds 5 products (Honda, iPad, iPhone, MacBook, Tesla), and `index_kb.py` produces 21 chunks, so retrieval numbers are not comparable with today's index. No accuracy, throughput or cost figures were measured.

## Setup

Local Docker environment: single Kafka broker (KRaft), all services running as background processes via `bun run start`.  
ChromaDB collection: iPhone, MacBook, Tesla (3 products).  
LLM: OpenAI gpt-4o-mini (router + synthesizer).  
RAG: sentence-transformers/all-MiniLM-L6-v2 (local embedding).  
Session history: enabled — prior conversation turns injected into router and synthesizer prompts.

Pipeline: `WebServer (WebSocket) → RouterService → Orchestrator → Workers → AnswerSynthesizer → WebServer`

Latency measured via `timestamp` fields embedded in event payloads across **27 queries**.

---

## Results

### Router Latency — `router.log`

All queries routed in LLM mode (gpt-4o-mini). Representative sample:

| Query type | Latency |
|---|---|
| Single-tool (weather/exchange/math/chat) | 597–1048 ms |
| Multi-tool (RAG + exchange) | 1082–1303 ms |
| Complex 3-tool plan with full history | 3297 ms |

**Average across 27 queries: ~1,113 ms**  
**Min: 597 ms | Max: 3,297 ms**

> The 3,297 ms outlier was the São Paulo/Tesla query, which had a full session history injected into the prompt.

---

### Worker Latency — `orchestrator.log`

Time from first tool dispatch to final result (covers all steps in a multi-step plan).

| Tool | Latency |
|---|---|
| weather (single city) | 13–16 ms |
| weather (two cities) | 15 ms |
| exchange (single) | 11–13 ms |
| math | 14–18 ms |
| chat | 6–14 ms |
| getProductInformation (RAG, single) | 34–72 ms |
| getProductInformation (RAG, two calls) | 49–84 ms |
| RAG → exchange (chained, 2 steps) | 38–66 ms |
| RAG + weather + chat (3 steps) | 53 ms |

**Average across 27 queries: ~39 ms**  
**Simple tools average: ~14 ms | RAG average: ~65 ms | Min: 6 ms | Max: 136 ms**

---

### Synthesizer Latency — `answer.log`

| Query type | Latency |
|---|---|
| Simple (weather/exchange/math) | 784–1,215 ms |
| Chat + history recall | 975–1,025 ms |
| Single RAG answer | 1,575–3,950 ms |
| Multi-tool (2 results + history) | 1,575–2,814 ms |
| Complex 3-tool (3 results) | 2,815 ms |

**Average across 27 queries: ~2,189 ms**  
**Min: 784 ms | Max: 4,694 ms**

> Synthesizer latency is higher than a history-less baseline (~1,687 ms) because conversation history is now injected into every synthesis prompt, increasing token count.

---

## End-to-End

```
~1,113 ms  (router — LLM plan generation + history injection)
+    ~39 ms  (workers — tool execution + Kafka round-trips)
+ ~2,189 ms  (synthesizer — LLM answer synthesis + history injection)
──────────────────────────────────────────────────────────────────
≈  3,341 ms  (~3.3 s average end-to-end)
   Min: 1,589 ms | Max: 6,165 ms
```

Kafka pipeline overhead (routing + orchestration + Kafka round-trips, excluding LLM): **< 50 ms**.  
LLM inference accounts for **~99% of end-to-end latency**.

---

## Consumer Lag

Consumer lag measures how far behind each consumer group is from the latest offset — a lag of 0 means all messages have been processed.

**How to measure:**
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
```

**Observed results (measured after 27-query test run):**

| Consumer Group | Topic | LAG | Status |
|---|---|---|---|
| `router-plan-service` | `user-commands` | 0 | ✅ fully caught up |
| `orchestrator-service` | `conversation-events` | 0 | ✅ fully caught up |
| `answer-synthesizer` | `user-commands` | 0 | ✅ fully caught up |
| `aggregator-service` | `conversation-events` | 0 | ✅ fully caught up |
| `math-tool-worker` | `tool-invocation-requests` | 0 | ✅ fully caught up |
| `weather-tool-worker` | `tool-invocation-requests` | 0 | ✅ fully caught up |
| `exchange-tool-worker` | `tool-invocation-requests` | 0 | ✅ fully caught up |
| `chat-tool-worker` | `tool-invocation-requests` | 0 | ✅ fully caught up |
| `rag-tool-worker` | `tool-invocation-requests` | 0 | ✅ fully caught up |
| `ui-web-final-answer` | `conversation-events` | 0 | ✅ fully caught up |

**Interpretation:** Lag stays at 0 during normal operation because workers process events faster than they arrive (LLM calls are the main slowdown). During a worker crash, lag builds on `tool-invocation-requests` for the affected group and drains automatically on restart — this is the mechanism demonstrated in Resilience Scenario 1.

---

## Analysis

The Kafka pipeline — routing, orchestration, and all tool workers — adds **under 50 ms** of overhead. RAG retrieval adds ~65 ms on average (ChromaDB vector search + sentence-transformer embedding). The dominant cost is LLM inference: the router call (~1.1 s) and synthesis call (~2.2 s) together account for **~99% of end-to-end latency**.

**Session history impact:** Adding persistent session history increases prompt token counts for both router and synthesizer. The synthesizer average rose from ~1,687 ms (baseline) to ~2,189 ms. This is expected and acceptable — the capability gain (multi-turn context, pronoun resolution, name recall) justifies the cost.

**Key findings:**
- Kafka is not the bottleneck — Kafka overhead is < 50 ms regardless of plan complexity.
- Switching to a faster LLM or enabling streaming output would have far more impact than any Kafka tuning.
- RAG retrieval (ChromaDB) took ~65 ms on average on the 3-product KB (recorded retrieval scores: 0.69–0.73).
- Session history is injected into both prompts; follow-up resolution was not scored in this run (see `test-run-2026-04-11.md` for a recorded run that includes two follow-up rows marked ⚠️).
