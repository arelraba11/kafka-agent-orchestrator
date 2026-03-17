# Benchmark Report

## Setup

Local Docker environment: single Kafka broker (KRaft), all services running.

Pipeline: `UserInterface → RouterService → Orchestrator → Workers → AnswerSynthesizer`

Latency measured via `timestamp` fields embedded in event payloads. Three requests were executed and each pipeline segment was timed independently.

---

## Results

### Router Latency — `router.log`

| Request | Latency |
|---------|---------|
| 1 | 5 ms |
| 2 | 4 ms |
| 3 | 5 ms |

**Average: ~4.7 ms**

### Worker Latency — `orchestrator.log`

| Request | Latency |
|---------|---------|
| 1 | 25 ms |
| 2 | 7 ms |
| 3 | 11 ms |

**Average: ~14 ms**

### Synthesizer Latency — `answer.log`

| Request | Latency |
|---------|---------|
| 1 | 1369 ms |
| 2 | 3104 ms |
| 3 | 2901 ms |

**Average: ~2458 ms (~2.46 s)**

---

## End-to-End

```
4.7 ms  (router)
+ 14 ms  (workers)
+ 2458 ms (synthesizer)
─────────────────────
≈ 2477 ms (~2.5 s)
```

---

## Analysis

The Kafka pipeline — routing, orchestration, and tool execution — adds under **20 ms** of overhead. The dominant cost is the LLM call in the AnswerSynthesizer (~2.46 s). This confirms that Kafka is not the bottleneck: it provides a low-latency event backbone, and the end-to-end response time is effectively bounded by LLM inference.
