# Kafka AI Microservices — Course Project

A progressive Kafka learning course that builds a distributed AI system step by step. Each exercise introduces new architectural patterns on top of the previous one, from regex-based routing to LLM pipelines and self-correcting AI processors.

---

## Table of Contents

- [Technologies](#technologies)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Exercise 1 — Distributed Chatbot](#exercise-1--distributed-chatbot)
- [Exercise 2 — LLM Prompt Engineering Pipeline](#exercise-2--llm-prompt-engineering-pipeline)
- [Exercise 3 — Review Analysis Pipeline](#exercise-3--review-analysis-pipeline)
- [Kafka Topics Reference](#kafka-topics-reference)
- [Logs and Debugging](#logs-and-debugging)
- [Stopping Services](#stopping-services)

---

## Technologies

| Layer | Technology |
|---|---|
| Messaging | Apache Kafka 3.8.0 (KRaft mode, no ZooKeeper) |
| Runtime | [Bun](https://bun.sh/) 1.0+ |
| Language | TypeScript |
| AI / LLM | OpenAI API (`gpt-4o-mini`) |
| Infrastructure | Docker, docker-compose |
| Kafka client | KafkaJS |

---

## Repository Structure

```
kafka-beginners-course-main/
│
├── infra/
│   ├── docker-compose.yml      # Single Kafka broker (KRaft, port 9092)
│   └── topics.sh               # Creates all Kafka topics for Exercises 1–3
│
├── scripts/
│   ├── start-ex1.sh            # Start Exercise 1 pipeline (background)
│   ├── start-ex2.sh            # Start Exercise 2 pipeline (background)
│   ├── start-ex3.sh            # Start Exercise 3 pipeline (background)
│   └── stop-all.sh             # Stop all running Bun services
│
├── shared/                     # Shared TypeScript modules (all exercises)
│   ├── kafka/client.ts         # KafkaJS factory (producer, consumer, helpers)
│   ├── llm/openai.ts           # OpenAI wrapper (callLLM → gpt-4o-mini)
│   ├── prompts/prompts.ts      # All LLM prompt templates
│   ├── topics.ts               # Kafka topic name constants
│   └── types/                  # TypeScript interfaces for all events
│
├── services/                   # All microservices (used across all exercises)
│   ├── user-interface/         # CLI input/output — started manually
│   ├── memory-service/         # Conversation history persistence
│   ├── router-service/         # Intent router (regex or LLM mode)
│   ├── response-aggregator/    # Formats and forwards bot replies
│   ├── apps/                   # Domain apps: math, weather, exchange, chat
│   ├── guardrail-service/      # Exercise 2: safety filter
│   ├── llm-router-service/     # Exercise 2: Few-Shot LLM classifier
│   ├── cot-math-service/       # Exercise 2: Chain-of-Thought math solver
│   ├── review-producer/        # Exercise 3: stdin → Kafka producer
│   ├── review-processor/       # Exercise 3: 3-step LLM pipeline
│   └── review-analytics/       # Exercise 3: real-time insight display
│
├── logs/                       # Created automatically on first run
│
├── .env                        # OPENAI_API_KEY, ROUTER_MODE (not committed)
├── .env.example                # Environment variable reference
├── package.json                # Dependencies: kafkajs, openai
└── tsconfig.json               # TypeScript config
```

---

## Prerequisites

**Docker**
```bash
docker --version
docker compose version
```

**Bun** (TypeScript runtime)
```bash
# Install: https://bun.sh
bun --version   # >= 1.0
```

**OpenAI API key** (required for Exercise 2 and Exercise 3)

```bash
cp .env.example .env
# Edit .env and set:
# OPENAI_API_KEY=sk-...
```

---

## Quick Start

```bash
# 1. Start Kafka broker
docker-compose -f infra/docker-compose.yml up -d

# 2. Create all Kafka topics
bash infra/topics.sh

# 3. Install TypeScript dependencies
bun install

# 4. Start an exercise pipeline (picks one)
bash scripts/start-ex1.sh

# 5. Start the User Interface in a separate terminal
bun run services/user-interface/userInterface.ts
```

---

## Exercise 1 — Distributed Chatbot

A fully distributed chatbot where eight independent microservices communicate exclusively through Kafka topics. No service calls another directly.

### Architecture

```
UserInterface ──► [user-input-events]
                        │
          ┌─────────────┴──────────────┐
          ▼                            ▼
    MemoryService               RouterService (regex)
  (history.json)                       │
          │                  ┌─────────┼──────────┬────────────┐
   [history-update]    [intent-math] [intent-    [intent-     [intent-
          │                  │        weather]    exchange]    general-chat]
          ▼                  ▼           ▼            ▼             ▼
    RouterService         MathApp   WeatherApp  ExchangeApp  GeneralChatApp
                              └─────────┴────────────┴─────────────┘
                                                  │
                                           [app-results]
                                                  │
                                       ResponseAggregator
                                                  │
                                          [bot-responses]
                                                  │
                                          UserInterface
```

### Services

| Service | Responsibility |
|---|---|
| **UserInterface** | Reads stdin, publishes to `user-input-events`; displays `bot-responses` |
| **MemoryService** | Appends each turn to `history.json`; publishes history updates |
| **RouterService** | Classifies input by regex; routes to the matching `intent-*` topic |
| **MathApp** | Recursive descent arithmetic parser (no `eval`) |
| **WeatherApp** | Mock weather lookup for 10 predefined cities |
| **ExchangeApp** | Static currency exchange rates (ILS cross-rates) |
| **GeneralChatApp** | Keyword-matching fallback with canned responses |
| **ResponseAggregator** | Receives `app-results` and forwards a formatted reply to `bot-responses` |

### Router Classification (regex mode)

| Intent | Trigger pattern |
|---|---|
| Math | `/\d+\s*[+\-*/]\s*\d+/` |
| Weather | `/\b(weather\|temperature\|forecast\|hot\|cold\|rain\|sunny)\b/i` |
| Exchange | `/\b(USD\|EUR\|ILS\|GBP\|JPY\|CHF\|CAD\|AUD)\b/i` |
| General chat | Default fallback |

### Running Exercise 1

```bash
# Set router mode
echo "ROUTER_MODE=regex" >> .env

# Start all pipeline services in the background
bash scripts/start-ex1.sh

# Start the UI manually in a separate terminal
bun run services/user-interface/userInterface.ts
```

Output from `start-ex1.sh`:
```
Exercise 1 services started (ROUTER_MODE=regex). Logs in logs/
Start the UI manually: bun run services/user-interface/userInterface.ts
```

---

## Exercise 2 — LLM Prompt Engineering Pipeline

Extends Exercise 1 by replacing the regex router with an LLM-powered classification pipeline. Three new services intercept the message flow between the router and the domain apps.

### What's New

| Service | Prompt Technique | Purpose |
|---|---|---|
| **GuardrailService** | Keyword rules | Blocks unsafe input (politics, malware) before it reaches any LLM |
| **LLMRouterService** | Few-Shot Prompting | Classifies intent and extracts parameters as structured JSON (gpt-4o-mini) |
| **CotMathService** | Chain-of-Thought | Converts natural language word problems into arithmetic expressions (gpt-4o-mini) |

### Architecture

```
[user-input-events]
        │
        ├──► GuardrailService ──► [guardrail-violation-events]  (if unsafe, stops here)
        │
        └──► RouterService ──► [router-decision-events]
                                        │
                            ┌───────────┴──────────────┐
                            ▼                          ▼
                    LLMRouterService            CotMathService
                   (weather, exchange,          (word problems
                    general chat)                and expressions)
                            │                          │
               ┌────────────┤                   [intent-math]
               ▼            ▼                          │
      [intent-weather]  [intent-exchange]              ▼
      [intent-general-chat]                        MathApp
               │                                       │
               ▼                                       │
   WeatherApp / ExchangeApp / GeneralChatApp           │
               └───────────────────────────────────────┘
                                    │
                             [app-results]
                                    │
                         ResponseAggregator ──► [bot-responses] ──► UserInterface
```

### Prompt Engineering Techniques

| Technique | Service | Description |
|---|---|---|
| **Few-Shot Prompting** | LLMRouterService | 7 labeled examples teach the model to classify and extract parameters |
| **Structured JSON Output** | LLMRouterService | Response schema enforced; code fences stripped before parse |
| **Chain-of-Thought** | CotMathService | Model reasons step by step before emitting a clean arithmetic expression |
| **Persona Prompt** | GeneralChatApp | System prompt gives the bot a "cynical data engineer" persona named Pipeline |
| **Keyword Guardrail** | GuardrailService | Rejects politics/malware keywords before any LLM call |

### Running Exercise 2

```bash
# Requires OPENAI_API_KEY in .env
echo "ROUTER_MODE=llm" >> .env

bash scripts/start-ex2.sh

# Start the UI manually in a separate terminal
bun run services/user-interface/userInterface.ts
```

---

## Exercise 3 — Review Analysis Pipeline

A standalone real-time review processing system. Users submit product reviews through an interactive CLI producer. A three-step LLM pipeline processes each review, and a live analytics consumer displays insights and a running average score.

### Architecture

```
stdin
  │
  ▼
ReviewProducer ──► [raw-reviews-topic]
                           │
                           ▼
                   ReviewProcessor
                    │
                    ├── Step 1 — Zero-Shot router
                    │           Is this a review? (analyzeReview / ignore)
                    │
                    ├── Step 2 — Structured extraction
                    │           { summary, overall_sentiment, score (1–10), aspects[] }
                    │
                    └── Step 3 — Self-correction
                                If score < 4 AND sentiment == "Positive" → re-analyze
                           │
                           ▼
                   [processed-insights-topic]
                           │
                           ▼
                   ReviewAnalytics
                   (live display + running average score)
```

### Event Schema

**`raw-reviews-topic`**
```json
{ "reviewId": "uuid", "text": "The product was great!", "timestamp": "ISO 8601" }
```

**`processed-insights-topic`**
```json
{
  "reviewId": "uuid",
  "originalText": "...",
  "summary": "Positive experience with fast delivery",
  "overall_sentiment": "Positive",
  "score": 8,
  "aspects": [
    { "aspect": "delivery", "sentiment": "Positive" },
    { "aspect": "packaging", "sentiment": "Neutral" }
  ],
  "corrected": false,
  "timestamp": "ISO 8601"
}
```

### Running Exercise 3

```bash
# Requires OPENAI_API_KEY in .env

# Start analytics consumer and LLM processor in the background
bash scripts/start-ex3.sh

# Start the interactive review producer in a separate terminal
bun run services/review-producer/reviewProducer.ts
```

The processor and analytics services run silently in the background. The producer reads from stdin — type or paste a review and press Enter to submit it.

---

## Kafka Topics Reference

All topics are created by `bash infra/topics.sh` (partitions: 1, replication: 1).

### Exercise 1 + 2

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `user-input-events` | UserInterface | RouterService, MemoryService, GuardrailService | User message broadcast |
| `user-control-events` | UserInterface | MemoryService | Reset conversation history |
| `conversation-history-update` | MemoryService | RouterService | History sync for routing context |
| `router-decision-events` | RouterService | LLMRouterService, CotMathService | LLM intent + parameters (Ex2) |
| `intent-math` | RouterService / CotMathService | MathApp | Math request |
| `intent-weather` | RouterService / LLMRouterService | WeatherApp | Weather request |
| `intent-exchange` | RouterService / LLMRouterService | ExchangeApp | Exchange request |
| `intent-general-chat` | RouterService / LLMRouterService | GeneralChatApp | General chat request |
| `app-results` | MathApp, WeatherApp, ExchangeApp, GeneralChatApp | ResponseAggregator, MemoryService | App response |
| `bot-responses` | ResponseAggregator | UserInterface | Final reply to user |
| `guardrail-violation-events` | GuardrailService | — | Blocked input audit log |

### Exercise 3

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw-reviews-topic` | ReviewProducer | ReviewProcessor | Raw review text |
| `processed-insights-topic` | ReviewProcessor | ReviewAnalytics | Structured LLM analysis |

---

## Logs and Debugging

All `start-ex*.sh` scripts create the `logs/` directory and redirect every service's stdout and stderr to a dedicated file.

```bash
# Watch a service in real time
tail -f logs/router-service.log
tail -f logs/llm-router-service.log
tail -f logs/review-processor.log

# Scan all logs for errors
grep -i error logs/*.log
```

| Log File | Service | Exercise |
|---|---|---|
| `logs/memory-service.log` | MemoryService | 1, 2 |
| `logs/router-service.log` | RouterService | 1, 2 |
| `logs/response-aggregator.log` | ResponseAggregator | 1, 2 |
| `logs/math-app.log` | MathApp | 1, 2 |
| `logs/weather-app.log` | WeatherApp | 1, 2 |
| `logs/exchange-app.log` | ExchangeApp | 1, 2 |
| `logs/general-chat-app.log` | GeneralChatApp | 1, 2 |
| `logs/guardrail-service.log` | GuardrailService | 2 |
| `logs/llm-router-service.log` | LLMRouterService | 2 |
| `logs/cot-math-service.log` | CotMathService | 2 |
| `logs/review-analytics.log` | ReviewAnalytics | 3 |
| `logs/review-processor.log` | ReviewProcessor | 3 |

**Conversation history** is persisted to `services/memory-service/history.json`. Send `reset` as a chat message to clear it.

---

## Stopping Services

```bash
bash scripts/stop-all.sh
```

This runs `pkill -f bun` which terminates all background Bun processes. Re-run the relevant `start-ex*.sh` to restart.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | Exercise 2, 3 | OpenAI API key for LLM calls |
| `ROUTER_MODE` | Exercise 1, 2 | `regex` (no LLM) or `llm` (LLM routing) |

```bash
cp .env.example .env
# Fill in values before running any exercise
```
