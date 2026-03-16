# Kafka AI Microservices — Course Project

A progressive Kafka learning course that builds a distributed, event-driven AI system step by step. Each exercise adds a new architectural layer — from regex routing to LLM pipelines, self-correcting processors, and parallel AI inference with stream joins.

---

## Technologies

| Layer | Technology |
|---|---|
| Messaging | Apache Kafka 3.8.0 (KRaft, no ZooKeeper) |
| Runtime | [Bun](https://bun.sh/) 1.0+ |
| Language | TypeScript |
| AI / LLM | OpenAI `gpt-4o-mini`, Ollama `llama3` (Ex4) |
| Infrastructure | Docker, docker-compose |
| Kafka client | KafkaJS |

---

## Project Structure

```
kafka-beginners-course-main/
├── infra/
│   ├── docker-compose.yml      # Single Kafka broker (KRaft, port 9092)
│   └── topics.sh               # Creates all 17 Kafka topics
├── scripts/
│   ├── start-ex{1-4}.sh        # Background service launchers per exercise
│   ├── stop-all.sh             # pkill -f bun
│   └── logs/                   # ex{1-4}-services/<service>.log
├── shared/
│   ├── kafka/client.ts         # KafkaJS factory (createProducer, createConsumer, etc.)
│   ├── llm/openai.ts           # callLLM(prompt) → gpt-4o-mini
│   ├── topics.ts               # All topic name constants — import here, never hardcode
│   ├── prompts/                # LLM prompt templates (Ex2/3 and Ex4)
│   └── types/                  # TypeScript interfaces for all events
├── services/
│   ├── core/                   # UI, memory, router, response aggregator (Ex1–2)
│   ├── apps/                   # math, weather, exchange, general-chat handlers
│   ├── llm/                    # guardrail, LLM router, CoT math service (Ex2)
│   ├── reviews/                # review producer, processor, analytics (Ex3)
│   ├── customer-support/       # support producer, sanitizer, sentiment, urgency, aggregator (Ex4)
│   └── python-workers/         # optional HuggingFace sentiment/urgency workers (Ex4)
├── kafka-basics/               # Java: Producer/Consumer demos
├── kafka-producer-wikimedia/   # Java: Wikimedia SSE → Kafka
├── kafka-consumer-opensearch/  # Java: Kafka → OpenSearch
└── kafka-streams-wikimedia/    # Java: Kafka Streams aggregations
```

---

## Kafka Setup

```bash
# 1. Start broker
docker-compose -f infra/docker-compose.yml up -d

# 2. Create all 17 topics
bash infra/topics.sh

# 3. Install dependencies
bun install

# 4. Configure environment
cp .env.example .env
```

| Variable | Required for | Value |
|---|---|---|
| `OPENAI_API_KEY` | Exercises 2, 3, 4 | `sk-...` |
| `ROUTER_MODE` | Exercises 1, 2 | `regex` or `llm` |

---

## System Architecture

**Chatbot flow (Exercises 1 & 2):**
```
stdin → UserInterface → [user-input-events]
  → MemoryService + RouterService → [intent-*]
  → Apps → [app-results] → ResponseAggregator → [bot-responses] → UserInterface
```

**Reviews pipeline (Exercise 3):**
```
stdin → ReviewProducer → [raw-reviews-topic]
→ ReviewProcessor (3-step LLM) → [processed-insights-topic] → ReviewAnalytics → stdout
```

**Customer support pipeline (Exercise 4):**
```
stdin → CustomerSupportProducer → [raw-customer-messages]
→ SanitizerService → [sanitized-messages]
  ├→ SentimentAnalyzer → [analysis-sentiment] ─┐
  └→ UrgencyClassifier → [analysis-urgency]   ─┴→ InsightAggregator → stdout
```

---

## Running the System

Stop services between exercises: `bash scripts/stop-all.sh`

### Exercise 1 — Distributed Chatbot (Regex Router)

Eight microservices communicate exclusively through Kafka. The router classifies intent with regex and routes to a domain app.

**Routing rules:**
- Math: `/\d+\s*[+\-*/]\s*\d+/`
- Weather: `/\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i`
- Exchange: `/\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i`
- Default: general-chat

```bash
# .env: ROUTER_MODE=regex
bash scripts/start-ex1.sh
bun run services/core/userInterface.ts   # separate terminal
```

```
> 42 * 7            → Bot [math]: 294
> weather in London → Bot [weather]: Weather in London is 12°C and rainy.
> convert 20 EUR to ILS → Bot [exchange]: 20 EUR = 80 ILS
> my name is Arel   → Bot [chat]: Nice to meet you, Arel!
```

---

### Exercise 2 — LLM Prompt Engineering (adds to Exercise 1)

Three new services replace regex with an LLM classification pipeline.

| Service | Technique | Purpose |
|---|---|---|
| `guardrailService` | Keyword filter | Blocks politics/malware before any LLM call |
| `llmRouterService` | Few-Shot (9 examples) | Classifies intent + extracts structured JSON |
| `cotMathService` | Chain-of-Thought | Converts word problems → arithmetic expressions |

```bash
# .env: ROUTER_MODE=llm, OPENAI_API_KEY=sk-...
bash scripts/start-ex2.sh
bun run services/core/userInterface.ts   # separate terminal
```

```
> What's the temperature in Tokyo? → Bot [weather]: 20°C, humid
> five plus three                  → Bot [math]: 8
> hack the mainframe               → [guardrail] Blocked — malware keywords detected
```

---

### Exercise 3 — Review Analysis Pipeline (standalone)

A 3-step LLM processor analyzes product reviews with zero-shot routing, structured extraction, and self-correction.

**ReviewProcessor pipeline:**
1. **Zero-Shot route** — is this a review? (`analyzeReview | ignore`)
2. **Structured extraction** — `{ summary, sentiment, score(1–10), aspects[] }`
3. **Self-correction** — if `score < 4` and `sentiment == "Positive"` → re-analyze

```bash
# .env: OPENAI_API_KEY=sk-...
bash scripts/start-ex3.sh
bun run services/reviews/reviewProducer.ts   # separate terminal
```

```
> The product arrived on time and worked perfectly.
[analytics] Sentiment: Positive | Score: 9/10 | Avg: 9.0

> Not a review, just noise.
[processor] Ignored — not a review.
```

---

### Exercise 4 — Customer Support Analysis Pipeline (standalone)

Demonstrates Kafka fan-out, parallel AI inference, and stream join by `message_id`. Two independent classifiers process each sanitized message; the aggregator correlates results.

```bash
# .env: OPENAI_API_KEY=sk-...
# Requires: ollama serve && ollama pull llama3
bash scripts/start-ex4.sh
bun run services/customer-support/customerSupportProducer.ts   # separate terminal
```

**Alert rule:** `NEGATIVE` + `Urgent` → `🚨 STRONG ALERT`

```
> Jane called 555-1234, payment failed and account locked
[sanitizer] [NAME] called [NUMBER], payment failed and account locked
[insight] 🚨 STRONG ALERT — Sentiment: NEGATIVE | Urgency: Urgent

> Just checking on order status
[insight] Sentiment: POSITIVE | Urgency: General Inquiry
```

**Optional Python workers** (HuggingFace, replace TS sentiment/urgency):
```bash
cd services/python-workers
python -m venv venv && source venv/bin/activate && pip install -r requirements.txt
python sentiment_worker.py   # group: sentiment-group
python urgency_worker.py     # group: urgency-group
```

---

## Logs and Debugging

```bash
tail -f scripts/logs/ex1-services/router-service.log
tail -f scripts/logs/ex2-services/llm-router-service.log
tail -f scripts/logs/ex3-services/review-processor.log
tail -f scripts/logs/ex4-services/insight-aggregator.log
```

Conversation history: `services/core/history.json` — send `reset` in the UI to clear.

---

## Future Architecture

The system will evolve into a fully event-sourced AI agent architecture — adding persistent state stores, multi-agent coordination, tool-use via Kafka, and replay-based debugging across all pipelines.
