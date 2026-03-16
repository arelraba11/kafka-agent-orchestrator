# Kafka AI Microservices — Course Project

A progressive Kafka learning course that builds a distributed, event-driven AI system step by step. Each exercise adds a new architectural layer on top of the previous one — from regex routing to LLM pipelines, self-correcting processors, and parallel AI inference with stream joins.

---

## Technologies

| Layer | Technology |
|---|---|
| Messaging | Apache Kafka 3.8.0 (KRaft mode, no ZooKeeper) |
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
│   └── topics.sh               # Creates all 17 Kafka topics for Exercises 1–4
├── scripts/
│   ├── start-ex1.sh            # Start Exercise 1 services (background)
│   ├── start-ex2.sh            # Start Exercise 2 services (background)
│   ├── start-ex3.sh            # Start Exercise 3 services (background)
│   ├── start-ex4.sh            # Start Exercise 4 services (background)
│   ├── stop-all.sh             # pkill -f bun
│   └── logs/                   # Auto-created; ex1-services/, ex2-services/, …
├── shared/
│   ├── kafka/client.ts         # KafkaJS factory (createProducer, createConsumer, etc.)
│   ├── llm/openai.ts           # callLLM(prompt) → gpt-4o-mini
│   ├── topics.ts               # All topic name constants
│   ├── prompts/                # LLM prompt templates (Ex2/Ex3 and Ex4)
│   ├── types/                  # TypeScript interfaces for all events
│   └── customerSupport/        # Latency benchmark helpers (Ex4)
└── services/
    ├── user-interface/         # CLI stdin/stdout — started manually
    ├── memory-service/         # Conversation history (history.json)
    ├── router-service/         # Intent router (ROUTER_MODE=regex|llm)
    ├── response-aggregator/    # Formats app-results → bot-responses
    ├── apps/                   # math, weather, exchange, general-chat
    ├── guardrail-service/      # Ex2: safety keyword filter
    ├── llm-router-service/     # Ex2: Few-Shot LLM classifier
    ├── cot-math-service/       # Ex2: Chain-of-Thought math solver
    ├── review-producer/        # Ex3: stdin → raw-reviews-topic
    ├── review-processor/       # Ex3: 3-step LLM pipeline
    ├── review-analytics/       # Ex3: live display + running avg score
    ├── customer-support-producer/  # Ex4: stdin → raw-customer-messages
    ├── sanitizer-service/      # Ex4: PII scrub via Ollama llama3
    ├── sentiment-analyzer/     # Ex4: POSITIVE/NEGATIVE (gpt-4o-mini)
    ├── urgency-classifier/     # Ex4: Urgent/Complaint/General (gpt-4o-mini)
    ├── insight-aggregator/     # Ex4: stream join by message_id → alert
    └── python-workers/         # Ex4: optional HuggingFace alternatives
```

---

## Kafka Setup

```bash
# Start Kafka broker
docker-compose -f infra/docker-compose.yml up -d

# Create all topics (partitions: 1, replication: 1)
bash infra/topics.sh

# Install TypeScript dependencies
bun install
```

---

## Environment Setup

```bash
cp .env.example .env
```

| Variable | Required for | Value |
|---|---|---|
| `OPENAI_API_KEY` | Exercises 2, 3, 4 | `sk-...` |
| `ROUTER_MODE` | Exercises 1, 2 | `regex` or `llm` |

---

## Running the System

Each exercise runs independently. Start the background services with the script, then manually start the interactive producer/UI in a separate terminal.

```bash
bash scripts/stop-all.sh   # stop everything between exercises
```

---

## Exercises Overview

### Exercise 1 — Distributed Chatbot (Regex Router)

Eight microservices communicate exclusively through Kafka. No service calls another directly. The router classifies input with regex and routes to a domain app.

**Event flow:**
```
stdin → UserInterface → [user-input-events] → MemoryService + RouterService
→ [intent-*] → Apps → [app-results] → ResponseAggregator → [bot-responses] → UserInterface
```

**Regex routing rules:**
- Math: `/\d+\s*[+\-*/]\s*\d+/`
- Weather: `/\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i`
- Exchange: `/\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i`
- Default: general-chat

**Run:**
```bash
# In .env: ROUTER_MODE=regex
bash scripts/start-ex1.sh
bun run services/user-interface/userInterface.ts   # separate terminal
```

**Example interactions:**
```
> 42 * 7
Bot [math]: 294

> weather in London
Bot [weather]: Weather in London is 12°C and rainy.

> convert 20 EUR to ILS
Bot [exchange]: 20 EUR = 80 ILS

> hello
Bot [chat]: Hello! How can I help you today?
```

---

### Exercise 2 — LLM Prompt Engineering (adds to Exercise 1)

Replaces regex routing with an LLM classification pipeline. Three new services intercept the flow between RouterService and domain apps.

**New services:**
| Service | Technique | Purpose |
|---|---|---|
| GuardrailService | Keyword filter | Blocks politics/malware input before any LLM call |
| LLMRouterService | Few-Shot (9 examples) | Classifies intent + extracts structured JSON parameters |
| CotMathService | Chain-of-Thought | Converts word problems into arithmetic expressions |

**Event flow:**
```
[user-input-events] → GuardrailService (block unsafe)
                    → RouterService → [router-decision-events]
                      → LLMRouterService → [intent-weather/exchange/chat]
                      → CotMathService  → [intent-math]
→ Apps → [app-results] → ResponseAggregator → [bot-responses] → UserInterface
```

**Run:**
```bash
# In .env: ROUTER_MODE=llm, OPENAI_API_KEY=sk-...
bash scripts/start-ex2.sh
bun run services/user-interface/userInterface.ts   # separate terminal
```

**Example interactions:**
```
> What's the temperature in Tokyo?
Bot [weather]: Weather in Tokyo is 20°C and humid.

> five apples plus three oranges
Bot [math]: 8

> hack the mainframe
[guardrail] Blocked — malware keywords detected.
```

---

### Exercise 3 — Review Analysis Pipeline (standalone)

A three-step LLM processor analyzes user-submitted product reviews in real time. A live analytics consumer displays results and maintains a running average score.

**Event flow:**
```
stdin → ReviewProducer → [raw-reviews-topic]
→ ReviewProcessor (3-step LLM) → [processed-insights-topic]
→ ReviewAnalytics → stdout
```

**ReviewProcessor pipeline:**
1. **Zero-Shot router** — is this a review? (`analyzeReview | ignore`)
2. **Structured extraction** — `{ summary, overall_sentiment, score (1–10), aspects[] }`
3. **Self-correction** — if `score < 4` and `sentiment == "Positive"` → re-analyze

**Run:**
```bash
# In .env: OPENAI_API_KEY=sk-...
bash scripts/start-ex3.sh
bun run services/review-producer/reviewProducer.ts   # separate terminal
```

**Example interactions:**
```
> The product arrived on time and worked perfectly.
[analytics] Sentiment: Positive | Score: 9/10 | Summary: Fast delivery, product worked as expected
[analytics] Running average score: 9.0

> Not a review, just a random sentence.
[processor] Ignored — not a review.
```

---

### Exercise 4 — Customer Support Analysis Pipeline (standalone)

Demonstrates Kafka fan-out, parallel AI inference, and stream join (event correlation). Two independent classifiers process each sanitized message, and the aggregator joins their results by `message_id`.

**Event flow:**
```
stdin → CustomerSupportProducer → [raw-customer-messages]
→ SanitizerService (Ollama llama3, PII scrub) → [sanitized-messages]
  → SentimentAnalyzer  (sentiment-group) → [analysis-sentiment] ─┐
  → UrgencyClassifier  (urgency-group)   → [analysis-urgency]   ─┴→ InsightAggregator → stdout
```

**Alert rule:** `NEGATIVE` + `Urgent` → `🚨 STRONG ALERT`; otherwise `[insight] normal log`.

**Run:**
```bash
# Requires: OPENAI_API_KEY in .env
# Requires: ollama serve && ollama pull llama3
bash scripts/start-ex4.sh
bun run services/customer-support-producer/customerSupportProducer.ts   # separate terminal
```

**Optional — Python workers** (HuggingFace, replace TypeScript sentiment/urgency services):
```bash
cd services/python-workers
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python sentiment_worker.py   # consumer group: sentiment-group
python urgency_worker.py     # consumer group: urgency-group
```

**Example interactions:**
```
> Jane called 555-1234, payment failed and account locked
[sanitizer] Sanitized: [NAME] called [NUMBER], payment failed and account locked
[insight] 🚨 STRONG ALERT — Sentiment: NEGATIVE | Urgency: Urgent

> Just checking on order status
[insight] Sentiment: POSITIVE | Urgency: General Inquiry
```

---

## Logs and Debugging

```bash
# Watch a service in real time
tail -f scripts/logs/ex1-services/router-service.log
tail -f scripts/logs/ex2-services/llm-router-service.log
tail -f scripts/logs/ex3-services/review-processor.log
tail -f scripts/logs/ex4-services/insight-aggregator.log

# Scan all logs for errors
grep -i error scripts/logs/**/*.log
```

Log pattern: `scripts/logs/ex{1–4}-services/<service-name>.log`

Conversation history: `services/memory-service/history.json` — send `/reset` in the UI to clear it.

---

## Future Architecture

The system will evolve into a fully event-sourced AI agent architecture — adding persistent state stores, multi-agent coordination, tool-use via Kafka, and replay-based debugging across all pipelines.
