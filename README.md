# Kafka Microservices Chatbot

Advanced Data Engineering course project.

This repository implements a series of Kafka-based data engineering exercises. Each exercise builds on the previous, progressing from a distributed microservices chatbot to an LLM-powered prompt engineering pipeline to a real-time review analysis system.

- **Exercise 1** — baseline distributed router with memory and rule-based intent detection
- **Exercise 2** — extends Exercise 1 with an LLM prompt engineering layer (Few-Shot classification, structured JSON extraction, Chain-of-Thought reasoning, and safety guardrails)
- **Exercise 3** — real-time review intelligence pipeline: producer streams user reviews through Kafka, an LLM processor extracts structured sentiment insights, and an analytics consumer displays results live

---

## Technology Stack

| Layer | Technology |
|---|---|
| Language | TypeScript |
| Runtime | Bun |
| Messaging | Apache Kafka (KafkaJS client) |
| Infrastructure | Docker, Docker Compose |
| Architecture | Event-Driven Microservices |
| LLM Integration | OpenAI API (`gpt-4o-mini`) |

---

## Architecture Overview

### Exercise 1 — Distributed Chatbot

```
User Input
    │
    ▼
UserInterface (stdin)
    │
    ▼
Kafka Topic
    │
    ├──▶ MemoryService       (persists conversation history)
    │
    └──▶ RouterService       (classifies intent)
              │
     ┌────────┼────────┬──────────────┐
     ▼        ▼        ▼              ▼
  MathApp  WeatherApp  ExchangeApp  GeneralChatApp
     │        │        │              │
     └────────┴────────┴──────────────┘
                       │
                       ▼
              ResponseAggregator
                       │
                       ▼
                UserInterface (stdout)
```

### Exercise 2 — LLM Prompt Engineering

```
user_input_events
    ├──▶ GuardrailService
    └──▶ LLMRouterService ──▶ router_decision_events
                                    ├──▶ LLMExtractionService ──▶ llm_response_events
                                    │         └──▶ JSONParserService ──▶ function_execution_requests
                                    └──▶ CotMathService ──▶ cot_math_expression_events
```

### Exercise 3 — Review Intelligence Pipeline

```
User Input
    │
    ▼
Producer
    │
    ▼
raw-reviews-topic
    │
    ▼
Processor (LLM routing + sentiment analysis + self-correction)
    │
    ▼
processed-insights-topic
    │
    ▼
Analytics (real-time insights + running average score)
```

---

## Repository Structure

```
kafka-beginners-course-main/
├── exercise1/               # Distributed router and memory bot (Kafka + Bun)
│   ├── shared/              # Kafka client, topics, event types
│   ├── services/            # 8 microservices
│   ├── package.json
│   ├── tsconfig.json
│   └── README.md
│
├── exercise2/               # LLM prompt engineering over Kafka
│   ├── shared/              # Extended topics and event types
│   ├── services/            # 5 new LLM pipeline services
│   ├── kafka/               # Producer and consumer utilities
│   ├── prompts/             # Prompt templates
│   ├── docker-compose.yml
│   ├── topics.sh
│   ├── package.json
│   ├── tsconfig.json
│   └── README.md
│
├── exercise3/               # Real-time review intelligence pipeline
│   ├── kafka/               # Kafka client utilities
│   ├── llm/                 # OpenAI client
│   ├── shared/              # Topics and event types
│   ├── producer.ts          # CLI review producer
│   ├── processor.ts         # LLM processing service
│   ├── analytics.ts         # Real-time analytics consumer
│   ├── prompts.ts           # Prompt templates
│   ├── docker-compose.yml
│   ├── topics.sh
│   ├── package.json
│   ├── tsconfig.json
│   └── README.md
│
└── README.md                # This file
```

---

## Prerequisites

The following tools must be installed before running any exercise.

**Docker**
```bash
docker --version
docker compose version
```

**Bun**
```bash
bun --version   # >= 1.0
```

**OpenAI API key** (required for Exercise 3)

Create a `.env` file at the project root:
```
OPENAI_API_KEY=your_key_here
```

Kafka runs inside Docker in KRaft mode (no Zookeeper required). No local Kafka installation is needed.

---

## Getting Started

Each exercise is self-contained. Refer to the exercise-specific README for full setup and run instructions:

- [`exercise1/README.md`](exercise1/README.md) — baseline distributed chatbot
- [`exercise2/README.md`](exercise2/README.md) — LLM prompt engineering pipeline
- [`exercise3/README.md`](exercise3/README.md) — real-time review intelligence pipeline
