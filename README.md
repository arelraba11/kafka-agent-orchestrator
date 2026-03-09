# Kafka Microservices Chatbot

Advanced Data Engineering course project.

This repository implements a distributed chatbot system built on Apache Kafka and event-driven microservices. User messages are processed by a pipeline of independent services that communicate exclusively through Kafka topics — there is no direct service-to-service communication.

The project is structured as two progressive exercises:

- **Exercise 1** — baseline distributed router with memory and rule-based intent detection
- **Exercise 2** — extends Exercise 1 with an LLM prompt engineering layer (Few-Shot classification, structured JSON extraction, Chain-of-Thought reasoning, and safety guardrails)

---

## Technology Stack

| Layer | Technology |
|---|---|
| Language | TypeScript |
| Runtime | Bun |
| Messaging | Apache Kafka (KafkaJS client) |
| Infrastructure | Docker, Docker Compose |
| Architecture | Event-Driven Microservices |
| LLM Integration | Pluggable `callLLM()` stub (Exercise 2) |

---

## Architecture Overview

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

In Exercise 2, an LLM pipeline is inserted between the router and the domain apps:

```
user_input_events
    ├──▶ GuardrailService
    └──▶ LLMRouterService ──▶ router_decision_events
                                    ├──▶ LLMExtractionService ──▶ llm_response_events
                                    │         └──▶ JSONParserService ──▶ function_execution_requests
                                    └──▶ CotMathService ──▶ cot_math_expression_events
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
└── README.md                # This file
```

---

## Prerequisites

The following tools must be installed before running either exercise.

**Docker**
```bash
docker --version
docker compose version
```

**Bun**
```bash
bun --version   # >= 1.0
```

Kafka runs inside Docker via `bitnami/kafka:3.5` in KRaft mode (no Zookeeper required). No local Kafka installation is needed.

---

## Getting Started

Each exercise is self-contained. Refer to the exercise-specific README for full setup and run instructions:

- [`exercise1/README.md`](exercise1/README.md) — baseline distributed chatbot
- [`exercise2/README.md`](exercise2/README.md) — LLM-enhanced pipeline
