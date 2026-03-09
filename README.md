# Kafka AI Microservices Project

## Overview

This repository contains four progressive exercises that build a distributed AI system using Apache Kafka and microservices. Each exercise introduces new architectural concepts while reusing and extending the infrastructure from previous ones.

**Technologies used:**

| Layer | Technology |
|---|---|
| Messaging | Apache Kafka (KafkaJS) |
| Runtime | Bun |
| Language | TypeScript, Python |
| AI / LLM | OpenAI API, Ollama, HuggingFace Transformers |
| Infrastructure | Docker, docker-compose |

**Concepts covered across the exercises:**

- Event-driven architecture and Kafka producers/consumers
- Microservices communication through Kafka topics
- LLM integration for routing, extraction, and classification
- Streaming AI pipelines with parallel workers
- Stream fan-out and event correlation (stream join)
- Real-time alerting based on AI model output

---

## Repository Structure

```
kafka-beginners-course-main/
├── exercise1/          # Microservices chat system (8 services)
├── exercise2/          # Kafka + LLM prompt engineering pipeline
├── exercise3/          # LLM review processing pipeline
├── exercise4/          # Streaming AI customer support analyzer
└── README.md           # This file
```

Each folder is a standalone exercise with its own dependencies, Kafka topics, and README.

---

## System Requirements

Install the following tools before running any exercise.

**Docker and docker-compose**
```bash
docker --version
docker compose version
```

**Bun** (Node.js runtime used by all TypeScript exercises)
```bash
bun --version   # >= 1.0
```

**Python** (required for Exercise 4 Python workers)
```bash
python --version   # >= 3.10
```

**Ollama** (required for Exercise 4 PII sanitization)

Install from [ollama.com](https://ollama.com), then pull the model:
```bash
ollama pull llama3
```

**OpenAI API key** (required for Exercise 3)

Create a `.env` file inside `exercise3/`:
```
OPENAI_API_KEY=your_key_here
```

---

## Running Kafka

Each exercise includes its own `docker-compose.yml`. Start Kafka from inside the exercise folder before running any services:

```bash
cd exercise1   # or exercise2, exercise3, exercise4
docker compose up -d
```

Kafka must be running before starting any producer, consumer, or worker. Refer to each exercise README for the exact startup order.

---

## Exercises Overview

### Exercise 1 — Microservices Chat System

A fully distributed chatbot where every component is a separate Kafka microservice. No service calls another directly — all communication flows exclusively through Kafka topics. Supports weather queries, currency exchange, math calculations, and general chat.

→ [`exercise1/README.md`](exercise1/README.md)

---

### Exercise 2 — Kafka Integration with LLM

Extends Exercise 1 by inserting an LLM-powered classification and extraction pipeline. Demonstrates four prompt engineering techniques: Few-Shot Prompting, Structured JSON Output, Chain-of-Thought Reasoning, and Persona Prompt. Includes a GuardrailService that screens unsafe input.

→ [`exercise2/README.md`](exercise2/README.md)

---

### Exercise 3 — LLM Processing Pipeline

A real-time review analysis system. Users submit reviews through a CLI producer. A processor applies a three-step LLM pipeline (routing, structured extraction, and self-correction) before results are displayed by a live analytics consumer.

→ [`exercise3/README.md`](exercise3/README.md)

---

### Exercise 4 — Streaming AI Customer Support Analyzer

A production-style streaming pipeline for customer support. Messages are sanitized by a local Ollama model, fanned out to two parallel HuggingFace AI workers, and correlated by an aggregator that raises a strong alert when a message is both negative and urgent.

→ [`exercise4/README.md`](exercise4/README.md)
