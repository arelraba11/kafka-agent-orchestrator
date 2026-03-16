#!/usr/bin/env bash
# Creates all Exercise 1–4 and Final Project Kafka topics.
# Run after Kafka is up: bash infra/topics.sh

KAFKA_CONTAINER="kafka"
PARTITIONS=1
REPLICATION=1

TOPICS=(
  "user-input-events"
  "user-control-events"
  "intent-math"
  "intent-weather"
  "intent-exchange"
  "intent-general-chat"
  "app-results"
  "bot-responses"
  "conversation-history-update"
  "router-decision-events"
  "guardrail-violation-events"
  # Exercise 3 — Review Analysis Pipeline
  "raw-reviews-topic"
  "processed-insights-topic"
  # Exercise 4 — Customer Support Analysis Pipeline
  "raw-customer-messages"
  "sanitized-messages"
  "analysis-sentiment"
  "analysis-urgency"
  # Final Project — Event-Sourced AI Agent
  "user-commands"
  "conversation-events"
  "tool-invocation-requests"
  "dead-letter-queue"
)

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
done

echo "All topics created."
