#!/usr/bin/env bash
# Creates Final Project Kafka topics (1 partition for development).
# For production (3 partitions), use: bash infra/topics-final.sh
# Run after Kafka is up: bash infra/topics.sh

KAFKA_CONTAINER="kafka"
PARTITIONS=1
REPLICATION=1

TOPICS=(
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
