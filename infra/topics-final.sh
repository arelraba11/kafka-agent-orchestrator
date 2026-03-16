#!/usr/bin/env bash
# Creates the Final Project Kafka topics with 3 partitions.
# Run after Kafka is up: bash infra/topics-final.sh
#
# The general topics.sh creates all topics with 1 partition.
# This script recreates the four final-project topics with 3 partitions
# so the orchestrator and tool workers can scale horizontally.

set -euo pipefail

KAFKA_CONTAINER="kafka"
PARTITIONS=3
REPLICATION=1

TOPICS=(
  "user-commands"
  "conversation-events"
  "tool-invocation-requests"
  "dead-letter-queue"
)

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC  (partitions=$PARTITIONS, replication-factor=$REPLICATION)"
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
done

echo ""
echo "Final-project topics ready."
echo ""
echo "Verify with:"
echo "  docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \\"
echo "    --bootstrap-server localhost:9092 --list"
