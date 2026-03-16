#!/usr/bin/env bash
# Exercise 3 — Review Analysis Pipeline
# Usage: bash scripts/start-ex3.sh
#
# Starts the analytics consumer and LLM processor in the background.
# Run the producer separately (it is interactive):
#   bun run services/review-producer/reviewProducer.ts

set -e
cd "$(dirname "$0")/.."

LOG_DIR="scripts/logs/ex3-services"
mkdir -p "$LOG_DIR"

bun run services/reviews/reviewProcessor.ts  > "$LOG_DIR/review-processor.log"   2>&1 &
bun run services/reviews/reviewAnalytics.ts  > "$LOG_DIR/review-analytics.log"   2>&1 &

echo "Exercise 3 services started. Logs in $LOG_DIR/"
echo "Start the producer manually: bun run services/reviews/reviewProducer.ts"
