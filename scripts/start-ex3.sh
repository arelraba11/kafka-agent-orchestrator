#!/usr/bin/env bash
# Exercise 3 — Review Analysis Pipeline
# Usage: bash scripts/start-ex3.sh
#
# Starts the analytics consumer and LLM processor in the background.
# Run the producer separately (it is interactive):
#   bun run services/review-producer/reviewProducer.ts

set -e
cd "$(dirname "$0")/.."

mkdir -p logs

bun run services/review-analytics/reviewAnalytics.ts > logs/review-analytics.log 2>&1 &
bun run services/review-processor/reviewProcessor.ts > logs/review-processor.log 2>&1 &

echo "Exercise 3 services started. Logs in logs/"
echo "Start the producer manually: bun run services/review-producer/reviewProducer.ts"
