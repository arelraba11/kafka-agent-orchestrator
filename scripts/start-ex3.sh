#!/usr/bin/env bash
# Exercise 3 — Review Analysis Pipeline
# Usage: bash scripts/start-ex3.sh
#
# Starts the analytics consumer and LLM processor.
# Run the producer separately (it is interactive):
#   bun run services/review-producer/reviewProducer.ts

set -e
cd "$(dirname "$0")/.."

echo "=== Starting Exercise 3 — Review Analysis Pipeline ==="
echo ""
echo "Note: producer is interactive — run it manually in a separate terminal:"
echo "  bun run services/review-producer/reviewProducer.ts"
echo ""

bun run services/review-analytics/reviewAnalytics.ts &
bun run services/review-processor/reviewProcessor.ts &

echo "Analytics and processor started. Waiting..."

wait
