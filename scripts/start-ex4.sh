#!/usr/bin/env bash
# Exercise 4 — Customer Support Analysis Pipeline
# Usage: bash scripts/start-ex4.sh
#
# Starts the sanitizer, sentiment analyzer, urgency classifier, and
# insight aggregator in the background.
# Run the producer separately (it is interactive):
#   bun run src/node/customer-support/customerSupportProducer.ts
#
# Requirements:
#   - OPENAI_API_KEY set in the environment (sentimentAnalyzer, urgencyClassifier)
#   - Ollama running locally with llama3 pulled (sanitizerService)

set -e
cd "$(dirname "$0")/.."

LOG_DIR="scripts/logs/ex4-services"
mkdir -p "$LOG_DIR"

bun run src/node/customer-support/sanitizerService.ts      > "$LOG_DIR/sanitizer-service.log"    2>&1 &
bun run src/node/customer-support/sentimentAnalyzer.ts     > "$LOG_DIR/sentiment-analyzer.log"   2>&1 &
bun run src/node/customer-support/urgencyClassifier.ts     > "$LOG_DIR/urgency-classifier.log"   2>&1 &
bun run src/node/customer-support/insightAggregator.ts     > "$LOG_DIR/insight-aggregator.log"   2>&1 &

echo "Exercise 4 services started. Logs in $LOG_DIR/"
echo "Start the producer manually: bun run src/node/customer-support/customerSupportProducer.ts"
