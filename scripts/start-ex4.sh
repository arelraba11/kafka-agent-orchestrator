#!/usr/bin/env bash
# Exercise 4 — Customer Support Analysis Pipeline
# Usage: bash scripts/start-ex4.sh
#
# Starts the sanitizer, sentiment analyzer, urgency classifier, and
# insight aggregator in the background.
# Run the producer separately (it is interactive):
#   bun run services/customer-support-producer/customerSupportProducer.ts
#
# Requirements:
#   - OPENAI_API_KEY set in the environment (sentimentAnalyzer, urgencyClassifier)
#   - Ollama running locally with llama3 pulled (sanitizerService)

set -e
cd "$(dirname "$0")/.."

LOG_DIR="scripts/logs/ex4-services"
mkdir -p "$LOG_DIR"

bun run services/sanitizer-service/sanitizerService.ts             > "$LOG_DIR/sanitizer-service.log"    2>&1 &
bun run services/sentiment-analyzer/sentimentAnalyzer.ts           > "$LOG_DIR/sentiment-analyzer.log"   2>&1 &
bun run services/urgency-classifier/urgencyClassifier.ts           > "$LOG_DIR/urgency-classifier.log"   2>&1 &
bun run services/insight-aggregator/insightAggregator.ts           > "$LOG_DIR/insight-aggregator.log"   2>&1 &

echo "Exercise 4 services started. Logs in $LOG_DIR/"
echo "Start the producer manually: bun run services/customer-support-producer/customerSupportProducer.ts"
