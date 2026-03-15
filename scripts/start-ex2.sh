#!/usr/bin/env bash
# Exercise 2 — LLM Prompt Engineering Pipeline
# Usage: bash scripts/start-ex2.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Starting Exercise 2 — LLM Prompt Engineering Pipeline (ROUTER_MODE=llm) ==="

export ROUTER_MODE=llm

bun run services/memory-service/memoryService.ts &
bun run services/response-aggregator/responseAggregator.ts &

bun run services/apps/mathApp.ts &
bun run services/apps/weatherApp.ts &
bun run services/apps/exchangeApp.ts &
bun run services/apps/generalChatApp.ts &

bun run services/guardrail-service/guardrailService.ts &
bun run services/llm-router-service/llmRouterService.ts &
bun run services/cot-math-service/cotMathService.ts &

bun run services/router-service/routerService.ts &

echo ""
echo "All background services started. Launching user interface..."
echo ""

bun run services/user-interface/userInterface.ts

wait
