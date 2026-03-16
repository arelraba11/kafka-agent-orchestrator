#!/usr/bin/env bash
# Exercise 2 — LLM Prompt Engineering Pipeline
# Usage: bash scripts/start-ex2.sh

set -e
cd "$(dirname "$0")/.."

LOG_DIR="scripts/logs/ex2-services"
mkdir -p "$LOG_DIR"

export ROUTER_MODE=llm

bun run services/core/memoryService.ts        > "$LOG_DIR/memory-service.log"        2>&1 &
bun run services/core/routerService.ts        > "$LOG_DIR/router-service.log"         2>&1 &
bun run services/llm/guardrailService.ts      > "$LOG_DIR/guardrail-service.log"      2>&1 &
bun run services/llm/llmRouterService.ts      > "$LOG_DIR/llm-router-service.log"     2>&1 &
bun run services/llm/cotMathService.ts        > "$LOG_DIR/cot-math-service.log"       2>&1 &
bun run services/apps/mathApp.ts              > "$LOG_DIR/math-app.log"               2>&1 &
bun run services/apps/weatherApp.ts           > "$LOG_DIR/weather-app.log"            2>&1 &
bun run services/apps/exchangeApp.ts          > "$LOG_DIR/exchange-app.log"           2>&1 &
bun run services/apps/generalChatApp.ts       > "$LOG_DIR/general-chat-app.log"       2>&1 &
bun run services/core/responseAggregator.ts   > "$LOG_DIR/response-aggregator.log"   2>&1 &

echo "Exercise 2 services started (ROUTER_MODE=llm). Logs in $LOG_DIR/"
echo "Start the UI manually: bun run services/core/userInterface.ts"
