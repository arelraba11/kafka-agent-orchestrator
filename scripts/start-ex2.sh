#!/usr/bin/env bash
# Exercise 2 — LLM Prompt Engineering Pipeline
# Usage: bash scripts/start-ex2.sh

set -e
cd "$(dirname "$0")/.."

mkdir -p logs

export ROUTER_MODE=llm

bun run services/memory-service/memoryService.ts           > logs/memory-service.log 2>&1 &
bun run services/response-aggregator/responseAggregator.ts > logs/response-aggregator.log 2>&1 &
bun run services/apps/mathApp.ts                           > logs/math-app.log 2>&1 &
bun run services/apps/weatherApp.ts                        > logs/weather-app.log 2>&1 &
bun run services/apps/exchangeApp.ts                       > logs/exchange-app.log 2>&1 &
bun run services/apps/generalChatApp.ts                    > logs/general-chat-app.log 2>&1 &
bun run services/guardrail-service/guardrailService.ts     > logs/guardrail-service.log 2>&1 &
bun run services/llm-router-service/llmRouterService.ts    > logs/llm-router-service.log 2>&1 &
bun run services/cot-math-service/cotMathService.ts        > logs/cot-math-service.log 2>&1 &
bun run services/router-service/routerService.ts           > logs/router-service.log 2>&1 &

echo "Exercise 2 services started (ROUTER_MODE=llm). Logs in logs/"
echo "Start the UI manually: bun run services/user-interface/userInterface.ts"
