#!/usr/bin/env bash
# Exercise 1 — Distributed Chatbot (regex router)
# Usage: bash scripts/start-ex1.sh

set -e
cd "$(dirname "$0")/.."

mkdir -p logs

export ROUTER_MODE=regex

bun run services/memory-service/memoryService.ts           > logs/memory-service.log 2>&1 &
bun run services/response-aggregator/responseAggregator.ts > logs/response-aggregator.log 2>&1 &
bun run services/apps/mathApp.ts                           > logs/math-app.log 2>&1 &
bun run services/apps/weatherApp.ts                        > logs/weather-app.log 2>&1 &
bun run services/apps/exchangeApp.ts                       > logs/exchange-app.log 2>&1 &
bun run services/apps/generalChatApp.ts                    > logs/general-chat-app.log 2>&1 &
bun run services/router-service/routerService.ts           > logs/router-service.log 2>&1 &

echo "Exercise 1 services started (ROUTER_MODE=regex). Logs in logs/"
echo "Start the UI manually: bun run services/user-interface/userInterface.ts"
