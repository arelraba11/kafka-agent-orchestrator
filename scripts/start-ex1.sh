#!/usr/bin/env bash
# Exercise 1 — Distributed Chatbot (regex router)
# Usage: bash scripts/start-ex1.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Starting Exercise 1 — Distributed Chatbot (ROUTER_MODE=regex) ==="

export ROUTER_MODE=regex

bun run services/memory-service/memoryService.ts &
bun run services/response-aggregator/responseAggregator.ts &

bun run services/apps/mathApp.ts &
bun run services/apps/weatherApp.ts &
bun run services/apps/exchangeApp.ts &
bun run services/apps/generalChatApp.ts &

bun run services/router-service/routerService.ts &

echo ""
echo "All background services started. Launching user interface..."
echo ""

bun run services/user-interface/userInterface.ts

wait
