#!/usr/bin/env bash
# scripts/start-regex.sh
# Starts the full system with ROUTER_MODE=regex (rule-based intent classification).
# Usage: bash scripts/start-regex.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export ROUTER_MODE=regex

echo "[start] ─────────────────────────────────────────────"
echo "[start]  Kafka Beginners Course — REGEX mode"
echo "[start] ─────────────────────────────────────────────"
echo "[start] Creating Kafka topics..."
bash "$ROOT/infra/topics.sh"
echo ""

echo "[start] Starting services (ROUTER_MODE=regex)..."
echo ""

# Core services
bun run "$ROOT/services/memory-service/memoryService.ts" &
echo "[start] Started memory-service (PID $!)"

bun run "$ROOT/services/response-aggregator/responseAggregator.ts" &
echo "[start] Started response-aggregator (PID $!)"

# Domain apps
bun run "$ROOT/services/apps/mathApp.ts" &
echo "[start] Started math-app (PID $!)"

bun run "$ROOT/services/apps/weatherApp.ts" &
echo "[start] Started weather-app (PID $!)"

bun run "$ROOT/services/apps/exchangeApp.ts" &
echo "[start] Started exchange-app (PID $!)"

bun run "$ROOT/services/apps/generalChatApp.ts" &
echo "[start] Started general-chat-app (PID $!)"

# Router
bun run "$ROOT/services/router-service/routerService.ts" &
echo "[start] Started router-service (PID $!)"

# User interface — started last
sleep 1
bun run "$ROOT/services/user-interface/userInterface.ts" &
echo "[start] Started user-interface (PID $!)"

echo ""
echo "[start] ─────────────────────────────────────────────"
echo "[start]  All 8 services running. Press CTRL+C to stop."
echo "[start] ─────────────────────────────────────────────"
echo ""

wait
