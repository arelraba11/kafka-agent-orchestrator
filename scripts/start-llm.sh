#!/usr/bin/env bash
# scripts/start-llm.sh
# Starts the full system with ROUTER_MODE=llm (LLM-based intent classification).
# Requires OPENAI_API_KEY set in .env or the shell environment.
# Usage: bash scripts/start-llm.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export ROUTER_MODE=llm

# Load .env if present
if [[ -f "$ROOT/.env" ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +o allexport
fi

# Guard: OPENAI_API_KEY must be set
if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "[start] ERROR: OPENAI_API_KEY is not set."
  echo "[start]        Add it to .env or export it before running this script."
  exit 1
fi

echo "[start] ─────────────────────────────────────────────"
echo "[start]  Kafka Beginners Course — LLM mode"
echo "[start] ─────────────────────────────────────────────"
echo "[start] Creating Kafka topics..."
bash "$ROOT/infra/topics.sh"
echo ""

echo "[start] Starting services (ROUTER_MODE=llm)..."
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

# LLM pipeline services
bun run "$ROOT/services/guardrail-service/guardrailService.ts" &
echo "[start] Started guardrail-service (PID $!)"

bun run "$ROOT/services/llm-router-service/llmRouterService.ts" &
echo "[start] Started llm-router-service (PID $!)"

bun run "$ROOT/services/cot-math-service/cotMathService.ts" &
echo "[start] Started cot-math-service (PID $!)"

# Router
bun run "$ROOT/services/router-service/routerService.ts" &
echo "[start] Started router-service (PID $!)"

# User interface — started last
sleep 1
bun run "$ROOT/services/user-interface/userInterface.ts" &
echo "[start] Started user-interface (PID $!)"

echo ""
echo "[start] ─────────────────────────────────────────────"
echo "[start]  All 11 services running. Press CTRL+C to stop."
echo "[start] ─────────────────────────────────────────────"
echo ""

wait
