#!/usr/bin/env bash
# Final Project — Event-Sourced Kafka Agent
# Usage: bash scripts/start-final.sh

set -e
cd "$(dirname "$0")/.."

LOG_DIR="scripts/logs/final-project-services"
mkdir -p "$LOG_DIR"

bun run services/core/routerService.ts                  > "$LOG_DIR/router.log"       2>&1 &
echo "Started routerService"

bun run services/orchestration/orchestrator.ts          > "$LOG_DIR/orchestrator.log" 2>&1 &
echo "Started orchestrator"

bun run services/apps/mathApp.ts                        > "$LOG_DIR/math.log"         2>&1 &
echo "Started mathApp"

bun run services/apps/weatherApp.ts                     > "$LOG_DIR/weather.log"      2>&1 &
echo "Started weatherApp"

bun run services/apps/exchangeApp.ts                    > "$LOG_DIR/exchange.log"     2>&1 &
echo "Started exchangeApp"

bun run services/apps/generalChatApp.ts                 > "$LOG_DIR/chat.log"         2>&1 &
echo "Started generalChatApp"

bun run services/orchestration/answerSynthesizer.ts     > "$LOG_DIR/answer.log"       2>&1 &
echo "Started answerSynthesizer"

echo ""
echo "FINAL PROJECT SERVICES STARTED"
echo "Logs in $LOG_DIR/"
echo ""
echo "Start the UI manually:"
echo "  bun run services/core/userInterface.ts"
echo ""
echo "To stop all services:"
echo "  bash scripts/stop-all.sh"
echo "or"
echo "  pkill -f bun"
