#!/usr/bin/env bash
# Final Project — Event-Sourced Kafka Agent
# Usage: bash scripts/start-final.sh

set -e
cd "$(dirname "$0")/.."

LOG_DIR="scripts/logs/final-project-services"
mkdir -p "$LOG_DIR"

# ─── Wait for Kafka broker to be reachable ────────────────────────────────────
wait_for_kafka() {
  local host="localhost"
  local port="9092"
  local max_wait=60
  local elapsed=0
  echo "Waiting for Kafka at ${host}:${port}..."
  while ! nc -z "$host" "$port" 2>/dev/null; do
    if [ "$elapsed" -ge "$max_wait" ]; then
      echo "ERROR: Kafka not reachable after ${max_wait}s. Is 'docker-compose up -d' running?"
      exit 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "Kafka is reachable. Waiting 3s for metadata to settle..."
  sleep 3
}

# ─── Ensure topics exist ─────────────────────────────────────────────────────
ensure_topics() {
  echo "Creating/verifying topics..."
  bash infra/topics-final.sh 2>/dev/null || true
  echo "Topics ready."
}

# ─── Run pre-flight checks ────────────────────────────────────────────────────
wait_for_kafka
ensure_topics

# ─── Launch services ─────────────────────────────────────────────────────────

bun run src/node/core/routerService.ts                  > "$LOG_DIR/router.log"       2>&1 &
echo "Started routerService (PID $!)"

bun run src/node/orchestration/orchestrator.ts          > "$LOG_DIR/orchestrator.log" 2>&1 &
echo "Started orchestrator (PID $!)"

bun run src/node/orchestration/aggregator.ts            > "$LOG_DIR/aggregator.log"   2>&1 &
echo "Started aggregator (PID $!)"

bun run src/node/apps/mathApp.ts                        > "$LOG_DIR/math.log"         2>&1 &
echo "Started mathApp (PID $!)"

bun run src/node/apps/weatherApp.ts                     > "$LOG_DIR/weather.log"      2>&1 &
echo "Started weatherApp (PID $!)"

bun run src/node/apps/exchangeApp.ts                    > "$LOG_DIR/exchange.log"     2>&1 &
echo "Started exchangeApp (PID $!)"

bun run src/node/apps/generalChatApp.ts                 > "$LOG_DIR/chat.log"         2>&1 &
echo "Started generalChatApp (PID $!)"

bun run src/node/orchestration/answerSynthesizer.ts     > "$LOG_DIR/answer.log"       2>&1 &
echo "Started answerSynthesizer (PID $!)"

src/python/venv/bin/python -u src/python/rag/rag_retriever.py > "$LOG_DIR/rag.log"   2>&1 &
echo "Started rag_retriever (PID $!)"

echo ""
echo "FINAL PROJECT SERVICES STARTED"
echo "Logs in $LOG_DIR/"
echo ""
echo "Start the UI manually:"
echo "  bun run src/node/core/userInterface.ts"
echo ""
echo "To stop all services:"
echo "  bash scripts/stop-all.sh"
