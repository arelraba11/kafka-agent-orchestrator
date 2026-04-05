#!/usr/bin/env bash
# Stop all project services (bun workers, webServer, Vite, RAG retriever).
# Usage: bash scripts/stop-all.sh  OR  bun run stop

set -euo pipefail
cd "$(dirname "$0")/.."

# ─── Helper ──────────────────────────────────────────────────────────────────

kill_graceful() {
  local label="$1"
  local pattern="$2"

  if pkill -TERM -f "$pattern" 2>/dev/null; then
    echo "  [SIGTERM] $label"
    sleep 1
    # Force-kill anything still alive
    if pkill -KILL -f "$pattern" 2>/dev/null; then
      echo "  [SIGKILL] $label (did not exit cleanly)"
    fi
  else
    echo "  [skip]    $label — not running"
  fi
}

echo ""
echo "=== Stopping all project services ==="
echo ""

# ─── Bun service workers ─────────────────────────────────────────────────────
kill_graceful "bun services"         "bun run"
kill_graceful "bun ts runner"        "bun src/"

# ─── Vite dev server (bun run web:dev) ───────────────────────────────────────
kill_graceful "vite dev server"      "vite"

# ─── RAG retriever (Python) ──────────────────────────────────────────────────
kill_graceful "rag_retriever"        "rag_retriever.py"

# ─── LevelDB lock cleanup ─────────────────────────────────────────────────────
# The orchestrator holds a lock on .plan-store/LOCK while running.
# If it was killed abnormally the file lingers and blocks the next start.
LOCK_FILE=".plan-store/LOCK"
if [ -f "$LOCK_FILE" ]; then
  rm -f "$LOCK_FILE"
  echo "  [clean]   removed stale LevelDB lock ($LOCK_FILE)"
fi

echo ""
echo "=== All services stopped ==="
echo ""
