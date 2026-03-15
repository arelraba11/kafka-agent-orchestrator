#!/usr/bin/env bash
# Stop all running Bun services.
# Usage: bash scripts/stop-all.sh

echo "=== Stopping all Bun services ==="
pkill -f bun && echo "Done." || echo "No Bun processes found."
