#!/usr/bin/env bash
# scripts/stop-all.sh
# Stops all running Bun services.
# Usage: bash scripts/stop-all.sh

pkill -f bun && echo "[stop] All Bun services stopped." || echo "[stop] No Bun processes found."
