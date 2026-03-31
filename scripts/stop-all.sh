#!/usr/bin/env bash
# Stop all running Bun services.
# Usage: bash scripts/stop-all.sh

echo "=== Stopping all Bun services ==="
pkill -f bun && echo "Bun processes stopped." || echo "No Bun processes found."

echo "=== Stopping RAG retriever ==="
pkill -f "rag_retriever.py" && echo "RAG retriever stopped." || echo "No RAG retriever process found."
