# Backlog

Issues found while verifying a fresh clone (2026-10-28). Not committed.

## Bugs

### 1. Synthesizer failure is silent
- **Where:** `src/node/orchestration/answerSynthesizer.ts` (no error handling around `callLLM`), `src/node/core/webServer.ts` (only forwards `FinalAnswerSynthesized`).
- **What happens:** with an invalid `OPENAI_API_KEY`, `callLLM` throws a 401. The kafkajs consumer retries five times, crashes, and restarts in a loop (`answer.log`). No error event is produced. `conversation-events` has no `FinalAnswerSynthesized`, and the browser client waits the full 60 seconds with no feedback.
- **Fix:** catch the failure in the synthesizer, publish an error event (for example `PlanFailed` or a new `FinalAnswerFailed`) to `conversation-events`, and have `webServer.ts` send `{ type: "error" }` to the client.

### 2. `start.ts` pulls `llama3.2` even when `LLM_PROVIDER=openai`
- **Where:** `scripts/start.ts`, `pullOllamaModel()`.
- **What happens:** whenever Ollama is reachable, the script pulls the model (about 2 GB), even though `LLM_PROVIDER=openai` never calls Ollama.
- **Fix:** return early when `process.env.LLM_PROVIDER === "openai"`.

## README corrections

- **Chunk count:** `docs/benchmark.md` reports a 53-chunk index. `index_kb.py` produces 21 chunks from the current `data/products/`. Either update the benchmark or the README.
- **Product file list:** the README file tree lists `iphone.txt`, `macbook.txt` and `tesla.txt`. `data/products/` also contains `honda.txt` and `ipad.txt`.
- **Frontend install:** `bun run web:dev` needs `cd src/frontend && bun install`. The README only shows the root `bun install`. (`bun run start` works without it, because `src/frontend/dist/` is committed.)
- **WebSocket path:** the endpoint is `ws://localhost:3001/ws`. This is not documented. The server rejects upgrades on `/`.
- **OpenAI key requirement:** with the default `LLM_PROVIDER=openai`, `.env` needs a valid `OPENAI_API_KEY`. An invalid key fails silently (see bug 1). Also state the Python 3.12 requirement, which currently appears only in `CLAUDE.md`.

## Also observed (not requested)

- `docs/benchmark.md` accuracy scores have no stated method (see the earlier handoff).
- The `chromadb` service in `infra/docker-compose.yml` is unused. `rag_retriever.py` and `index_kb.py` use a local `PersistentClient`.
- The `ollama` service is unused when `LLM_PROVIDER=openai`, and `start.ts` still probes it.
