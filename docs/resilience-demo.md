# Resilience Demo — Kafka AI Agent

Three failure scenarios with the behavior the code has today and how to reproduce each one.

All commands assume the repository root as the working directory, Kafka running via `infra/docker-compose.yml`, and the services started with `bun run start`. Logs are written to `scripts/logs/services/`.

Kafka CLI tools are not on the container's PATH; call them as `/opt/kafka/bin/<tool>.sh`.

---

## Scenario 1 — Worker Crash

**What breaks:** A tool worker process is killed while a plan is in flight.

**Why it recovers:** Kafka keeps the messages after the worker's last committed offset. When the worker restarts it rejoins its consumer group and reads the `ToolInvocationRequested` events it had not yet processed.

### Steps

```bash
# 1. Start all services
bun run start
open http://localhost:3001   # or bun run web:dev for hot-reload

# 2. Send a query that routes to the weather worker. Type in the UI:
weather in tel aviv
#    Expected log sequence (scripts/logs/services/):
#      router.log        [router] conversationId=<id> plan=[weather] input="..."
#      router.log        [Benchmark] conversationId=<id> routerLatency=Xms
#      orchestrator.log  [orchestrator] conversationId=<id> dispatching tool="weather"
#      weather.log       [weather] conversationId=<id> ...
#      orchestrator.log  [orchestrator] conversationId=<id> step 1/1 completed tool="weather"
#      orchestrator.log  [Benchmark] conversationId=<id> workerLatency=Xms

# 3. Kill the weather worker
pkill -f "weatherApp.ts"

# 4. Send another query while the worker is down. Type in the UI:
weather in haifa
#    The orchestrator dispatches ToolInvocationRequested to Kafka, but no worker
#    consumes it. The plan stays pending and no answer arrives.

# 5. Restart the weather worker
bun run weather >> scripts/logs/services/weather.log 2>&1 &
#    The worker reads the pending message from its last committed offset and
#    emits ToolInvocationResulted. The orchestrator completes the plan and the
#    UI receives the answer.
```

### Expected outcome

The UI receives the weather answer for the second query after the worker is back up. No manual Kafka intervention is needed.

---

## Scenario 2 — Orchestrator Crash

**What breaks:** The orchestrator process is killed and restarted.

**Why it recovers:** Two mechanisms work together. Kafka redelivers events whose offsets the orchestrator had not committed. Plan state lives in LevelDB (`shared/state/planStore.ts`, `.plan-store/`), so when the next `ToolInvocationResulted` for a stored plan arrives, the restarted orchestrator finds the state and continues from the saved `stepIndex`.

There is no startup scan: the orchestrator does not look for incomplete plans when it starts. A plan whose result event was already committed and consumed before the crash, with the next step never dispatched, is not resumed.

### Steps

```bash
# 1. Start all services
bun run start
open http://localhost:3001

# 2. Verify end-to-end works. Type in the UI:
weather in jerusalem
#    Confirm the answer arrives.

# 3. Kill the orchestrator
pkill -f "orchestrator.ts"

# 4. Restart the orchestrator
bun run orchestrator >> scripts/logs/services/orchestrator.log 2>&1 &
#    Expected startup lines:
#      [orchestrator] PlanStore initialized
#      [orchestrator] OrchestratorService started.

# 5. Send a new query. Type in the UI:
convert 50 eur to usd
#    The restarted orchestrator receives the PlanGenerated event, stores state
#    in LevelDB, dispatches the tool, and completes the plan.
```

### Expected log output (orchestrator.log)

```
[orchestrator] conversationId=<id> plan received steps=[exchange]
[orchestrator] conversationId=<id> dispatching tool="exchange"
[orchestrator] conversationId=<id> step 1/1 completed tool="exchange"
[orchestrator] conversationId=<id> plan completed, results=1
[Benchmark] conversationId=<id> workerLatency=Xms
```

### Expected outcome

New requests complete end-to-end after the restart. A plan that was in flight when the orchestrator died is also redelivered by Kafka if its triggering event was not yet committed; plans that fall between committed events are not resumed (see above).

---

## Scenario 3 — Duplicate Events

**What breaks:** A `ToolInvocationRequested` or `ToolInvocationResulted` event is delivered more than once (consumer offset not committed, Kafka retry, or a manual re-publish via the CLI).

**What the code does:** Duplicate handling is partial. Phase 2 will fix the gaps listed here.

### What is protected

- **Workers filter by `toolName`.** Each worker ignores requests for other tools (`if (req.payload.toolName !== "weather") return;` in `src/node/apps/weatherApp.ts`, and the same in the other workers). That is routing, not deduplication.
- **A stale result is dropped.** When a `ToolInvocationResulted` arrives for a `conversationId` with no stored plan (the plan already completed and was deleted), the orchestrator logs a warning and returns:

```typescript
// src/node/orchestration/orchestrator.ts
if (!state) {
  console.warn(`[orchestrator] Received result for unknown conversationId=${conversationId}, skipping.`);
  return;
}
```

### What is not protected

- **A duplicate result while the plan is live** is not detected. The orchestrator appends it to `results` and advances `stepIndex` without checking it against `plan[stepIndex].tool`. This can skip a step or emit `PlanCompleted` before all steps ran.
- **A redelivered `PlanGenerated`** overwrites the stored state and re-dispatches step 0.
- **Workers do not deduplicate**, so a duplicate `ToolInvocationRequested` runs the tool again and emits another result.

### Steps to reproduce the stale-result drop

```bash
# After a plan has completed, re-publish one of its ToolInvocationResulted events.

# 1. Read recent messages from conversation-events
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic conversation-events \
  --from-beginning \
  --max-messages 30

# 2. Copy a ToolInvocationResulted JSON, then re-publish it
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic conversation-events
# Paste the JSON on one line and press Enter.
```

### Expected log output (orchestrator.log)

```
[orchestrator] conversationId=<id> plan completed, results=1
[Benchmark] conversationId=<id> workerLatency=Xms
[orchestrator] Received result for unknown conversationId=<id>, skipping.
```

The UI receives exactly one answer.

To see the unprotected case, re-publish a `ToolInvocationResulted` from step 1 of a multi-step plan (for example `weather in paris and convert 100 USD to EUR`) while the plan is still running. From reading `orchestrator.ts:157-160`, the duplicate is counted as the next step's result. This has not been exercised by an automated test yet; phase 5 adds integration tests.

---

## Summary

| Test | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka redelivers from the last committed offset on restart | Automatic |
| Orchestrator crash | Kafka redelivery + LevelDB state when the next result arrives (no startup scan) | Automatic for redelivered events |
| Duplicate events | Stale result for an unknown `conversationId` is dropped; a duplicate for a live plan is not detected | Partial (fixed in phase 2) |
