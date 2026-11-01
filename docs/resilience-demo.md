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

There is no startup scan: the orchestrator does not look for incomplete plans when it starts. Because the next request is emitted before the new state is persisted and before the offset is committed, a crash mid-handler replays the triggering event instead of skipping a step.

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

New requests complete end-to-end after the restart. A plan that was in flight when the orchestrator died is redelivered by Kafka from the last committed offset, and the idempotent handler either re-applies the event or ignores it as a duplicate.

---

## Scenario 3 — Duplicate Events

**What breaks:** A `ToolInvocationRequested` or `ToolInvocationResulted` event is delivered more than once (consumer offset not committed, Kafka retry, or a manual re-publish via the CLI).

**What the code does:** The orchestrator is idempotent. `ToolInvocationRequested` and `ToolInvocationResulted` carry `payload.stepIndex`, and workers echo it back. The orchestrator emits its outbound events first, persists the new plan state second (`sync: true`), and Kafka commits the offset after the handler returns, so a crash at any point replays the message safely.

### What is protected

- **Workers filter by `toolName`.** Each worker ignores requests for other tools (`if (req.payload.toolName !== "weather") return;` in `src/node/apps/weatherApp.ts`, and the same in the other workers). That is routing, not deduplication.
- **A duplicate, stale or out-of-order result never changes the plan.** The orchestrator applies a result only when the plan is `running`, `stepIndex` is the step it awaits, and `toolName` matches that step. Otherwise it writes nothing, emits nothing and logs `ignored reason=<reason>`. The reasons are `unknown-plan`, `plan-terminal` (the plan already completed or failed), `missing-step-index`, `duplicate-step`, `future-step` and `tool-mismatch`.
- **A replayed `PlanGenerated`** for a conversation that already has a plan is ignored (`duplicate-plan`).
- **Terminal plans are kept as tombstones**, so a result that arrives after the plan finished is recognised (`plan-terminal`) instead of looking like an unknown conversation.
- **The RAG worker no longer loses a result.** It commits an offset only after the result send is acknowledged, so a failed send leaves the request uncommitted for redelivery.

### What is not protected

- **Workers do not deduplicate**, so a duplicate `ToolInvocationRequested` runs the tool again and emits another result. The orchestrator ignores the extra one (`duplicate-step`).
- **A duplicate `PlanCompleted` downstream.** If the orchestrator crashes after publishing `PlanCompleted` and before persisting `completed`, the last result is replayed and `PlanCompleted` is published again. The aggregator and synthesizer then run twice, giving two LLM calls and a doubled `history.json` entry. The web client shows only the first answer. Planned for phase 4.
- **Plan tombstones are never pruned** and grow without bound.

### Steps to reproduce the terminal-plan drop

```bash
# After a plan has completed, re-publish one of its ToolInvocationResulted events.

# 1. Read recent messages from conversation-events
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic conversation-events \
  --from-beginning \
  --max-messages 30

# 2. Copy a conversationId and its last ToolInvocationResulted JSON, then re-publish
#    it with the key, as <conversationId>|<json> on one line
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic conversation-events \
  --property parse.key=true \
  --property key.separator='|'
```

### Expected log output (orchestrator.log)

```
[orchestrator] conversationId=<id> plan completed, results=1
[Benchmark] conversationId=<id> workerLatency=Xms
[orchestrator] conversationId=<id> ignored reason=plan-terminal plan status=completed
```

The UI receives exactly one answer, and `aggregator.log` shows no second `PlanCompleted`.

To see the live-plan case, re-publish a `ToolInvocationResulted` from step 1 of a multi-step plan (for example `weather in paris and convert 100 USD to EUR`) while the plan is still running. The orchestrator logs `ignored reason=duplicate-step` and the plan finishes normally. No automated integration test covers this yet; phase 5 adds them.

---

## Summary

| Test | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka redelivers from the last committed offset on restart | Automatic |
| Orchestrator crash | Kafka redelivery + LevelDB state when the next result arrives (no startup scan) | Automatic for redelivered events |
| Duplicate events | `stepIndex` + `toolName` check in the orchestrator; emit, then persist, then offset commit | Automatic for plan state (duplicate downstream events remain) |
