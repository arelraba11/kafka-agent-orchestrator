# Resilience Demo — Kafka AI Agent

Five failure scenarios with the behavior the code has today and how to reproduce each one.

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

## Scenario 4 — Poison Message

**What breaks:** A message that can never be processed lands on a topic: malformed JSON, a known type that violates its contract, or a request the RAG worker cannot read.

**What the code does:** Every consumer validates through `subscribeAndRun` (the Python RAG worker does the same in its own loop). A poison message is written to `dead-letter-queue` as a `MessageDeadLettered` record and its offset is committed, so the partition keeps flowing and no conversation stalls. Before this was in place, a message like that was either dropped silently (bad JSON) or thrown at forever (wrong shape). Transient infrastructure errors are different: they are redelivered, never dead-lettered.

### Steps

```bash
# 1. Start all services (a fresh run, so the dead-letter-queue holds only what you inject)
bun run start

# 2. Inject poison on partitions 0, 1 and 2 of two topics:
#    conversation-events:      "{not json" (no key), and a ToolInvocationResulted missing stepIndex and result
#    tool-invocation-requests: a ToolInvocationRequested for getProductInformation missing stepIndex
bun -e '
import { Kafka } from "kafkajs";
const producer = new Kafka({ clientId: "poison-injector", brokers: ["localhost:9092"] }).producer();
await producer.connect();
const uuid = () => crypto.randomUUID();
for (const partition of [0, 1, 2]) {
  const cid = uuid();
  await producer.send({ topic: "conversation-events", messages: [
    { partition, value: "{not json" },
    { partition, key: cid, value: JSON.stringify({ conversationId: cid, timestamp: 1, eventType: "ToolInvocationResulted", payload: { toolName: "math" } }) },
  ]});
  const rid = uuid();
  await producer.send({ topic: "tool-invocation-requests", messages: [
    { partition, key: rid, value: JSON.stringify({ conversationId: rid, timestamp: 1, eventType: "ToolInvocationRequested", payload: { toolName: "getProductInformation", input: {} } }) },
  ]});
}
await producer.disconnect();
console.log("poison injected on partitions 0, 1, 2");
'

# 3. Read the dead-letter-queue and validate every record against MessageDeadLetteredSchema
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic dead-letter-queue --from-beginning --timeout-ms 10000 2>/dev/null \
  | bun -e '
import { MessageDeadLetteredSchema } from "./shared/schemas/MessageDeadLettered";
const lines = (await Bun.stdin.text()).split("\n").filter(Boolean);
const tally = new Map<string, number>();
let invalid = 0;
for (const line of lines) {
  const r = MessageDeadLetteredSchema.safeParse(JSON.parse(line));
  if (!r.success) { invalid++; continue; }
  const { error, consumer, source } = r.data.payload;
  const k = `${error.class} / ${consumer.group} (${source.topic})`;
  tally.set(k, (tally.get(k) ?? 0) + 1);
}
console.log(`records=${lines.length} invalid=${invalid}`);
for (const [k, n] of [...tally].sort()) console.log(`${n} x ${k}`);
'

# 4. Confirm the partitions kept flowing: LAG must be 0 on every partition of every group
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

### Expected outcome

All 30 records parse (`invalid=0`). One bad message produces one record per consumer group that handles its type:

```
3 x contract-violation / chat-tool-worker (tool-invocation-requests)
3 x contract-violation / exchange-tool-worker (tool-invocation-requests)
3 x contract-violation / math-tool-worker (tool-invocation-requests)
3 x contract-violation / orchestrator-service (conversation-events)
3 x contract-violation / rag-tool-worker (tool-invocation-requests)
3 x contract-violation / ui-web-final-answer (conversation-events)
3 x contract-violation / weather-tool-worker (tool-invocation-requests)
3 x malformed-json / aggregator-service (conversation-events)
3 x malformed-json / orchestrator-service (conversation-events)
3 x malformed-json / ui-web-final-answer (conversation-events)
```

The aggregator skips the invalid `ToolInvocationResulted` (it handles `PlanCompleted` only), so only the malformed JSON, which no group can classify, reaches it. A record looks like this (shortened):

```json
{"eventType":"MessageDeadLettered","schemaVersion":1,"conversationId":"<uuid>","timestamp":1793527699468,
 "payload":{"source":{"topic":"tool-invocation-requests","partition":0,"offset":"3","key":"<uuid>","timestamp":"1793527699464"},
 "consumer":{"group":"rag-tool-worker","service":"rag"},
 "error":{"class":"contract-violation","name":"ContractViolation","reason":"invalid ToolInvocationRequested","attempts":1,
          "issues":[{"path":"payload.stepIndex","message":"Invalid input: required"}]},
 "originalValue":{"encoding":"utf8","data":"{...the original message...}","truncated":false}}}
```

Each service logs one line per record, for example `[kafka] dead-lettered topic=tool-invocation-requests partition=0 offset=3 group=math-tool-worker class=contract-violation reason="invalid ToolInvocationRequested"`. `kafka-consumer-groups.sh` shows `LAG 0` everywhere, no service logs a crash or restart, and the next normal query still completes.

**Not covered:** the dead letter is not acted on. A dead-lettered `ToolInvocationRequested` or `ToolInvocationResulted` leaves its plan `running` until a step timeout exists (phase 4), and there is no replay tool.

---

## Scenario 5 — Tool Error

**What breaks:** A tool returns an error for one step of a plan.

**What the code does:** The worker answers with a `ToolInvocationResulted` carrying an `error`. The orchestrator marks the plan `failed` and emits `PlanFailed`. A failed plan is a domain event: **nothing is written to `dead-letter-queue`**, which holds poison messages only.

A typed `what is 2/0` does not reproduce this: the LLM router sends that query, and every division-by-zero phrasing tried, to `chat`, so the math error path is never reached. The scenario injects the plan instead, publishing `PlanGenerated [math "2/0"]` straight to `conversation-events` (the `<cid>|` prefix is the Kafka key):

### Steps

```bash
CID=$(uuidgen | tr A-Z a-z)
printf '%s|{"conversationId":"%s","sessionId":"smoke","timestamp":%s000,"eventType":"PlanGenerated","payload":{"steps":[{"tool":"math","args":{"expression":"2/0"}}]}}\n' "$CID" "$CID" "$(date +%s)" \
  | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic conversation-events --property parse.key=true --property key.separator='|'
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic conversation-events --from-beginning --property print.key=true --timeout-ms 10000 | grep "$CID"
```

### Expected outcome

Three events for that `CID`, in order: `PlanGenerated`, `ToolInvocationResulted` (math, `"error":"Division by zero"`), then `PlanFailed` (`"reason":"Division by zero","failedTool":"math"`). `math.log` shows `error="Division by zero"` and `orchestrator.log` shows `plan FAILED reason=Division by zero`. The `dead-letter-queue` gains no record.

(`kafka-console-consumer.sh` ends with a `TimeoutException` after `--timeout-ms`; that is how it exits when it has read everything.)

No socket is pending for an injected conversation, so the web server sends **no** UI frame. This scenario checks the events only. With a real UI query that reaches a failing tool, the web client receives `{type:"error"}`.

---

## Summary

| Test | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka redelivers from the last committed offset on restart | Automatic |
| Orchestrator crash | Kafka redelivery + LevelDB state when the next result arrives (no startup scan) | Automatic for redelivered events |
| Duplicate events | `stepIndex` + `toolName` check in the orchestrator; emit, then persist, then offset commit | Automatic for plan state (duplicate downstream events remain) |
| Poison message | Consumer wrapper validates, writes `MessageDeadLettered`, commits the offset | Automatic for the partition; the dead letter needs manual inspection |
| Tool error | Orchestrator emits `PlanFailed`; no dead letter | The plan fails; the error reaches the UI |
