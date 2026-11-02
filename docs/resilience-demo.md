# Resilience Demo — Kafka AI Agent

Fourteen failure scenarios with the behavior the code has today and how to reproduce each one.

All commands assume the repository root as the working directory, Kafka running via `infra/docker-compose.yml`, and the services started with `bun run start`. Logs are written to `scripts/logs/services/` (append mode; restarts are recorded in `supervisor.log`), and each process's pid is in `.run/<service>.pid`: `web`, `router`, `orchestrator`, `aggregator`, `synthesizer`, `math`, `weather`, `exchange`, `chat`, `rag`. Signal the pid from those files, not a `pkill` pattern.

Kafka CLI tools are not on the container's PATH; call them as `/opt/kafka/bin/<tool>.sh`.

Scenarios with an **Evidence** line are also proven by an automated test against a real broker (`bun run test:int`, see the README's Testing section). The manual steps below remain as demos.

---

## Scenario 1 — Worker Crash

**What breaks:** A tool worker process is killed while a plan is in flight.

**Why it recovers:** The supervisor restarts the worker (about 1 s later, see Scenario 11). Kafka keeps the messages after the worker's last committed offset, so the restarted worker rejoins its consumer group and reads the `ToolInvocationRequested` events it had not yet processed.

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
kill -9 $(cat .run/weather.pid)

# 4. Send another query right away. Type in the UI:
weather in haifa
#    supervisor.log:  weather exited (exit code 137); restarting in 1000ms
#                     started weather pid=<new pid>
#    The orchestrator dispatches ToolInvocationRequested to Kafka. If the worker is
#    not back yet, the message waits in the topic; the restarted worker reads it from
#    its last committed offset and emits ToolInvocationResulted. The orchestrator
#    completes the plan and the UI receives the answer.
```

### Expected outcome

The UI receives the weather answer for the second query, with no manual Kafka intervention and no manual restart. If the worker could not come back within the step timeout, the UI would have shown "The weather tool didn't respond within 30 s.", and the late result would be logged `ignored reason=plan-terminal` (to hold a worker down on purpose, pause it with `kill -STOP`: Scenario 6).

---

## Scenario 2 — Orchestrator Crash

**What breaks:** The orchestrator process is killed and restarted (by the supervisor).

**Why it recovers:** Two mechanisms work together. Kafka redelivers events whose offsets the orchestrator had not committed. Plan state lives in LevelDB (`shared/state/planStore.ts`, `.plan-store/`), so when the next `ToolInvocationResulted` for a stored plan arrives, the restarted orchestrator finds the state and continues from the saved `stepIndex`.

The orchestrator does not resume incomplete plans when it starts; its one startup scan re-arms the step timers of plans that were `running` (see Scenario 6). Because the next request is emitted before the new state is persisted and before the offset is committed, a crash mid-handler replays the triggering event instead of skipping a step.

### Steps

```bash
# 1. Start all services
bun run start
open http://localhost:3001

# 2. Verify end-to-end works. Type in the UI:
weather in jerusalem
#    Confirm the answer arrives.

# 3. Kill the orchestrator
kill -9 $(cat .run/orchestrator.pid)

# 4. The supervisor restarts it after about 1 s (supervisor.log: "orchestrator exited ... restarting in 1000ms")
#    Expected startup lines in orchestrator.log:
#      [orchestrator] PlanStore initialized
#      [orchestrator] rescheduled 0 step timer(s) stepTimeoutMs=30000
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


**Evidence:** `test/integration/crash-orchestrator.test.ts` › C-b1, C-b2, C-b3 (killed after emitting, before persisting) and C-c1, C-c2, C-c3 (killed after persisting, before the offset commit). Each kills the orchestrator with `SIGKILL` at a named fault point and restarts it.

---

## Scenario 3 — Duplicate Events

**What breaks:** A `ToolInvocationRequested` or `ToolInvocationResulted` event is delivered more than once (consumer offset not committed, Kafka retry, or a manual re-publish via the CLI).

**What the code does:** The orchestrator is idempotent. `ToolInvocationRequested` and `ToolInvocationResulted` carry `payload.stepIndex`, and workers echo it back. The orchestrator emits its outbound events first, persists the new plan state second (`sync: true`), and Kafka commits the offset after the handler returns, so a crash at any point replays the message safely.

### What is protected

- **Workers filter by `toolName`.** Each worker ignores requests for other tools (`if (req.payload.toolName !== "weather") return;` in `src/node/apps/weatherApp.ts`, and the same in the other workers). That is routing, not deduplication.
- **A duplicate, stale or out-of-order result never changes the plan.** The orchestrator applies a result only when the plan is `running`, `stepIndex` is the step it awaits, and `toolName` matches that step. Otherwise it writes nothing, emits nothing and logs `ignored reason=<reason>`. The reasons are `unknown-plan`, `plan-terminal` (the plan already completed or failed), `missing-step-index`, `duplicate-step`, `future-step` and `tool-mismatch`. A stale `StepTimedOut` is `stale-timeout`.
- **A replayed `PlanGenerated`** for a conversation that already has a plan is ignored (`duplicate-plan`).
- **Terminal plans are kept as tombstones**, so a result that arrives after the plan finished is recognised (`plan-terminal`) instead of looking like an unknown conversation.
- **The RAG worker no longer loses a result.** It commits an offset only after the result send is acknowledged, so a failed send leaves the request uncommitted for redelivery.

### What is not protected

- **Workers do not deduplicate**, so a duplicate `ToolInvocationRequested` runs the tool again and emits another result. The orchestrator ignores the extra one (`duplicate-step`).
- **A duplicate `PlanCompleted` is absorbed downstream.** If the orchestrator crashes after publishing `PlanCompleted` and before persisting `completed`, the last result is replayed and `PlanCompleted` is published again. The aggregator emits a second `SynthesizeFinalAnswerRequested`, but the synthesizer recognises the conversation as done and neither calls the LLM nor writes history again (Scenario 8).
- **Plan tombstones and `.synth-store/` `done:` markers are never pruned** and grow without bound.

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


**Evidence:** `test/integration/idempotency.test.ts` › D1 (a duplicate result while the plan is live does not re-dispatch the next step).

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

**Not covered:** a dead letter is not replayed (there is no replay tool). A dead-lettered `ToolInvocationRequested` or `ToolInvocationResulted` is not reported by a hook; its plan fails through the step timeout (Scenario 6). Poison messages that a service owns are also reported to the user (Scenario 9).


**Evidence:** `test/integration/poison.test.ts` › P2 (malformed, contract-violating and key-mismatched records on every partition of `conversation-events` are dead-lettered once per group that reads the type) and P-flow (the partitions keep flowing afterwards). `test/integration/retryable.test.ts` › X1 (a producer failure is retried, never dead-lettered).

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

Three events for that `CID`, in order: `PlanGenerated`, `ToolInvocationResulted` (math, `"error":"Division by zero"`), then `PlanFailed` (`"code":"tool-error","reason":"Division by zero","failedTool":"math"`). `math.log` shows `error="Division by zero"` and `orchestrator.log` shows `PlanFailed code=tool-error failedTool=math reason=Division by zero`. The `dead-letter-queue` gains no record, and every consumer group has lag 0.

(`kafka-console-consumer.sh` ends with a `TimeoutException` after `--timeout-ms`; that is how it exits when it has read everything.)

No socket is pending for an injected conversation, so the web server sends **no** UI frame. This scenario checks the events only. With a real UI query that reaches a failing tool, the web client receives `{type:"error", reason:"The math tool failed: Division by zero", failedTool:"math", code:"tool-error", conversationId}`; `toClientFrame`'s mapping is covered by `src/node/core/clientFrames.test.ts`.

---

## Scenario 6 — Unanswered Tool Step (Step Timeout)

**What breaks:** A tool worker is down, paused, or its request or result was dead-lettered, so a dispatched step never gets an answer.

**What the code does:** Every dispatched step has a persisted deadline (`STEP_TIMEOUT_MS`, default 30 s). A timer in the orchestrator publishes `StepTimedOut`; the event is handled in the conversation's partition like any other, so whichever of the result and the timeout comes first in the log wins. The plan becomes `failed` with `PlanFailed{code:"step-timeout"}`, and the user sees "The `<tool>` tool didn't respond within `<n>` s."

### Steps

```bash
# 1. Stop the repo's services, set a short timeout in .env, and start again
bun run stop
echo "STEP_TIMEOUT_MS=10000" >> .env
bun run start      # then wait until orchestrator.log shows "OrchestratorService started."

# 2. Pause the math worker
MATH=$(cat .run/math.pid)
kill -STOP $MATH

# 3. Ask in the UI (or over the WebSocket):
what is 12*9
#    After ~10 s the UI shows "The math tool didn't respond within 10 s."
#    orchestrator.log:  PlanFailed code=step-timeout failedTool=math reason=step 0 (math) did not respond within 10s

# 4. Resume the worker: its late result is ignored
kill -CONT $MATH
#    orchestrator.log:  ignored reason=plan-terminal plan status=failed
```

### Restart persistence

```bash
# Pause math again and ask again. As soon as orchestrator.log shows dispatching tool="math":
kill -STOP $MATH
kill -9 $(cat .run/orchestrator.pid)     # the supervisor restarts it after about 1 s
```

The restarted orchestrator logs `rescheduled 1 step timer(s)` and publishes `StepTimedOut` at the original deadline (an overdue deadline fires immediately). The `PlanFailed` follows as soon as its consumer is assigned its partitions again: a `kill -9`'d consumer holds its group membership until the broker's session timeout (about 30 s with kafkajs defaults), so the UI may see the failure after that, not exactly at the deadline. If the worker's result is already in the log it has a lower offset than the new `StepTimedOut`, so the real result wins.

Timers live in the orchestrator: no step times out while it is down.


**Evidence:** `test/integration/timeouts.test.ts` › T1 (step timeout, `PlanFailed{code:"step-timeout"}`) and T3 (a restarted orchestrator re-arms the timer with the original deadline).

---

## Scenario 7 — LLM Misconfigured, Slow or Empty

**What breaks:** The language model cannot answer: a bad or missing key, a model server that hangs, or one that returns blank text.

**What the code does:** `callLLM` bounds every attempt and the whole call. The router falls back to its regex planner on any LLM failure. The synthesizer turns it into a `ConversationFailed` that the web server maps to the existing error frame. No DLQ record is written and no partition is blocked.

### Steps

```bash
# A. Invalid key. In .env: LLM_PROVIDER=openai and OPENAI_API_KEY=sk-invalid, then restart and ask: hi
#    router.log  [router] mode=regex-fallback reason="OpenAI HTTP 401 …"
#    answer.log  one "[llm] provider=openai attempt=1/3 outcome=auth …", no retry
#    UI          "The assistant's language model is misconfigured. Check the server logs."

# B. Missing key. Remove OPENAI_API_KEY with LLM_PROVIDER=openai: every service still starts, and the
#    query shows the same message; the logs say "OPENAI_API_KEY is not set (LLM_PROVIDER=openai)".
#    With LLM_PROVIDER=ollama and Ollama running, no key is needed at all.

# C. Hanging model. Run a server that never answers, and point Ollama at it:
bun -e 'Bun.serve({port:11999,fetch:()=>new Promise(()=>{})})' &
#    .env: LLM_PROVIDER=ollama  OLLAMA_HOST=http://localhost:11999  LLM_ATTEMPT_TIMEOUT_MS=3000   (restart, ask: hi)
#    router.log  one timed-out attempt (outcome=timeout ms=3000), then mode=regex-fallback
#    answer.log  attempt=1/3, 2/3, 3/3 all outcome=timeout
#    UI          after ~10 s of synthesis: "The language model is unavailable right now. Try again."
#    A second query right afterwards is handled: nothing is stuck.

# D. Blank answers. Point OLLAMA_HOST at a server that replies {"message":{"content":"  "}}:
#    answer.log shows three outcome=empty-response attempts, then the UI shows
#    "The language model returned an empty answer. Try again."
```

### Expected outcome

`ConversationFailed{stage:"synthesis"}` with `llm-misconfigured`, `llm-unavailable` or `empty-answer` respectively. The `dead-letter-queue` gains no record, and every consumer group, including `answer-synthesizer`, has lag 0.


**Evidence:** `test/integration/llm-failures.test.ts` › L2 (5xx on every attempt → `llm-unavailable`) and L3 (401 → `llm-misconfigured`, one call). The empty-answer, timeout, `Retry-After` and router-fallback cases are not yet integration-tested (planned: phase 5 Part B).

---

## Scenario 8 — Duplicate `PlanCompleted`

**What breaks:** `PlanCompleted` is published twice (for example after an orchestrator crash between the publish and the state write).

**What the code does:** The synthesizer writes `done:<conversationId>` only after the terminal event was published. A second synthesize command finds it and does nothing.

### Steps

```bash
# 1. Ask a normal question in the UI, e.g. "what is 7*8", then read its PlanCompleted (key and value, tab-separated)
CID=<the conversationId from aggregator.log>
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic conversation-events --from-beginning --property print.key=true --timeout-ms 5000 \
  | grep "$CID" | grep PlanCompleted > plancompleted.txt

# 2. Republish it (the key and the JSON are separated by a tab)
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic conversation-events --property parse.key=true --property key.separator=$'\t' < plancompleted.txt
```

### Expected outcome

`aggregator.log` shows a second `SynthesizeFinalAnswerRequested emitted`. `answer.log` shows `[synthesizer] conversationId=<cid> ignored duplicate` and no new `[llm]` line, and `jq` finds exactly one user/assistant pair with that `conversationId` in `history.json`:

```bash
jq --arg c "$CID" '[.[] | .[] | select(.conversationId==$c)] | length' history.json   # 2
```


**Evidence:** `test/integration/idempotency.test.ts` › D4 (one LLM call, one history entry) and `test/integration/crash-synthesizer.test.ts` › S3 (synthesizer killed after publishing, before history and `done`).

---

## Scenario 9 — Poison Message That Reaches the User

**What breaks:** A message that belongs to a conversation can never be processed.

**What the code does:** After the DLQ record is published, the service that owns that message type publishes `ConversationFailed{code:"internal-error"}` so the user is told (the `onDeadLettered` hook; see `docs/architecture.md`). Exactly one service owns each type, so one poison message produces one such event.

### Steps

```bash
U=$(uuidgen | tr A-Z a-z)
# empty userInput violates the UserQueryReceived contract
printf '%s|{"conversationId":"%s","timestamp":1,"eventType":"UserQueryReceived","payload":{"userInput":"","sessionId":"s"}}\n' "$U" "$U" \
  | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic user-commands --property parse.key=true --property key.separator='|'

# DLQ records for $U
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dead-letter-queue --from-beginning --timeout-ms 8000 2>/dev/null | grep "$U" \
  | jq -c '{class:.payload.error.class, group:.payload.consumer.group}'
# ConversationFailed events for $U
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic conversation-events --from-beginning --property print.key=true --timeout-ms 8000 2>/dev/null | grep "$U" | cut -f2 \
  | jq -c '{eventType, stage:.payload.stage, code:.payload.code}'
```

### Expected outcome

Two `contract-violation` DLQ records (`router-plan-service` and `answer-synthesizer`), and **exactly one** `ConversationFailed{stage:"routing", code:"internal-error"}`, none with `stage:"synthesis"`: the synthesizer's `UserQueryReceived` consumption is a cache fill, so the router alone owns that failure. `router.log` shows `ConversationFailed stage=routing code=internal-error (dead-lettered UserQueryReceived)`. In the UI the user would see "Something went wrong while planning your request (ref `<first 8 characters of the id>`)."

The same pattern with a contract-violating `PlanGenerated` (`"steps":[]`) gives `ConversationFailed{stage:"orchestration"}` (the orchestrator reports only when no plan exists), and with a contract-violating `PlanCompleted` it gives `stage:"aggregation"`.

---

## Scenario 10 — Corrupt `history.json`

**What breaks:** `history.json` is truncated or contains invalid data (a crash during an old non-atomic write, a manual edit).

**What the code does:** Writes are atomic, so the app itself does not produce this. If it happens, the synthesizer (the only writer) moves the file to `history.json.corrupt-<timestamp>`, logs an ERROR and starts a fresh file; readers log an ERROR and use no history. Earlier context stays on disk in the quarantined file.

### Steps

```bash
cp history.json history.backup.json     # keep your history
printf '{' > history.json
#    Ask any question in the UI; it is answered.
ls history.json*                         # history.json  history.json.corrupt-<ts>
jq . history.json                        # valid again, holding the new exchange
```

### Expected outcome

`answer.log` shows `[history] ERROR … was corrupt (…); moved to …/history.json.corrupt-<ts>, starting a fresh history`, and `router.log` shows an ERROR for the read it did first. The answer still arrives.

---

## Scenario 11 — Service Crash, Restart and Give-up

**What breaks:** Any service process dies (`kill -9`, an uncaught exception, the RAG worker exiting on a failed send).

**What the code does:** `scripts/supervisor.ts` owns every service process and restarts it according to `scripts/lib/supervisorPolicy.ts`: backoff 1 s, doubling to a 30 s cap, reset after 60 s of uptime. After 5 crashes within 2 min it writes `GAVE UP <service>` to `supervisor.log` and stops restarting that service. `bun run stop` is a requested stop and never triggers a restart.

### Steps

```bash
kill -9 $(cat .run/weather.pid)
sleep 2
cat .run/weather.pid                      # a new pid
tail -3 scripts/logs/services/supervisor.log
#    <ts> [supervisor] weather exited (exit code 137); restarting in 1000ms
#    <ts> [supervisor] started weather pid=<new pid>
#    Ask "weather in tel aviv" in the UI: it is answered.

# Repeat the kill quickly: the delay doubles (2000ms, 4000ms, 8000ms); the 5th crash within 2 min logs
#    <ts> [supervisor] GAVE UP weather (exit code 137; crashed too often, not restarting)

# Running start twice is refused, and the instance is untouched
bun run start                             # ERROR: already running (supervisor pid <n>, .../.run). Run 'bun run stop' first.

# A second copy elsewhere (separate pid files and logs; it still shares Kafka and the stores, so stop the first one first)
bun run stop
RUN_DIR=$TMPDIR/run LOG_DIR=$TMPDIR/logs bun run start
RUN_DIR=$TMPDIR/run bun run stop
```

### Expected outcome

A crashed service is back in about a second and the next request is answered. If the supervisor gave up on a service, requests that need it fail with the step timeout (tools) or the client timeout (Scenario 12). `bun run stop` leaves unrelated processes alone (try `bun run idle` from a directory whose `package.json` has `"idle": "sleep 600"`: it survives) and does not delete `.plan-store/LOCK`.

---

## Scenario 12 — Nothing Answers (Client Idle Timeout)

**What breaks:** The pipeline goes silent past every server-side bound, for example because the orchestrator is down or stopped.

**What the code does:** The UI fails the in-flight request when it has received no frame for it for 90 s (the timer is reset by `ack` and `tool`).

### Steps

```bash
kill -STOP $(cat .run/orchestrator.pid)
#    Ask any question in the UI. After 90 s: "No response after 90 s. The request may still finish; try again." and a Try again button.
kill -CONT $(cat .run/orchestrator.pid)
#    The late answer is ignored by the UI (the request was already failed).
```

### Expected outcome

The error bubble appears 90 s after the last frame (the ack), with a Try again button. Nothing happens in the UI when the orchestrator resumes and the answer arrives. The orchestrator's step timers cannot fire while it is paused; this timeout is what covers that gap.

---

## Scenario 13 — WebSocket Disconnect While Waiting

**What breaks:** The web server dies (or the connection drops) with a request in flight.

**What the code does:** When the socket closes the UI fails the in-flight request, then reconnects after 3 s. The supervisor restarts the web server.

### Steps

```bash
#    Ask a question that takes a few seconds (e.g. "what is the weather in london"), then immediately:
kill -9 $(cat .run/web.pid)
```

### Expected outcome

The pending bubble turns into "Connection lost while waiting for the answer." with a Try again button, the header shows the disconnected state, and the UI reconnects once the supervisor has restarted the web server (about 1 s plus the server's startup time, about 15 s). Try again then sends the question again.

---

## Scenario 14 — Query Cannot Be Published

**What breaks:** Kafka is unreachable when the user sends a question.

**What the code does:** The web server's `publish` fails, so it forgets the request and sends `error{code:"unavailable"}` instead of an ack.

### Steps

```bash
docker pause kafka
#    Ask a question in the UI.
docker unpause kafka
```

### Expected outcome

With a paused broker, each kafkajs produce attempt times out after about 30 s (`webserver.log` shows `Request Produce … timed out`, retryCount 0, 1, 2, …), so the send outlasts the client's 90 s idle timeout and the user sees "No response after 90 s…" first. The `unavailable` frame ("The service is unavailable right now. Try again.", with a Try again button) is what the user sees once the send actually fails (`webserver.log`: `publish failed: …`).

A Kafka failure at `bun run start` is loud too: with the `kafka` container missing or renamed, `bun run start` exits 1 with the topic script's error and starts nothing (`.run/` stays empty).

---

## Summary

| Test | Mechanism | Recovery |
|---|---|---|
| Worker crash | Kafka redelivers from the last committed offset on restart | Automatic |
| Orchestrator crash | Kafka redelivery + LevelDB state when the next result arrives; step timers re-armed on startup | Automatic for redelivered events |
| Duplicate events | `stepIndex` + `toolName` check in the orchestrator; emit, then persist, then offset commit | Automatic for plan state (duplicate downstream events remain) |
| Poison message | Consumer wrapper validates, writes `MessageDeadLettered`, commits the offset | Automatic for the partition; the dead letter needs manual inspection |
| Tool error | Orchestrator emits `PlanFailed{code:"tool-error"}`; no dead letter | The plan fails; the error reaches the UI |
| Unanswered tool step | `StepTimedOut` through the partition → `PlanFailed{code:"step-timeout"}`; deadlines persisted and re-armed | The plan fails after `STEP_TIMEOUT_MS`; the UI shows which tool timed out |
| LLM misconfigured, slow or empty | Per-attempt timeouts, bounded retries in `callLLM`, regex fallback in the router, `ConversationFailed` from the synthesizer | The user sees an error message; no DLQ record |
| Duplicate `PlanCompleted` | Synthesizer `done:` marker, set only after the publish | One answer, one history entry |
| Poison message that belongs to a conversation | DLQ record, then `ConversationFailed{internal-error}` from the owning service | The user is told; the dead letter needs manual inspection |
| Corrupt `history.json` | Atomic writes; the writer quarantines the file | Answers continue; old context kept on disk |
| Service crash | Supervisor restarts with backoff, gives up after 5 crashes in 2 min | Automatic; `GAVE UP` in `supervisor.log` if it cannot |
| Nothing answers | UI idle timeout, 90 s without a frame | The user sees an error bubble with a Try again button |
| WebSocket disconnect | UI fails the in-flight request and reconnects | The user sees "Connection lost…" with a Try again button |
| Query cannot be published | `error{code:"unavailable"}` frame | The user sees "The service is unavailable right now" with a Try again button (after the send fails; with a paused broker the 90 s timeout usually comes first) |
