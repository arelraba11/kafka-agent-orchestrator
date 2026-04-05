Resilience Tests — Kafka AI Agent (Final Project)

This document describes three practical resilience demonstrations for the
event-driven pipeline. All commands assume the repository root as the working
directory and that Kafka is running via infra/docker-compose.yml.

Log files are written to scripts/logs/final-project-services/.

-------------------------------------------------------------------------------
Test A — Worker Crash Recovery
-------------------------------------------------------------------------------

Goal: show that killing and restarting a tool worker does not break the
pipeline for subsequent requests.

Kafka guarantees: when a worker restarts it rejoins its consumer group and
resumes from the last committed offset, so no message is lost.

Step 1 — Start all services

    bash scripts/start-final.sh
    open http://localhost:3001   # or bun run web:dev for hot-reload

Step 2 — Send a query that triggers the weather worker

    Type in the UI:
        weather in tel aviv

    Expected log sequence (scripts/logs/final-project-services/):

        [router]        conversationId=<id> plan=[weather]
        [Benchmark]     conversationId=<id> routerLatency=Xms
        [orchestrator]  conversationId=<id> dispatching tool="weather"
        [weather]       conversationId=<id> tool=weather result=...
        [orchestrator]  conversationId=<id> step 1/1 completed tool="weather"
        [Benchmark]     conversationId=<id> workerLatency=Xms
        [synthesizer]   conversationId=<id> answer="Weather: ..."
        [Benchmark]     conversationId=<id> synthesizerLatency=Xms

Step 3 — Kill the weather worker

    pkill -f "weatherApp.ts"

Step 4 — Send another query while the worker is down

    Type in the UI:
        weather in haifa

    The orchestrator dispatches the tool to the topic but the worker is gone.
    The plan will not complete until the worker is restarted. The orchestrator
    holds state in memory and waits.

Step 5 — Restart the weather worker

    bun run services/apps/weatherApp.ts >> scripts/logs/final-project-services/weather.log 2>&1 &

    The restarted worker picks up the pending message from Kafka and processes it.
    The pipeline completes and FinalAnswerSynthesized is delivered to the UI.

Expected outcome: the UI receives a weather answer for the second query after
the worker is back up. No manual intervention is needed beyond the restart.

-------------------------------------------------------------------------------
Test B — Orchestrator Restart
-------------------------------------------------------------------------------

Goal: show that restarting the orchestrator does not prevent new requests from
completing successfully.

Important: the orchestrator holds its plan state in memory only. In-flight
plans at the moment of a crash are not recovered. New plans started after the
restart work normally. This is by design for this course project.

Step 1 — Start all services

    bash scripts/start-final.sh
    open http://localhost:3001   # or bun run web:dev for hot-reload

Step 2 — Send a few queries and verify they complete

    Type in the UI:
        weather in tel aviv
        convert usd to ils

    Confirm answers appear in the UI.

Step 3 — Stop the orchestrator

    pkill -f "orchestrator.ts"

Step 4 — Restart the orchestrator

    bun run services/orchestration/orchestrator.ts >> scripts/logs/final-project-services/orchestrator.log 2>&1 &

Step 5 — Send a new query

    Type in the UI:
        weather in jerusalem

    Expected behaviour: the restarted orchestrator picks up the PlanGenerated
    event from Kafka, dispatches the tool, collects the result, and emits
    PlanCompleted. The UI receives FinalAnswerSynthesized as normal.

Expected outcome: new requests complete end-to-end after the restart. There is
no error in the UI and no manual flush of Kafka topics is needed.

-------------------------------------------------------------------------------
Test C — Duplicate Event Handling
-------------------------------------------------------------------------------

Goal: show that the system tolerates duplicate or stale events without
producing incorrect output.

Two built-in guards protect the pipeline:


Guard 1 — Tool workers filter by toolName
------------------------------------------

Every tool worker checks the toolName field before processing a
ToolInvocationRequested event. If the name does not match, the message is
silently skipped.

Source (services/apps/weatherApp.ts):
    if (req.payload.toolName !== "weather") return;

Source (services/apps/mathApp.ts):
    if (req.payload.toolName !== "math") return;

This means a broadcast or a duplicate ToolInvocationRequested event for a
different tool is ignored at the consumer level. Only the correct worker
handles each request.


Guard 2 — Orchestrator drops stale conversationId results
----------------------------------------------------------

When a ToolInvocationResulted event arrives, the orchestrator looks up the
conversationId in its state store. If the id is unknown (e.g. a duplicate
result after the plan was already completed and the state was deleted) the
event is discarded with a warning.

Source (services/orchestration/orchestrator.ts):
    if (!state) {
      console.warn(`[orchestrator] Received result for unknown conversationId=${conversationId}, skipping.`);
      return;
    }


Example log flow for a duplicate result
-----------------------------------------

Assume a ToolInvocationResulted is replayed after the plan already completed:

    [orchestrator]  conversationId=abc-123 plan completed, results=1
    [Benchmark]     conversationId=abc-123 workerLatency=42ms
    [orchestrator]  Received result for unknown conversationId=abc-123, skipping.

The second event is ignored. The UI already received the FinalAnswerSynthesized
from the first completion. No double-answer is produced.


Reproducing a duplicate result manually
-----------------------------------------

You can simulate a duplicate by re-publishing a ToolInvocationResulted event
using the Kafka CLI after a plan has already completed:

    # List the last message on conversation-events
    docker exec -it kafka kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic conversation-events \
      --from-beginning \
      --max-messages 20

    # Use kafka-console-producer to re-publish one of the ToolInvocationResulted
    # messages (copy the JSON from the consumer output above)
    docker exec -it kafka kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic conversation-events

    Paste the ToolInvocationResulted JSON and press Enter.

Expected log output in orchestrator.log:
    [orchestrator] Received result for unknown conversationId=<id>, skipping.

The UI is unaffected. No duplicate answer is produced.

-------------------------------------------------------------------------------
Summary
-------------------------------------------------------------------------------

Test                    Mechanism                           Recovery
------------------      --------------------------------    -------------------
Worker crash            Kafka offset replay on restart      Automatic
Orchestrator restart    Stateless for new plans             Automatic
Duplicate events        toolName filter + id guard          Silent drop
