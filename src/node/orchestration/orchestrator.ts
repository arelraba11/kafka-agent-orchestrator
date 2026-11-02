import {
  createProducer,
  createConsumer,
  publish,
  validateOutbound,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { nonRetryable } from "../../../shared/kafka/errors";
import { TOPICS, type TopicName } from "../../../shared/topics";
import { initializeStore, getPlan, savePlan, listRunningPlans } from "../../../shared/state/planStore";
import { transition, type OrchestratorInput } from "./planTransition";
import { createStepTimers, rescheduleRunning, type StepTimers } from "./stepTimers";

/** One global bound on how long a dispatched tool step may go unanswered. */
const STEP_TIMEOUT_MS = (() => {
  const n = Number(process.env.STEP_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : 30_000;
})();

// INVARIANT: emit-before-persist is correct only because every event for a
// conversation is keyed by conversationId (→ one partition of conversation-events)
// and kafkajs handles a partition serially (partitionsConsumedConcurrently = 1).
// A redelivered message is therefore always re-handled before any later event for
// the same conversation, against the same persisted state. Do not raise
// concurrency per partition, or key these events by anything else, without
// revisiting this.
async function handle(
  producer: Awaited<ReturnType<typeof createProducer>>,
  timers: StepTimers,
  event: OrchestratorInput
): Promise<void> {
  const { conversationId } = event;

  const state = await getPlan(conversationId); // throws on I/O error → redelivery
  // transition() is pure: anything it throws is a bug in this message's handling,
  // not an infrastructure failure, so it is dead-lettered rather than retried.
  const { state: next, emit, decision } = nonRetryable(() =>
    transition(state, event, Date.now(), { stepTimeoutMs: STEP_TIMEOUT_MS })
  );

  if (decision.kind === "ignored") {
    const log = decision.reason === "future-step" ? console.error : console.log;
    log(
      `[orchestrator] conversationId=${conversationId} ignored reason=${decision.reason} ${decision.detail}`
    );
    return; // no write, no emit
  }

  // 1. validate every outbound message BEFORE the first send, so a contract bug can
  //    never leave a half-emitted set (throws OutboundContractViolationError → DLQ)
  for (const m of emit) {
    validateOutbound<TopicName>(m.topic, m.message);
  }

  // 2. emit (acks=-1; a failed send throws ProducerError → kafkajs redelivers,
  //    nothing persisted yet)
  for (const m of emit) {
    await publish<TopicName>(producer, m.topic, m.message);
  }

  // 3. persist (sync; throws StoreError)
  await savePlan(conversationId, next!);

  // 3b. arm or disarm the step timer. Timers only publish StepTimedOut; the decision
  //     happens in transition(), in this partition's order.
  if (next!.status === "running") {
    timers.schedule(conversationId, next!.stepIndex, next!.stepDeadline!);
  } else {
    timers.cancel(conversationId);
  }

  // 4. return → kafkajs resolves the offset; it is committed at the end of the batch
  switch (decision.kind) {
    case "started":
      console.log(`[orchestrator] conversationId=${conversationId} plan received steps=[${next!.plan.map(s => s.tool).join(", ")}]`);
      console.log(`[orchestrator] conversationId=${conversationId} dispatching tool="${next!.plan[0].tool}"`);
      break;
    case "advanced": {
      const done = decision.completedStep + 1;
      console.log(`[orchestrator] conversationId=${conversationId} step ${done}/${next!.plan.length} completed tool="${next!.plan[decision.completedStep].tool}"`);
      console.log(`[orchestrator] conversationId=${conversationId} dispatching tool="${next!.plan[next!.stepIndex].tool}"`);
      break;
    }
    case "completed": {
      const workerLatency = Date.now() - next!.planReceivedAt;
      console.log(`[orchestrator] conversationId=${conversationId} plan completed, results=${next!.results.length}`);
      console.log(`[Benchmark] conversationId=${conversationId} workerLatency=${workerLatency}ms`);
      break;
    }
    case "failed":
      console.log(
        `[orchestrator] conversationId=${conversationId} PlanFailed code=${decision.code} failedTool=${decision.failedTool} reason=${decision.reason}`
      );
      break;
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

await initializeStore();
console.log("[orchestrator] PlanStore initialized");

const producer = await createProducer();

const timers = createStepTimers({
  setTimer: (fn, ms) => setTimeout(fn, ms),
  clearTimer: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
  now: () => Date.now(),
  publish: (event) => publish(producer, TOPICS.CONVERSATION_EVENTS, event),
});

// Plans that were running when the previous process stopped: re-arm their deadlines
// (overdue ones fire now, and a result already in the log still wins by offset order).
const rescheduled = rescheduleRunning(timers, await listRunningPlans(), Date.now(), STEP_TIMEOUT_MS);
console.log(`[orchestrator] rescheduled ${rescheduled} step timer(s) stepTimeoutMs=${STEP_TIMEOUT_MS}`);

const consumer = await createConsumer("orchestrator-service");

registerShutdown([producer, consumer]);

await subscribeAndRun({
  consumer,
  groupId: "orchestrator-service",
  service: "orchestrator",
  producer,
  topic: TOPICS.CONVERSATION_EVENTS,
  types: ["PlanGenerated", "ToolInvocationResulted", "StepTimedOut"],
  handler: (event) => handle(producer, timers, event),
});

console.log("[orchestrator] OrchestratorService started.");
