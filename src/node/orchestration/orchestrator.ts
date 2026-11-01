import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { initializeStore, getPlan, savePlan } from "../../../shared/state/planStore";
import { transition, type OrchestratorInput } from "./planTransition";

// INVARIANT: emit-before-persist is correct only because every event for a
// conversation is keyed by conversationId (→ one partition of conversation-events)
// and kafkajs handles a partition serially (partitionsConsumedConcurrently = 1).
// A redelivered message is therefore always re-handled before any later event for
// the same conversation, against the same persisted state. Do not raise
// concurrency per partition, or key these events by anything else, without
// revisiting this.
async function handle(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: OrchestratorInput
): Promise<void> {
  const { conversationId } = event;

  const state = await getPlan(conversationId); // throws on I/O error → redelivery
  const { state: next, emit, decision } = transition(state, event, Date.now());

  if (decision.kind === "ignored") {
    const log = decision.reason === "future-step" ? console.error : console.log;
    log(
      `[orchestrator] conversationId=${conversationId} ignored reason=${decision.reason} ${decision.detail}`
    );
    return; // no write, no emit
  }

  // 1. emit (acks=-1; a failed send throws → kafkajs redelivers, nothing persisted yet)
  for (const m of emit) {
    await sendMessage(producer, m.topic, m.key, m.value);
  }

  // 2. persist (sync, throws)
  await savePlan(conversationId, next!);

  // 3. return → kafkajs resolves the offset; it is committed at the end of the batch
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
      console.log(`[orchestrator] conversationId=${conversationId} plan FAILED reason=${decision.reason}`);
      break;
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

await initializeStore();
console.log("[orchestrator] PlanStore initialized");

const producer = await createProducer();
const consumer = await createConsumer("orchestrator-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as { eventType: string };

    switch (event.eventType) {
      case "PlanGenerated":
      case "ToolInvocationResulted":
        await handle(producer, event as OrchestratorInput);
        break;
      default:
        break;
    }
  }
);

console.log("[orchestrator] OrchestratorService started.");
