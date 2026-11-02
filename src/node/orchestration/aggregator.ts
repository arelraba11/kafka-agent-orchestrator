import {
  createProducer,
  createConsumer,
  publish,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { conversationFailedHook } from "../../../shared/kafka/conversationFailed";
import { FAILURE_OWNERSHIP } from "../../../shared/kafka/failureOwnership";
import type { PlanCompleted } from "../../../shared/schemas/PlanCompleted";
import type { SynthesizeFinalAnswerRequested } from "../../../shared/schemas/SynthesizeFinalAnswerRequested";

// ─── Handler ──────────────────────────────────────────────────────────────────

async function onPlanCompleted(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: PlanCompleted
): Promise<void> {
  const { conversationId, sessionId, payload } = event;
  const { results } = payload;

  console.log(
    `[aggregator] PlanCompleted received | conversationId=${conversationId} | steps=${results.length}`
  );

  const command: SynthesizeFinalAnswerRequested = {
    conversationId,
    sessionId,
    timestamp: Date.now(),
    commandType: "SynthesizeFinalAnswerRequested",
    payload: { results },
  };

  await publish(producer, TOPICS.USER_COMMANDS, command);

  console.log(
    `[aggregator] SynthesizeFinalAnswerRequested emitted | conversationId=${conversationId}`
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("aggregator-service");

registerShutdown([producer, consumer]);

await subscribeAndRun({
  consumer,
  groupId: "aggregator-service",
  service: "aggregator",
  producer,
  topic: TOPICS.CONVERSATION_EVENTS,
  types: ["PlanCompleted"],
  onDeadLettered: conversationFailedHook(producer, FAILURE_OWNERSHIP.aggregator),
  handler: (event) => onPlanCompleted(producer, event),
});

console.log("[aggregator] AggregatorService started.");
