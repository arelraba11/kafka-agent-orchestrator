import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { PlanCompleted } from "../../../shared/schemas/PlanCompleted";

// ─── Command type ─────────────────────────────────────────────────────────────

interface SynthesizeFinalAnswerRequested {
  conversationId: string;
  timestamp: number;
  commandType: "SynthesizeFinalAnswerRequested";
  payload: {
    results: Record<string, unknown>[];
  };
}

// ─── Handler ──────────────────────────────────────────────────────────────────

async function onPlanCompleted(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: PlanCompleted
): Promise<void> {
  const { conversationId, payload } = event;
  const { results } = payload;

  console.log(
    `[aggregator] PlanCompleted received | conversationId=${conversationId} | steps=${results.length}`
  );

  const command: SynthesizeFinalAnswerRequested = {
    conversationId,
    timestamp: Date.now(),
    commandType: "SynthesizeFinalAnswerRequested",
    payload: { results },
  };

  await sendMessage(producer, TOPICS.USER_COMMANDS, conversationId, command);

  console.log(
    `[aggregator] SynthesizeFinalAnswerRequested emitted | conversationId=${conversationId}`
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("aggregator-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as { eventType: string };

    if (event.eventType === "PlanCompleted") {
      await onPlanCompleted(producer, event as PlanCompleted);
    }
  }
);

console.log("[aggregator] AggregatorService started.");
