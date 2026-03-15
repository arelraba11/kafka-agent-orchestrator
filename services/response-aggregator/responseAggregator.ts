import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { AppResultEvent, BotResponseEvent } from "../../shared/types/events";

// ─── Message formatting ───────────────────────────────────────────────────────

function formatMessage(event: AppResultEvent): string {
  if (!event.success) {
    return `Sorry, I couldn't process your request. ${event.error ?? "Unknown error."}`;
  }
  return event.result;
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("response-aggregator");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.APP_RESULTS],
  async (_topic, _key, value) => {
    const event = value as AppResultEvent;
    const { userId, type } = event;

    console.log(`[aggregator] userId=${userId} type=${type} success=${event.success}`);

    const payload: BotResponseEvent = {
      userId,
      message: formatMessage(event),
      sourceType: type,
      timestamp: new Date().toISOString(),
    };

    await sendMessage(producer, TOPICS.BOT_RESPONSES, userId, payload);

    console.log(`[aggregator] Published bot response for userId=${userId}`);
  }
);

console.log("[aggregator] ResponseAggregator started.");
