import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { FinalAnswerSynthesized } from "../../../shared/schemas/FinalAnswerSynthesized";
import type { PlanFailed } from "../../../shared/schemas/PlanFailed";

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("ui-final-answer");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as { eventType: string };

    if (event.eventType === "FinalAnswerSynthesized") {
      const e = value as FinalAnswerSynthesized;
      console.log(`\nBot: ${e.payload.answer}\n> `);
    } else if (event.eventType === "PlanFailed") {
      const e = value as PlanFailed;
      console.log(`\nPlan failed: ${e.payload.reason} (tool: ${e.payload.failedTool})\n> `);
    }
  }
);

// ─── Read user input from console ────────────────────────────────────────────

console.log("AI Agent ready. Type your question:\n>");

const decoder = new TextDecoder();

for await (const chunk of Bun.stdin.stream()) {
  const input = decoder.decode(chunk).trim();
  if (!input) continue;

  const conversationId = crypto.randomUUID();

  const event: UserQueryReceived = {
    conversationId,
    timestamp: Date.now(),
    eventType: "UserQueryReceived",
    payload: { userInput: input },
  };

  await sendMessage(producer, TOPICS.USER_COMMANDS, conversationId, event);
  console.log(`[ui] conversationId=${conversationId} sent`);
}
