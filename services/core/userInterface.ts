import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { UserInputEvent, UserControlEvent, BotResponseEvent } from "../../shared/types/events";
import type { UserQueryReceived } from "../../shared/schemas/UserQueryReceived";
import type { FinalAnswerSynthesized } from "../../shared/schemas/FinalAnswerSynthesized";

// ─── Config ──────────────────────────────────────────────────────────────────

const USER_ID = process.env.USER_ID ?? "user-1";

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("ui-service");
const consumerFinalAnswer = await createConsumer("ui-service-final-answer");

registerShutdown([producer, consumer, consumerFinalAnswer]);

// ─── Consume bot responses (Ex1/2) ───────────────────────────────────────────

subscribeAndRun(
  consumer,
  [TOPICS.BOT_RESPONSES],
  async (_topic, _key, value) => {
    const event = value as BotResponseEvent;
    if (event.userId !== USER_ID) return;
    console.log(`\nBot [${event.sourceType}]: ${event.message}\n> `);
  }
).catch(err => console.error("[ui] Consumer error:", err));

// ─── Consume final answers (Final Project) ───────────────────────────────────

subscribeAndRun(
  consumerFinalAnswer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as FinalAnswerSynthesized;
    if (event.eventType !== "FinalAnswerSynthesized") return;
    console.log(`\nbot: ${event.payload.answer}\n> `);
  }
).catch(err => console.error("[ui] Final answer consumer error:", err));

// ─── Read user input from console ────────────────────────────────────────────

console.log(`Distributed Bot started. User: ${USER_ID}`);
console.log('Type a message or "/reset" to clear history.\n');

const decoder = new TextDecoder();

for await (const chunk of Bun.stdin.stream()) {
  const input = decoder.decode(chunk).trim();
  if (!input) continue;

  if (input === "/reset") {
    const event: UserControlEvent = {
      userId: USER_ID,
      command: "reset",
      timestamp: new Date().toISOString(),
    };
    await sendMessage(producer, TOPICS.USER_CONTROL, USER_ID, event);
    console.log("[ui] Reset command sent.\n> ");
    continue;
  }

  // Ex1/2: send legacy UserInputEvent
  const legacyEvent: UserInputEvent = {
    userId: USER_ID,
    userInput: input,
    timestamp: new Date().toISOString(),
  };
  await sendMessage(producer, TOPICS.USER_INPUT, USER_ID, legacyEvent);

  // Final Project: send UserQueryReceived to user-commands
  const conversationId = crypto.randomUUID();
  const queryEvent: UserQueryReceived = {
    conversationId,
    timestamp: Date.now(),
    eventType: "UserQueryReceived",
    payload: { userInput: input },
  };
  await sendMessage(producer, TOPICS.USER_COMMANDS, conversationId, queryEvent);

  process.stdout.write("> ");
}
