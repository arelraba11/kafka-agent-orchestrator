import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { UserInputEvent, UserControlEvent, BotResponseEvent } from "../../shared/types/events";

// ─── Config ──────────────────────────────────────────────────────────────────

const USER_ID = process.env.USER_ID ?? "user-1";

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("ui-service");

registerShutdown([producer, consumer]);

// ─── Consume bot responses ────────────────────────────────────────────────────

subscribeAndRun(
  consumer,
  [TOPICS.BOT_RESPONSES],
  async (_topic, _key, value) => {
    const event = value as BotResponseEvent;
    if (event.userId !== USER_ID) return;
    console.log(`\nBot [${event.sourceType}]: ${event.message}\n> `);
  }
).catch(err => console.error("[ui] Consumer error:", err));

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

  const event: UserInputEvent = {
    userId: USER_ID,
    userInput: input,
    timestamp: new Date().toISOString(),
  };

  await sendMessage(producer, TOPICS.USER_INPUT, USER_ID, event);
  process.stdout.write("> ");
}
