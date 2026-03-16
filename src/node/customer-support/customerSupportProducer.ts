/**
 * customerSupportProducer.ts
 *
 * Reads raw customer messages from CLI and publishes them to
 * the raw-customer-messages Kafka topic.
 *
 * Usage:
 *   bun run services/customer-support-producer/customerSupportProducer.ts
 *   bun run services/customer-support-producer/customerSupportProducer.ts "Jane called 555-1234"
 *
 * Topics:
 *   OUT → raw-customer-messages
 */

import { createProducer, sendMessage } from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { RawMessage } from "../../../shared/types/customerSupport";

async function publish(
  producer: Awaited<ReturnType<typeof createProducer>>,
  text: string
): Promise<void> {
  const message_id = crypto.randomUUID();

  const payload: RawMessage = {
    message_id,
    text,
    timestamp: new Date().toISOString(),
    source: "cli",
    t_produced: Date.now(),
  };

  await sendMessage(producer, TOPICS.RAW_CUSTOMER_MESSAGES, message_id, payload);
  console.log(`[producer] Sent message ${message_id}`);
}

async function main(): Promise<void> {
  const producer = await createProducer();

  const argText = process.argv[2]?.trim();

  if (argText) {
    await publish(producer, argText);
    await producer.disconnect();
    return;
  }

  console.log("[producer] Connected. Type a message and press Enter. Ctrl+C to exit.\n");
  process.stdout.write("> ");

  const decoder = new TextDecoder();

  for await (const chunk of Bun.stdin.stream()) {
    const text = decoder.decode(chunk).trim();
    if (!text) {
      process.stdout.write("> ");
      continue;
    }

    await publish(producer, text);
    process.stdout.write("> ");
  }

  await producer.disconnect();
}

main().catch((err) => {
  console.error("[producer] Fatal error:", err);
  process.exit(1);
});
