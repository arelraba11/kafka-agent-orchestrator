/**
 * sanitizerService.ts
 *
 * Consumes raw customer messages, calls Ollama to scrub PII,
 * and publishes sanitized messages downstream.
 *
 * Usage:
 *   bun run src/node/customer-support/sanitizerService.ts
 *
 * Topics:
 *   IN  ← raw-customer-messages  (group: sanitizer-group)
 *   OUT → sanitized-messages
 *
 * Dependencies:
 *   Ollama must be running locally (default: http://localhost:11434)
 *   Model: llama3 (configurable via OLLAMA_MODEL env var)
 */

import { createProducer, createConsumer, sendMessage, registerShutdown } from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { buildSanitizePrompt } from "../../../shared/prompts/customerSupportPrompts";
import type { RawMessage, SanitizedMessage } from "../../../shared/types/customerSupport";

const OLLAMA_BASE_URL = process.env.OLLAMA_BASE_URL ?? "http://localhost:11434";
const OLLAMA_MODEL = process.env.OLLAMA_MODEL ?? "llama3";

async function callOllama(text: string): Promise<string> {
  const response = await fetch(`${OLLAMA_BASE_URL}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: OLLAMA_MODEL,
      prompt: buildSanitizePrompt(text),
      stream: false,
    }),
  });

  if (!response.ok) {
    throw new Error(`Ollama error: ${response.status} ${response.statusText}`);
  }

  const data = (await response.json()) as { response: string };
  return data.response.trim();
}

async function main(): Promise<void> {
  const producer = await createProducer();
  const consumer = await createConsumer("sanitizer-group");

  registerShutdown([producer, consumer]);

  await consumer.subscribe({
    topic: TOPICS.RAW_CUSTOMER_MESSAGES,
    fromBeginning: false,
  });

  console.log("[sanitizer] Connected. Waiting for messages...");

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      const parsed = JSON.parse(raw) as RawMessage;
      const { message_id, text, timestamp, t_produced } = parsed;

      console.log(`[sanitizer] received ${message_id}`);

      const sanitized_text = await callOllama(text);

      const payload: SanitizedMessage = {
        message_id,
        timestamp,
        sanitized_text,
        t_produced,
        t_sanitizer_out: Date.now(),
      };

      await sendMessage(producer, TOPICS.SANITIZED_MESSAGES, message_id, payload);

      console.log(`[sanitizer] sanitized ${message_id}`);

      await consumer.commitOffsets([
        { topic, partition, offset: (Number(message.offset) + 1).toString() },
      ]);
    },
  });
}

main().catch((err) => {
  console.error("[sanitizer] Fatal error:", err);
  process.exit(1);
});
