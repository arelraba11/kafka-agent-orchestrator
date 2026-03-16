/**
 * urgencyClassifier.ts
 *
 * Consumes sanitized customer messages and classifies urgency
 * using gpt-4o-mini via the shared OpenAI client.
 *
 * Usage:
 *   bun run services/urgency-classifier/urgencyClassifier.ts
 *
 * Topics:
 *   IN  ← sanitized-messages     (group: urgency-group)
 *   OUT → analysis-urgency
 */

import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import { callLLM } from "../../shared/llm/openai";
import { urgencyPrompt } from "../../shared/prompts/customerSupportPrompts";
import type {
  SanitizedMessage,
  UrgencyLabel,
  UrgencyResult,
  UrgencyScores,
} from "../../shared/types/customerSupport";

const VALID_LABELS: UrgencyLabel[] = ["Urgent", "Complaint", "General Inquiry"];

async function main(): Promise<void> {
  const producer = await createProducer();
  const consumer = await createConsumer("urgency-group");

  registerShutdown([producer, consumer]);

  await subscribeAndRun(
    consumer,
    [TOPICS.SANITIZED_MESSAGES],
    async (_topic, _key, value) => {
      const msg = value as SanitizedMessage;
      const { message_id, sanitized_text, t_produced, t_sanitizer_out } = msg;

      console.log(`[urgency] received ${message_id}`);

      const raw = await callLLM(urgencyPrompt(sanitized_text));
      const parsed = JSON.parse(raw) as {
        label: string;
        scores: Record<string, number>;
      };

      const label: UrgencyLabel = VALID_LABELS.includes(parsed.label as UrgencyLabel)
        ? (parsed.label as UrgencyLabel)
        : "General Inquiry";

      const scores: UrgencyScores = {
        Urgent: parsed.scores["Urgent"] ?? 0,
        Complaint: parsed.scores["Complaint"] ?? 0,
        "General Inquiry": parsed.scores["General Inquiry"] ?? 0,
      };

      const payload: UrgencyResult = {
        message_id,
        label,
        scores,
        t_produced,
        t_sanitizer_out,
        t_urgency_out: Date.now(),
      };

      console.log(`[urgency] label=${label}`);

      await sendMessage(producer, TOPICS.ANALYSIS_URGENCY, message_id, payload);
    }
  );

  console.log("[urgency] UrgencyClassifier started.");
}

main().catch((err) => {
  console.error("[urgency] Fatal error:", err);
  process.exit(1);
});
