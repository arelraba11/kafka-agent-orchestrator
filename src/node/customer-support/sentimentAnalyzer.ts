/**
 * sentimentAnalyzer.ts
 *
 * Consumes sanitized customer messages and classifies sentiment
 * using gpt-4o-mini via the shared OpenAI client.
 *
 * Usage:
 *   bun run services/sentiment-analyzer/sentimentAnalyzer.ts
 *
 * Topics:
 *   IN  ← sanitized-messages     (group: sentiment-group)
 *   OUT → analysis-sentiment
 */

import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { callLLM } from "../../../shared/llm/openai";
import { sentimentPrompt } from "../../../prompts/customerSupportPrompts";
import type { SanitizedMessage, SentimentResult } from "../../../shared/types/customerSupport";

async function main(): Promise<void> {
  const producer = await createProducer();
  const consumer = await createConsumer("sentiment-group");

  registerShutdown([producer, consumer]);

  await subscribeAndRun(
    consumer,
    [TOPICS.SANITIZED_MESSAGES],
    async (_topic, _key, value) => {
      const msg = value as SanitizedMessage;
      const { message_id, sanitized_text, t_produced, t_sanitizer_out } = msg;

      console.log(`[sentiment] received ${message_id}`);

      const raw = await callLLM(sentimentPrompt(sanitized_text));
      const parsed = JSON.parse(raw) as { label: string; score: number };

      const label = parsed.label === "NEGATIVE" ? "NEGATIVE" : "POSITIVE";

      const payload: SentimentResult = {
        message_id,
        label,
        score: parsed.score,
        t_produced,
        t_sanitizer_out,
        t_sentiment_out: Date.now(),
      };

      console.log(`[sentiment] label=${label} score=${parsed.score}`);

      await sendMessage(producer, TOPICS.ANALYSIS_SENTIMENT, message_id, payload);
    }
  );

  console.log("[sentiment] SentimentAnalyzer started.");
}

main().catch((err) => {
  console.error("[sentiment] Fatal error:", err);
  process.exit(1);
});
