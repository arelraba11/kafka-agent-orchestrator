/**
 * insightAggregator.ts
 *
 * Consumes analysis-sentiment and analysis-urgency topics concurrently.
 * Correlates results by message_id using an in-memory Map.
 * Emits a strong alert or normal log once both branches arrive.
 *
 * Usage:
 *   bun run src/node/customer-support/insightAggregator.ts
 *
 * Topics:
 *   IN ← analysis-sentiment   (group: insight-aggregator-group)
 *   IN ← analysis-urgency     (group: insight-aggregator-group)
 *
 * Business logic:
 *   label === "NEGATIVE" AND label === "Urgent"
 *     → 🚨 STRONG ALERT
 *   otherwise
 *     → [insight] normal log
 */

import {
  createConsumer,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { SentimentResult, UrgencyResult } from "../../../shared/types/customerSupport";

// ---------------------------------------------------------------------------
// Correlation store
// ---------------------------------------------------------------------------

interface PartialInsight {
  sentiment?: Pick<SentimentResult, "label" | "score">;
  urgency?: Pick<UrgencyResult, "label" | "scores">;
}

const store = new Map<string, PartialInsight>();

// ---------------------------------------------------------------------------
// Business logic
// ---------------------------------------------------------------------------

function evaluateInsight(
  id: string,
  insight: Required<PartialInsight>
): void {
  const { sentiment, urgency } = insight;

  if (sentiment.label === "NEGATIVE" && urgency.label === "Urgent") {
    console.log(`\n🚨 STRONG ALERT: customer issue requires immediate attention`);
    console.log(
      `   id=${id}  sentiment=${sentiment.label} (${sentiment.score})  urgency=${urgency.label}\n`
    );
  } else {
    console.log(
      `[insight] id=${id} sentiment=${sentiment.label} urgency=${urgency.label}`
    );
  }
}

function upsertAndEvaluate(
  id: string,
  update: Partial<PartialInsight>
): void {
  const existing = store.get(id) ?? {};
  const merged: PartialInsight = { ...existing, ...update };

  if (merged.sentiment && merged.urgency) {
    evaluateInsight(id, merged as Required<PartialInsight>);
    store.delete(id);
  } else {
    store.set(id, merged);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("[insight] Connecting consumer...");

  const consumer = await createConsumer("insight-aggregator-group");

  registerShutdown([consumer]);

  await subscribeAndRun(
    consumer,
    [TOPICS.ANALYSIS_SENTIMENT, TOPICS.ANALYSIS_URGENCY],
    async (topic, _key, value) => {
      if (topic === TOPICS.ANALYSIS_SENTIMENT) {
        const msg = value as SentimentResult;
        console.log(`[insight] received sentiment ${msg.message_id}`);
        upsertAndEvaluate(msg.message_id, {
          sentiment: { label: msg.label, score: msg.score },
        });
      } else if (topic === TOPICS.ANALYSIS_URGENCY) {
        const msg = value as UrgencyResult;
        console.log(`[insight] received urgency ${msg.message_id}`);
        upsertAndEvaluate(msg.message_id, {
          urgency: { label: msg.label, scores: msg.scores },
        });
      }
    }
  );
}

main().catch((err) => {
  console.error("[insight] Fatal error:", err);
  process.exit(1);
});
