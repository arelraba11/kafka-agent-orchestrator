import type { FullInsight, PartialInsight } from "../types/customerSupport";

// Computes derived latency fields from a completed PartialInsight.
// Returns null if either branch is still missing.
export function computeBenchmark(
  partial: PartialInsight
): FullInsight["benchmark"] | null {
  if (!partial.sentiment || !partial.urgency) return null;

  return {
    sanitize_latency_ms:
      partial.t_sanitizer_out - partial.t_produced,
    sentiment_latency_ms:
      partial.sentiment.t_sentiment_out - partial.t_sanitizer_out,
    urgency_latency_ms:
      partial.urgency.t_urgency_out - partial.t_sanitizer_out,
    total_latency_ms:
      Math.max(
        partial.sentiment.t_sentiment_out,
        partial.urgency.t_urgency_out
      ) - partial.t_produced,
  };
}

// Merges a completed PartialInsight into a FullInsight.
// Caller must ensure both branches are non-null before calling.
export function mergeInsight(partial: PartialInsight): FullInsight {
  const benchmark = computeBenchmark(partial);
  if (!benchmark) throw new Error("mergeInsight: partial insight is incomplete");

  return {
    message_id: partial.message_id,
    sanitized_text: partial.sanitized_text,
    sentiment: {
      label: partial.sentiment!.label,
      score: partial.sentiment!.score,
    },
    urgency: {
      label: partial.urgency!.label,
      scores: partial.urgency!.scores,
    },
    benchmark,
  };
}
