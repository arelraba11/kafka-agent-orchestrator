// Message schemas for the customer support analysis pipeline (Exercise 4)

// -------------------------------------------------------------------
// raw-customer-messages
// -------------------------------------------------------------------
export interface RawMessage {
  message_id: string;    // UUID v4
  timestamp: string;     // ISO-8601
  source: "cli";
  text: string;          // original, unfiltered customer input
  t_produced: number;    // epoch ms — set by producer for latency tracking
}

// -------------------------------------------------------------------
// sanitized-messages
// -------------------------------------------------------------------
export interface SanitizedMessage {
  message_id: string;       // same UUID as the originating RawMessage
  timestamp: string;        // ISO-8601 (time of sanitization)
  sanitized_text: string;   // PII-scrubbed: [NAME], [NUMBER], etc.
  t_produced: number;       // epoch ms — copied from producer for latency tracking
  t_sanitizer_out: number;  // epoch ms — set by sanitizer after Ollama call
}

// -------------------------------------------------------------------
// analysis-sentiment
// -------------------------------------------------------------------
export type SentimentLabel = "POSITIVE" | "NEGATIVE";

export interface SentimentResult {
  message_id: string;
  label: SentimentLabel;
  score: number;            // confidence 0–1
  t_produced: number;       // epoch ms — propagated from original message
  t_sanitizer_out: number;  // epoch ms — propagated from sanitized message
  t_sentiment_out: number;  // epoch ms — set by sentiment worker
}

// -------------------------------------------------------------------
// analysis-urgency
// -------------------------------------------------------------------
export type UrgencyLabel = "Urgent" | "Complaint" | "General Inquiry";

export interface UrgencyScores {
  Urgent: number;
  Complaint: number;
  "General Inquiry": number;
}

export interface UrgencyResult {
  message_id: string;
  label: UrgencyLabel;
  scores: UrgencyScores;
  t_produced: number;       // epoch ms — propagated from original message
  t_sanitizer_out: number;  // epoch ms — propagated from sanitized message
  t_urgency_out: number;    // epoch ms — set by urgency worker
}

// -------------------------------------------------------------------
// insight-aggregator internal state
// -------------------------------------------------------------------
export interface PartialInsight {
  message_id: string;
  sanitized_text: string;
  t_produced: number;
  t_sanitizer_out: number;
  sentiment: Pick<SentimentResult, "label" | "score" | "t_sentiment_out"> | null;
  urgency: Pick<UrgencyResult, "label" | "scores" | "t_urgency_out"> | null;
}

export interface FullInsight {
  message_id: string;
  sanitized_text: string;
  sentiment: Pick<SentimentResult, "label" | "score">;
  urgency: Pick<UrgencyResult, "label" | "scores">;
  benchmark: {
    sanitize_latency_ms: number;
    sentiment_latency_ms: number;
    urgency_latency_ms: number;
    total_latency_ms: number;
  };
}
