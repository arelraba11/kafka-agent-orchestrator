// Sentiment values used across events
export type Sentiment = "Positive" | "Negative" | "Mixed" | "Neutral";

// A single aspect extracted from a review
export interface AspectSentiment {
  aspect: string;
  sentiment: "Positive" | "Negative" | "Neutral";
}

// Published to raw-reviews-topic by review-producer
export interface ReviewEvent {
  reviewId: string;
  text: string;
  timestamp: string; // ISO 8601
}

// Router decision returned by reviewRouterPrompt
export interface RouterDecision {
  intent: "analyzeReview" | "ignore";
  reason: string;
}

// LLM analysis result returned by reviewAnalyzerPrompt
export interface AnalysisResult {
  summary: string;
  overall_sentiment: Sentiment;
  score: number; // 1–10
  aspects: AspectSentiment[];
}

// Published to processed-insights-topic by review-processor
export interface ReviewInsightEvent {
  reviewId: string;
  originalText: string;
  summary: string;
  overall_sentiment: Sentiment;
  score: number; // 1–10
  aspects: AspectSentiment[];
  corrected: boolean; // true if self-correction was applied
  timestamp: string; // ISO 8601
}
