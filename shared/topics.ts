export const TOPICS = {
  // ── Exercise 1 — Distributed Chatbot ──────────────────────────────────────
  USER_INPUT:     "user-input-events",
  USER_CONTROL:   "user-control-events",
  INTENT_MATH:    "intent-math",
  INTENT_WEATHER: "intent-weather",
  INTENT_EXCHANGE:"intent-exchange",
  INTENT_CHAT:    "intent-general-chat",
  APP_RESULTS:    "app-results",
  BOT_RESPONSES:  "bot-responses",
  HISTORY_UPDATE: "conversation-history-update",

  // ── Exercise 2 — LLM Prompt Engineering Pipeline ──────────────────────────
  ROUTER_DECISION:            "router-decision-events",
  GUARDRAIL_VIOLATION:        "guardrail-violation-events",

  // ── Exercise 3 — Review Analysis Pipeline ────────────────────────────────
  RAW_REVIEWS:        "raw-reviews-topic",
  PROCESSED_INSIGHTS: "processed-insights-topic",

  // ── Exercise 4 — Customer Support Analysis Pipeline ───────────────────────
  RAW_CUSTOMER_MESSAGES: "raw-customer-messages",
  SANITIZED_MESSAGES:    "sanitized-messages",
  ANALYSIS_SENTIMENT:    "analysis-sentiment",
  ANALYSIS_URGENCY:      "analysis-urgency",

  // ── Final Project — Event-Sourced AI Agent ────────────────────────────────
  USER_COMMANDS:             "user-commands",
  CONVERSATION_EVENTS:       "conversation-events",
  TOOL_INVOCATION_REQUESTS:  "tool-invocation-requests",
  DEAD_LETTER_QUEUE:         "dead-letter-queue",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];
