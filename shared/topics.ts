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
  LLM_PROMPT_REQUESTS:        "llm-prompt-requests",
  LLM_RESPONSE_EVENTS:        "llm-response-events",
  FUNCTION_EXECUTION_REQUESTS:"function-execution-requests",
  BOT_OUTPUT_EVENTS:          "bot-output-events",
  COT_MATH_EXPRESSION_EVENTS: "cot-math-expression-events",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];
