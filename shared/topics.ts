export const TOPICS = {
  USER_INPUT:     "user-input-events",
  USER_CONTROL:   "user-control-events",
  INTENT_MATH:    "intent-math",
  INTENT_WEATHER: "intent-weather",
  INTENT_EXCHANGE:"intent-exchange",
  INTENT_CHAT:    "intent-general-chat",
  APP_RESULTS:    "app-results",
  BOT_RESPONSES:  "bot-responses",
  HISTORY_UPDATE:       "conversation-history-update",
  ROUTER_DECISION:      "router-decision-events",
  GUARDRAIL_VIOLATION:  "guardrail-violation-events",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];
