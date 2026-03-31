export const TOPICS = {
  USER_COMMANDS:            "user-commands",
  CONVERSATION_EVENTS:      "conversation-events",
  TOOL_INVOCATION_REQUESTS: "tool-invocation-requests",
  DEAD_LETTER_QUEUE:        "dead-letter-queue",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];
