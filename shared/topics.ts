export const TOPICS = {
  USER_COMMANDS:            "user-commands",
  CONVERSATION_EVENTS:      "conversation-events",
  TOOL_INVOCATION_REQUESTS: "tool-invocation-requests",
  DEAD_LETTER_QUEUE:        "dead-letter-queue",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];

// ─── Namespacing (tests only) ────────────────────────────────────────────────
// KAFKA_NAMESPACE prefixes every topic and consumer group at the I/O edge (client.ts),
// so one broker can host many isolated test namespaces. Everything above the edge,
// including MessageDeadLettered.source.topic and consumer.group, stays logical. The
// default namespace is "" (the identity).

export function physicalTopic(namespace: string, name: TopicName): string {
  return namespace + name;
}

/** The logical topic behind a physical one, or null when it is outside `namespace` or unknown. */
export function logicalTopic(namespace: string, physical: string): TopicName | null {
  if (!physical.startsWith(namespace)) return null;
  const name = physical.slice(namespace.length);
  return (Object.values(TOPICS) as string[]).includes(name) ? (name as TopicName) : null;
}

export function physicalGroup(namespace: string, group: string): string {
  return namespace + group;
}
