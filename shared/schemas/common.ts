import { z } from "zod";
import { TOPICS } from "../topics";

// ─── Shared building blocks for every message contract ───────────────────────
// Contract schemas must stay representable in JSON Schema: no .refine() and no
// .transform() (scripts/gen-schemas.ts fails loudly if one sneaks in).

/** Names of the four Kafka topics, as a tuple usable in z.enum. */
export const TOPIC_NAMES = [
  TOPICS.USER_COMMANDS,
  TOPICS.CONVERSATION_EVENTS,
  TOPICS.TOOL_INVOCATION_REQUESTS,
  TOPICS.DEAD_LETTER_QUEUE,
] as const;

export const ConversationId = z.uuid();
export const Timestamp = z.number().int().nonnegative();
export const StepIndex = z.number().int().nonnegative();

/** Closed set of tools the router may plan and the workers implement. */
export const ToolName = z.enum([
  "weather",
  "exchange",
  "math",
  "chat",
  "getProductInformation",
]);

export const ToolArgs = z.record(z.string(), z.unknown());

/** One completed plan step as accumulated by the orchestrator. */
export const StepResult = z.object({
  toolName: ToolName,
  result: z.record(z.string(), z.unknown()),
});
export type StepResultEntry = z.infer<typeof StepResult>;

/**
 * Envelope shared by every event: conversationId (also the Kafka key), timestamp,
 * the eventType discriminator and a payload. `extra` adds top-level fields (sessionId).
 */
export function event<T extends string, P extends z.ZodType, E extends z.ZodRawShape = {}>(
  type: T,
  payload: P,
  extra?: E
) {
  return z.object({
    conversationId: ConversationId,
    timestamp: Timestamp,
    eventType: z.literal(type),
    payload,
    ...((extra ?? {}) as E),
  });
}

/** Same envelope for commands, which use `commandType` as the discriminator. */
export function command<T extends string, P extends z.ZodType, E extends z.ZodRawShape = {}>(
  type: T,
  payload: P,
  extra?: E
) {
  return z.object({
    conversationId: ConversationId,
    timestamp: Timestamp,
    commandType: z.literal(type),
    payload,
    ...((extra ?? {}) as E),
  });
}
