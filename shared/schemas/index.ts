import type { z } from "zod";
import { TOPICS, type TopicName } from "../topics";
import { UserQueryReceivedSchema } from "./UserQueryReceived";
import { PlanGeneratedSchema } from "./PlanGenerated";
import { ToolInvocationRequestedSchema } from "./ToolInvocationRequested";
import { ToolInvocationResultedSchema } from "./ToolInvocationResulted";
import { PlanCompletedSchema } from "./PlanCompleted";
import { PlanFailedSchema } from "./PlanFailed";
import { SynthesizeFinalAnswerRequestedSchema } from "./SynthesizeFinalAnswerRequested";
import { FinalAnswerSynthesizedSchema } from "./FinalAnswerSynthesized";
import { MessageDeadLetteredSchema } from "./MessageDeadLettered";

export type { UserQueryReceived, UserQueryReceivedPayload } from "./UserQueryReceived";
export type { PlanGenerated, PlanGeneratedPayload, PlanStep } from "./PlanGenerated";
export type { ToolInvocationRequested, ToolInvocationRequestedPayload } from "./ToolInvocationRequested";
export type { ToolInvocationResulted, ToolInvocationResultedPayload } from "./ToolInvocationResulted";
export type { PlanCompleted, PlanCompletedPayload } from "./PlanCompleted";
export type { PlanFailed } from "./PlanFailed";
export type { SynthesizeFinalAnswerRequested } from "./SynthesizeFinalAnswerRequested";
export type { FinalAnswerSynthesized, FinalAnswerSynthesizedPayload } from "./FinalAnswerSynthesized";
export type { MessageDeadLettered, DeadLetterErrorClass } from "./MessageDeadLettered";

// ─── Registry ─────────────────────────────────────────────────────────────────

/** Message name → zod schema. The single source of truth for every wire format. */
export const MESSAGE_SCHEMAS = {
  UserQueryReceived: UserQueryReceivedSchema,
  SynthesizeFinalAnswerRequested: SynthesizeFinalAnswerRequestedSchema,
  PlanGenerated: PlanGeneratedSchema,
  ToolInvocationRequested: ToolInvocationRequestedSchema,
  ToolInvocationResulted: ToolInvocationResultedSchema,
  PlanCompleted: PlanCompletedSchema,
  PlanFailed: PlanFailedSchema,
  FinalAnswerSynthesized: FinalAnswerSynthesizedSchema,
  MessageDeadLettered: MessageDeadLetteredSchema,
} as const;

export type MessageName = keyof typeof MESSAGE_SCHEMAS;

/** Which message types may appear on which topic. */
export const TOPIC_MESSAGES = {
  [TOPICS.USER_COMMANDS]: ["UserQueryReceived", "SynthesizeFinalAnswerRequested"],
  [TOPICS.CONVERSATION_EVENTS]: [
    "PlanGenerated",
    "ToolInvocationResulted",
    "PlanCompleted",
    "PlanFailed",
    "FinalAnswerSynthesized",
  ],
  [TOPICS.TOOL_INVOCATION_REQUESTS]: ["ToolInvocationRequested"],
  [TOPICS.DEAD_LETTER_QUEUE]: ["MessageDeadLettered"],
} as const satisfies Record<TopicName, readonly MessageName[]>;

/** The parsed message type for a message name. */
export type MessageOf<N extends MessageName> = z.infer<(typeof MESSAGE_SCHEMAS)[N]>;

/** Union of every message that may be published to or read from a topic. */
export type TopicMessage<T extends TopicName> = MessageOf<(typeof TOPIC_MESSAGES)[T][number]>;

/** The discriminator of a raw message: `eventType`, or `commandType` for commands. */
export function messageTypeOf(v: unknown): string | undefined {
  if (typeof v !== "object" || v === null) return undefined;
  const o = v as Record<string, unknown>;
  const t = o.eventType ?? o.commandType;
  return typeof t === "string" ? t : undefined;
}

/**
 * Kafka key for a message: the conversationId (per-conversation ordering), or the
 * original message's key for a dead-letter record (partition affinity for replay).
 */
export function keyOf(topic: TopicName, msg: TopicMessage<TopicName>): string | null {
  if (topic === TOPICS.DEAD_LETTER_QUEUE) {
    return (msg as MessageOf<"MessageDeadLettered">).payload.source.key;
  }
  return (msg as { conversationId: string }).conversationId;
}
