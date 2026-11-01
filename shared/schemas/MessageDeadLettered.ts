import { z } from "zod";
import { ConversationId, Timestamp, TOPIC_NAMES } from "./common";

export const DEAD_LETTER_ERROR_CLASSES = [
  "malformed-json",
  "missing-value",
  "contract-violation",
  "outbound-contract-violation",
  "key-mismatch",
  "handler-non-retryable",
  "unknown-exhausted",
] as const;

export const MessageDeadLetteredSchema = z
  .object({
    eventType: z.literal("MessageDeadLettered"),
    schemaVersion: z.literal(1),
    conversationId: ConversationId.nullable().meta({
      description: "Best-effort from the original message; null when it could not be parsed.",
    }),
    timestamp: Timestamp.meta({ description: "When the message was dead-lettered (epoch ms)." }),
    payload: z.object({
      source: z.object({
        topic: z.enum(TOPIC_NAMES),
        partition: z.number().int().nonnegative(),
        offset: z.string().regex(/^\d+$/).meta({ description: "int64 as a decimal string." }),
        key: z.string().nullable(),
        timestamp: z.string().regex(/^\d+$/).nullable().meta({
          description: "Broker or create time of the original message, as a decimal string.",
        }),
      }),
      consumer: z.object({ group: z.string().min(1), service: z.string().min(1) }),
      error: z.object({
        class: z.enum(DEAD_LETTER_ERROR_CLASSES),
        name: z.string(),
        reason: z.string(),
        issues: z
          .array(z.object({ path: z.string(), message: z.string() }))
          .optional()
          .meta({
            description:
              "For contract-violation these describe the original message; for outbound-contract-violation they describe the message the handler tried to produce.",
          }),
        outbound: z
          .object({ topic: z.enum(TOPIC_NAMES), messageType: z.string() })
          .optional()
          .meta({ description: "Only for outbound-contract-violation: the rejected output." }),
        attempts: z.number().int().positive(),
      }),
      originalValue: z
        .object({
          encoding: z.enum(["utf8", "base64"]),
          data: z.string(),
          truncated: z.boolean(),
        })
        .nullable()
        .meta({ description: "Raw bytes of the original value; null for a null-value message." }),
    }),
  })
  .meta({
    title: "MessageDeadLettered",
    description:
      "A poison message that can never be processed. Topic: dead-letter-queue, keyed by the original message key. Producers: every consumer wrapper and the Python rag_retriever. Business failures are PlanFailed events, never dead letters.",
  });

export type MessageDeadLettered = z.infer<typeof MessageDeadLetteredSchema>;
export type DeadLetterErrorClass = (typeof DEAD_LETTER_ERROR_CLASSES)[number];
