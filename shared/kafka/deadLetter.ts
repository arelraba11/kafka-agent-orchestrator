import { ConversationId } from "../schemas/common";
import type { MessageDeadLettered } from "../schemas/MessageDeadLettered";
import type { TopicName } from "../topics";
import {
  NonRetryableError,
  OutboundContractViolationError,
  ContractViolationError,
  deadLetterClassOf,
  type Issue,
} from "./errors";

/** The parts of a kafkajs message the wrapper and the DLQ record need. */
export interface RawMessage {
  topic: TopicName;
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
  timestamp: string | null;
}

export interface ConsumerIdentity {
  group: string;
  service: string;
}

/** Original value is cut to this many bytes; with base64 this keeps a record well under 1 MiB. */
export const MAX_ORIGINAL_BYTES = 512 * 1024;
const MAX_ENCODED_CHARS = 700_000; // base64 of MAX_ORIGINAL_BYTES is ~699k
const MAX_ISSUES = 50;
const MAX_TEXT = 1000;

function clip(s: string, n: number): string {
  return s.length > n ? s.slice(0, n) + "…" : s;
}

function roundTrips(buf: Buffer): string | null {
  const s = buf.toString("utf8");
  return Buffer.from(s, "utf8").equals(buf) ? s : null;
}

function encodeOriginal(value: Buffer | null): MessageDeadLettered["payload"]["originalValue"] {
  if (value === null) return null;
  const truncated = value.length > MAX_ORIGINAL_BYTES;
  const bytes = truncated ? value.subarray(0, MAX_ORIGINAL_BYTES) : value;

  // Prefer readable utf8. A cut in the middle of a multi-byte character is repaired
  // by dropping the dangling bytes. JSON escaping can expand control characters up
  // to 6x, so utf8 is used only when its escaped size stays small.
  for (let drop = 0; drop <= (truncated ? 3 : 0); drop++) {
    const s = roundTrips(bytes.subarray(0, bytes.length - drop));
    if (s !== null && JSON.stringify(s).length <= MAX_ENCODED_CHARS) {
      return { encoding: "utf8", data: s, truncated };
    }
  }
  return { encoding: "base64", data: bytes.toString("base64"), truncated };
}

function extractConversationId(value: Buffer | null): string | null {
  if (value === null) return null;
  try {
    const parsed = JSON.parse(value.toString("utf8")) as { conversationId?: unknown } | null;
    const r = ConversationId.safeParse(parsed?.conversationId);
    return r.success ? r.data : null;
  } catch {
    return null;
  }
}

function issuesOf(err: unknown): Issue[] | undefined {
  if (!(err instanceof ContractViolationError) && !(err instanceof OutboundContractViolationError)) {
    return undefined;
  }
  return err.issues
    .slice(0, MAX_ISSUES)
    .map((i) => ({ path: clip(i.path, 200), message: clip(i.message, 500) }));
}

/**
 * Pure. Builds the DLQ record for a poison message. `err` is whatever made the
 * message undeliverable; an error that is not a NonRetryableError becomes class
 * unknown-exhausted. For an outbound violation, `raw` is the (valid) input and the
 * record's issues/outbound describe the message the handler tried to produce.
 */
export function buildDeadLetter(
  raw: RawMessage,
  consumer: ConsumerIdentity,
  err: unknown,
  attempts: number,
  now: number
): MessageDeadLettered {
  const error = err instanceof Error ? err : new Error(String(err));
  // A bare NonRetryableError wraps a pure-code exception; report the original name.
  const cause = (error as { cause?: unknown }).cause;
  const name =
    error.constructor === NonRetryableError && cause instanceof Error ? cause.name : error.name;

  const issues = issuesOf(err);
  return {
    eventType: "MessageDeadLettered",
    schemaVersion: 1,
    conversationId: extractConversationId(raw.value),
    timestamp: now,
    payload: {
      source: {
        topic: raw.topic,
        partition: raw.partition,
        offset: raw.offset,
        key: raw.key === null ? null : raw.key.toString("utf8"),
        timestamp: raw.timestamp,
      },
      consumer,
      error: {
        class: deadLetterClassOf(err),
        name,
        reason: clip(error.message, MAX_TEXT),
        ...(issues ? { issues } : {}),
        ...(err instanceof OutboundContractViolationError
          ? { outbound: { topic: err.topic, messageType: err.messageType } }
          : {}),
        attempts,
      },
      originalValue: encodeOriginal(raw.value),
    },
  };
}
