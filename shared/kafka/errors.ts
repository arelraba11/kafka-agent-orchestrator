import type { TopicName } from "../topics";
import type { DeadLetterErrorClass } from "../schemas/MessageDeadLettered";

// ─── Error taxonomy ───────────────────────────────────────────────────────────
// Handlers signal what should happen to the message by what they throw:
//   RetryableError     infrastructure failed (store, producer): redeliver, forever
//   NonRetryableError  the message can never be processed: dead-letter and move on
//   anything else      unknown: bounded retry, then dead-letter (see consume.ts)

export interface Issue {
  path: string;
  message: string;
}

// ── Retryable ────────────────────────────────────────────────────────────────

export class RetryableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** A LevelDB I/O failure (open, read, write, closed store). A missing key is NOT one. */
export class StoreError extends RetryableError {}

/** A producer.send failure. */
export class ProducerError extends RetryableError {}

// ── Non-retryable ────────────────────────────────────────────────────────────

export class NonRetryableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** Null value, or a value that is not JSON. */
export class MalformedMessageError extends NonRetryableError {
  constructor(
    public readonly kind: "malformed-json" | "missing-value",
    message: string,
    options?: { cause?: unknown }
  ) {
    super(message, options);
  }
}

/** An inbound message failed its schema (or has no usable discriminator). */
export class ContractViolationError extends NonRetryableError {
  constructor(message: string, public readonly issues: Issue[]) {
    super(message);
  }
}

/** A message the handler tried to publish failed its schema: the producer has a bug. */
export class OutboundContractViolationError extends NonRetryableError {
  constructor(
    message: string,
    public readonly issues: Issue[],
    public readonly messageType: string,
    public readonly topic: TopicName
  ) {
    super(message);
  }
}

/** The Kafka key is not the message's conversationId. */
export class KeyMismatchError extends NonRetryableError {}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Runs pure code and converts anything it throws into a NonRetryableError. Existing
 * NonRetryableError instances pass through so their class (and DLQ class) survives.
 */
export function nonRetryable<T>(fn: () => T): T {
  try {
    return fn();
  } catch (err) {
    if (err instanceof NonRetryableError) throw err;
    const message = err instanceof Error ? err.message : String(err);
    throw new NonRetryableError(message, { cause: err });
  }
}

export type Disposition = "retry" | "dead-letter" | "unknown";

/** What the wrapper should do with a message whose handler threw `err`. Pure. */
export function classifyError(err: unknown): Disposition {
  if (err instanceof RetryableError) return "retry";
  if (err instanceof NonRetryableError) return "dead-letter";
  // Defense in depth: an infrastructure error that escaped its wrapper is still
  // transient. kafkajs marks some of these (NumberOfRetriesExceeded) non-retriable,
  // so its own flag cannot be used; match by name instead.
  if (err instanceof Error && err.name.startsWith("KafkaJS")) return "retry";
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === "string" && code.startsWith("LEVEL_")) return "retry";
  return "unknown";
}

/** DLQ `error.class` for an error. Most specific first. */
export function deadLetterClassOf(err: unknown): DeadLetterErrorClass {
  if (err instanceof MalformedMessageError) return err.kind;
  if (err instanceof ContractViolationError) return "contract-violation";
  if (err instanceof OutboundContractViolationError) return "outbound-contract-violation";
  if (err instanceof KeyMismatchError) return "key-mismatch";
  if (err instanceof NonRetryableError) return "handler-non-retryable";
  return "unknown-exhausted";
}

/** Converts zod issues (or similar) into the flat DLQ/log shape. */
export function toIssues(
  zodIssues: ReadonlyArray<{ path: ReadonlyArray<PropertyKey>; message: string }>
): Issue[] {
  return zodIssues.map((i) => ({
    path: i.path.map(String).join(".") || "(root)",
    message: i.message,
  }));
}
