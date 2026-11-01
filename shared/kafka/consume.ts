import {
  MESSAGE_SCHEMAS,
  TOPIC_MESSAGES,
  messageTypeOf,
  type MessageName,
  type MessageOf,
} from "../schemas/index";
import type { MessageDeadLettered } from "../schemas/MessageDeadLettered";
import type { TopicName } from "../topics";
import { buildDeadLetter, type RawMessage } from "./deadLetter";
import {
  ContractViolationError,
  KeyMismatchError,
  MalformedMessageError,
  RetryableError,
  classifyError,
  toIssues,
} from "./errors";

/** An unknown error is retried this many times (per process lifetime), then dead-lettered. */
export const MAX_UNKNOWN_ATTEMPTS = 3;

export interface ConsumeContext<N extends MessageName = MessageName> {
  topic: TopicName;
  group: string;
  service: string;
  /** The message types this service handles; everything else on the topic is skipped. */
  types: readonly N[];
}

export interface ConsumeDeps {
  publishDeadLetter(record: MessageDeadLettered): Promise<void>;
  /** In-memory unknown-error attempt counters, keyed `topic:partition:offset`. */
  attempts: Map<string, number>;
  now(): number;
  maxUnknownAttempts: number;
  log?: Pick<Console, "info" | "warn" | "error">;
}

export type Outcome = "handled" | "skipped" | "dead-lettered";

function describe(raw: RawMessage): string {
  return `topic=${raw.topic} partition=${raw.partition} offset=${raw.offset}`;
}

/**
 * Validates one inbound message: null check → JSON.parse → discriminator → schema →
 * key check. Returns null to skip (a type this service does not handle). Throws only
 * NonRetryableError subclasses.
 */
function parseInbound<N extends MessageName>(
  raw: RawMessage,
  ctx: ConsumeContext<N>,
  log: Pick<Console, "info" | "warn" | "error">
): MessageOf<N> | null {
  if (raw.value === null) {
    throw new MalformedMessageError("missing-value", "message has a null value");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.value.toString("utf8"));
  } catch (cause) {
    throw new MalformedMessageError("malformed-json", `value is not valid JSON: ${(cause as Error).message}`, { cause });
  }

  const type = messageTypeOf(parsed);
  if (type === undefined) {
    throw new ContractViolationError("message has no eventType or commandType", [
      { path: "eventType", message: "missing discriminator" },
    ]);
  }

  // Forward compatible: a type this topic does not know is skipped, not dead-lettered.
  if (!(TOPIC_MESSAGES[ctx.topic] as readonly string[]).includes(type)) {
    log.warn(`[kafka] skipping unknown message type=${type} ${describe(raw)}`);
    return null;
  }
  if (!(ctx.types as readonly string[]).includes(type)) return null;

  const result = MESSAGE_SCHEMAS[type as MessageName].safeParse(parsed);
  if (!result.success) {
    throw new ContractViolationError(`invalid ${type}`, toIssues(result.error.issues));
  }
  const message = result.data as MessageOf<N>;

  // Phase 2's partition invariant: every event is keyed by its conversationId.
  const key = raw.key === null ? null : raw.key.toString("utf8");
  const conversationId = (message as { conversationId: string }).conversationId;
  if (key !== conversationId) {
    throw new KeyMismatchError(`key ${JSON.stringify(key)} does not match conversationId ${conversationId}`);
  }
  return message;
}

/**
 * The pure core of the consumer wrapper.
 *
 * INVARIANT that makes dead-lettering safe: every side effect in a handler goes
 * through publish() or planStore, and those always throw RetryableError. So the only
 * errors that are non-retryable or unknown come from validation or pure code, which
 * (in the orchestrator) run before any emit or persist. A dead-lettered input has
 * therefore produced no partial effects.
 *
 *   retry        rethrow: kafkajs leaves the offset unresolved and redelivers, without
 *                limit (while LevelDB or Kafka is down every message would fail anyway)
 *   dead-letter  publish the DLQ record, then return: kafkajs resolves the offset
 *   unknown      rethrow until MAX attempts, then dead-letter as unknown-exhausted
 *
 * A failure while dead-lettering is rethrown as retryable: a message is never
 * skipped without a DLQ record.
 */
export async function processMessage<N extends MessageName>(
  raw: RawMessage,
  ctx: ConsumeContext<N>,
  handler: (message: MessageOf<N>) => Promise<void>,
  deps: ConsumeDeps
): Promise<Outcome> {
  const log = deps.log ?? console;
  const attemptKey = `${raw.topic}:${raw.partition}:${raw.offset}`;

  async function deadLetter(err: unknown, attempts: number): Promise<Outcome> {
    let record: MessageDeadLettered;
    try {
      record = buildDeadLetter(raw, { group: ctx.group, service: ctx.service }, err, attempts, deps.now());
      await deps.publishDeadLetter(record);
    } catch (cause) {
      throw new RetryableError(`failed to dead-letter ${describe(raw)}: ${(cause as Error).message}`, { cause });
    }
    deps.attempts.delete(attemptKey);
    const e = record.payload.error;
    log.error(
      `[kafka] dead-lettered ${describe(raw)} group=${ctx.group} class=${e.class} reason=${JSON.stringify(e.reason)}`
    );
    if (err instanceof Error && err.stack) log.error(err.stack);
    return "dead-lettered";
  }

  let message: MessageOf<N> | null;
  try {
    message = parseInbound(raw, ctx, log);
  } catch (err) {
    if (classifyError(err) !== "dead-letter") throw err;
    return deadLetter(err, 1);
  }
  if (message === null) return "skipped";

  try {
    await handler(message);
    deps.attempts.delete(attemptKey);
    return "handled";
  } catch (err) {
    const prior = deps.attempts.get(attemptKey) ?? 0;
    switch (classifyError(err)) {
      case "retry":
        log.warn(
          `[kafka] retrying ${describe(raw)} group=${ctx.group} reason=${JSON.stringify((err as Error).message)} (infrastructure error, will be redelivered)`
        );
        throw err;
      case "dead-letter":
        return deadLetter(err, prior + 1);
      case "unknown": {
        const attempt = prior + 1;
        if (attempt >= deps.maxUnknownAttempts) return deadLetter(err, attempt);
        deps.attempts.set(attemptKey, attempt);
        log.warn(
          `[kafka] retrying ${describe(raw)} group=${ctx.group} attempt=${attempt}/${deps.maxUnknownAttempts} reason=${JSON.stringify(err instanceof Error ? err.message : String(err))}`
        );
        throw err;
      }
    }
  }
}
