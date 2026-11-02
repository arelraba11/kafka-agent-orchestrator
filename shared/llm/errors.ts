import { NonRetryableError } from "../kafka/errors";

// ─── LLM errors ───────────────────────────────────────────────────────────────
// callLLM retries transient kinds itself, inside its budget. By the time an LlmError
// leaves callLLM, Kafka redelivery cannot help, which is what NonRetryableError means
// in the consumer wrapper. Callers MUST catch it (the synthesizer turns it into a
// ConversationFailed, the router falls back to regex). An uncaught one is
// dead-lettered as handler-non-retryable rather than redelivered.

export type LlmErrorKind =
  | "timeout"
  | "network"
  | "rate-limited"
  | "server"
  | "empty-response"
  | "auth"
  | "config";

/** Transient kinds are retried within the budget; the others are final. */
export const TRANSIENT_KINDS: ReadonlySet<LlmErrorKind> = new Set([
  "timeout",
  "network",
  "rate-limited",
  "server",
  "empty-response",
]);

export interface LlmErrorOptions {
  provider: string;
  status?: number;
  /** From a Retry-After header (429), in ms. */
  retryAfterMs?: number;
  cause?: unknown;
}

export class LlmError extends NonRetryableError {
  readonly transient: boolean;
  readonly provider: string;
  readonly status?: number;
  readonly retryAfterMs?: number;

  constructor(
    public readonly kind: LlmErrorKind,
    message: string,
    options: LlmErrorOptions
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.transient = TRANSIENT_KINDS.has(kind);
    this.provider = options.provider;
    this.status = options.status;
    this.retryAfterMs = options.retryAfterMs;
  }
}
