import type { ConversationFailed } from "../schemas/ConversationFailed";
import type { MessageName } from "../schemas/index";

// Which service reports a dead-lettered message to the user, and as which stage.
// Side-effect-free on purpose: the service shells import their entry from here, and
// failureOwnership.test.ts pins the table without starting Kafka.
//
// A message type is owned by AT MOST ONE service. Several consumer groups can
// dead-letter the same poison message (each writes its own DLQ record), but only the
// owner turns it into a user-visible event, so the user is told exactly once. See
// DeadLetteredHook in consume.ts.

type Stage = ConversationFailed["payload"]["stage"];

export const FAILURE_OWNERSHIP = {
  /** Inbound violations, and an outbound PlanGenerated violation raised while handling one. */
  router: { owns: "UserQueryReceived", stage: "routing" },
  /** Only when no plan exists for the conversation (the shell vetoes otherwise). */
  orchestrator: { owns: "PlanGenerated", stage: "orchestration" },
  aggregator: { owns: "PlanCompleted", stage: "aggregation" },
  /**
   * Only the synthesize command. The synthesizer also consumes UserQueryReceived, but
   * as a cache fill; the router reports that failure, so a poison query yields exactly
   * one ConversationFailed.
   */
  synthesizer: { owns: "SynthesizeFinalAnswerRequested", stage: "synthesis" },
} as const satisfies Record<string, { owns: MessageName; stage: Stage }>;

/**
 * The web server reports no event; it sends a direct error frame for a poison final
 * event if the conversation is still pending.
 */
export const WEB_OWNED_TYPES = [
  "FinalAnswerSynthesized",
  "PlanFailed",
  "ConversationFailed",
] as const satisfies readonly MessageName[];
