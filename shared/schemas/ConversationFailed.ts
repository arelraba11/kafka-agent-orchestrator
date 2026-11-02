import { z } from "zod";
import { event } from "./common";

export const ConversationFailedSchema = event(
  "ConversationFailed",
  z.object({
    stage: z.enum(["routing", "orchestration", "aggregation", "synthesis"]),
    code: z.enum([
      "llm-unavailable",
      "llm-misconfigured",
      "empty-answer",
      "missing-question",
      "internal-error",
    ]),
    reason: z.string().meta({
      description: "Operator-facing detail for logs. The UI shows a message derived from `code`, never this.",
    }),
  })
).meta({
  title: "ConversationFailed",
  description:
    "A conversation failed at a stage that has no plan state (routing, orchestration before a plan exists, aggregation, synthesis), so the user can be told. Topic: conversation-events. Producers: answerSynthesizer, and the dead-letter hook of routerService, orchestrator and aggregator. Consumer: webServer.",
});

export type ConversationFailed = z.infer<typeof ConversationFailedSchema>;
export type ConversationFailedPayload = ConversationFailed["payload"];
