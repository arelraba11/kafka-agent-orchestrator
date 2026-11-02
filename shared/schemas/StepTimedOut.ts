import { z } from "zod";
import { event, StepIndex, Timestamp } from "./common";

export const StepTimedOutSchema = event(
  "StepTimedOut",
  z.object({
    stepIndex: StepIndex,
    deadline: Timestamp.meta({
      description: "The stepDeadline (epoch ms) the timer was armed for; a mismatch with the plan's deadline marks it stale.",
    }),
  })
).meta({
  title: "StepTimedOut",
  description:
    "A tool step did not answer before its deadline. Published by the orchestrator's timer so the timeout flows through the same serialized event path as results. Topic: conversation-events. Producer and consumer: orchestrator.",
});

export type StepTimedOut = z.infer<typeof StepTimedOutSchema>;
export type StepTimedOutPayload = StepTimedOut["payload"];
