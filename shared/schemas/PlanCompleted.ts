import { z } from "zod";
import { event, StepResult } from "./common";

export const PlanCompletedSchema = event(
  "PlanCompleted",
  z.object({ results: z.array(StepResult) }),
  { sessionId: z.string() }
).meta({
  title: "PlanCompleted",
  description:
    "Every step of a plan produced a result. Topic: conversation-events. Producer: orchestrator. Consumer: aggregator.",
});

export type PlanCompleted = z.infer<typeof PlanCompletedSchema>;
export type PlanCompletedPayload = PlanCompleted["payload"];
