import { z } from "zod";
import { event, StepResult } from "./common";

export const PlanFailedSchema = event(
  "PlanFailed",
  z.object({
    reason: z.string(),
    failedTool: z.string().meta({
      description: 'The tool whose step failed, or "none" for a plan with no steps.',
    }),
    completedResults: z.array(StepResult),
  })
).meta({
  title: "PlanFailed",
  description:
    "A plan could not finish (a tool returned an error, or the plan was empty). Topic: conversation-events. Producer: orchestrator. Consumer: webServer.",
});

export type PlanFailed = z.infer<typeof PlanFailedSchema>;
