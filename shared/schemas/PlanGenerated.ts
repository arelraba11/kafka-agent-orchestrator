import { z } from "zod";
import { event, ToolArgs, ToolName } from "./common";

export const PlanStepSchema = z.object({
  tool: ToolName,
  args: ToolArgs,
});

export const PlanGeneratedSchema = event(
  "PlanGenerated",
  z.object({ steps: z.array(PlanStepSchema).min(1) }),
  { sessionId: z.string() }
).meta({
  title: "PlanGenerated",
  description:
    "The router turned a query into an ordered list of tool steps. Topic: conversation-events. Producer: routerService. Consumer: orchestrator.",
});

export type PlanStep = z.infer<typeof PlanStepSchema>;
export type PlanGenerated = z.infer<typeof PlanGeneratedSchema>;
export type PlanGeneratedPayload = PlanGenerated["payload"];
