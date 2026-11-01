import { z } from "zod";
import { event, StepIndex, ToolArgs, ToolName } from "./common";

export const ToolInvocationRequestedSchema = event(
  "ToolInvocationRequested",
  z.object({
    toolName: ToolName,
    stepIndex: StepIndex.meta({ description: "Index of the plan step this request executes." }),
    input: ToolArgs,
  })
).meta({
  title: "ToolInvocationRequested",
  description:
    "The orchestrator asks one tool worker to run one plan step. Topic: tool-invocation-requests. Producer: orchestrator. Consumers: the math, weather, exchange and chat workers and the Python rag_retriever; each handles its own toolName.",
});

export type ToolInvocationRequested = z.infer<typeof ToolInvocationRequestedSchema>;
export type ToolInvocationRequestedPayload = ToolInvocationRequested["payload"];
