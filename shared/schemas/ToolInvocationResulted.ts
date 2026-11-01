import { z } from "zod";
import { event, StepIndex, ToolName } from "./common";

export const ToolInvocationResultedSchema = event(
  "ToolInvocationResulted",
  z.object({
    toolName: ToolName,
    stepIndex: StepIndex.meta({
      description: "Echo of ToolInvocationRequested.payload.stepIndex.",
    }),
    result: z.record(z.string(), z.unknown()).meta({
      description:
        "Open record. An `error` string marks the step as failed. `value` is the scalar a downstream `{{step_N.result}}` resolves to; the RAG worker sets it to the product price in USD, digits only, when known.",
    }),
  })
).meta({
  title: "ToolInvocationResulted",
  description:
    "A tool worker finished one plan step. Topic: conversation-events. Producers: the math, weather, exchange and chat workers and the Python rag_retriever. Consumers: orchestrator (advances the plan), webServer (progress frame).",
});

export type ToolInvocationResulted = z.infer<typeof ToolInvocationResultedSchema>;
export type ToolInvocationResultedPayload = ToolInvocationResulted["payload"];
