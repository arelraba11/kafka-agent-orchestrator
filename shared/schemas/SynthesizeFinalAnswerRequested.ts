import { z } from "zod";
import { command, StepResult } from "./common";

export const SynthesizeFinalAnswerRequestedSchema = command(
  "SynthesizeFinalAnswerRequested",
  z.object({ results: z.array(StepResult) }),
  { sessionId: z.string() }
).meta({
  title: "SynthesizeFinalAnswerRequested",
  description:
    "Command to turn a completed plan's results into a final answer. Uses commandType, not eventType, as its discriminator. Topic: user-commands. Producer: aggregator. Consumer: answerSynthesizer.",
});

export type SynthesizeFinalAnswerRequested = z.infer<typeof SynthesizeFinalAnswerRequestedSchema>;
