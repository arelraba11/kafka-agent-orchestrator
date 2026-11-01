import { z } from "zod";
import { event } from "./common";

export const FinalAnswerSynthesizedSchema = event(
  "FinalAnswerSynthesized",
  z.object({ answer: z.string() })
).meta({
  title: "FinalAnswerSynthesized",
  description:
    "The LLM-written answer for a conversation. An empty answer is allowed by the contract. Topic: conversation-events. Producer: answerSynthesizer. Consumer: webServer.",
});

export type FinalAnswerSynthesized = z.infer<typeof FinalAnswerSynthesizedSchema>;
export type FinalAnswerSynthesizedPayload = FinalAnswerSynthesized["payload"];
