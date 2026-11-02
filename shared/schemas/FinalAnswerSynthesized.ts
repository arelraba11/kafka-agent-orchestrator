import { z } from "zod";
import { event } from "./common";

export const FinalAnswerSynthesizedSchema = event(
  "FinalAnswerSynthesized",
  z.object({ answer: z.string().regex(/\S/) })
).meta({
  title: "FinalAnswerSynthesized",
  description:
    "The LLM-written answer for a conversation. The answer must contain a non-whitespace character; an empty LLM answer is reported as a ConversationFailed instead. Topic: conversation-events. Producer: answerSynthesizer. Consumer: webServer.",
});

export type FinalAnswerSynthesized = z.infer<typeof FinalAnswerSynthesizedSchema>;
export type FinalAnswerSynthesizedPayload = FinalAnswerSynthesized["payload"];
