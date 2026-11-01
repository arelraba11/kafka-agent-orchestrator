import { z } from "zod";
import { event } from "./common";

export const UserQueryReceivedSchema = event(
  "UserQueryReceived",
  z.object({
    userInput: z.string().min(1),
    sessionId: z.string(),
  })
).meta({
  title: "UserQueryReceived",
  description:
    "A user typed a query. Topic: user-commands. Producer: webServer. Consumers: routerService (plans it), answerSynthesizer (caches it).",
});

export type UserQueryReceived = z.infer<typeof UserQueryReceivedSchema>;
export type UserQueryReceivedPayload = UserQueryReceived["payload"];
