// ─── UserQueryReceived ───────────────────────────────────────────────────────
// Topic:    user-commands
// Producer: userInterface.ts
// Consumer: routerService.ts (plan generator)

export interface UserQueryReceivedPayload {
  userInput: string;
}

export interface UserQueryReceived {
  conversationId: string;
  timestamp: number;
  eventType: "UserQueryReceived";
  payload: UserQueryReceivedPayload;
}
