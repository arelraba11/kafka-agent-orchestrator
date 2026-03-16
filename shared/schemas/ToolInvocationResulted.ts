// ─── ToolInvocationResulted ──────────────────────────────────────────────────
// Topic:    conversation-events
// Producer: tool workers (mathApp, weatherApp, exchangeApp, generalChatApp)
// Consumer: orchestrator.ts, aggregator.ts

export interface ToolInvocationResultedPayload {
  toolName: string;
  result: Record<string, unknown>;
}

export interface ToolInvocationResulted {
  conversationId: string;
  timestamp: number;
  eventType: "ToolInvocationResulted";
  payload: ToolInvocationResultedPayload;
}
