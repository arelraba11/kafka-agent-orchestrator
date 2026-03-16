// ─── ToolInvocationRequested ─────────────────────────────────────────────────
// Topic:    tool-invocation-requests
// Producer: orchestrator.ts
// Consumer: tool workers (mathApp, weatherApp, exchangeApp, generalChatApp)

export interface ToolInvocationRequestedPayload {
  toolName: string;
  input: Record<string, unknown>;
}

export interface ToolInvocationRequested {
  conversationId: string;
  timestamp: number;
  eventType: "ToolInvocationRequested";
  payload: ToolInvocationRequestedPayload;
}
