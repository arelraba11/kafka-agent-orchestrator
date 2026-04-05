// ─── PlanCompleted ───────────────────────────────────────────────────────────
// Topic:    conversation-events
// Producer: orchestrator.ts
// Consumer: aggregator.ts

export interface PlanCompletedPayload {
  results: Record<string, unknown>[];
}

export interface PlanCompleted {
  conversationId: string;
  sessionId: string;
  timestamp: number;
  eventType: "PlanCompleted";
  payload: PlanCompletedPayload;
}
