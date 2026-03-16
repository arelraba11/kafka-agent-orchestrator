// ─── PlanGenerated ───────────────────────────────────────────────────────────
// Topic:    conversation-events
// Producer: routerService.ts (plan generator)
// Consumer: orchestrator.ts

export interface PlanGeneratedPayload {
  steps: string[];
}

export interface PlanGenerated {
  conversationId: string;
  timestamp: number;
  eventType: "PlanGenerated";
  payload: PlanGeneratedPayload;
}
