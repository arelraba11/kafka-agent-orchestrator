// ─── PlanGenerated ───────────────────────────────────────────────────────────
// Topic:    conversation-events
// Producer: routerService.ts (plan generator)
// Consumer: orchestrator.ts

export interface PlanStep {
  tool: string;
  args: Record<string, unknown>;
}

export interface PlanGeneratedPayload {
  steps: PlanStep[];
}

export interface PlanGenerated {
  conversationId: string;
  sessionId: string;
  timestamp: number;
  eventType: "PlanGenerated";
  payload: PlanGeneratedPayload;
}
