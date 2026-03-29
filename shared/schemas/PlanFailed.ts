export interface PlanFailed {
  conversationId: string;
  timestamp: number;
  eventType: "PlanFailed";
  payload: {
    reason: string;
    failedTool: string;
    completedResults: Record<string, unknown>[];
  };
}
