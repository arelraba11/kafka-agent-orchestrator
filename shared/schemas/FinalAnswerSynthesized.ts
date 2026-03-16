// ─── FinalAnswerSynthesized ──────────────────────────────────────────────────
// Topic:    conversation-events
// Producer: synthesisWorker.ts
// Consumer: userInterface.ts

export interface FinalAnswerSynthesizedPayload {
  answer: string;
}

export interface FinalAnswerSynthesized {
  conversationId: string;
  timestamp: number;
  eventType: "FinalAnswerSynthesized";
  payload: FinalAnswerSynthesizedPayload;
}
