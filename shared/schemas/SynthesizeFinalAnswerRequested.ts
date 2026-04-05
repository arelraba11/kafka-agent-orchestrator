export interface SynthesizeFinalAnswerRequested {
  conversationId: string;
  sessionId: string;
  timestamp: number;
  commandType: "SynthesizeFinalAnswerRequested";
  payload: {
    results: Record<string, unknown>[];
  };
}
