export interface SynthesizeFinalAnswerRequested {
  conversationId: string;
  timestamp: number;
  commandType: "SynthesizeFinalAnswerRequested";
  payload: {
    results: Record<string, unknown>[];
  };
}
