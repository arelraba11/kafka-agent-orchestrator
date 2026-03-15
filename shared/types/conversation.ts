export interface ConversationMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: string; // ISO 8601
}

export type ConversationHistory = ConversationMessage[];
