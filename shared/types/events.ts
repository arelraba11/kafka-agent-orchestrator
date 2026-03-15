import { ConversationHistory } from "./conversation";

// ─── user-input-events ──────────────────────────────────────────────────────
// Producer: userInterface.ts
// Consumers: memoryService.ts, routerService.ts

export interface UserInputEvent {
  userId: string;
  userInput: string;
  timestamp: string;
}

// ─── user-control-events ────────────────────────────────────────────────────
// Producer: userInterface.ts
// Consumer:  memoryService.ts

export type ControlCommand = "reset";

export interface UserControlEvent {
  userId: string;
  command: ControlCommand;
  timestamp: string;
}

// ─── Intent events ──────────────────────────────────────────────────────────
// Producer: routerService.ts
// Consumers: mathApp, weatherApp, exchangeApp, generalChatApp

export interface IntentMathEvent {
  userId: string;
  expression: string;
  timestamp: string;
}

export interface IntentWeatherEvent {
  userId: string;
  city: string;
  timestamp: string;
}

export interface IntentExchangeEvent {
  userId: string;
  currencyCode: string;
  targetCurrency: string;
  timestamp: string;
}

export interface IntentGeneralChatEvent {
  userId: string;
  userInput: string;
  context: ConversationHistory;
  timestamp: string;
}

// ─── app-results ────────────────────────────────────────────────────────────
// Producers: mathApp, weatherApp, exchangeApp, generalChatApp
// Consumers: responseAggregator.ts, memoryService.ts

export type AppResultType = "math" | "weather" | "exchange" | "chat";

export interface AppResultEvent {
  userId: string;
  type: AppResultType;
  result: string;
  success: boolean;
  error?: string;
  timestamp: string;
}

// ─── bot-responses ──────────────────────────────────────────────────────────
// Producer: responseAggregator.ts
// Consumer:  userInterface.ts

export interface BotResponseEvent {
  userId: string;
  message: string;
  sourceType: AppResultType;
  timestamp: string;
}

// ─── conversation-history-update ────────────────────────────────────────────
// Producer: memoryService.ts
// Consumer:  routerService.ts

export interface ConversationHistoryUpdateEvent {
  userId: string;
  history: ConversationHistory;
  timestamp: string;
}
