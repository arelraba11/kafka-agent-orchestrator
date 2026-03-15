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

// ─── router-decision-events ──────────────────────────────────────────────────
// Producer: routerService.ts (ROUTER_MODE=llm only)
// Consumers: llm-router-service, cot-math-service

export type LLMIntent = "getWeather" | "calculateMath" | "currencyExchange" | "generalChat";

export interface RouterDecisionEvent {
  userId: string;
  input: string;
  // Populated downstream by llm-router-service after LLM classification.
  // Not present when routerService forwards raw input in ROUTER_MODE=llm.
  intent?: LLMIntent;
  parameters?: Record<string, unknown>;
  confidence?: number;
  timestamp: string;
}

// ─── guardrail-violation-events ──────────────────────────────────────────────
// Producer: guardrail-service
// Consumer:  (audit log only — no downstream consumer)

export type ViolationType = "politics" | "malware";

export interface GuardrailViolationEvent {
  userId: string;
  userInput: string;
  violationType: ViolationType;
  message: string;
  timestamp: string;
}

// ─── cot-math (local only — not produced to Kafka) ───────────────────────────

export interface CotMathExpressionEvent {
  userId: string;
  originalInput: string;
  expression: string;
  reasoning: string;
  timestamp: string;
}

// ─── Exercise 2 events ─────────────────────────────────────────────────────
// LLM pipeline events for the prompt-engineering pipeline (Exercise 2).

// ─── llm-prompt-requests ─────────────────────────────────────────────────────
// Producer: llm-router-service
// Consumer: (LLM gateway — future)

export interface LLMPromptRequestEvent {
  userId: string;
  intent: LLMIntent;
  prompt: string;
  timestamp: string;
}

// ─── llm-response-events ─────────────────────────────────────────────────────
// Producer: llm-router-service
// Consumer: (json-parser — merged into llm-router-service)

export interface LLMResponseEvent {
  userId: string;
  intent: LLMIntent;
  rawResponse: string;
  timestamp: string;
}

// ─── function-execution-requests ─────────────────────────────────────────────
// Producer: llm-router-service (after JSON parsing)
// Consumers: mathApp, weatherApp, exchangeApp, generalChatApp

export interface FunctionExecutionRequestEvent {
  userId: string;
  intent: LLMIntent;
  parameters: Record<string, unknown>;
  confidence: number;
  timestamp: string;
}

// ─── bot-output-events ───────────────────────────────────────────────────────
// Producer: apps (mathApp, weatherApp, exchangeApp, generalChatApp)
// Consumer: response-aggregator

export interface BotOutputEvent {
  userId: string;
  intent: LLMIntent;
  result: string;
  success: boolean;
  error?: string;
  timestamp: string;
}
