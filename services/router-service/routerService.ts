import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type {
  UserInputEvent,
  IntentMathEvent,
  IntentWeatherEvent,
  IntentExchangeEvent,
  IntentGeneralChatEvent,
  ConversationHistoryUpdateEvent,
} from "../../shared/types/events";
import type { ConversationHistory } from "../../shared/types/conversation";

// ─── Intent classification ────────────────────────────────────────────────────

const MATH_REGEX = /[\d]+\s*[+\-*/]\s*[\d]+/;
const WEATHER_REGEX = /\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i;
const CURRENCY_REGEX = /\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i;
const CITY_REGEX = /\bin\s+([A-Za-z\s]+?)(?:\?|$)/i;

type Intent = "math" | "weather" | "exchange" | "chat";

function classify(input: string): Intent {
  if (MATH_REGEX.test(input))    return "math";
  if (WEATHER_REGEX.test(input)) return "weather";
  if (CURRENCY_REGEX.test(input)) return "exchange";
  return "chat";
}

function extractExpression(input: string): string {
  const match = input.match(/[\d]+[\s\d+\-*/().]+/);
  return match ? match[0].trim() : input;
}

function extractCity(input: string): string {
  const match = input.match(CITY_REGEX);
  return match ? match[1].trim() : "Tel Aviv";
}

function extractCurrency(input: string): { currencyCode: string; targetCurrency: string } {
  const match = input.match(CURRENCY_REGEX);
  return {
    currencyCode: match ? match[0].toUpperCase() : "USD",
    targetCurrency: "ILS",
  };
}

// ─── Routing ──────────────────────────────────────────────────────────────────

async function route(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: UserInputEvent,
  history: ConversationHistoryCache
): Promise<void> {
  const { userId, userInput, timestamp } = event;
  const intent = classify(userInput);

  console.log(`[router] userId=${userId} intent=${intent} input="${userInput}"`);

  switch (intent) {
    case "math": {
      const payload: IntentMathEvent = {
        userId,
        expression: extractExpression(userInput),
        timestamp,
      };
      await sendMessage(producer, TOPICS.INTENT_MATH, userId, payload);
      break;
    }
    case "weather": {
      const payload: IntentWeatherEvent = {
        userId,
        city: extractCity(userInput),
        timestamp,
      };
      await sendMessage(producer, TOPICS.INTENT_WEATHER, userId, payload);
      break;
    }
    case "exchange": {
      const { currencyCode, targetCurrency } = extractCurrency(userInput);
      const payload: IntentExchangeEvent = {
        userId,
        currencyCode,
        targetCurrency,
        timestamp,
      };
      await sendMessage(producer, TOPICS.INTENT_EXCHANGE, userId, payload);
      break;
    }
    case "chat":
    default: {
      const payload: IntentGeneralChatEvent = {
        userId,
        userInput,
        context: history[userId] ?? [],
        timestamp,
      };
      await sendMessage(producer, TOPICS.INTENT_CHAT, userId, payload);
      break;
    }
  }
}

// ─── Conversation history cache ───────────────────────────────────────────────
// routerService keeps a local copy of each user's history,
// updated by conversation-history-update events from memoryService.

type ConversationHistoryCache = Record<string, ConversationHistory>;

const historyCache: ConversationHistoryCache = {};

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("router-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.USER_INPUT, TOPICS.HISTORY_UPDATE],
  async (topic, _key, value) => {
    if (topic === TOPICS.HISTORY_UPDATE) {
      const event = value as ConversationHistoryUpdateEvent;
      historyCache[event.userId] = event.history;
      return;
    }

    if (topic === TOPICS.USER_INPUT) {
      const event = value as UserInputEvent;
      await route(producer, event, historyCache);
    }
  }
);

console.log("[router] RouterService started.");
