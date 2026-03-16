import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type {
  UserInputEvent,
  IntentMathEvent,
  IntentWeatherEvent,
  IntentExchangeEvent,
  IntentGeneralChatEvent,
  ConversationHistoryUpdateEvent,
  RouterDecisionEvent,
  AppResultEvent,
} from "../../../shared/types/events";
import type { ConversationHistory } from "../../../shared/types/conversation";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { PlanGenerated } from "../../../shared/schemas/PlanGenerated";

// ─── Mode ─────────────────────────────────────────────────────────────────────

const ROUTER_MODE = process.env.ROUTER_MODE ?? "regex";

// ─── Guardrail check (mirrors guardrail-service logic) ────────────────────────
// Running the check inline guarantees routing is blocked before any message
// reaches downstream services — no race condition with the separate audit service.

const GUARDRAIL_POLITICS =
  /\b(politics|politician|election|government|democracy|dictatorship|president|prime minister|coup|regime|propaganda|vote|ballot|senate|congress|parliament)\b/i;

const GUARDRAIL_MALWARE =
  /\b(hack|hacking|malware|virus|ransomware|exploit|zero.?day|sql injection|xss|ddos|botnet|trojan|keylogger|phishing|rootkit|payload|reverse shell|bypass security|bomb|weapon|explosive|kill|attack|murder|terrorist|terrorism|assassin|poison|grenade|firearm|shooting)\b/i;

function getGuardrailViolation(input: string): string | null {
  if (GUARDRAIL_MALWARE.test(input))
    return "Input contains potentially malicious content and has been blocked.";
  if (GUARDRAIL_POLITICS.test(input))
    return "Input touches political topics which are outside the scope of this system.";
  return null;
}

// ─── Intent classification (regex — used in ROUTER_MODE=regex) ────────────────

// Matches symbolic expressions: 2+2, 25 * 4, 10/5, 7-3, etc.
const MATH_OPERATOR_REGEX = /\d+\s*[+\-*/]\s*\d+/;
// Matches natural-language math requests that contain at least one digit
const MATH_WORD_REGEX = /\b(multiply|multiplied|times|product|calculate|divided|plus|minus)\b/i;
const WEATHER_REGEX = /\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i;
const CURRENCY_REGEX = /\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i;
const CITY_REGEX = /\bin\s+([A-Za-z\s]+?)(?:\?|$)/i;

function isMathInput(input: string): boolean {
  if (MATH_OPERATOR_REGEX.test(input)) return true;
  // Word-form only counts if there is also a digit in the input
  if (MATH_WORD_REGEX.test(input) && /\d/.test(input)) return true;
  return false;
}

type Intent = "math" | "weather" | "exchange" | "chat";

function regexClassify(input: string): Intent {
  if (isMathInput(input))         return "math";
  if (WEATHER_REGEX.test(input))  return "weather";
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

  // ── Guardrail check — block before any routing ────────────────────────────
  const violation = getGuardrailViolation(userInput);
  if (violation) {
    const payload: AppResultEvent = {
      userId,
      type: "chat",
      result: violation,
      success: true,
      timestamp,
    };
    await sendMessage(producer, TOPICS.APP_RESULTS, userId, payload);
    console.log(`[router] GUARDRAIL BLOCK userId=${userId} input="${userInput}"`);
    return;
  }

  // ── LLM mode: forward raw input to llm-router-service + cot-math-service ──
  if (ROUTER_MODE === "llm") {
    const payload: RouterDecisionEvent = {
      userId,
      input: userInput,
      context: history[userId] ?? [],
      timestamp,
    };
    await sendMessage(producer, TOPICS.ROUTER_DECISION, userId, payload);
    console.log(`[router] userId=${userId} mode=llm → published to router-decision-events`);
    return;
  }

  // ── Regex mode: existing logic unchanged ──────────────────────────────────
  const intent = regexClassify(userInput);

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

// ─── Plan generation (Final Project) ─────────────────────────────────────────
// Keyword-based multi-step planner. Rules are evaluated independently so a
// single query can produce multiple steps (e.g. "weather and exchange rate").

const PLAN_WEATHER_REGEX  = /\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i;
const PLAN_EXCHANGE_REGEX = /\b(exchange|convert|USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i;

function generatePlan(userInput: string): string[] {
  const steps: string[] = [];

  if (PLAN_WEATHER_REGEX.test(userInput))  steps.push("weather");
  if (PLAN_EXCHANGE_REGEX.test(userInput)) steps.push("exchange");
  if (isMathInput(userInput))              steps.push("math");

  if (steps.length === 0) steps.push("chat");

  return steps;
}

async function routePlan(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: UserQueryReceived
): Promise<void> {
  const { conversationId, payload } = event;
  const steps = generatePlan(payload.userInput);

  const planEvent: PlanGenerated = {
    conversationId,
    timestamp: Date.now(),
    eventType: "PlanGenerated",
    payload: { steps, userInput: payload.userInput },
  };

  await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, planEvent);

  const routerLatency = Date.now() - event.timestamp;
  console.log(`[router] conversationId=${conversationId} plan=[${steps.join(", ")}] input="${payload.userInput}"`);
  console.log(`[Benchmark] conversationId=${conversationId} routerLatency=${routerLatency}ms`);
}

// ─── Conversation history cache ───────────────────────────────────────────────
// routerService keeps a local copy of each user's history,
// updated by conversation-history-update events from memoryService.

type ConversationHistoryCache = Record<string, ConversationHistory>;

const historyCache: ConversationHistoryCache = {};

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("router-service");
const consumerPlan = await createConsumer("router-plan-service");

registerShutdown([producer, consumer, consumerPlan]);

// ── Ex1/2 routing (unchanged) ─────────────────────────────────────────────────

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

// ── Final Project: plan generation ───────────────────────────────────────────

subscribeAndRun(
  consumerPlan,
  [TOPICS.USER_COMMANDS],
  async (_topic, _key, value) => {
    const event = value as UserQueryReceived;
    if (event.eventType !== "UserQueryReceived") return;
    await routePlan(producer, event);
  }
).catch(err => console.error("[router] Plan consumer error:", err));

console.log("[router] RouterService started.");
