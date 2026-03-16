// ─── LLM Router Service ───────────────────────────────────────────────────────
// Merges llmExtractionService + jsonParserService from exercise2.
//
// Consumes: router-decision-events
// Produces: intent-weather | intent-exchange | intent-general-chat
//           app-results  (on parse failure — AppResultEvent { success: false })
//
// Math intent is intentionally skipped here — cot-math-service owns it.

import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import { callLLM } from "../../shared/llm/openai";
import { llmRouterPrompt, llmExtractionPrompt } from "../../shared/prompts/prompts";
import type {
  RouterDecisionEvent,
  LLMIntent,
  IntentMathEvent,
  IntentWeatherEvent,
  IntentExchangeEvent,
  IntentGeneralChatEvent,
  AppResultEvent,
} from "../../shared/types/events";

// ─── Currency normalisation ───────────────────────────────────────────────────
// The LLM may return informal names ("euro", "dollars", "shekel").
// Map them to the ISO codes that exchangeApp understands.

const CURRENCY_ALIASES: Record<string, string> = {
  eur: "EUR", euro: "EUR", euros: "EUR",
  usd: "USD", dollar: "USD", dollars: "USD",
  ils: "ILS", shekel: "ILS", shekels: "ILS",
  gbp: "GBP", pound: "GBP", pounds: "GBP",
  jpy: "JPY", yen: "JPY",
  chf: "CHF", franc: "CHF", francs: "CHF",
  cad: "CAD", aud: "AUD",
};

function normalizeCurrency(raw: unknown): string | undefined {
  if (!raw || typeof raw !== "string") return undefined;
  return CURRENCY_ALIASES[raw.toLowerCase()] ?? raw.toUpperCase();
}

// ─── Classification ───────────────────────────────────────────────────────────

async function classify(
  input: string
): Promise<{ intent: LLMIntent; parameters: Record<string, unknown>; confidence: number }> {
  const raw = await callLLM(llmRouterPrompt(input));
  return JSON.parse(raw);
}

// ─── Extraction ───────────────────────────────────────────────────────────────

async function extract(
  intent: string,
  input: string
): Promise<Record<string, unknown>> {
  const raw = await callLLM(llmExtractionPrompt(intent, input));
  const parsed = JSON.parse(raw);
  return parsed.parameters ?? parsed;
}

// ─── Routing ─────────────────────────────────────────────────────────────────

async function route(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: RouterDecisionEvent
): Promise<void> {
  const { userId, input, timestamp } = event;

  let intent: LLMIntent;
  let parameters: Record<string, unknown>;

  try {
    const result = await classify(input);
    intent = result.intent;
    parameters = result.parameters;
    console.log(`[llm-router] userId=${userId} intent=${intent} confidence=${result.confidence}`);
  } catch (err) {
    console.error("[llm-router] Classification failed:", err);
    const errorPayload: AppResultEvent = {
      userId,
      type: "chat",
      result: "Sorry, I couldn't understand your request. Please try again.",
      success: false,
      error: String(err),
      timestamp,
    };
    await sendMessage(producer, TOPICS.APP_RESULTS, userId, errorPayload);
    return;
  }

  // Math is owned by cot-math-service, but handle word-form expressions it may miss
  if (intent === "calculateMath") {
    const expression = parameters.expression as string | undefined;
    if (!expression) {
      const match = input.match(/(\d+)\s+(multiplied by|times|multiply)\s+(\d+)/i);
      if (match) {
        const mathPayload: IntentMathEvent = {
          userId,
          expression: `${match[1]} * ${match[3]}`,
          timestamp,
        };
        console.log(`[llm-router] Word-form math fallback: "${mathPayload.expression}"`);
        await sendMessage(producer, TOPICS.INTENT_MATH, userId, mathPayload);
        return;
      }
    }
    console.log(`[llm-router] Skipping math intent — delegated to cot-math-service`);
    return;
  }

  try {
    // Re-extract with intent-specific prompt for more accurate parameter parsing
    parameters = await extract(intent, input);
  } catch {
    // Fall back to parameters from classification step
  }

  switch (intent) {
    case "getWeather": {
      const city = (parameters.city as string) ?? "Tel Aviv";
      const payload: IntentWeatherEvent = { userId, city, timestamp };
      await sendMessage(producer, TOPICS.INTENT_WEATHER, userId, payload);
      break;
    }
    case "currencyExchange": {
      const rawFrom = (parameters.from as string) ?? (parameters.currencyCode as string);
      const rawTo   = (parameters.to   as string) ?? (parameters.targetCurrency as string);
      // Fallback: scan input text for a currency keyword when LLM omitted "from"
      let resolvedFrom = normalizeCurrency(rawFrom);
      if (!resolvedFrom) {
        const inputLower = input.toLowerCase();
        for (const [alias, code] of Object.entries(CURRENCY_ALIASES)) {
          if (new RegExp(`\\b${alias}\\b`).test(inputLower)) {
            resolvedFrom = code;
            break;
          }
        }
      }
      const currencyCode   = resolvedFrom ?? "USD";
      const targetCurrency = normalizeCurrency(rawTo)    ?? "ILS";
      const amount =
        typeof parameters.amount === "number" ? parameters.amount : undefined;
      const payload: IntentExchangeEvent = { userId, currencyCode, targetCurrency, amount, timestamp };
      await sendMessage(producer, TOPICS.INTENT_EXCHANGE, userId, payload);
      break;
    }
    case "generalChat":
    default: {
      const payload: IntentGeneralChatEvent = {
        userId,
        userInput: input,
        context: event.context ?? [],   // forward history from router cache
        timestamp,
      };
      await sendMessage(producer, TOPICS.INTENT_CHAT, userId, payload);
      break;
    }
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("llm-router-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(consumer, [TOPICS.ROUTER_DECISION], async (_topic, _key, value) => {
  const event = value as RouterDecisionEvent;
  await route(producer, event);
});

console.log("[llm-router] LLMRouterService started.");
