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
  IntentWeatherEvent,
  IntentExchangeEvent,
  IntentGeneralChatEvent,
  AppResultEvent,
} from "../../shared/types/events";

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

  // Math is owned by cot-math-service
  if (intent === "calculateMath") {
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
      const currencyCode =
        (parameters.from as string) ?? (parameters.currencyCode as string) ?? "USD";
      const targetCurrency =
        (parameters.to as string) ?? (parameters.targetCurrency as string) ?? "ILS";
      const payload: IntentExchangeEvent = { userId, currencyCode, targetCurrency, timestamp };
      await sendMessage(producer, TOPICS.INTENT_EXCHANGE, userId, payload);
      break;
    }
    case "generalChat":
    default: {
      const payload: IntentGeneralChatEvent = { userId, userInput: input, context: [], timestamp };
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
