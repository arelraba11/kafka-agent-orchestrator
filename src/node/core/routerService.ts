import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { PlanGenerated, PlanStep } from "../../../shared/schemas/PlanGenerated";
import { callLLM } from "../../../shared/llm/openai";
import { routerPlanPrompt } from "../../../shared/prompts/routerPlanPrompt";

// ─── Regex fallback planner ───────────────────────────────────────────────────

const WEATHER_REGEX  = /\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i;
const EXCHANGE_REGEX = /\b(exchange|convert|USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i;
const MATH_REGEX     = /\d+\s*[+\-*/]\s*\d+|\b(multiply|times|divided|plus|minus|calculate)\b.*\d/i;
const PRODUCT_REGEX  = /\b(iphone|macbook|mac book|tesla|model\s*[3sxy]|cybertruck|product|laptop|phone|smartphone|electric vehicle|ev)\b/i;
const CITY_REGEX     = /\bin\s+([A-Za-z\s]+?)(?:\?|$)/i;
const CURRENCIES_RE  = /\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/gi;
const AMOUNT_RE      = /\b(\d+(?:\.\d+)?)\s+(?:USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i;

function generatePlanRegex(userInput: string): PlanStep[] {
  const steps: PlanStep[] = [];

  if (WEATHER_REGEX.test(userInput)) {
    const cityMatch = userInput.match(CITY_REGEX);
    steps.push({ tool: "weather", args: { city: cityMatch ? cityMatch[1].trim() : "Tel Aviv" } });
  }

  if (EXCHANGE_REGEX.test(userInput)) {
    const currencies = Array.from(userInput.matchAll(CURRENCIES_RE)).map(m => m[0].toUpperCase());
    const amountMatch = userInput.match(AMOUNT_RE);
    steps.push({
      tool: "exchange",
      args: {
        currencyCode:   currencies[0] ?? "USD",
        targetCurrency: currencies[1] ?? "ILS",
        amount: amountMatch ? parseFloat(amountMatch[1]) : 1,
      },
    });
  }

  if (MATH_REGEX.test(userInput)) {
    const match = userInput.match(/[\d]+[\s\d+\-*/().]+/);
    steps.push({ tool: "math", args: { expression: match ? match[0].trim() : userInput } });
  }

  if (PRODUCT_REGEX.test(userInput)) {
    steps.push({ tool: "getProductInformation", args: { query: userInput } });
  }

  if (steps.length === 0) {
    steps.push({ tool: "chat", args: { userInput } });
  }

  return steps;
}

// ─── LLM plan generation ──────────────────────────────────────────────────────

const VALID_TOOLS = new Set(["weather", "exchange", "math", "chat", "getProductInformation"]);

async function generatePlanLLM(userInput: string): Promise<PlanStep[]> {
  try {
    const raw = await callLLM(routerPlanPrompt(userInput));
    const parsed = JSON.parse(raw);

    if (
      !parsed ||
      !Array.isArray(parsed.steps) ||
      parsed.steps.length === 0 ||
      !parsed.steps.every(
        (s: unknown) =>
          typeof s === "object" &&
          s !== null &&
          typeof (s as Record<string, unknown>).tool === "string" &&
          VALID_TOOLS.has((s as Record<string, unknown>).tool as string) &&
          typeof (s as Record<string, unknown>).args === "object" &&
          (s as Record<string, unknown>).args !== null
      )
    ) {
      throw new Error("Invalid plan shape");
    }

    const steps: PlanStep[] = parsed.steps;
    console.log(`[router] mode=llm steps=[${steps.map(s => s.tool).join(", ")}]`);
    return steps;
  } catch (err) {
    console.log(`[router] mode=regex-fallback reason="${(err as Error).message}"`);
    return generatePlanRegex(userInput);
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("router-plan-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.USER_COMMANDS],
  async (_topic, _key, value) => {
    const event = value as UserQueryReceived;
    if (event.eventType !== "UserQueryReceived") return;

    const { conversationId, payload } = event;
    const steps = await generatePlanLLM(payload.userInput);

    const planEvent: PlanGenerated = {
      conversationId,
      timestamp: Date.now(),
      eventType: "PlanGenerated",
      payload: { steps },
    };

    await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, planEvent);

    const routerLatency = Date.now() - event.timestamp;
    console.log(`[router] conversationId=${conversationId} plan=[${steps.map(s => s.tool).join(", ")}] input="${payload.userInput}"`);
    console.log(`[Benchmark] conversationId=${conversationId} routerLatency=${routerLatency}ms`);
  }
);

console.log("[router] RouterService started.");
