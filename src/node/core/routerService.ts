import {
  createProducer,
  createConsumer,
  publish,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { ToolName } from "../../../shared/schemas/common";
import type { PlanGenerated, PlanStep } from "../../../shared/schemas/PlanGenerated";
import { callLLM } from "../../../shared/llm/callLLM";
import { routerBudgetMs } from "../../../shared/llm/budgets";
import { routerPlanPrompt } from "../../../shared/prompts/routerPlanPrompt";
import { getHistory } from "../../../shared/state/historyStore";
import { conversationFailedHook } from "../../../shared/kafka/conversationFailed";
import { FAILURE_OWNERSHIP } from "../../../shared/kafka/failureOwnership";

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
    const match = userInput.match(/[(\d][\d\s+\-*/().]+/);
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

const VALID_TOOLS = new Set<string>(ToolName.options);

// One attempt within 15 s: a slow or failing LLM must not hold the partition. Any
// LlmError (or a bad plan) falls back to the regex planner.
const ROUTER_LLM = { budgetMs: routerBudgetMs(), maxAttempts: 1 } as const;

async function generatePlanLLM(userInput: string, sessionId: string): Promise<PlanStep[]> {
  try {
    const history = await getHistory(sessionId);
    const raw = await callLLM(routerPlanPrompt(userInput, history), ROUTER_LLM);
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

await subscribeAndRun({
  consumer,
  groupId: "router-plan-service",
  service: "router",
  producer,
  topic: TOPICS.USER_COMMANDS,
  types: ["UserQueryReceived"],
  // A poison UserQueryReceived (or an outbound-violating PlanGenerated built from one)
  // is dead-lettered; tell the user their request could not be planned.
  onDeadLettered: conversationFailedHook(producer, FAILURE_OWNERSHIP.router),
  handler: async (event) => {
    const { conversationId, payload } = event;
    const sessionId = payload.sessionId;

    // generatePlanLLM falls back to the regex planner, so the only realistic failure
    // here is the send. That is transient: let it throw and the message is redelivered.
    const steps = await generatePlanLLM(payload.userInput, sessionId);

    const planEvent: PlanGenerated = {
      conversationId,
      sessionId,
      timestamp: Date.now(),
      eventType: "PlanGenerated",
      payload: { steps },
    };

    await publish(producer, TOPICS.CONVERSATION_EVENTS, planEvent);

    const routerLatency = Date.now() - event.timestamp;
    console.log(`[router] conversationId=${conversationId} plan=[${steps.map(s => s.tool).join(", ")}] input="${payload.userInput}"`);
    console.log(`[Benchmark] conversationId=${conversationId} routerLatency=${routerLatency}ms`);
  },
});

console.log("[router] RouterService started.");
