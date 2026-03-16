import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { PlanGenerated } from "../../../shared/schemas/PlanGenerated";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";
import type { PlanCompleted } from "../../../shared/schemas/PlanCompleted";
import {
  initializeStore,
  getPlan,
  savePlan,
  updatePlan,
  deletePlan,
} from "../../../shared/state/planStore";

// ─── Per-tool input extraction ────────────────────────────────────────────────
// Extracts the relevant parameters from the raw user query for each tool type.
// Workers already have fallback defaults; these provide the actual user intent.

const EXTRACT_MATH_EXPR    = /\d+[\s\d+\-*/().]+/;
const EXTRACT_CITY         = /\bin\s+([A-Za-z][A-Za-z\s]*?)(?=\s+and\b|\?|$)/i;
const EXTRACT_CURRENCIES   = /\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/gi;
const EXTRACT_AMOUNT       = /\b(\d+(?:\.\d+)?)\s+(?:USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i;

function buildToolInput(toolName: string, userInput: string): Record<string, unknown> {
  switch (toolName) {
    case "math": {
      const match = userInput.match(EXTRACT_MATH_EXPR);
      const expression = match ? match[0].trim() : userInput;
      console.log(`[orchestrator] math expression extracted: "${expression}"`);
      return { expression };
    }
    case "weather": {
      const match = userInput.match(EXTRACT_CITY);
      const city = match ? match[1].trim() : "Tel Aviv";
      return { city };
    }
    case "exchange": {
      const currencies = Array.from(userInput.matchAll(EXTRACT_CURRENCIES)).map(m => m[0].toUpperCase());
      const amountMatch = userInput.match(EXTRACT_AMOUNT);
      return {
        currencyCode:   currencies[0] ?? "USD",
        targetCurrency: currencies[1] ?? "ILS",
        amount: amountMatch ? parseFloat(amountMatch[1]) : 1,
      };
    }
    case "chat":
      return { userInput };
    default:
      return {};
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function dispatchStep(
  producer: Awaited<ReturnType<typeof createProducer>>,
  conversationId: string,
  toolName: string,
  userInput: string
): Promise<void> {
  const event: ToolInvocationRequested = {
    conversationId,
    timestamp: Date.now(),
    eventType: "ToolInvocationRequested",
    payload: { toolName, input: buildToolInput(toolName, userInput) },
  };
  await sendMessage(producer, TOPICS.TOOL_INVOCATION_REQUESTS, conversationId, event);
  console.log(`[orchestrator] conversationId=${conversationId} dispatching tool="${toolName}"`);
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async function onPlanGenerated(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: PlanGenerated
): Promise<void> {
  const { conversationId, payload } = event;
  const { steps, userInput = "" } = payload;

  await savePlan(conversationId, {
    plan: steps,
    stepIndex: 0,
    results: [],
    status: "in_progress",
    planReceivedAt: Date.now(),
    userInput,
  });
  console.log(`[orchestrator] conversationId=${conversationId} plan received steps=[${steps.join(", ")}]`);

  await dispatchStep(producer, conversationId, steps[0], userInput);
}

async function onToolInvocationResulted(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: ToolInvocationResulted
): Promise<void> {
  const { conversationId, payload } = event;
  const state = await getPlan(conversationId);

  if (!state) {
    console.warn(`[orchestrator] Received result for unknown conversationId=${conversationId}, skipping.`);
    return;
  }

  const updatedResults = [...state.results, { toolName: payload.toolName, result: payload.result }];
  const updatedStepIndex = state.stepIndex + 1;

  await updatePlan(conversationId, { results: updatedResults, stepIndex: updatedStepIndex });

  console.log(`[orchestrator] conversationId=${conversationId} step ${updatedStepIndex}/${state.plan.length} completed tool="${payload.toolName}"`);

  if (updatedStepIndex < state.plan.length) {
    // More steps to execute — pass userInput so next tool gets the right params
    await dispatchStep(producer, conversationId, state.plan[updatedStepIndex], state.userInput ?? "");
    return;
  }

  // All steps done — emit PlanCompleted
  const planCompleted: PlanCompleted = {
    conversationId,
    timestamp: Date.now(),
    eventType: "PlanCompleted",
    payload: { results: updatedResults },
  };

  await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, planCompleted);

  const workerLatency = Date.now() - state.planReceivedAt;
  console.log(`[orchestrator] conversationId=${conversationId} plan completed, results=${updatedResults.length}`);
  console.log(`[Benchmark] conversationId=${conversationId} workerLatency=${workerLatency}ms`);

  await deletePlan(conversationId);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

await initializeStore();
console.log("[orchestrator] PlanStore initialized");

const producer = await createProducer();
const consumer = await createConsumer("orchestrator-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as { eventType: string };

    switch (event.eventType) {
      case "PlanGenerated":
        await onPlanGenerated(producer, event as PlanGenerated);
        break;
      case "ToolInvocationResulted":
        await onToolInvocationResulted(producer, event as ToolInvocationResulted);
        break;
      default:
        // Ignore other event types (PlanCompleted, FinalAnswerSynthesized, etc.)
        break;
    }
  }
);

console.log("[orchestrator] OrchestratorService started.");
