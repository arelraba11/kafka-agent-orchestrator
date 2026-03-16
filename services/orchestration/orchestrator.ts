import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { PlanGenerated } from "../../shared/schemas/PlanGenerated";
import type { ToolInvocationRequested } from "../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../shared/schemas/ToolInvocationResulted";
import type { PlanCompleted } from "../../shared/schemas/PlanCompleted";
import {
  initializeStore,
  getPlan,
  savePlan,
  updatePlan,
  deletePlan,
} from "../../shared/state/planStore";

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function dispatchStep(
  producer: Awaited<ReturnType<typeof createProducer>>,
  conversationId: string,
  toolName: string
): Promise<void> {
  const event: ToolInvocationRequested = {
    conversationId,
    timestamp: Date.now(),
    eventType: "ToolInvocationRequested",
    payload: { toolName, input: {} },
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
  const { steps } = payload;

  await savePlan(conversationId, { plan: steps, stepIndex: 0, results: [], status: "in_progress", planReceivedAt: Date.now() });
  console.log(`[orchestrator] conversationId=${conversationId} plan received steps=[${steps.join(", ")}]`);

  await dispatchStep(producer, conversationId, steps[0]);
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
    // More steps to execute
    await dispatchStep(producer, conversationId, state.plan[updatedStepIndex]);
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
