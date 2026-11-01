import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { PlanGenerated, PlanStep } from "../../../shared/schemas/PlanGenerated";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";
import type { PlanCompleted } from "../../../shared/schemas/PlanCompleted";
import type { PlanFailed } from "../../../shared/schemas/PlanFailed";
import {
  initializeStore,
  getPlan,
  savePlan,
  updatePlan,
  deletePlan,
} from "../../../shared/state/planStore";
import { resolvePlaceholders } from "./planTransition";

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function dispatchStep(
  producer: Awaited<ReturnType<typeof createProducer>>,
  conversationId: string,
  step: PlanStep,
  stepIndex: number,
  accumulatedResults: Record<string, unknown>[]
): Promise<void> {
  const resolvedArgs = resolvePlaceholders(step.args, accumulatedResults);

  const event: ToolInvocationRequested = {
    conversationId,
    timestamp: Date.now(),
    eventType: "ToolInvocationRequested",
    payload: { toolName: step.tool, stepIndex, input: resolvedArgs },
  };

  await sendMessage(producer, TOPICS.TOOL_INVOCATION_REQUESTS, conversationId, event);
  console.log(`[orchestrator] conversationId=${conversationId} dispatching tool="${step.tool}"`);
}

async function emitPlanFailed(
  producer: Awaited<ReturnType<typeof createProducer>>,
  conversationId: string,
  reason: string,
  failedTool: string,
  completedResults: Record<string, unknown>[]
): Promise<void> {
  const now = Date.now();

  const planFailed: PlanFailed = {
    conversationId,
    timestamp: now,
    eventType: "PlanFailed",
    payload: { reason, failedTool, completedResults },
  };
  await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, planFailed);

  await sendMessage(producer, TOPICS.DEAD_LETTER_QUEUE, conversationId, {
    conversationId,
    timestamp: now,
    source: "orchestrator",
    reason,
    failedTool,
  });

  console.log(`[orchestrator] conversationId=${conversationId} plan FAILED reason=${reason}`);
  console.log(`[orchestrator] conversationId=${conversationId} dead-letter published`);
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async function onPlanGenerated(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: PlanGenerated
): Promise<void> {
  const { conversationId, payload } = event;
  const { steps } = payload;

  await savePlan(conversationId, {
    plan: steps,
    stepIndex: 0,
    results: [],
    status: "running",
    planReceivedAt: Date.now(),
    sessionId: event.sessionId,
  });

  console.log(`[orchestrator] conversationId=${conversationId} plan received steps=[${steps.map(s => s.tool).join(", ")}]`);

  try {
    await dispatchStep(producer, conversationId, steps[0], 0, []);
    await updatePlan(conversationId, { status: "running" });
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    await updatePlan(conversationId, { status: "failed" });
    await emitPlanFailed(producer, conversationId, reason, steps[0].tool, []);
    await deletePlan(conversationId);
  }
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

  const resultPayload = payload.result as { error?: string } | undefined;
  if (resultPayload && typeof resultPayload.error === "string") {
    const reason = resultPayload.error;
    const completedResults = [...state.results];
    await updatePlan(conversationId, { status: "failed" });
    await emitPlanFailed(producer, conversationId, reason, payload.toolName, completedResults);
    await deletePlan(conversationId);
    return;
  }

  const updatedResults = [...state.results, { toolName: payload.toolName, result: payload.result }];
  const updatedStepIndex = state.stepIndex + 1;

  await updatePlan(conversationId, { results: updatedResults, stepIndex: updatedStepIndex });

  console.log(`[orchestrator] conversationId=${conversationId} step ${updatedStepIndex}/${state.plan.length} completed tool="${payload.toolName}"`);

  if (updatedStepIndex < state.plan.length) {
    try {
      await dispatchStep(producer, conversationId, state.plan[updatedStepIndex], updatedStepIndex, updatedResults);
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      const nextTool = state.plan[updatedStepIndex].tool;
      await updatePlan(conversationId, { status: "failed" });
      await emitPlanFailed(producer, conversationId, reason, nextTool, updatedResults);
      await deletePlan(conversationId);
    }
    return;
  }

  await updatePlan(conversationId, { status: "completed" });

  const planCompleted: PlanCompleted = {
    conversationId,
    sessionId: state.sessionId,
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
        break;
    }
  }
);

console.log("[orchestrator] OrchestratorService started.");
