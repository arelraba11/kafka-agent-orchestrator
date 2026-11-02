import { TOPICS, type TopicName } from "../../../shared/topics";
import type { PlanState } from "../../../shared/state/planStore";
import type { StepResultEntry } from "../../../shared/schemas/common";
import type { PlanGenerated, PlanStep } from "../../../shared/schemas/PlanGenerated";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";
import type { PlanCompleted } from "../../../shared/schemas/PlanCompleted";
import type { PlanFailed } from "../../../shared/schemas/PlanFailed";
import type { TopicMessage } from "../../../shared/schemas/index";

// Pure plan state machine: no Kafka, no LevelDB. The orchestrator shell loads the
// state, calls transition(), emits the result's messages, then persists the new state.

export type OrchestratorInput = PlanGenerated | ToolInvocationResulted;

/** A message to publish; the Kafka key is derived from it by publish(). */
export type OutboundMessage = { [T in TopicName]: { topic: T; message: TopicMessage<T> } }[TopicName];

export type IgnoreReason =
  | "unknown-plan"
  | "duplicate-plan"
  | "plan-terminal"
  | "missing-step-index"
  | "duplicate-step"
  | "future-step"
  | "tool-mismatch";

export type Decision =
  | { kind: "started" }
  | { kind: "advanced"; completedStep: number }
  | { kind: "completed" }
  | { kind: "failed"; reason: string; failedTool: string }
  | { kind: "ignored"; reason: IgnoreReason; detail: string };

export interface TransitionResult {
  state: PlanState | null;
  emit: OutboundMessage[];
  decision: Decision;
}

/**
 * Resolves {{step_N.result}} placeholders in step.args string values.
 * N is the 0-indexed step number; replacement uses result.value if present,
 * otherwise the full JSON-stringified result object.
 */
export function resolvePlaceholders(
  args: Record<string, unknown>,
  results: ReadonlyArray<{ result?: Record<string, unknown> }>
): Record<string, unknown> {
  const resolved: Record<string, unknown> = {};

  for (const [key, val] of Object.entries(args)) {
    if (typeof val === "string") {
      resolved[key] = val.replace(/\{\{step_(\d+)\.result\}\}/g, (_match, nStr) => {
        const n = parseInt(nStr, 10);
        const entry = results[n];
        if (!entry) return "";
        const result = entry.result;
        if (!result) return "";
        // Standard tools return { value }; RAG returns { context }
        if (result.value !== undefined) return String(result.value);
        if (result.context !== undefined) return String(result.context);
        return JSON.stringify(result);
      });
    } else {
      resolved[key] = val;
    }
  }

  return resolved;
}

function ignored(state: PlanState | null, reason: IgnoreReason, detail: string): TransitionResult {
  return { state, emit: [], decision: { kind: "ignored", reason, detail } };
}

function requestMessage(
  conversationId: string,
  step: PlanStep,
  stepIndex: number,
  results: StepResultEntry[],
  now: number
): OutboundMessage {
  const value: ToolInvocationRequested = {
    conversationId,
    timestamp: now,
    eventType: "ToolInvocationRequested",
    payload: {
      toolName: step.tool,
      stepIndex,
      input: resolvePlaceholders(step.args, results),
    },
  };
  return { topic: TOPICS.TOOL_INVOCATION_REQUESTS, message: value };
}

function failureMessages(
  conversationId: string,
  code: PlanFailed["payload"]["code"],
  reason: string,
  failedTool: string,
  completedResults: StepResultEntry[],
  now: number
): OutboundMessage[] {
  const planFailed: PlanFailed = {
    conversationId,
    timestamp: now,
    eventType: "PlanFailed",
    payload: { code, reason, failedTool, completedResults },
  };
  // A failed plan is a domain event; the dead-letter queue holds poison messages only.
  return [{ topic: TOPICS.CONVERSATION_EVENTS, message: planFailed }];
}

function onPlanGenerated(
  state: PlanState | null,
  event: PlanGenerated,
  now: number
): TransitionResult {
  const { conversationId, sessionId, payload } = event;
  const steps = payload.steps;

  if (state) {
    return ignored(state, "duplicate-plan", `plan already exists with status=${state.status}`);
  }

  if (steps.length === 0) {
    const reason = "plan has no steps";
    return {
      state: {
        plan: [],
        stepIndex: 0,
        results: [],
        status: "failed",
        planReceivedAt: now,
        sessionId,
      },
      emit: failureMessages(conversationId, "empty-plan", reason, "none", [], now),
      decision: { kind: "failed", reason, failedTool: "none" },
    };
  }

  return {
    state: {
      plan: steps,
      stepIndex: 0,
      results: [],
      status: "running",
      planReceivedAt: now,
      sessionId,
    },
    emit: [requestMessage(conversationId, steps[0], 0, [], now)],
    decision: { kind: "started" },
  };
}

function onToolInvocationResulted(
  state: PlanState | null,
  event: ToolInvocationResulted,
  now: number
): TransitionResult {
  const { conversationId, payload } = event;

  if (!state) {
    return ignored(null, "unknown-plan", "no plan stored for this conversation");
  }
  if (state.status !== "running") {
    return ignored(state, "plan-terminal", `plan status=${state.status}`);
  }

  const stepIndex = payload.stepIndex;
  if (!Number.isInteger(stepIndex)) {
    return ignored(state, "missing-step-index", `stepIndex=${String(stepIndex)}`);
  }
  if (stepIndex < state.stepIndex) {
    return ignored(state, "duplicate-step", `stepIndex=${stepIndex} awaiting=${state.stepIndex}`);
  }
  if (stepIndex > state.stepIndex) {
    return ignored(state, "future-step", `stepIndex=${stepIndex} awaiting=${state.stepIndex}`);
  }

  const step = state.plan[stepIndex];
  if (payload.toolName !== step.tool) {
    return ignored(
      state,
      "tool-mismatch",
      `stepIndex=${stepIndex} expected=${step.tool} got=${payload.toolName}`
    );
  }

  const result = payload.result as { error?: unknown } | undefined;
  if (result && typeof result.error === "string") {
    const reason = result.error;
    return {
      state: { ...state, status: "failed" },
      emit: failureMessages(conversationId, "tool-error", reason, payload.toolName, state.results, now),
      decision: { kind: "failed", reason, failedTool: payload.toolName },
    };
  }

  const results = [...state.results, { toolName: payload.toolName, result: payload.result }];
  const nextIndex = stepIndex + 1;

  if (nextIndex < state.plan.length) {
    return {
      state: { ...state, results, stepIndex: nextIndex, status: "running" },
      emit: [requestMessage(conversationId, state.plan[nextIndex], nextIndex, results, now)],
      decision: { kind: "advanced", completedStep: stepIndex },
    };
  }

  const planCompleted: PlanCompleted = {
    conversationId,
    sessionId: state.sessionId,
    timestamp: now,
    eventType: "PlanCompleted",
    payload: { results },
  };
  return {
    state: { ...state, results, stepIndex: nextIndex, status: "completed" },
    emit: [{ topic: TOPICS.CONVERSATION_EVENTS, message: planCompleted }],
    decision: { kind: "completed" },
  };
}

/**
 * Deterministic for a given (state, event, now). When the decision is "ignored" the
 * input state is returned unchanged and nothing is emitted.
 */
export function transition(
  state: PlanState | null,
  event: OrchestratorInput,
  now: number
): TransitionResult {
  switch (event.eventType) {
    case "PlanGenerated":
      return onPlanGenerated(state, event, now);
    case "ToolInvocationResulted":
      return onToolInvocationResulted(state, event, now);
  }
}
