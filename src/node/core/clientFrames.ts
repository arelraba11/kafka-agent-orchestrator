import type { ConversationFailed } from "../../../shared/schemas/ConversationFailed";
import type { PlanFailed } from "../../../shared/schemas/PlanFailed";

// Side-effect-free: maps a failure event to the WebSocket frame the UI renders.
// The frame keeps the EXISTING error shape ({type:"error", reason, failedTool?}); the UI
// shows `reason`. conversationId and code are extra fields the current UI ignores.
//
// `reason` is user text derived from the failure code, never the operator-facing
// reason in the event, with one exception: a tool's own error message is shown for
// tool-error ("The math tool failed: Division by zero").

export interface ErrorFrame {
  type: "error";
  reason: string;
  failedTool?: string;
  conversationId: string;
  code: PlanFailed["payload"]["code"] | ConversationFailed["payload"]["code"] | "unavailable";
}

export interface ToolFrame {
  type: "tool";
  toolName: string;
  conversationId: string;
}

export interface AnswerFrame {
  type: "answer";
  answer: string;
  conversationId: string;
}

export const toolFrame = (conversationId: string, toolName: string): ToolFrame => ({
  type: "tool",
  toolName,
  conversationId,
});

export const answerFrame = (conversationId: string, answer: string): AnswerFrame => ({
  type: "answer",
  answer,
  conversationId,
});

/** The web server could not publish the query (Kafka down): no pipeline will answer. */
export const unavailableFrame = (conversationId: string): ErrorFrame => ({
  type: "error",
  reason: "The service is unavailable right now. Try again.",
  conversationId,
  code: "unavailable",
});

export interface FrameConfig {
  /** The orchestrator's STEP_TIMEOUT_MS, to word step timeouts ("within 30 s"). */
  stepTimeoutMs: number;
}

const ref = (conversationId: string) => conversationId.slice(0, 8);

function conversationMessage(event: ConversationFailed): string {
  const { stage, code } = event.payload;
  switch (code) {
    case "llm-unavailable":
      return "The language model is unavailable right now. Try again.";
    case "llm-misconfigured":
      return "The assistant's language model is misconfigured. Check the server logs.";
    case "empty-answer":
      return "The language model returned an empty answer. Try again.";
    case "missing-question":
      return `Something went wrong while answering (ref ${ref(event.conversationId)}).`;
    case "internal-error":
      switch (stage) {
        case "routing":
          return `Something went wrong while planning your request (ref ${ref(event.conversationId)}).`;
        case "orchestration":
          return `Something went wrong while processing your request (ref ${ref(event.conversationId)}).`;
        case "aggregation":
        case "synthesis":
          return `Something went wrong while answering (ref ${ref(event.conversationId)}).`;
      }
  }
}

export function toClientFrame(event: PlanFailed | ConversationFailed, config: FrameConfig): ErrorFrame {
  if (event.eventType === "ConversationFailed") {
    return {
      type: "error",
      reason: conversationMessage(event),
      conversationId: event.conversationId,
      code: event.payload.code,
    };
  }

  const { code, failedTool, reason } = event.payload;
  switch (code) {
    case "tool-error":
      return {
        type: "error",
        reason: `The ${failedTool} tool failed: ${reason}`,
        failedTool,
        conversationId: event.conversationId,
        code,
      };
    case "step-timeout":
      return {
        type: "error",
        reason: `The ${failedTool} tool didn't respond within ${Math.round(config.stepTimeoutMs / 1000)} s.`,
        failedTool,
        conversationId: event.conversationId,
        code,
      };
    case "empty-plan":
      return {
        type: "error",
        reason: "I couldn't turn that into a plan. Try rephrasing your request.",
        failedTool,
        conversationId: event.conversationId,
        code,
      };
  }
}
