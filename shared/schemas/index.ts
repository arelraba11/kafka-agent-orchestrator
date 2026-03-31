export type { UserQueryReceived, UserQueryReceivedPayload } from "./UserQueryReceived";
export type { PlanGenerated, PlanGeneratedPayload, PlanStep } from "./PlanGenerated";
export type { ToolInvocationRequested, ToolInvocationRequestedPayload } from "./ToolInvocationRequested";
export type { ToolInvocationResulted, ToolInvocationResultedPayload } from "./ToolInvocationResulted";
export type { PlanCompleted, PlanCompletedPayload } from "./PlanCompleted";
export type { PlanFailed } from "./PlanFailed";
export type { SynthesizeFinalAnswerRequested } from "./SynthesizeFinalAnswerRequested";
export type { FinalAnswerSynthesized, FinalAnswerSynthesizedPayload } from "./FinalAnswerSynthesized";

// ─── Discriminated union types ────────────────────────────────────────────────

import type { PlanGenerated } from "./PlanGenerated";
import type { ToolInvocationResulted } from "./ToolInvocationResulted";
import type { PlanCompleted } from "./PlanCompleted";
import type { PlanFailed } from "./PlanFailed";
import type { FinalAnswerSynthesized } from "./FinalAnswerSynthesized";
import type { UserQueryReceived } from "./UserQueryReceived";
import type { SynthesizeFinalAnswerRequested } from "./SynthesizeFinalAnswerRequested";

export type ConversationEvent =
  | PlanGenerated
  | ToolInvocationResulted
  | PlanCompleted
  | PlanFailed
  | FinalAnswerSynthesized;

export type UserCommand =
  | UserQueryReceived
  | SynthesizeFinalAnswerRequested;
