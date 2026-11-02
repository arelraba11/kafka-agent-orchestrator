import { describe, expect, test } from "bun:test";
import type { ConversationFailed } from "../../../shared/schemas/ConversationFailed";
import type { PlanFailed } from "../../../shared/schemas/PlanFailed";
import { toClientFrame } from "./clientFrames";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const CONFIG = { stepTimeoutMs: 30_000 };

const planFailed = (code: PlanFailed["payload"]["code"], failedTool: string, reason: string): PlanFailed => ({
  conversationId: CID,
  timestamp: 1,
  eventType: "PlanFailed",
  payload: { code, reason, failedTool, completedResults: [] },
});

const convFailed = (
  stage: ConversationFailed["payload"]["stage"],
  code: ConversationFailed["payload"]["code"],
  reason = "SECRET operator detail"
): ConversationFailed => ({
  conversationId: CID,
  timestamp: 1,
  eventType: "ConversationFailed",
  payload: { stage, code, reason },
});

describe("PlanFailed → existing error frame", () => {
  test("tool-error keeps the tool's own reason", () => {
    expect(toClientFrame(planFailed("tool-error", "math", "Division by zero"), CONFIG)).toEqual({
      type: "error",
      reason: "The math tool failed: Division by zero",
      failedTool: "math",
      conversationId: CID,
      code: "tool-error",
    });
  });

  test("step-timeout names the tool and the configured seconds, never the raw reason", () => {
    const raw = "step 0 (weather) did not respond within 30s";
    const f = toClientFrame(planFailed("step-timeout", "weather", raw), CONFIG);
    expect(f).toEqual({
      type: "error",
      reason: "The weather tool didn't respond within 30 s.",
      failedTool: "weather",
      conversationId: CID,
      code: "step-timeout",
    });
    expect(f.reason).not.toContain(raw);
  });

  test("step-timeout words the configured STEP_TIMEOUT_MS", () => {
    const f = toClientFrame(planFailed("step-timeout", "math", "x"), { stepTimeoutMs: 10_000 });
    expect(f.reason).toBe("The math tool didn't respond within 10 s.");
  });

  test("empty-plan does not leak the raw reason", () => {
    const f = toClientFrame(planFailed("empty-plan", "none", "plan has no steps"), CONFIG);
    expect(f.code).toBe("empty-plan");
    expect(f.type).toBe("error");
    expect(f.reason).not.toContain("plan has no steps");
    expect(f.conversationId).toBe(CID);
  });
});

describe("ConversationFailed → existing error frame", () => {
  const cases: [ConversationFailed["payload"]["stage"], ConversationFailed["payload"]["code"], string][] = [
    ["synthesis", "llm-unavailable", "The language model is unavailable right now. Try again."],
    ["synthesis", "llm-misconfigured", "The assistant's language model is misconfigured. Check the server logs."],
    ["synthesis", "empty-answer", "The language model returned an empty answer. Try again."],
    ["synthesis", "missing-question", "Something went wrong while answering (ref 3f2b8c1e)."],
    ["routing", "internal-error", "Something went wrong while planning your request (ref 3f2b8c1e)."],
    ["orchestration", "internal-error", "Something went wrong while processing your request (ref 3f2b8c1e)."],
    ["aggregation", "internal-error", "Something went wrong while answering (ref 3f2b8c1e)."],
    ["synthesis", "internal-error", "Something went wrong while answering (ref 3f2b8c1e)."],
  ];

  for (const [stage, code, text] of cases) {
    test(`${stage}/${code}`, () => {
      expect(toClientFrame(convFailed(stage, code), CONFIG)).toEqual({
        type: "error",
        reason: text,
        conversationId: CID,
        code,
      });
    });
  }

  test("never leaks the operator-facing reason, and carries no failedTool", () => {
    for (const [stage, code] of cases) {
      const f = toClientFrame(convFailed(stage, code), CONFIG);
      expect(JSON.stringify(f)).not.toContain("SECRET");
      expect(f).not.toHaveProperty("failedTool");
    }
  });
});

describe("frame shape", () => {
  test("is JSON-serializable and keeps the fields the current UI reads", () => {
    const f = JSON.parse(JSON.stringify(toClientFrame(planFailed("tool-error", "math", "boom"), CONFIG)));
    expect(f.type).toBe("error");
    expect(typeof f.reason).toBe("string");
  });
});
