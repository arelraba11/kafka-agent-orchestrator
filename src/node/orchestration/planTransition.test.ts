import { describe, expect, test } from "bun:test";
import { TOPICS } from "../../../shared/topics";
import type { PlanState } from "../../../shared/state/planStore";
import type { PlanGenerated, PlanStep } from "../../../shared/schemas/PlanGenerated";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";
import type { StepTimedOut } from "../../../shared/schemas/StepTimedOut";
import { resolvePlaceholders, transition } from "./planTransition";

const NOW = 1_700_000_000_000;
const CONFIG = { stepTimeoutMs: 30_000 };
const ID = "conv-1";
const SESSION = "session-1";

function planGenerated(steps: PlanStep[]): PlanGenerated {
  return {
    conversationId: ID,
    sessionId: SESSION,
    timestamp: NOW - 1,
    eventType: "PlanGenerated",
    payload: { steps },
  };
}

function resulted(
  toolName: string,
  stepIndex: number | undefined,
  result: Record<string, unknown>
): ToolInvocationResulted {
  return {
    conversationId: ID,
    timestamp: NOW - 1,
    eventType: "ToolInvocationResulted",
    payload: { toolName, stepIndex, result } as ToolInvocationResulted["payload"],
  };
}

const WEATHER_MATH: PlanStep[] = [
  { tool: "weather", args: { city: "Tel Aviv" } },
  { tool: "math", args: { expression: "1+1" } },
];

/** Applies an event and asserts it was not ignored, returning the new state. */
function mustApply(state: PlanState | null, event: Parameters<typeof transition>[1]) {
  const r = transition(state, event, NOW, CONFIG);
  expect(r.decision.kind).not.toBe("ignored");
  return r;
}

describe("normal progression", () => {
  test("PlanGenerated -> result 0 -> result 1 walks the plan to completed", () => {
    const started = transition(null, planGenerated(WEATHER_MATH), NOW, CONFIG);
    expect(started.decision).toEqual({ kind: "started" });
    expect(started.state).toEqual({
      plan: WEATHER_MATH,
      stepIndex: 0,
      results: [],
      status: "running",
      stepDeadline: NOW + CONFIG.stepTimeoutMs,
      planReceivedAt: NOW,
      sessionId: SESSION,
    });
    expect(started.emit).toHaveLength(1);
    expect(started.emit[0].topic).toBe(TOPICS.TOOL_INVOCATION_REQUESTS);
    expect(started.emit[0].message).toMatchObject({
      conversationId: ID,
      eventType: "ToolInvocationRequested",
      payload: { toolName: "weather", stepIndex: 0, input: { city: "Tel Aviv" } },
    });

    const advanced = transition(
      started.state,
      resulted("weather", 0, { value: "sunny", success: true }),
      NOW, CONFIG
    );
    expect(advanced.decision).toEqual({ kind: "advanced", completedStep: 0 });
    expect(advanced.state!.stepIndex).toBe(1);
    expect(advanced.state!.status).toBe("running");
    expect(advanced.emit).toHaveLength(1);
    expect(advanced.emit[0].topic).toBe(TOPICS.TOOL_INVOCATION_REQUESTS);
    expect(advanced.emit[0].message).toMatchObject({
      eventType: "ToolInvocationRequested",
      payload: { toolName: "math", stepIndex: 1, input: { expression: "1+1" } },
    });

    const done = transition(
      advanced.state,
      resulted("math", 1, { value: "2", success: true }),
      NOW, CONFIG
    );
    expect(done.decision).toEqual({ kind: "completed" });
    expect(done.state!.status).toBe("completed");
    expect(done.state!.stepIndex).toBe(2);
    expect(done.emit).toHaveLength(1);
    expect(done.emit[0].topic).toBe(TOPICS.CONVERSATION_EVENTS);
    expect(done.emit[0].message).toMatchObject({
      conversationId: ID,
      sessionId: SESSION,
      eventType: "PlanCompleted",
    });
    const results = (done.emit[0].message as { payload: { results: unknown[] } }).payload.results;
    expect(results).toEqual([
      { toolName: "weather", result: { value: "sunny", success: true } },
      { toolName: "math", result: { value: "2", success: true } },
    ]);
  });
});

describe("duplicate result", () => {
  test("a second result for an already-applied step is ignored with no emit and no state change", () => {
    const s0 = mustApply(null, planGenerated(WEATHER_MATH)).state!;
    const r0 = resulted("weather", 0, { value: "sunny", success: true });
    const s1 = mustApply(s0, r0).state!;

    const dup = transition(s1, r0, NOW, CONFIG);
    expect(dup.decision.kind).toBe("ignored");
    expect((dup.decision as { reason: string }).reason).toBe("duplicate-step");
    expect(dup.emit).toEqual([]);
    expect(dup.state).toEqual(s1);
  });
});

describe("stale or out-of-order results", () => {
  const running = () => {
    const s0 = mustApply(null, planGenerated(WEATHER_MATH)).state!;
    return mustApply(s0, resulted("weather", 0, { value: "sunny" })).state!; // awaiting step 1
  };

  test("a result for a later step than the one awaited is future-step", () => {
    const s = running();
    const r = transition(s, resulted("math", 2, { value: "2" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "future-step" });
    expect(r.emit).toEqual([]);
    expect(r.state).toEqual(s);
  });

  test("a result whose toolName does not match the awaited step is tool-mismatch", () => {
    const s = running();
    const r = transition(s, resulted("weather", 1, { value: "x" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "tool-mismatch" });
    expect(r.emit).toEqual([]);
    expect(r.state).toEqual(s);
  });

  test("a result without stepIndex is missing-step-index", () => {
    const s = running();
    const r = transition(s, resulted("math", undefined, { value: "2" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "missing-step-index" });
    expect(r.emit).toEqual([]);
    expect(r.state).toEqual(s);
  });

  test("a non-integer stepIndex is missing-step-index", () => {
    const s = running();
    const r = transition(s, resulted("math", 1.5, { value: "2" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "missing-step-index" });
  });
});

describe("terminal and unknown plans", () => {
  const completed = (): PlanState => ({
    plan: [{ tool: "chat", args: { userInput: "hi" } }],
    stepIndex: 1,
    results: [{ toolName: "chat", result: { value: "hello" } }],
    status: "completed",
    stepDeadline: null,
    planReceivedAt: NOW - 10,
    sessionId: SESSION,
  });

  test("a result for a completed plan is plan-terminal", () => {
    const s = completed();
    const r = transition(s, resulted("chat", 0, { value: "hello" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "plan-terminal" });
    expect(r.emit).toEqual([]);
    expect(r.state).toEqual(s);
  });

  test("a result for a failed plan is plan-terminal", () => {
    const s: PlanState = { ...completed(), stepIndex: 0, results: [], status: "failed" };
    const r = transition(s, resulted("chat", 0, { value: "late" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "plan-terminal" });
    expect(r.emit).toEqual([]);
    expect(r.state).toEqual(s);
  });

  test("a result with no stored plan is unknown-plan", () => {
    const r = transition(null, resulted("chat", 0, { value: "hello" }), NOW, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "unknown-plan" });
    expect(r.emit).toEqual([]);
    expect(r.state).toBeNull();
  });

  test("PlanGenerated when a plan already exists is duplicate-plan, whatever its status", () => {
    for (const status of ["running", "completed", "failed"] as const) {
      const s: PlanState = { ...completed(), status };
      const r = transition(s, planGenerated(WEATHER_MATH), NOW, CONFIG);
      expect(r.decision).toMatchObject({ kind: "ignored", reason: "duplicate-plan" });
      expect(r.emit).toEqual([]);
      expect(r.state).toEqual(s);
    }
  });
});

describe("failed step", () => {
  test("an error result fails the plan, emitting only PlanFailed (no DLQ record)", () => {
    const plan: PlanStep[] = [
      { tool: "weather", args: { city: "Tel Aviv" } },
      { tool: "math", args: { expression: "2/0" } },
    ];
    const s0 = mustApply(null, planGenerated(plan)).state!;
    const s1 = mustApply(s0, resulted("weather", 0, { value: "sunny" })).state!;

    const r = transition(
      s1,
      resulted("math", 1, { value: "", success: false, error: "Division by zero" }),
      NOW, CONFIG
    );

    expect(r.decision).toEqual({
      kind: "failed",
      code: "tool-error",
      reason: "Division by zero",
      failedTool: "math",
    });
    expect(r.state!.status).toBe("failed");
    // Business failures are PlanFailed events, never dead letters.
    expect(r.emit).toHaveLength(1);

    const [failed] = r.emit;
    expect(failed.topic).toBe(TOPICS.CONVERSATION_EVENTS);
    expect(failed.message).toEqual({
      conversationId: ID,
      timestamp: NOW,
      eventType: "PlanFailed",
      payload: {
        code: "tool-error",
        reason: "Division by zero",
        failedTool: "math",
        completedResults: [{ toolName: "weather", result: { value: "sunny" } }],
      },
    });
  });
});

describe("empty plan", () => {
  test("PlanGenerated with no steps fails with a tombstone and PlanFailed(failedTool none)", () => {
    const r = transition(null, planGenerated([]), NOW, CONFIG);
    expect(r.decision).toEqual({
      kind: "failed",
      code: "empty-plan",
      reason: "plan has no steps",
      failedTool: "none",
    });
    expect(r.state).toMatchObject({ status: "failed", plan: [], stepIndex: 0, results: [] });
    expect(r.emit).toHaveLength(1);
    expect(r.emit[0].topic).toBe(TOPICS.CONVERSATION_EVENTS);
    expect(r.emit[0].message).toMatchObject({
      eventType: "PlanFailed",
      payload: { code: "empty-plan", reason: "plan has no steps", failedTool: "none", completedResults: [] },
    });
  });
});

describe("{{step_N.result}} placeholders", () => {
  test("a RAG context result is substituted into the next request", () => {
    const plan: PlanStep[] = [
      { tool: "getProductInformation", args: { query: "iphone price" } },
      { tool: "exchange", args: { amount: "{{step_0.result}}", currencyCode: "ILS" } },
    ];
    const s0 = mustApply(null, planGenerated(plan)).state!;
    const r = transition(
      s0,
      resulted("getProductInformation", 0, { context: "3700 ILS", success: true }),
      NOW, CONFIG
    );
    expect(r.decision.kind).toBe("advanced");
    expect(r.emit[0].message).toMatchObject({
      payload: { toolName: "exchange", stepIndex: 1, input: { amount: "3700 ILS" } },
    });
  });

  describe("resolvePlaceholders", () => {
    const results = [
      { toolName: "a", result: { value: "V", context: "C" } },
      { toolName: "b", result: { context: "only-context" } },
      { toolName: "c", result: { foo: 1 } },
    ];

    test("value wins over context", () => {
      expect(resolvePlaceholders({ x: "{{step_0.result}}" }, results)).toEqual({ x: "V" });
    });

    test("a RAG result with a price resolves to the price, not the context text", () => {
      const rag = [
        {
          toolName: "getProductInformation",
          result: { value: "1199", context: "[iphone]\niPhone 16 Pro Max:\nPrice: $1,199" },
        },
      ];
      expect(resolvePlaceholders({ amount: "{{step_0.result}}" }, rag)).toEqual({ amount: "1199" });
    });

    test("context is used when there is no value", () => {
      expect(resolvePlaceholders({ x: "{{step_1.result}}" }, results)).toEqual({
        x: "only-context",
      });
    });

    test("falls back to JSON of the result", () => {
      expect(resolvePlaceholders({ x: "{{step_2.result}}" }, results)).toEqual({
        x: '{"foo":1}',
      });
    });

    test("a missing step resolves to the empty string", () => {
      expect(resolvePlaceholders({ x: "{{step_9.result}}" }, results)).toEqual({ x: "" });
    });

    test("non-string args are unchanged", () => {
      expect(resolvePlaceholders({ n: 5, b: true, o: { k: "{{step_0.result}}" } }, results)).toEqual({
        n: 5,
        b: true,
        o: { k: "{{step_0.result}}" },
      });
    });

    test("several placeholders in one string are all replaced", () => {
      expect(
        resolvePlaceholders({ x: "{{step_0.result}} and {{step_1.result}}" }, results)
      ).toEqual({ x: "V and only-context" });
    });
  });
});

describe("crash replay", () => {
  test("window b: the same event applied twice to the same old state yields identical output", () => {
    const s0 = mustApply(null, planGenerated(WEATHER_MATH)).state!;
    const ev = resulted("weather", 0, { value: "sunny" });
    const first = transition(s0, ev, NOW, CONFIG);
    const second = transition(s0, ev, NOW, CONFIG);
    expect(second).toEqual(first);
    expect(first.emit.length).toBeGreaterThan(0);
  });

  test("window c: re-applying the event to the resulting state is ignored", () => {
    const s0 = mustApply(null, planGenerated(WEATHER_MATH)).state!;
    const ev = resulted("weather", 0, { value: "sunny" });
    const after = transition(s0, ev, NOW, CONFIG);
    const replay = transition(after.state, ev, NOW, CONFIG);
    expect(replay.decision).toMatchObject({ kind: "ignored", reason: "duplicate-step" });
    expect(replay.emit).toEqual([]);
    expect(replay.state).toEqual(after.state);
  });

  test("window c: a replayed PlanGenerated against the started state is duplicate-plan", () => {
    const ev = planGenerated(WEATHER_MATH);
    const started = transition(null, ev, NOW, CONFIG);
    const replay = transition(started.state, ev, NOW, CONFIG);
    expect(replay.decision).toMatchObject({ kind: "ignored", reason: "duplicate-plan" });
    expect(replay.emit).toEqual([]);
  });

  test("window c at completion: the last result replayed against the completed plan is plan-terminal", () => {
    const s0 = mustApply(null, planGenerated(WEATHER_MATH)).state!;
    const s1 = mustApply(s0, resulted("weather", 0, { value: "sunny" })).state!;
    const last = resulted("math", 1, { value: "2" });
    const done = mustApply(s1, last);
    const replay = transition(done.state, last, NOW, CONFIG);
    expect(replay.decision).toMatchObject({ kind: "ignored", reason: "plan-terminal" });
    expect(replay.emit).toEqual([]);
  });
});

// ─── Step timeouts ────────────────────────────────────────────────────────────

function timedOut(stepIndex: number, deadline: number): StepTimedOut {
  return {
    conversationId: ID,
    timestamp: NOW + 30_000,
    eventType: "StepTimedOut",
    payload: { stepIndex, deadline },
  };
}

describe("step deadlines", () => {
  const LATER = NOW + 5_000;

  test("dispatching the first step sets stepDeadline to now + stepTimeoutMs", () => {
    const r = transition(null, planGenerated(WEATHER_MATH), NOW, CONFIG);
    expect(r.state!.stepDeadline).toBe(NOW + 30_000);
  });

  test("advancing re-arms the deadline from the time of the advance", () => {
    const s0 = mustApply(null, planGenerated(WEATHER_MATH)).state!;
    const r = transition(s0, resulted("weather", 0, { value: "sunny" }), LATER, CONFIG);
    expect(r.decision.kind).toBe("advanced");
    expect(r.state!.stepDeadline).toBe(LATER + 30_000);
  });

  test("stepTimeoutMs comes from the config", () => {
    const r = transition(null, planGenerated(WEATHER_MATH), NOW, { stepTimeoutMs: 10_000 });
    expect(r.state!.stepDeadline).toBe(NOW + 10_000);
  });

  test("completed, tool-error and empty-plan states clear the deadline", () => {
    const one = mustApply(null, planGenerated([{ tool: "chat", args: { userInput: "hi" } }])).state!;
    expect(transition(one, resulted("chat", 0, { value: "x" }), NOW, CONFIG).state!.stepDeadline).toBeNull();
    expect(
      transition(one, resulted("chat", 0, { error: "boom" }), NOW, CONFIG).state!.stepDeadline
    ).toBeNull();
    expect(transition(null, planGenerated([]), NOW, CONFIG).state!.stepDeadline).toBeNull();
  });
});

describe("StepTimedOut", () => {
  const started = () => mustApply(null, planGenerated(WEATHER_MATH)).state!; // step 0, deadline NOW+30s
  const DEADLINE = NOW + 30_000;

  test("a matching timeout fails the plan with PlanFailed{step-timeout}", () => {
    const s = started();
    const r = transition(s, timedOut(0, DEADLINE), DEADLINE, CONFIG);
    expect(r.decision).toEqual({
      kind: "failed",
      code: "step-timeout",
      reason: "step 0 (weather) did not respond within 30s",
      failedTool: "weather",
    });
    expect(r.state).toMatchObject({ status: "failed", stepDeadline: null });
    expect(r.emit).toHaveLength(1);
    expect(r.emit[0].topic).toBe(TOPICS.CONVERSATION_EVENTS);
    expect(r.emit[0].message).toEqual({
      conversationId: ID,
      timestamp: DEADLINE,
      eventType: "PlanFailed",
      payload: {
        code: "step-timeout",
        reason: "step 0 (weather) did not respond within 30s",
        failedTool: "weather",
        completedResults: [],
      },
    });
  });

  test("the failure carries the results completed so far and names the stuck tool", () => {
    const s0 = started();
    const s1 = transition(s0, resulted("weather", 0, { value: "sunny" }), NOW + 1_000, CONFIG).state!;
    const r = transition(s1, timedOut(1, NOW + 31_000), NOW + 31_000, CONFIG);
    expect(r.decision).toMatchObject({ kind: "failed", code: "step-timeout", failedTool: "math" });
    expect(r.emit[0].message).toMatchObject({
      payload: { failedTool: "math", completedResults: [{ toolName: "weather", result: { value: "sunny" } }] },
    });
  });

  test("a stale stepIndex is ignored as stale-timeout", () => {
    const s0 = started();
    const s1 = transition(s0, resulted("weather", 0, { value: "sunny" }), NOW + 1_000, CONFIG).state!;
    const r = transition(s1, timedOut(0, DEADLINE), DEADLINE, CONFIG); // step 0 already answered
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "stale-timeout" });
    expect(r.state).toEqual(s1);
    expect(r.emit).toEqual([]);
  });

  test("a stale deadline for the right step is ignored as stale-timeout", () => {
    const s = started();
    const r = transition(s, timedOut(0, DEADLINE - 1), DEADLINE, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "stale-timeout" });
    expect(r.state).toEqual(s);
    expect(r.emit).toEqual([]);
  });

  test("a terminal plan ignores the timeout as plan-terminal", () => {
    const s = started();
    const failed = transition(s, timedOut(0, DEADLINE), DEADLINE, CONFIG).state!;
    const dup = transition(failed, timedOut(0, DEADLINE), DEADLINE, CONFIG);
    expect(dup.decision).toMatchObject({ kind: "ignored", reason: "plan-terminal" });
    expect(dup.emit).toEqual([]);

    const one = mustApply(null, planGenerated([{ tool: "chat", args: { userInput: "hi" } }])).state!;
    const done = transition(one, resulted("chat", 0, { value: "x" }), NOW, CONFIG).state!;
    expect(transition(done, timedOut(0, DEADLINE), DEADLINE, CONFIG).decision).toMatchObject({
      kind: "ignored",
      reason: "plan-terminal",
    });
  });

  test("no stored plan ignores the timeout as unknown-plan", () => {
    const r = transition(null, timedOut(0, DEADLINE), DEADLINE, CONFIG);
    expect(r.decision).toMatchObject({ kind: "ignored", reason: "unknown-plan" });
    expect(r.state).toBeNull();
    expect(r.emit).toEqual([]);
  });

  test("a running plan without stepDeadline (legacy) matches on stepIndex alone, with any deadline", () => {
    const legacy = started();
    delete (legacy as Partial<PlanState>).stepDeadline; // persisted before this phase
    const r = transition(legacy, timedOut(0, 123), DEADLINE, CONFIG);
    expect(r.decision).toMatchObject({ kind: "failed", code: "step-timeout" });
    // ...but a different step is still stale
    expect(transition(legacy, timedOut(1, 123), DEADLINE, CONFIG).decision).toMatchObject({
      kind: "ignored",
      reason: "stale-timeout",
    });
    // and a null deadline behaves the same
    expect(transition({ ...legacy, stepDeadline: null }, timedOut(0, 123), DEADLINE, CONFIG).decision.kind).toBe("failed");
  });

  test("result-then-timeout: the result wins, the timeout is stale", () => {
    const s = started();
    const afterResult = transition(s, resulted("weather", 0, { value: "sunny" }), NOW + 10, CONFIG);
    expect(afterResult.decision.kind).toBe("advanced");
    const afterTimeout = transition(afterResult.state, timedOut(0, DEADLINE), DEADLINE, CONFIG);
    expect(afterTimeout.decision).toMatchObject({ kind: "ignored", reason: "stale-timeout" });
    expect(afterTimeout.state!.status).toBe("running");
  });

  test("timeout-then-result: the timeout wins, the late result is plan-terminal", () => {
    const s = started();
    const afterTimeout = transition(s, timedOut(0, DEADLINE), DEADLINE, CONFIG);
    expect(afterTimeout.decision.kind).toBe("failed");
    const afterResult = transition(afterTimeout.state, resulted("weather", 0, { value: "sunny" }), DEADLINE + 5, CONFIG);
    expect(afterResult.decision).toMatchObject({ kind: "ignored", reason: "plan-terminal" });
    expect(afterResult.state!.status).toBe("failed");
  });

  test("is deterministic for the same (state, event, now, config)", () => {
    const s = started();
    expect(transition(s, timedOut(0, DEADLINE), DEADLINE, CONFIG)).toEqual(
      transition(s, timedOut(0, DEADLINE), DEADLINE, CONFIG)
    );
  });
});
