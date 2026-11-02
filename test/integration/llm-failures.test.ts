import { describe, expect, test } from "bun:test";
import { tag, token, useHarness } from "./harness";

const LLM_SYNTH_BUDGET_MS = 8000; // the harness's suite value (harness/services.ts)

const h = useHarness("llmfail", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "math", "weather", "exchange", "chat", "web"],
});

/** Runs a `chat` plan whose synthesis uses `synth` and returns the finished conversation. */
async function failingSynthesis(synth: Parameters<typeof h.fake.script>[2]) {
  const t = token();
  const input = `say hi ${tag(t)}`;
  h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
  h.fake.script("synth", t, synth);
  const res = await h.ask(input);
  await h.settle();
  return { t, res };
}

describe("LLM failure codes", () => {
  test("L2: 5xx on every attempt ends as llm-unavailable", async () => {
    const { t, res } = await failingSynthesis([{ status: 500 }, { status: 503 }, { status: 500 }]);
    const cid = res.cid;

    const failed = h.observer.events(cid, "ConversationFailed");
    expect(failed).toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "synthesis", code: "llm-unavailable" });
    expect(h.fake.calls("synth", t)).toHaveLength(3);
    expect(h.history(cid)).toEqual([]);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toEqual([]);
    expect(res.final).toMatchObject({ type: "error", code: "llm-unavailable" });
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });

  test("L3: 401 ends as llm-misconfigured and is not retried", async () => {
    const { t, res } = await failingSynthesis([{ status: 401 }]);
    const cid = res.cid;

    const failed = h.observer.events(cid, "ConversationFailed");
    expect(failed).toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "synthesis", code: "llm-misconfigured" });
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(h.history(cid)).toEqual([]);
    expect(res.final).toMatchObject({ type: "error", code: "llm-misconfigured" });
  });

  test("L1: 429 with Retry-After is retried after the wait and then succeeds", async () => {
    const { t, res } = await failingSynthesis([{ status: 429, retryAfter: 1 }, { ok: "A" }]);

    expect(res.final).toMatchObject({ type: "answer", answer: "A" });
    const calls = h.fake.calls("synth", t);
    expect(calls).toHaveLength(2);
    expect(calls[1].at - calls[0].at).toBeGreaterThanOrEqual(1000); // Retry-After: 1 was honored
    expect(h.observer.events(res.cid, "ConversationFailed")).toEqual([]);
    expect(h.history(res.cid).map((m) => m.role)).toEqual(["user", "assistant"]);
  });

  test("L4: an empty answer on every attempt ends as empty-answer", async () => {
    const { t, res } = await failingSynthesis([{ empty: true }, { empty: true }, { empty: true }]);

    const failed = h.observer.events(res.cid, "ConversationFailed");
    expect(failed).toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "synthesis", code: "empty-answer" });
    expect(h.fake.calls("synth", t)).toHaveLength(3);
    expect(res.final).toMatchObject({ type: "error", code: "empty-answer" });
    expect(h.history(res.cid)).toEqual([]);
  });

  /** Milliseconds from the synthesize command to the terminal ConversationFailed. */
  const failureLatency = (cid: string) => {
    const [cmd] = h.observer.events(cid, "SynthesizeFinalAnswerRequested");
    const [failed] = h.observer.events(cid, "ConversationFailed");
    return failed.value.timestamp - cmd.value.timestamp;
  };

  test("L5: three timeouts end as llm-unavailable inside the synthesizer's budget", async () => {
    const { t, res } = await failingSynthesis([{ hang: true }, { hang: true }, { hang: true }]);

    const failed = h.observer.events(res.cid, "ConversationFailed");
    expect(failed).toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "synthesis", code: "llm-unavailable" });
    expect(h.fake.calls("synth", t)).toHaveLength(3);
    expect(failureLatency(res.cid)).toBeLessThan(LLM_SYNTH_BUDGET_MS + 1000);
    expect(res.final).toMatchObject({ type: "error", code: "llm-unavailable" });
  });

  test("L6: a Retry-After beyond the budget fails fast instead of waiting", async () => {
    const { t, res } = await failingSynthesis([{ status: 429, retryAfter: 30 }]);

    const failed = h.observer.events(res.cid, "ConversationFailed");
    expect(failed).toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "synthesis", code: "llm-unavailable" });
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(failureLatency(res.cid)).toBeLessThan(2000);
    expect(res.final).toMatchObject({ type: "error", code: "llm-unavailable" });
  });

  test("L7: a router LLM failure falls back to the regex planner and the conversation still answers", async () => {
    const t = token();
    const input = `what is 6*7 ${tag(t)}`;
    h.fake.script("router", t, [{ status: 500 }]);
    h.fake.answer(t, "It is 42.");

    const res = await h.ask(input);
    await h.settle();
    const cid = res.cid;

    expect(h.fake.calls("router", t)).toHaveLength(1); // one attempt, no retry
    const [plan] = h.observer.events(cid, "PlanGenerated");
    expect(plan.value.payload.steps.map((s: { tool: string }) => s.tool)).toEqual(["math"]);
    expect(res.final).toMatchObject({ type: "answer", answer: "It is 42." });
    expect(h.observer.events(cid, "ConversationFailed")).toEqual([]);
    expect(h.observer.events(cid, "PlanFailed")).toEqual([]);
  });
});
