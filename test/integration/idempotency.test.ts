import { describe, expect, test } from "bun:test";
import { TOPICS } from "../../shared/topics";
import { tag, token, useHarness, waitFor } from "./harness";

// math is NOT started with the rest: D1 needs step 1 to stay pending.
const h = useHarness("idempotency", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "weather", "exchange", "chat", "web"],
});

const historyRoles = (cid: string) => h.history(cid).map((m) => m.role);

describe("idempotency", () => {
  test("D1: a duplicate result while the plan is live does not re-dispatch the next step", async () => {
    const t = token();
    const input = `weather then math ${tag(t)}`;
    h.fake.plan(t, [
      { tool: "weather", args: { city: "Tel Aviv" } },
      { tool: "math", args: { expression: "6*7" } },
    ]);
    h.fake.answer(t, "Sunny, and 42.");

    const q = h.query(input);
    const cid = await q.cid;

    // step 1 is dispatched (nobody consumes it: math is down)
    await waitFor(() => h.observer.requestsFor(cid, 1)[0], {
      what: `ToolInvocationRequested{1} for ${cid}`,
      context: () => h.observer.describe(cid),
    });
    const [result0] = h.observer.resultsFor(cid, 0);
    expect(result0).toBeDefined();

    // republish Resulted{0} byte-for-byte, then wait until the orchestrator has handled it
    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, result0.raw!);
    await h.settle();
    expect(h.observer.requestsFor(cid, 1)).toHaveLength(1);

    await h.services.start("math");
    const res = await q.result;
    await h.settle();

    expect(res.final.type).toBe("answer");
    expect(h.observer.requestsFor(cid, 1)).toHaveLength(1);
    const completed = h.observer.events(cid, "PlanCompleted");
    expect(completed).toHaveLength(1);
    expect(completed[0].value.payload.results.map((r: { toolName: string }) => r.toolName)).toEqual(["weather", "math"]);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(historyRoles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });

  test("D4: a duplicate PlanCompleted costs one LLM call and one history entry", async () => {
    const t = token();
    const input = `say hi ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.answer(t, "Hi there.");

    const res = await h.ask(input);
    await h.settle();
    const cid = res.cid;
    const [completed] = h.observer.events(cid, "PlanCompleted");

    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, completed.raw!);
    await h.settle();

    expect(h.observer.events(cid, "SynthesizeFinalAnswerRequested")).toHaveLength(2);
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    // the duplicate is ignored, not answered with a failure for an already-answered conversation
    expect(h.observer.events(cid, "ConversationFailed")).toEqual([]);
    expect(historyRoles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });
});
