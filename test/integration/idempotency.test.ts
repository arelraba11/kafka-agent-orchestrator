import { describe, expect, test } from "bun:test";
import { TOPICS } from "../../shared/topics";
import { newCid, tag, token, useHarness, waitFor } from "./harness";

// math is NOT started with the rest: D1 needs step 1 to stay pending.
const h = useHarness("idempotency", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "weather", "exchange", "chat", "web"],
});

// D1 leaves math running; a test run on its own (-t) has to start it.
const ensureMath = async () => {
  if (!h.services.isRunning("math")) await h.services.start("math");
};

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

  test("D2: a duplicate result after completion changes nothing downstream", async () => {
    const t = token();
    const input = `weather then math ${tag(t)}`;
    h.fake.plan(t, [
      { tool: "weather", args: { city: "Tel Aviv" } },
      { tool: "math", args: { expression: "6*7" } },
    ]);
    h.fake.answer(t, "Sunny, and 42.");

    await ensureMath();
    const res = await h.ask(input);
    await h.settle();
    const cid = res.cid;
    const [result1] = h.observer.resultsFor(cid, 1);

    const before = h.observer.messages.length;
    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, result1.raw!);
    await h.settle();

    // the duplicate is the only new record
    expect(h.observer.messages.length).toBe(before + 1);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "SynthesizeFinalAnswerRequested")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(historyRoles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });

  test("D3: a duplicate PlanGenerated, while running and after the answer, dispatches nothing twice", async () => {
    const t = token();
    const input = `say hi ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.answer(t, "Hi there.");

    const q = h.query(input);
    const cid = await q.cid;
    const [plan] = await waitFor(() => {
      const found = h.observer.events(cid, "PlanGenerated");
      return found.length > 0 ? found : undefined;
    }, { what: `PlanGenerated for ${cid}`, context: () => h.observer.describe(cid) });

    // (a) as soon as it is observed
    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, plan.raw!);
    const res = await q.result;
    await h.settle();
    expect(res.final.type).toBe("answer");

    // (b) after the conversation is answered
    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, plan.raw!);
    await h.settle();

    expect(h.observer.events(cid, "PlanGenerated")).toHaveLength(3);
    expect(h.observer.requestsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(historyRoles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });

  test("E1: unknown extra fields are accepted by the TS consumers", async () => {
    await ensureMath();
    const t = token();
    const input = `2+2 ${tag(t)}`;
    h.fake.plan(t, [{ tool: "math", args: { expression: "2+2" } }]);
    h.fake.answer(t, "Four.");

    // a UserQueryReceived from a "newer" producer: extra envelope field and extra payload field
    const cid = newCid();
    await h.produce(
      TOPICS.USER_COMMANDS,
      cid,
      JSON.stringify({
        conversationId: cid,
        timestamp: Date.now(),
        eventType: "UserQueryReceived",
        traceId: "trace-1",
        payload: { userInput: input, sessionId: `s-${t}`, locale: "en-US" },
      })
    );
    // a ToolInvocationRequested with extra fields, for a conversation with no plan: the math
    // worker answers it, the orchestrator and the web server ignore the stray result
    const direct = newCid();
    await h.produce(
      TOPICS.TOOL_INVOCATION_REQUESTS,
      direct,
      JSON.stringify({
        conversationId: direct,
        timestamp: Date.now(),
        eventType: "ToolInvocationRequested",
        traceId: "trace-2",
        payload: { toolName: "math", stepIndex: 0, input: { expression: "2+2" }, priority: "high" },
      })
    );

    await waitFor(() => h.observer.events(cid, "FinalAnswerSynthesized")[0], {
      what: `FinalAnswerSynthesized for ${cid}`,
      context: () => h.observer.describe(cid),
    });
    await waitFor(() => h.observer.resultsFor(direct, 0)[0], {
      what: `ToolInvocationResulted{0} for ${direct}`,
      context: () => h.observer.describe(direct),
    });
    await h.settle();

    expect(h.observer.events(cid, "FinalAnswerSynthesized")[0].value.payload.answer).toBe("Four.");
    expect(h.observer.events(cid, "ConversationFailed")).toEqual([]);
    const results = h.observer.resultsFor(direct, 0);
    expect(results).toHaveLength(1);
    expect(results[0].value.payload.result.value).toBe("4");
    expect(h.observer.dlq()).toEqual([]);
    expect(h.observer.invalid()).toEqual([]);
  });
});
