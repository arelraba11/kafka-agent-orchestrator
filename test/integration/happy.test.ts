import { describe, expect, test } from "bun:test";
import { tag, token, useHarness } from "./harness";

const h = useHarness("happy", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "math", "weather", "exchange", "chat", "web"],
});

describe("happy path", () => {
  test("H1: single step via WebSocket", async () => {
    const t = token();
    const input = `hello there ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.answer(t, "Hello from the fake synthesizer.");

    const res = await h.ask(input);
    await h.settle();

    expect(res.frames.map((f) => f.type)).toEqual(["ack", "tool", "answer"]);
    expect(res.final.answer).toBe("Hello from the fake synthesizer.");

    const cid = res.cid;
    const count = (type: Parameters<typeof h.observer.events>[1]) => h.observer.events(cid, type).length;
    expect(count("UserQueryReceived")).toBe(1);
    expect(count("PlanGenerated")).toBe(1);
    expect(h.observer.requestsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.resultsFor(cid, 0)).toHaveLength(1);
    expect(count("PlanCompleted")).toBe(1);
    expect(count("SynthesizeFinalAnswerRequested")).toBe(1);
    expect(count("FinalAnswerSynthesized")).toBe(1);
    expect(h.observer.events(cid).filter((m) => m.type === "ConversationFailed" || m.type === "PlanFailed")).toEqual([]);

    const history = h.history(cid);
    expect(history.map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(history[1].content).toBe("Hello from the fake synthesizer.");

    expect(h.fake.calls("router", t)).toHaveLength(1);
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(h.observer.dlq()).toEqual([]);
  });

  test("H2: a multi-step plan runs in order and the synthesizer sees both results", async () => {
    const t = token();
    const input = `weather in Tel Aviv, then 6*7 ${tag(t)}`;
    h.fake.plan(t, [
      { tool: "weather", args: { city: "Tel Aviv" } },
      { tool: "math", args: { expression: "6*7" } },
    ]);
    h.fake.answer(t, "Sunny in Tel Aviv, and 42.");

    const res = await h.ask(input);
    await h.settle();
    const cid = res.cid;

    expect(res.final.answer).toBe("Sunny in Tel Aviv, and 42.");

    // Step 1 is dispatched only after step 0's result: causally by the event timestamps, and
    // by offset where the two share a topic and a partition (the key is the cid).
    const [req0] = h.observer.requestsFor(cid, 0);
    const [req1] = h.observer.requestsFor(cid, 1);
    const [res0] = h.observer.resultsFor(cid, 0);
    const [res1] = h.observer.resultsFor(cid, 1);
    expect(h.observer.requestsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.requestsFor(cid, 1)).toHaveLength(1);
    expect(h.observer.resultsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.resultsFor(cid, 1)).toHaveLength(1);
    expect(req1.value.timestamp).toBeGreaterThanOrEqual(res0.value.timestamp);
    expect(Number(req1.offset)).toBeGreaterThan(Number(req0.offset)); // tool-invocation-requests
    expect(Number(res1.offset)).toBeGreaterThan(Number(res0.offset)); // conversation-events
    expect(req0.partition).toBe(req1.partition);
    expect(res0.partition).toBe(res1.partition);

    const completed = h.observer.events(cid, "PlanCompleted");
    expect(completed).toHaveLength(1);
    const results = completed[0].value.payload.results;
    expect(results.map((r: { toolName: string }) => r.toolName)).toEqual(["weather", "math"]);
    expect(results[0].result.value).toContain("Tel Aviv");
    expect(results[1].result.value).toBe("42");

    // the synthesizer's prompt carries both results
    const [call] = h.fake.calls("synth", t);
    expect(call.prompt).toContain("[weather]: ");
    expect(call.prompt).toContain("Tel Aviv");
    expect(call.prompt).toContain("[math]: 42");

    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq()).toEqual([]);
  });
});
