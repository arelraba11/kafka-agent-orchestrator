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
});
