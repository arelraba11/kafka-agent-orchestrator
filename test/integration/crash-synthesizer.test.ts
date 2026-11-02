import { describe, expect, test } from "bun:test";
import { tag, token, useHarness } from "./harness";

// The synthesizer is started by each test, with the fault env of that test.
const h = useHarness("crashsynth", {
  start: ["router", "orchestrator", "aggregator", "math", "weather", "exchange", "chat", "web"],
});

describe("synthesizer crash windows", () => {
  test("S3: window after-publish, before history and done: one LLM call, one history entry", async () => {
    const t = token();
    const input = `say hi ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.script("synth", t, [{ ok: "A" }]);

    const faults = h.services.faults("synthesizer");
    faults.clear("synthesizer:after-publish");
    await h.services.restart("synthesizer", faults.crashAt("synthesizer:after-publish"));

    const q = h.query(input);
    const exit = await h.services.waitExit("synthesizer", "crash at synthesizer:after-publish");
    expect(exit.signal).toBe("SIGKILL");
    expect(faults.hits("synthesizer:after-publish")).toBe(1);

    await h.services.start("synthesizer"); // recovery: no fault env
    const res = await q.result;
    await h.settle();
    const cid = res.cid;

    expect(res.final).toMatchObject({ type: "answer", answer: "A" });
    expect(h.fake.calls("synth", t)).toHaveLength(1); // the memoized outcome is reused
    // republished from the memo after the restart
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(2);
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });
});
