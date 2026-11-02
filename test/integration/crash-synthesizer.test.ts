import { describe, expect, test } from "bun:test";
import type { FaultPoint } from "../../shared/testing/faultPoint";
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

  /**
   * Runs one conversation with the synthesizer killed (SIGKILL) at `point`, restarts it without
   * the fault, and returns the finished conversation.
   */
  async function crashAt(point: FaultPoint, synth: Parameters<typeof h.fake.script>[2]) {
    const t = token();
    const input = `say hi ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.script("synth", t, synth);

    const faults = h.services.faults("synthesizer");
    faults.clear(point);
    await h.services.restart("synthesizer", faults.crashAt(point));

    const q = h.query(input);
    const exit = await h.services.waitExit("synthesizer", `crash at ${point}`);
    expect(exit.signal).toBe("SIGKILL");
    expect(faults.hits(point)).toBe(1);

    await h.services.start("synthesizer"); // recovery: no fault env
    const res = await q.result;
    await h.settle();
    return { t, res, cid: res.cid };
  }

  test("S1: window after-llm, before the memo: the LLM is asked again and the cached question survives", async () => {
    const { t, res, cid } = await crashAt("synthesizer:after-llm", [{ ok: "A" }, { ok: "B" }]);

    expect(h.fake.calls("synth", t)).toHaveLength(2); // the first outcome was never memoized
    expect(res.final).toMatchObject({ type: "answer", answer: "B" });
    const answers = h.observer.events(cid, "FinalAnswerSynthesized");
    expect(answers).toHaveLength(1);
    expect(answers[0].value.payload.answer).toBe("B");
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
    // the q: cache survived the crash: no missing-question failure
    expect(h.observer.events(cid, "ConversationFailed")).toEqual([]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });

  test("S2: window after-memo, before the publish: the memoized outcome is published after the restart", async () => {
    const { t, res, cid } = await crashAt("synthesizer:after-memo", [{ ok: "A" }]);

    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(res.final).toMatchObject({ type: "answer", answer: "A" });
    const answers = h.observer.events(cid, "FinalAnswerSynthesized");
    expect(answers).toHaveLength(1);
    expect(answers[0].value.payload.answer).toBe("A");
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(h.observer.events(cid, "ConversationFailed")).toEqual([]);
  });

  test("S4: window after-history, before the done batch: the republished answer does not duplicate the history", async () => {
    const { t, res, cid } = await crashAt("synthesizer:after-history", [{ ok: "A" }]);

    expect(h.fake.calls("synth", t)).toHaveLength(1);
    expect(res.final).toMatchObject({ type: "answer", answer: "A" });
    // published before the crash and again after the restart
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(2);
    // the append is idempotent by conversationId
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });
});
