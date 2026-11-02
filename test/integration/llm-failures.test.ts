import { describe, expect, test } from "bun:test";
import { tag, token, useHarness } from "./harness";

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
});
