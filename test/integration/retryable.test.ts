import { afterAll, describe, expect, test } from "bun:test";
import { pauseBroker, tag, token, unpauseBroker, useHarness, waitFor } from "./harness";

// A paused broker must surface quickly (1 retry, 2 s request timeout) and must not
// outlive the group session (10 s).
const h = useHarness("retryable", {
  start: ["router", "aggregator", "synthesizer", "math", "weather", "exchange", "chat", "web"],
  env: { KAFKA_RETRIES: "1", KAFKA_REQUEST_TIMEOUT_MS: "2000", KAFKA_SESSION_TIMEOUT_MS: "10000" },
});

afterAll(unpauseBroker); // never leave the broker paused

describe("retryable errors", () => {
  test("X1: a producer failure is retried, never dead-lettered", async () => {
    const t = token();
    const input = `say hi ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.answer(t, "Hi.");

    const point = "orchestrator:before-emit";
    const faults = h.services.faults("orchestrator");
    faults.clear(point);
    faults.clear("kafka:retry");
    await h.services.restart("orchestrator", faults.holdAt(point, "started"));
    const pid = h.services.pid("orchestrator");

    const q = h.query(input, 45_000);
    await faults.waitReached(point);

    try {
      await pauseBroker();
      faults.release(point); // the emit now runs against a paused broker
      await waitFor(() => faults.hits("kafka:retry") >= 1, {
        timeoutMs: 20_000,
        what: "the orchestrator to classify the failed send as a retryable error (kafka:retry)",
        context: () => `  log:\n${h.services.logTail("orchestrator")}`,
      });
    } finally {
      await unpauseBroker();
    }

    const res = await q.result;
    await h.settle();
    const cid = res.cid;

    expect(res.final.type).toBe("answer");
    expect(h.services.pid("orchestrator")).toBe(pid); // never crashed
    expect(h.services.isRunning("orchestrator")).toBe(true);
    expect(h.observer.dlq(), "no dead letters in the namespace").toEqual([]);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
  }, 60_000);
});
