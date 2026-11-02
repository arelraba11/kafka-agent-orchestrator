import { afterAll, describe, expect, test } from "bun:test";
import { TOPICS } from "../../shared/topics";
import { newCid, pauseBroker, tag, token, unpauseBroker, useHarness, waitFor } from "./harness";

// A paused broker must surface within seconds: 1 retry, 2 s request timeout, 3 s confirmed-send
// timeout in the RAG worker (RAG_SEND_TIMEOUT_S, set by the harness). KAFKA_MAX_BLOCK_MS bounds the
// metadata wait that precedes it: a cold producer (the worker has not sent to the topic yet)
// would otherwise block for the library default of 60 s before failing.
const h = useHarness("rag", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "exchange", "web", "rag"],
  env: { KAFKA_RETRIES: "1", KAFKA_REQUEST_TIMEOUT_MS: "2000", KAFKA_MAX_BLOCK_MS: "3000" },
});

afterAll(unpauseBroker); // never leave the broker paused

/** The R0 plan: RAG price, then a currency conversion whose amount is the RAG step's result. */
function ragPlan(t: string) {
  h.fake.plan(t, [
    { tool: "getProductInformation", args: { query: "iPhone price" } },
    { tool: "exchange", args: { currencyCode: "USD", targetCurrency: "ILS", amount: "{{step_0.result}}" } },
  ]);
  h.fake.answer(t, "About 4436 shekels.");
}

const roles = (cid: string) => h.history(cid).map((m) => m.role);

describe("RAG worker", () => {
  test("R0: RAG result, then the placeholder resolves to its value (the price, not the first number in the context)", async () => {
    const t = token();
    const input = `how much is an iPhone in shekels ${tag(t)}`;
    ragPlan(t);

    const res = await h.ask(input);
    await h.settle(); // includes rag-tool-worker lag 0
    const cid = res.cid;

    const [rag] = h.observer.resultsFor(cid, 0);
    expect(rag.value.payload.toolName).toBe("getProductInformation");
    expect(rag.value.payload.result.context).toBeString();
    expect(rag.value.payload.result.context.length).toBeGreaterThan(0);
    // "Price: $1,199" in data/products/iphone.txt, digits only; the first number in the
    // context is 16 ("iPhone 16 Pro Max")
    expect(rag.value.payload.result.value).toBe("1199");

    const [req1] = h.observer.requestsFor(cid, 1);
    expect(req1.value.payload.input.amount).toBe("1199");
    const [exchange] = h.observer.resultsFor(cid, 1);
    expect(exchange.value.payload.result.value).toBe("1199 USD = 4436.3 ILS");

    expect(res.final.type).toBe("answer");
    expect(h.observer.resultsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(roles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq()).toEqual([]);
  });

  test("R1a: a cold producer that cannot reach the broker exits and the result is not lost (metadata path)", async () => {
    const t = token();
    const input = `how much is an iPhone in shekels ${tag(t)}`;
    ragPlan(t);

    const point = "rag:before-send";
    const faults = h.services.faults("rag");
    faults.clear(point);
    await h.services.restart("rag", faults.holdAt(point));

    const q = h.query(input, 60_000);
    await faults.waitReached(point);

    try {
      await pauseBroker();
      faults.release(point); // the first send of this process runs against a paused broker
      // no metadata for the topic yet: send() itself gives up after KAFKA_MAX_BLOCK_MS
      const exit = await h.services.waitExit("rag", "the cold send to the paused broker to fail", 30_000);
      expect(exit.signal).toBeNull();
      expect(exit.code).not.toBe(0); // it exited on the error, uncommitted
      expect(exit.code).not.toBeNull();
    } finally {
      await unpauseBroker();
    }

    await h.services.start("rag"); // no fault env: the record is redelivered
    const res = await q.result;
    await h.settle();
    const cid = res.cid;

    expect(res.final.type).toBe("answer");
    expect(h.observer.resultsFor(cid, 0).length).toBeGreaterThanOrEqual(1);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(roles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq()).toEqual([]);
  }, 90_000);

  test("R1b: a warm producer whose send is never acknowledged does not commit, and the result is not lost (ack path)", async () => {
    const t = token();
    const input = `how much is an iPhone in shekels ${tag(t)}`;
    ragPlan(t);
    const warm = token();
    const warmInput = `how much is an iPhone in shekels ${tag(warm)}`;
    ragPlan(warm);

    const before = "rag:before-send";
    const after = "rag:after-send";
    const faults = h.services.faults("rag");
    faults.clear(before);
    faults.clear(after);
    // hold only the SECOND request's send; the first one warms the producer's metadata
    await h.services.restart("rag", faults.holdAt(before, undefined, 2));

    const first = await h.ask(warmInput);
    expect(first.final.type).toBe("answer");
    expect(faults.hits(before)).toBe(1);
    expect(faults.hits(after)).toBe(1);
    expect(faults.reached(before)).toBe(false);

    const q = h.query(input, 60_000);
    await faults.waitReached(before);

    try {
      await pauseBroker();
      faults.release(before); // metadata is cached, so send() returns at once; only the ack can fail
      // Either the worker dies on the unacknowledged send (correct), or it walks on to the commit
      // point without an ack (a bare send()): rag:after-send is the point right before the commit.
      await waitFor(() => !h.services.isRunning("rag") || faults.hits(after) > 1, {
        timeoutMs: 30_000,
        what: "the worker to exit on the unacknowledged send, or to reach rag:after-send",
        context: () => `  log:\n${h.services.logTail("rag")}`,
      });
      expect(faults.hits(after), "the worker reached the commit point without an acknowledged send").toBe(1);
      const exit = await h.services.waitExit("rag", "the unacknowledged send to end the worker", 5_000);
      expect(exit.signal).toBeNull();
      expect(exit.code).not.toBe(0);
      expect(exit.code).not.toBeNull();
    } finally {
      await unpauseBroker();
    }

    await h.services.start("rag"); // no fault env: the uncommitted record is redelivered
    const res = await q.result;
    await h.settle();
    const cid = res.cid;

    expect(res.final.type).toBe("answer");
    expect(h.observer.resultsFor(cid, 0).length).toBeGreaterThanOrEqual(1);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(roles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq()).toEqual([]);
  }, 90_000);

  test("R2: a crash after the send, before the commit, redelivers and the duplicate result is ignored", async () => {
    const t = token();
    const input = `how much is an iPhone in shekels ${tag(t)}`;
    ragPlan(t);

    const point = "rag:after-send";
    const faults = h.services.faults("rag");
    faults.clear(point);
    await h.services.restart("rag", faults.crashAt(point));

    const q = h.query(input, 60_000);
    const exit = await h.services.waitExit("rag", `crash at ${point}`, 60_000);
    expect(exit.signal).toBe("SIGKILL");
    expect(faults.hits(point)).toBe(1);

    await h.services.start("rag"); // recovery: no fault env
    const res = await q.result;
    await h.settle();
    const cid = res.cid;

    expect(res.final.type).toBe("answer");
    expect(h.observer.resultsFor(cid, 0)).toHaveLength(2); // sent before the crash, again after the redelivery
    expect(h.observer.requestsFor(cid, 1)).toHaveLength(1);
    expect(h.observer.events(cid, "PlanCompleted")).toHaveLength(1);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    expect(roles(cid)).toEqual(["user", "assistant"]);
    expect(h.observer.dlq()).toEqual([]);
  }, 90_000);

  test("E2: unknown extra fields are accepted by the Python worker", async () => {
    const cid = newCid();
    await h.produce(
      TOPICS.TOOL_INVOCATION_REQUESTS,
      cid,
      JSON.stringify({
        conversationId: cid,
        timestamp: Date.now(),
        eventType: "ToolInvocationRequested",
        traceId: "trace-3",
        payload: { toolName: "getProductInformation", stepIndex: 0, input: { query: "iPhone price" }, priority: "high" },
      })
    );

    const [result] = await waitFor(() => {
      const found = h.observer.resultsFor(cid, 0);
      return found.length > 0 ? found : undefined;
    }, { what: `ToolInvocationResulted{0} from the RAG worker for ${cid}`, context: () => h.observer.describe(cid) });
    await h.settle();

    expect(result.value.payload.toolName).toBe("getProductInformation");
    expect(result.value.payload.result.context).toBeString();
    expect(result.value.payload.result.context.length).toBeGreaterThan(0);
    expect(h.observer.resultsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.dlq(), "no dead letter from rag-tool-worker").toEqual([]);
  });
});
