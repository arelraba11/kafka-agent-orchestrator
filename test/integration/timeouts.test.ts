import { describe, expect, test } from "bun:test";
import { tag, token, useHarness, waitFor } from "./harness";

const STEP_TIMEOUT_MS = 2000;

// math is never started: its step can only time out.
const h = useHarness("timeouts", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "weather", "exchange", "chat", "web"],
  env: { STEP_TIMEOUT_MS: String(STEP_TIMEOUT_MS) },
});

/** The step-0 timeout assertions shared by T1 and T3. */
function expectTimedOut(cid: string) {
  const [req0] = h.observer.requestsFor(cid, 0);
  const timedOut = h.observer.events(cid, "StepTimedOut");
  expect(timedOut, "StepTimedOut").toHaveLength(1);
  expect(timedOut[0].value.payload.stepIndex).toBe(0);
  // the deadline is the dispatch time plus the step timeout, to the millisecond
  expect(Math.abs(timedOut[0].value.payload.deadline - (req0.value.timestamp + STEP_TIMEOUT_MS))).toBeLessThanOrEqual(50);
  const failed = h.observer.events(cid, "PlanFailed");
  expect(failed, "PlanFailed").toHaveLength(1);
  expect(failed[0].value.payload.code).toBe("step-timeout");
  expect(h.observer.events(cid, "PlanCompleted")).toEqual([]);
  expect(h.observer.events(cid, "FinalAnswerSynthesized")).toEqual([]);
  expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
}

// T1's conversation, which T2 continues
let t1Cid: string | undefined;

describe("step timeouts", () => {
  test("T1: a step nobody answers times out", async () => {
    const t = token();
    const input = `6*7 ${tag(t)}`;
    h.fake.plan(t, [{ tool: "math", args: { expression: "6*7" } }]);

    const res = await h.ask(input);
    await h.settle();

    expect(res.final).toMatchObject({ type: "error", code: "step-timeout" });
    expect(h.observer.requestsFor(res.cid, 0)).toHaveLength(1);
    expectTimedOut(res.cid);
    expect(h.fake.calls("synth", t)).toEqual([]);
    t1Cid = res.cid;
  });

  // runs right after T1: the math worker finally starts and answers the request T1 timed out
  test("T2: a result that arrives after the timeout is ignored", async () => {
    const cid = t1Cid;
    if (cid === undefined) throw new Error("T2 continues T1: run the file without -t, or with -t 'T1|T2'");
    expect(h.observer.resultsFor(cid, 0)).toEqual([]); // nobody has answered yet

    try {
      await h.services.start("math"); // reads the old request from the beginning of the topic
      await waitFor(() => h.observer.resultsFor(cid, 0)[0], {
        what: `the late ToolInvocationResulted{0} for ${cid}`,
        context: () => h.observer.describe(cid),
      });
      await h.settle(); // the orchestrator has handled it
    } finally {
      await h.services.stop("math"); // T3 needs step 0 to stay unanswered
    }

    expect(h.observer.resultsFor(cid, 0)).toHaveLength(1);
    expect(h.observer.events(cid, "PlanFailed")).toHaveLength(1);
    expect(h.observer.events(cid, "PlanCompleted")).toEqual([]);
    expect(h.observer.events(cid, "SynthesizeFinalAnswerRequested")).toEqual([]);
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toEqual([]);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });

  test("T3: a restarted orchestrator re-arms the timer with the ORIGINAL deadline", async () => {
    const t = token();
    const input = `6*7 ${tag(t)}`;
    h.fake.plan(t, [{ tool: "math", args: { expression: "6*7" } }]);

    // killed right after the plan was persisted, before it armed its timer
    const faults = h.services.faults("orchestrator");
    faults.clear("orchestrator:after-persist");
    await h.services.restart("orchestrator", faults.crashAt("orchestrator:after-persist", "started"));

    const q = h.query(input);
    const exit = await h.services.waitExit("orchestrator", "crash at orchestrator:after-persist@started");
    expect(exit.signal).toBe("SIGKILL");
    expect(faults.hits("orchestrator:after-persist", "started")).toBe(1);
    expect(h.observer.events(await q.cid, "StepTimedOut")).toEqual([]); // the dead process armed nothing

    await h.services.start("orchestrator"); // recovery: reads the persisted deadline
    const res = await q.result;
    await h.settle();

    expect(res.final).toMatchObject({ type: "error", code: "step-timeout" });
    expect(h.observer.requestsFor(res.cid, 0)).toHaveLength(1);
    expectTimedOut(res.cid);
  });
});
