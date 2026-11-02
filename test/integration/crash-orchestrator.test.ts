import { describe, expect, test } from "bun:test";
import type { FaultPoint } from "../../shared/testing/faultPoint";
import { tag, token, useHarness } from "./harness";

// The orchestrator is started by each test, with the fault env of that test.
const h = useHarness("crashorch", {
  start: ["router", "aggregator", "synthesizer", "math", "weather", "exchange", "chat", "web"],
});

interface Case {
  id: string;
  point: FaultPoint;
  decision: "started" | "advanced" | "completed";
  /** Expected counts after recovery. */
  requested0: number;
  requested1: number;
  completed: number;
}

// window b: the outbound events were emitted, the state was not persisted
// window c: the state was persisted, the offset was not committed
const CASES: Case[] = [
  { id: "C-b1", point: "orchestrator:after-emit", decision: "started", requested0: 2, requested1: 1, completed: 1 },
  { id: "C-b2", point: "orchestrator:after-emit", decision: "advanced", requested0: 1, requested1: 2, completed: 1 },
  { id: "C-b3", point: "orchestrator:after-emit", decision: "completed", requested0: 1, requested1: 1, completed: 2 },
  { id: "C-c1", point: "orchestrator:after-persist", decision: "started", requested0: 1, requested1: 1, completed: 1 },
  { id: "C-c2", point: "orchestrator:after-persist", decision: "advanced", requested0: 1, requested1: 1, completed: 1 },
  { id: "C-c3", point: "orchestrator:after-persist", decision: "completed", requested0: 1, requested1: 1, completed: 1 },
];

describe("orchestrator crash windows", () => {
  test.each(CASES)("$id: window $point@$decision", async (c) => {
    const t = token();
    const input = `weather then chat ${tag(t)}`;
    h.fake.plan(t, [
      { tool: "weather", args: { city: "Tel Aviv" } },
      { tool: "chat", args: { userInput: input } },
    ]);
    h.fake.answer(t, "Done.");

    const faults = h.services.faults("orchestrator");
    faults.clear(c.point);
    await h.services.restart("orchestrator", faults.crashAt(c.point, c.decision));

    const q = h.query(input);
    const exit = await h.services.waitExit("orchestrator", `crash at ${c.point}@${c.decision}`);
    expect(exit.signal).toBe("SIGKILL");
    expect(faults.hits(c.point, c.decision), `${c.point} hit with decision ${c.decision}`).toBe(1);

    await h.services.start("orchestrator"); // recovery: no fault env
    const res = await q.result;
    await h.settle();
    const cid = res.cid;

    expect(res.final.type).toBe("answer");
    expect(h.observer.events(cid, "FinalAnswerSynthesized")).toHaveLength(1);
    for (const pc of h.observer.events(cid, "PlanCompleted")) {
      expect(pc.value.payload.results.map((r: { toolName: string }) => r.toolName)).toEqual(["weather", "chat"]);
    }
    expect(h.history(cid).map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(h.fake.calls("synth", t)).toHaveLength(1);

    expect(h.observer.requestsFor(cid, 0), "Requested{0}").toHaveLength(c.requested0);
    expect(h.observer.requestsFor(cid, 1), "Requested{1}").toHaveLength(c.requested1);
    expect(h.observer.events(cid, "PlanCompleted"), "PlanCompleted").toHaveLength(c.completed);
    expect(h.observer.dlq().filter((r) => r.conversationId === cid)).toEqual([]);
  });
});
