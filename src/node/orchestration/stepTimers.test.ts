import { describe, expect, test } from "bun:test";
import type { StepTimedOut } from "../../../shared/schemas/StepTimedOut";
import type { PlanState } from "../../../shared/state/planStore";
import { REARM_DELAY_MS, createStepTimers, rescheduleRunning } from "./stepTimers";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const CID2 = "11111111-1111-4111-8111-111111111111";

/** Manual clock + timer queue; advance() runs due timers in order. */
function rig() {
  let clock = 10_000;
  let nextId = 1;
  const timers = new Map<number, { at: number; fn: () => void }>();
  const published: StepTimedOut[] = [];
  const control = { failPublishes: 0 };
  const warnings: string[] = [];

  const deps = {
    setTimer: (fn: () => void, ms: number) => {
      const id = nextId++;
      timers.set(id, { at: clock + ms, fn });
      return id;
    },
    clearTimer: (h: unknown) => void timers.delete(h as number),
    now: () => clock,
    publish: async (e: StepTimedOut) => {
      if (control.failPublishes > 0) {
        control.failPublishes--;
        throw new Error("kafka down");
      }
      published.push(e);
    },
    log: { log: () => {}, warn: (m: string) => void warnings.push(m) },
  };

  async function advance(ms: number) {
    clock += ms;
    for (;;) {
      const due = [...timers.entries()].filter(([, t]) => t.at <= clock).sort((a, b) => a[1].at - b[1].at)[0];
      if (!due) break;
      timers.delete(due[0]);
      due[1].fn();
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    }
  }

  return { timers: createStepTimers(deps), published, control, warnings, advance, pending: () => timers.size, now: () => clock };
}

describe("schedule / replace / cancel", () => {
  test("schedule publishes StepTimedOut with the stored deadline when the timer fires", async () => {
    const r = rig();
    r.timers.schedule(CID, 0, r.now() + 30_000);
    await r.advance(29_999);
    expect(r.published).toHaveLength(0);
    await r.advance(1);
    expect(r.published).toEqual([
      { conversationId: CID, timestamp: r.now(), eventType: "StepTimedOut", payload: { stepIndex: 0, deadline: 40_000 } },
    ]);
    expect(r.timers.size).toBe(0);
  });

  test("a second schedule for the same conversation replaces the first", async () => {
    const r = rig();
    r.timers.schedule(CID, 0, r.now() + 30_000);
    r.timers.schedule(CID, 1, r.now() + 60_000);
    expect(r.timers.size).toBe(1);
    expect(r.pending()).toBe(1);
    await r.advance(30_000);
    expect(r.published).toHaveLength(0); // the old step-0 timer is gone
    await r.advance(30_000);
    expect(r.published).toHaveLength(1);
    expect(r.published[0].payload).toEqual({ stepIndex: 1, deadline: 70_000 });
  });

  test("cancel prevents the timer from firing", async () => {
    const r = rig();
    r.timers.schedule(CID, 0, r.now() + 30_000);
    r.timers.cancel(CID);
    expect(r.timers.size).toBe(0);
    await r.advance(60_000);
    expect(r.published).toHaveLength(0);
  });

  test("cancel of an unknown conversation is a no-op", () => {
    const r = rig();
    r.timers.cancel(CID);
    expect(r.timers.size).toBe(0);
  });

  test("conversations are independent", async () => {
    const r = rig();
    r.timers.schedule(CID, 0, r.now() + 10_000);
    r.timers.schedule(CID2, 2, r.now() + 20_000);
    r.timers.cancel(CID);
    await r.advance(25_000);
    expect(r.published.map((e) => e.conversationId)).toEqual([CID2]);
  });

  test("an overdue deadline fires immediately", async () => {
    const r = rig();
    r.timers.schedule(CID, 0, r.now() - 5_000);
    await r.advance(0);
    expect(r.published).toHaveLength(1);
    expect(r.published[0].payload.deadline).toBe(5_000); // the stored deadline, not now
  });
});

describe("publish failure", () => {
  test("a failed publish is logged and re-armed after 5 s", async () => {
    const r = rig();
    r.control.failPublishes = 1;
    r.timers.schedule(CID, 0, r.now() + 1_000);
    await r.advance(1_000);
    expect(r.published).toHaveLength(0);
    expect(r.warnings).toHaveLength(1);
    expect(r.warnings[0]).toContain("re-arming in 5000ms");
    expect(r.timers.size).toBe(1); // still armed

    await r.advance(REARM_DELAY_MS - 1);
    expect(r.published).toHaveLength(0);
    await r.advance(1);
    expect(r.published).toHaveLength(1);
    expect(r.published[0].payload).toEqual({ stepIndex: 0, deadline: 11_000 }); // same deadline
    expect(r.timers.size).toBe(0);
  });

  test("keeps re-arming while publishes keep failing", async () => {
    const r = rig();
    r.control.failPublishes = 3;
    r.timers.schedule(CID, 0, r.now());
    await r.advance(0);
    await r.advance(REARM_DELAY_MS);
    await r.advance(REARM_DELAY_MS);
    expect(r.published).toHaveLength(0);
    await r.advance(REARM_DELAY_MS);
    expect(r.published).toHaveLength(1);
  });

  test("a cancel while a re-arm is pending stops it", async () => {
    const r = rig();
    r.control.failPublishes = 1;
    r.timers.schedule(CID, 0, r.now());
    await r.advance(0);
    r.timers.cancel(CID);
    await r.advance(60_000);
    expect(r.published).toHaveLength(0);
  });
});

describe("startup scan", () => {
  const plan = (over: Partial<PlanState>): PlanState => ({
    plan: [{ tool: "math", args: {} }],
    stepIndex: 0,
    results: [],
    status: "running",
    stepDeadline: null,
    planReceivedAt: 1,
    sessionId: "s",
    ...over,
  });

  test("future deadlines fire at the stored deadline", async () => {
    const r = rig();
    const n = rescheduleRunning(r.timers, [{ conversationId: CID, state: plan({ stepDeadline: r.now() + 7_000, stepIndex: 1 }) }], r.now(), 30_000);
    expect(n).toBe(1);
    await r.advance(6_999);
    expect(r.published).toHaveLength(0);
    await r.advance(1);
    expect(r.published[0].payload).toEqual({ stepIndex: 1, deadline: 17_000 });
  });

  test("overdue deadlines fire immediately", async () => {
    const r = rig();
    rescheduleRunning(r.timers, [{ conversationId: CID, state: plan({ stepDeadline: r.now() - 1 }) }], r.now(), 30_000);
    await r.advance(0);
    expect(r.published).toHaveLength(1);
  });

  test("a legacy plan without a deadline gets now + stepTimeoutMs", async () => {
    const r = rig();
    const legacy = plan({});
    delete (legacy as Partial<PlanState>).stepDeadline;
    rescheduleRunning(r.timers, [{ conversationId: CID, state: legacy }], r.now(), 30_000);
    await r.advance(29_999);
    expect(r.published).toHaveLength(0);
    await r.advance(1);
    expect(r.published[0].payload.deadline).toBe(40_000);
  });

  test("returns the count and ignores non-running plans", () => {
    const r = rig();
    const n = rescheduleRunning(
      r.timers,
      [
        { conversationId: CID, state: plan({ stepDeadline: r.now() + 1 }) },
        { conversationId: CID2, state: plan({ status: "completed" }) },
      ],
      r.now(),
      30_000
    );
    expect(n).toBe(1);
    expect(r.timers.size).toBe(1);
  });

  test("zero running plans arms nothing", () => {
    const r = rig();
    expect(rescheduleRunning(r.timers, [], r.now(), 30_000)).toBe(0);
  });
});
