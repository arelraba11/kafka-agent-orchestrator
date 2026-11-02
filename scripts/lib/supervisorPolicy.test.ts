import { describe, expect, test } from "bun:test";
import {
  BACKOFF_INITIAL_MS,
  BACKOFF_MAX_MS,
  GIVE_UP_CRASHES,
  GIVE_UP_WINDOW_MS,
  RESET_UPTIME_MS,
  initialRestartState,
  onServiceExit,
  type RestartState,
} from "./supervisorPolicy";

const SHORT = 1_000; // uptime that does not reset the backoff

describe("constants", () => {
  test("match the plan: 1 s → 30 s cap, reset after 60 s, give up at 5 crashes in 2 min", () => {
    expect(BACKOFF_INITIAL_MS).toBe(1_000);
    expect(BACKOFF_MAX_MS).toBe(30_000);
    expect(RESET_UPTIME_MS).toBe(60_000);
    expect(GIVE_UP_CRASHES).toBe(5);
    expect(GIVE_UP_WINDOW_MS).toBe(120_000);
  });
});

describe("backoff", () => {
  test("doubles from 1 s on each quick crash", () => {
    let state: RestartState = initialRestartState;
    const delays: number[] = [];
    // crashes 10 s apart: four restarts, the fifth crash in the window gives up
    for (let i = 0; i < 4; i++) {
      const d = onServiceExit(state, { now: i * 10_000, uptimeMs: SHORT, stopRequested: false });
      if (d.action !== "restart") throw new Error(`expected restart, got ${d.action}`);
      delays.push(d.delayMs);
      state = d.state;
    }
    expect(delays).toEqual([1_000, 2_000, 4_000, 8_000]);
  });

  test("is capped at 30 s", () => {
    // Crashes 130 s apart stay below the give-up threshold, so the backoff keeps growing.
    let state: RestartState = initialRestartState;
    const delays: number[] = [];
    for (let i = 0; i < 8; i++) {
      const d = onServiceExit(state, { now: i * 130_000, uptimeMs: SHORT, stopRequested: false });
      if (d.action !== "restart") throw new Error(`expected restart, got ${d.action}`);
      delays.push(d.delayMs);
      state = d.state;
    }
    expect(delays).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000]);
  });

  test("resets to 1 s after a run of at least 60 s", () => {
    let state: RestartState = initialRestartState;
    for (let i = 0; i < 3; i++) {
      const d = onServiceExit(state, { now: i * 130_000, uptimeMs: SHORT, stopRequested: false });
      if (d.action !== "restart") throw new Error("expected restart");
      state = d.state;
    }
    const healthy = onServiceExit(state, { now: 500_000, uptimeMs: RESET_UPTIME_MS, stopRequested: false });
    expect(healthy.action).toBe("restart");
    if (healthy.action === "restart") expect(healthy.delayMs).toBe(1_000);

    const justBelow = onServiceExit(state, { now: 500_000, uptimeMs: RESET_UPTIME_MS - 1, stopRequested: false });
    if (justBelow.action === "restart") expect(justBelow.delayMs).toBe(8_000);
  });
});

describe("giving up", () => {
  test("gives up on the 5th crash within 2 min", () => {
    let state: RestartState = initialRestartState;
    const actions: string[] = [];
    for (let i = 0; i < 5; i++) {
      const d = onServiceExit(state, { now: i * 20_000, uptimeMs: SHORT, stopRequested: false });
      actions.push(d.action);
      if (d.action === "restart") state = d.state;
    }
    expect(actions).toEqual(["restart", "restart", "restart", "restart", "give-up"]);
  });

  test("crashes older than 2 min do not count", () => {
    let state: RestartState = initialRestartState;
    for (let i = 0; i < 4; i++) {
      const d = onServiceExit(state, { now: i * 10_000, uptimeMs: SHORT, stopRequested: false });
      if (d.action !== "restart") throw new Error("expected restart");
      state = d.state;
    }
    // 4 crashes at t<=30 s; the next is at 30 s + 2 min + 1 ms, so only it is in the window.
    const d = onServiceExit(state, { now: 30_000 + GIVE_UP_WINDOW_MS + 1, uptimeMs: SHORT, stopRequested: false });
    expect(d.action).toBe("restart");
  });
});

describe("requested stop", () => {
  test("never restarts, whatever the history", () => {
    const d = onServiceExit(initialRestartState, { now: 0, uptimeMs: SHORT, stopRequested: true });
    expect(d.action).toBe("none");
  });
});
