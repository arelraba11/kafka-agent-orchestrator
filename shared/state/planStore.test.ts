import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { StoreError } from "../kafka/errors";
import { closeStore, getPlan, initializeStore, listRunningPlans, savePlan, type PlanState } from "./planStore";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";

const STATE: PlanState = {
  plan: [{ tool: "math", args: { expression: "1+1" } }],
  stepIndex: 0,
  results: [],
  status: "running",
  stepDeadline: 5_000,
  planReceivedAt: 1,
  sessionId: "s",
};

let dir: string;

beforeEach(async () => {
  dir = mkdtempSync(join(tmpdir(), "planstore-test-"));
  await initializeStore(join(dir, "db"));
});

afterEach(async () => {
  await closeStore();
  rmSync(dir, { recursive: true, force: true });
});

describe("planStore", () => {
  test("a missing key resolves null: not a rejection, not a StoreError", async () => {
    expect(await getPlan(CID)).toBeNull();
  });

  test("savePlan then getPlan round-trips the state", async () => {
    await savePlan(CID, STATE);
    expect(await getPlan(CID)).toEqual(STATE);
  });

  test("a plan stored before stepDeadline existed reads back with stepDeadline null", async () => {
    const { stepDeadline: _omit, ...legacy } = STATE;
    await savePlan(CID, legacy as PlanState);
    expect((await getPlan(CID))!.stepDeadline).toBeNull();
  });

  test("listRunningPlans returns only running plans (tombstones excluded), legacy ones with a null deadline", async () => {
    const OTHER = "11111111-1111-4111-8111-111111111111";
    const DONE = "22222222-2222-4222-8222-222222222222";
    const LEGACY = "33333333-3333-4333-8333-333333333333";
    const { stepDeadline: _omit, ...legacy } = STATE;
    await savePlan(CID, STATE);
    await savePlan(OTHER, { ...STATE, stepDeadline: 9_000 });
    await savePlan(DONE, { ...STATE, status: "completed", stepDeadline: null });
    await savePlan(LEGACY, legacy as PlanState);
    const running = await listRunningPlans();
    expect(running.map((r) => r.conversationId).sort()).toEqual([CID, OTHER, LEGACY].sort());
    expect(running.find((r) => r.conversationId === LEGACY)!.state.stepDeadline).toBeNull();
    expect(running.find((r) => r.conversationId === OTHER)!.state.stepDeadline).toBe(9_000);
  });

  test("an empty store lists no running plans", async () => {
    expect(await listRunningPlans()).toEqual([]);
  });

  test("after closeStore, listRunningPlans rejects with StoreError", async () => {
    await closeStore();
    await expect(listRunningPlans()).rejects.toBeInstanceOf(StoreError);
  });

  test("after closeStore, getPlan and savePlan reject with StoreError", async () => {
    await savePlan(CID, STATE);
    await closeStore();
    await expect(getPlan(CID)).rejects.toBeInstanceOf(StoreError);
    await expect(savePlan(CID, STATE)).rejects.toBeInstanceOf(StoreError);
  });
});
