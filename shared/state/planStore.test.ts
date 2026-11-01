import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { StoreError } from "../kafka/errors";
import { closeStore, getPlan, initializeStore, savePlan, type PlanState } from "./planStore";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";

const STATE: PlanState = {
  plan: [{ tool: "math", args: { expression: "1+1" } }],
  stepIndex: 0,
  results: [],
  status: "running",
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

  test("after closeStore, getPlan and savePlan reject with StoreError", async () => {
    await savePlan(CID, STATE);
    await closeStore();
    await expect(getPlan(CID)).rejects.toBeInstanceOf(StoreError);
    await expect(savePlan(CID, STATE)).rejects.toBeInstanceOf(StoreError);
  });
});
