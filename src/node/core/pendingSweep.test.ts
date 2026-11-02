import { describe, expect, test } from "bun:test";
import { PENDING_TTL_MS, sweepPending } from "./pendingSweep";

describe("sweepPending", () => {
  test("TTL is 5 minutes", () => {
    expect(PENDING_TTL_MS).toBe(5 * 60_000);
  });

  test("drops entries older than the TTL and keeps newer ones", () => {
    const pending = new Map([
      ["old", { ws: "a", at: 0 }],
      ["edge", { ws: "b", at: 1_000 }],
      ["new", { ws: "c", at: 200_000 }],
    ]);
    const dropped = sweepPending(pending, 1_000 + PENDING_TTL_MS + 1, PENDING_TTL_MS);
    expect(dropped).toEqual(["old", "edge"]);
    expect([...pending.keys()]).toEqual(["new"]);
  });

  test("an entry exactly at the TTL is kept", () => {
    const pending = new Map([["x", { ws: "a", at: 0 }]]);
    expect(sweepPending(pending, PENDING_TTL_MS, PENDING_TTL_MS)).toEqual([]);
    expect(pending.size).toBe(1);
  });
});
