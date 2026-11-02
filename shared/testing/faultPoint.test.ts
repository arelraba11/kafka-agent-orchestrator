import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { createFaultPoint, parseFaultSpec } from "./faultPoint";

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "fault-test-"));
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

const hits = (point: string) =>
  existsSync(join(dir, `${point}.hits`)) ? readFileSync(join(dir, `${point}.hits`), "utf8").trim().split("\n") : [];

describe("parseFaultSpec", () => {
  test("a bare point", () => expect(parseFaultSpec("orchestrator:after-emit")).toEqual({ point: "orchestrator:after-emit" }));
  test("point@decision", () =>
    expect(parseFaultSpec("orchestrator:after-emit@advanced")).toEqual({ point: "orchestrator:after-emit", decision: "advanced" }));
  test("unset or empty is null", () => {
    expect(parseFaultSpec(undefined)).toBeNull();
    expect(parseFaultSpec("")).toBeNull();
  });
});

describe("faultPoint without FAULT_DIR", () => {
  test("is a no-op: no files, no crash, no hold, even with FAULT_CRASH set", async () => {
    const fp = createFaultPoint({ FAULT_CRASH: "orchestrator:after-emit" });
    await fp("orchestrator:after-emit");
    expect(readdirSync(dir)).toEqual([]);
  });
});

describe("count", () => {
  test("every call appends one line to <point>.hits, whatever the decision", async () => {
    const fp = createFaultPoint({ FAULT_DIR: dir });
    await fp("kafka:retry");
    await fp("kafka:retry", { decision: "started" });
    await fp("orchestrator:after-emit", { decision: "advanced" });
    expect(hits("kafka:retry")).toHaveLength(2);
    expect(hits("orchestrator:after-emit")).toHaveLength(1);
  });
});

describe("crash", () => {
  test("SIGKILLs the process at the named point (child process)", async () => {
    const code = `
      import { faultPoint } from ${JSON.stringify(join(import.meta.dir, "faultPoint"))};
      await faultPoint("orchestrator:after-emit", { decision: "started" });
      console.log("survived");`;
    const run = async (crash: string) => {
      const p = Bun.spawn(["bun", "--no-env-file", "-e", code], {
        env: { PATH: process.env.PATH ?? "", FAULT_DIR: dir, FAULT_CRASH: crash },
        stdout: "pipe",
        stderr: "pipe",
      });
      const out = await new Response(p.stdout).text();
      await p.exited;
      return { out, signal: p.signalCode };
    };

    const hit = await run("orchestrator:after-emit@started");
    expect(hit.signal).toBe("SIGKILL");
    expect(hit.out).not.toContain("survived");
    expect(hits("orchestrator:after-emit")).toHaveLength(1); // counted before dying

    const otherDecision = await run("orchestrator:after-emit@completed");
    expect(otherDecision.signal).toBeNull();
    expect(otherDecision.out).toContain("survived");

    const otherPoint = await run("orchestrator:after-persist");
    expect(otherPoint.out).toContain("survived");
  });
});

describe("hold", () => {
  test("writes .reached and waits for .release; only the first matching call holds", async () => {
    const fp = createFaultPoint({ FAULT_DIR: dir, FAULT_HOLD: "orchestrator:before-emit@started" });

    let first = false;
    const held = fp("orchestrator:before-emit", { decision: "started" }).then(() => (first = true));
    await Bun.sleep(100);
    expect(existsSync(join(dir, "orchestrator:before-emit.reached"))).toBe(true);
    expect(first).toBe(false);

    writeFileSync(join(dir, "orchestrator:before-emit.release"), "");
    await held;
    expect(first).toBe(true);

    // the second matching call passes straight through
    await fp("orchestrator:before-emit", { decision: "started" });
    expect(hits("orchestrator:before-emit")).toHaveLength(2);
  });

  test("a non-matching decision neither holds nor writes .reached", async () => {
    const fp = createFaultPoint({ FAULT_DIR: dir, FAULT_HOLD: "orchestrator:before-emit@started" });
    await fp("orchestrator:before-emit", { decision: "advanced" });
    await fp("orchestrator:before-emit");
    expect(existsSync(join(dir, "orchestrator:before-emit.reached"))).toBe(false);
  });
});
