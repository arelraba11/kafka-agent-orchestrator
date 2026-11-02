import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, existsSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
  isAlive,
  findLiveOwnedPids,
  isOwnedPid,
  listPidFiles,
  readPidFile,
  removePidFile,
  resolveDirs,
  writePidFile,
} from "./pidFiles";

const ROOT = "/Users/someone/projects/kafka-agent-orchestrator";

describe("resolveDirs", () => {
  test("defaults to <repo>/.run and <repo>/scripts/logs/services", () => {
    expect(resolveDirs(ROOT, {})).toEqual({
      runDir: `${ROOT}/.run`,
      logDir: `${ROOT}/scripts/logs/services`,
    });
  });

  test("honors RUN_DIR and LOG_DIR", () => {
    expect(resolveDirs(ROOT, { RUN_DIR: "/tmp/r", LOG_DIR: "/tmp/l" })).toEqual({
      runDir: "/tmp/r",
      logDir: "/tmp/l",
    });
  });

  test("a blank value falls back to the default", () => {
    expect(resolveDirs(ROOT, { RUN_DIR: "  ", LOG_DIR: "" }).runDir).toBe(`${ROOT}/.run`);
  });
});

describe("isOwnedPid", () => {
  test("accepts a command line containing the repo root", () => {
    const ps = () => `bun ${ROOT}/src/node/apps/mathApp.ts`;
    expect(isOwnedPid(process.pid, ROOT, ps)).toBe(true);
  });

  test("rejects other commands", () => {
    expect(isOwnedPid(process.pid, ROOT, () => "sleep 600")).toBe(false);
    expect(isOwnedPid(process.pid, ROOT, () => "bun /Users/other/kafka-agent-orchestrator/x.ts")).toBe(false);
  });

  test("a sibling directory that merely starts with the repo root is not a match", () => {
    expect(isOwnedPid(process.pid, ROOT, () => `bun ${ROOT}-copy/src/x.ts`)).toBe(false);
  });

  test("rejects dead pids without asking ps", async () => {
    const child = Bun.spawn(["true"]);
    await child.exited;
    let asked = false;
    expect(
      isOwnedPid(child.pid, ROOT, () => {
        asked = true;
        return `bun ${ROOT}/x.ts`;
      })
    ).toBe(false);
    expect(asked).toBe(false);
  });

  test("works against the real ps for a live process", () => {
    const child = Bun.spawn(["bash", "-c", "sleep 30; true", ROOT]);
    try {
      expect(isAlive(child.pid)).toBe(true);
      expect(isOwnedPid(child.pid, ROOT)).toBe(true);
      expect(isOwnedPid(child.pid, "/definitely/not/this/repo")).toBe(false);
    } finally {
      child.kill("SIGKILL");
    }
  });
});

describe("pid files", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "pidfiles-"));
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test("write, read, list and remove round-trip, creating the run dir", () => {
    const runDir = join(dir, "nested", "run");
    writePidFile(runDir, "weather", 4242);
    writePidFile(runDir, "supervisor", 1);
    expect(readPidFile(runDir, "weather")).toBe(4242);
    expect(listPidFiles(runDir).sort((a, b) => a.name.localeCompare(b.name))).toEqual([
      { name: "supervisor", pid: 1 },
      { name: "weather", pid: 4242 },
    ]);
    removePidFile(runDir, "weather");
    expect(readPidFile(runDir, "weather")).toBeNull();
    expect(existsSync(join(runDir, "weather.pid"))).toBe(false);
  });

  test("a missing dir or file reads as null / empty, and garbage is ignored", async () => {
    expect(readPidFile(join(dir, "nope"), "x")).toBeNull();
    expect(listPidFiles(join(dir, "nope"))).toEqual([]);
    await Bun.write(join(dir, "bad.pid"), "not-a-number\n");
    expect(readPidFile(dir, "bad")).toBeNull();
    expect(listPidFiles(dir)).toEqual([]);
  });

  test("removing a missing pid file does not throw", () => {
    expect(() => removePidFile(dir, "ghost")).not.toThrow();
  });
});

describe("findLiveOwnedPids", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "liveowned-"));
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  const owned = () => `bun ${ROOT}/src/node/apps/weatherApp.ts`;

  test("a live service pid is found even though the supervisor pid is dead", async () => {
    const dead = Bun.spawn(["true"]);
    await dead.exited;
    writePidFile(dir, "supervisor", dead.pid); // supervisor was killed with -9
    writePidFile(dir, "weather", process.pid); // its child lives on
    expect(findLiveOwnedPids(dir, ROOT, owned)).toEqual([{ name: "weather", pid: process.pid }]);
  });

  test("dead pids, foreign processes and an empty or missing dir are not reported", async () => {
    const dead = Bun.spawn(["true"]);
    await dead.exited;
    writePidFile(dir, "math", dead.pid);
    writePidFile(dir, "chat", process.pid);
    expect(findLiveOwnedPids(dir, ROOT, () => "sleep 600")).toEqual([]);
    rmSync(join(dir, "chat.pid"));
    expect(findLiveOwnedPids(dir, ROOT, owned)).toEqual([]);
    expect(findLiveOwnedPids(join(dir, "nope"), ROOT, owned)).toEqual([]);
  });
});
