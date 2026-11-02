import { afterAll, describe, expect, test } from "bun:test";
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readdirSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { REPO_ROOT } from "./harness/broker";
import { FAKE_API_KEY } from "./harness/fakeLlm";
import { assertRagPrerequisites } from "./harness/services";
import { tag, token, useHarness, waitFor } from "./harness";

// SV1 needs the supervisor from phase 4 Part B. Without it the file is skipped, with a reason.
const SUPERVISOR = join(REPO_ROOT, "scripts/supervisor.ts");
const HAS_SUPERVISOR = existsSync(SUPERVISOR);
if (!HAS_SUPERVISOR) console.log("[int] SV1 skipped: scripts/supervisor.ts does not exist (phase 4 Part B is absent)");

// Every supervised service, by pid-file name (scripts/supervisor.ts).
const SUPERVISED = ["web", "router", "orchestrator", "aggregator", "math", "weather", "exchange", "chat", "synthesizer", "rag"];

const isAlive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

describe.skipIf(!HAS_SUPERVISOR)("supervisor", () => {
  let runDir = "";
  let logDir = "";
  let supervisor: ReturnType<typeof Bun.spawn> | undefined;
  const seenPids = new Set<number>(); // every supervised pid we have read, to reap strays even if stop fails

  // Registered BEFORE useHarness so it runs first: the harness's afterAll deletes the temp dir
  // (the pid files) and would leave the ten supervised processes running.
  afterAll(async () => {
    if (runDir && existsSync(runDir)) await stopSupervised();
    supervisor?.kill("SIGKILL");
    for (const pid of seenPids) if (isAlive(pid)) process.kill(pid, "SIGKILL");
  }, 60_000);

  // No service is started by the harness: the supervisor starts all of them.
  const h = useHarness("supervisor", { start: [] });

  const readPid = (name: string): number | undefined => {
    try {
      const n = Number(readFileSync(join(runDir, `${name}.pid`), "utf8").trim());
      return Number.isInteger(n) && n > 0 ? n : undefined;
    } catch {
      return undefined;
    }
  };

  /** `bun run stop`, aimed at the suite's own RUN_DIR (never the dev stack's .run). */
  async function stopSupervised() {
    expect(runDir.startsWith(tmpdir()) || runDir.includes("/int-supervisor-"), `RUN_DIR ${runDir} is the suite's`).toBe(true);
    const p = Bun.spawn([process.execPath, "--no-env-file", join(REPO_ROOT, "scripts/stop.ts")], {
      cwd: REPO_ROOT,
      env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", RUN_DIR: runDir, LOG_DIR: logDir },
      stdout: "ignore",
      stderr: "ignore",
    });
    await p.exited;
  }

  test("SV1: the supervisor restarts a crashed worker and the system keeps answering", async () => {
    assertRagPrerequisites(); // the supervisor starts the Python worker too
    runDir = join(h.ns.dir, "run");
    logDir = join(h.ns.logDir, "supervised");
    mkdirSync(runDir, { recursive: true });
    mkdirSync(logDir, { recursive: true });

    // The supervisor hands one env to all its services, so router and synthesizer share one fake
    // endpoint (the router stage); a conversation's script is [plan, answer], consumed in call order.
    const env = h.services.sharedEnv({
      RUN_DIR: runDir,
      LOG_DIR: logDir,
      OPENAI_API_KEY: FAKE_API_KEY, // beats the repo's .env, which the supervised children still load
      OPENAI_BASE_URL: `${h.fake.baseUrl}/router/v1`,
      PATH: `${dirname(process.execPath)}:${process.env.PATH ?? ""}`,
    });
    const fd = openSync(join(logDir, "supervisor.stdout.log"), "a");
    supervisor = Bun.spawn([process.execPath, "--no-env-file", SUPERVISOR], { cwd: REPO_ROOT, env, stdin: "ignore", stdout: fd, stderr: fd });
    closeSync(fd);

    await waitFor(() => SUPERVISED.every((n) => readPid(n) !== undefined), {
      timeoutMs: 30_000,
      what: `the supervisor to write a pid file for every service (${SUPERVISED.join(", ")})`,
      context: () => `  run dir: ${readdirSync(runDir).join(", ")}`,
    });
    // the group that will be killed is up before we kill it
    const before = await waitFor(() => h.services.groupMembers("weather"), {
      timeoutMs: 30_000,
      what: "the weather-tool-worker group to be stable under the supervisor",
    });
    const oldPid = readPid("weather")!;
    const allPids = Object.fromEntries(["supervisor", ...SUPERVISED].map((n) => [n, readPid(n)!]));
    for (const pid of Object.values(allPids)) seenPids.add(pid);

    process.kill(oldPid, "SIGKILL"); // kill -9 $(cat $RUN_DIR/weather.pid)

    const newPid = await waitFor(() => {
      const pid = readPid("weather");
      return pid !== undefined && pid !== oldPid ? pid : undefined;
    }, { timeoutMs: 20_000, what: `a new pid in ${join(runDir, "weather.pid")} (was ${oldPid})` });
    seenPids.add(newPid);
    expect(isAlive(newPid)).toBe(true);
    // stable again, and the dead member (which lingers until its session expires) is gone
    await waitFor(async () => {
      const m = await h.services.groupMembers("weather");
      return m !== null && [...m].every((id) => !before.has(id));
    }, { timeoutMs: 30_000, what: "the weather-tool-worker group to be stable again with only the new member" });

    const t = token();
    const input = `weather in Tel Aviv ${tag(t)}`;
    h.fake.script("router", t, [
      { ok: JSON.stringify({ steps: [{ tool: "weather", args: { city: "Tel Aviv" } }] }) },
      { ok: "Sunny in Tel Aviv." },
    ]);
    const res = await h.ask(input);
    expect(res.final).toMatchObject({ type: "answer", answer: "Sunny in Tel Aviv." });
    expect(h.observer.resultsFor(res.cid, 0)).toHaveLength(1);

    await stopSupervised();
    await waitFor(() => Object.values(allPids).filter((p) => p !== oldPid).every((p) => !isAlive(p)) && !isAlive(newPid), {
      timeoutMs: 20_000,
      what: "every supervised process to be gone after stop",
      context: () => `  alive: ${Object.entries({ ...allPids, weather: newPid }).filter(([, p]) => isAlive(p)).map(([n, p]) => `${n}=${p}`).join(" ")}`,
    });
    expect(readdirSync(runDir).filter((f) => f.endsWith(".pid"))).toEqual([]);
  }, 120_000);
});
