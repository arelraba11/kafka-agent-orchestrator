/**
 * stop.ts — Stop the services started by `bun run start`
 * Usage: bun run stop
 *
 * Signals only pids recorded in $RUN_DIR (default <repo>/.run) whose command line contains
 * this repo's path. No pkill, no vite kill, no LevelDB LOCK deletion.
 */

import { join } from "path";
import { isAlive, isOwnedPid, listPidFiles, readPidFile, removePidFile, resolveDirs } from "./lib/pidFiles";

const ROOT = join(import.meta.dir, "..");
const { runDir: RUN_DIR } = resolveDirs(ROOT, process.env);

// The supervisor waits up to 5 s for its children before SIGKILL, so allow it a little longer.
const SUPERVISOR_EXIT_WAIT_MS = 10_000;
const CHILD_EXIT_WAIT_MS = 5_000;

async function waitForExit(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isAlive(pid)) return true;
    await Bun.sleep(100);
  }
  return !isAlive(pid);
}

/** SIGTERM, wait, SIGKILL — for a pid that already passed the ownership check. */
async function terminate(label: string, pid: number, waitMs: number): Promise<void> {
  process.kill(pid, "SIGTERM");
  console.log(`  [SIGTERM] ${label} (pid ${pid})`);
  if (await waitForExit(pid, waitMs)) return;
  try {
    process.kill(pid, "SIGKILL");
    console.log(`  [SIGKILL] ${label} (pid ${pid}, did not exit cleanly)`);
  } catch {
    // exited in between
  }
}

console.log("\n=== Stopping project services ===\n");

// 1. The supervisor stops its own children and removes the pid files.
const supervisorPid = readPidFile(RUN_DIR, "supervisor");
if (supervisorPid === null) {
  console.log("  [skip]    supervisor — not running");
} else if (!isAlive(supervisorPid)) {
  console.log(`  [skip]    supervisor — pid ${supervisorPid} is not running (stale pid file)`);
} else if (isOwnedPid(supervisorPid, ROOT)) {
  await terminate("supervisor", supervisorPid, SUPERVISOR_EXIT_WAIT_MS);
} else {
  console.log(`  [skip]    supervisor — pid ${supervisorPid} is not a process of this repo`);
}
removePidFile(RUN_DIR, "supervisor");

// 2. Fallback for children the supervisor did not get to (e.g. it was killed with -9).
for (const { name, pid } of listPidFiles(RUN_DIR)) {
  if (isOwnedPid(pid, ROOT)) {
    await terminate(name, pid, CHILD_EXIT_WAIT_MS);
  } else if (isAlive(pid)) {
    console.log(`  [skip]    ${name} — pid ${pid} is not a process of this repo (stale pid file)`);
  }
  removePidFile(RUN_DIR, name);
}

console.log("\n=== Done ===\n");
