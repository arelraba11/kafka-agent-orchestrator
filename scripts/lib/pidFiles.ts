// Side-effect-free helpers for the supervisor's pid files and the ownership check that
// stop.ts relies on. RUN_DIR / LOG_DIR are read here so start, the supervisor and stop
// agree on where things live.

import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "fs";
import { join, resolve } from "path";

type Env = Record<string, string | undefined>;

export interface Dirs {
  runDir: string;
  logDir: string;
}

export function resolveDirs(repoRoot: string, env: Env): Dirs {
  const pick = (value: string | undefined, fallback: string) =>
    value && value.trim() ? resolve(repoRoot, value.trim()) : fallback;
  return {
    runDir: pick(env.RUN_DIR, join(repoRoot, ".run")),
    logDir: pick(env.LOG_DIR, join(repoRoot, "scripts/logs/services")),
  };
}

const pidPath = (runDir: string, name: string) => join(runDir, `${name}.pid`);

export function writePidFile(runDir: string, name: string, pid: number): void {
  mkdirSync(runDir, { recursive: true });
  writeFileSync(pidPath(runDir, name), `${pid}\n`);
}

function parsePid(text: string): number | null {
  const n = Number(text.trim());
  return Number.isInteger(n) && n > 0 ? n : null;
}

export function readPidFile(runDir: string, name: string): number | null {
  try {
    return parsePid(readFileSync(pidPath(runDir, name), "utf8"));
  } catch {
    return null;
  }
}

export function removePidFile(runDir: string, name: string): void {
  rmSync(pidPath(runDir, name), { force: true });
}

export function listPidFiles(runDir: string): { name: string; pid: number }[] {
  let files: string[];
  try {
    files = readdirSync(runDir);
  } catch {
    return [];
  }
  const out: { name: string; pid: number }[] = [];
  for (const file of files) {
    if (!file.endsWith(".pid")) continue;
    const name = file.slice(0, -".pid".length);
    const pid = readPidFile(runDir, name);
    if (pid !== null) out.push({ name, pid });
  }
  return out;
}

export function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === "EPERM"; // exists, not ours
  }
}

/** The full command line of a pid, or null if there is none. */
export function psCommand(pid: number): string | null {
  const res = Bun.spawnSync(["ps", "-o", "command=", "-p", String(pid)]);
  if (res.exitCode !== 0) return null;
  const text = res.stdout.toString().trim();
  return text || null;
}

/**
 * True only for a live process whose command line contains this repo's absolute path.
 * Guards against pid reuse: a pid file that now points at some unrelated process is
 * never signalled. The match is on the path followed by `/` or end/whitespace, so
 * `<root>-copy` is not `<root>`.
 */
export function isOwnedPid(
  pid: number,
  repoRoot: string,
  ps: (pid: number) => string | null = psCommand
): boolean {
  if (!isAlive(pid)) return false;
  const command = ps(pid);
  if (!command) return false;
  let from = 0;
  for (;;) {
    const at = command.indexOf(repoRoot, from);
    if (at === -1) return false;
    const next = command[at + repoRoot.length];
    if (next === undefined || next === "/" || /\s/.test(next)) return true;
    from = at + 1;
  }
}

/**
 * Every pid file in `runDir` (supervisor and services) whose process is alive and owned by
 * this repo. `start` refuses to run while this is non-empty: a supervisor killed with -9
 * leaves its children running, and a second set would fight them for ports and groups.
 */
export function findLiveOwnedPids(
  runDir: string,
  repoRoot: string,
  ps: (pid: number) => string | null = psCommand
): { name: string; pid: number }[] {
  return listPidFiles(runDir).filter(({ pid }) => isOwnedPid(pid, repoRoot, ps));
}
