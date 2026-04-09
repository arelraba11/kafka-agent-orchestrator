/**
 * stop.ts — Stop all Final Project services
 * Usage: bun run stop
 */

import { join } from "path";
import { existsSync, rmSync } from "fs";

const ROOT = join(import.meta.dir, "..");

async function killGraceful(label: string, pattern: string): Promise<void> {
  const term = Bun.spawn(["pkill", "-TERM", "-f", pattern], { cwd: ROOT });
  await term.exited;

  if (term.exitCode === 0) {
    console.log(`  [SIGTERM] ${label}`);
    await Bun.sleep(1000);
    const kill = Bun.spawn(["pkill", "-KILL", "-f", pattern], { cwd: ROOT });
    await kill.exited;
    if (kill.exitCode === 0) {
      console.log(`  [SIGKILL] ${label} (did not exit cleanly)`);
    }
  } else {
    console.log(`  [skip]    ${label} — not running`);
  }
}

console.log("\n=== Stopping all project services ===\n");

await killGraceful("bun services",    "bun run");
await killGraceful("bun ts runner",   "bun src/");
await killGraceful("vite dev server", "vite");
await killGraceful("rag_retriever",   "rag_retriever.py");

// LevelDB lock cleanup
const lockFile = join(ROOT, ".plan-store/LOCK");
if (existsSync(lockFile)) {
  rmSync(lockFile);
  console.log("  [clean]   removed stale LevelDB lock (.plan-store/LOCK)");
}

console.log("\n=== All services stopped ===\n");
