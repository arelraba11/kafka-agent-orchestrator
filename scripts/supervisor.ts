/**
 * supervisor.ts — Run every service as one direct child process and restart it on a crash.
 * Started detached by `bun run start`; stopped by `bun run stop` (SIGTERM).
 *
 * - Services are spawned directly (`bun <abs path>`, `venv/bin/python -u <abs path>`), so each
 *   is a single process, and its command line contains the repo's absolute path (stop.ts
 *   uses that to verify a pid before signalling it).
 * - $RUN_DIR/supervisor.pid and $RUN_DIR/<service>.pid are written here; <service> is the
 *   package.json script name (plus `rag` for the Python worker).
 * - Restart policy (backoff, give-up) is scripts/lib/supervisorPolicy.ts. Events go to
 *   $LOG_DIR/supervisor.log; service logs are opened in append mode.
 */

import { appendFileSync, closeSync, mkdirSync, openSync } from "fs";
import { join } from "path";
import {
  isOwnedPid,
  listPidFiles,
  readPidFile,
  removePidFile,
  resolveDirs,
  writePidFile,
} from "./lib/pidFiles";
import { initialRestartState, onServiceExit, type RestartState } from "./lib/supervisorPolicy";

const ROOT = join(import.meta.dir, "..");
const { runDir: RUN_DIR, logDir: LOG_DIR } = resolveDirs(ROOT, process.env);
const STOP_GRACE_MS = 5_000;

interface ServiceDef {
  /** package.json script name; also the pid file name. */
  name: string;
  cmd: string[];
  logFile: string;
}

const bunService = (name: string, path: string, logFile: string): ServiceDef => ({
  name,
  cmd: ["bun", join(ROOT, path)],
  logFile,
});

const SERVICES: ServiceDef[] = [
  bunService("web", "src/node/core/webServer.ts", "webserver.log"),
  bunService("router", "src/node/core/routerService.ts", "router.log"),
  bunService("orchestrator", "src/node/orchestration/orchestrator.ts", "orchestrator.log"),
  bunService("aggregator", "src/node/orchestration/aggregator.ts", "aggregator.log"),
  bunService("math", "src/node/apps/mathApp.ts", "math.log"),
  bunService("weather", "src/node/apps/weatherApp.ts", "weather.log"),
  bunService("exchange", "src/node/apps/exchangeApp.ts", "exchange.log"),
  bunService("chat", "src/node/apps/generalChatApp.ts", "chat.log"),
  bunService("synthesizer", "src/node/orchestration/answerSynthesizer.ts", "answer.log"),
  {
    name: "rag",
    cmd: [join(ROOT, "src/python/venv/bin/python"), "-u", join(ROOT, "src/python/rag/rag_retriever.py")],
    logFile: "rag.log",
  },
];

mkdirSync(LOG_DIR, { recursive: true });
const SUPERVISOR_LOG = join(LOG_DIR, "supervisor.log");

function log(message: string): void {
  appendFileSync(SUPERVISOR_LOG, `${new Date().toISOString()} [supervisor] ${message}\n`);
}

let stopRequested = false;
const running = new Map<string, ReturnType<typeof Bun.spawn>>();
const restartTimers = new Map<string, ReturnType<typeof setTimeout>>();

function superviseService(def: ServiceDef, state: RestartState = initialRestartState): void {
  if (stopRequested) return;

  const startedAt = Date.now();
  let proc: ReturnType<typeof Bun.spawn> | null = null;
  let spawnError: string | null = null;
  try {
    const fd = openSync(join(LOG_DIR, def.logFile), "a");
    try {
      proc = Bun.spawn(def.cmd, { cwd: ROOT, stdout: fd, stderr: fd });
    } finally {
      closeSync(fd); // the child keeps its own copy
    }
  } catch (err) {
    spawnError = err instanceof Error ? err.message : String(err);
  }

  if (proc) {
    running.set(def.name, proc);
    writePidFile(RUN_DIR, def.name, proc.pid);
    log(`started ${def.name} pid=${proc.pid}`);
  } else {
    log(`failed to spawn ${def.name}: ${spawnError}`);
  }

  const afterExit = (detail: string) => {
    running.delete(def.name);
    removePidFile(RUN_DIR, def.name);
    const now = Date.now();
    const decision = onServiceExit(state, { now, uptimeMs: now - startedAt, stopRequested });
    switch (decision.action) {
      case "none":
        return;
      case "give-up":
        log(`GAVE UP ${def.name} (${detail}; crashed too often, not restarting)`);
        return;
      case "restart": {
        log(`${def.name} exited (${detail}); restarting in ${decision.delayMs}ms`);
        const timer = setTimeout(() => {
          restartTimers.delete(def.name);
          superviseService(def, decision.state);
        }, decision.delayMs);
        restartTimers.set(def.name, timer);
      }
    }
  };

  if (proc) {
    proc.exited.then((code) => afterExit(`exit code ${code}`));
  } else {
    queueMicrotask(() => afterExit("spawn failed"));
  }
}

async function shutdown(signal: string): Promise<void> {
  if (stopRequested) return;
  stopRequested = true;
  log(`${signal} received; stopping ${running.size} service(s)`);
  for (const timer of restartTimers.values()) clearTimeout(timer);
  restartTimers.clear();

  for (const proc of running.values()) proc.kill("SIGTERM");

  const allExited = Promise.all([...running.values()].map((p) => p.exited));
  const timedOut = await Promise.race([
    allExited.then(() => false),
    Bun.sleep(STOP_GRACE_MS).then(() => true),
  ]);
  if (timedOut) {
    for (const [name, proc] of running) {
      log(`SIGKILL ${name} pid=${proc.pid} (did not exit within ${STOP_GRACE_MS}ms)`);
      proc.kill("SIGKILL");
    }
    await allExited;
  }

  for (const { name } of listPidFiles(RUN_DIR)) removePidFile(RUN_DIR, name);
  log("stopped");
  process.exit(0);
}

const existing = readPidFile(RUN_DIR, "supervisor");
if (existing !== null && existing !== process.pid && isOwnedPid(existing, ROOT)) {
  console.error(`[supervisor] already running (pid ${existing}); refusing to start a second one`);
  process.exit(1);
}

writePidFile(RUN_DIR, "supervisor", process.pid);
log(`supervisor pid=${process.pid} run=${RUN_DIR} logs=${LOG_DIR}`);
process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

for (const def of SERVICES) superviseService(def);
