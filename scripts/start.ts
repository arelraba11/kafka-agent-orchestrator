/**
 * start.ts — Launch all services
 * Usage: bun run start
 */

import net from "net";
import { spawn } from "child_process";
import { join } from "path";
import { mkdirSync, openSync, closeSync } from "fs";
import { DEFAULT_OLLAMA_MODEL } from "../shared/llm/providers";
import { findLiveOwnedPids, isOwnedPid, readPidFile, resolveDirs } from "./lib/pidFiles";
import { hasModel, shouldPullModel } from "./lib/startChecks";

const ROOT = join(import.meta.dir, "..");
const { runDir: RUN_DIR, logDir: LOG_DIR } = resolveDirs(ROOT, process.env);

// ─── Already running? ─────────────────────────────────────────────────────────

const alreadyRunning = findLiveOwnedPids(RUN_DIR, ROOT);
if (alreadyRunning.length > 0) {
  const list = alreadyRunning.map(({ name, pid }) => `${name}=${pid}`).join(" ");
  console.error(`ERROR: already running (${list}; pid files in ${RUN_DIR}). Run 'bun run stop' first.`);
  process.exit(1);
}

// ─── Kafka readiness ──────────────────────────────────────────────────────────

function isKafkaReachable(): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: "localhost", port: 9092 });
    const timer = setTimeout(() => { socket.destroy(); resolve(false); }, 1000);
    socket.on("connect", () => { clearTimeout(timer); socket.destroy(); resolve(true); });
    socket.on("error",   () => { clearTimeout(timer); resolve(false); });
  });
}

async function waitForKafka(maxWaitSec = 60): Promise<void> {
  console.log("Waiting for Kafka at localhost:9092...");
  let elapsed = 0;
  while (!(await isKafkaReachable())) {
    if (elapsed >= maxWaitSec) {
      console.error(`ERROR: Kafka not reachable after ${maxWaitSec}s. Is 'docker-compose up -d' running?`);
      process.exit(1);
    }
    await Bun.sleep(2000);
    elapsed += 2;
  }
  console.log("Kafka is reachable. Waiting 3s for metadata to settle...");
  await Bun.sleep(3000);
}

// ─── Topics ───────────────────────────────────────────────────────────────────

async function ensureTopics(): Promise<void> {
  console.log("Creating/verifying topics...");
  const proc = Bun.spawn(["bash", "infra/create-topics.sh"], {
    cwd: ROOT,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (proc.exitCode !== 0) {
    console.error(`ERROR: topic creation failed (exit ${proc.exitCode}). Is the 'kafka' container running?`);
    console.error(`${stdout}${stderr}`.trim());
    process.exit(1);
  }
  console.log("Topics ready.");
}

// ─── Ollama model pull ────────────────────────────────────────────────────────

async function ensureOllamaModel(provider: string): Promise<void> {
  if (!shouldPullModel(provider)) {
    console.log(`Skipping Ollama model pull (LLM_PROVIDER=${provider}).`);
    return;
  }

  const model = process.env.OLLAMA_MODEL ?? DEFAULT_OLLAMA_MODEL;
  const host  = process.env.OLLAMA_HOST  ?? "http://localhost:11434";

  const fail = (why: string): void => {
    if (provider === "ollama") {
      console.error(`ERROR: ${why} LLM_PROVIDER=ollama has no fallback.`);
      process.exit(1);
    }
    console.warn(`WARNING: ${why} Calls will fall back to OpenAI.`);
  };

  console.log(`Checking Ollama at ${host}...`);
  try {
    const tags = await fetch(`${host}/api/tags`);
    if (!tags.ok) throw new Error(`/api/tags returned status ${tags.status}`);
    if (hasModel(await tags.json(), model)) {
      console.log(`Ollama model '${model}' already present.`);
      return;
    }

    console.log(`Pulling Ollama model '${model}' (this can take a while)...`);
    const res = await fetch(`${host}/api/pull`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: model, stream: false }),
    });
    if (!res.ok) throw new Error(`pull returned status ${res.status}: ${(await res.text()).trim()}`);
    console.log(`Ollama model '${model}' ready.`);
  } catch (err) {
    fail(`Ollama model '${model}' is not available at ${host} (${err instanceof Error ? err.message : String(err)}).`);
  }
}

// ─── Supervisor ───────────────────────────────────────────────────────────────

async function startSupervisor(): Promise<void> {
  mkdirSync(LOG_DIR, { recursive: true });
  const fd = openSync(join(LOG_DIR, "supervisor.log"), "a");
  const child = spawn("bun", [join(ROOT, "scripts/supervisor.ts")], {
    cwd: ROOT,
    detached: true,
    stdio: ["ignore", fd, fd],
    env: { ...process.env, RUN_DIR, LOG_DIR },
  });
  closeSync(fd);
  child.unref();

  // The supervisor writes its pid file once it is up; wait for it so a failed start is loud.
  for (let i = 0; i < 50; i++) {
    const pid = readPidFile(RUN_DIR, "supervisor");
    if (pid !== null && isOwnedPid(pid, ROOT)) {
      console.log(`Supervisor started (PID ${pid}); services start under it.`);
      return;
    }
    await Bun.sleep(100);
  }
  console.error(`ERROR: the supervisor did not come up. See ${join(LOG_DIR, "supervisor.log")}.`);
  process.exit(1);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

await waitForKafka();
await ensureTopics();
await ensureOllamaModel(process.env.LLM_PROVIDER?.trim() || "auto");
await startSupervisor();

console.log(`
SERVICES STARTED
Logs in ${LOG_DIR}/ (append mode; supervisor events in supervisor.log)
Pids in ${RUN_DIR}/

Web server: http://localhost:3001  (log: webserver.log)

Open the Web UI:
  http://localhost:3001

  Or for hot-reload dev mode:
    bun run web:dev    # Vite dev server at http://localhost:5173

To stop all services:
  bun run stop
`);
