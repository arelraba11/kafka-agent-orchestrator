/**
 * start.ts — Launch all Final Project services
 * Usage: bun run start
 */

import net from "net";
import { join } from "path";
import { mkdirSync, openSync, closeSync } from "fs";

const ROOT    = join(import.meta.dir, "..");
const LOG_DIR = join(ROOT, "scripts/logs/final-project-services");

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
  const proc = Bun.spawn(["bash", "infra/topics-final.sh"], {
    cwd: ROOT,
    stdout: "ignore",
    stderr: "ignore",
  });
  await proc.exited;
  console.log("Topics ready.");
}

// ─── Ollama model pull ────────────────────────────────────────────────────────

async function pullOllamaModel(): Promise<void> {
  const model = process.env.OLLAMA_MODEL ?? "llama3.2";
  const host  = process.env.OLLAMA_HOST  ?? "http://localhost:11434";
  console.log(`Checking Ollama at ${host}...`);
  try {
    const res = await fetch(`${host}/api/tags`);
    if (!res.ok) throw new Error(`status ${res.status}`);
    console.log(`Ollama is running — pulling model '${model}' (skipped if already present)...`);
    await fetch(`${host}/api/pull`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: model }),
    });
    console.log(`Ollama model '${model}' ready.`);
  } catch {
    console.log("Ollama not reachable — skipping model pull (will fall back to OpenAI).");
  }
}

// ─── Service launcher ─────────────────────────────────────────────────────────

function startService(label: string, cmd: string[], logFile: string): void {
  const fd   = openSync(join(LOG_DIR, logFile), "w");
  const proc = Bun.spawn(cmd, { cwd: ROOT, stdout: fd, stderr: fd });
  closeSync(fd); // parent closes its copy; child keeps its own
  console.log(`Started ${label} (PID ${proc.pid})`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

mkdirSync(LOG_DIR, { recursive: true });

await waitForKafka();
await ensureTopics();
await pullOllamaModel();

startService("webServer",         ["bun", "run", "web"],         "webserver.log");
startService("routerService",     ["bun", "run", "router"],      "router.log");
startService("orchestrator",      ["bun", "run", "orchestrator"],"orchestrator.log");
startService("aggregator",        ["bun", "run", "aggregator"],  "aggregator.log");
startService("mathApp",           ["bun", "run", "math"],        "math.log");
startService("weatherApp",        ["bun", "run", "weather"],     "weather.log");
startService("exchangeApp",       ["bun", "run", "exchange"],    "exchange.log");
startService("generalChatApp",    ["bun", "run", "chat"],        "chat.log");
startService("answerSynthesizer", ["bun", "run", "synthesizer"], "answer.log");
startService("rag_retriever",     [
  join(ROOT, "src/python/venv/bin/python"),
  "-u", "src/python/rag/rag_retriever.py",
], "rag.log");

console.log(`
FINAL PROJECT SERVICES STARTED
Logs in scripts/logs/final-project-services/

Web server: http://localhost:3001  (log: webserver.log)

Open the Web UI:
  http://localhost:3001

  Or for hot-reload dev mode:
    bun run web:dev    # Vite dev server at http://localhost:5173

To stop all services:
  bun run stop
`);
