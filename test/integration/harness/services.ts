import { AssignerProtocol } from "kafkajs";
import { closeSync, existsSync, mkdirSync, openSync, readFileSync } from "fs";
import { join } from "path";
import { TOPICS, type TopicName } from "../../../shared/topics";
import { BROKERS, REPO_ROOT } from "./broker";
import { FAKE_API_KEY, type FakeLlm } from "./fakeLlm";
import { faultsFor, type Faults } from "./faults";
import { type Namespace } from "./namespace";
import { type LiveGroup } from "./settle";
import { wake, waitFor } from "./wait";

// Every service runs as a real OS process (`bun --no-env-file <service>`), so crash tests
// get real death and real redelivery. The child env is built from scratch: the parent env
// is never passed through, and --no-env-file keeps the repo's .env (a real API key) out.

export type ServiceName =
  | "router" | "orchestrator" | "aggregator" | "synthesizer"
  | "math" | "weather" | "exchange" | "chat" | "web" | "rag";

export const SERVICE_DEFS: Record<ServiceName, { file: string; group: string; topic: TopicName }> = {
  router:       { file: "src/node/core/routerService.ts",          group: "router-plan-service",   topic: TOPICS.USER_COMMANDS },
  orchestrator: { file: "src/node/orchestration/orchestrator.ts",  group: "orchestrator-service",  topic: TOPICS.CONVERSATION_EVENTS },
  aggregator:   { file: "src/node/orchestration/aggregator.ts",    group: "aggregator-service",    topic: TOPICS.CONVERSATION_EVENTS },
  synthesizer:  { file: "src/node/orchestration/answerSynthesizer.ts", group: "answer-synthesizer", topic: TOPICS.USER_COMMANDS },
  math:         { file: "src/node/apps/mathApp.ts",                group: "math-tool-worker",      topic: TOPICS.TOOL_INVOCATION_REQUESTS },
  weather:      { file: "src/node/apps/weatherApp.ts",             group: "weather-tool-worker",   topic: TOPICS.TOOL_INVOCATION_REQUESTS },
  exchange:     { file: "src/node/apps/exchangeApp.ts",            group: "exchange-tool-worker",  topic: TOPICS.TOOL_INVOCATION_REQUESTS },
  chat:         { file: "src/node/apps/generalChatApp.ts",         group: "chat-tool-worker",      topic: TOPICS.TOOL_INVOCATION_REQUESTS },
  web:          { file: "src/node/core/webServer.ts",              group: "ui-web-final-answer",   topic: TOPICS.CONVERSATION_EVENTS },
  rag:          { file: "src/python/rag/rag_retriever.py",         group: "rag-tool-worker",       topic: TOPICS.TOOL_INVOCATION_REQUESTS },
};

// The RAG worker is the one Python service. It runs as `start.ts` runs it (`-u`, no `-I`: it
// imports its sibling contracts.py), and needs the venv plus the index built by index_kb.py.
const PYTHON_BIN = process.env.PYTHON_BIN || join(REPO_ROOT, "src/python/venv/bin/python");
const CHROMA_DIR = join(REPO_ROOT, "src/python/rag/chroma_db");

/** Fails fast, with the fix, when the RAG worker cannot run (instead of a startup timeout). */
export function assertRagPrerequisites(chromaDir = CHROMA_DIR, python = PYTHON_BIN): void {
  if (!existsSync(python)) {
    throw new Error(
      `RAG prerequisite missing: no Python at ${python}. Create the venv: ` +
        "`cd src/python && python3 -m venv venv && source venv/bin/activate && pip install -r rag/requirements.txt` (or set PYTHON_BIN)."
    );
  }
  if (!existsSync(chromaDir)) {
    throw new Error(
      `RAG prerequisite missing: no ChromaDB index at ${chromaDir}. Build it once: \`python src/python/rag/index_kb.py\` (with the venv active).`
    );
  }
}

export interface ExitInfo {
  code: number | null;
  signal: string | null;
}

export interface Services {
  webPort: number;
  start(name: ServiceName, envOverrides?: Record<string, string>): Promise<void>;
  /** Sends `signal` (default SIGKILL) and resolves with how the process ended. */
  kill(name: ServiceName, signal?: NodeJS.Signals): Promise<ExitInfo>;
  /** Graceful stop (SIGTERM, then SIGKILL after 5 s); the service is no longer expected to run. */
  stop(name: ServiceName): Promise<void>;
  /** Stops (if running) and starts again; `envOverrides` apply to the new process only. */
  restart(name: ServiceName, envOverrides?: Record<string, string>): Promise<void>;
  /** Resolves when the current (or last) process of `name` ends. */
  exited(name: ServiceName): Promise<ExitInfo>;
  /** Like exited(), but fails with `what` and the service log after `timeoutMs`. */
  waitExit(name: ServiceName, what: string, timeoutMs?: number): Promise<ExitInfo>;
  pid(name: ServiceName): number;
  isRunning(name: ServiceName): boolean;
  faults(name: ServiceName): Faults;
  /** The suite env every service shares (no FAULT_DIR, no LLM endpoint): for a process that starts services itself. */
  sharedEnv(overrides?: Record<string, string>): Record<string, string>;
  /** The member ids of `name`'s group once it is Stable and covers every partition; null otherwise. */
  groupMembers(name: ServiceName): Promise<Set<string> | null>;
  /** The groups of every service that should be running (for settle). */
  liveGroups(): LiveGroup[];
  /** Restarts every expected service that is not running (a test killed it). Returns their names. */
  restartDead(): Promise<ServiceName[]>;
  logPath(name: ServiceName): string;
  logTail(name: ServiceName, lines?: number): string;
  stopAll(): Promise<void>;
}

interface Proc {
  proc: ReturnType<typeof Bun.spawn>;
  exit: Promise<ExitInfo>;
  info: ExitInfo | null;
  members: Set<string>;
}

const allProcs = new Set<Proc>();
process.on("exit", () => {
  for (const p of allProcs) if (p.info === null) p.proc.kill("SIGKILL");
});

async function freePort(): Promise<number> {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response("") });
  const port = s.port!;
  await s.stop(true);
  return port;
}

export async function createServices(
  ns: Namespace,
  fake: FakeLlm,
  opts: { env?: Record<string, string> } = {}
): Promise<Services> {
  const webPort = await freePort();
  const procs = new Map<ServiceName, Proc>();
  const expected = new Set<ServiceName>();
  const faultMap = new Map<ServiceName, Faults>();
  const lastMembers = new Map<ServiceName, Set<string>>();

  const faults = (name: ServiceName): Faults => {
    let f = faultMap.get(name);
    if (!f) faultMap.set(name, (f = faultsFor(join(ns.dir, "fault", name))));
    return f;
  };
  const logPath = (name: ServiceName) => join(ns.logDir, `${name}.log`);
  const logTail = (name: ServiceName, lines = 30) => {
    try {
      return readFileSync(logPath(name), "utf8").split("\n").slice(-lines).join("\n");
    } catch {
      return "(no log)";
    }
  };

  function sharedEnv(overrides: Record<string, string> = {}): Record<string, string> {
    return {
      PATH: process.env.PATH ?? "",
      HOME: process.env.HOME ?? "",
      KAFKAJS_NO_PARTITIONER_WARNING: "1",
      KAFKA_BROKERS: BROKERS.join(","),
      KAFKA_NAMESPACE: ns.prefix,
      KAFKA_OFFSET_RESET: "earliest",
      KAFKA_SESSION_TIMEOUT_MS: "3000",
      KAFKA_HEARTBEAT_INTERVAL_MS: "500",
      KAFKA_REBALANCE_TIMEOUT_MS: "5000",
      // Rule: the fetch wait must be well below the session timeout. kafkajs sends heartbeats
      // between fetches, so with kafkajs's default 5000 ms wait an idle member is expired by a
      // 3000 ms session about every 5 s, and the resulting rebalance redelivers in-flight messages.
      KAFKA_MAX_WAIT_MS: "500",
      PLAN_STORE_PATH: ns.paths.planStore,
      SYNTH_STORE_PATH: ns.paths.synthStore,
      HISTORY_PATH: ns.paths.history,
      WEB_PORT: String(webPort),
      STEP_TIMEOUT_MS: "20000",
      LLM_PROVIDER: "openai",
      LLM_ATTEMPT_TIMEOUT_MS: "1000",
      LLM_ROUTER_BUDGET_MS: "3000",
      LLM_SYNTH_BUDGET_MS: "8000",
      RAG_SEND_TIMEOUT_S: "3",
      ...opts.env,
      ...overrides,
    };
  }

  function buildEnv(name: ServiceName, overrides: Record<string, string>): Record<string, string> {
    const env: Record<string, string> = { ...sharedEnv(), FAULT_DIR: faults(name).dir };
    if (name === "router" || name === "synthesizer") {
      env.OPENAI_API_KEY = FAKE_API_KEY;
      env.OPENAI_BASE_URL = `${fake.baseUrl}/${name === "router" ? "router" : "synth"}/v1`;
    }
    return { ...env, ...opts.env, ...overrides };
  }

  async function groupMembers(name: ServiceName): Promise<Set<string> | null> {
    const def = SERVICE_DEFS[name];
    const gid = ns.prefix + def.group;
    const [d] = (await ns.admin.describeGroups([gid])).groups;
    if (d.state !== "Stable" || d.members.length === 0) return null;
    const assigned = new Set<number>();
    for (const m of d.members) {
      const a = AssignerProtocol.MemberAssignment.decode(m.memberAssignment);
      for (const p of a?.assignment[ns.physical(def.topic)] ?? []) assigned.add(p);
    }
    if (![0, 1, 2].every((p) => assigned.has(p))) return null;
    return new Set(d.members.map((m) => m.memberId));
  }

  async function start(name: ServiceName, overrides: Record<string, string> = {}): Promise<void> {
    const existing = procs.get(name);
    if (existing && existing.info === null) throw new Error(`service ${name} is already running`);
    if (name === "rag") assertRagPrerequisites();
    mkdirSync(ns.logDir, { recursive: true });
    const fd = openSync(logPath(name), "a");
    const cmd =
      name === "rag"
        ? [PYTHON_BIN, "-u", join(REPO_ROOT, SERVICE_DEFS[name].file)]
        : [process.execPath, "--no-env-file", join(REPO_ROOT, SERVICE_DEFS[name].file)];
    const proc = Bun.spawn(cmd, {
      cwd: REPO_ROOT,
      env: buildEnv(name, overrides),
      stdin: "ignore",
      stdout: fd,
      stderr: fd,
    });
    closeSync(fd);
    const entry: Proc = { proc, info: null, members: new Set(), exit: null as never };
    entry.exit = proc.exited.then((code) => {
      entry.info = { code: proc.signalCode ? null : code, signal: proc.signalCode ?? null };
      wake();
      return entry.info;
    });
    procs.set(name, entry);
    allProcs.add(entry);
    expected.add(name);

    // Ready = the group is Stable, covers every partition, and none of the members is a
    // previous instance (a SIGKILLed member lingers until its session expires).
    const stale = lastMembers.get(name) ?? new Set<string>();
    const members = await waitFor(
      async () => {
        if (entry.info !== null) {
          throw new Error(
            `service ${name} exited during startup (${JSON.stringify(entry.info)}); log ${logPath(name)}:\n${logTail(name)}`
          );
        }
        const m = await groupMembers(name);
        if (m === null || [...m].some((id) => stale.has(id))) return null;
        if (name === "web") {
          // Any HTTP answer means the server is listening (without a UI build it answers 503).
          const ok = await fetch(`http://127.0.0.1:${webPort}/`).then(() => true, () => false);
          if (!ok) return null;
        }
        return m;
      },
      {
        // the RAG worker loads an embedding model first
        timeoutMs: name === "rag" ? 90_000 : 30_000,
        what: `service ${name} to be ready (group ${SERVICE_DEFS[name].group} stable and fully assigned)`,
        context: () => `  log ${logPath(name)}:\n${logTail(name)}`,
      }
    );
    entry.members = members;
    lastMembers.set(name, members);
  }

  const get = (name: ServiceName): Proc => {
    const p = procs.get(name);
    if (!p) throw new Error(`service ${name} was never started`);
    return p;
  };

  const services: Services = {
    webPort,
    start,
    sharedEnv,
    groupMembers,
    async kill(name, signal = "SIGKILL") {
      const p = get(name);
      if (p.info === null) p.proc.kill(signal);
      return p.exit;
    },
    async stop(name) {
      expected.delete(name);
      const p = procs.get(name);
      if (!p || p.info !== null) return;
      p.proc.kill("SIGTERM");
      try {
        await waitFor(() => p.info !== null, { timeoutMs: 5_000, what: `service ${name} to stop after SIGTERM` });
      } catch {
        p.proc.kill("SIGKILL");
        await p.exit;
      }
    },
    async restart(name, overrides) {
      await services.stop(name);
      await start(name, overrides);
    },
    exited: (name) => get(name).exit,
    async waitExit(name, what, timeoutMs = 15_000) {
      const p = get(name);
      await waitFor(() => p.info !== null, {
        timeoutMs,
        what: `service ${name} to exit (${what})`,
        context: () => `  log ${logPath(name)}:\n${logTail(name)}`,
      });
      return p.info!;
    },
    pid: (name) => get(name).proc.pid,
    isRunning: (name) => procs.get(name)?.info === null,
    faults,
    liveGroups: () => [...expected].map((n) => ({ group: SERVICE_DEFS[n].group, topic: SERVICE_DEFS[n].topic })),
    async restartDead() {
      const dead = [...expected].filter((n) => !services.isRunning(n));
      for (const n of dead) await start(n);
      return dead;
    },
    logPath,
    logTail,
    async stopAll() {
      await Promise.all([...procs.keys()].map((n) => services.stop(n)));
    },
  };
  return services;
}
