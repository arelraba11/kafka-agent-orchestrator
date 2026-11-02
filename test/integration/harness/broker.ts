import { Kafka, logLevel } from "kafkajs";
import { createServer } from "net";
import { join } from "path";

// Lifecycle and clients for the dedicated TEST broker (infra/docker-compose.test.yml,
// compose project kafka-agent-orchestrator-test, port 19092). Nothing in this suite may
// ever connect to the dev broker on 9092.

export const REPO_ROOT = join(import.meta.dir, "../../..");
export const COMPOSE_FILE = join(REPO_ROOT, "infra/docker-compose.test.yml");
export const TEST_PORT = Number(process.env.TEST_KAFKA_PORT ?? 19092);
export const BROKERS = [`localhost:${TEST_PORT}`];

if (TEST_PORT === 9092) {
  throw new Error("TEST_KAFKA_PORT=9092 is the dev broker's port; the integration suite never uses it");
}

async function run(cmd: string[], opts: { allowFail?: boolean } = {}): Promise<{ code: number; out: string }> {
  const p = Bun.spawn(cmd, {
    cwd: REPO_ROOT,
    env: { ...process.env, TEST_KAFKA_PORT: String(TEST_PORT) },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [out, err, code] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited]);
  if (code !== 0 && !opts.allowFail) throw new Error(`${cmd.join(" ")} failed (${code}):\n${err || out}`);
  return { code, out: out + err };
}

const compose = (...args: string[]) => ["docker", "compose", "-f", COMPOSE_FILE, ...args];

function portInUse(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const s = createServer();
    s.once("error", () => resolve(true));
    s.once("listening", () => s.close(() => resolve(false)));
    s.listen(port, "127.0.0.1");
  });
}

/** Starts the test broker and waits until its healthcheck passes. Fails fast with one clear message. */
export async function brokerUp(): Promise<void> {
  const info = await run(["docker", "info", "--format", "{{.ServerVersion}}"], { allowFail: true });
  if (info.code !== 0) {
    throw new Error("The Docker daemon is not reachable. Start Docker and re-run `bun run test:int`.");
  }
  const ours = (await run(compose("ps", "-q", "kafka"), { allowFail: true })).out.trim();
  if (ours === "" && (await portInUse(TEST_PORT))) {
    throw new Error(
      `Port ${TEST_PORT} is already in use by something other than the test broker. Free it or set TEST_KAFKA_PORT.`
    );
  }
  await run(compose("up", "-d", "--wait"));
}

/** Removes the broker and its data (a fresh broker per run). */
export async function brokerDown(): Promise<void> {
  await run(compose("down", "-v", "--remove-orphans"), { allowFail: true });
}

export const pauseBroker = () => run(compose("pause", "kafka")).then(() => {});
export const unpauseBroker = () => run(compose("unpause", "kafka"), { allowFail: true }).then(() => {});

export function testKafka(clientId: string): Kafka {
  return new Kafka({ clientId, brokers: BROKERS, logLevel: logLevel.ERROR, retry: { retries: 5 } });
}
