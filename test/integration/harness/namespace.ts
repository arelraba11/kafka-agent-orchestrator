import type { Admin } from "kafkajs";
import { mkdirSync, mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { TOPICS, physicalTopic, type TopicName } from "../../../shared/topics";
import { REPO_ROOT, testKafka } from "./broker";
import { runId, short } from "./ids";

// One namespace per test FILE: `<run8>-<file>-` prefixes every topic and consumer group,
// so DLQ counts and "every group's lag is 0" are exact without time filters.

export interface Namespace {
  /** The prefix, e.g. "k3j2h1g0-happy-". */
  prefix: string;
  name: string;
  /** Per-file temp dir: state paths and fault dirs live here. */
  dir: string;
  /** Debug logs for this file's services (gitignored). */
  logDir: string;
  admin: Admin;
  physical(topic: TopicName): string;
  paths: { planStore: string; synthStore: string; history: string };
  close(): Promise<void>;
}

export async function createNamespace(name: string): Promise<Namespace> {
  const prefix = `${runId()}-${name}-`;
  const dir = mkdtempSync(join(tmpdir(), `int-${name}-`));
  const logDir = join(REPO_ROOT, "test/integration/.logs", runId(), name);
  mkdirSync(logDir, { recursive: true });

  const admin = testKafka(`admin-${name}-${short()}`).admin();
  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: Object.values(TOPICS).map((t) => ({
      topic: physicalTopic(prefix, t),
      numPartitions: 3,
      replicationFactor: 1,
    })),
  });

  return {
    prefix,
    name,
    dir,
    logDir,
    admin,
    physical: (t) => physicalTopic(prefix, t),
    paths: {
      planStore: join(dir, "plan-store"),
      synthStore: join(dir, "synth-store"),
      history: join(dir, "history.json"),
    },
    async close() {
      await admin.disconnect().catch(() => {});
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
