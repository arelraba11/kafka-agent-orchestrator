import type { ConsumerConfig, KafkaConfig } from "kafkajs";
import { envInt } from "../env";
import { physicalGroup } from "../topics";

// Env-driven Kafka settings. Every default equals the previous hardcoded behavior.
// Only the integration suite sets these (see test/integration/harness).

type Env = Record<string, string | undefined>;

/** KAFKA_BROKERS, comma-separated. Default localhost:9092. */
export function brokersFromEnv(env: Env = process.env): string[] {
  const raw = env.KAFKA_BROKERS;
  if (raw === undefined || raw.trim() === "") return ["localhost:9092"];
  return raw.split(",").map((b) => b.trim()).filter((b) => b !== "");
}

/** KAFKA_NAMESPACE, prefixed to every topic and consumer group. Default "". */
export function namespaceFromEnv(env: Env = process.env): string {
  return env.KAFKA_NAMESPACE ?? "";
}

/** KAFKA_OFFSET_RESET=earliest|latest → kafkajs `fromBeginning`. Default latest (false). */
export function fromBeginningFromEnv(env: Env = process.env): boolean {
  const v = env.KAFKA_OFFSET_RESET;
  if (v === undefined || v === "" || v === "latest") return false;
  if (v === "earliest") return true;
  throw new Error(`KAFKA_OFFSET_RESET must be "earliest" or "latest", got ${JSON.stringify(v)}`);
}

/** Group id (prefixed with the namespace) plus the optional session/heartbeat/rebalance timeouts. */
export function consumerConfig(groupId: string, env: Env = process.env): ConsumerConfig {
  const sessionTimeout = envInt(env, "KAFKA_SESSION_TIMEOUT_MS");
  const heartbeatInterval = envInt(env, "KAFKA_HEARTBEAT_INTERVAL_MS");
  const rebalanceTimeout = envInt(env, "KAFKA_REBALANCE_TIMEOUT_MS");
  return {
    groupId: physicalGroup(namespaceFromEnv(env), groupId),
    ...(sessionTimeout !== undefined ? { sessionTimeout } : {}),
    ...(heartbeatInterval !== undefined ? { heartbeatInterval } : {}),
    ...(rebalanceTimeout !== undefined ? { rebalanceTimeout } : {}),
  };
}

/** Client config: brokers, KAFKA_RETRIES (default 10) and KAFKA_REQUEST_TIMEOUT_MS (default: kafkajs's). */
export function kafkaConfig(env: Env = process.env): KafkaConfig {
  const requestTimeout = envInt(env, "KAFKA_REQUEST_TIMEOUT_MS");
  return {
    clientId: "distributed-bot",
    brokers: brokersFromEnv(env),
    retry: {
      initialRetryTime: 500,
      retries: envInt(env, "KAFKA_RETRIES") ?? 10,
    },
    ...(requestTimeout !== undefined ? { requestTimeout } : {}),
  };
}
