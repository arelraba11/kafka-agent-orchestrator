import { describe, expect, test } from "bun:test";
import {
  brokersFromEnv,
  consumerConfig,
  fromBeginningFromEnv,
  kafkaConfig,
  namespaceFromEnv,
} from "./config";

describe("brokersFromEnv", () => {
  test("defaults to localhost:9092", () => {
    expect(brokersFromEnv({})).toEqual(["localhost:9092"]);
    expect(brokersFromEnv({ KAFKA_BROKERS: "" })).toEqual(["localhost:9092"]);
  });
  test("splits a comma-separated list and trims", () => {
    expect(brokersFromEnv({ KAFKA_BROKERS: "a:1, b:2" })).toEqual(["a:1", "b:2"]);
  });
  test("the singular KAFKA_BROKER is not read by TS", () => {
    expect(brokersFromEnv({ KAFKA_BROKER: "x:9" })).toEqual(["localhost:9092"]);
  });
});

describe("namespaceFromEnv", () => {
  test("defaults to the empty string", () => expect(namespaceFromEnv({})).toBe(""));
  test("reads KAFKA_NAMESPACE", () => expect(namespaceFromEnv({ KAFKA_NAMESPACE: "r1-f-" })).toBe("r1-f-"));
});

describe("fromBeginningFromEnv", () => {
  test("defaults to latest (false)", () => expect(fromBeginningFromEnv({})).toBe(false));
  test("earliest is true, latest is false", () => {
    expect(fromBeginningFromEnv({ KAFKA_OFFSET_RESET: "earliest" })).toBe(true);
    expect(fromBeginningFromEnv({ KAFKA_OFFSET_RESET: "latest" })).toBe(false);
  });
  test("anything else is rejected", () => {
    expect(() => fromBeginningFromEnv({ KAFKA_OFFSET_RESET: "oldest" })).toThrow(/KAFKA_OFFSET_RESET/);
  });
});

describe("consumerConfig", () => {
  test("defaults pass only the group id (library defaults for timeouts)", () => {
    expect(consumerConfig("orchestrator-service", {})).toEqual({ groupId: "orchestrator-service" });
  });
  test("the group is prefixed with the namespace", () => {
    expect(consumerConfig("orchestrator-service", { KAFKA_NAMESPACE: "ns-" }).groupId).toBe("ns-orchestrator-service");
  });
  test("session, heartbeat and rebalance timeouts come from env", () => {
    expect(
      consumerConfig("g", {
        KAFKA_SESSION_TIMEOUT_MS: "3000",
        KAFKA_HEARTBEAT_INTERVAL_MS: "500",
        KAFKA_REBALANCE_TIMEOUT_MS: "5000",
      })
    ).toEqual({ groupId: "g", sessionTimeout: 3000, heartbeatInterval: 500, rebalanceTimeout: 5000 });
  });
});

describe("consumerConfig fetch wait", () => {
  test("KAFKA_MAX_WAIT_MS maps to maxWaitTimeInMs; unset leaves kafkajs's default (5000)", () => {
    expect(consumerConfig("g", {})).not.toHaveProperty("maxWaitTimeInMs");
    expect(consumerConfig("g", { KAFKA_MAX_WAIT_MS: "500" })).toEqual({ groupId: "g", maxWaitTimeInMs: 500 });
  });
  test("a non-positive value is rejected with the variable name", () => {
    expect(() => consumerConfig("g", { KAFKA_MAX_WAIT_MS: "0" })).toThrow(/KAFKA_MAX_WAIT_MS/);
  });
});

describe("kafkaConfig", () => {
  test("defaults equal the previous hardcoded config", () => {
    expect(kafkaConfig({})).toEqual({
      clientId: "distributed-bot",
      brokers: ["localhost:9092"],
      retry: { initialRetryTime: 500, retries: 10 },
    });
  });
  test("brokers, retries and request timeout come from env", () => {
    const c = kafkaConfig({ KAFKA_BROKERS: "h:19092", KAFKA_RETRIES: "1", KAFKA_REQUEST_TIMEOUT_MS: "2000" });
    expect(c.brokers).toEqual(["h:19092"]);
    expect(c.retry).toEqual({ initialRetryTime: 500, retries: 1 });
    expect(c.requestTimeout).toBe(2000);
  });
});
