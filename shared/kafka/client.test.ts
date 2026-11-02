import { afterEach, describe, expect, mock, test } from "bun:test";
import type { Consumer, Producer } from "kafkajs";
import { TOPICS } from "../topics";
import { publish, subscribeAndRun } from "./client";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const NS = "ab12cd34-unit-";

const failed = {
  conversationId: CID,
  timestamp: 1,
  eventType: "ConversationFailed" as const,
  payload: { stage: "routing" as const, code: "internal-error" as const, reason: "r" },
};

const saved = { ns: process.env.KAFKA_NAMESPACE, reset: process.env.KAFKA_OFFSET_RESET };
afterEach(() => {
  for (const [k, v] of [["KAFKA_NAMESPACE", saved.ns], ["KAFKA_OFFSET_RESET", saved.reset]] as const) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
});

describe("namespace at the I/O edge", () => {
  test("publish sends to the physical topic; the message is unchanged", async () => {
    process.env.KAFKA_NAMESPACE = NS;
    const send = mock(async (_r: unknown) => []);
    await publish({ send } as unknown as Producer, TOPICS.CONVERSATION_EVENTS, failed);
    const arg = send.mock.calls[0][0] as { topic: string; messages: { key: string; value: string }[] };
    expect(arg.topic).toBe(`${NS}conversation-events`);
    expect(JSON.parse(arg.messages[0].value)).toEqual(failed);
  });

  test("subscribeAndRun subscribes physically, hands logical topics to the handler and writes logical DLQ fields", async () => {
    process.env.KAFKA_NAMESPACE = NS;
    process.env.KAFKA_OFFSET_RESET = "earliest";
    const subscribe = mock(async (_a: unknown) => {});
    let eachMessage!: (p: unknown) => Promise<void>;
    const run = mock(async (cfg: { eachMessage: (p: unknown) => Promise<void> }) => {
      eachMessage = cfg.eachMessage;
    });
    const send = mock(async (_r: unknown) => []);
    const handled: unknown[] = [];

    await subscribeAndRun({
      consumer: { subscribe, run } as unknown as Consumer,
      groupId: "orchestrator-service",
      service: "orchestrator",
      producer: { send } as unknown as Producer,
      topic: TOPICS.CONVERSATION_EVENTS,
      types: ["ConversationFailed"],
      handler: async (m) => {
        handled.push(m);
      },
    });

    expect(subscribe.mock.calls[0][0]).toEqual({ topic: `${NS}conversation-events`, fromBeginning: true });

    // a valid message on the physical topic reaches the handler untouched
    await eachMessage({
      topic: `${NS}conversation-events`,
      partition: 0,
      message: { offset: "0", key: Buffer.from(CID), value: Buffer.from(JSON.stringify(failed)), timestamp: "1" },
    });
    expect(handled).toEqual([failed]);

    // a poison message is dead-lettered to the physical DLQ with logical source.topic and group
    await eachMessage({
      topic: `${NS}conversation-events`,
      partition: 2,
      message: { offset: "7", key: Buffer.from("k"), value: Buffer.from("{not json"), timestamp: "1" },
    });
    const dlq = send.mock.calls[0][0] as { topic: string; messages: { value: string }[] };
    expect(dlq.topic).toBe(`${NS}dead-letter-queue`);
    const record = JSON.parse(dlq.messages[0].value);
    expect(record.payload.source.topic).toBe("conversation-events");
    expect(record.payload.consumer.group).toBe("orchestrator-service");
  });
});
