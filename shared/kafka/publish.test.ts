import { describe, expect, mock, test } from "bun:test";
import type { Producer } from "kafkajs";
import { TOPICS } from "../topics";
import { publish, validateOutbound } from "./client";
import { OutboundContractViolationError, ProducerError } from "./errors";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";

const result = {
  conversationId: CID,
  timestamp: 1,
  eventType: "ToolInvocationResulted" as const,
  payload: { toolName: "math" as const, stepIndex: 0, result: { value: "4", success: true } },
};

function fakeProducer(send = mock(async (_r: unknown) => [])) {
  return { producer: { send } as unknown as Producer, send };
}

describe("publish", () => {
  test("sends the validated message JSON, keyed by conversationId", async () => {
    const { producer, send } = fakeProducer();
    await publish(producer, TOPICS.CONVERSATION_EVENTS, result);
    expect(send).toHaveBeenCalledTimes(1);
    const arg = send.mock.calls[0][0] as { topic: string; messages: { key: string; value: string }[] };
    expect(arg.topic).toBe("conversation-events");
    expect(arg.messages).toHaveLength(1);
    expect(arg.messages[0].key).toBe(CID);
    expect(JSON.parse(arg.messages[0].value)).toEqual(result);
  });

  test("a contract violation throws before any send, with issues, messageType and topic", async () => {
    const { producer, send } = fakeProducer();
    const bad = { ...result, payload: { toolName: "math", result: {} } } as never;
    const err = await publish(producer, TOPICS.CONVERSATION_EVENTS, bad).catch((e) => e);
    expect(err).toBeInstanceOf(OutboundContractViolationError);
    expect(err.messageType).toBe("ToolInvocationResulted");
    expect(err.topic).toBe("conversation-events");
    expect(err.issues.map((i: { path: string }) => i.path)).toContain("payload.stepIndex");
    expect(send).not.toHaveBeenCalled();
  });

  test("rejects a message type that does not belong to the topic", async () => {
    const { producer, send } = fakeProducer();
    const err = await publish(producer, TOPICS.TOOL_INVOCATION_REQUESTS, result as never).catch((e) => e);
    expect(err).toBeInstanceOf(OutboundContractViolationError);
    expect(err.messageType).toBe("ToolInvocationResulted");
    expect(err.topic).toBe("tool-invocation-requests");
    expect(send).not.toHaveBeenCalled();
  });

  test("a message with no discriminator is an outbound violation", () => {
    expect(() => validateOutbound(TOPICS.CONVERSATION_EVENTS, { conversationId: CID } as never)).toThrow(
      OutboundContractViolationError
    );
  });

  test("a dead-letter record is keyed by the original key, or null", async () => {
    const { producer, send } = fakeProducer();
    const rec = (key: string | null) => ({
      eventType: "MessageDeadLettered" as const,
      schemaVersion: 1 as const,
      conversationId: null,
      timestamp: 1,
      payload: {
        source: { topic: TOPICS.CONVERSATION_EVENTS, partition: 0, offset: "1", key, timestamp: null },
        consumer: { group: "g", service: "s" },
        error: { class: "malformed-json" as const, name: "E", reason: "r", attempts: 1 },
        originalValue: null,
      },
    });
    await publish(producer, TOPICS.DEAD_LETTER_QUEUE, rec("abc"));
    await publish(producer, TOPICS.DEAD_LETTER_QUEUE, rec(null));
    expect((send.mock.calls[0][0] as any).messages[0].key).toBe("abc");
    expect((send.mock.calls[1][0] as any).messages[0].key).toBeNull();
  });

  test("a send rejection is wrapped in ProducerError", async () => {
    const cause = new Error("broker down");
    const { producer } = fakeProducer(mock(async () => { throw cause; }));
    const err = await publish(producer, TOPICS.CONVERSATION_EVENTS, result).catch((e) => e);
    expect(err).toBeInstanceOf(ProducerError);
    expect(err.cause).toBe(cause);
  });
});
