import { describe, expect, test } from "bun:test";
import { MessageDeadLetteredSchema } from "../schemas/MessageDeadLettered";
import { TOPICS } from "../topics";
import { buildDeadLetter, MAX_ORIGINAL_BYTES, type RawMessage } from "./deadLetter";
import {
  ContractViolationError,
  KeyMismatchError,
  MalformedMessageError,
  NonRetryableError,
  OutboundContractViolationError,
} from "./errors";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const NOW = 1_700_000_000_000;
const CTX = { group: "math-tool-worker", service: "math" };

function raw(over: Partial<RawMessage> = {}): RawMessage {
  return {
    topic: TOPICS.TOOL_INVOCATION_REQUESTS,
    partition: 2,
    offset: "17",
    key: Buffer.from(CID),
    value: Buffer.from(JSON.stringify({ conversationId: CID, eventType: "ToolInvocationRequested" })),
    timestamp: "1700000000123",
    ...over,
  };
}

function build(r: RawMessage, err: Error, attempts = 1) {
  const rec = buildDeadLetter(r, CTX, err, attempts, NOW);
  expect(MessageDeadLetteredSchema.safeParse(rec).success).toBe(true);
  return rec;
}

describe("buildDeadLetter", () => {
  test("record passes MessageDeadLetteredSchema and carries source and consumer", () => {
    const rec = build(raw(), new ContractViolationError("bad", [{ path: "payload.stepIndex", message: "required" }]));
    expect(rec.eventType).toBe("MessageDeadLettered");
    expect(rec.schemaVersion).toBe(1);
    expect(rec.timestamp).toBe(NOW);
    expect(rec.payload.source).toEqual({
      topic: "tool-invocation-requests",
      partition: 2,
      offset: "17",
      key: CID,
      timestamp: "1700000000123",
    });
    expect(rec.payload.consumer).toEqual(CTX);
    expect(rec.payload.error.class).toBe("contract-violation");
    expect(rec.payload.error.issues).toEqual([{ path: "payload.stepIndex", message: "required" }]);
    expect(rec.payload.error.attempts).toBe(1);
  });

  test("conversationId is extracted when the value parses, null otherwise", () => {
    expect(build(raw(), new KeyMismatchError("x")).conversationId).toBe(CID);
    const bad = build(raw({ value: Buffer.from("{not json") }), new MalformedMessageError("malformed-json", "x"));
    expect(bad.conversationId).toBeNull();
    const notUuid = build(
      raw({ value: Buffer.from(JSON.stringify({ conversationId: "unknown" })) }),
      new ContractViolationError("x", [])
    );
    expect(notUuid.conversationId).toBeNull();
  });

  test("the original key is preserved, including a null key", () => {
    expect(build(raw(), new KeyMismatchError("x")).payload.source.key).toBe(CID);
    expect(build(raw({ key: null }), new KeyMismatchError("x")).payload.source.key).toBeNull();
  });

  test("valid utf8 is stored as utf8; non-UTF-8 bytes as base64", () => {
    const ok = build(raw({ value: Buffer.from("héllo {") }), new MalformedMessageError("malformed-json", "x"));
    expect(ok.payload.originalValue).toEqual({ encoding: "utf8", data: "héllo {", truncated: false });

    const bytes = Buffer.from([0xff, 0xfe, 0x00, 0x80]);
    const b64 = build(raw({ value: bytes }), new MalformedMessageError("malformed-json", "x"));
    expect(b64.payload.originalValue).toEqual({
      encoding: "base64",
      data: bytes.toString("base64"),
      truncated: false,
    });
  });

  test("a null value gives originalValue null and class missing-value", () => {
    const rec = build(raw({ value: null }), new MalformedMessageError("missing-value", "null value"));
    expect(rec.payload.originalValue).toBeNull();
    expect(rec.payload.error.class).toBe("missing-value");
    expect(rec.conversationId).toBeNull();
  });

  test("a value over 512 KiB is truncated and the record stays under 1 MiB", () => {
    const big = Buffer.alloc(MAX_ORIGINAL_BYTES + 1000, "a");
    const rec = build(raw({ value: big }), new MalformedMessageError("malformed-json", "x"));
    expect(rec.payload.originalValue!.truncated).toBe(true);
    expect(Buffer.from(rec.payload.originalValue!.data, "utf8").length).toBe(MAX_ORIGINAL_BYTES);
    expect(Buffer.byteLength(JSON.stringify(rec))).toBeLessThan(1024 * 1024);
  });

  test("worst-case escaping and base64 expansion still fit under 1 MiB", () => {
    const ctrl = Buffer.alloc(MAX_ORIGINAL_BYTES + 10, 1); // each byte escapes to \u0001 in JSON
    const rec = build(raw({ value: ctrl }), new MalformedMessageError("malformed-json", "x"));
    expect(Buffer.byteLength(JSON.stringify(rec))).toBeLessThan(1024 * 1024);
    const bin = Buffer.alloc(MAX_ORIGINAL_BYTES * 2, 0xff);
    const rec2 = build(raw({ value: bin }), new MalformedMessageError("malformed-json", "x"));
    expect(Buffer.byteLength(JSON.stringify(rec2))).toBeLessThan(1024 * 1024);
  });

  test("truncating in the middle of a multi-byte character stays valid", () => {
    const s = "é".repeat(MAX_ORIGINAL_BYTES); // 2 bytes each
    const rec = build(raw({ value: Buffer.from(s) }), new MalformedMessageError("malformed-json", "x"));
    expect(rec.payload.originalValue!.truncated).toBe(true);
  });

  test("outbound violation: issues and outbound describe the output, originalValue is the valid input", () => {
    const input = raw();
    const err = new OutboundContractViolationError(
      "ToolInvocationResulted rejected",
      [{ path: "payload.stepIndex", message: "required" }],
      "ToolInvocationResulted",
      TOPICS.CONVERSATION_EVENTS
    );
    const rec = build(input, err);
    expect(rec.payload.error.class).toBe("outbound-contract-violation");
    expect(rec.payload.error.outbound).toEqual({
      topic: "conversation-events",
      messageType: "ToolInvocationResulted",
    });
    expect(rec.payload.error.issues).toEqual([{ path: "payload.stepIndex", message: "required" }]);
    expect(rec.payload.originalValue!.data).toBe(input.value!.toString());
  });

  test("a wrapped handler exception is handler-non-retryable and keeps the cause name", () => {
    const rec = build(raw(), new NonRetryableError("boom", { cause: new TypeError("boom") }));
    expect(rec.payload.error.class).toBe("handler-non-retryable");
    expect(rec.payload.error.name).toBe("TypeError");
  });

  test("an unwrapped error is unknown-exhausted with the attempt count", () => {
    const rec = build(raw(), new TypeError("x is undefined"), 3);
    expect(rec.payload.error.class).toBe("unknown-exhausted");
    expect(rec.payload.error.name).toBe("TypeError");
    expect(rec.payload.error.attempts).toBe(3);
  });

  test("a huge issue list is capped", () => {
    const issues = Array.from({ length: 5000 }, (_, i) => ({ path: `p${i}`, message: "m".repeat(5000) }));
    const rec = build(raw(), new ContractViolationError("many", issues));
    expect(rec.payload.error.issues!.length).toBeLessThanOrEqual(50);
    expect(Buffer.byteLength(JSON.stringify(rec))).toBeLessThan(1024 * 1024);
  });
});
