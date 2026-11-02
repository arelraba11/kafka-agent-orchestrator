import { describe, expect, mock, test } from "bun:test";
import { KafkaJSNumberOfRetriesExceeded } from "kafkajs";
import { MessageDeadLetteredSchema, type MessageDeadLettered } from "../schemas/MessageDeadLettered";
import { TOPICS } from "../topics";
import { processMessage, type ConsumeContext, type ConsumeDeps } from "./consume";
import type { RawMessage } from "./deadLetter";
import {
  ContractViolationError,
  KeyMismatchError,
  NonRetryableError,
  OutboundContractViolationError,
  ProducerError,
  RetryableError,
  StoreError,
  classifyError,
  nonRetryable,
} from "./errors";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";

const validRequest = {
  conversationId: CID,
  timestamp: 1,
  eventType: "ToolInvocationRequested",
  payload: { toolName: "math", stepIndex: 0, input: { expression: "1+1" } },
};

function rawOf(value: unknown, over: Partial<RawMessage> = {}): RawMessage {
  return {
    topic: TOPICS.TOOL_INVOCATION_REQUESTS,
    partition: 1,
    offset: "5",
    key: Buffer.from(CID),
    value: value === null ? null : Buffer.from(typeof value === "string" ? value : JSON.stringify(value)),
    timestamp: "123",
    ...over,
  };
}

const ctx: ConsumeContext<"ToolInvocationRequested"> = {
  topic: TOPICS.TOOL_INVOCATION_REQUESTS,
  group: "math-tool-worker",
  service: "math",
  types: ["ToolInvocationRequested"],
};

function makeDeps(over: Partial<ConsumeDeps> = {}) {
  const dead: MessageDeadLettered[] = [];
  const deps: ConsumeDeps = {
    publishDeadLetter: async (r) => {
      dead.push(r);
    },
    attempts: new Map(),
    now: () => 1000,
    maxUnknownAttempts: 3,
    log: { info() {}, warn() {}, error() {} },
    ...over,
  };
  return { deps, dead };
}

const noop = async () => {};

describe("processMessage: valid and skipped", () => {
  test("valid message → handler called with the parsed value, no DLQ", async () => {
    const { deps, dead } = makeDeps();
    const handler = mock(async (_m: unknown) => {});
    const out = await processMessage(rawOf(validRequest), ctx, handler, deps);
    expect(out).toBe("handled");
    expect(handler).toHaveBeenCalledTimes(1);
    expect((handler.mock.calls[0][0] as any).payload.toolName).toBe("math");
    expect(dead).toEqual([]);
  });

  test("unknown fields are stripped before the handler sees them", async () => {
    const { deps } = makeDeps();
    const handler = mock(async (_m: unknown) => {});
    await processMessage(rawOf({ ...validRequest, traceId: "x" }), ctx, handler, deps);
    expect(handler.mock.calls[0][0]).not.toHaveProperty("traceId");
  });

  test("a type the topic does not know is skipped", async () => {
    const { deps, dead } = makeDeps();
    const handler = mock(noop);
    const out = await processMessage(rawOf({ ...validRequest, eventType: "SomethingNew" }), ctx, handler, deps);
    expect(out).toBe("skipped");
    expect(handler).not.toHaveBeenCalled();
    expect(dead).toEqual([]);
  });

  test("a type on the topic that this service does not handle is skipped, even if invalid", async () => {
    const { deps, dead } = makeDeps();
    const handler = mock(noop);
    const out = await processMessage(
      rawOf({ conversationId: "x", eventType: "PlanCompleted" }, { topic: TOPICS.CONVERSATION_EVENTS }),
      { ...ctx, topic: TOPICS.CONVERSATION_EVENTS, types: ["PlanGenerated"] },
      handler,
      deps
    );
    expect(out).toBe("skipped");
    expect(handler).not.toHaveBeenCalled();
    expect(dead).toEqual([]);
  });

  test("a command is routed by commandType", async () => {
    const { deps } = makeDeps();
    const handler = mock(async (_m: unknown) => {});
    const cmd = {
      conversationId: CID,
      sessionId: "s",
      timestamp: 1,
      commandType: "SynthesizeFinalAnswerRequested",
      payload: { results: [] },
    };
    const out = await processMessage(
      rawOf(cmd, { topic: TOPICS.USER_COMMANDS }),
      { topic: TOPICS.USER_COMMANDS, group: "answer-synthesizer", service: "synth", types: ["SynthesizeFinalAnswerRequested"] },
      handler,
      deps
    );
    expect(out).toBe("handled");
  });
});

describe("processMessage: poison input", () => {
  async function dl(raw: RawMessage) {
    const { deps, dead } = makeDeps();
    const handler = mock(noop);
    const out = await processMessage(raw, ctx, handler, deps);
    expect(out).toBe("dead-lettered");
    expect(handler).not.toHaveBeenCalled();
    expect(dead).toHaveLength(1);
    expect(MessageDeadLetteredSchema.safeParse(dead[0]).success).toBe(true);
    return dead[0];
  }

  test("null value → missing-value", async () => {
    expect((await dl(rawOf(null))).payload.error.class).toBe("missing-value");
  });

  test("bad JSON → malformed-json", async () => {
    const rec = await dl(rawOf("{not json"));
    expect(rec.payload.error.class).toBe("malformed-json");
    expect(rec.payload.originalValue!.data).toBe("{not json");
  });

  test("empty string value → malformed-json (not silently dropped)", async () => {
    expect((await dl(rawOf(""))).payload.error.class).toBe("malformed-json");
  });

  test("JSON that is not an object → contract-violation", async () => {
    expect((await dl(rawOf("5"))).payload.error.class).toBe("contract-violation");
  });

  test("no eventType/commandType → contract-violation", async () => {
    expect((await dl(rawOf({ conversationId: CID }))).payload.error.class).toBe("contract-violation");
  });

  test("wrong shape → contract-violation with issues", async () => {
    const { stepIndex: _s, ...payload } = validRequest.payload;
    const rec = await dl(rawOf({ ...validRequest, payload }));
    expect(rec.payload.error.class).toBe("contract-violation");
    expect(rec.payload.error.issues!.map((i) => i.path)).toContain("payload.stepIndex");
    expect(rec.conversationId).toBe(CID);
  });

  test("key different from conversationId → key-mismatch", async () => {
    const rec = await dl(rawOf(validRequest, { key: Buffer.from("other") }));
    expect(rec.payload.error.class).toBe("key-mismatch");
    expect(rec.payload.source.key).toBe("other");
  });

  test("missing key → key-mismatch", async () => {
    expect((await dl(rawOf(validRequest, { key: null }))).payload.error.class).toBe("key-mismatch");
  });

  test("record identifies the source and the consumer group", async () => {
    const rec = await dl(rawOf("{bad", { partition: 2, offset: "99" }));
    expect(rec.payload.source).toMatchObject({ topic: "tool-invocation-requests", partition: 2, offset: "99" });
    expect(rec.payload.consumer).toEqual({ group: "math-tool-worker", service: "math" });
  });
});

describe("processMessage: handler failures", () => {
  test.each([
    ["StoreError", () => new StoreError("db closed")],
    ["ProducerError", () => new ProducerError("broker down")],
  ])("%s → rethrown every time, never dead-lettered", async (_n, make) => {
    const { deps, dead } = makeDeps();
    for (let i = 0; i < 10; i++) {
      await expect(
        processMessage(rawOf(validRequest), ctx, async () => { throw make(); }, deps)
      ).rejects.toBeInstanceOf(RetryableError);
    }
    expect(dead).toEqual([]);
    expect(deps.attempts.size).toBe(0);
  });

  test("NonRetryableError → handler-non-retryable DLQ, returns normally", async () => {
    const { deps, dead } = makeDeps();
    const out = await processMessage(rawOf(validRequest), ctx, async () => { throw new NonRetryableError("nope"); }, deps);
    expect(out).toBe("dead-lettered");
    expect(dead[0].payload.error.class).toBe("handler-non-retryable");
  });

  test("an exception wrapped by nonRetryable() → handler-non-retryable", async () => {
    const { deps, dead } = makeDeps();
    await processMessage(
      rawOf(validRequest),
      ctx,
      async () => { nonRetryable(() => { throw new TypeError("pure bug"); }); },
      deps
    );
    expect(dead[0].payload.error.class).toBe("handler-non-retryable");
    expect(dead[0].payload.error.name).toBe("TypeError");
  });

  test("valid input whose output the handler's publish rejects → outbound-contract-violation", async () => {
    const { deps, dead } = makeDeps();
    const input = rawOf(validRequest);
    const out = await processMessage(
      input,
      ctx,
      async () => {
        throw new OutboundContractViolationError(
          "ToolInvocationResulted rejected",
          [{ path: "payload.stepIndex", message: "required" }],
          "ToolInvocationResulted",
          TOPICS.CONVERSATION_EVENTS
        );
      },
      deps
    );
    expect(out).toBe("dead-lettered");
    const e = dead[0].payload.error;
    expect(e.class).toBe("outbound-contract-violation");
    expect(e.outbound).toEqual({ topic: "conversation-events", messageType: "ToolInvocationResulted" });
    expect(e.issues).toEqual([{ path: "payload.stepIndex", message: "required" }]);
    expect(dead[0].payload.originalValue!.data).toBe(input.value!.toString());
    expect(dead[0].conversationId).toBe(CID);
  });

  test("an OutboundContractViolationError wrapped by nonRetryable() keeps its class", async () => {
    const { deps, dead } = makeDeps();
    await processMessage(
      rawOf(validRequest),
      ctx,
      async () => {
        nonRetryable(() => {
          throw new OutboundContractViolationError("x", [], "PlanFailed", TOPICS.CONVERSATION_EVENTS);
        });
      },
      deps
    );
    expect(dead[0].payload.error.class).toBe("outbound-contract-violation");
  });

  test("TypeError → rethrown on attempts 1 and 2, DLQ unknown-exhausted on attempt 3, counter cleared", async () => {
    const { deps, dead } = makeDeps();
    const raw = rawOf(validRequest);
    const boom = async () => { throw new TypeError("x is undefined"); };

    await expect(processMessage(raw, ctx, boom, deps)).rejects.toBeInstanceOf(TypeError);
    expect(deps.attempts.get("tool-invocation-requests:1:5")).toBe(1);
    await expect(processMessage(raw, ctx, boom, deps)).rejects.toBeInstanceOf(TypeError);
    expect(deps.attempts.get("tool-invocation-requests:1:5")).toBe(2);
    expect(dead).toEqual([]);

    const out = await processMessage(raw, ctx, boom, deps);
    expect(out).toBe("dead-lettered");
    expect(dead).toHaveLength(1);
    expect(dead[0].payload.error.class).toBe("unknown-exhausted");
    expect(dead[0].payload.error.attempts).toBe(3);
    expect(deps.attempts.size).toBe(0);
  });

  test("a transient unknown error that succeeds on retry clears the counter", async () => {
    const { deps, dead } = makeDeps();
    const raw = rawOf(validRequest);
    await expect(processMessage(raw, ctx, async () => { throw new TypeError("blip"); }, deps)).rejects.toThrow();
    expect(deps.attempts.size).toBe(1);
    expect(await processMessage(raw, ctx, noop, deps)).toBe("handled");
    expect(deps.attempts.size).toBe(0);
    expect(dead).toEqual([]);
  });

  test("DLQ publish fails → rethrown as retryable, nothing skipped", async () => {
    const { deps } = makeDeps({
      publishDeadLetter: async () => { throw new Error("broker down"); },
    });
    await expect(processMessage(rawOf("{bad"), ctx, noop, deps)).rejects.toBeInstanceOf(RetryableError);
    await expect(
      processMessage(rawOf(validRequest), ctx, async () => { throw new NonRetryableError("x"); }, deps)
    ).rejects.toBeInstanceOf(RetryableError);
  });
});

describe("classifyError", () => {
  test("RetryableError subclasses → retry", () => {
    expect(classifyError(new RetryableError("x"))).toBe("retry");
    expect(classifyError(new StoreError("x"))).toBe("retry");
    expect(classifyError(new ProducerError("x"))).toBe("retry");
  });

  test("NonRetryableError subclasses → dead-letter", () => {
    expect(classifyError(new NonRetryableError("x"))).toBe("dead-letter");
    expect(classifyError(new ContractViolationError("x", []))).toBe("dead-letter");
    expect(classifyError(new KeyMismatchError("x"))).toBe("dead-letter");
    expect(classifyError(new OutboundContractViolationError("x", [], "T", TOPICS.CONVERSATION_EVENTS))).toBe("dead-letter");
  });

  test("an unwrapped KafkaJSNumberOfRetriesExceeded → retry (kafkajs flags it non-retriable)", () => {
    const err = new KafkaJSNumberOfRetriesExceeded(new Error("ECONNREFUSED"), { retryCount: 10, retryTime: 1000 });
    expect(err.retriable).toBe(false);
    expect(classifyError(err)).toBe("retry");
  });

  test("an unwrapped LEVEL_* error → retry", () => {
    const err = Object.assign(new Error("Database is not open"), { code: "LEVEL_DATABASE_NOT_OPEN" });
    expect(classifyError(err)).toBe("retry");
  });

  test("anything else → unknown", () => {
    expect(classifyError(new TypeError("x"))).toBe("unknown");
    expect(classifyError("string")).toBe("unknown");
    expect(classifyError(null)).toBe("unknown");
    expect(classifyError(Object.assign(new Error("x"), { code: "ENOENT" }))).toBe("unknown");
  });
});

// ─── onDeadLettered hook ──────────────────────────────────────────────────────

describe("processMessage: onDeadLettered hook", () => {
  const poisonQuery = {
    conversationId: CID,
    timestamp: 1,
    eventType: "UserQueryReceived",
    payload: { userInput: "", sessionId: "s" }, // empty userInput violates the contract
  };
  const validQuery = { ...poisonQuery, payload: { userInput: "hi", sessionId: "s" } };

  const userCommandsRaw = (value: unknown, over: Partial<RawMessage> = {}) =>
    rawOf(value, { topic: TOPICS.USER_COMMANDS, ...over });

  function hookCtx(owns: ("UserQueryReceived" | "SynthesizeFinalAnswerRequested")[], handle: (r: MessageDeadLettered) => Promise<void>) {
    return {
      topic: TOPICS.USER_COMMANDS,
      group: "g",
      service: "svc",
      types: ["UserQueryReceived", "SynthesizeFinalAnswerRequested"] as const,
      onDeadLettered: { owns, handle },
    };
  }

  test("is called once, after the DLQ record is published, with that record", async () => {
    const order: string[] = [];
    const { deps, dead } = makeDeps({
      publishDeadLetter: async (r) => {
        order.push("dlq");
        dead.push(r);
      },
    });
    const handle = mock(async (r: MessageDeadLettered) => {
      order.push(`hook:${r.conversationId}`);
      expect(dead).toHaveLength(1); // the record is already published
    });
    const out = await processMessage(userCommandsRaw(poisonQuery), hookCtx(["UserQueryReceived"], handle), noop, deps);
    expect(out).toBe("dead-lettered");
    expect(handle).toHaveBeenCalledTimes(1);
    expect(order).toEqual(["dlq", `hook:${CID}`]);
    expect(handle.mock.calls[0][0].payload.error.class).toBe("contract-violation");
  });

  test("is not called when the DLQ publish fails (the failure is retryable)", async () => {
    const { deps } = makeDeps({ publishDeadLetter: async () => { throw new Error("broker down"); } });
    const handle = mock(async (_r: MessageDeadLettered) => {});
    await expect(
      processMessage(userCommandsRaw(poisonQuery), hookCtx(["UserQueryReceived"], handle), noop, deps)
    ).rejects.toBeInstanceOf(RetryableError);
    expect(handle).not.toHaveBeenCalled();
  });

  test("is not called when the record has no conversationId", async () => {
    const { deps, dead } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    const noId = { ...poisonQuery, conversationId: "not-a-uuid" };
    await processMessage(userCommandsRaw(noId, { key: null }), hookCtx(["UserQueryReceived"], handle), noop, deps);
    expect(dead).toHaveLength(1);
    expect(dead[0].conversationId).toBeNull();
    expect(handle).not.toHaveBeenCalled();
  });

  test("a hook that throws is rethrown as retryable", async () => {
    const { deps, dead } = makeDeps();
    const handle = async () => { throw new Error("kafka down"); };
    const err = await processMessage(userCommandsRaw(poisonQuery), hookCtx(["UserQueryReceived"], handle), noop, deps).catch((e) => e);
    expect(err).toBeInstanceOf(RetryableError);
    expect(classifyError(err)).toBe("retry");
    expect(dead).toHaveLength(1); // the DLQ record stands; redelivery may duplicate both (tolerated)
  });

  test("ownership: with owns=[SynthesizeFinalAnswerRequested], a poison UserQueryReceived is dead-lettered but handle is NOT called", async () => {
    const { deps, dead } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    await processMessage(userCommandsRaw(poisonQuery), hookCtx(["SynthesizeFinalAnswerRequested"], handle), noop, deps);
    expect(dead).toHaveLength(1);
    expect(handle).not.toHaveBeenCalled();
  });

  test("ownership: with owns=[UserQueryReceived], the same message calls handle once", async () => {
    const { deps } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    await processMessage(userCommandsRaw(poisonQuery), hookCtx(["UserQueryReceived"], handle), noop, deps);
    expect(handle).toHaveBeenCalledTimes(1);
  });

  test("malformed JSON never calls handle, whatever owns contains", async () => {
    const { deps, dead } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    await processMessage(
      userCommandsRaw("{ not json", { key: Buffer.from(CID) }),
      hookCtx(["UserQueryReceived", "SynthesizeFinalAnswerRequested"], handle),
      noop,
      deps
    );
    await processMessage(
      userCommandsRaw(null),
      hookCtx(["UserQueryReceived", "SynthesizeFinalAnswerRequested"], handle),
      noop,
      deps
    );
    expect(dead.map((d) => d.payload.error.class)).toEqual(["malformed-json", "missing-value"]);
    expect(handle).not.toHaveBeenCalled();
  });

  test("a JSON value without a discriminator never calls handle", async () => {
    const { deps, dead } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    await processMessage(userCommandsRaw({ conversationId: CID, hello: 1 }), hookCtx(["UserQueryReceived"], handle), noop, deps);
    expect(dead).toHaveLength(1);
    expect(handle).not.toHaveBeenCalled();
  });

  test("an outbound violation while handling an owned input type calls handle", async () => {
    const { deps, dead } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    const out = await processMessage(
      userCommandsRaw(validQuery),
      hookCtx(["UserQueryReceived"], handle),
      async () => {
        throw new OutboundContractViolationError("bad PlanGenerated", [], "PlanGenerated", TOPICS.CONVERSATION_EVENTS);
      },
      deps
    );
    expect(out).toBe("dead-lettered");
    expect(dead[0].payload.error.class).toBe("outbound-contract-violation");
    expect(handle).toHaveBeenCalledTimes(1);
  });

  test("a handled message and a skipped type never call handle", async () => {
    const { deps } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    await processMessage(userCommandsRaw(validQuery), hookCtx(["UserQueryReceived"], handle), noop, deps);
    await processMessage(
      userCommandsRaw({ ...validQuery, eventType: "SomethingNew" }),
      hookCtx(["UserQueryReceived"], handle),
      noop,
      deps
    );
    expect(handle).not.toHaveBeenCalled();
  });

  test("an unknown-exhausted error calls handle only on the final attempt", async () => {
    const { deps } = makeDeps();
    const handle = mock(async (_r: MessageDeadLettered) => {});
    const c = hookCtx(["UserQueryReceived"], handle);
    const raw = userCommandsRaw(validQuery);
    const boom = async () => { throw new TypeError("bug"); };
    await expect(processMessage(raw, c, boom, deps)).rejects.toThrow();
    await expect(processMessage(raw, c, boom, deps)).rejects.toThrow();
    expect(handle).not.toHaveBeenCalled();
    expect(await processMessage(raw, c, boom, deps)).toBe("dead-lettered");
    expect(handle).toHaveBeenCalledTimes(1);
  });

  test("no hook configured: dead-lettering is unchanged", async () => {
    const { deps, dead } = makeDeps();
    const out = await processMessage(userCommandsRaw(poisonQuery), { ...hookCtx([], noop), onDeadLettered: undefined }, noop, deps);
    expect(out).toBe("dead-lettered");
    expect(dead).toHaveLength(1);
  });
});
