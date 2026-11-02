import { describe, expect, test } from "bun:test";
import { LlmError, type LlmErrorKind } from "../../../shared/llm/errors";
import { ProducerError, RetryableError, StoreError } from "../../../shared/kafka/errors";
import type { ConversationFailed } from "../../../shared/schemas/ConversationFailed";
import type { FinalAnswerSynthesized } from "../../../shared/schemas/FinalAnswerSynthesized";
import type { SynthesizeFinalAnswerRequested } from "../../../shared/schemas/SynthesizeFinalAnswerRequested";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { BatchOp, SynthStore } from "../../../shared/state/synthStore";
import { cacheUserQuery, failureCodeOf, synthesize, type SynthesizeDeps } from "./synthesize";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const SESSION = "sess-1";

const COMMAND: SynthesizeFinalAnswerRequested = {
  conversationId: CID,
  sessionId: SESSION,
  timestamp: 1_000,
  commandType: "SynthesizeFinalAnswerRequested",
  payload: { results: [{ toolName: "math", result: { value: "108" } }] },
};

/** Fake store with real semantics: missing → null. Records every operation in `ops`. */
function makeStore(ops: string[], initial: Record<string, unknown> = {}) {
  const data = new Map<string, unknown>(Object.entries(initial));
  const failNext: { put?: Error; batch?: Error } = {};
  const store: SynthStore = {
    async get(key) {
      ops.push(`get ${key}`);
      return (data.has(key) ? data.get(key) : null) as never;
    },
    async put(key, value) {
      ops.push(`put ${key}`);
      if (failNext.put) throw failNext.put;
      data.set(key, value);
    },
    async batch(batchOps: BatchOp[]) {
      ops.push(`batch [${batchOps.map((o) => `${o.type} ${o.key}`).join(", ")}]`);
      if (failNext.batch) throw failNext.batch;
      for (const o of batchOps) {
        if (o.type === "put") data.set(o.key, o.value);
        else data.delete(o.key);
      }
    },
  };
  return { store, data, failNext };
}

type Published = FinalAnswerSynthesized | ConversationFailed;

function setup(opts: { initial?: Record<string, unknown>; llm?: () => Promise<string> } = {}) {
  const ops: string[] = [];
  const { store, data, failNext } = makeStore(ops, opts.initial ?? { [`q:${CID}`]: { userInput: "what is 12*9", sessionId: SESSION } });
  const published: Published[] = [];
  const history: { sessionId: string; cid: string; user: string; assistant: string }[] = [];
  const control = { publishError: null as Error | null, historyError: null as Error | null, llmCalls: 0 };

  const deps: SynthesizeDeps = {
    store,
    async callLLM(prompt) {
      ops.push("llm");
      control.llmCalls++;
      expect(prompt).toContain("what is 12*9");
      return opts.llm ? opts.llm() : "It is 108.";
    },
    async publish(message) {
      ops.push(`publish ${message.eventType}`);
      if (control.publishError) throw control.publishError;
      published.push(message);
    },
    async getHistory() {
      return [];
    },
    async appendToHistory(sessionId, cid, user, assistant) {
      ops.push("history");
      if (control.historyError) throw control.historyError;
      // idempotent by conversationId, like the real store
      if (!history.some((h) => h.cid === cid)) history.push({ sessionId, cid, user, assistant });
    },
    now: () => 2_000,
    log: { log: () => {}, error: () => {} },
  };
  return { deps, ops, data, failNext, published, history, control };
}

const writes = (ops: string[]) => ops.filter((o) => !o.startsWith("get "));

describe("happy path", () => {
  test("LLM → put o: → publish → history → one batch, in that order", async () => {
    const t = setup();
    await synthesize(COMMAND, t.deps);

    expect(writes(t.ops)).toEqual([
      "llm",
      `put o:${CID}`,
      "publish FinalAnswerSynthesized",
      "history",
      `batch [put done:${CID}, del q:${CID}, del o:${CID}]`,
    ]);
    expect(t.published).toHaveLength(1);
    expect(t.published[0]).toMatchObject({ eventType: "FinalAnswerSynthesized", payload: { answer: "It is 108." } });
    expect(t.history).toEqual([{ sessionId: SESSION, cid: CID, user: "what is 12*9", assistant: "It is 108." }]);
    expect(t.data.has(`done:${CID}`)).toBe(true);
    expect(t.data.has(`q:${CID}`)).toBe(false);
    expect(t.data.has(`o:${CID}`)).toBe(false);
    expect(t.ops.filter((o) => o.startsWith("batch"))).toHaveLength(1);
  });
});

describe("failure outcomes", () => {
  const cases: [string, () => Promise<string>, string][] = [
    ["transient exhausted", async () => { throw new LlmError("timeout", "t", { provider: "openai" }); }, "llm-unavailable"],
    ["rate limited", async () => { throw new LlmError("rate-limited", "r", { provider: "openai" }); }, "llm-unavailable"],
    ["auth", async () => { throw new LlmError("auth", "401", { provider: "openai" }); }, "llm-misconfigured"],
    ["config", async () => { throw new LlmError("config", "no key", { provider: "openai" }); }, "llm-misconfigured"],
    ["empty", async () => { throw new LlmError("empty-response", "blank", { provider: "openai" }); }, "empty-answer"],
  ];

  for (const [name, llm, code] of cases) {
    test(`${name} → put o: → publish ConversationFailed(${code}) → batch; no history; returns normally`, async () => {
      const t = setup({ llm });
      await synthesize(COMMAND, t.deps); // resolves: the offset commits, no DLQ record

      expect(writes(t.ops)).toEqual([
        "llm",
        `put o:${CID}`,
        "publish ConversationFailed",
        `batch [put done:${CID}, del q:${CID}, del o:${CID}]`,
      ]);
      expect(t.published[0]).toMatchObject({ eventType: "ConversationFailed", payload: { stage: "synthesis", code } });
      expect(t.history).toEqual([]);
      expect(t.data.has(`done:${CID}`)).toBe(true);
    });
  }

  test("missing question → ConversationFailed(missing-question) with no LLM call", async () => {
    const t = setup({ initial: {} });
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(0);
    expect(writes(t.ops)).toEqual([
      `put o:${CID}`,
      "publish ConversationFailed",
      `batch [put done:${CID}, del q:${CID}, del o:${CID}]`,
    ]);
    expect(t.published[0]).toMatchObject({ payload: { stage: "synthesis", code: "missing-question" } });
    expect(t.history).toEqual([]);
  });

  test("a non-LLM error from the LLM step propagates and nothing is written", async () => {
    const t = setup({ llm: async () => { throw new TypeError("bug"); } });
    await expect(synthesize(COMMAND, t.deps)).rejects.toThrow("bug");
    expect(writes(t.ops)).toEqual(["llm"]);
  });

  test("failureCodeOf maps every kind", () => {
    const kinds: [LlmErrorKind, ReturnType<typeof failureCodeOf>][] = [
      ["timeout", "llm-unavailable"], ["network", "llm-unavailable"], ["rate-limited", "llm-unavailable"],
      ["server", "llm-unavailable"], ["auth", "llm-misconfigured"], ["config", "llm-misconfigured"],
      ["empty-response", "empty-answer"],
    ];
    for (const [kind, code] of kinds) expect(failureCodeOf(new LlmError(kind, "x", { provider: "p" }))).toBe(code);
  });
});

describe("crash windows", () => {
  test("crash after publish, before done (history throws): redelivery republishes, no LLM call, one history entry, then done", async () => {
    const t = setup();
    t.control.historyError = new StoreError("disk full");
    await expect(synthesize(COMMAND, t.deps)).rejects.toBeInstanceOf(StoreError);
    expect(t.data.has(`done:${CID}`)).toBe(false);
    expect(t.data.has(`o:${CID}`)).toBe(true); // the memo survives
    expect(t.published).toHaveLength(1);
    expect(t.control.llmCalls).toBe(1);

    t.control.historyError = null; // redelivery
    await synthesize(COMMAND, t.deps);
    expect(t.published).toHaveLength(2); // duplicate answer frame, tolerated
    expect(t.published[1]).toEqual({ ...t.published[0] });
    expect(t.control.llmCalls).toBe(1); // no second LLM call
    expect(t.history).toHaveLength(1);
    expect(t.data.has(`done:${CID}`)).toBe(true);
    expect(t.data.has(`q:${CID}`)).toBe(false);
  });

  test("crash after publish, before done (batch throws): same recovery", async () => {
    const t = setup();
    t.failNext.batch = new StoreError("leveldb gone");
    await expect(synthesize(COMMAND, t.deps)).rejects.toBeInstanceOf(StoreError);
    expect(t.data.has(`done:${CID}`)).toBe(false);

    t.failNext.batch = undefined;
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(1);
    expect(t.published).toHaveLength(2);
    expect(t.history).toHaveLength(1);
    expect(t.data.has(`done:${CID}`)).toBe(true);
  });

  test("crash after o:, before publish: redelivery publishes from the memo with no LLM call", async () => {
    const t = setup();
    t.control.publishError = new ProducerError("kafka down");
    await expect(synthesize(COMMAND, t.deps)).rejects.toBeInstanceOf(ProducerError);
    expect(t.data.has(`o:${CID}`)).toBe(true);
    expect(t.published).toHaveLength(0);
    expect(t.control.llmCalls).toBe(1);

    t.control.publishError = null;
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(1);
    expect(t.published).toHaveLength(1);
    expect(t.published[0]).toMatchObject({ eventType: "FinalAnswerSynthesized", payload: { answer: "It is 108." } });
  });

  test("crash before o: is written (put throws): redelivery recomputes, nothing was published", async () => {
    const t = setup();
    t.failNext.put = new StoreError("leveldb gone");
    await expect(synthesize(COMMAND, t.deps)).rejects.toBeInstanceOf(StoreError);
    expect(t.published).toHaveLength(0);

    t.failNext.put = undefined;
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(2); // at most one wasted call
    expect(t.published).toHaveLength(1);
  });

  test("a memoized failure is republished without an LLM call", async () => {
    const t = setup({
      initial: {
        [`q:${CID}`]: { userInput: "what is 12*9", sessionId: SESSION },
        [`o:${CID}`]: { kind: "failed", code: "llm-unavailable", reason: "timeout" },
      },
    });
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(0);
    expect(t.published[0]).toMatchObject({ eventType: "ConversationFailed", payload: { code: "llm-unavailable" } });
  });

  test("a history StoreError propagates and done stays absent", async () => {
    const t = setup();
    t.control.historyError = new StoreError("disk");
    await expect(synthesize(COMMAND, t.deps)).rejects.toBeInstanceOf(StoreError);
    expect(t.data.has(`done:${CID}`)).toBe(false);
    expect(t.ops.some((o) => o.startsWith("batch"))).toBe(false);
  });
});

describe("done is never set without a successful publish", () => {
  const outcomes: [string, Record<string, unknown>, (() => Promise<string>) | undefined][] = [
    ["answer", { [`q:${CID}`]: { userInput: "what is 12*9", sessionId: SESSION } }, undefined],
    ["llm failure", { [`q:${CID}`]: { userInput: "what is 12*9", sessionId: SESSION } }, async () => { throw new LlmError("timeout", "t", { provider: "x" }); }],
    ["auth failure", { [`q:${CID}`]: { userInput: "what is 12*9", sessionId: SESSION } }, async () => { throw new LlmError("auth", "a", { provider: "x" }); }],
    ["empty answer", { [`q:${CID}`]: { userInput: "what is 12*9", sessionId: SESSION } }, async () => { throw new LlmError("empty-response", "e", { provider: "x" }); }],
    ["missing question", {}, undefined],
  ];

  for (const [name, initial, llm] of outcomes) {
    test(`${name}: a publish failure rethrows a retryable error and done stays absent`, async () => {
      const t = setup({ initial, llm });
      t.control.publishError = new ProducerError("kafka down");
      const err = await synthesize(COMMAND, t.deps).catch((e) => e);
      expect(err).toBeInstanceOf(RetryableError);
      expect(t.data.has(`done:${CID}`)).toBe(false);
      expect(t.ops.some((o) => o.startsWith("batch"))).toBe(false);
      expect(t.ops.some((o) => o === "history")).toBe(false);
    });

    test(`${name}: done is written only after the publish`, async () => {
      const t = setup({ initial, llm });
      await synthesize(COMMAND, t.deps);
      const w = writes(t.ops);
      const publishAt = w.findIndex((o) => o.startsWith("publish"));
      const doneAt = w.findIndex((o) => o.includes(`put done:${CID}`));
      expect(publishAt).toBeGreaterThanOrEqual(0);
      expect(doneAt).toBeGreaterThan(publishAt);
    });
  }
});

describe("duplicates", () => {
  test("done: present → no LLM call, no publish, no history, no writes", async () => {
    const t = setup({ initial: { [`done:${CID}`]: { at: 1 } } });
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(0);
    expect(t.published).toHaveLength(0);
    expect(t.history).toHaveLength(0);
    expect(writes(t.ops)).toEqual([]);
  });

  test("a second identical command after completion is ignored", async () => {
    const t = setup();
    await synthesize(COMMAND, t.deps);
    await synthesize({ ...COMMAND, timestamp: 5_000 }, t.deps);
    expect(t.control.llmCalls).toBe(1);
    expect(t.published).toHaveLength(1);
    expect(t.history).toHaveLength(1);
  });

  test("a missing done: reads as not done (the fake returns null like the real store)", async () => {
    const t = setup();
    await synthesize(COMMAND, t.deps);
    expect(t.control.llmCalls).toBe(1); // answered, not 'ignored duplicate'
  });
});

describe("question cache fill", () => {
  const QUERY: UserQueryReceived = {
    conversationId: CID,
    timestamp: 1,
    eventType: "UserQueryReceived",
    payload: { userInput: "hello", sessionId: SESSION },
  };

  test("stores q:<cid> when the conversation is not done", async () => {
    const ops: string[] = [];
    const { store, data } = makeStore(ops);
    await cacheUserQuery(QUERY, { store });
    expect(data.get(`q:${CID}`)).toEqual({ userInput: "hello", sessionId: SESSION });
  });

  test("skips the put when done:<cid> exists, so no orphan q: key is left", async () => {
    const ops: string[] = [];
    const { store, data } = makeStore(ops, { [`done:${CID}`]: { at: 1 } });
    await cacheUserQuery(QUERY, { store });
    expect(data.has(`q:${CID}`)).toBe(false);
    expect(writes(ops)).toEqual([]);
  });
});
