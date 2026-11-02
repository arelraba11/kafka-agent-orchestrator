import { describe, expect, test } from "bun:test";
import { TOPICS, type TopicName } from "../../shared/topics";
import { cidForPartition, newCid, partitionOf, tag, token, useHarness, waitFor } from "./harness";

// Poison records are injected on purpose, so invalid records on contract topics are expected.
const h = useHarness("poison", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "math", "weather", "exchange", "chat", "web", "rag"],
  allowInvalid: true,
});

const PARTITIONS = [0, 1, 2];
// the groups on conversation-events: orchestrator and web handle ToolInvocationResulted,
// the aggregator handles only PlanCompleted (it skips a well-formed result)
const CONV_GROUPS = ["orchestrator-service", "aggregator-service", "ui-web-final-answer"];
const RESULT_GROUPS = ["orchestrator-service", "ui-web-final-answer"];
// the groups on tool-invocation-requests: every tool worker, the Python one included
const WORKER_GROUPS = ["math-tool-worker", "weather-tool-worker", "exchange-tool-worker", "chat-tool-worker", "rag-tool-worker"];
// the groups on user-commands
const COMMAND_GROUPS = ["router-plan-service", "answer-synthesizer"];

const result = (cid: string, withStepIndex: boolean) => ({
  conversationId: cid,
  timestamp: Date.now(),
  eventType: "ToolInvocationResulted",
  payload: { toolName: "math", ...(withStepIndex ? { stepIndex: 0 } : {}), result: { value: "4", success: true } },
});

/** Adds one expected dead-letter record per group for `key`. */
function expectDlq(expected: Record<string, number>, groups: string[], cls: string, key: string) {
  for (const g of groups) expected[`${g}|${cls}|${key}`] = 1;
}

/** The file's tests share one namespace, so each test looks only at the dead letters of its own keys. */
const keyOf = (countKey: string) => countKey.split("|")[2];
const dlqOf = (keys: Set<string>) => Object.fromEntries(Object.entries(h.observer.dlqCounts()).filter(([k]) => keys.has(keyOf(k))));

/** Waits for the expected dead letters, settles, and checks the exact counts (plus every record's shape). */
async function expectExactDlq(expected: Record<string, number>, topic: TopicName, partitions = PARTITIONS) {
  const keys = new Set(Object.keys(expected).map(keyOf));
  const total = Object.keys(expected).length;
  await waitFor(() => Object.values(dlqOf(keys)).reduce((a, b) => a + b, 0) >= total, {
    what: `${total} dead-letter records`,
    context: () => JSON.stringify(h.observer.dlqCounts(), null, 1),
  });
  await h.settle();
  expect(dlqOf(keys)).toEqual(expected);
  for (const r of h.observer.dlq().filter((r) => keys.has(r.payload.source.key!))) {
    expect(r.payload.source.topic).toBe(topic); // logical, never the namespace
    expect(partitions).toContain(r.payload.source.partition);
  }
}

describe("poison messages", () => {
  test("P1: poison on user-commands is dead-lettered by both groups that read it; the synthesizer reports its own command", async () => {
    const run = token();
    const expected: Record<string, number> = {};
    const commandCids: string[] = [];

    for (const p of PARTITIONS) {
      const malformedKey = `cmd-malformed-${run}-${p}`;
      const cid = cidForPartition(p);
      commandCids.push(cid);
      await h.produce(TOPICS.USER_COMMANDS, malformedKey, "{not json", p);
      // a synthesize command without its payload
      await h.produce(
        TOPICS.USER_COMMANDS,
        cid,
        JSON.stringify({ conversationId: cid, sessionId: `s-${run}`, timestamp: Date.now(), commandType: "SynthesizeFinalAnswerRequested" }),
        p
      );
      expectDlq(expected, COMMAND_GROUPS, "malformed-json", malformedKey);
      expectDlq(expected, ["answer-synthesizer"], "contract-violation", cid);
    }

    await expectExactDlq(expected, "user-commands");
    // the synthesizer owns SynthesizeFinalAnswerRequested: exactly one failure per command
    for (const cid of commandCids) {
      const failed = h.observer.events(cid, "ConversationFailed");
      expect(failed, `ConversationFailed for ${cid}`).toHaveLength(1);
      expect(failed[0].value.payload).toMatchObject({ stage: "synthesis", code: "internal-error" });
    }
    expect(h.observer.messages.filter((m) => m.type === "ConversationFailed")).toHaveLength(3);
  });

  test("P2: poison on conversation-events is dead-lettered by every group that reads it, per partition", async () => {
    const run = token();
    const expected: Record<string, number> = {};
    const add = (groups: string[], cls: string, key: string) => expectDlq(expected, groups, cls, key);
    const ownKeys: string[] = [];

    for (const p of PARTITIONS) {
      const malformedKey = `malformed-${run}-${p}`;
      const invalidKey = newCid();
      const mismatchedBody = newCid();
      const mismatchedKey = newCid();
      ownKeys.push(malformedKey, invalidKey, mismatchedBody, mismatchedKey);

      await h.produce(TOPICS.CONVERSATION_EVENTS, malformedKey, "{not json", p);
      await h.produce(TOPICS.CONVERSATION_EVENTS, invalidKey, JSON.stringify(result(invalidKey, false)), p);
      await h.produce(TOPICS.CONVERSATION_EVENTS, mismatchedKey, JSON.stringify(result(mismatchedBody, true)), p);

      add(CONV_GROUPS, "malformed-json", malformedKey);
      add(RESULT_GROUPS, "contract-violation", invalidKey);
      add(RESULT_GROUPS, "key-mismatch", mismatchedKey);
    }

    await expectExactDlq(expected, "conversation-events");
    for (const r of h.observer.dlq().filter((r) => ownKeys.includes(r.payload.source.key!))) {
      expect(partitionOf(r.payload.source.key!)).toBeDefined();
    }
    // no service owns a failure of these types, so nobody tells a user
    expect(h.observer.messages.filter((m) => m.type === "ConversationFailed" && ownKeys.includes(m.value?.conversationId))).toEqual([]);
  });

  test("P3: poison on tool-invocation-requests is dead-lettered by every worker group, the Python one included", async () => {
    const run = token();
    const expected: Record<string, number> = {};
    const ragPid = h.services.pid("rag");

    for (const p of PARTITIONS) {
      const malformedKey = `req-malformed-${run}-${p}`;
      const invalidKey = newCid();
      await h.produce(TOPICS.TOOL_INVOCATION_REQUESTS, malformedKey, "{not json", p);
      // a RAG request without its stepIndex
      await h.produce(
        TOPICS.TOOL_INVOCATION_REQUESTS,
        invalidKey,
        JSON.stringify({
          conversationId: invalidKey,
          timestamp: Date.now(),
          eventType: "ToolInvocationRequested",
          payload: { toolName: "getProductInformation", input: { query: "iPhone price" } },
        }),
        p
      );
      expectDlq(expected, WORKER_GROUPS, "malformed-json", malformedKey);
      expectDlq(expected, WORKER_GROUPS, "contract-violation", invalidKey);
    }

    await expectExactDlq(expected, "tool-invocation-requests");
    expect(h.services.isRunning("rag")).toBe(true);
    expect(h.services.pid("rag")).toBe(ragPid);
  });

  test("P4: a poison UserQueryReceived is reported once, by the router, as a routing failure", async () => {
    const cid = newCid();
    await h.produce(
      TOPICS.USER_COMMANDS,
      cid,
      JSON.stringify({ conversationId: cid, timestamp: Date.now(), eventType: "UserQueryReceived", payload: { userInput: "", sessionId: "s-p4" } })
    );

    const expected: Record<string, number> = {};
    expectDlq(expected, COMMAND_GROUPS, "contract-violation", cid);
    await waitFor(() => h.observer.dlqCounts()[`router-plan-service|contract-violation|${cid}`] === 1, {
      what: `the router's dead letter for ${cid}`,
      context: () => JSON.stringify(h.observer.dlqCounts(), null, 1),
    });
    await h.settle();

    const mine = Object.fromEntries(Object.entries(h.observer.dlqCounts()).filter(([k]) => k.endsWith(`|${cid}`)));
    expect(mine).toEqual(expected);
    const failed = h.observer.events(cid, "ConversationFailed");
    expect(failed, "exactly one ConversationFailed").toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "routing", code: "internal-error" });
    expect(failed.filter((m) => m.value.payload.stage === "synthesis")).toEqual([]); // the synthesizer's cache fill is not owned
    expect(h.observer.events(cid, "PlanGenerated")).toEqual([]);
  });

  /** A PlanGenerated that breaks its contract (an empty step list). */
  const badPlan = (cid: string) =>
    JSON.stringify({ conversationId: cid, sessionId: "s-po", timestamp: Date.now(), eventType: "PlanGenerated", payload: { steps: [] } });

  test("PO1: a poison PlanGenerated for a conversation with no plan is reported once, as an orchestration failure", async () => {
    const cid = newCid();
    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, badPlan(cid));

    await waitFor(() => h.observer.dlqCounts()[`orchestrator-service|contract-violation|${cid}`] === 1, {
      what: `the orchestrator's dead letter for ${cid}`,
      context: () => JSON.stringify(h.observer.dlqCounts(), null, 1),
    });
    await h.settle();

    const mine = Object.fromEntries(Object.entries(h.observer.dlqCounts()).filter(([k]) => k.endsWith(`|${cid}`)));
    expect(mine).toEqual({ [`orchestrator-service|contract-violation|${cid}`]: 1 });
    const failed = h.observer.events(cid, "ConversationFailed");
    expect(failed, "exactly one ConversationFailed").toHaveLength(1);
    expect(failed[0].value.payload).toMatchObject({ stage: "orchestration", code: "internal-error" });
  });

  test("PO2: a poison PlanGenerated for a conversation whose plan exists is dead-lettered but not reported", async () => {
    const t = token();
    const input = `say hi ${tag(t)}`;
    h.fake.plan(t, [{ tool: "chat", args: { userInput: input } }]);
    h.fake.answer(t, "Hi.");
    const { cid } = await h.ask(input); // the plan now exists in the orchestrator's store
    await h.settle();

    await h.produce(TOPICS.CONVERSATION_EVENTS, cid, badPlan(cid));
    await waitFor(() => h.observer.dlqCounts()[`orchestrator-service|contract-violation|${cid}`] === 1, {
      what: `the orchestrator's dead letter for ${cid}`,
      context: () => JSON.stringify(h.observer.dlqCounts(), null, 1),
    });
    await h.settle();

    const mine = Object.fromEntries(Object.entries(h.observer.dlqCounts()).filter(([k]) => k.endsWith(`|${cid}`)));
    expect(mine).toEqual({ [`orchestrator-service|contract-violation|${cid}`]: 1 });
    // an existing plan has its own failure path (the step timeout); the hook stays silent
    expect(h.observer.events(cid, "ConversationFailed")).toEqual([]);
    expect(h.observer.events(cid, "PlanFailed")).toEqual([]);
  });

  // runs last in the file: it proves the partitions the poison was sent to keep flowing
  test("P-flow: partitions keep flowing after poison", async () => {
    const pids = Object.fromEntries(
      (["router", "orchestrator", "aggregator", "synthesizer", "math", "web", "rag"] as const).map((n) => [n, h.services.pid(n)])
    );

    const cases = PARTITIONS.map((p) => {
      const t = token();
      const cid = cidForPartition(p);
      h.fake.plan(t, [{ tool: "math", args: { expression: "6*7" } }]);
      h.fake.answer(t, `answer-${p}`);
      return { p, t, cid };
    });
    for (const c of cases) {
      await h.produce(
        TOPICS.USER_COMMANDS,
        c.cid,
        JSON.stringify({
          conversationId: c.cid,
          timestamp: Date.now(),
          eventType: "UserQueryReceived",
          payload: { userInput: `6*7 ${tag(c.t)}`, sessionId: `s-${c.t}` },
        })
      );
    }

    for (const c of cases) {
      await waitFor(() => h.observer.events(c.cid, "FinalAnswerSynthesized")[0], {
        what: `FinalAnswerSynthesized for partition ${c.p} (${c.cid})`,
        context: () => h.observer.describe(c.cid),
      });
    }
    await h.settle(); // every live group at lag 0 on every partition

    for (const c of cases) {
      expect(h.observer.events(c.cid, "FinalAnswerSynthesized")).toHaveLength(1);
      expect(h.observer.events(c.cid, "FinalAnswerSynthesized")[0].value.payload.answer).toBe(`answer-${c.p}`);
    }
    for (const [name, pid] of Object.entries(pids)) {
      expect(h.services.isRunning(name as never), `${name} still running`).toBe(true);
      expect(h.services.pid(name as never), `${name} pid`).toBe(pid);
    }
  });
});
