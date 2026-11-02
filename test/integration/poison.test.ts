import { describe, expect, test } from "bun:test";
import { TOPICS } from "../../shared/topics";
import { cidForPartition, newCid, partitionOf, tag, token, useHarness, waitFor } from "./harness";

// Poison records are injected on purpose, so invalid records on contract topics are expected.
const h = useHarness("poison", {
  start: ["router", "orchestrator", "aggregator", "synthesizer", "math", "weather", "exchange", "chat", "web"],
  allowInvalid: true,
});

const PARTITIONS = [0, 1, 2];
// the groups on conversation-events: orchestrator and web handle ToolInvocationResulted,
// the aggregator handles only PlanCompleted (it skips a well-formed result)
const CONV_GROUPS = ["orchestrator-service", "aggregator-service", "ui-web-final-answer"];
const RESULT_GROUPS = ["orchestrator-service", "ui-web-final-answer"];

const result = (cid: string, withStepIndex: boolean) => ({
  conversationId: cid,
  timestamp: Date.now(),
  eventType: "ToolInvocationResulted",
  payload: { toolName: "math", ...(withStepIndex ? { stepIndex: 0 } : {}), result: { value: "4", success: true } },
});

describe("poison messages", () => {
  test("P2: poison on conversation-events is dead-lettered by every group that reads it, per partition", async () => {
    const run = token();
    const expected: Record<string, number> = {};
    const add = (groups: string[], cls: string, key: string) => {
      for (const g of groups) expected[`${g}|${cls}|${key}`] = 1;
    };

    for (const p of PARTITIONS) {
      const malformedKey = `malformed-${run}-${p}`;
      const invalidKey = newCid();
      const mismatchedBody = newCid();
      const mismatchedKey = newCid();

      await h.produce(TOPICS.CONVERSATION_EVENTS, malformedKey, "{not json", p);
      await h.produce(TOPICS.CONVERSATION_EVENTS, invalidKey, JSON.stringify(result(invalidKey, false)), p);
      await h.produce(TOPICS.CONVERSATION_EVENTS, mismatchedKey, JSON.stringify(result(mismatchedBody, true)), p);

      add(CONV_GROUPS, "malformed-json", malformedKey);
      add(RESULT_GROUPS, "contract-violation", invalidKey);
      add(RESULT_GROUPS, "key-mismatch", mismatchedKey);
    }

    await waitFor(() => h.observer.dlq().length >= Object.keys(expected).length, {
      what: `${Object.keys(expected).length} dead-letter records`,
      context: () => JSON.stringify(h.observer.dlqCounts(), null, 1),
    });
    await h.settle();

    expect(h.observer.dlqCounts()).toEqual(expected);
    for (const r of h.observer.dlq()) {
      expect(r.payload.source.topic).toBe("conversation-events"); // logical, never the namespace
      expect(partitionOf(r.payload.source.key!)).toBeDefined();
      expect(PARTITIONS).toContain(r.payload.source.partition);
    }
    // no service owns a failure of these types, so nobody tells a user
    expect(h.observer.messages.filter((m) => m.type === "ConversationFailed")).toEqual([]);
  });

  // runs last in the file: it proves the partitions the poison was sent to keep flowing
  test("P-flow: partitions keep flowing after poison", async () => {
    const pids = Object.fromEntries(
      (["router", "orchestrator", "aggregator", "synthesizer", "math", "web"] as const).map((n) => [n, h.services.pid(n)])
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
