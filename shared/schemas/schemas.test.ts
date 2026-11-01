import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "fs";
import { join } from "path";
import {
  MESSAGE_SCHEMAS,
  TOPIC_MESSAGES,
  keyOf,
  messageTypeOf,
  type MessageName,
} from "./index";
import { TOPICS } from "../topics";

const FIXTURES_DIR = join(import.meta.dir, "__fixtures__");

function fixtures(name: string, kind: "valid" | "invalid"): [string, unknown][] {
  const dir = join(FIXTURES_DIR, name, kind);
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .sort()
    .map((f) => [f, JSON.parse(readFileSync(join(dir, f), "utf8"))]);
}

const NAMES = Object.keys(MESSAGE_SCHEMAS) as MessageName[];

describe("fixtures", () => {
  test("every message has at least one valid and one invalid fixture", () => {
    for (const name of NAMES) {
      expect(fixtures(name, "valid").length).toBeGreaterThan(0);
      expect(fixtures(name, "invalid").length).toBeGreaterThan(0);
    }
  });

  for (const name of NAMES) {
    describe(name, () => {
      for (const [file, body] of fixtures(name, "valid")) {
        test(`accepts valid/${file}`, () => {
          const r = MESSAGE_SCHEMAS[name].safeParse(body);
          if (!r.success) throw new Error(JSON.stringify(r.error.issues));
        });
      }
      for (const [file, body] of fixtures(name, "invalid")) {
        test(`rejects invalid/${file}`, () => {
          expect(MESSAGE_SCHEMAS[name].safeParse(body).success).toBe(false);
        });
      }
    });
  }
});

describe("tolerant reader", () => {
  test("an unknown field is accepted and stripped from the parsed value", () => {
    const body = {
      conversationId: "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b",
      timestamp: 1,
      eventType: "UserQueryReceived",
      traceId: "abc",
      payload: { userInput: "hi", sessionId: "s", futureField: true },
    };
    const r = MESSAGE_SCHEMAS.UserQueryReceived.safeParse(body);
    expect(r.success).toBe(true);
    if (r.success) {
      expect(r.data).not.toHaveProperty("traceId");
      expect(r.data.payload).not.toHaveProperty("futureField");
    }
  });

  test("open result records keep their extra keys", () => {
    const r = MESSAGE_SCHEMAS.ToolInvocationResulted.safeParse({
      conversationId: "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b",
      timestamp: 1,
      eventType: "ToolInvocationResulted",
      payload: { toolName: "weather", stepIndex: 0, result: { value: "x", city: "Paris" } },
    });
    expect(r.success).toBe(true);
    if (r.success) expect(r.data.payload.result.city).toBe("Paris");
  });
});

describe("registry", () => {
  test("messageTypeOf reads eventType, then commandType", () => {
    expect(messageTypeOf({ eventType: "PlanFailed" })).toBe("PlanFailed");
    expect(messageTypeOf({ commandType: "SynthesizeFinalAnswerRequested" })).toBe(
      "SynthesizeFinalAnswerRequested"
    );
    expect(messageTypeOf({})).toBeUndefined();
    expect(messageTypeOf(null)).toBeUndefined();
    expect(messageTypeOf("x")).toBeUndefined();
    expect(messageTypeOf({ eventType: 5 })).toBeUndefined();
  });

  test("every registered message belongs to exactly one topic", () => {
    const seen = new Map<string, string>();
    for (const [topic, names] of Object.entries(TOPIC_MESSAGES)) {
      for (const n of names) {
        expect(seen.has(n)).toBe(false);
        seen.set(n, topic);
      }
    }
    expect([...seen.keys()].sort()).toEqual([...NAMES].sort());
  });

  test("keyOf is the conversationId, or the original key for a dead letter", () => {
    const [, dl] = fixtures("MessageDeadLettered", "valid").find(([f]) => f === "malformed-json.json")!;
    expect(keyOf(TOPICS.DEAD_LETTER_QUEUE, dl as never)).toBeNull();
    const [, ev] = fixtures("PlanFailed", "valid")[0];
    expect(keyOf(TOPICS.CONVERSATION_EVENTS, ev as never)).toBe(
      "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b"
    );
  });
});
