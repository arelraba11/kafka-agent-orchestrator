import { describe, expect, test } from "bun:test";
import { MESSAGE_SCHEMAS } from "../schemas/index";
import type { MessageDeadLettered } from "../schemas/MessageDeadLettered";
import { buildConversationFailed } from "./conversationFailed";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";

const record = (conversationId: string | null): MessageDeadLettered => ({
  eventType: "MessageDeadLettered",
  schemaVersion: 1,
  conversationId,
  timestamp: 1,
  payload: {
    source: { topic: "user-commands", partition: 2, offset: "17", key: CID, timestamp: "1" },
    consumer: { group: "router-plan-service", service: "router" },
    error: { class: "contract-violation", name: "ContractViolationError", reason: "invalid UserQueryReceived", attempts: 1 },
    originalValue: null,
  },
});

describe("buildConversationFailed", () => {
  test("builds a valid ConversationFailed{internal-error} that names the source and cause", () => {
    const e = buildConversationFailed(record(CID), "routing", 99);
    expect(MESSAGE_SCHEMAS.ConversationFailed.safeParse(e).success).toBe(true);
    expect(e).toMatchObject({
      conversationId: CID,
      timestamp: 99,
      eventType: "ConversationFailed",
      payload: { stage: "routing", code: "internal-error" },
    });
    expect(e.payload.reason).toContain("user-commands");
    expect(e.payload.reason).toContain("offset 17");
    expect(e.payload.reason).toContain("router");
    expect(e.payload.reason).toContain("contract-violation");
  });

  test("refuses a record with no conversationId", () => {
    expect(() => buildConversationFailed(record(null), "routing", 1)).toThrow();
  });
});
