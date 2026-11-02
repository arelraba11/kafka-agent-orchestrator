import { describe, expect, test } from "bun:test";
import { TOPICS, logicalTopic, physicalGroup, physicalTopic } from "./topics";

describe("topic and group namespacing", () => {
  test("an empty namespace is the identity", () => {
    for (const name of Object.values(TOPICS)) {
      expect(physicalTopic("", name)).toBe(name);
      expect(logicalTopic("", name)).toBe(name);
    }
    expect(physicalGroup("", "orchestrator-service")).toBe("orchestrator-service");
  });

  test("physicalTopic prefixes and logicalTopic maps back, for every topic", () => {
    for (const name of Object.values(TOPICS)) {
      const physical = physicalTopic("ab12cd34-happy-", name);
      expect(physical).toBe(`ab12cd34-happy-${name}`);
      expect(logicalTopic("ab12cd34-happy-", physical)).toBe(name);
    }
  });

  test("logicalTopic returns null for a topic outside the namespace or unknown", () => {
    expect(logicalTopic("ns-", "conversation-events")).toBeNull();
    expect(logicalTopic("ns-", "other-conversation-events")).toBeNull();
    expect(logicalTopic("ns-", "ns-not-a-topic")).toBeNull();
  });

  test("physicalGroup prefixes the group", () => {
    expect(physicalGroup("ns-", "rag-tool-worker")).toBe("ns-rag-tool-worker");
  });
});
