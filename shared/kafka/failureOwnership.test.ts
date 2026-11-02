import { describe, expect, test } from "bun:test";
import { FAILURE_OWNERSHIP, WEB_OWNED_TYPES } from "./failureOwnership";

const entries = Object.entries(FAILURE_OWNERSHIP) as [
  keyof typeof FAILURE_OWNERSHIP,
  (typeof FAILURE_OWNERSHIP)[keyof typeof FAILURE_OWNERSHIP],
][];

describe("dead-letter ownership", () => {
  test("every message type is owned by at most one service (so a failure is reported once)", () => {
    const owners = new Map<string, string[]>();
    for (const [service, { owns }] of entries) owners.set(owns, [...(owners.get(owns) ?? []), service]);
    for (const type of WEB_OWNED_TYPES) owners.set(type, [...(owners.get(type) ?? []), "web"]);
    for (const [type, services] of owners) expect({ type, services: services.length }).toEqual({ type, services: 1 });
  });

  test("UserQueryReceived → router / routing", () => {
    expect(FAILURE_OWNERSHIP.router).toEqual({ owns: "UserQueryReceived", stage: "routing" });
  });

  test("SynthesizeFinalAnswerRequested → synthesizer / synthesis", () => {
    expect(FAILURE_OWNERSHIP.synthesizer).toEqual({ owns: "SynthesizeFinalAnswerRequested", stage: "synthesis" });
  });

  test("PlanCompleted → aggregator / aggregation", () => {
    expect(FAILURE_OWNERSHIP.aggregator).toEqual({ owns: "PlanCompleted", stage: "aggregation" });
  });

  test("PlanGenerated → orchestrator / orchestration", () => {
    expect(FAILURE_OWNERSHIP.orchestrator).toEqual({ owns: "PlanGenerated", stage: "orchestration" });
  });

  test("the synthesizer does not own UserQueryReceived (the router reports a poison query)", () => {
    expect(FAILURE_OWNERSHIP.synthesizer.owns).not.toBe("UserQueryReceived");
    expect(entries.filter(([, e]) => e.owns === "UserQueryReceived").map(([s]) => s)).toEqual(["router"]);
  });

  test("the web server owns the final events and nothing else", () => {
    expect([...WEB_OWNED_TYPES].sort()).toEqual(["ConversationFailed", "FinalAnswerSynthesized", "PlanFailed"]);
  });
});
