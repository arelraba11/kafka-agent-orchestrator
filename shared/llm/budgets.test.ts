import { describe, expect, test } from "bun:test";
import { routerBudgetMs, synthBudgetMs } from "./budgets";

describe("LLM caller budgets", () => {
  test("defaults are 15 s (router) and 60 s (synthesizer)", () => {
    expect(routerBudgetMs({})).toBe(15_000);
    expect(synthBudgetMs({})).toBe(60_000);
  });
  test("env overrides", () => {
    expect(routerBudgetMs({ LLM_ROUTER_BUDGET_MS: "3000" })).toBe(3000);
    expect(synthBudgetMs({ LLM_SYNTH_BUDGET_MS: "8000" })).toBe(8000);
  });
});
