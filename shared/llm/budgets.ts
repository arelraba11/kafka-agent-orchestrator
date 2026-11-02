import { envInt } from "../env";

type Env = Record<string, string | undefined>;

/** Router: one attempt within this budget (LLM_ROUTER_BUDGET_MS, default 15 s). */
export function routerBudgetMs(env: Env = process.env): number {
  return envInt(env, "LLM_ROUTER_BUDGET_MS") ?? 15_000;
}

/** Synthesizer: up to 3 attempts within this budget (LLM_SYNTH_BUDGET_MS, default 60 s). */
export function synthBudgetMs(env: Env = process.env): number {
  return envInt(env, "LLM_SYNTH_BUDGET_MS") ?? 60_000;
}
