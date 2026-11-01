import { Level } from "level";
import { join } from "path";
import type { PlanStep } from "../schemas/PlanGenerated";
import type { StepResultEntry } from "../schemas/common";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface PlanState {
  plan: PlanStep[];                  // ordered steps with tool name + args
  stepIndex: number;                 // index of the step awaiting a result; equals plan.length once completed
  results: StepResultEntry[];        // accumulated tool results
  status: "running" | "completed" | "failed";
  planReceivedAt: number;            // epoch ms — used for latency benchmarking
  sessionId: string;                 // groups conversations into a session for history
}

// ─── DB instance ──────────────────────────────────────────────────────────────

const DB_PATH = join(process.cwd(), ".plan-store");

let db: Level<string, string>;

// ─── Public API ───────────────────────────────────────────────────────────────

export async function initializeStore(): Promise<void> {
  if (db && db.status === "open") return;
  db = new Level<string, string>(DB_PATH, { valueEncoding: "utf8" });
  await db.open();
}

export async function closeStore(): Promise<void> {
  if (db && db.status === "open") {
    await db.close();
  }
}

export async function getPlan(conversationId: string): Promise<PlanState | null> {
  try {
    const raw = await db.get(conversationId);
    // level 10 / abstract-level 3 returns undefined for a missing key instead of
    // throwing LEVEL_NOT_FOUND.
    if (raw === undefined) return null;
    return JSON.parse(raw) as PlanState;
  } catch (err: any) {
    if (err.code === "LEVEL_NOT_FOUND") return null;
    console.error(`[planStore] getPlan error | conversationId=${conversationId}`, err);
    throw err;
  }
}

export async function savePlan(conversationId: string, state: PlanState): Promise<void> {
  try {
    await db.put(conversationId, JSON.stringify(state), { sync: true });
  } catch (err) {
    console.error(`[planStore] savePlan error | conversationId=${conversationId}`, err);
    throw err;
  }
}
