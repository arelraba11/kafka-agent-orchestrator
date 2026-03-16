import { Level } from "level";
import { join } from "path";
import type { PlanStep } from "../schemas/PlanGenerated";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface PlanState {
  plan: PlanStep[];                  // ordered steps with tool name + args
  stepIndex: number;                 // index of the next step to dispatch
  results: Record<string, unknown>[]; // accumulated tool results
  status: "in_progress" | "completed";
  planReceivedAt: number;            // epoch ms — used for latency benchmarking
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
    return JSON.parse(raw) as PlanState;
  } catch (err: any) {
    if (err.code === "LEVEL_NOT_FOUND") return null;
    console.error(`[planStore] getPlan error | conversationId=${conversationId}`, err);
    return null;
  }
}

export async function savePlan(conversationId: string, state: PlanState): Promise<void> {
  try {
    await db.put(conversationId, JSON.stringify(state));
  } catch (err) {
    console.error(`[planStore] savePlan error | conversationId=${conversationId}`, err);
  }
}

export async function updatePlan(
  conversationId: string,
  partial: Partial<PlanState>
): Promise<void> {
  const existing = await getPlan(conversationId);
  if (!existing) {
    console.error(`[planStore] updatePlan: no state found | conversationId=${conversationId}`);
    return;
  }
  await savePlan(conversationId, { ...existing, ...partial });
}

export async function deletePlan(conversationId: string): Promise<void> {
  try {
    await db.del(conversationId);
  } catch (err) {
    console.error(`[planStore] deletePlan error | conversationId=${conversationId}`, err);
  }
}
