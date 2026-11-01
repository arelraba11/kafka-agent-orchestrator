import { Level } from "level";
import { join } from "path";
import type { PlanStep } from "../schemas/PlanGenerated";
import type { StepResultEntry } from "../schemas/common";
import { StoreError } from "../kafka/errors";

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

/**
 * Opens the store. `path` is optional so tests can use a temp dir; the default is
 * `.plan-store` in the working directory.
 */
export async function initializeStore(path: string = DB_PATH): Promise<void> {
  if (db && db.status === "open") return;
  try {
    db = new Level<string, string>(path, { valueEncoding: "utf8" });
    await db.open();
  } catch (cause) {
    throw new StoreError(`planStore open failed: ${(cause as Error).message}`, { cause });
  }
}

export async function closeStore(): Promise<void> {
  if (db && db.status === "open") {
    await db.close();
  }
}

/**
 * Returns the stored state, or null when the key is missing. A missing key is NOT a
 * failure: level 10 returns undefined for it (a legacy LEVEL_NOT_FOUND is also
 * handled). Every first PlanGenerated of a conversation reads a missing key, so
 * wrapping it in StoreError would redeliver that message forever. Only I/O failures
 * become StoreError (retryable).
 */
export async function getPlan(conversationId: string): Promise<PlanState | null> {
  let raw: string | undefined;
  try {
    raw = await db.get(conversationId);
  } catch (err: any) {
    if (err?.code === "LEVEL_NOT_FOUND") return null;
    console.error(`[planStore] getPlan error | conversationId=${conversationId}`, err);
    throw new StoreError(`planStore read failed: ${err?.message ?? err}`, { cause: err });
  }
  if (raw === undefined) return null;
  return JSON.parse(raw) as PlanState;
}

export async function savePlan(conversationId: string, state: PlanState): Promise<void> {
  try {
    await db.put(conversationId, JSON.stringify(state), { sync: true });
  } catch (err: any) {
    console.error(`[planStore] savePlan error | conversationId=${conversationId}`, err);
    throw new StoreError(`planStore write failed: ${err?.message ?? err}`, { cause: err });
  }
}
