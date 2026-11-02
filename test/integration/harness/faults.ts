import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { join } from "path";
import { type FaultPoint as TsFaultPoint } from "../../../shared/testing/faultPoint";
import { waitFor } from "./wait";

/** The TS points plus the Python worker's (src/python/rag/fault_point.py), which have no TS call site. */
export type FaultPoint = TsFaultPoint | "rag:before-send" | "rag:after-send";

// Controls the env-gated fault points (shared/testing/faultPoint.ts) of ONE service.
// Each service gets its own FAULT_DIR under the file's temp dir.

export interface Faults {
  dir: string;
  /** Env entries that crash the process at `point` (optionally only for one orchestrator decision). */
  crashAt(point: FaultPoint, decision?: string, nth?: number): Record<string, string>;
  /**
   * Env entries that hold the first matching call at `point`. `nth` (1-based) selects the n-th call
   * of the point instead; only the Python worker's `rag:*` points support it.
   */
  holdAt(point: FaultPoint, decision?: string, nth?: number): Record<string, string>;
  /** How many times the point was reached (with `decision`: only calls made with that decision). */
  hits(point: FaultPoint, decision?: string): number;
  reached(point: FaultPoint): boolean;
  waitReached(point: FaultPoint, timeoutMs?: number): Promise<void>;
  release(point: FaultPoint): void;
  /** Forget a point's files (before restarting a service that already held or crashed there). */
  clear(point: FaultPoint): void;
}

const spec = (point: FaultPoint, decision?: string, nth?: number) =>
  `${point}${decision ? `@${decision}` : ""}${nth !== undefined ? `#${nth}` : ""}`;

export function faultsFor(dir: string): Faults {
  mkdirSync(dir, { recursive: true });
  const file = (point: FaultPoint, ext: string) => join(dir, `${point}.${ext}`);
  return {
    dir,
    crashAt: (point, decision, nth) => ({ FAULT_DIR: dir, FAULT_CRASH: spec(point, decision, nth) }),
    holdAt: (point, decision, nth) => ({ FAULT_DIR: dir, FAULT_HOLD: spec(point, decision, nth) }),
    hits(point, decision) {
      const f = file(point, "hits");
      if (!existsSync(f)) return 0;
      const lines = readFileSync(f, "utf8").split("\n").filter(Boolean);
      return decision === undefined ? lines.length : lines.filter((l) => l === decision).length;
    },
    reached: (point) => existsSync(file(point, "reached")),
    async waitReached(point, timeoutMs = 15_000) {
      await waitFor(() => existsSync(file(point, "reached")), { timeoutMs, what: `fault point ${point} to be reached` });
    },
    release: (point) => writeFileSync(file(point, "release"), ""),
    clear(point) {
      for (const ext of ["hits", "reached", "release"]) rmSync(file(point, ext), { force: true });
    },
  };
}
