import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { join } from "path";
import { type FaultPoint } from "../../../shared/testing/faultPoint";
import { waitFor } from "./wait";

// Controls the env-gated fault points (shared/testing/faultPoint.ts) of ONE service.
// Each service gets its own FAULT_DIR under the file's temp dir.

export interface Faults {
  dir: string;
  /** Env entries that crash the process at `point` (optionally only for one orchestrator decision). */
  crashAt(point: FaultPoint, decision?: string): Record<string, string>;
  /** Env entries that hold the first matching call at `point`. */
  holdAt(point: FaultPoint, decision?: string): Record<string, string>;
  /** How many times the point was reached (with `decision`: only calls made with that decision). */
  hits(point: FaultPoint, decision?: string): number;
  reached(point: FaultPoint): boolean;
  waitReached(point: FaultPoint, timeoutMs?: number): Promise<void>;
  release(point: FaultPoint): void;
  /** Forget a point's files (before restarting a service that already held or crashed there). */
  clear(point: FaultPoint): void;
}

const spec = (point: FaultPoint, decision?: string) => (decision ? `${point}@${decision}` : point);

export function faultsFor(dir: string): Faults {
  mkdirSync(dir, { recursive: true });
  const file = (point: FaultPoint, ext: string) => join(dir, `${point}.${ext}`);
  return {
    dir,
    crashAt: (point, decision) => ({ FAULT_DIR: dir, FAULT_CRASH: spec(point, decision) }),
    holdAt: (point, decision) => ({ FAULT_DIR: dir, FAULT_HOLD: spec(point, decision) }),
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
