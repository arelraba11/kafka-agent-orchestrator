import { appendFileSync, existsSync, writeFileSync } from "fs";
import { join } from "path";

// Env-gated fault injection for the integration suite. Production code calls
// faultPoint("<name>") at named places; without FAULT_DIR it returns immediately.
//
//   FAULT_DIR          enables everything; per-point files live here
//   FAULT_CRASH=<point>[@<decision>]   SIGKILL this process when the point is reached
//   FAULT_HOLD=<point>[@<decision>]    on the FIRST matching call: write <point>.reached,
//                                      then wait for <point>.release (polled every 20 ms)
//
// Every call (matching or not) appends one line to <FAULT_DIR>/<point>.hits.
// `@<decision>` restricts a point to one orchestrator decision (started | advanced | completed).
// Python twin: src/python/rag/fault_point.py.

export type FaultPoint =
  | "orchestrator:before-emit"
  | "orchestrator:after-emit"
  | "orchestrator:after-persist"
  | "synthesizer:after-llm"
  | "synthesizer:after-memo"
  | "synthesizer:after-publish"
  | "synthesizer:after-history"
  | "kafka:retry";

type Env = Record<string, string | undefined>;

export interface FaultSpec {
  point: string;
  decision?: string;
}

export function parseFaultSpec(raw: string | undefined): FaultSpec | null {
  if (!raw) return null;
  const at = raw.indexOf("@");
  return at === -1 ? { point: raw } : { point: raw.slice(0, at), decision: raw.slice(at + 1) };
}

const POLL_MS = 20;

function matches(spec: FaultSpec | null, name: string, decision: string | undefined): boolean {
  if (spec === null || spec.point !== name) return false;
  return spec.decision === undefined || spec.decision === decision;
}

/** Builds a faultPoint bound to `env` (read once, here). Exported so tests can inject env. */
export function createFaultPoint(env: Env) {
  const dir = env.FAULT_DIR;
  const crash = parseFaultSpec(env.FAULT_CRASH);
  const hold = parseFaultSpec(env.FAULT_HOLD);
  let held = false;

  return async function faultPoint(name: FaultPoint, ctx?: { decision?: string }): Promise<void> {
    if (!dir) return;
    const decision = ctx?.decision;

    // sync, so the count is on disk even if the next line kills the process
    appendFileSync(join(dir, `${name}.hits`), `${decision ?? "-"}\n`);

    if (matches(crash, name, decision)) {
      process.kill(process.pid, "SIGKILL");
      await new Promise<never>(() => {}); // never run another instruction of the caller
    }

    if (!held && matches(hold, name, decision)) {
      held = true;
      writeFileSync(join(dir, `${name}.reached`), "");
      const release = join(dir, `${name}.release`);
      while (!existsSync(release)) {
        await new Promise((r) => setTimeout(r, POLL_MS));
      }
    }
  };
}

export const faultPoint = createFaultPoint(process.env);
