// Pure restart policy for scripts/supervisor.ts, unit-tested without spawning anything.
//
// Backoff 1 s → 30 s cap (doubling), reset after a run of 60 s. After 5 crashes within
// 2 min the service is given up on (the supervisor logs `GAVE UP <service>`). A requested
// stop never restarts.

export const BACKOFF_INITIAL_MS = 1_000;
export const BACKOFF_MAX_MS = 30_000;
export const RESET_UPTIME_MS = 60_000;
export const GIVE_UP_CRASHES = 5;
export const GIVE_UP_WINDOW_MS = 120_000;

export interface RestartState {
  /** Delay to use for the next restart. */
  backoffMs: number;
  /** Timestamps of recent crashes, pruned to the give-up window. */
  crashes: number[];
}

export const initialRestartState: RestartState = { backoffMs: BACKOFF_INITIAL_MS, crashes: [] };

export interface ExitInfo {
  now: number;
  /** How long the process ran before it exited. */
  uptimeMs: number;
  stopRequested: boolean;
}

export type RestartDecision =
  | { action: "none" }
  | { action: "restart"; delayMs: number; state: RestartState }
  | { action: "give-up"; state: RestartState };

export function onServiceExit(state: RestartState, exit: ExitInfo): RestartDecision {
  if (exit.stopRequested) return { action: "none" };

  const crashes = [...state.crashes.filter((t) => exit.now - t <= GIVE_UP_WINDOW_MS), exit.now];
  const backoffMs = exit.uptimeMs >= RESET_UPTIME_MS ? BACKOFF_INITIAL_MS : state.backoffMs;

  if (crashes.length >= GIVE_UP_CRASHES) {
    return { action: "give-up", state: { backoffMs, crashes } };
  }
  return {
    action: "restart",
    delayMs: backoffMs,
    state: { backoffMs: Math.min(backoffMs * 2, BACKOFF_MAX_MS), crashes },
  };
}
