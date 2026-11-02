import type { StepTimedOut } from "../../../shared/schemas/StepTimedOut";
import type { PlanState } from "../../../shared/state/planStore";

// Step timers only PUBLISH a StepTimedOut event; they never read or mutate plan
// state. The event then flows through the conversation's partition and the pure
// transition() decides: whichever of the result and the timeout comes first in the log
// wins, deterministically. A timer that fires late, twice, or for an old step is
// harmless: transition() ignores it as stale-timeout or plan-terminal.

export const REARM_DELAY_MS = 5_000;

export interface StepTimerDeps {
  setTimer(fn: () => void, ms: number): unknown;
  clearTimer(handle: unknown): void;
  now(): number;
  /** Publishes the event to conversation-events. Rejects on failure. */
  publish(event: StepTimedOut): Promise<void>;
  log?: Pick<Console, "log" | "warn">;
}

interface Armed {
  handle: unknown;
  stepIndex: number;
  deadline: number;
}

export function createStepTimers(deps: StepTimerDeps) {
  const log = deps.log ?? console;
  const armed = new Map<string, Armed>();

  function arm(conversationId: string, stepIndex: number, deadline: number, delayMs: number): void {
    const entry: Armed = { handle: undefined, stepIndex, deadline };
    entry.handle = deps.setTimer(() => void fire(conversationId, entry), Math.max(0, delayMs));
    armed.set(conversationId, entry);
  }

  async function fire(conversationId: string, entry: Armed): Promise<void> {
    if (armed.get(conversationId) !== entry) return; // replaced or cancelled meanwhile
    const event: StepTimedOut = {
      conversationId,
      timestamp: deps.now(),
      eventType: "StepTimedOut",
      payload: { stepIndex: entry.stepIndex, deadline: entry.deadline },
    };
    try {
      await deps.publish(event);
    } catch (err) {
      if (armed.get(conversationId) !== entry) return;
      log.warn(
        `[orchestrator] conversationId=${conversationId} StepTimedOut publish failed (${(err as Error).message}); re-arming in ${REARM_DELAY_MS}ms`
      );
      arm(conversationId, entry.stepIndex, entry.deadline, REARM_DELAY_MS);
      return;
    }
    if (armed.get(conversationId) === entry) armed.delete(conversationId);
    log.log(`[orchestrator] conversationId=${conversationId} step ${entry.stepIndex} timed out; StepTimedOut published`);
  }

  return {
    /** Arms (or replaces) the conversation's timer to fire at `deadline`. Overdue fires immediately. */
    schedule(conversationId: string, stepIndex: number, deadline: number): void {
      this.cancel(conversationId);
      arm(conversationId, stepIndex, deadline, deadline - deps.now());
    },

    cancel(conversationId: string): void {
      const existing = armed.get(conversationId);
      if (!existing) return;
      deps.clearTimer(existing.handle);
      armed.delete(conversationId);
    },

    /** Number of armed timers. */
    get size(): number {
      return armed.size;
    },
  };
}

export type StepTimers = ReturnType<typeof createStepTimers>;

/**
 * Startup scan: arm a timer for every running plan at its stored deadline. Overdue
 * plans fire immediately. A plan with no deadline (persisted before step timeouts
 * existed) gets now + stepTimeoutMs. Returns how many timers were armed.
 */
export function rescheduleRunning(
  timers: StepTimers,
  plans: ReadonlyArray<{ conversationId: string; state: PlanState }>,
  now: number,
  stepTimeoutMs: number
): number {
  let count = 0;
  for (const { conversationId, state } of plans) {
    if (state.status !== "running") continue;
    timers.schedule(conversationId, state.stepIndex, state.stepDeadline ?? now + stepTimeoutMs);
    count++;
  }
  return count;
}
