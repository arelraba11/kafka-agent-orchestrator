/** One global bound on how long a dispatched tool step may go unanswered. */
export const DEFAULT_STEP_TIMEOUT_MS = 30_000;

/** STEP_TIMEOUT_MS when it is a positive number, otherwise the default. Shared so the web server words the timeout the way the orchestrator enforces it. */
export function stepTimeoutMs(env: Record<string, string | undefined> = process.env): number {
  const n = Number(env.STEP_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_STEP_TIMEOUT_MS;
}
