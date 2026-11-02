// The ONLY place in test/integration that waits on time. Every wait in a test is a
// condition with a deadline: waitFor() re-evaluates its predicate on every wake() (an
// observer message or a process exit) and at least every POLL_MS (fault files, group
// state), and fails with a description plus whatever `context()` can show.

const POLL_MS = 20;

type Waker = () => void;
const wakers = new Set<Waker>();

/** Called by the observer and the service runner: something observable changed. */
export function wake(): void {
  for (const w of [...wakers]) w();
}

export interface WaitOptions {
  timeoutMs?: number;
  /** What we were waiting for, in the failure message. */
  what: string;
  /** Extra detail for the failure message (for example the matching observed messages). */
  context?: () => string;
}

export async function waitFor<T>(
  predicate: () => T | Promise<T>,
  { timeoutMs = 15_000, what, context }: WaitOptions
): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await predicate();
    if (value) return value as NonNullable<T>;
    const left = deadline - Date.now();
    if (left <= 0) {
      const extra = context ? `\n${context()}` : "";
      throw new Error(`timed out after ${timeoutMs}ms waiting for: ${what}${extra}`);
    }
    await new Promise<void>((resolve) => {
      const timer = setTimeout(done, Math.min(POLL_MS, left));
      function done() {
        clearTimeout(timer);
        wakers.delete(done);
        resolve();
      }
      wakers.add(done);
    });
  }
}
