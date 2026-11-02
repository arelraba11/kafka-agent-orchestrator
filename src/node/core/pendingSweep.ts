// Side-effect-free TTL sweep for the web server's pending map. Memory hygiene only: an
// entry whose answer never arrives (and whose socket stays open) is dropped after the TTL.
// The client's own idle timeout is what tells the user.

export const PENDING_TTL_MS = 5 * 60_000;

export function sweepPending(
  pending: Map<string, { at: number }>,
  now: number,
  ttlMs: number = PENDING_TTL_MS
): string[] {
  const dropped: string[] = [];
  for (const [conversationId, entry] of pending) {
    if (now - entry.at > ttlMs) {
      pending.delete(conversationId);
      dropped.push(conversationId);
    }
  }
  return dropped;
}
