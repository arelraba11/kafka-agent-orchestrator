/**
 * Reads a positive integer from env. Unset or empty → undefined (the caller applies its
 * default). A set but invalid value throws, naming the variable: a typo in a timeout
 * must not silently run with the default.
 */
export function envInt(env: Record<string, string | undefined>, name: string): number | undefined {
  const raw = env[name];
  if (raw === undefined || raw === "") return undefined;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`${name} must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return n;
}
