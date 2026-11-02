import { envInt } from "./env";

/** WEB_PORT, default 3001. */
export function webPort(env: Record<string, string | undefined> = process.env): number {
  return envInt(env, "WEB_PORT") ?? 3001;
}
