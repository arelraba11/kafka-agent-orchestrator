/**
 * webBuild.ts — decides whether the frontend must be (re)built before `bun run start`.
 * Side-effect free at import time (start.ts runs its main flow on import).
 */

import { existsSync, readdirSync, statSync } from "fs";
import { join } from "path";

/** True when there is no build, or any build input is newer than it. */
export function needsWebBuild(distMtimeMs: number | null, sourceMtimesMs: number[]): boolean {
  if (distMtimeMs === null) return true;
  return sourceMtimesMs.some((m) => m > distMtimeMs);
}

const TOP_LEVEL_INPUTS = /^(index\.html|package\.json|bun\.lock|vite\.config\.ts|tsconfig.*\.json)$/;

function walkMtimes(dir: string, out: number[]): void {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) walkMtimes(p, out);
    else out.push(statSync(p).mtimeMs);
  }
}

/** mtimes of every file the frontend build depends on. */
export function webSourceMtimes(frontendDir: string): number[] {
  const out: number[] = [];
  const src = join(frontendDir, "src");
  if (existsSync(src)) walkMtimes(src, out);
  for (const entry of readdirSync(frontendDir, { withFileTypes: true })) {
    if (entry.isFile() && TOP_LEVEL_INPUTS.test(entry.name)) {
      out.push(statSync(join(frontendDir, entry.name)).mtimeMs);
    }
  }
  return out;
}

/** mtime of dist/index.html, or null when the UI is not built. */
export function distIndexMtime(frontendDir: string): number | null {
  const index = join(frontendDir, "dist", "index.html");
  return existsSync(index) ? statSync(index).mtimeMs : null;
}
