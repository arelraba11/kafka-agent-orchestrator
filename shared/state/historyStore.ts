// ─── History Store ────────────────────────────────────────────────────────────
// Persists conversation history in history.json (HISTORY_PATH overrides the path),
// keyed by sessionId. Format: { [sessionId]: HistoryMessage[] }
//
// SINGLE-WRITER INVARIANT: only the answer synthesizer calls appendToHistory. The
// router reads, and never writes. The in-process mutex below serializes appends within
// the synthesizer; there is deliberately no cross-process lock. A second writer
// process would lose updates (read-modify-write), so do not add one without moving
// this store behind a lock or a database.
//
// Writes are atomic (temp file + fsync + rename), so a reader sees the old file or
// the new one, never a half-written one. A file that does not parse or validate is
// quarantined by the writer (history.json.corrupt-<ts>) instead of being overwritten.

import { randomBytes } from "crypto";
import { closeSync, fsyncSync, openSync, writeSync } from "fs";
import { readFile, rename, rm } from "fs/promises";
import { basename, dirname, join } from "path";
import { z } from "zod";
import { StoreError } from "../kafka/errors";

const HistoryMessageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.string(),
  /** Which conversation produced this message. Absent on entries written before phase 4. */
  conversationId: z.string().optional(),
});
export const HistoryFileSchema = z.record(z.string(), z.array(HistoryMessageSchema));

export type HistoryMessage = z.infer<typeof HistoryMessageSchema>;
type HistoryFile = z.infer<typeof HistoryFileSchema>;

type ReadResult =
  | { kind: "ok"; data: HistoryFile }
  | { kind: "missing" }
  | { kind: "corrupt"; reason: string };

export interface HistoryStoreOptions {
  /** Resolved on every call, so tests can point it at a temp dir. */
  path?: () => string;
  /** Test hook: runs after the temp file is written and synced, before the rename. */
  beforeRename?: (tmpPath: string) => void | Promise<void>;
  log?: Pick<Console, "error" | "warn">;
}

export function defaultHistoryPath(): string {
  return process.env.HISTORY_PATH ?? join(process.cwd(), "history.json");
}

export function createHistoryStore(options: HistoryStoreOptions = {}) {
  const resolvePath = options.path ?? defaultHistoryPath;
  const log = options.log ?? console;

  async function read(path: string): Promise<ReadResult> {
    let text: string;
    try {
      text = await readFile(path, "utf8");
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return { kind: "missing" };
      throw new StoreError(`history read failed: ${(err as Error).message}`, { cause: err });
    }
    let json: unknown;
    try {
      json = JSON.parse(text);
    } catch (err) {
      return { kind: "corrupt", reason: `not valid JSON: ${(err as Error).message}` };
    }
    const parsed = HistoryFileSchema.safeParse(json);
    if (!parsed.success) {
      return { kind: "corrupt", reason: `schema mismatch: ${parsed.error.issues[0]?.message ?? "invalid"}` };
    }
    return { kind: "ok", data: parsed.data };
  }

  /** temp file in the same directory → fsync → rename over the target. */
  async function writeAtomic(path: string, data: HistoryFile): Promise<void> {
    const tmp = join(dirname(path), `${basename(path)}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`);
    try {
      const fd = openSync(tmp, "wx");
      try {
        writeSync(fd, JSON.stringify(data, null, 2));
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      await options.beforeRename?.(tmp);
      await rename(tmp, path);
    } catch (err) {
      await rm(tmp, { force: true }).catch(() => {});
      throw err instanceof StoreError ? err : new StoreError(`history write failed: ${(err as Error).message}`, { cause: err });
    }
  }

  /** Reader: never writes. A corrupt or unreadable file is logged and treated as empty. */
  async function getHistory(sessionId: string): Promise<HistoryMessage[]> {
    const path = resolvePath();
    try {
      const r = await read(path);
      if (r.kind === "ok") return r.data[sessionId] ?? [];
      if (r.kind === "corrupt") {
        log.error(`[history] ERROR ${path} is corrupt (${r.reason}); using empty history until the writer quarantines it`);
      }
    } catch (err) {
      log.error(`[history] ERROR could not read ${path}: ${(err as Error).message}; using empty history`);
    }
    return [];
  }

  // Promise-chain mutex: appends run one at a time, in call order.
  let tail: Promise<unknown> = Promise.resolve();
  function serialized<T>(fn: () => Promise<T>): Promise<T> {
    const run = tail.then(fn, fn);
    tail = run.catch(() => {});
    return run;
  }

  /**
   * Appends one exchange. Idempotent by conversationId: if the session already holds
   * an entry for this conversation the call is a no-op, so a redelivered or duplicate
   * synthesis cannot double the history. Throws StoreError (retryable) on disk failure.
   */
  function appendToHistory(
    sessionId: string,
    conversationId: string,
    userMsg: string,
    assistantMsg: string
  ): Promise<void> {
    return serialized(async () => {
      const path = resolvePath();
      const r = await read(path);
      let all: HistoryFile = {};
      if (r.kind === "ok") {
        all = r.data;
      } else if (r.kind === "corrupt") {
        const quarantine = `${path}.corrupt-${Date.now()}`;
        try {
          await rename(path, quarantine);
        } catch (err) {
          throw new StoreError(`could not quarantine corrupt history: ${(err as Error).message}`, { cause: err });
        }
        log.error(`[history] ERROR ${path} was corrupt (${r.reason}); moved to ${quarantine}, starting a fresh history`);
      }

      const existing = all[sessionId] ?? [];
      if (existing.some((m) => m.conversationId === conversationId)) {
        log.warn(`[history] conversationId=${conversationId} already in session history; skipping append`);
        return;
      }
      all[sessionId] = [
        ...existing,
        { role: "user", content: userMsg, conversationId },
        { role: "assistant", content: assistantMsg, conversationId },
      ];
      await writeAtomic(path, all);
    });
  }

  return { getHistory, appendToHistory };
}

const defaultStore = createHistoryStore();
export const getHistory = defaultStore.getHistory;
export const appendToHistory = defaultStore.appendToHistory;
