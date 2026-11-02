import { Level } from "level";
import { join } from "path";
import { StoreError } from "../kafka/errors";

// ─── Synthesizer store (.synth-store/) ────────────────────────────────────────
// Same pattern as planStore: sync writes, I/O failures wrapped in StoreError
// (retryable), and a MISSING KEY is null, never an error.
//
//   q:<cid>     { userInput, sessionId }   the original question, written when
//                                          UserQueryReceived is consumed
//   o:<cid>     Outcome                    the memoized result of the LLM step
//   done:<cid>  { at }                     the terminal event was published

export type FailureCode = "llm-unavailable" | "llm-misconfigured" | "empty-answer" | "missing-question";

export type Outcome =
  | { kind: "answer"; answer: string }
  | { kind: "failed"; code: FailureCode; reason: string };

export interface StoredQuery {
  userInput: string;
  sessionId: string;
}

export interface DoneMarker {
  at: number;
}

export interface SynthValues {
  q: StoredQuery;
  o: Outcome;
  done: DoneMarker;
}

export type SynthKey = { [K in keyof SynthValues]: `${K}:${string}` }[keyof SynthValues];
type ValueOf<K extends SynthKey> = K extends `${infer P}:${string}` ? P extends keyof SynthValues ? SynthValues[P] : never : never;

export type BatchOp =
  | { type: "put"; key: SynthKey; value: SynthValues[keyof SynthValues] }
  | { type: "del"; key: SynthKey };

/** What the synthesizer needs from its store; the real one is below, tests use a fake. */
export interface SynthStore {
  /** null when the key is missing. Rejects with StoreError only on an I/O failure. */
  get<K extends SynthKey>(key: K): Promise<ValueOf<K> | null>;
  put<K extends SynthKey>(key: K, value: ValueOf<K>): Promise<void>;
  /** Atomic. Deleting an absent key is not an error. */
  batch(ops: BatchOp[]): Promise<void>;
}

/** SYNTH_STORE_PATH, default `<cwd>/.synth-store`. */
export function defaultSynthStorePath(env: Record<string, string | undefined> = process.env): string {
  return env.SYNTH_STORE_PATH || join(process.cwd(), ".synth-store");
}

let db: Level<string, string>;

/** Opens the store. `path` is optional so tests can use a temp dir. */
export async function initializeSynthStore(path: string = defaultSynthStorePath()): Promise<void> {
  if (db && db.status === "open") return;
  try {
    db = new Level<string, string>(path, { valueEncoding: "utf8" });
    await db.open();
  } catch (cause) {
    throw new StoreError(`synthStore open failed: ${(cause as Error).message}`, { cause });
  }
}

export async function closeSynthStore(): Promise<void> {
  if (db && db.status === "open") await db.close();
}

export const synthStore: SynthStore = {
  async get(key) {
    let raw: string | undefined;
    try {
      raw = await db.get(key);
    } catch (err: any) {
      // Legacy abstract-level threw for a missing key; level 10 returns undefined.
      if (err?.code === "LEVEL_NOT_FOUND") return null;
      throw new StoreError(`synthStore read failed: ${err?.message ?? err}`, { cause: err });
    }
    if (raw === undefined) return null;
    return JSON.parse(raw);
  },

  async put(key, value) {
    try {
      await db.put(key, JSON.stringify(value), { sync: true });
    } catch (err: any) {
      throw new StoreError(`synthStore write failed: ${err?.message ?? err}`, { cause: err });
    }
  },

  async batch(ops) {
    try {
      await db.batch(
        ops.map((op) =>
          op.type === "put"
            ? { type: "put" as const, key: op.key, value: JSON.stringify(op.value) }
            : { type: "del" as const, key: op.key }
        ),
        { sync: true }
      );
    } catch (err: any) {
      throw new StoreError(`synthStore batch failed: ${err?.message ?? err}`, { cause: err });
    }
  },
};
