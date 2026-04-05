// ─── History Store ────────────────────────────────────────────────────────────
// Persists conversation history in history.json keyed by sessionId.
// Format: { [sessionId]: HistoryMessage[] }

import { join } from "path";

export interface HistoryMessage {
  role: "user" | "assistant";
  content: string;
}

const HISTORY_PATH = join(process.cwd(), "history.json");

let writeLock = false;

async function readAll(): Promise<Record<string, HistoryMessage[]>> {
  try {
    const file = Bun.file(HISTORY_PATH);
    if (!(await file.exists())) return {};
    const text = await file.text();
    return JSON.parse(text) as Record<string, HistoryMessage[]>;
  } catch {
    return {};
  }
}

async function writeAll(data: Record<string, HistoryMessage[]>): Promise<void> {
  await Bun.write(HISTORY_PATH, JSON.stringify(data, null, 2));
}

export async function getHistory(sessionId: string): Promise<HistoryMessage[]> {
  const all = await readAll();
  return all[sessionId] ?? [];
}

export async function appendToHistory(
  sessionId: string,
  userMsg: string,
  assistantMsg: string
): Promise<void> {
  // Simple spin-lock to prevent concurrent write corruption
  while (writeLock) {
    await new Promise((r) => setTimeout(r, 10));
  }
  writeLock = true;
  try {
    const all = await readAll();
    const existing = all[sessionId] ?? [];
    all[sessionId] = [
      ...existing,
      { role: "user", content: userMsg },
      { role: "assistant", content: assistantMsg },
    ];
    await writeAll(all);
  } finally {
    writeLock = false;
  }
}
