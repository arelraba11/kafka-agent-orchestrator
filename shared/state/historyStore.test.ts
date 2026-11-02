import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { StoreError } from "../kafka/errors";
import { createHistoryStore } from "./historyStore";

let dir: string;
let path: string;
let logged: string[];

const quiet = {
  error: (m: string) => void logged.push(`ERROR ${m}`),
  warn: (m: string) => void logged.push(`WARN ${m}`),
};

function store(extra: Parameters<typeof createHistoryStore>[0] = {}) {
  return createHistoryStore({ path: () => path, log: quiet, ...extra });
}

const readFile = () => JSON.parse(readFileSync(path, "utf8"));
const files = () => readdirSync(dir).sort();

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "history-test-"));
  path = join(dir, "history.json");
  logged = [];
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("atomic writes", () => {
  test("append creates the file and leaves no temp file behind", async () => {
    await store().appendToHistory("s", "c1", "hi", "hello");
    expect(readFile()).toEqual({
      s: [
        { role: "user", content: "hi", conversationId: "c1" },
        { role: "assistant", content: "hello", conversationId: "c1" },
      ],
    });
    expect(files()).toEqual(["history.json"]);
  });

  test("a crash after the temp write leaves the original intact", async () => {
    const good = store();
    await good.appendToHistory("s", "c1", "q1", "a1");
    const before = readFileSync(path, "utf8");

    const crashing = store({
      beforeRename: (tmp) => {
        // The temp file is complete and sits beside the target, not in place of it.
        expect(JSON.parse(readFileSync(tmp, "utf8")).s).toHaveLength(4);
        expect(readFileSync(path, "utf8")).toBe(before);
        throw new Error("simulated crash before rename");
      },
    });
    await expect(crashing.appendToHistory("s", "c2", "q2", "a2")).rejects.toBeInstanceOf(StoreError);

    expect(readFileSync(path, "utf8")).toBe(before);
    expect(files()).toEqual(["history.json"]); // the failed write cleaned up its temp file
    expect(await good.getHistory("s")).toHaveLength(2);
  });

  test("a write failure is a retryable StoreError", async () => {
    const bad = createHistoryStore({ path: () => join(dir, "no-such-dir", "history.json"), log: quiet });
    await expect(bad.appendToHistory("s", "c1", "q", "a")).rejects.toBeInstanceOf(StoreError);
  });
});

describe("validated reads", () => {
  test("a missing file reads as empty, without noise", async () => {
    expect(await store().getHistory("s")).toEqual([]);
    expect(logged).toEqual([]);
  });

  test("legacy entries without a conversationId are accepted", async () => {
    writeFileSync(path, JSON.stringify({ s: [{ role: "user", content: "old q" }, { role: "assistant", content: "old a" }] }));
    const h = store();
    expect(await h.getHistory("s")).toEqual([
      { role: "user", content: "old q" },
      { role: "assistant", content: "old a" },
    ]);
    await h.appendToHistory("s", "c1", "new q", "new a");
    expect(readFile().s).toHaveLength(4);
    expect(files()).toEqual(["history.json"]); // nothing was quarantined
  });

  test("the reader logs ERROR and returns [] for a corrupt file, and does not touch it", async () => {
    writeFileSync(path, "{");
    expect(await store().getHistory("s")).toEqual([]);
    expect(logged.some((l) => l.startsWith("ERROR") && l.includes("corrupt"))).toBe(true);
    expect(readFileSync(path, "utf8")).toBe("{");
    expect(files()).toEqual(["history.json"]);
  });

  test("a schema mismatch counts as corrupt for the reader", async () => {
    writeFileSync(path, JSON.stringify({ s: [{ role: "robot", content: "x" }] }));
    expect(await store().getHistory("s")).toEqual([]);
    expect(logged.some((l) => l.includes("schema mismatch"))).toBe(true);
  });

  test("an unreadable path is logged and read as empty", async () => {
    // reading a directory as the file fails with EISDIR, which is not ENOENT
    const asDir = createHistoryStore({ path: () => dir, log: quiet });
    expect(await asDir.getHistory("s")).toEqual([]);
    expect(logged.some((l) => l.startsWith("ERROR"))).toBe(true);
  });
});

describe("writer quarantine", () => {
  test("the writer moves a corrupt file aside, logs ERROR and starts fresh", async () => {
    writeFileSync(path, "{ not json");
    await store().appendToHistory("s", "c1", "q", "a");

    const quarantined = files().filter((f) => f.startsWith("history.json.corrupt-"));
    expect(quarantined).toHaveLength(1);
    expect(readFileSync(join(dir, quarantined[0]), "utf8")).toBe("{ not json"); // preserved on disk
    expect(readFile().s).toHaveLength(2); // the new file is valid
    expect(logged.some((l) => l.startsWith("ERROR") && l.includes(quarantined[0]))).toBe(true);
  });

  test("a schema-invalid file is quarantined too", async () => {
    writeFileSync(path, JSON.stringify([1, 2, 3]));
    await store().appendToHistory("s", "c1", "q", "a");
    expect(files().some((f) => f.startsWith("history.json.corrupt-"))).toBe(true);
    expect(Object.keys(readFile())).toEqual(["s"]);
  });
});

describe("idempotent append", () => {
  test("the same conversationId appends once", async () => {
    const h = store();
    await h.appendToHistory("s", "c1", "q", "a");
    await h.appendToHistory("s", "c1", "q", "a");
    await h.appendToHistory("s", "c1", "q", "different answer");
    expect(readFile().s).toHaveLength(2);
    expect(readFile().s[1].content).toBe("a");
  });

  test("different conversations in a session append in order", async () => {
    const h = store();
    await h.appendToHistory("s", "c1", "q1", "a1");
    await h.appendToHistory("s", "c2", "q2", "a2");
    expect((await h.getHistory("s")).map((m) => m.content)).toEqual(["q1", "a1", "q2", "a2"]);
  });

  test("the same conversationId in another session is not a duplicate", async () => {
    const h = store();
    await h.appendToHistory("s1", "c1", "q", "a");
    await h.appendToHistory("s2", "c1", "q", "a");
    expect(readFile().s2).toHaveLength(2);
  });
});

describe("serialized appends", () => {
  test("concurrent appends lose nothing", async () => {
    const h = store();
    await Promise.all(Array.from({ length: 25 }, (_, i) => h.appendToHistory("s", `c${i}`, `q${i}`, `a${i}`)));
    expect(readFile().s).toHaveLength(50);
    expect(files()).toEqual(["history.json"]);
  });

  test("concurrent duplicates of one conversation append once", async () => {
    const h = store();
    await Promise.all([1, 2, 3, 4].map(() => h.appendToHistory("s", "c1", "q", "a")));
    expect(readFile().s).toHaveLength(2);
  });

  test("a failed append does not poison the queue", async () => {
    let fail = true;
    const h = store({
      beforeRename: () => {
        if (fail) throw new Error("boom");
      },
    });
    const first = h.appendToHistory("s", "c1", "q", "a");
    const second = h.appendToHistory("s", "c2", "q", "a");
    await expect(first).rejects.toBeInstanceOf(StoreError);
    fail = false;
    await second;
    expect(existsSync(path)).toBe(true);
    expect(readFile().s.map((m: { conversationId: string }) => m.conversationId)).toEqual(["c2", "c2"]);
  });
});
