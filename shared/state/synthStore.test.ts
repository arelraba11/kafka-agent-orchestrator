import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { StoreError } from "../kafka/errors";
import { closeSynthStore, defaultSynthStorePath, initializeSynthStore, synthStore } from "./synthStore";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";

let dir: string;

beforeEach(async () => {
  dir = mkdtempSync(join(tmpdir(), "synthstore-test-"));
  await initializeSynthStore(join(dir, "db"));
});

afterEach(async () => {
  await closeSynthStore();
  rmSync(dir, { recursive: true, force: true });
});

describe("synthStore: a missing key is null", () => {
  // level 10 returns undefined for a missing key. The synthesizer's `!== null` checks
  // depend on this: a raw undefined would read every message as "already done", and a
  // StoreError would redeliver every first message forever.
  test.each([["q"], ["o"], ["done"]] as const)("%s: resolves null, neither rejecting nor throwing StoreError", async (kind) => {
    const key = `${kind}:${CID}` as const;
    const value = await synthStore.get(key);
    expect(value).toBeNull();
    expect(value).not.toBeUndefined();
  });
});

describe("synthStore: round trips", () => {
  test("q:", async () => {
    await synthStore.put(`q:${CID}`, { userInput: "what is 2+2", sessionId: "s" });
    expect(await synthStore.get(`q:${CID}`)).toEqual({ userInput: "what is 2+2", sessionId: "s" });
  });

  test("o: answer and failed", async () => {
    await synthStore.put(`o:${CID}`, { kind: "answer", answer: "4" });
    expect(await synthStore.get(`o:${CID}`)).toEqual({ kind: "answer", answer: "4" });
    await synthStore.put(`o:${CID}`, { kind: "failed", code: "llm-unavailable", reason: "timeout" });
    expect(await synthStore.get(`o:${CID}`)).toEqual({ kind: "failed", code: "llm-unavailable", reason: "timeout" });
  });

  test("done:", async () => {
    await synthStore.put(`done:${CID}`, { at: 123 });
    expect(await synthStore.get(`done:${CID}`)).toEqual({ at: 123 });
  });
});

describe("synthStore: batch", () => {
  const cleanup = [
    { type: "put", key: `done:${CID}`, value: { at: 1 } },
    { type: "del", key: `q:${CID}` },
    { type: "del", key: `o:${CID}` },
  ] as const;

  test("[put done, del q, del o] succeeds when q: and o: are already absent", async () => {
    await synthStore.batch([...cleanup]);
    expect(await synthStore.get(`done:${CID}`)).toEqual({ at: 1 });
    expect(await synthStore.get(`q:${CID}`)).toBeNull();
    expect(await synthStore.get(`o:${CID}`)).toBeNull();
  });

  test("deletes q: and o: when they are present, in the same step as done:", async () => {
    await synthStore.put(`q:${CID}`, { userInput: "x", sessionId: "s" });
    await synthStore.put(`o:${CID}`, { kind: "answer", answer: "y" });
    await synthStore.batch([...cleanup]);
    expect(await synthStore.get(`done:${CID}`)).toEqual({ at: 1 });
    expect(await synthStore.get(`q:${CID}`)).toBeNull();
    expect(await synthStore.get(`o:${CID}`)).toBeNull();
  });

  test("is repeatable (a redelivery after a completed cleanup)", async () => {
    await synthStore.batch([...cleanup]);
    await synthStore.batch([...cleanup]);
    expect(await synthStore.get(`done:${CID}`)).toEqual({ at: 1 });
  });
});

describe("synthStore: I/O failures", () => {
  test("after the store is closed, get, put and batch reject with StoreError", async () => {
    await synthStore.put(`done:${CID}`, { at: 1 });
    await closeSynthStore();
    await expect(synthStore.get(`done:${CID}`)).rejects.toBeInstanceOf(StoreError);
    await expect(synthStore.put(`done:${CID}`, { at: 2 })).rejects.toBeInstanceOf(StoreError);
    await expect(synthStore.batch([{ type: "del", key: `q:${CID}` }])).rejects.toBeInstanceOf(StoreError);
  });
});

describe("defaultSynthStorePath", () => {
  test("is <cwd>/.synth-store unless SYNTH_STORE_PATH is set", () => {
    expect(defaultSynthStorePath({})).toBe(join(process.cwd(), ".synth-store"));
    expect(defaultSynthStorePath({ SYNTH_STORE_PATH: "/x/y" })).toBe("/x/y");
  });
});
