import { afterAll, afterEach, beforeAll, expect } from "bun:test";
import { createNamespace, type Namespace } from "./namespace";
import { startObserver, type Observer } from "./observer";
import { startFakeLlm, type FakeLlm } from "./fakeLlm";
import { createServices, type ServiceName, type Services } from "./services";
import { settle } from "./settle";
import { readFileSync } from "fs";
import { HistoryFileSchema, type HistoryMessage } from "../../../shared/state/historyStore";
import { ask } from "./wsClient";

export { newCid, short, tag, token } from "./ids";
export { waitFor } from "./wait";
export type { ServiceName } from "./services";

export interface Harness {
  ns: Namespace;
  observer: Observer;
  fake: FakeLlm;
  services: Services;
  /** Sends a query over the web server's WebSocket. */
  ask(input: string, timeoutMs?: number): ReturnType<typeof ask>;
  /** The messages in history.json that belong to `cid` (parsed with HistoryFileSchema); [] if the file does not exist. */
  history(cid: string): HistoryMessage[];
  /** Waits for quiescence: every live group at lag 0 and the observer caught up. */
  settle(): ReturnType<typeof settle>;
}

export interface HarnessOptions {
  /** Services started before the first test (a test may start others later). */
  start: ServiceName[];
  /** Env for every service of this file (for example STEP_TIMEOUT_MS). */
  env?: Record<string, string>;
  /** Poison records are expected in this file: do not fail on invalid records. */
  allowInvalid?: boolean;
}

/**
 * Per-file setup: a namespace of topics, an observer, a fake LLM and the requested
 * services. Registers beforeAll / afterEach / afterAll. The afterEach settles, checks the
 * fake LLM saw no unscripted or badly authenticated call, and restarts killed services.
 */
export function useHarness(name: string, options: HarnessOptions): Harness {
  const h = {} as Harness;

  beforeAll(async () => {
    h.ns = await createNamespace(name);
    h.fake = startFakeLlm();
    h.observer = await startObserver(h.ns);
    h.services = await createServices(h.ns, h.fake, { env: options.env });
    // start in parallel: group joins are independent
    await Promise.all(options.start.map((s) => h.services.start(s)));
    h.ask = (input, timeoutMs) => ask(h.services.webPort, input, timeoutMs);
    h.history = (cid) => {
      let text: string;
      try {
        text = readFileSync(h.ns.paths.history, "utf8");
      } catch {
        return [];
      }
      const all = HistoryFileSchema.parse(JSON.parse(text));
      return Object.values(all).flat().filter((m) => m.conversationId === cid);
    };
    h.settle = () => settle(h.ns, h.observer, h.services.liveGroups());
  }, 90_000);

  afterEach(async () => {
    await h.services.restartDead();
    await h.settle();
    const unmatched = h.fake.unmatched();
    const badAuth = h.fake.badAuth();
    h.fake.clearViolations();
    expect(unmatched, "fake LLM saw unscripted calls").toEqual([]);
    expect(badAuth, "fake LLM saw calls without the fake key").toEqual([]);
    if (!options.allowInvalid) expect(h.observer.invalid(), "invalid records on contract topics").toEqual([]);
  }, 60_000);

  afterAll(async () => {
    await h.services?.stopAll();
    await h.observer?.stop();
    await h.fake?.stop();
    await h.ns?.close();
  }, 60_000);

  return h;
}
