import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync, readdirSync, statSync } from "fs";
import { join, relative } from "path";
import { TOPICS } from "../../shared/topics";
import { COMPOSE_FILE, TEST_PROJECT, compose, run, testKafka } from "./harness/broker";
import { FAKE_API_KEY, startFakeLlm, type FakeLlm } from "./harness/fakeLlm";
import { newCid, short, tag } from "./harness/ids";
import { createNamespace, type Namespace } from "./harness/namespace";
import { startObserver, type Observer } from "./harness/observer";
import { settle } from "./harness/settle";

// Self-test of the harness itself: if these fail, no other result can be trusted.

describe("fake LLM server", () => {
  let fake: FakeLlm;
  beforeAll(() => {
    fake = startFakeLlm();
  });
  afterAll(() => fake.stop());

  const post = (stage: string, prompt: string, key: string | null = FAKE_API_KEY) =>
    fetch(`${fake.baseUrl}/${stage}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json", ...(key ? { authorization: `Bearer ${key}` } : {}) },
      body: JSON.stringify({ model: "m", messages: [{ role: "user", content: prompt }] }),
    });

  test("a scripted response is served in order, keyed by stage and the prompt's token", async () => {
    const t = short();
    fake.script("synth", t, [{ ok: "first" }, { status: 429, retryAfter: 1 }, { ok: "third" }]);

    const r1 = await post("synth", `question ${tag(t)}`);
    expect(r1.status).toBe(200);
    expect((await r1.json()).choices[0].message.content).toBe("first");

    const r2 = await post("synth", `question ${tag(t)}`);
    expect(r2.status).toBe(429);
    expect(r2.headers.get("retry-after")).toBe("1");

    const r3 = await post("synth", `question ${tag(t)}`);
    expect((await r3.json()).choices[0].message.content).toBe("third");

    expect(fake.calls("synth", t).map((c) => c.kind)).toEqual(["ok", "status-429", "ok"]);
    expect(fake.calls("router", t)).toEqual([]);
  });

  test("an unscripted request gets 500 and is recorded as unmatched", async () => {
    const before = fake.unmatched().length;
    const t = short();
    const res = await post("router", `no script for ${tag(t)}`);
    expect(res.status).toBe(500);
    expect(fake.unmatched()).toHaveLength(before + 1);
    expect(fake.unmatched().at(-1)!.token).toBe(t);
  });

  test("a request without the fake bearer token gets 400 and is recorded as bad-auth", async () => {
    const t = short();
    fake.script("router", t, [{ ok: "{}" }]);
    const before = fake.badAuth().length;

    expect((await post("router", `x ${tag(t)}`, null)).status).toBe(400);
    expect((await post("router", `x ${tag(t)}`, "sk-some-real-looking-key")).status).toBe(400);

    expect(fake.badAuth()).toHaveLength(before + 2);
    expect(fake.calls("router", t).every((c) => !c.authOk)).toBe(true);
    // a rejected request does not consume the script
    expect((await post("router", `x ${tag(t)}`)).status).toBe(200);
  });
});

describe("settle()", () => {
  let ns: Namespace;
  let observer: Observer;
  beforeAll(async () => {
    ns = await createNamespace("selftest");
    observer = await startObserver(ns);
  }, 60_000);
  afterAll(async () => {
    await observer?.stop();
    await ns?.close();
  });

  test("on an empty namespace it settles immediately", async () => {
    const report = await settle(ns, observer, [], { timeoutMs: 2_000 });
    expect(report.lagByGroup).toEqual({});
  });

  test("it fails, naming the group and its lag, while a live group has not consumed", async () => {
    const producer = testKafka("selftest-producer").producer();
    await producer.connect();
    const cid = newCid();
    await producer.send({ topic: ns.physical(TOPICS.USER_COMMANDS), messages: [{ key: cid, value: "{}" }] });
    await producer.disconnect();

    const err = await settle(ns, observer, [{ group: "ghost-group", topic: TOPICS.USER_COMMANDS }], { timeoutMs: 500 }).catch((e) => e);
    expect(String(err)).toMatch(/timed out/);
    expect(String(err)).toMatch(/ghost-group lag=1/);
  });
});

describe("docker isolation", () => {
  const DEV_PROJECT = "kafka-agent-orchestrator";

  test("every compose command names the test file AND the test project explicitly", () => {
    const args = compose("ps");
    expect(args[args.indexOf("-f") + 1]).toBe(COMPOSE_FILE);
    expect(args[args.indexOf("-p") + 1]).toBe(TEST_PROJECT);
    expect(TEST_PROJECT).not.toBe(DEV_PROJECT);
  });

  test("an exported COMPOSE_PROJECT_NAME of the dev project cannot redirect them to the dev container", async () => {
    const containerOf = (project: string) =>
      run([
        "docker", "ps", "-q",
        "--filter", `label=com.docker.compose.project=${project}`,
        "--filter", "label=com.docker.compose.service=kafka",
      ]).then((r) => r.out.trim());
    const testBroker = await containerOf(TEST_PROJECT);
    expect(testBroker, "the test broker is up (preload)").not.toBe("");

    const saved = process.env.COMPOSE_PROJECT_NAME;
    process.env.COMPOSE_PROJECT_NAME = DEV_PROJECT; // what a dev shell might export
    try {
      const seen = (await run(compose("ps", "-q", "kafka"))).out.trim();
      // read-only ps -q: must be the TEST broker (docker ps prints the short id, compose the full one)
      expect(seen.startsWith(testBroker), `compose resolved to ${seen}, the test broker is ${testBroker}`).toBe(true);
    } finally {
      if (saved === undefined) delete process.env.COMPOSE_PROJECT_NAME;
      else process.env.COMPOSE_PROJECT_NAME = saved;
    }
  });
});

describe("no fixed sleeps", () => {
  // The patterns are built from pieces so this file does not match itself.
  const forbidden = [new RegExp("Bun" + "\\.sleep"), new RegExp("set" + "Timeout\\(")];
  const root = import.meta.dir;
  const allowed = join(root, "harness", "wait.ts");

  function sources(dir: string): string[] {
    return readdirSync(dir).flatMap((entry) => {
      if (entry === ".logs" || entry === "node_modules") return [];
      const p = join(dir, entry);
      return statSync(p).isDirectory() ? sources(p) : p.endsWith(".ts") ? [p] : [];
    });
  }

  test("only harness/wait.ts waits on time", () => {
    const offenders: string[] = [];
    for (const file of sources(root)) {
      if (file === allowed) continue;
      const text = readFileSync(file, "utf8");
      for (const re of forbidden) if (re.test(text)) offenders.push(`${relative(root, file)}: ${re.source}`);
    }
    expect(offenders).toEqual([]);
  });
});
