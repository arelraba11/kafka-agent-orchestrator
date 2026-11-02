import { describe, expect, test } from "bun:test";
import OpenAI from "openai";
import { NonRetryableError } from "../kafka/errors";
import { callLLM, type CallDeps } from "./callLLM";
import { LlmError, type LlmErrorKind } from "./errors";
import {
  classifyOpenAIError,
  createOpenAIProvider,
  kindForStatus,
  parseRetryAfter,
  type LlmProvider,
  type OpenAIClientOptions,
  type ProviderName,
} from "./providers";

// Fake clock: sleep() advances it, and a scripted provider step can advance it too.
type Step = string | LlmError | { text: string; ms: number } | { error: LlmError; ms: number };

function harness(script: Partial<Record<ProviderName, Step[]>>, env: Record<string, string> = {}) {
  let clock = 1_000;
  const sleeps: number[] = [];
  const calls: { provider: ProviderName; timeoutMs: number }[] = [];
  const lines: string[] = [];

  function fake(name: ProviderName): LlmProvider {
    const steps = [...(script[name] ?? [])];
    return {
      name,
      async call(_prompt, { timeoutMs }) {
        calls.push({ provider: name, timeoutMs });
        const step = steps.shift();
        if (step === undefined) throw new Error(`unscripted ${name} call`);
        const advance = typeof step === "object" && "ms" in step ? step.ms : 0;
        clock += advance;
        if (typeof step === "string") return step;
        if (step instanceof LlmError) throw step;
        if ("error" in step) throw step.error;
        return step.text;
      },
    };
  }

  const deps: CallDeps = {
    providers: { ollama: fake("ollama"), openai: fake("openai") },
    env,
    now: () => clock,
    sleep: async (ms) => {
      sleeps.push(ms);
      clock += ms;
    },
    random: () => 0,
    log: (l) => lines.push(l),
  };
  return { deps, sleeps, calls, lines };
}

const err = (kind: LlmErrorKind, extra: { retryAfterMs?: number; provider?: string } = {}) =>
  new LlmError(kind, `${kind} error`, { provider: extra.provider ?? "openai", retryAfterMs: extra.retryAfterMs });

const OPTS = { budgetMs: 60_000, maxAttempts: 3 };
const OPENAI = { LLM_PROVIDER: "openai" };

async function failure(p: Promise<unknown>): Promise<LlmError> {
  try {
    await p;
  } catch (e) {
    return e as LlmError;
  }
  throw new Error("expected callLLM to reject");
}

describe("callLLM retries", () => {
  test("a timeout is retried, then succeeds", async () => {
    const h = harness({ openai: [err("timeout"), "hello"] }, OPENAI);
    expect(await callLLM("p", OPTS, h.deps)).toBe("hello");
    expect(h.calls).toHaveLength(2);
    expect(h.sleeps).toEqual([500]); // 500 · 2^0 (+ zero jitter)
    expect(h.lines.some((l) => l.includes("attempt=1/3 outcome=timeout"))).toBe(true);
    expect(h.lines.some((l) => l.includes("attempt=2/3 outcome=ok"))).toBe(true);
  });

  test("backoff doubles per retry", async () => {
    const h = harness({ openai: [err("server"), err("server"), "ok"] }, OPENAI);
    await callLLM("p", OPTS, h.deps);
    expect(h.sleeps).toEqual([500, 1000]);
  });

  test("429 honors Retry-After", async () => {
    const h = harness({ openai: [err("rate-limited", { retryAfterMs: 2000 }), "ok"] }, OPENAI);
    expect(await callLLM("p", OPTS, h.deps)).toBe("ok");
    expect(h.sleeps).toEqual([2000]);
  });

  test("a Retry-After beyond the remaining budget fails immediately, without sleeping", async () => {
    const h = harness({ openai: [err("rate-limited", { retryAfterMs: 90_000 }), "never"] }, OPENAI);
    const e = await failure(callLLM("p", OPTS, h.deps));
    expect(e.kind).toBe("rate-limited");
    expect(h.sleeps).toEqual([]);
    expect(h.calls).toHaveLength(1);
  });

  test("5xx is retried until the attempts run out", async () => {
    const h = harness({ openai: [err("server"), err("server"), err("server")] }, OPENAI);
    const e = await failure(callLLM("p", OPTS, h.deps));
    expect(e.kind).toBe("server");
    expect(h.calls).toHaveLength(3);
  });

  test("401 is not retried and surfaces as auth", async () => {
    const h = harness({ openai: [err("auth"), "never"] }, OPENAI);
    const e = await failure(callLLM("p", OPTS, h.deps));
    expect(e.kind).toBe("auth");
    expect(h.calls).toHaveLength(1);
    expect(h.sleeps).toEqual([]);
  });

  test("a config error is not retried", async () => {
    const h = harness({ openai: [err("config")] }, OPENAI);
    expect((await failure(callLLM("p", OPTS, h.deps))).kind).toBe("config");
    expect(h.calls).toHaveLength(1);
  });

  test("blank content is retried, then gives empty-response", async () => {
    const h = harness({ openai: ["  \n", "```json\n```", "   "] }, OPENAI);
    const e = await failure(callLLM("p", OPTS, h.deps));
    expect(e.kind).toBe("empty-response");
    expect(e.transient).toBe(true);
    expect(h.calls).toHaveLength(3);
  });

  test("blank content then a real answer succeeds", async () => {
    const h = harness({ openai: ["", "answer"] }, OPENAI);
    expect(await callLLM("p", OPTS, h.deps)).toBe("answer");
  });

  test("markdown fences are stripped", async () => {
    const h = harness({ openai: ['```json\n{"a":1}\n```'] }, OPENAI);
    expect(await callLLM("p", OPTS, h.deps)).toBe('{"a":1}');
  });

  test("maxAttempts 1 makes exactly one attempt", async () => {
    const h = harness({ openai: [err("timeout"), "never"] }, OPENAI);
    const e = await failure(callLLM("p", { budgetMs: 15_000, maxAttempts: 1 }, h.deps));
    expect(e.kind).toBe("timeout");
    expect(h.calls).toHaveLength(1);
  });
});

describe("callLLM timeouts and budget", () => {
  test("the per-attempt timeout is LLM_ATTEMPT_TIMEOUT_MS", async () => {
    const h = harness({ openai: ["ok"] }, { ...OPENAI, LLM_ATTEMPT_TIMEOUT_MS: "3000" });
    await callLLM("p", OPTS, h.deps);
    expect(h.calls[0].timeoutMs).toBe(3000);
  });

  test("the default per-attempt timeout is 20 s", async () => {
    const h = harness({ openai: ["ok"] }, OPENAI);
    await callLLM("p", OPTS, h.deps);
    expect(h.calls[0].timeoutMs).toBe(20_000);
  });

  test("per-attempt timeouts are capped by the remaining budget", async () => {
    // budget 25 s; attempt 1 burns 20 s then times out; 500 ms backoff; 4.5 s remain
    const h = harness({ openai: [{ error: err("timeout"), ms: 20_000 }, "ok"] }, OPENAI);
    await callLLM("p", { budgetMs: 25_000, maxAttempts: 3 }, h.deps);
    expect(h.calls.map((c) => c.timeoutMs)).toEqual([20_000, 4_500]);
  });

  test("no further attempt starts when the backoff would exhaust the budget", async () => {
    const h = harness({ openai: [{ error: err("timeout"), ms: 14_800 }, "never"] }, OPENAI);
    const e = await failure(callLLM("p", { budgetMs: 15_000, maxAttempts: 3 }, h.deps));
    expect(e.kind).toBe("timeout");
    expect(h.calls).toHaveLength(1);
    expect(h.sleeps).toEqual([]);
  });
});

describe("callLLM provider selection", () => {
  test("an invalid LLM_PROVIDER gives a config error before any call", async () => {
    const h = harness({ openai: ["never"] }, { LLM_PROVIDER: "gemini" });
    const e = await failure(callLLM("p", OPTS, h.deps));
    expect(e.kind).toBe("config");
    expect(e.message).toContain("gemini");
    expect(h.calls).toHaveLength(0);
  });

  test("unset LLM_PROVIDER means auto", async () => {
    const h = harness({ ollama: ["from ollama"] });
    expect(await callLLM("p", OPTS, h.deps)).toBe("from ollama");
  });

  test("auto: connection refused falls back to OpenAI immediately", async () => {
    const h = harness({ ollama: [err("network", { provider: "ollama" })], openai: ["from openai"] });
    expect(await callLLM("p", OPTS, h.deps)).toBe("from openai");
    expect(h.calls.map((c) => c.provider)).toEqual(["ollama", "openai"]);
    expect(h.sleeps).toEqual([]);
  });

  test("auto: Ollama makes one attempt even when maxAttempts is 3", async () => {
    const h = harness({ ollama: [err("timeout", { provider: "ollama" }), "never"], openai: ["ok"] });
    await callLLM("p", OPTS, h.deps);
    expect(h.calls.filter((c) => c.provider === "ollama")).toHaveLength(1);
  });

  test("auto: after an Ollama timeout OpenAI gets only the remaining budget", async () => {
    const h = harness({
      ollama: [{ error: err("timeout", { provider: "ollama" }), ms: 20_000 }],
      openai: ["ok"],
    });
    await callLLM("p", { budgetMs: 25_000, maxAttempts: 3 }, h.deps);
    expect(h.calls).toEqual([
      { provider: "ollama", timeoutMs: 20_000 },
      { provider: "openai", timeoutMs: 5_000 },
    ]);
  });

  test("auto: OpenAI is retried after the fallback", async () => {
    const h = harness({
      ollama: [err("network", { provider: "ollama" })],
      openai: [err("server"), "ok"],
    });
    expect(await callLLM("p", OPTS, h.deps)).toBe("ok");
    expect(h.calls.map((c) => c.provider)).toEqual(["ollama", "openai", "openai"]);
  });

  test("auto: when both providers fail, OpenAI's error is thrown", async () => {
    const h = harness({
      ollama: [err("network", { provider: "ollama" })],
      openai: [err("auth")],
    });
    expect((await failure(callLLM("p", OPTS, h.deps))).kind).toBe("auth");
  });

  test("ollama mode never touches OpenAI", async () => {
    const h = harness({ ollama: [err("network", { provider: "ollama" }), err("network", { provider: "ollama" }), err("network", { provider: "ollama" })] }, {
      LLM_PROVIDER: "ollama",
    });
    expect((await failure(callLLM("p", OPTS, h.deps))).kind).toBe("network");
    expect(h.calls.every((c) => c.provider === "ollama")).toBe(true);
  });
});

describe("OpenAI provider", () => {
  test("a missing key gives a config error without constructing the client", async () => {
    let constructed = 0;
    const provider = createOpenAIProvider({ LLM_PROVIDER: "openai" }, () => {
      constructed++;
      throw new Error("must not be constructed");
    });
    const h = harness({});
    h.deps.providers = { ...h.deps.providers!, openai: provider };
    h.deps.env = { LLM_PROVIDER: "openai" };
    const e = await failure(callLLM("p", OPTS, h.deps));
    expect(e.kind).toBe("config");
    expect(e.message).toContain("OPENAI_API_KEY is not set");
    expect(e.message).toContain("LLM_PROVIDER=openai");
    expect(constructed).toBe(0);
  });

  test("the client is built with maxRetries: 0, lazily and once", async () => {
    const seen: OpenAIClientOptions[] = [];
    const requests: number[] = [];
    const provider = createOpenAIProvider({ OPENAI_API_KEY: "sk-test" }, (opts) => {
      seen.push(opts);
      return {
        chat: {
          completions: {
            create: async (_body, options) => {
              requests.push(options.timeout);
              return { choices: [{ message: { content: "hi" } }] };
            },
          },
        },
      };
    });
    expect(seen).toHaveLength(0); // nothing at construction
    expect(await provider.call("p", { timeoutMs: 1234 })).toBe("hi");
    expect(await provider.call("p", { timeoutMs: 99 })).toBe("hi");
    expect(seen).toEqual([{ apiKey: "sk-test", maxRetries: 0 }]);
    expect(requests).toEqual([1234, 99]); // each request carries its own timeout
  });

  test("null content comes back as an empty string (callLLM turns it into empty-response)", async () => {
    const provider = createOpenAIProvider({ OPENAI_API_KEY: "sk-test" }, () => ({
      chat: { completions: { create: async () => ({ choices: [{ message: { content: null } }] }) } },
    }));
    expect(await provider.call("p", { timeoutMs: 1 })).toBe("");
  });

  test("SDK errors map onto kinds", () => {
    const mk = (status: number, headers?: Headers) => OpenAI.APIError.generate(status, {}, "boom", headers ?? new Headers());
    expect(classifyOpenAIError(mk(401), 0).kind).toBe("auth");
    expect(classifyOpenAIError(mk(403), 0).kind).toBe("auth");
    expect(classifyOpenAIError(mk(404), 0).kind).toBe("config");
    expect(classifyOpenAIError(mk(400), 0).kind).toBe("config");
    expect(classifyOpenAIError(mk(500), 0).kind).toBe("server");
    expect(classifyOpenAIError(mk(503), 0).kind).toBe("server");
    const limited = classifyOpenAIError(mk(429, new Headers({ "retry-after": "3" })), 0);
    expect(limited.kind).toBe("rate-limited");
    expect(limited.retryAfterMs).toBe(3000);
    expect(limited.status).toBe(429);
    expect(classifyOpenAIError(new OpenAI.APIConnectionTimeoutError(), 0).kind).toBe("timeout");
    expect(classifyOpenAIError(new OpenAI.APIConnectionError({ message: "x" }), 0).kind).toBe("network");
  });

  test("status and Retry-After helpers", () => {
    expect(kindForStatus(502)).toBe("server");
    expect(kindForStatus(422)).toBe("config");
    expect(parseRetryAfter("2", 0)).toBe(2000);
    expect(parseRetryAfter("Thu, 01 Jan 1970 00:00:10 GMT", 4000)).toBe(6000);
    expect(parseRetryAfter("soon", 0)).toBeUndefined();
    expect(parseRetryAfter(null, 0)).toBeUndefined();
  });
});

describe("LlmError", () => {
  test("is a NonRetryableError, with transient derived from the kind", () => {
    const e = new LlmError("timeout", "t", { provider: "x" });
    expect(e).toBeInstanceOf(NonRetryableError);
    expect(e.transient).toBe(true);
    expect(new LlmError("auth", "a", { provider: "x" }).transient).toBe(false);
    expect(new LlmError("config", "c", { provider: "x" }).transient).toBe(false);
  });
});
