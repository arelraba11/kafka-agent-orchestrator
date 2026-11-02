import OpenAI from "openai";
import { LlmError, type LlmErrorKind } from "./errors";

// ─── Providers ────────────────────────────────────────────────────────────────
// One attempt per call(). A provider never retries: retries live in callLLM, in one
// layer. Every failure is thrown as an LlmError with its kind already decided.

export const DEFAULT_OLLAMA_MODEL = "llama3.2:1b";
export const OPENAI_MODEL = "gpt-4o-mini";

export type ProviderName = "ollama" | "openai";
export type ProviderMode = "auto" | ProviderName;

export interface LlmProvider {
  readonly name: ProviderName;
  /** One request, aborted after `timeoutMs`. Returns the raw model text. */
  call(prompt: string, opts: { timeoutMs: number }): Promise<string>;
}

type Env = Record<string, string | undefined>;

const MODES: readonly ProviderMode[] = ["auto", "ollama", "openai"];

/** Parses LLM_PROVIDER. Unset or blank is "auto"; anything else unknown is a config error. */
export function parseProviderMode(env: Env): ProviderMode {
  const raw = env.LLM_PROVIDER?.trim();
  if (!raw) return "auto";
  if ((MODES as readonly string[]).includes(raw)) return raw as ProviderMode;
  throw new LlmError(
    "config",
    `unknown LLM_PROVIDER ${JSON.stringify(raw)} (expected auto, ollama or openai)`,
    { provider: "config" }
  );
}

/** Maps an HTTP status to an error kind. */
export function kindForStatus(status: number): LlmErrorKind {
  if (status === 401 || status === 403) return "auth";
  if (status === 429) return "rate-limited";
  if (status >= 500) return "server";
  // 400, 404 (model not found) and any other 4xx: retrying the same request won't help.
  return "config";
}

/** Retry-After as milliseconds: delta-seconds or an HTTP date. Undefined if absent or unparseable. */
export function parseRetryAfter(value: string | null | undefined, nowMs: number): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000);
  const at = Date.parse(value);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, at - nowMs);
}

function isAbort(err: unknown): boolean {
  const name = (err as { name?: string } | null)?.name;
  return name === "TimeoutError" || name === "AbortError";
}

// ─── Ollama ───────────────────────────────────────────────────────────────────

export function createOllamaProvider(env: Env = process.env, fetchImpl: typeof fetch = fetch): LlmProvider {
  return {
    name: "ollama",
    async call(prompt, { timeoutMs }) {
      const host = env.OLLAMA_HOST ?? "http://localhost:11434";
      const model = env.OLLAMA_MODEL ?? DEFAULT_OLLAMA_MODEL;
      try {
        const res = await fetchImpl(`${host}/api/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model,
            messages: [{ role: "user", content: prompt }],
            stream: false,
          }),
          signal: AbortSignal.timeout(timeoutMs),
        });
        if (!res.ok) {
          const body = await res.text();
          throw new LlmError(kindForStatus(res.status), `Ollama HTTP ${res.status}: ${body.slice(0, 200)}`, {
            provider: "ollama",
            status: res.status,
            retryAfterMs: parseRetryAfter(res.headers.get("retry-after"), Date.now()),
          });
        }
        const data = (await res.json()) as { message?: { content?: unknown } };
        const content = data?.message?.content;
        return typeof content === "string" ? content : "";
      } catch (err) {
        if (err instanceof LlmError) throw err;
        if (isAbort(err)) {
          throw new LlmError("timeout", `Ollama request timed out after ${timeoutMs}ms`, { provider: "ollama", cause: err });
        }
        // fetch failures (ECONNREFUSED, ECONNRESET, DNS) and a body that is not JSON
        throw new LlmError("network", `Ollama request failed: ${(err as Error).message}`, {
          provider: "ollama",
          cause: err,
        });
      }
    },
  };
}

// ─── OpenAI ───────────────────────────────────────────────────────────────────

/** Constructor options the OpenAI provider passes to the client. */
export interface OpenAIClientOptions {
  apiKey: string;
  maxRetries: 0;
}

interface ChatClient {
  chat: {
    completions: {
      create(
        body: { model: string; messages: { role: "user"; content: string }[] },
        options: { timeout: number }
      ): Promise<{ choices: { message: { content: string | null } }[] }>;
    };
  };
}

/** Maps an error thrown by the OpenAI SDK onto an LlmError. */
export function classifyOpenAIError(err: unknown, nowMs: number): LlmError {
  if (err instanceof LlmError) return err;
  if (err instanceof OpenAI.APIConnectionTimeoutError || isAbort(err)) {
    return new LlmError("timeout", `OpenAI request timed out: ${(err as Error).message}`, { provider: "openai", cause: err });
  }
  if (err instanceof OpenAI.APIConnectionError) {
    return new LlmError("network", `OpenAI connection failed: ${err.message}`, { provider: "openai", cause: err });
  }
  if (err instanceof OpenAI.APIError && typeof err.status === "number") {
    return new LlmError(kindForStatus(err.status), `OpenAI HTTP ${err.status}: ${err.message}`, {
      provider: "openai",
      status: err.status,
      retryAfterMs: parseRetryAfter(err.headers?.get("retry-after"), nowMs),
      cause: err,
    });
  }
  return new LlmError("network", `OpenAI request failed: ${(err as Error)?.message ?? String(err)}`, {
    provider: "openai",
    cause: err,
  });
}

/**
 * The client is created on the first call that reaches OpenAI, never at import. With
 * no key that is a config error, so a service that never needs OpenAI starts without
 * one. maxRetries is 0: retries happen in callLLM only.
 */
export function createOpenAIProvider(
  env: Env = process.env,
  makeClient: (opts: OpenAIClientOptions) => ChatClient = (opts) => new OpenAI(opts) as unknown as ChatClient
): LlmProvider {
  let client: ChatClient | null = null;

  function getOpenAI(): ChatClient {
    if (client) return client;
    const apiKey = env.OPENAI_API_KEY?.trim();
    if (!apiKey) {
      throw new LlmError(
        "config",
        `OPENAI_API_KEY is not set (LLM_PROVIDER=${env.LLM_PROVIDER?.trim() || "auto"})`,
        { provider: "openai" }
      );
    }
    client = makeClient({ apiKey, maxRetries: 0 });
    return client;
  }

  return {
    name: "openai",
    async call(prompt, { timeoutMs }) {
      const openai = getOpenAI();
      try {
        const res = await openai.chat.completions.create(
          { model: OPENAI_MODEL, messages: [{ role: "user", content: prompt }] },
          { timeout: timeoutMs }
        );
        return res.choices[0]?.message.content ?? "";
      } catch (err) {
        throw classifyOpenAIError(err, Date.now());
      }
    },
  };
}
