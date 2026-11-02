import { LlmError } from "./errors";
import {
  createOllamaProvider,
  createOpenAIProvider,
  parseProviderMode,
  type LlmProvider,
  type ProviderName,
} from "./providers";

// ─── LLM call policy ──────────────────────────────────────────────────────────
// callLLM(prompt, { budgetMs, maxAttempts })
//   - every attempt has a timeout: min(LLM_ATTEMPT_TIMEOUT_MS, remaining budget)
//   - transient kinds (timeout, network, 429, 5xx, empty) are retried with backoff
//     500ms · 2^n + jitter, honoring Retry-After, all inside the budget
//   - auth and config errors are never retried
//   - this is the ONLY retry layer: the OpenAI client has maxRetries 0, and Kafka
//     redelivery is not used for LLM failures (LlmError is non-retryable there)
//   - auto mode: one Ollama attempt, then OpenAI with whatever budget is left

export interface CallOptions {
  /** Total wall-clock budget across every attempt and backoff sleep. */
  budgetMs: number;
  /** Attempts per provider (auto mode makes one Ollama attempt, then up to this many on OpenAI). */
  maxAttempts: number;
}

type Env = Record<string, string | undefined>;

export interface CallDeps {
  providers?: Record<ProviderName, LlmProvider>;
  env?: Env;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  /** Uniform [0, 1); used for backoff jitter. */
  random?: () => number;
  log?: (line: string) => void;
}

export const DEFAULT_ATTEMPT_TIMEOUT_MS = 20_000;
const BACKOFF_BASE_MS = 500;
const JITTER_MS = 100;

const defaultProviders: Record<ProviderName, LlmProvider> = {
  ollama: createOllamaProvider(),
  openai: createOpenAIProvider(),
};

function stripFences(text: string): string {
  return text.replace(/^```json\n?/, "").replace(/\n?```$/, "").trim();
}

function attemptTimeoutMs(env: Env): number {
  const n = Number(env.LLM_ATTEMPT_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_ATTEMPT_TIMEOUT_MS;
}

export async function callLLM(prompt: string, opts: CallOptions, deps: CallDeps = {}): Promise<string> {
  const env = deps.env ?? process.env;
  const providers = deps.providers ?? defaultProviders;
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const random = deps.random ?? Math.random;
  const log = deps.log ?? ((line: string) => console.log(line));

  const mode = parseProviderMode(env);
  const perAttempt = attemptTimeoutMs(env);
  const started = now();
  const remaining = () => opts.budgetMs - (now() - started);

  async function runProvider(provider: LlmProvider, maxAttempts: number): Promise<string> {
    for (let attempt = 1; ; attempt++) {
      const left = remaining();
      if (left <= 0) {
        throw new LlmError("timeout", `LLM budget of ${opts.budgetMs}ms exhausted before attempt ${attempt}`, {
          provider: provider.name,
        });
      }
      const t0 = now();
      try {
        const text = stripFences(await provider.call(prompt, { timeoutMs: Math.min(perAttempt, left) }));
        if (!/\S/.test(text)) {
          throw new LlmError("empty-response", `${provider.name} returned an empty response`, {
            provider: provider.name,
          });
        }
        log(`[llm] provider=${provider.name} attempt=${attempt}/${maxAttempts} outcome=ok ms=${now() - t0}`);
        return text;
      } catch (err) {
        if (!(err instanceof LlmError)) throw err;
        log(
          `[llm] provider=${provider.name} attempt=${attempt}/${maxAttempts} outcome=${err.kind} ms=${now() - t0}` +
            (err.kind === "auth" || err.kind === "config" ? ` reason=${JSON.stringify(err.message)}` : "")
        );
        if (!err.transient || attempt >= maxAttempts) throw err;

        const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1) + Math.floor(random() * JITTER_MS);
        const delay = err.retryAfterMs ?? backoff;
        // A Retry-After beyond the budget, or no room left for another attempt after
        // sleeping: fail now rather than spend the budget sleeping.
        if (delay >= remaining()) throw err;
        await sleep(delay);
      }
    }
  }

  if (mode !== "auto") return runProvider(providers[mode], opts.maxAttempts);

  // auto: Ollama primary (one attempt), OpenAI fallback with the remaining budget.
  try {
    return await runProvider(providers.ollama, 1);
  } catch (err) {
    if (!(err instanceof LlmError)) throw err;
    log(`[llm] ollama-unavailable kind=${err.kind} reason=${JSON.stringify(err.message)} fallback=openai`);
    return runProvider(providers.openai, opts.maxAttempts);
  }
}
