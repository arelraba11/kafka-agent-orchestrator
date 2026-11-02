// A fake OpenAI chat-completions server, so the suite never reaches a real LLM.
// Router and synthesizer each get their own base URL (…/router/v1, …/synth/v1) through
// OPENAI_BASE_URL, so the server knows the caller without inspecting prompts. Scripts are
// keyed by (stage, token): the token is the `[t:xxxxxxxx]` the test put in the user input.

export const FAKE_API_KEY = "test-key-not-real";

export type Stage = "router" | "synth";

export type FakeResponse =
  | { ok: string }
  | { empty: true }
  | { status: 429; retryAfter: number }
  | { status: 500 | 503 | 401 }
  | { hang: true };

export interface LlmCall {
  stage: Stage;
  /** The token found in the prompt, or null when there was none. */
  token: string | null;
  at: number;
  kind: string;
  authOk: boolean;
  /** The first 2000 chars of the prompt, for assertions about its content. */
  prompt: string;
}

export interface PlanStepScript {
  tool: string;
  args: Record<string, unknown>;
}

export interface FakeLlm {
  /** http://127.0.0.1:<port>; services get `${baseUrl}/router/v1` or `${baseUrl}/synth/v1`. */
  baseUrl: string;
  script(stage: Stage, token: string, responses: FakeResponse[]): void;
  /** Router script: one response whose JSON is `{steps}`. */
  plan(token: string, steps: PlanStepScript[]): void;
  /** Synthesizer script: one `{ok: text}` response. */
  answer(token: string, text: string): void;
  calls(stage: Stage, token: string): LlmCall[];
  /** Requests that had no scripted response left (answered 500). */
  unmatched(): LlmCall[];
  /** Requests without the fake bearer token (answered 400). */
  badAuth(): LlmCall[];
  /** Forget the recorded unmatched and bad-auth calls, so one failing test does not fail the next. */
  clearViolations(): void;
  stop(): Promise<void>;
}

const TOKEN_RE = /\[t:([a-z0-9]{8})\]/;

function completion(content: string): Response {
  return Response.json({
    id: "chatcmpl-fake",
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: "gpt-4o-mini",
    choices: [{ index: 0, message: { role: "assistant", content }, finish_reason: "stop" }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
}

function errorResponse(status: number, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify({ error: { message: `fake LLM error ${status}`, type: "fake_error", code: null } }), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function startFakeLlm(): FakeLlm {
  const scripts = new Map<string, FakeResponse[]>();
  const log: LlmCall[] = [];
  // Requests answered with `hang`: released when the client gives up, and on stop() (a handler
  // that never returns would keep server.stop() waiting).
  const hanging = new Set<() => void>();
  const key = (stage: Stage, token: string) => `${stage}:${token}`;

  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      const m = /^\/(router|synth)\/v1\/chat\/completions$/.exec(url.pathname);
      if (req.method !== "POST" || !m) return new Response("not found", { status: 404 });
      const stage = m[1] as Stage;

      const authOk = req.headers.get("authorization") === `Bearer ${FAKE_API_KEY}`;
      const body = (await req.json().catch(() => ({}))) as { messages?: { content?: unknown }[] };
      const prompt = (body.messages ?? []).map((x) => String(x.content ?? "")).join("\n");
      const token = TOKEN_RE.exec(prompt)?.[1] ?? null;
      const record = (kind: string) => log.push({ stage, token, at: Date.now(), kind, authOk, prompt: prompt.slice(0, 2000) });

      if (!authOk) {
        record("bad-auth");
        return errorResponse(400);
      }
      const queue = token === null ? undefined : scripts.get(key(stage, token));
      const next = queue?.shift();
      if (next === undefined) {
        record("unmatched");
        return errorResponse(500);
      }
      if ("ok" in next) {
        record("ok");
        return completion(next.ok);
      }
      if ("empty" in next) {
        record("empty");
        return completion("");
      }
      if ("hang" in next) {
        record("hang");
        // never answered: the client's per-attempt timeout aborts it
        return new Promise<Response>((resolve) => {
          const release = () => {
            hanging.delete(release);
            resolve(new Response(null, { status: 499 }));
          };
          hanging.add(release);
          req.signal.addEventListener("abort", release);
        });
      }
      record(`status-${next.status}`);
      return "retryAfter" in next
        ? errorResponse(next.status, { "retry-after": String(next.retryAfter) })
        : errorResponse(next.status);
    },
  });

  const fake: FakeLlm = {
    baseUrl: `http://127.0.0.1:${server.port}`,
    script(stage, token, responses) {
      scripts.set(key(stage, token), [...responses]);
    },
    plan(token, steps) {
      fake.script("router", token, [{ ok: JSON.stringify({ steps }) }]);
    },
    answer(token, text) {
      fake.script("synth", token, [{ ok: text }]);
    },
    calls: (stage, token) => log.filter((c) => c.stage === stage && c.token === token),
    unmatched: () => log.filter((c) => c.kind === "unmatched"),
    badAuth: () => log.filter((c) => !c.authOk),
    clearViolations() {
      for (let i = log.length - 1; i >= 0; i--) if (log[i].kind === "unmatched" || !log[i].authOk) log.splice(i, 1);
    },
    stop: async () => {
      for (const release of [...hanging]) release();
      await server.stop(true);
    },
  };
  return fake;
}
