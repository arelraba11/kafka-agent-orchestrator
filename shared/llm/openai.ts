import OpenAI from "openai";

const OLLAMA_HOST  = process.env.OLLAMA_HOST  ?? "http://localhost:11434";
const OLLAMA_MODEL = process.env.OLLAMA_MODEL ?? "llama3.2";
// "auto"   → try Ollama first, fall back to OpenAI
// "ollama" → Ollama only (error if unavailable)
// "openai" → OpenAI only
const LLM_PROVIDER = process.env.LLM_PROVIDER ?? "auto";

export const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

function stripFences(text: string): string {
  return text.replace(/^```json\n?/, "").replace(/\n?```$/, "").trim();
}

async function callOllama(prompt: string): Promise<string> {
  const res = await fetch(`${OLLAMA_HOST}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: OLLAMA_MODEL,
      messages: [{ role: "user", content: prompt }],
      stream: false,
    }),
  });
  if (!res.ok) throw new Error(`Ollama HTTP ${res.status}: ${await res.text()}`);
  const data = (await res.json()) as { message: { content: string } };
  return stripFences(data.message.content);
}

async function callOpenAI(prompt: string): Promise<string> {
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
  });
  return stripFences(res.choices[0].message.content ?? "");
}

export async function callLLM(prompt: string): Promise<string> {
  if (LLM_PROVIDER === "openai") {
    console.log("[llm] provider=openai model=gpt-4o-mini");
    return callOpenAI(prompt);
  }

  if (LLM_PROVIDER === "ollama") {
    console.log(`[llm] provider=ollama model=${OLLAMA_MODEL}`);
    return callOllama(prompt);
  }

  // "auto": Ollama primary → OpenAI fallback
  try {
    const result = await callOllama(prompt);
    console.log(`[llm] provider=ollama model=${OLLAMA_MODEL}`);
    return result;
  } catch (err) {
    console.log(`[llm] ollama-unavailable reason="${(err as Error).message}" fallback=openai`);
    return callOpenAI(prompt);
  }
}
