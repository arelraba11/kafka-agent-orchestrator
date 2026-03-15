import OpenAI from "openai";

export const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export async function callLLM(prompt: string): Promise<string> {
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
  });
  const content = res.choices[0].message.content ?? "";
  // Strip markdown code fences that some LLM responses include
  return content.replace(/^```json\n?/, "").replace(/\n?```$/, "").trim();
}
