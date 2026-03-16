// ─── Final Answer Synthesis Prompt ───────────────────────────────────────────
// Used by: synthesisWorker (final project)
// Technique: Structured summarisation
// Purpose:   Combine all tool results into a single coherent reply for the user.

export function synthesisPrompt(
  userQuery: string,
  toolResults: Array<{ tool: string; result: string }>
): string {
  const resultLines = toolResults
    .map((r) => `[${r.tool}]: ${r.result}`)
    .join("\n");

  return `
You are a response synthesizer for a distributed AI agent.
The agent ran several tools to answer the user's query.
Your job is to combine all tool outputs into one clear, friendly reply.

Rules:
- Write a single, coherent response that addresses the original query.
- Include every relevant piece of information from the tool results.
- Do not mention tool names or internal system details in your reply.
- Do not add information that is not present in the tool results.
- Keep the tone conversational and concise.
- Do not use markdown formatting.

Original user query: "${userQuery}"

Tool results:
${resultLines}

Final answer:
`.trim();
}
