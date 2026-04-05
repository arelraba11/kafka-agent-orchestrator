// ─── Final Answer Synthesis Prompt ───────────────────────────────────────────
// Used by: answerSynthesizer (Final Project)
// Technique: Structured summarisation
// Purpose:   Combine all tool results into a single coherent reply for the user.
// Note:      This is the ORCHESTRATION_SYNTHESIS_PROMPT referenced in the course requirements.

export interface HistoryMessage {
  role: "user" | "assistant";
  content: string;
}

export function synthesisPrompt(
  userQuery: string,
  toolResults: Array<{ tool: string; result: string }>,
  history?: HistoryMessage[]
): string {
  const resultLines = toolResults
    .map((r) => `[${r.tool}]: ${r.result}`)
    .join("\n");

  let historyBlock = "";
  if (history && history.length > 0) {
    const lines = history
      .map((m) => `${m.role === "user" ? "User" : "Assistant"}: ${m.content}`)
      .join("\n");
    historyBlock = `\nConversation history:\n${lines}\n`;
  }

  return `
You are a response synthesizer for a distributed AI agent.
The agent ran several tools to answer the user's query.
Your job is to combine all tool outputs into one clear, friendly reply.

Rules:
- Write a single, coherent response that addresses the original query.
- Include every relevant piece of information from the tool results.
- If the answer can be derived from conversation history, use it.
- Do not mention tool names or internal system details in your reply.
- Do not add information that is not present in the tool results or conversation history.
- Keep the tone conversational and concise.
- Do not use markdown formatting.
${historyBlock}
Original user query: "${userQuery}"

Tool results:
${resultLines}

Final answer:
`.trim();
}
