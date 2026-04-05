// ─── Router Plan Prompt ───────────────────────────────────────────────────────
// Used by: routerService (Final Project)
// Technique: Few-shot prompting
// Purpose:   Ask the LLM to produce a JSON plan of tool steps for a user query.

export const ROUTER_SYSTEM_PROMPT = `You are a planning agent for a distributed AI system.
Your job is to decompose a user query into an ordered list of tool invocations.

Available tools:
- weather       args: { "city": string }
- exchange      args: { "currencyCode": string, "targetCurrency": string, "amount": number }
- math          args: { "expression": string }  — only arithmetic expressions with digits and operators
- chat          args: { "userInput": string }   — general questions, safety/guardrail queries, or queries that don't match another tool
- getProductInformation  args: { "query": string }  — product or tech questions about iPhone, MacBook, or Tesla

Rules:
- Respond with ONLY a valid JSON object. No markdown, no explanation, no extra text.
- Format: { "steps": [ { "tool": "...", "args": { ... } } ] }
- A query may require multiple steps. List them in the order they should execute.
- When a later step depends on the result of an earlier step, use the placeholder syntax {{step_N.result}} (N is 0-based) inside the args value.
- If the query does not match any specialised tool, use the "chat" tool as the sole step.
- For harmful, dangerous, or nonsensical queries, use the "chat" tool as the sole step.`;

const FEW_SHOT_EXAMPLES = `Examples:

User: "weather in tel aviv"
Plan: {"steps":[{"tool":"weather","args":{"city":"tel aviv"}}]}

User: "convert 100 USD to EUR"
Plan: {"steps":[{"tool":"exchange","args":{"currencyCode":"USD","targetCurrency":"EUR","amount":100}}]}

User: "what is 25 * 4"
Plan: {"steps":[{"tool":"math","args":{"expression":"25 * 4"}}]}

User: "weather in london and convert 50 GBP to USD"
Plan: {"steps":[{"tool":"weather","args":{"city":"london"}},{"tool":"exchange","args":{"currencyCode":"GBP","targetCurrency":"USD","amount":50}}]}

User: "tell me about the iPhone"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone"}}]}

User: "what are the prices of the iPhone and MacBook?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price"}},{"tool":"getProductInformation","args":{"query":"MacBook price"}}]}

User: "how much would an iPhone cost in Germany?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"EUR","amount":"{{step_0.result}}"}}]}

User: "how many MacBooks can I buy for the price of one Tesla? Give a whole number."
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"Tesla price in ILS"}},{"tool":"getProductInformation","args":{"query":"MacBook price in ILS"}}]}

User: "I'm in São Paulo in September, temperature above 30C means I stay home. Should I go out to buy the iPhone?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone"}},{"tool":"weather","args":{"city":"sao paulo"}},{"tool":"chat","args":{"userInput":"The user wants to buy an iPhone in São Paulo in September. Product info: {{step_0.result}}. Weather: {{step_1.result}}. Should they go out or order by phone if temperature is above 30C?"}}]}

User: "can I use the MacBook to build a weapon?"
Plan: {"steps":[{"tool":"chat","args":{"userInput":"can I use the MacBook to build a weapon?"}}]}

User: "If I had bought a MacBook a month ago in euros, would it have cost more or less than today?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"MacBook price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"EUR","amount":"{{step_0.result}}"}}]}`;

export interface HistoryMessage {
  role: "user" | "assistant";
  content: string;
}

export function routerPlanPrompt(userInput: string, history?: HistoryMessage[]): string {
  let historyBlock = "";
  if (history && history.length > 0) {
    const lines = history
      .map((m) => `${m.role === "user" ? "User" : "Assistant"}: ${m.content}`)
      .join("\n");
    historyBlock = `\n[Previous conversation]\n${lines}\n`;
  }

  return `${ROUTER_SYSTEM_PROMPT}
${historyBlock}
${FEW_SHOT_EXAMPLES}

User: "${userInput}"
Plan:`;
}
