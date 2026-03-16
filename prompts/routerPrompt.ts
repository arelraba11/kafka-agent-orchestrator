// ─── Router / Plan Generator Prompt ──────────────────────────────────────────
// Used by: routerService (final project)
// Technique: Few-Shot Prompting
// Purpose:   Convert a user query into an ordered plan of tool steps.

export function routerPrompt(userQuery: string): string {
  return `
You are a planning engine for a distributed AI agent system.
Given a user query, produce an ordered list of tool steps needed to answer it.

Available tools:
  - weather          : get current weather for a city
  - exchange         : convert between currencies
  - math             : evaluate a math expression
  - chat             : answer general questions or conversation
  - getProductInformation : retrieve product facts from the knowledge base

Rules:
- Return ONLY a valid JSON object. No explanation. No markdown.
- steps must be an array of tool names in the order they should run.
- Use only tools from the list above.
- Include every tool required to fully answer the query.
- If the query needs no special tool, use ["chat"].

Schema:
{
  "steps": string[]
}

--- Examples ---

User: "What is the weather in Tel Aviv?"
Response:
{
  "steps": ["weather"]
}

User: "Convert 200 USD to ILS and tell me the weather in Jerusalem"
Response:
{
  "steps": ["exchange", "weather"]
}

User: "What is 15 multiplied by 8?"
Response:
{
  "steps": ["math"]
}

User: "Tell me about the MacBook"
Response:
{
  "steps": ["getProductInformation"]
}

User: "What is the weather in Haifa, exchange 50 EUR to ILS, and calculate 100 / 4"
Response:
{
  "steps": ["weather", "exchange", "math"]
}

--- Now plan ---

User: "${userQuery}"
Response:
`.trim();
}
