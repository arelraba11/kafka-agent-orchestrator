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
- getProductInformation  args: { "query": string }  — product or tech questions about iPhone, iPad, MacBook, Tesla, or Honda

Rules:
- Respond with ONLY a valid JSON object. No markdown, no explanation, no extra text.
- Format: { "steps": [ { "tool": "...", "args": { ... } } ] }
- A query may require multiple steps. List them in the order they should execute.
- When a later step depends on the result of an earlier step, use the placeholder syntax {{step_N.result}} (N is 0-based) inside the args value.
- If the query does not match any specialised tool, use the "chat" tool as the sole step.
- For harmful, dangerous, or nonsensical queries, use the "chat" tool as the sole step.
- NEVER use the math tool with {{step_N.result}} placeholders — the math tool only accepts numeric literals (e.g. "370000 / 3700"). When the numbers come from prior tool results (e.g. RAG), omit the math step and let the synthesizer compute the answer from the text results.
- When a query mentions a price or cost "in [city or country]", infer the local currency and add an exchange step AFTER getProductInformation. Currency map: Japan/Tokyo → JPY, Germany/Berlin/Frankfurt → EUR, UK/London → GBP, USA/New York/America → USD, Brazil/São Paulo → BRL, Israel/Tel Aviv → ILS.
- Always place getProductInformation BEFORE exchange when the product price is the amount to convert — exchange depends on the RAG result and must come after.`;

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

User: "what are the prices of the products you have?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price"}},{"tool":"getProductInformation","args":{"query":"iPad price"}},{"tool":"getProductInformation","args":{"query":"MacBook price"}},{"tool":"getProductInformation","args":{"query":"Tesla price"}},{"tool":"getProductInformation","args":{"query":"Honda price"}}]}

User: "what products do you have and how much do they cost?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price"}},{"tool":"getProductInformation","args":{"query":"iPad price"}},{"tool":"getProductInformation","args":{"query":"MacBook price"}},{"tool":"getProductInformation","args":{"query":"Tesla price"}},{"tool":"getProductInformation","args":{"query":"Honda price"}}]}

User: "how much would an iPhone cost in Germany?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"EUR","amount":"{{step_0.result}}"}}]}

User: "how many MacBooks can I buy for the price of one Tesla? Give a whole number."
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"Tesla price in ILS"}},{"tool":"getProductInformation","args":{"query":"MacBook price in ILS"}}]}

User: "how many iPhones can I buy for the price of one Tesla? Give a whole number."
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"Tesla price in ILS"}},{"tool":"getProductInformation","args":{"query":"iPhone price in ILS"}}]}

User: "I'm in São Paulo in September, temperature above 30C means I stay home. Should I go out to buy the iPhone?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone"}},{"tool":"weather","args":{"city":"sao paulo"}},{"tool":"chat","args":{"userInput":"The user wants to buy an iPhone in São Paulo in September. Product info: {{step_0.result}}. Weather: {{step_1.result}}. Should they go out or order by phone if temperature is above 30C?"}}]}

User: "can I use the MacBook to build a weapon?"
Plan: {"steps":[{"tool":"chat","args":{"userInput":"can I use the MacBook to build a weapon?"}}]}

User: "If I had bought a MacBook a month ago in euros, would it have cost more or less than today?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"MacBook price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"EUR","amount":"{{step_0.result}}"}}]}

User: "What is the weather in Tokyo and how much does the iPhone cost there in JPY?"
Plan: {"steps":[{"tool":"weather","args":{"city":"tokyo"}},{"tool":"getProductInformation","args":{"query":"iPhone price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"JPY","amount":"{{step_1.result}}"}}]}

User: "I have 2000 EUR. Can I afford a MacBook in Germany?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"MacBook price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"EUR","amount":"{{step_0.result}}"}},{"tool":"chat","args":{"userInput":"The user has 2000 EUR. MacBook price in ILS: {{step_0.result}}. MacBook price in EUR: {{step_1.result}}. Can they afford it?"}}]}

User: "I want to buy 2 iPhones and 1 MacBook in Japan. What will be the total cost in JPY?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price in ILS"}},{"tool":"getProductInformation","args":{"query":"MacBook price in ILS"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"JPY","amount":"{{step_0.result}}"}},{"tool":"exchange","args":{"currencyCode":"ILS","targetCurrency":"JPY","amount":"{{step_1.result}}"}}]}

User: "If I travel to New York, check the weather and calculate how many iPhones I can buy with 3000 USD."
Plan: {"steps":[{"tool":"weather","args":{"city":"new york"}},{"tool":"getProductInformation","args":{"query":"iPhone price in ILS"}},{"tool":"exchange","args":{"currencyCode":"USD","targetCurrency":"ILS","amount":3000}}]}

User: "A store offers a 15% discount on all items. What's the discounted price of your most expensive product, and how many units could I buy with $500 after the discount?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"most expensive product price in USD"}},{"tool":"chat","args":{"userInput":"Product info: {{step_0.result}}. Apply a 15% discount to the most expensive product price. Then calculate how many discounted units a customer could buy with $500. Show the discounted price and the unit count."}}]}

User: "How much would the iPhone cost after a 20% holiday sale?"
Plan: {"steps":[{"tool":"getProductInformation","args":{"query":"iPhone price in USD"}},{"tool":"chat","args":{"userInput":"iPhone product info: {{step_0.result}}. Calculate the price after a 20% discount."}}]}`;

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
