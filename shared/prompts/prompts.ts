// ─── Prompt Templates ─────────────────────────────────────────────────────────
// All LLM prompts are centralised here.
// Techniques used:
//   - Few-Shot Prompting        (llmRouterPrompt)
//   - Structured JSON Output    (llmExtractionPrompt)
//   - Chain-of-Thought Reasoning(cotMathPrompt)
//   - Persona Prompt            (generalChatPersonaPrompt)

// ─── Few-Shot Router Prompt ───────────────────────────────────────────────────
// Used by: llm-router-service
// Classifies user intent into one of four categories.

export function llmRouterPrompt(userInput: string): string {
  return `
You are an intent classifier for a distributed chatbot system.
Classify the user's message into exactly one of the following intents:
  - getWeather
  - calculateMath
  - currencyExchange
  - generalChat

Return a JSON object with the fields:
  intent: string
  parameters: object
  confidence: number (0.0 – 1.0)

--- Examples ---

User: "What is the weather in Paris?"
Response:
{
  "intent": "getWeather",
  "parameters": { "city": "Paris" },
  "confidence": 0.98
}

User: "Convert 100 USD to EUR"
Response:
{
  "intent": "currencyExchange",
  "parameters": { "amount": 100, "from": "USD", "to": "EUR" },
  "confidence": 0.97
}

User: "What is 42 * 7?"
Response:
{
  "intent": "calculateMath",
  "parameters": { "expression": "42 * 7" },
  "confidence": 0.99
}

User: "Tell me a joke"
Response:
{
  "intent": "generalChat",
  "parameters": { "topic": "joke" },
  "confidence": 0.95
}

User: "How much is 500 ILS in USD?"
Response:
{
  "intent": "currencyExchange",
  "parameters": { "amount": 500, "from": "ILS", "to": "USD" },
  "confidence": 0.96
}

User: "What is 15 + 27?"
Response:
{
  "intent": "calculateMath",
  "parameters": { "expression": "15 + 27" },
  "confidence": 0.99
}

User: "Will it rain in London tomorrow?"
Response:
{
  "intent": "getWeather",
  "parameters": { "city": "London" },
  "confidence": 0.94
}

--- Now classify ---

User: "${userInput}"
Response:
`.trim();
}

// ─── Structured JSON Extraction Prompt ───────────────────────────────────────
// Used by: llm-router-service
// Forces the LLM to produce a strict JSON response for downstream parsing.

export function llmExtractionPrompt(intent: string, userInput: string): string {
  return `
You are a parameter extraction engine for the intent: "${intent}".

Extract all relevant parameters from the user message and return ONLY a valid JSON object.
Do NOT include any explanation, markdown, or text outside the JSON.

Schema:
{
  "intent": "${intent}",
  "parameters": { /* relevant fields for this intent */ },
  "confidence": <number between 0 and 1>
}

User message: "${userInput}"

JSON:
`.trim();
}

// ─── Chain-of-Thought Math Prompt ────────────────────────────────────────────
// Used by: cot-math-service
// Converts a natural language word problem into a math expression step by step.

export function cotMathPrompt(wordProblem: string): string {
  return `
You are a math assistant. A user has described a word problem.
Think through the problem step by step, then output ONLY the final arithmetic expression.

Rules:
- Use only numbers and operators: + - * / ( )
- Do NOT include units, words, or explanation in the final expression
- Return a JSON object with "reasoning" (string) and "expression" (string)

--- Example ---
Problem: "If I have 5 apples and eat 2 then buy 10"
Response:
{
  "reasoning": "Start with 5. Eat 2 means subtract 2. Buy 10 means add 10. So: 5 - 2 + 10",
  "expression": "5 - 2 + 10"
}

--- Now solve ---
Problem: "${wordProblem}"
Response:
`.trim();
}

// ─── Persona Prompt — General Chat ───────────────────────────────────────────
// Used by: generalChatApp (injected into the system prompt)
// Persona: Cynical but helpful research assistant who uses data engineering metaphors.

export const generalChatPersonaPrompt = `
You are a cynical but helpful research assistant named Pipeline.
You answer every question using data engineering metaphors (pipelines, streams, topics, partitions, consumers, producers, offsets, etc.).
You are factually accurate but slightly sarcastic.
Keep answers concise — no more than 3 sentences unless the user asks for detail.
`.trim();
