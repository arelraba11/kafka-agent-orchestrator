// ─── Prompt Templates ─────────────────────────────────────────────────────────
// All LLM prompts are centralised here.
// Techniques used:
//   - Few-Shot Prompting        (llmRouterPrompt)
//   - Structured JSON Output    (llmExtractionPrompt)
//   - Chain-of-Thought Reasoning(cotMathPrompt)

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

User: "What is 25 multiplied by 4?"
Response:
{
  "intent": "calculateMath",
  "parameters": { "expression": "25 * 4" },
  "confidence": 0.97
}

User: "convert 20 eur to ils"
Response:
{
  "intent": "currencyExchange",
  "parameters": { "amount": 20, "from": "EUR", "to": "ILS" },
  "confidence": 0.98
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

// ─── Exercise 3 — Review Analysis Prompts ────────────────────────────────────

// ─── Review Router Prompt ─────────────────────────────────────────────────────
// Used by: review-processor (Step 1)
// Technique: Zero-shot classification
// Purpose:   Determine if a message is a product review or should be ignored.

export function reviewRouterPrompt(userInput: string): string {
  return `You are a message classifier for a review processing pipeline.

Classify the following user message into one of these intents:
- analyzeReview   : The message is a product or service review containing an opinion.
- ignore          : The message is not a review (spam, greeting, test input, question, etc.).

Respond with valid JSON only. No explanation. No markdown.

Schema:
{
  "intent": "analyzeReview" | "ignore",
  "reason": string
}

Message:
"""
${userInput}
"""`;
}

// ─── Review Analyzer Prompt ───────────────────────────────────────────────────
// Used by: review-processor (Step 2)
// Technique: Structured JSON output
// Purpose:   Extract structured sentiment insights from review text.

export function reviewAnalyzerPrompt(reviewText: string): string {
  return `You are a sentiment analysis engine for product reviews.

Analyze the following review and return structured insights.

Rules:
- Respond with valid JSON only. No explanation. No markdown fences.
- score must be an integer between 1 and 10.
- overall_sentiment must be exactly one of: "Positive", "Negative", "Mixed", "Neutral".
- aspects is an array of { aspect: string, sentiment: "Positive" | "Negative" | "Neutral" }.
- summary must be one sentence, maximum 20 words.

Schema:
{
  "summary": string,
  "overall_sentiment": "Positive" | "Negative" | "Mixed" | "Neutral",
  "score": number,
  "aspects": [
    { "aspect": string, "sentiment": "Positive" | "Negative" | "Neutral" }
  ]
}

Review:
"""
${reviewText}
"""`;
}

// ─── Self-Correction Prompt ───────────────────────────────────────────────────
// Used by: review-processor (Step 3)
// Technique: Self-correction / reflective prompting
// Purpose:   Resolve the contradiction when score < 4 but sentiment == "Positive".

export function selfCorrectionPrompt(
  reviewText: string,
  previousResult: string,
  score: number
): string {
  return `You previously analyzed a product review and returned this result:

${previousResult}

There is a logical inconsistency in this result:
- The score is ${score}/10, which indicates a poor or very poor review.
- Yet the overall_sentiment is "Positive", which contradicts a low score.

Please re-analyze the original review and return a corrected, consistent result.

Rules:
- Respond with valid JSON only. No explanation. No markdown fences.
- The corrected score and sentiment must be logically consistent.
- Use the same schema as before.

Original review:
"""
${reviewText}
"""`;
}
