// ─── CoT Math Service ─────────────────────────────────────────────────────────
// Consumes: router-decision-events
// Produces: intent-math
//
// Handles all math intents using Chain-of-Thought reasoning for word problems
// and direct pass-through for pure arithmetic expressions.

import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import { callLLM } from "../../shared/llm/openai";
import { cotMathPrompt } from "../../shared/prompts/prompts";
import type { RouterDecisionEvent, IntentMathEvent } from "../../shared/types/events";

// ─── Heuristics ───────────────────────────────────────────────────────────────

// Matches pure arithmetic expressions: "42 * 7", "15 + 27", "100 / 4"
const PURE_EXPRESSION_REGEX = /^\s*[\d\s+\-*/().]+\s*$/;

// Matches math-related inputs including word problems
const MATH_SIGNAL_REGEX =
  /[\d]+\s*[+\-*/]\s*[\d]+|\b(plus|minus|times|divided|multiply|subtract|add|sum|total|how many|calculate|compute|apples|oranges|items|percent|average|difference|product|quotient)\b/i;

// Currency exchange messages that contain numbers should NOT be treated as math.
// "convert 50 dollars to shekels" — "dollars" and digit would otherwise fire MATH_SIGNAL_REGEX.
const CURRENCY_EXCHANGE_REGEX =
  /\b(convert|exchange|to\s+(usd|eur|ils|gbp|jpy|chf|cad|aud|dollars?|euros?|shekels?|pounds?|yen|francs?))\b/i;

function isMathInput(input: string): boolean {
  if (CURRENCY_EXCHANGE_REGEX.test(input)) return false;   // currency, not math
  return MATH_SIGNAL_REGEX.test(input);
}

function isPureExpression(input: string): boolean {
  return PURE_EXPRESSION_REGEX.test(input);
}

function extractPureExpression(input: string): string {
  const match = input.match(/[\d]+[\s\d+\-*/().]+/);
  return match ? match[0].trim() : input.trim();
}

// ─── Processing ───────────────────────────────────────────────────────────────

async function processMath(
  producer: Awaited<ReturnType<typeof createProducer>>,
  event: RouterDecisionEvent
): Promise<void> {
  const { userId, input, timestamp } = event;

  if (!isMathInput(input)) {
    return; // not math — skip
  }

  let expression: string;

  if (isPureExpression(input)) {
    // Direct arithmetic — no LLM needed
    expression = extractPureExpression(input);
    console.log(`[cot-math] userId=${userId} pure expression="${expression}"`);
  } else {
    // Word problem — use Chain-of-Thought reasoning
    try {
      const raw = await callLLM(cotMathPrompt(input));
      const parsed: { reasoning: string; expression: string } = JSON.parse(raw);
      expression = parsed.expression;
      console.log(`[cot-math] userId=${userId} reasoning="${parsed.reasoning}"`);
      console.log(`[cot-math] userId=${userId} expression="${expression}"`);
    } catch (err) {
      console.error(`[cot-math] Failed to parse CoT response for userId=${userId}:`, err);
      return;
    }
  }

  const payload: IntentMathEvent = { userId, expression, timestamp };
  await sendMessage(producer, TOPICS.INTENT_MATH, userId, payload);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("cot-math-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(consumer, [TOPICS.ROUTER_DECISION], async (_topic, _key, value) => {
  const event = value as RouterDecisionEvent;
  await processMath(producer, event);
});

console.log("[cot-math] CotMathService started.");
