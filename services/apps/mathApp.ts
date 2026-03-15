import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { IntentMathEvent, AppResultEvent } from "../../shared/types/events";

// ─── Safe math evaluator ──────────────────────────────────────────────────────
// Accepts only expressions containing digits, whitespace, and + - * / ( )
// No eval(), no dynamic code execution.

const SAFE_EXPR_REGEX = /^[\d\s+\-*/().]+$/;

function evaluate(expression: string): number {
  if (!SAFE_EXPR_REGEX.test(expression)) {
    throw new Error(`Unsafe expression rejected: "${expression}"`);
  }

  // Tokenize and evaluate using a simple recursive descent parser.
  const tokens = tokenize(expression);
  const result = parseExpr(tokens);

  if (tokens.length > 0) {
    throw new Error(`Unexpected token: "${tokens[0]}"`);
  }

  return result;
}

// ─── Tokenizer ────────────────────────────────────────────────────────────────

function tokenize(expr: string): string[] {
  return expr.match(/\d+\.?\d*|[+\-*/()]/g) ?? [];
}

// ─── Recursive descent parser: handles +/- then */÷ then parentheses ─────────

function parseExpr(tokens: string[]): number {
  let left = parseTerm(tokens);

  while (tokens[0] === "+" || tokens[0] === "-") {
    const op = tokens.shift()!;
    const right = parseTerm(tokens);
    left = op === "+" ? left + right : left - right;
  }

  return left;
}

function parseTerm(tokens: string[]): number {
  let left = parseFactor(tokens);

  while (tokens[0] === "*" || tokens[0] === "/") {
    const op = tokens.shift()!;
    const right = parseFactor(tokens);
    if (op === "/" && right === 0) throw new Error("Division by zero");
    left = op === "*" ? left * right : left / right;
  }

  return left;
}

function parseFactor(tokens: string[]): number {
  const token = tokens.shift();

  if (token === undefined) throw new Error("Unexpected end of expression");

  if (token === "(") {
    const value = parseExpr(tokens);
    if (tokens.shift() !== ")") throw new Error("Missing closing parenthesis");
    return value;
  }

  const num = parseFloat(token);
  if (isNaN(num)) throw new Error(`Invalid token: "${token}"`);
  return num;
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("math-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.INTENT_MATH],
  async (_topic, _key, value) => {
    const event = value as IntentMathEvent;
    const { userId, expression } = event;

    console.log(`[math] userId=${userId} expression="${expression}"`);

    let payload: AppResultEvent;

    try {
      const result = evaluate(expression);
      const formatted = Number.isInteger(result)
        ? result.toString()
        : result.toFixed(4).replace(/\.?0+$/, "");

      payload = {
        userId,
        type: "math",
        result: formatted,
        success: true,
        timestamp: new Date().toISOString(),
      };

      console.log(`[math] result="${formatted}"`);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);

      payload = {
        userId,
        type: "math",
        result: "",
        success: false,
        error,
        timestamp: new Date().toISOString(),
      };

      console.error(`[math] error="${error}"`);
    }

    await sendMessage(producer, TOPICS.APP_RESULTS, userId, payload);
  }
);

console.log("[math] MathApp started.");
