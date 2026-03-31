import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";

// ─── Safe math evaluator ──────────────────────────────────────────────────────
// Accepts only expressions containing digits, whitespace, and + - * / ( )
// No eval(), no dynamic code execution.

const SAFE_EXPR_REGEX = /^[\d\s+\-*/().]+$/;

function evaluate(expression: string): number {
  if (!SAFE_EXPR_REGEX.test(expression)) {
    throw new Error(`Unsafe expression rejected: "${expression}"`);
  }

  const tokens = tokenize(expression);
  const result = parseExpr(tokens);

  if (tokens.length > 0) {
    throw new Error(`Unexpected token: "${tokens[0]}"`);
  }

  return result;
}

function tokenize(expr: string): string[] {
  return expr.match(/\d+\.?\d*|[+\-*/()]/g) ?? [];
}

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
const consumer = await createConsumer("math-tool-worker");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.TOOL_INVOCATION_REQUESTS],
  async (_topic, _key, value) => {
    const req = value as ToolInvocationRequested;
    if (req.payload.toolName !== "math") return;

    const { conversationId } = req;
    const expression = (req.payload.input.expression as string) ?? "";

    console.log(`[math] conversationId=${conversationId} expression="${expression}"`);

    let resultValue: string;
    let success: boolean;
    let errorMsg: string | undefined;

    try {
      const result = evaluate(expression);
      resultValue = Number.isInteger(result)
        ? result.toString()
        : result.toFixed(4).replace(/\.?0+$/, "");
      success = true;
      console.log(`[math] result="${resultValue}"`);
    } catch (err) {
      errorMsg = err instanceof Error ? err.message : String(err);
      resultValue = "";
      success = false;
      console.error(`[math] error="${errorMsg}"`);
    }

    const event: ToolInvocationResulted = {
      conversationId,
      timestamp: Date.now(),
      eventType: "ToolInvocationResulted",
      payload: {
        toolName: "math",
        result: { value: resultValue, success, ...(errorMsg ? { error: errorMsg } : {}) },
      },
    };

    await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, event);
  }
);

console.log("[math] MathApp started.");
