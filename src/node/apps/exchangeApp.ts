import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { IntentExchangeEvent, AppResultEvent } from "../../../shared/types/events";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";

// ─── Static exchange rates (all relative to ILS) ──────────────────────────────

const RATES_TO_ILS: Record<string, number> = {
  USD: 3.70,
  EUR: 4.00,
  GBP: 4.65,
  CHF: 4.10,
  JPY: 0.025,
  CAD: 2.72,
  AUD: 2.40,
  ILS: 1.00,
};

function getRate(from: string, to: string): number {
  const fromRate = RATES_TO_ILS[from.toUpperCase()];
  const toRate   = RATES_TO_ILS[to.toUpperCase()];

  if (fromRate === undefined) throw new Error(`Unknown currency: ${from}`);
  if (toRate   === undefined) throw new Error(`Unknown currency: ${to}`);

  return fromRate / toRate;
}

function formatResult(from: string, to: string, rate: number, amount: number = 1): string {
  const total = parseFloat((rate * amount).toFixed(4));
  return `${amount} ${from.toUpperCase()} = ${total} ${to.toUpperCase()}`;
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("exchange-service");
const consumerTool = await createConsumer("exchange-tool-worker");

registerShutdown([producer, consumer, consumerTool]);

// ── Ex1/2: intent-exchange consumer (unchanged) ───────────────────────────────

await subscribeAndRun(
  consumer,
  [TOPICS.INTENT_EXCHANGE],
  async (_topic, _key, value) => {
    const event = value as IntentExchangeEvent;
    const { userId, currencyCode, targetCurrency, amount } = event;

    console.log(`[exchange] userId=${userId} from=${currencyCode} to=${targetCurrency} amount=${amount ?? 1}`);

    let payload: AppResultEvent;

    try {
      const rate   = getRate(currencyCode, targetCurrency);
      const result = formatResult(currencyCode, targetCurrency, rate, amount);

      payload = {
        userId,
        type: "exchange",
        result,
        success: true,
        timestamp: new Date().toISOString(),
      };

      console.log(`[exchange] result="${result}"`);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);

      payload = {
        userId,
        type: "exchange",
        result: "",
        success: false,
        error,
        timestamp: new Date().toISOString(),
      };

      console.error(`[exchange] error="${error}"`);
    }

    await sendMessage(producer, TOPICS.APP_RESULTS, userId, payload);
  }
);

// ── Final Project: tool-invocation-requests consumer ─────────────────────────

subscribeAndRun(
  consumerTool,
  [TOPICS.TOOL_INVOCATION_REQUESTS],
  async (_topic, _key, value) => {
    const req = value as ToolInvocationRequested;
    if (req.payload.toolName !== "exchange") return;

    const { conversationId } = req;
    const currencyCode   = (req.payload.input.currencyCode   as string) ?? "USD";
    const targetCurrency = (req.payload.input.targetCurrency as string) ?? "ILS";
    const amount         = (req.payload.input.amount         as number) ?? 1;

    console.log(`[exchange] tool conversationId=${conversationId} from=${currencyCode} to=${targetCurrency} amount=${amount}`);

    let resultStr: string;
    let success: boolean;
    let errorMsg: string | undefined;

    try {
      const rate = getRate(currencyCode, targetCurrency);
      resultStr = formatResult(currencyCode, targetCurrency, rate, amount);
      success = true;
      console.log(`[exchange] tool result="${resultStr}"`);
    } catch (err) {
      errorMsg = err instanceof Error ? err.message : String(err);
      resultStr = "";
      success = false;
      console.error(`[exchange] tool error="${errorMsg}"`);
    }

    const event: ToolInvocationResulted = {
      conversationId,
      timestamp: Date.now(),
      eventType: "ToolInvocationResulted",
      payload: {
        toolName: "exchange",
        result: { value: resultStr, success, ...(errorMsg ? { error: errorMsg } : {}) },
      },
    };

    await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, event);
  }
).catch(err => console.error("[exchange] Tool worker error:", err));

console.log("[exchange] ExchangeApp started.");
