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
const consumer = await createConsumer("exchange-tool-worker");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.TOOL_INVOCATION_REQUESTS],
  async (_topic, _key, value) => {
    const req = value as ToolInvocationRequested;
    if (req.payload.toolName !== "exchange") return;

    const { conversationId } = req;
    const currencyCode   = (req.payload.input.currencyCode   as string) ?? "USD";
    const targetCurrency = (req.payload.input.targetCurrency as string) ?? "ILS";
    const amount         = parseFloat(String(req.payload.input.amount ?? 1));

    console.log(`[exchange] conversationId=${conversationId} from=${currencyCode} to=${targetCurrency} amount=${amount}`);

    let resultStr: string;
    let success: boolean;
    let errorMsg: string | undefined;

    try {
      const rate = getRate(currencyCode, targetCurrency);
      resultStr = formatResult(currencyCode, targetCurrency, rate, amount);
      success = true;
      console.log(`[exchange] result="${resultStr}"`);
    } catch (err) {
      errorMsg = err instanceof Error ? err.message : String(err);
      resultStr = "";
      success = false;
      console.error(`[exchange] error="${errorMsg}"`);
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
);

console.log("[exchange] ExchangeApp started.");
