import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { IntentExchangeEvent, AppResultEvent } from "../../shared/types/events";

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

registerShutdown([producer, consumer]);

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

console.log("[exchange] ExchangeApp started.");
