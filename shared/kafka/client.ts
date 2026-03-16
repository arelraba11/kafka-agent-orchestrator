import {
  Kafka,
  Producer,
  Consumer,
  EachMessagePayload,
  KafkaConfig,
} from "kafkajs";

// ─── Singleton Kafka instance ────────────────────────────────────────────────

const config: KafkaConfig = {
  clientId: "distributed-bot",
  brokers: ["localhost:9092"],
  retry: {
    initialRetryTime: 500,
    retries: 10,
  },
};

export const kafka = new Kafka(config);

// ─── Retry helper ─────────────────────────────────────────────────────────────
// Retries an async operation with linear backoff. Used to absorb the window
// between Kafka broker startup and full topic-metadata availability.

async function withRetry<T>(
  label: string,
  fn: () => Promise<T>,
  maxAttempts = 10,
  baseDelayMs = 2000
): Promise<T> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === maxAttempts) throw err;
      const delay = Math.min(baseDelayMs * attempt, 15000);
      console.warn(
        `[kafka] ${label} failed (attempt ${attempt}/${maxAttempts}), retrying in ${delay}ms... — ${(err as Error).message}`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  throw new Error(`${label}: unreachable`);
}

// ─── Producer ────────────────────────────────────────────────────────────────

export async function createProducer(): Promise<Producer> {
  return withRetry("createProducer", async () => {
    const producer = kafka.producer();
    await producer.connect();
    return producer;
  });
}

export async function sendMessage(
  producer: Producer,
  topic: string,
  key: string,
  value: unknown
): Promise<void> {
  await producer.send({
    topic,
    messages: [
      {
        key,
        value: JSON.stringify(value),
      },
    ],
  });
}

// ─── Consumer ────────────────────────────────────────────────────────────────

export async function createConsumer(groupId: string): Promise<Consumer> {
  return withRetry(`createConsumer:${groupId}`, async () => {
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    return consumer;
  });
}

export async function subscribeAndRun(
  consumer: Consumer,
  topics: string[],
  handler: (topic: string, key: string | null, value: unknown) => Promise<void>
): Promise<void> {
  // Retry subscribe independently: UNKNOWN_TOPIC_OR_PARTITION resolves once
  // the broker has propagated topic metadata (typically within a few seconds).
  await withRetry(`subscribe:${topics.join(",")}`, async () => {
    for (const topic of topics) {
      await consumer.subscribe({ topic, fromBeginning: false });
    }
  });

  await consumer.run({
    eachMessage: async ({ topic, message }: EachMessagePayload) => {
      const key = message.key?.toString() ?? null;
      const raw = message.value?.toString();
      if (!raw) return;

      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch {
        console.error(`[kafka] Failed to parse message on topic ${topic}:`, raw);
        return;
      }

      await handler(topic, key, parsed);
    },
  });
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────

export function registerShutdown(
  resources: Array<Producer | Consumer>
): void {
  const shutdown = async () => {
    for (const resource of resources) {
      await resource.disconnect();
    }
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
