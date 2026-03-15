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
};

export const kafka = new Kafka(config);

// ─── Producer ────────────────────────────────────────────────────────────────

export async function createProducer(): Promise<Producer> {
  const producer = kafka.producer();
  await producer.connect();
  return producer;
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
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}

export async function subscribeAndRun(
  consumer: Consumer,
  topics: string[],
  handler: (topic: string, key: string | null, value: unknown) => Promise<void>
): Promise<void> {
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

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
