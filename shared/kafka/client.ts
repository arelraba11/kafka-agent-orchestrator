import {
  Kafka,
  Producer,
  Consumer,
  EachMessagePayload,
  KafkaConfig,
} from "kafkajs";
import { TOPICS, type TopicName } from "../topics";
import {
  MESSAGE_SCHEMAS,
  TOPIC_MESSAGES,
  keyOf,
  messageTypeOf,
  type MessageName,
  type MessageOf,
  type TopicMessage,
} from "../schemas/index";
import { processMessage, MAX_UNKNOWN_ATTEMPTS, type ConsumeDeps } from "./consume";
import { OutboundContractViolationError, ProducerError, toIssues } from "./errors";

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

/**
 * Checks an outbound message against its contract without sending it. Throws
 * OutboundContractViolationError (non-retryable) when the message has no known type,
 * the type does not belong to `topic`, or the schema rejects it.
 */
export function validateOutbound<T extends TopicName>(topic: T, message: TopicMessage<T>): void {
  const type = messageTypeOf(message);
  if (type === undefined) {
    throw new OutboundContractViolationError(
      `outbound message for ${topic} has no eventType or commandType`,
      [{ path: "eventType", message: "missing discriminator" }],
      "(unknown)",
      topic
    );
  }
  if (!(TOPIC_MESSAGES[topic] as readonly string[]).includes(type)) {
    throw new OutboundContractViolationError(
      `${type} does not belong on topic ${topic}`,
      [{ path: "eventType", message: `${type} is not published to ${topic}` }],
      type,
      topic
    );
  }
  const result = MESSAGE_SCHEMAS[type as MessageName].safeParse(message);
  if (!result.success) {
    throw new OutboundContractViolationError(
      `outbound ${type} for ${topic} violates its contract`,
      toIssues(result.error.issues),
      type,
      topic
    );
  }
}

/**
 * Validates, then sends. The key is derived from the message (conversationId, or the
 * original key for a dead letter), so callers cannot pick a wrong one. A contract
 * violation throws OutboundContractViolationError before any send; a send failure is
 * wrapped in ProducerError (retryable).
 */
export async function publish<T extends TopicName>(
  producer: Producer,
  topic: T,
  message: TopicMessage<T>
): Promise<void> {
  validateOutbound(topic, message);
  const key = keyOf(topic, message);
  try {
    await producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(message) }],
    });
  } catch (cause) {
    throw new ProducerError(`send to ${topic} failed: ${(cause as Error).message}`, { cause });
  }
}

// ─── Consumer ────────────────────────────────────────────────────────────────

export async function createConsumer(groupId: string): Promise<Consumer> {
  return withRetry(`createConsumer:${groupId}`, async () => {
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    return consumer;
  });
}

export interface SubscribeOptions<N extends MessageName> {
  consumer: Consumer;
  groupId: string;
  /** Short service name recorded in dead-letter records. */
  service: string;
  /** Used to write dead-letter records. */
  producer: Producer;
  topic: TopicName;
  /** The message types this service handles; the handler receives exactly those. */
  types: readonly N[];
  handler: (message: MessageOf<N>) => Promise<void>;
}

/**
 * Subscribes to one topic and runs every message through processMessage: validate,
 * dead-letter poison, rethrow transient failures. See consume.ts for the semantics.
 */
export async function subscribeAndRun<N extends MessageName>(opts: SubscribeOptions<N>): Promise<void> {
  const { consumer, groupId, service, producer, topic, types, handler } = opts;

  // Retry subscribe independently: UNKNOWN_TOPIC_OR_PARTITION resolves once
  // the broker has propagated topic metadata (typically within a few seconds).
  await withRetry(`subscribe:${topic}`, async () => {
    await consumer.subscribe({ topic, fromBeginning: false });
  });

  const deps: ConsumeDeps = {
    publishDeadLetter: (record) => publish(producer, TOPICS.DEAD_LETTER_QUEUE, record),
    attempts: new Map(),
    now: () => Date.now(),
    maxUnknownAttempts: MAX_UNKNOWN_ATTEMPTS,
  };
  const ctx = { topic, group: groupId, service, types };

  await consumer.run({
    eachMessage: async ({ message, partition }: EachMessagePayload) => {
      await processMessage(
        {
          topic,
          partition,
          offset: message.offset,
          key: message.key,
          value: message.value,
          timestamp: message.timestamp ?? null,
        },
        ctx,
        handler,
        deps
      );
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
