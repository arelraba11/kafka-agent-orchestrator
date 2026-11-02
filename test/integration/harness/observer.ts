import type { Consumer } from "kafkajs";
import { AssignerProtocol } from "kafkajs";
import { MESSAGE_SCHEMAS, messageTypeOf, type MessageName } from "../../../shared/schemas/index";
import { MessageDeadLetteredSchema, type MessageDeadLettered } from "../../../shared/schemas/MessageDeadLettered";
import { TOPICS, logicalTopic, type TopicName } from "../../../shared/topics";
import { testKafka } from "./broker";
import { type Namespace } from "./namespace";
import { wake, waitFor } from "./wait";

/** One record seen on a topic of the namespace, in the order this observer received it. */
export interface Observed {
  topic: TopicName;
  partition: number;
  offset: string;
  key: string | null;
  raw: string | null;
  /** Parsed JSON, or null when the value is missing or not JSON. */
  value: any;
  type: MessageName | undefined;
  /** False when the value is not JSON, has no known type, or fails its contract. */
  valid: boolean;
}

export interface Observer {
  messages: Observed[];
  /** Next offset to be read per `<logical topic>:<partition>`. */
  positions: Map<string, number>;
  events(cid: string, type?: MessageName): Observed[];
  requestsFor(cid: string, stepIndex: number): Observed[];
  resultsFor(cid: string, stepIndex: number): Observed[];
  /** Every dead-letter record, each validated against MessageDeadLetteredSchema (throws if one is invalid). */
  dlq(): MessageDeadLettered[];
  /** Records on contract topics that failed validation (poison the test did not inject on purpose). */
  invalid(): Observed[];
  describe(cid?: string): string;
  stop(): Promise<void>;
}

export async function startObserver(ns: Namespace): Promise<Observer> {
  const groupId = `${ns.prefix}observer`;
  const consumer: Consumer = testKafka(`observer-${ns.name}`).consumer({ groupId, sessionTimeout: 10_000, heartbeatInterval: 1_000 });
  await consumer.connect();
  const topics = Object.values(TOPICS).map((t) => ns.physical(t));
  await consumer.subscribe({ topics, fromBeginning: true });

  const messages: Observed[] = [];
  const positions = new Map<string, number>();

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const logical = logicalTopic(ns.prefix, topic)!;
      const raw = message.value === null ? null : message.value.toString("utf8");
      let value: any = null;
      let type: MessageName | undefined;
      let valid = false;
      if (raw !== null) {
        try {
          value = JSON.parse(raw);
          type = messageTypeOf(value) as MessageName | undefined;
          valid = type !== undefined && MESSAGE_SCHEMAS[type].safeParse(value).success;
        } catch {
          value = null;
        }
      }
      messages.push({
        topic: logical,
        partition,
        offset: message.offset,
        key: message.key === null ? null : message.key.toString("utf8"),
        raw,
        value,
        type,
        valid,
      });
      positions.set(`${logical}:${partition}`, Number(message.offset) + 1);
      wake();
    },
  });

  // Ready = the observer owns all 3 partitions of all 4 topics.
  await waitFor(
    async () => {
      const [d] = (await ns.admin.describeGroups([groupId])).groups;
      if (d.state !== "Stable") return false;
      const assigned = new Set<string>();
      for (const m of d.members) {
        const a = AssignerProtocol.MemberAssignment.decode(m.memberAssignment)!;
        for (const [t, ps] of Object.entries(a.assignment)) for (const p of ps) assigned.add(`${t}:${p}`);
      }
      return topics.every((t) => [0, 1, 2].every((p) => assigned.has(`${t}:${p}`)));
    },
    { timeoutMs: 20_000, what: `observer group ${groupId} to own every partition` }
  );

  const events = (cid: string, type?: MessageName) =>
    messages.filter((m) => m.value?.conversationId === cid && (type === undefined || m.type === type));

  const observer: Observer = {
    messages,
    positions,
    events,
    requestsFor: (cid, k) =>
      events(cid, "ToolInvocationRequested").filter((m) => m.value.payload.stepIndex === k),
    resultsFor: (cid, k) =>
      events(cid, "ToolInvocationResulted").filter((m) => m.value.payload.stepIndex === k),
    dlq: () =>
      messages
        .filter((m) => m.topic === TOPICS.DEAD_LETTER_QUEUE)
        .map((m) => MessageDeadLetteredSchema.parse(m.value) as MessageDeadLettered),
    invalid: () => messages.filter((m) => m.topic !== TOPICS.DEAD_LETTER_QUEUE && !m.valid),
    describe: (cid) =>
      messages
        .filter((m) => cid === undefined || m.value?.conversationId === cid)
        .map((m) => `  ${m.topic}[${m.partition}]@${m.offset} ${m.type ?? "(untyped)"} ${m.raw?.slice(0, 160)}`)
        .join("\n") || "  (no messages)",
    stop: () => consumer.disconnect(),
  };
  return observer;
}
