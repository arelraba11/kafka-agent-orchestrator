import { randomBytes } from "crypto";
import { Partitioners } from "kafkajs";

const ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";

/** 8 random [a-z0-9] characters. */
export function short(): string {
  const bytes = randomBytes(8);
  return Array.from(bytes, (b) => ALPHABET[b % ALPHABET.length]).join("");
}

/** The per-test token embedded as `[t:<token>]` in user input; it keys the fake LLM's scripts. */
export const token = short;

export const newCid = (): string => crypto.randomUUID();

/** One id for the whole `bun test` run (set once by setup.ts, so every file shares it). */
export function runId(): string {
  return (process.env.INT_RUN_ID ??= short());
}

export const tag = (t: string): string => `[t:${t}]`;

const partitioner = Partitioners.DefaultPartitioner();

/** The partition kafkajs's default (murmur2) partitioner picks for `key` on a topic with `partitions` partitions. */
export function partitionOf(key: string, partitions = 3): number {
  return partitioner({
    topic: "t",
    partitionMetadata: Array.from({ length: partitions }, (_, partitionId) => ({ partitionId, leader: 1 })) as never,
    message: { key, value: null },
  });
}

/** A fresh conversationId that hashes to `partition`. */
export function cidForPartition(partition: number, partitions = 3): string {
  for (;;) {
    const cid = newCid();
    if (partitionOf(cid, partitions) === partition) return cid;
  }
}
