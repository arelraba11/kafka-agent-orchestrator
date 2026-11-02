import { TOPICS, type TopicName } from "../../../shared/topics";
import { type Namespace } from "./namespace";
import { type Observer } from "./observer";
import { waitFor } from "./wait";

/** A consumer group that must reach lag 0: logical group name and the logical topic it reads. */
export interface LiveGroup {
  group: string;
  topic: TopicName;
}

export interface SettleReport {
  lagByGroup: Record<string, number>;
}

/**
 * Waits until the namespace is quiescent: every live group has lag 0, and the observer
 * has consumed every record produced so far. Sound without sleeps: every handler
 * publishes before its offset can commit, so once a group shows lag 0, everything it
 * emitted is already counted in the end offsets read AFTER the committed offsets.
 */
export async function settle(
  ns: Namespace,
  observer: Observer,
  groups: LiveGroup[],
  opts: { timeoutMs?: number } = {}
): Promise<SettleReport> {
  const topics = Object.values(TOPICS) as TopicName[];
  let last = "";

  const check = async (): Promise<SettleReport | false> => {
    // 1. committed offsets of every live group, FIRST
    const committed = new Map<string, number>(); // `${group}|${topic}:${p}` → offset
    for (const g of groups) {
      const res = await ns.admin.fetchOffsets({ groupId: ns.prefix + g.group, topics: [ns.physical(g.topic)] });
      for (const t of res) {
        for (const p of t.partitions) committed.set(`${g.group}|${g.topic}:${p.partition}`, Number(p.offset));
      }
    }
    // 2. then end offsets of every namespaced topic
    const end = new Map<string, number>();
    for (const t of topics) {
      for (const p of await ns.admin.fetchTopicOffsets(ns.physical(t))) end.set(`${t}:${p.partition}`, Number(p.offset));
    }

    const problems: string[] = [];
    const lagByGroup: Record<string, number> = {};
    for (const g of groups) {
      let lag = 0;
      for (const [key, e] of end) {
        if (!key.startsWith(`${g.topic}:`)) continue;
        const c = committed.get(`${g.group}|${key}`) ?? -1;
        lag += e - Math.max(c, 0);
      }
      lagByGroup[g.group] = lag;
      if (lag !== 0) problems.push(`${g.group} lag=${lag}`);
    }
    for (const [key, e] of end) {
      const seen = observer.positions.get(key) ?? 0;
      if (seen < e) problems.push(`observer behind on ${key}: ${seen}/${e}`);
    }
    last = problems.join(", ");
    return problems.length === 0 ? { lagByGroup } : false;
  };

  return (await waitFor(check, {
    timeoutMs: opts.timeoutMs ?? 15_000,
    what: "every live group at lag 0 and the observer caught up (settle)",
    context: () => `  not settled: ${last}\n${observer.describe()}`,
  })) as SettleReport;
}
