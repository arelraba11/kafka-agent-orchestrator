import type { Producer } from "kafkajs";
import type { ConversationFailed } from "../schemas/ConversationFailed";
import type { MessageDeadLettered } from "../schemas/MessageDeadLettered";
import type { MessageName } from "../schemas/index";
import { TOPICS } from "../topics";
import { publish } from "./client";
import type { DeadLetteredHook } from "./consume";

type Stage = ConversationFailed["payload"]["stage"];

/** Pure. The user-facing failure for a dead-lettered message. `record.conversationId` must be non-null. */
export function buildConversationFailed(
  record: MessageDeadLettered,
  stage: Stage,
  now: number
): ConversationFailed {
  if (record.conversationId === null) {
    throw new Error("cannot report a dead letter that has no conversationId");
  }
  const { source, consumer, error } = record.payload;
  return {
    conversationId: record.conversationId,
    timestamp: now,
    eventType: "ConversationFailed",
    payload: {
      stage,
      code: "internal-error",
      reason: `${source.topic} message (partition ${source.partition}, offset ${source.offset}) dead-lettered by ${consumer.service}: ${error.class}: ${error.reason}`,
    },
  };
}

/**
 * The hook a service passes to subscribeAndRun: when a message of type `owns` is
 * dead-lettered, publish ConversationFailed{stage, code:"internal-error"}. `when` can
 * veto (the orchestrator reports only when no plan exists).
 */
export function conversationFailedHook(
  producer: Producer,
  opts: {
    owns: MessageName;
    stage: Stage;
    when?: (record: MessageDeadLettered) => Promise<boolean>;
  }
): DeadLetteredHook {
  return {
    owns: [opts.owns],
    async handle(record) {
      if (opts.when && !(await opts.when(record))) return;
      const event = buildConversationFailed(record, opts.stage, Date.now());
      await publish(producer, TOPICS.CONVERSATION_EVENTS, event);
      console.log(
        `[${record.payload.consumer.service}] conversationId=${event.conversationId} ConversationFailed stage=${opts.stage} code=internal-error (dead-lettered ${opts.owns})`
      );
    },
  };
}
