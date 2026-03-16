import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type {
  UserInputEvent,
  AppResultEvent,
  UserControlEvent,
  ConversationHistoryUpdateEvent,
} from "../../shared/types/events";
import type { ConversationHistory, ConversationMessage } from "../../shared/types/conversation";

// ─── Persistence ──────────────────────────────────────────────────────────────

const HISTORY_FILE = new URL("history.json", import.meta.url).pathname;

type HistoryStore = Record<string, ConversationHistory>;

async function loadHistory(): Promise<HistoryStore> {
  const file = Bun.file(HISTORY_FILE);
  if (!(await file.exists())) {
    console.log("[memory] No history.json found, starting fresh.");
    return {};
  }
  const store = await file.json() as HistoryStore;
  const userCount = Object.keys(store).length;
  console.log(`[memory] Loaded history for ${userCount} user(s).`);
  return store;
}

async function saveHistory(store: HistoryStore): Promise<void> {
  await Bun.write(HISTORY_FILE, JSON.stringify(store, null, 2));
}

// ─── In-memory store ──────────────────────────────────────────────────────────

const store: HistoryStore = await loadHistory();

// ─── Store operations ─────────────────────────────────────────────────────────

function getHistory(userId: string): ConversationHistory {
  return store[userId] ?? [];
}

function appendMessage(userId: string, message: ConversationMessage): void {
  if (!store[userId]) store[userId] = [];
  store[userId].push(message);
}

function deleteHistory(userId: string): void {
  delete store[userId];
}

// ─── Publish history update ───────────────────────────────────────────────────

async function publishUpdate(
  producer: Awaited<ReturnType<typeof createProducer>>,
  userId: string
): Promise<void> {
  const event: ConversationHistoryUpdateEvent = {
    userId,
    history: getHistory(userId),
    timestamp: new Date().toISOString(),
  };
  await sendMessage(producer, TOPICS.HISTORY_UPDATE, userId, event);
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("memory-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.USER_INPUT, TOPICS.APP_RESULTS, TOPICS.USER_CONTROL],
  async (topic, _key, value) => {

    if (topic === TOPICS.USER_INPUT) {
      const event = value as UserInputEvent;
      const message: ConversationMessage = {
        role: "user",
        content: event.userInput,
        timestamp: event.timestamp,
      };
      appendMessage(event.userId, message);
      await saveHistory(store);
      await publishUpdate(producer, event.userId);
      console.log(`[memory] Saved user message for userId=${event.userId}`);
      return;
    }

    if (topic === TOPICS.APP_RESULTS) {
      const event = value as AppResultEvent;
      if (!event.success) return; // do not store failed responses
      const message: ConversationMessage = {
        role: "assistant",
        content: event.result,
        timestamp: event.timestamp,
      };
      appendMessage(event.userId, message);
      await saveHistory(store);
      await publishUpdate(producer, event.userId);
      console.log(`[memory] Saved bot response for userId=${event.userId}`);
      return;
    }

    if (topic === TOPICS.USER_CONTROL) {
      const event = value as UserControlEvent;
      if (event.command === "reset") {
        deleteHistory(event.userId);
        await saveHistory(store);
        await publishUpdate(producer, event.userId);
        console.log(`[memory] Reset history for userId=${event.userId}`);
      }
    }
  }
);

console.log("[memory] MemoryService started.");
