import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { IntentGeneralChatEvent, AppResultEvent } from "../../shared/types/events";
import type { ConversationHistory } from "../../shared/types/conversation";

// ─── Rule-based response engine ───────────────────────────────────────────────

interface Rule {
  pattern: RegExp;
  response: string;
}

const RULES: Rule[] = [
  { pattern: /\b(ai|artificial intelligence|machine learning|llm)\b/i,
    response: "AI is a fascinating field that is rapidly evolving. It's changing how we work, create, and solve problems." },

  { pattern: /\b(hello|hi|hey|shalom)\b/i,
    response: "Hello! How can I help you today?" },

  { pattern: /\b(how are you|how do you do|how.s it going)\b/i,
    response: "I'm doing great, thanks for asking! What's on your mind?" },

  { pattern: /\b(kafka|streaming|event.driven)\b/i,
    response: "Kafka is a powerful distributed event streaming platform. It's great for building real-time data pipelines and microservices." },

  { pattern: /\b(what is your name|who are you|what are you)\b/i,
    response: "I'm a distributed bot built on Kafka microservices. Each reply you see went through an intent router, a worker service, and an aggregator!" },

  { pattern: /\b(thank|thanks|thank you)\b/i,
    response: "You're welcome! Let me know if there's anything else I can help with." },

  { pattern: /\b(bye|goodbye|see you|ciao)\b/i,
    response: "Goodbye! Feel free to come back anytime." },

  { pattern: /\b(joke|funny|humor)\b/i,
    response: "Why do programmers prefer dark mode? Because light attracts bugs!" },

  { pattern: /\b(meaning of life|42)\b/i,
    response: "42, obviously. Though the real answer might be in the journey of asking the question." },
];

const FALLBACK_RESPONSES = [
  "That's an interesting thought. Tell me more.",
  "I'm not sure I fully understand. Could you elaborate?",
  "Good question. I'd need more context to give you a proper answer.",
  "Hmm, that's a broad topic. What aspect interests you most?",
  "I hear you. Let's explore that further.",
];

// ─── History-aware memory lookup ──────────────────────────────────────────────
// Searches past user messages for facts the user stated about themselves.

function lookupFromHistory(userInput: string, history: ConversationHistory): string | null {
  // "what is my name?" / "what's my name?" / "who am i?"
  if (/\b(what'?s? (is )?my name|who am i)\b/i.test(userInput)) {
    for (let i = history.length - 1; i >= 0; i--) {
      const msg = history[i];
      if (msg.role === "user") {
        const m = msg.content.match(/\bmy name is (\w+)\b/i);
        if (m) return `Your name is ${m[1]}.`;
      }
    }
    return "I don't know your name yet. Feel free to tell me!";
  }

  // "do you remember what I said about X?" / general "remember" queries
  if (/\bdo you remember\b/i.test(userInput)) {
    if (history.length > 0) {
      const lastUser = [...history].reverse().find(m => m.role === "user");
      if (lastUser) return `The last thing you told me was: "${lastUser.content}"`;
    }
    return "I don't have anything stored from our conversation yet.";
  }

  return null;
}

// ─── Response generation ──────────────────────────────────────────────────────

function generateResponse(userInput: string, history: ConversationHistory): string {
  // Check history-aware queries first
  const memoryResponse = lookupFromHistory(userInput, history);
  if (memoryResponse) return memoryResponse;

  for (const rule of RULES) {
    if (rule.pattern.test(userInput)) {
      return rule.response;
    }
  }

  // Use history length as a seed to rotate fallback responses
  const index = history.length % FALLBACK_RESPONSES.length;
  return FALLBACK_RESPONSES[index];
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("chat-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.INTENT_CHAT],
  async (_topic, _key, value) => {
    const event = value as IntentGeneralChatEvent;
    const { userId, userInput, context } = event;

    console.log(`[chat] userId=${userId} input="${userInput}" historyLength=${context.length}`);

    const result = generateResponse(userInput, context);

    const payload: AppResultEvent = {
      userId,
      type: "chat",
      result,
      success: true,
      timestamp: new Date().toISOString(),
    };

    console.log(`[chat] result="${result}"`);

    await sendMessage(producer, TOPICS.APP_RESULTS, userId, payload);
  }
);

console.log("[chat] GeneralChatApp started.");
