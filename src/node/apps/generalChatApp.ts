import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";

// ─── Rule-based response engine ───────────────────────────────────────────────

interface Rule {
  pattern: RegExp;
  response: string;
}

const GUARDRAIL_RULES: Rule[] = [
  { pattern: /\b(bomb|weapon|explosive|poison|kill|harm|attack|illegal)\b/i,
    response: "I'm not able to help with that request. Please ask me something else." },
];

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
    response: "I'm a distributed AI agent built on Kafka microservices. Each reply you see went through an intent router, an orchestrator, and a synthesizer!" },

  { pattern: /\b(thank|thanks|thank you)\b/i,
    response: "You're welcome! Let me know if there's anything else I can help with." },

  { pattern: /\b(bye|goodbye|see you|ciao)\b/i,
    response: "Goodbye! Feel free to come back anytime." },

  { pattern: /\b(joke|funny|humor)\b/i,
    response: "Why do programmers prefer dark mode? Because light attracts bugs!" },

  { pattern: /\b(meaning of life|42)\b/i,
    response: "42, obviously. Though the real answer might be in the journey of asking the question." },

  { pattern: /\b(happiness|joy|fulfillment)\b/i,
    response: "Happiness from a purchase is often short-lived, but the right product can genuinely improve your daily life and bring lasting satisfaction." },

  { pattern: /\b(recommend|suggestion|best|should i buy)\b/i,
    response: "Based on the product information available, I'd recommend considering your specific needs and budget. Each product has unique strengths worth exploring." },
];

const FALLBACK_RESPONSES = [
  "That's an interesting thought. Tell me more.",
  "I'm not sure I fully understand. Could you elaborate?",
  "Good question. I'd need more context to give you a proper answer.",
  "Hmm, that's a broad topic. What aspect interests you most?",
  "I hear you. Let's explore that further.",
];

function generateResponse(userInput: string): string {
  for (const rule of GUARDRAIL_RULES) {
    if (rule.pattern.test(userInput)) return rule.response;
  }

  for (const rule of RULES) {
    if (rule.pattern.test(userInput)) return rule.response;
  }

  const index = userInput.length % FALLBACK_RESPONSES.length;
  return FALLBACK_RESPONSES[index];
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("chat-tool-worker");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.TOOL_INVOCATION_REQUESTS],
  async (_topic, _key, value) => {
    const req = value as ToolInvocationRequested;
    if (req.payload.toolName !== "chat") return;

    const { conversationId } = req;
    const userInput = (req.payload.input.userInput as string) ?? "";

    console.log(`[chat] conversationId=${conversationId} input="${userInput}"`);

    try {
      const resultStr = generateResponse(userInput);

      console.log(`[chat] result="${resultStr}"`);

      const event: ToolInvocationResulted = {
        conversationId,
        timestamp: Date.now(),
        eventType: "ToolInvocationResulted",
        payload: {
          toolName: "chat",
          result: { value: resultStr, success: true },
        },
      };

      await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, event);
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.error(`[chat] error="${errorMsg}"`);

      const event: ToolInvocationResulted = {
        conversationId,
        timestamp: Date.now(),
        eventType: "ToolInvocationResulted",
        payload: {
          toolName: "chat",
          result: { value: "", success: false, error: errorMsg },
        },
      };

      await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, event);
    }
  }
);

console.log("[chat] GeneralChatApp started.");
