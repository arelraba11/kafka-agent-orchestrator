import {
  createProducer,
  createConsumer,
  publish,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { callLLM } from "../../../shared/llm/openai";
import { synthesisPrompt } from "../../../shared/prompts/synthesisPrompt";
import type { FinalAnswerSynthesized } from "../../../shared/schemas/FinalAnswerSynthesized";
import { getHistory, appendToHistory } from "../../../shared/state/historyStore";

// ─── User query cache ─────────────────────────────────────────────────────────
// user-commands carries both UserQueryReceived and SynthesizeFinalAnswerRequested.
// We cache userInput by conversationId so the synthesizer can include the original
// question in the LLM prompt. sessionId comes from the command itself.

const userQueryCache = new Map<string, { userInput: string }>();

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("answer-synthesizer");

registerShutdown([producer, consumer]);

await subscribeAndRun({
  consumer,
  groupId: "answer-synthesizer",
  service: "synthesizer",
  producer,
  topic: TOPICS.USER_COMMANDS,
  types: ["UserQueryReceived", "SynthesizeFinalAnswerRequested"],
  handler: async (event) => {
    // Cache user queries as they arrive on user-commands
    if ("eventType" in event) {
      userQueryCache.set(event.conversationId, { userInput: event.payload.userInput });
      return;
    }

    const { conversationId, sessionId, timestamp: planCompletedAt, payload } = event;

    console.log(`[synthesizer] conversationId=${conversationId} results=${payload.results.length}`);

    const cached = userQueryCache.get(conversationId);
    const userQuery = cached?.userInput ?? "";
    userQueryCache.delete(conversationId);

    const history = await getHistory(sessionId);

    const toolResults = payload.results.map((item) => ({
      tool: item.toolName,
      // RAG results carry retrieved text in `context`; other tools use `value`
      result: String(item.result.value ?? item.result.context ?? JSON.stringify(item.result)),
    }));

    const answer = await callLLM(synthesisPrompt(userQuery, toolResults, history));

    const finalEvent: FinalAnswerSynthesized = {
      conversationId,
      timestamp: Date.now(),
      eventType: "FinalAnswerSynthesized",
      payload: { answer },
    };

    await publish(producer, TOPICS.CONVERSATION_EVENTS, finalEvent);

    if (sessionId) {
      await appendToHistory(sessionId, userQuery, answer);
    }

    const synthesizerLatency = Date.now() - planCompletedAt;
    console.log(`[synthesizer] conversationId=${conversationId} answer="${answer}"`);
    console.log(`[Benchmark] conversationId=${conversationId} synthesizerLatency=${synthesizerLatency}ms`);
  },
});

console.log("[synthesizer] AnswerSynthesizer started.");
