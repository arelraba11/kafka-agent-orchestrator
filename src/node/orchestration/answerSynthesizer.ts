import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { callLLM } from "../../../shared/llm/openai";
import { synthesisPrompt } from "../../../shared/prompts/synthesisPrompt";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { FinalAnswerSynthesized } from "../../../shared/schemas/FinalAnswerSynthesized";
import type { SynthesizeFinalAnswerRequested } from "../../../shared/schemas/SynthesizeFinalAnswerRequested";

// ─── User query cache ─────────────────────────────────────────────────────────
// user-commands carries both UserQueryReceived and SynthesizeFinalAnswerRequested.
// We cache userInput by conversationId so the synthesizer can include the
// original question in the LLM prompt.

const userQueryCache = new Map<string, string>();

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("answer-synthesizer");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.USER_COMMANDS],
  async (_topic, _key, value) => {
    const event = value as { commandType?: string; eventType?: string };

    // Cache user queries as they arrive on user-commands
    if (event.eventType === "UserQueryReceived") {
      const q = event as UserQueryReceived;
      userQueryCache.set(q.conversationId, q.payload.userInput);
      return;
    }

    if (event.commandType !== "SynthesizeFinalAnswerRequested") return;

    const command = event as SynthesizeFinalAnswerRequested;
    const { conversationId, timestamp: planCompletedAt, payload } = command;

    console.log(`[synthesizer] conversationId=${conversationId} results=${payload.results.length}`);

    const userQuery = userQueryCache.get(conversationId) ?? "";
    userQueryCache.delete(conversationId);

    const toolResults = payload.results.map((r) => {
      const item = r as { toolName: string; result: Record<string, unknown> };
      return {
        tool: item.toolName,
        // RAG results carry retrieved text in `context`; other tools use `value`
        result: String(item.result.value ?? item.result.context ?? JSON.stringify(item.result)),
      };
    });

    const answer = await callLLM(synthesisPrompt(userQuery, toolResults));

    const finalEvent: FinalAnswerSynthesized = {
      conversationId,
      timestamp: Date.now(),
      eventType: "FinalAnswerSynthesized",
      payload: { answer },
    };

    await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, finalEvent);

    const synthesizerLatency = Date.now() - planCompletedAt;
    console.log(`[synthesizer] conversationId=${conversationId} answer="${answer}"`);
    console.log(`[Benchmark] conversationId=${conversationId} synthesizerLatency=${synthesizerLatency}ms`);
  }
);

console.log("[synthesizer] AnswerSynthesizer started.");
