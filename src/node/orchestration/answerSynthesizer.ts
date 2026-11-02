import {
  createProducer,
  createConsumer,
  publish,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { callLLM } from "../../../shared/llm/callLLM";
import { getHistory, appendToHistory } from "../../../shared/state/historyStore";
import { initializeSynthStore, synthStore } from "../../../shared/state/synthStore";
import { cacheUserQuery, synthesize, type SynthesizeDeps } from "./synthesize";

// Service shell: wires Kafka, the stores and the real LLM into synthesize.ts.
// user-commands carries both UserQueryReceived (cached in .synth-store so the question
// survives a restart) and SynthesizeFinalAnswerRequested (synthesized). The ordering
// rules live in synthesize.ts.

const SYNTHESIS_LLM = { budgetMs: 60_000, maxAttempts: 3 } as const;

await initializeSynthStore();
console.log("[synthesizer] SynthStore initialized");

const producer = await createProducer();
const consumer = await createConsumer("answer-synthesizer");

registerShutdown([producer, consumer]);

const deps: SynthesizeDeps = {
  store: synthStore,
  callLLM: (prompt) => callLLM(prompt, SYNTHESIS_LLM),
  publish: (message) => publish(producer, TOPICS.CONVERSATION_EVENTS, message),
  getHistory,
  appendToHistory,
  now: () => Date.now(),
};

await subscribeAndRun({
  consumer,
  groupId: "answer-synthesizer",
  service: "synthesizer",
  producer,
  topic: TOPICS.USER_COMMANDS,
  types: ["UserQueryReceived", "SynthesizeFinalAnswerRequested"],
  handler: async (event) => {
    if ("eventType" in event) {
      await cacheUserQuery(event, deps);
      return;
    }
    await synthesize(event, deps);
  },
});

console.log("[synthesizer] AnswerSynthesizer started.");
