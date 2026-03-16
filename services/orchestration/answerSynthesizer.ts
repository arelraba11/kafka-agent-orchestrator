import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { PlanCompleted } from "../../shared/schemas/PlanCompleted";
import type { FinalAnswerSynthesized } from "../../shared/schemas/FinalAnswerSynthesized";

// ─── Result extraction ────────────────────────────────────────────────────────
// Each entry in PlanCompleted.payload.results has the shape:
//   { toolName: string, result: { value: string, success: boolean, ... } }
// This matches what orchestrator.ts pushes after each ToolInvocationResulted.

interface ToolResult {
  toolName: string;
  result: Record<string, unknown>;
}

function extractAnswer(results: Record<string, unknown>[]): string {
  const lines: string[] = [];

  for (const item of results) {
    const { toolName, result } = item as unknown as ToolResult;
    const value = result.value as string | undefined;

    if (!value) continue;

    switch (toolName) {
      case "weather":
        lines.push(`Weather: ${value}`);
        break;
      case "exchange":
        lines.push(`Exchange: ${value}`);
        break;
      case "math":
        lines.push(`Result: ${value}`);
        break;
      case "chat":
        lines.push(value);
        break;
      default:
        lines.push(`[${toolName}] ${value}`);
    }
  }

  return lines.length > 0
    ? lines.join("\n")
    : "I was unable to process your request.";
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("answer-synthesizer");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as { eventType: string };
    if (event.eventType !== "PlanCompleted") return;

    const planCompleted = event as PlanCompleted;
    const { conversationId, timestamp: planCompletedAt, payload } = planCompleted;

    console.log(`[synthesizer] conversationId=${conversationId} results=${payload.results.length}`);

    const answer = extractAnswer(payload.results);

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
