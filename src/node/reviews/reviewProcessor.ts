// reviewProcessor.ts — Core LLM processing service
//
// Pipeline per message:
//   1. Consume ReviewEvent from raw-reviews-topic
//   2. LLM Router  — decide if intent is "analyzeReview"; skip otherwise
//   3. LLM Analyze — extract structured ReviewInsightEvent fields
//   4. Self-Correct — if score < 4 AND sentiment == "Positive", re-call LLM
//   5. Publish ReviewInsightEvent to processed-insights-topic
//
// LLM calls go through callLLM() (OpenAI). On failure the
// processor catches the error and falls back to deterministic stubs.

import { callLLM as generateText } from "../../../shared/llm/openai";
import { createProducer, createConsumer } from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { reviewRouterPrompt, reviewAnalyzerPrompt, selfCorrectionPrompt } from "../../../prompts/prompts";
import type { ReviewEvent, RouterDecision, AnalysisResult, ReviewInsightEvent } from "../../../shared/types/reviews";

const GROUP_ID = process.env.PROCESSOR_GROUP_ID ?? "review-processor-group";

// ---------------------------------------------------------------------------
// LLM — delegates to the shared OpenAI client
// ---------------------------------------------------------------------------
async function callLLM(prompt: string): Promise<string> {
  try {
    return await generateText(prompt);
  } catch (err) {
    console.error("[processor] LLM call failed:", err);
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Fallback stubs (used when callLLM throws)
// ---------------------------------------------------------------------------
function fallbackRouter(_text: string): RouterDecision {
  return { intent: "analyzeReview", reason: "fallback: assumed review" };
}

function fallbackAnalyzer(_text: string): AnalysisResult {
  return {
    summary: "Stub summary — LLM not available.",
    overall_sentiment: "Neutral",
    score: 5,
    aspects: [],
  };
}

// ---------------------------------------------------------------------------
// Step 1: Route — decide whether to process this message
// ---------------------------------------------------------------------------
async function route(text: string): Promise<RouterDecision> {
  try {
    const raw = await callLLM(reviewRouterPrompt(text));
    return JSON.parse(raw) as RouterDecision;
  } catch {
    return fallbackRouter(text);
  }
}

// ---------------------------------------------------------------------------
// Step 2: Analyze — extract structured insights from review text
// ---------------------------------------------------------------------------
async function analyze(text: string): Promise<AnalysisResult> {
  try {
    const raw = await callLLM(reviewAnalyzerPrompt(text));
    return JSON.parse(raw) as AnalysisResult;
  } catch {
    return fallbackAnalyzer(text);
  }
}

// ---------------------------------------------------------------------------
// Step 3: Self-correct — fix score < 4 + sentiment == "Positive" contradiction
// ---------------------------------------------------------------------------
async function selfCorrect(
  text: string,
  result: AnalysisResult
): Promise<AnalysisResult> {
  try {
    const raw = await callLLM(
      selfCorrectionPrompt(text, JSON.stringify(result), result.score)
    );
    return JSON.parse(raw) as AnalysisResult;
  } catch {
    return result;
  }
}

// ---------------------------------------------------------------------------
// Main consumer loop
// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  const consumer = await createConsumer(GROUP_ID);
  const producer = await createProducer();

  await consumer.subscribe({ topic: TOPICS.RAW_REVIEWS, fromBeginning: false });

  console.log("[processor] Started. Waiting for reviews...\n");

  await consumer.run({
    eachMessage: async ({ message, heartbeat }) => {
      // --- Parse ---
      if (!message.value) return;
      const review: ReviewEvent = JSON.parse(message.value.toString());

      console.log(`[processor] Received review ${review.reviewId}: "${review.text}"`);

      // --- Step 1: Route ---
      const decision = await route(review.text);

      console.log(
        `[processor] Routing decision for ${review.reviewId}: intent="${decision.intent}" reason="${decision.reason}"`
      );

      if (decision.intent !== "analyzeReview") {
        console.log(`[processor] Skipping ${review.reviewId} — not a review.\n`);
        return;
      }

      // --- Step 2: Analyze ---
      let result = await analyze(review.text);
      let corrected = false;

      // --- Step 3: Self-correct ---
      if (result.score < 4 && result.overall_sentiment === "Positive") {
        console.log(
          `[processor] Inconsistency detected for ${review.reviewId} ` +
          `(score=${result.score}, sentiment=${result.overall_sentiment}). Running self-correction...`
        );
        result = await selfCorrect(review.text, result);
        corrected = true;
        console.log(
          `[processor] Self-correction complete — score=${result.score}, sentiment=${result.overall_sentiment}`
        );
      }

      // --- Step 4: Publish ---
      const insight: ReviewInsightEvent = {
        reviewId:          review.reviewId,
        originalText:      review.text,
        summary:           result.summary,
        overall_sentiment: result.overall_sentiment,
        score:             result.score,
        aspects:           result.aspects,
        corrected,
        timestamp:         new Date().toISOString(),
      };

      await producer.send({
        topic: TOPICS.PROCESSED_INSIGHTS,
        messages: [{ key: insight.reviewId, value: JSON.stringify(insight) }],
      });

      console.log(`[processor] Insight published for ${review.reviewId}\n`);

      await heartbeat();
    },
  });
}

main();
