// reviewAnalytics.ts — Real-time insights display consumer
//
// Consumes processed-insights-topic and prints formatted output for each message.

import { createConsumer } from "../../shared/kafka/client";
import { TOPICS } from "../../shared/topics";
import type { ReviewInsightEvent } from "../../shared/types/reviews";

const GROUP_ID = process.env.ANALYTICS_GROUP_ID ?? "review-analytics-group";

// Running average state
let totalScore  = 0;
let reviewCount = 0;

const DIVIDER = "--------------------------------";

function printInsight(event: ReviewInsightEvent): void {
  totalScore  += event.score;
  reviewCount += 1;
  const average = (totalScore / reviewCount).toFixed(1);

  console.log(DIVIDER);
  console.log("New Insight Received!");
  console.log(`ID:        ${event.reviewId}`);
  console.log(`Score:     ${event.score}/10`);
  console.log(`Sentiment: ${event.overall_sentiment}`);
  console.log(`Summary:   ${event.summary}`);
  console.log(DIVIDER);
  console.log(`Average Score: ${average}/10  (over ${reviewCount} review${reviewCount === 1 ? "" : "s"})`);
  console.log();
}

async function main(): Promise<void> {
  const consumer = await createConsumer(GROUP_ID);

  await consumer.subscribe({ topic: TOPICS.PROCESSED_INSIGHTS, fromBeginning: false });

  console.log("[analytics] Started. Listening for insights...\n");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      let event: ReviewInsightEvent;
      try {
        event = JSON.parse(message.value.toString()) as ReviewInsightEvent;
      } catch (err) {
        console.error("[analytics] Failed to parse message:", err);
        return;
      }

      printInsight(event);
    },
  });
}

main();
