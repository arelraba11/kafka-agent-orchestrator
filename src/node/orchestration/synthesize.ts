import { LlmError } from "../../../shared/llm/errors";
import { synthesisPrompt } from "../../../shared/prompts/synthesisPrompt";
import { faultPoint } from "../../../shared/testing/faultPoint";
import type { ConversationFailed } from "../../../shared/schemas/ConversationFailed";
import type { FinalAnswerSynthesized } from "../../../shared/schemas/FinalAnswerSynthesized";
import type { SynthesizeFinalAnswerRequested } from "../../../shared/schemas/SynthesizeFinalAnswerRequested";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { HistoryMessage } from "../../../shared/state/historyStore";
import type { FailureCode, Outcome, SynthStore } from "../../../shared/state/synthStore";

// Side-effect-free: importing this module starts nothing. answerSynthesizer.ts is the
// service shell that wires Kafka, the stores and the real LLM into these functions.
//
// ORDER (publish → persist), for every outcome:
//   1. done:<cid> present?  → ignored duplicate (no LLM call, no publish, no history)
//   2. outcome = o:<cid> if memoized, else compute it (LLM call) and put o:<cid>
//   3. publish the terminal event (FinalAnswerSynthesized | ConversationFailed)
//   4. answer only: appendToHistory (idempotent by conversationId)
//   5. ONE batch: put done:<cid>, del q:<cid>, del o:<cid>
//
// INVARIANT: done:<cid> is never written unless step 3's publish succeeded. Writing
// done first and crashing before the publish would lose the user's notification.
// Any failure in 3-5 is retryable (StoreError / ProducerError): Kafka redelivers, step 2
// reuses o:<cid> so an outage never triggers another LLM call.

export interface SynthesizeDeps {
  store: SynthStore;
  /** The LLM call with its budget already bound. Throws LlmError. */
  callLLM(prompt: string): Promise<string>;
  /** Publishes to conversation-events. A failure must throw a RetryableError. */
  publish(message: FinalAnswerSynthesized | ConversationFailed): Promise<void>;
  getHistory(sessionId: string): Promise<HistoryMessage[]>;
  appendToHistory(sessionId: string, conversationId: string, userMsg: string, assistantMsg: string): Promise<void>;
  now(): number;
  log?: Pick<Console, "log" | "error">;
}

/** Maps an LlmError onto the failure code the user is shown a message for. */
export function failureCodeOf(err: LlmError): Exclude<FailureCode, "missing-question"> {
  switch (err.kind) {
    case "auth":
    case "config":
      return "llm-misconfigured";
    case "empty-response":
      return "empty-answer";
    default:
      return "llm-unavailable";
  }
}

/** Cache fill for UserQueryReceived: remember the question until its conversation is done. */
export async function cacheUserQuery(
  event: UserQueryReceived,
  deps: Pick<SynthesizeDeps, "store" | "log">
): Promise<void> {
  const { conversationId, payload } = event;
  // A query redelivered after its conversation completed must not leave an orphan q:
  // key that nothing would ever delete.
  if ((await deps.store.get(`done:${conversationId}`)) !== null) return;
  await deps.store.put(`q:${conversationId}`, { userInput: payload.userInput, sessionId: payload.sessionId });
}

async function computeOutcome(command: SynthesizeFinalAnswerRequested, deps: SynthesizeDeps): Promise<Outcome> {
  const { conversationId, sessionId, payload } = command;
  const log = deps.log ?? console;

  const query = await deps.store.get(`q:${conversationId}`);
  if (query === null) {
    return {
      kind: "failed",
      code: "missing-question",
      reason: `no stored question for conversationId=${conversationId} (q: key missing)`,
    };
  }

  const history = await deps.getHistory(sessionId);
  const toolResults = payload.results.map((item) => ({
    tool: item.toolName,
    // RAG results carry retrieved text in `context`; other tools use `value`
    result: String(item.result.value ?? item.result.context ?? JSON.stringify(item.result)),
  }));

  try {
    const answer = await deps.callLLM(synthesisPrompt(query.userInput, toolResults, history));
    return { kind: "answer", answer };
  } catch (err) {
    if (!(err instanceof LlmError)) throw err; // not an LLM outcome: let the wrapper classify it
    const code = failureCodeOf(err);
    const reason = `${err.provider}: ${err.kind}: ${err.message}`;
    if (code === "llm-misconfigured") {
      log.error(`[synthesizer] ERROR conversationId=${conversationId} LLM misconfigured: ${reason}`);
    }
    return { kind: "failed", code, reason };
  }
}

export async function synthesize(command: SynthesizeFinalAnswerRequested, deps: SynthesizeDeps): Promise<void> {
  const { conversationId, sessionId, timestamp: planCompletedAt, payload } = command;
  const { store } = deps;
  const log = deps.log ?? console;

  // 1. already finished: a duplicate PlanCompleted / command. No LLM call, no publish.
  if ((await store.get(`done:${conversationId}`)) !== null) {
    log.log(`[synthesizer] conversationId=${conversationId} ignored duplicate`);
    return;
  }

  log.log(`[synthesizer] conversationId=${conversationId} results=${payload.results.length}`);

  // 2. outcome: memoized, or computed once and memoized (a memo is not "done")
  let outcome = await store.get(`o:${conversationId}`);
  if (outcome === null) {
    outcome = await computeOutcome(command, deps);
    await faultPoint("synthesizer:after-llm");
    await store.put(`o:${conversationId}`, outcome);
    await faultPoint("synthesizer:after-memo");
  }

  // 3. publish the terminal event; a failure throws a RetryableError and done stays absent
  if (outcome.kind === "answer") {
    await deps.publish({
      conversationId,
      timestamp: deps.now(),
      eventType: "FinalAnswerSynthesized",
      payload: { answer: outcome.answer },
    });
  } else {
    await deps.publish({
      conversationId,
      timestamp: deps.now(),
      eventType: "ConversationFailed",
      payload: { stage: "synthesis", code: outcome.code, reason: outcome.reason },
    });
  }
  await faultPoint("synthesizer:after-publish");

  // 4. history (answers only), idempotent by conversationId
  if (outcome.kind === "answer" && sessionId) {
    const query = await store.get(`q:${conversationId}`);
    await deps.appendToHistory(sessionId, conversationId, query?.userInput ?? "", outcome.answer);
  }
  await faultPoint("synthesizer:after-history");

  // 5. done + cleanup, atomically
  await store.batch([
    { type: "put", key: `done:${conversationId}`, value: { at: deps.now() } },
    { type: "del", key: `q:${conversationId}` },
    { type: "del", key: `o:${conversationId}` },
  ]);

  if (outcome.kind === "answer") {
    log.log(`[synthesizer] conversationId=${conversationId} answer="${outcome.answer}"`);
    log.log(`[Benchmark] conversationId=${conversationId} synthesizerLatency=${deps.now() - planCompletedAt}ms`);
  } else {
    log.log(`[synthesizer] conversationId=${conversationId} FAILED stage=synthesis code=${outcome.code} reason=${JSON.stringify(outcome.reason)}`);
  }
}
