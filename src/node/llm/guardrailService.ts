// ─── Guardrail Service ────────────────────────────────────────────────────────
// Consumes: user-input-events
// Produces: guardrail-violation-events  (only when a violation is detected)
//
// Detects unsafe inputs using keyword/regex rules — no LLM call.
// Safe inputs are silently ignored; downstream services consume the original
// user-input-events topic independently.

import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { UserInputEvent, GuardrailViolationEvent } from "../../../shared/types/events";

// ─── Violation rules ──────────────────────────────────────────────────────────

const POLITICS_KEYWORDS =
  /\b(politics|politician|election|government|democracy|dictatorship|president|prime minister|coup|regime|propaganda|vote|ballot|senate|congress|parliament)\b/i;

const MALWARE_KEYWORDS =
  /\b(hack|hacking|malware|virus|ransomware|exploit|zero.?day|sql injection|xss|ddos|botnet|trojan|keylogger|phishing|rootkit|payload|reverse shell|bypass security|bomb|weapon|explosive|kill|attack|murder|terrorist|terrorism|assassin|poison|grenade|firearm|shooting)\b/i;

function detectViolation(
  input: string
): { violationType: "politics" | "malware"; message: string } | null {
  if (MALWARE_KEYWORDS.test(input)) {
    return {
      violationType: "malware",
      message: "Input contains potentially malicious content and has been blocked.",
    };
  }
  if (POLITICS_KEYWORDS.test(input)) {
    return {
      violationType: "politics",
      message: "Input touches political topics which are outside the scope of this system.",
    };
  }
  return null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("guardrail-service");

registerShutdown([producer, consumer]);

await subscribeAndRun(consumer, [TOPICS.USER_INPUT], async (_topic, _key, value) => {
  const event = value as UserInputEvent;
  const violation = detectViolation(event.userInput);

  if (!violation) return; // safe — pass through silently

  const payload: GuardrailViolationEvent = {
    userId: event.userId,
    userInput: event.userInput,
    violationType: violation.violationType,
    message: violation.message,
    timestamp: new Date().toISOString(),
  };

  console.log(
    `[guardrail] VIOLATION userId=${event.userId} type=${violation.violationType} input="${event.userInput}"`
  );

  await sendMessage(producer, TOPICS.GUARDRAIL_VIOLATION, event.userId, payload);
});

console.log("[guardrail] GuardrailService started.");
