// ─── Customer Support Prompt Templates (Exercise 4) ───────────────────────────

// ─── PII Sanitization Prompt ──────────────────────────────────────────────────
// Used by: sanitizer-service
// Instructs Ollama to scrub PII from a customer message.
// Returns only the sanitized text — no explanation, no extra tokens.

export function buildSanitizePrompt(rawText: string): string {
  return [
    "You are a PII scrubbing assistant.",
    "Replace every person's name with [NAME].",
    "Replace every phone number (any format) with [NUMBER].",
    "Return only the sanitized text. Do not add explanations, notes, or extra lines.",
    "",
    `Input: ${rawText}`,
    "Output:",
  ].join("\n");
}

// ─── Sentiment Classification Prompt ─────────────────────────────────────────
// Used by: sentiment-analyzer
// Classifies customer message sentiment as POSITIVE or NEGATIVE.

export function sentimentPrompt(text: string): string {
  return `You are a sentiment classifier for customer support messages.
Classify the sentiment of the following message as either POSITIVE or NEGATIVE.

Return ONLY valid JSON. No explanation. No markdown.

Schema:
{
  "label": "POSITIVE" | "NEGATIVE",
  "score": <confidence between 0.0 and 1.0>
}

Message:
"""
${text}
"""`.trim();
}

// ─── Urgency Classification Prompt ───────────────────────────────────────────
// Used by: urgency-classifier
// Classifies customer message urgency into one of three categories.

export function urgencyPrompt(text: string): string {
  return `You are an urgency classifier for customer support messages.
Classify the urgency of the following message into exactly one of these categories:
- Urgent
- Complaint
- General Inquiry

Return ONLY valid JSON. No explanation. No markdown.

Schema:
{
  "label": "Urgent" | "Complaint" | "General Inquiry",
  "scores": {
    "Urgent": <confidence 0.0–1.0>,
    "Complaint": <confidence 0.0–1.0>,
    "General Inquiry": <confidence 0.0–1.0>
  }
}

Message:
"""
${text}
"""`.trim();
}
