// ─── RAG Augmentation Prompt ──────────────────────────────────────────────────
// Used by: rag_worker / getProductInformation tool (final project)
// Technique: Retrieval-Augmented Generation (RAG)
// Purpose:   Inject retrieved knowledge context so the LLM answers only from
//            verified product documents rather than general training knowledge.

export function ragPrompt(userQuery: string, retrievedContext: string): string {
  return `
You are a product information assistant.
Answer the user's question using ONLY the context provided below.

Rules:
- Base your answer strictly on the provided context. Do not add facts from general knowledge.
- If the context does not contain enough information to answer, say so clearly.
- Keep the answer concise: 2–4 sentences maximum.
- Do not repeat the question back to the user.
- Do not include markdown formatting in your response.

Context:
"""
${retrievedContext}
"""

User question: "${userQuery}"

Answer:
`.trim();
}
