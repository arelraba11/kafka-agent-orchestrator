// Side-effect-free checks used by scripts/start.ts (which runs at import).

/** Ollama is only needed when the provider chain can reach it: `auto` and `ollama`. */
export function shouldPullModel(provider: string | undefined): boolean {
  return (provider?.trim() || "auto") !== "openai";
}

/** Whether an Ollama `/api/tags` response lists `model` (a name without a tag means `:latest`). */
export function hasModel(tags: unknown, model: string): boolean {
  const models = (tags as { models?: { name?: unknown }[] } | null)?.models;
  if (!Array.isArray(models)) return false;
  const wanted = model.includes(":") ? model : `${model}:latest`;
  return models.some((m) => m?.name === wanted);
}
