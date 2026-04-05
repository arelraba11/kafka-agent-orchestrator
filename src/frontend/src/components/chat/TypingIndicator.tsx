interface TypingIndicatorProps {
  phase?: "thinking" | "processing" | null;
}

export default function TypingIndicator({ phase }: TypingIndicatorProps) {
  const label = phase === "processing" ? "Processing tools…" : "Thinking…";
  return (
    <div className="flex items-center gap-2.5">
      <span className="text-xs text-(--foreground-2) font-medium">{label}</span>
      <div className="flex gap-1 items-center">
        {[0, 1, 2].map((i) => (
          <span
            key={i}
            className="h-1.5 w-1.5 rounded-full bg-(--primary)"
            style={{
              animation: `pulse-dot 1.2s ${i * 0.16}s ease-in-out infinite`,
            }}
          />
        ))}
      </div>
    </div>
  );
}
